"""Generic surreal-command that runs any Creation creator.

One command for all creators: it resolves the creator's declared model roles,
assembles a CreationRequest, runs ``creator.generate``, validates + persists the
result, and stores produced files under DATA_FOLDER. Dispatch is by ``creator_key``.

NOTE: do NOT add `from __future__ import annotations` here — it turns the
CommandInput field annotations into strings, which breaks the model that
surreal-commands generates for the command input ("... is not fully defined").
"""

import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger
from open_notebook_creator_sdk import (
    ContentBundle,
    CreationRequest,
    ModelRole,
)
from open_notebook_creator_sdk import __version__ as SDK_VERSION
from open_notebook_creator_sdk.schemas import validate_artifact_data
from pydantic import BaseModel
from surreal_commands import CommandInput, CommandOutput, command

from open_notebook.config import DATA_FOLDER
from open_notebook.creation.registry import (
    get_creator,
    load_registry,
    registry_digest,
)
from open_notebook.database.repository import ensure_record_id
from open_notebook.podcasts.models import _resolve_model_config

# Ensure the registry is populated in the worker process at import time.
load_registry()

_STATUS_MAP = {"SUCCESS": "completed", "PARTIAL": "partial", "FAILURE": "failed"}


class CreationGenerationInput(CommandInput):
    creator_key: str
    name: str
    content: str
    config: Dict[str, Any] = {}
    models: Dict[str, str] = {}  # role key -> Model record id
    notebook_id: Optional[str] = None
    language: Optional[str] = None
    artifact_uuid: str
    registry_digest: Optional[str] = None
    user_id: Optional[str] = None


class CreationGenerationOutput(CommandOutput):
    success: bool
    artifact_id: Optional[str] = None
    status: Optional[str] = None
    error_message: Optional[str] = None
    processing_time: float


def _safe_dump(model) -> Any:
    if isinstance(model, BaseModel):
        return model.model_dump()
    return model


@command("generate_creation_artifact", app="open_notebook", retry={"max_attempts": 1})
async def generate_creation_artifact_command(
    input_data: CreationGenerationInput,
) -> CreationGenerationOutput:
    from open_notebook.domain.creation_artifact import CreationArtifact

    start = time.time()

    # Guard against API/worker registry skew.
    if input_data.registry_digest and input_data.registry_digest != registry_digest():
        msg = (
            f"registry digest mismatch (job={input_data.registry_digest} "
            f"worker={registry_digest()}); refusing to run stale creator set"
        )
        logger.error(f"creation: {msg}")
        return CreationGenerationOutput(
            success=False, error_message=msg, processing_time=time.time() - start
        )

    try:
        creator = get_creator(input_data.creator_key)
    except ValueError as e:
        return CreationGenerationOutput(
            success=False, error_message=str(e), processing_time=time.time() - start
        )

    manifest = creator.manifest
    # Validate config against the creator's own schema early.
    try:
        creator.config_model.model_validate(input_data.config)
    except Exception as e:  # noqa: BLE001
        return CreationGenerationOutput(
            success=False,
            error_message=f"invalid config: {e}",
            processing_time=time.time() - start,
        )

    # Create the artifact row up front, linked to this job.
    command_id = (
        ensure_record_id(input_data.execution_context.command_id)
        if input_data.execution_context
        else None
    )
    artifact = CreationArtifact(
        notebook_id=input_data.notebook_id,
        creator_key=input_data.creator_key,
        creator_version=manifest.version,
        sdk_version=SDK_VERSION,
        name=input_data.name,
        status="running",
        config=input_data.config,
        command=command_id,
    )
    await artifact.save()

    output_dir = Path(DATA_FOLDER) / "creation" / input_data.artifact_uuid
    try:
        # Resolve declared model roles -> ModelRole (credentials stay in-process).
        models: Dict[str, ModelRole] = {}
        for spec in manifest.model_roles:
            model_id = input_data.models.get(spec.key)
            if not model_id:
                if spec.required:
                    raise ValueError(f"no model selected for required role '{spec.key}'")
                continue
            provider, model_name, cfg = await _resolve_model_config(model_id)
            models[spec.key] = ModelRole(
                provider=provider, model=model_name, config=cfg
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        request = CreationRequest(
            content=ContentBundle(text=input_data.content),
            config=input_data.config,
            models=models,
            output_dir=str(output_dir),
            artifact_id=input_data.artifact_uuid,
            language=input_data.language,
            user_id=input_data.user_id,
        )

        result = await creator.generate(request)

        # Validate result against the declared schema contract.
        if result.schema_id not in manifest.emits:
            raise ValueError(
                f"creator emitted '{result.schema_id}' not in manifest.emits {manifest.emits}"
            )
        if result.status != "FAILURE":
            validate_artifact_data(result.schema_id, result.data)

        # Persist files: enforce containment, keep relative paths for serving.
        stored_files = []
        for f in result.files:
            rel = Path(f.path)
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError(f"creator returned unsafe file path: {f.path}")
            abs_path = (output_dir / rel).resolve()
            if not str(abs_path).startswith(str(output_dir.resolve())):
                raise ValueError(f"file path escapes output_dir: {f.path}")
            if not abs_path.exists():
                raise ValueError(f"creator declared missing file: {f.path}")
            stored_files.append(
                {
                    "filename": f.filename,
                    "content_type": f.content_type,
                    "path": str(rel),  # relative to the artifact dir
                    "label": f.label,
                }
            )

        artifact.schema_id = result.schema_id
        artifact.data = result.data
        artifact.files = stored_files
        artifact.warnings = result.warnings
        artifact.errors = [_safe_dump(e) for e in result.errors]
        artifact.user_message = result.user_message
        artifact.status = _STATUS_MAP.get(result.status, "completed")
        await artifact.save()

        logger.info(
            f"creation: {input_data.creator_key} -> artifact {artifact.id} "
            f"status={artifact.status} files={len(stored_files)}"
        )
        return CreationGenerationOutput(
            success=result.status != "FAILURE",
            artifact_id=str(artifact.id),
            status=artifact.status,
            error_message=result.user_message if result.status == "FAILURE" else None,
            processing_time=time.time() - start,
        )

    except Exception as e:  # noqa: BLE001
        logger.exception(f"creation: generation failed for {input_data.creator_key}")
        # Guarantee a terminal status is persisted.
        artifact.status = "failed"
        artifact.error_message = str(e)
        try:
            await artifact.save()
        except Exception:  # noqa: BLE001
            logger.error("creation: failed to persist failed status")
        # Best-effort cleanup of partial output dir.
        try:
            if output_dir.exists() and not artifact.files:
                shutil.rmtree(output_dir, ignore_errors=True)
        except Exception:  # noqa: BLE001
            pass
        return CreationGenerationOutput(
            success=False,
            artifact_id=str(artifact.id),
            status="failed",
            error_message=str(e),
            processing_time=time.time() - start,
        )
