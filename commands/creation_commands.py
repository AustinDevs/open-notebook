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
    instructions: Optional[str] = None
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


def _spec_template(data) -> Optional[str]:
    """Extract the AntV template name from an infographic.v2 ``spec`` (its first
    line is ``infographic <template>``) so the next variant can be told to avoid
    it. Returns None for schemas without a template token (e.g. mindmaps)."""
    if not isinstance(data, dict):
        return None
    spec = data.get("spec")
    if not isinstance(spec, str):
        return None
    first = spec.strip().split("\n", 1)[0].split()
    if len(first) >= 2 and first[0] == "infographic":
        return first[1]
    return None


async def _generate_one(
    *, input_data, creator, manifest, models, name, instructions, variant_uuid
):
    """Create one CreationArtifact: run the creator, validate against its schema,
    persist files (path-contained) + data. Always leaves a terminal status."""
    from open_notebook.domain.creation_artifact import CreationArtifact

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
        name=name,
        status="running",
        config=input_data.config,
        command=command_id,
    )
    await artifact.save()

    output_dir = Path(DATA_FOLDER) / "creation" / variant_uuid
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        request = CreationRequest(
            content=ContentBundle(text=input_data.content),
            config=input_data.config,
            models=models,
            output_dir=str(output_dir),
            artifact_id=variant_uuid,
            language=input_data.language,
            instructions=instructions,
            user_id=input_data.user_id,
        )

        result = await creator.generate(request)

        if result.schema_id not in manifest.emits:
            raise ValueError(
                f"creator emitted '{result.schema_id}' not in manifest.emits {manifest.emits}"
            )
        if result.status != "FAILURE":
            validate_artifact_data(result.schema_id, result.data)

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
                    "path": str(rel),
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
    except Exception as e:  # noqa: BLE001
        logger.exception(f"creation: generation failed for {input_data.creator_key}")
        artifact.status = "failed"
        artifact.error_message = str(e)
        try:
            await artifact.save()
        except Exception:  # noqa: BLE001
            logger.error("creation: failed to persist failed status")
        try:
            if output_dir.exists() and not artifact.files:
                shutil.rmtree(output_dir, ignore_errors=True)
        except Exception:  # noqa: BLE001
            pass
    return artifact


@command("generate_creation_artifact", app="open_notebook", retry={"max_attempts": 1})
async def generate_creation_artifact_command(
    input_data: CreationGenerationInput,
) -> CreationGenerationOutput:
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

    # Resolve declared model roles once (credentials stay in-process); reused for
    # every variant below.
    try:
        models: Dict[str, ModelRole] = {}
        for spec in manifest.model_roles:
            model_id = input_data.models.get(spec.key)
            if not model_id:
                if spec.required:
                    raise ValueError(f"no model selected for required role '{spec.key}'")
                continue
            provider, model_name, cfg = await _resolve_model_config(model_id)
            models[spec.key] = ModelRole(provider=provider, model=model_name, config=cfg)
    except Exception as e:  # noqa: BLE001
        logger.exception(f"creation: model resolution failed for {input_data.creator_key}")
        return CreationGenerationOutput(
            success=False, error_message=str(e), processing_time=time.time() - start
        )

    # Generate `count` artifacts (default 1). For count>1, each variant is told to
    # use a different template/emphasis — and which AntV templates were already
    # used — so the set is visually distinct.
    count = max(1, min(int(input_data.config.get("count", 1) or 1), 6))
    artifacts = []
    used_templates: list = []
    for i in range(count):
        name = input_data.name if count == 1 else f"{input_data.name} {i + 1}"
        instr = (input_data.instructions or "").strip()
        if count > 1:
            directive = (
                f"This is variant {i + 1} of {count}. Make it clearly DIFFERENT from the "
                "other variants: choose a different layout/template family and emphasize a "
                "different aspect of the content."
            )
            if used_templates:
                directive += (
                    f" Do NOT reuse these templates: {', '.join(used_templates)}."
                )
            instr = f"{instr}\n\n{directive}".strip()
        variant_uuid = (
            input_data.artifact_uuid
            if count == 1
            else f"{input_data.artifact_uuid}-{i}"
        )
        artifact = await _generate_one(
            input_data=input_data,
            creator=creator,
            manifest=manifest,
            models=models,
            name=name,
            instructions=instr or None,
            variant_uuid=variant_uuid,
        )
        artifacts.append(artifact)
        tmpl = _spec_template(getattr(artifact, "data", None))
        if tmpl:
            used_templates.append(tmpl)

    succeeded = sum(1 for a in artifacts if a.status in ("completed", "partial"))
    first = artifacts[0]
    logger.info(
        f"creation: {input_data.creator_key} generated {succeeded}/{count} artifact(s)"
    )
    return CreationGenerationOutput(
        success=succeeded > 0,
        artifact_id=str(first.id),
        status=first.status,
        error_message=None if succeeded > 0 else (first.error_message or "generation failed"),
        processing_time=time.time() - start,
    )
