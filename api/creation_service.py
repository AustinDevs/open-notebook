"""Service layer for the Creation plugin system.

Validates the creator + config, assembles notebook content, fills model defaults
per declared role, stamps the registry digest, and submits the generic job.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from loguru import logger
from surreal_commands import submit_command

from open_notebook.ai.models import DefaultModels
from open_notebook.creation.content import assemble_content
from open_notebook.creation.registry import get_creator, registry_digest

# default Model id per role kind, drawn from the user's configured defaults
_DEFAULT_BY_KIND = {
    "language": ("default_transformation_model", "default_chat_model"),
    "text_to_speech": ("default_text_to_speech_model",),
    "speech_to_text": ("default_speech_to_text_model",),
    "embedding": ("default_embedding_model",),
}

# Mirror provision_langchain_model: above this many tokens, language work runs on
# the user's large_context_model instead of the selected/default model.
LARGE_CONTEXT_THRESHOLD = 105_000


async def _default_model_for_kind(defaults: DefaultModels, kind: str) -> Optional[str]:
    for attr in _DEFAULT_BY_KIND.get(kind, ()):  # first non-empty wins
        value = getattr(defaults, attr, None)
        if value:
            return value
    return None


class CreationService:
    @staticmethod
    async def submit_generation_job(
        creator_key: str,
        name: str,
        config: Dict[str, Any],
        models: Optional[Dict[str, str]] = None,
        notebook_id: Optional[str] = None,
        content: Optional[str] = None,
        language: Optional[str] = None,
        instructions: Optional[str] = None,
    ) -> tuple[str, str]:
        """Returns (job_id, artifact_uuid)."""
        creator = get_creator(creator_key)  # raises ValueError if unknown/unavailable
        manifest = creator.manifest

        # Validate config against the creator's schema (surfaces as 422 upstream).
        creator.config_model.model_validate(config or {})

        # Assemble content first so model selection can adapt to its size.
        bundle = await assemble_content(notebook_id=notebook_id, content=content)
        defaults = await DefaultModels.get_instance()
        large_ctx_model = getattr(defaults, "large_context_model", None)
        use_large_ctx = bundle.token_count > LARGE_CONTEXT_THRESHOLD

        if use_large_ctx and not large_ctx_model:
            logger.warning(
                f"creation: large content ({bundle.token_count} tokens) but no "
                "large_context_model configured — generation may exceed the "
                "selected model's context window."
            )

        # Resolve model selections, filling defaults for unset roles. For language
        # work on large content, switch to large_context_model (same behavior as
        # provision_langchain_model) regardless of the requested model.
        selections: Dict[str, str] = dict(models or {})
        for spec in manifest.model_roles:
            if spec.kind == "language" and use_large_ctx and large_ctx_model:
                selections[spec.key] = large_ctx_model
                logger.info(
                    f"creation: large content ({bundle.token_count} tokens) -> "
                    f"using large_context_model for role '{spec.key}'"
                )
                continue
            if spec.key in selections and selections[spec.key]:
                continue
            default_id = await _default_model_for_kind(defaults, spec.kind)
            if default_id:
                selections[spec.key] = default_id
            elif spec.required:
                raise ValueError(
                    f"no model selected and no default for required role "
                    f"'{spec.key}' (kind={spec.kind})"
                )

        artifact_uuid = str(uuid.uuid4())

        # Ensure the command module (and registry) are imported before submitting.
        import commands.creation_commands  # noqa: F401

        job_id = submit_command(
            "open_notebook",
            "generate_creation_artifact",
            {
                "creator_key": creator_key,
                "name": name,
                "content": bundle.text,
                "config": config or {},
                "models": selections,
                "notebook_id": notebook_id,
                "language": language,
                "instructions": instructions,
                "artifact_uuid": artifact_uuid,
                "registry_digest": registry_digest(),
            },
        )
        if not job_id:
            raise ValueError("Failed to submit creation job")
        logger.info(
            f"creation: submitted {creator_key} job {job_id} (artifact {artifact_uuid})"
        )
        return str(job_id), artifact_uuid
