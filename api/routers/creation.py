"""Generic REST API for the Creation plugin system.

One router serves every creator: list manifests, generate, list/get artifacts,
download files, delete, and read/write flashcard review state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from loguru import logger
from pydantic import BaseModel
from surreal_commands import get_command_status

from open_notebook.config import DATA_FOLDER
from open_notebook.creation.registry import all_loaded, registry_digest
from open_notebook.domain.creation_artifact import CreationArtifact, FlashcardReview

router = APIRouter()


# --- schemas -----------------------------------------------------------------
class GenerateCreationRequest(BaseModel):
    creator_key: str
    name: str
    config: Dict[str, Any] = {}
    models: Dict[str, str] = {}
    notebook_id: Optional[str] = None
    content: Optional[str] = None
    language: Optional[str] = None
    instructions: Optional[str] = None


class GenerateCreationResponse(BaseModel):
    job_id: str
    artifact_id: str  # the artifact_uuid used for file paths
    status: str


class ReviewStateRequest(BaseModel):
    states: Dict[str, Dict[str, Any]]  # card_id -> serialized ts-fsrs Card


def _artifact_dir(artifact_uuid: str) -> Path:
    return Path(DATA_FOLDER) / "creation" / artifact_uuid


# --- manifests ---------------------------------------------------------------
@router.get("/creation/creators")
async def list_creators():
    """List installed creators (manifests) to drive nav, forms, and model pickers."""
    out = []
    for key, lc in all_loaded().items():
        if lc.available and lc.manifest:
            m = lc.manifest
            out.append(
                {
                    "key": m.key,
                    "name": m.name,
                    "version": m.version,
                    "description": m.description,
                    "emits": m.emits,
                    "model_roles": [r.model_dump() for r in m.model_roles],
                    "config_schema": m.config_schema,
                    "icon": m.icon,
                    "has_custom_form": m.has_custom_form,
                    "available": True,
                }
            )
        else:
            out.append({"key": key, "available": False, "error": lc.error})
    return {"creators": out, "registry_digest": registry_digest()}


# --- generation --------------------------------------------------------------
@router.post("/creation/artifacts/generate", response_model=GenerateCreationResponse)
async def generate_artifact(request: GenerateCreationRequest):
    from api.creation_service import CreationService

    try:
        job_id, artifact_uuid = await CreationService.submit_generation_job(
            creator_key=request.creator_key,
            name=request.name,
            config=request.config,
            models=request.models,
            notebook_id=request.notebook_id,
            content=request.content,
            language=request.language,
            instructions=request.instructions,
        )
        return GenerateCreationResponse(
            job_id=job_id, artifact_id=artifact_uuid, status="submitted"
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:  # noqa: BLE001
        logger.error(f"creation: generate failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to start generation")


@router.get("/creation/artifacts/jobs/{job_id}")
async def get_job_status(job_id: str):
    try:
        status = await get_command_status(job_id)
        return {
            "job_id": job_id,
            "status": status.status if status else "unknown",
            "error_message": getattr(status, "error_message", None) if status else None,
        }
    except Exception as e:  # noqa: BLE001
        logger.error(f"creation: job status failed: {e}")
        raise HTTPException(status_code=404, detail="Job not found")


# --- artifacts ---------------------------------------------------------------
def _serialize(artifact: CreationArtifact) -> dict:
    return {
        "id": str(artifact.id),
        "notebook_id": artifact.notebook_id,
        "creator_key": artifact.creator_key,
        "creator_version": artifact.creator_version,
        "sdk_version": artifact.sdk_version,
        "schema_id": artifact.schema_id,
        "name": artifact.name,
        "status": artifact.status,
        "data": artifact.data,
        "files": artifact.files,
        "config": artifact.config,
        "warnings": artifact.warnings,
        "errors": artifact.errors,
        "user_message": artifact.user_message,
        "error_message": artifact.error_message,
        "command": str(artifact.command) if artifact.command else None,
    }


@router.get("/creation/artifacts")
async def list_artifacts(
    notebook_id: Optional[str] = None, creator_key: Optional[str] = None
):
    artifacts = await CreationArtifact.list_for(
        notebook_id=notebook_id, creator_key=creator_key
    )
    return [_serialize(a) for a in artifacts]


@router.get("/creation/artifacts/{artifact_id}")
async def get_artifact(artifact_id: str):
    try:
        artifact = await CreationArtifact.get(artifact_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Artifact not found")
    payload = _serialize(artifact)
    payload["job_status"] = (await artifact.get_job_detail())["status"]
    return payload


@router.get("/creation/artifacts/{artifact_id}/files/{file_index}")
async def download_artifact_file(artifact_id: str, file_index: int):
    try:
        artifact = await CreationArtifact.get(artifact_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Artifact not found")

    if file_index < 0 or file_index >= len(artifact.files):
        raise HTTPException(status_code=404, detail="File not found")

    file_meta = artifact.files[file_index]
    # artifact_uuid is the directory name; recover it from the stored job, else
    # fall back to scanning by stored relative path under DATA_FOLDER/creation.
    artifact_dir = _find_artifact_dir(artifact, file_meta)
    if artifact_dir is None:
        raise HTTPException(status_code=404, detail="File not found on disk")
    abs_path = (artifact_dir / file_meta["path"]).resolve()
    if not str(abs_path).startswith(str(artifact_dir.resolve())) or not abs_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(
        abs_path,
        media_type=file_meta.get("content_type", "application/octet-stream"),
        filename=file_meta.get("filename", abs_path.name),
    )


def _find_artifact_dir(artifact: CreationArtifact, file_meta: dict) -> Optional[Path]:
    """Locate the on-disk dir for an artifact's files.

    Files are stored under DATA_FOLDER/creation/<artifact_uuid>/. The uuid is
    persisted on the file metadata's parent via the relative path; we search the
    creation root for the matching relative file.
    """
    root = Path(DATA_FOLDER) / "creation"
    rel = file_meta.get("path", "")
    if not root.exists():
        return None
    for child in root.iterdir():
        if child.is_dir() and (child / rel).exists():
            return child
    return None


@router.delete("/creation/artifacts/{artifact_id}")
async def delete_artifact(artifact_id: str):
    try:
        artifact = await CreationArtifact.get(artifact_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Artifact not found")

    # Remove on-disk files for this artifact.
    if artifact.files:
        artifact_dir = _find_artifact_dir(artifact, artifact.files[0])
        if artifact_dir and artifact_dir.exists():
            import shutil

            shutil.rmtree(artifact_dir, ignore_errors=True)

    # Remove any flashcard review rows.
    try:
        for review in await FlashcardReview.list_for_artifact(artifact_id):
            await review.delete()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"creation: failed clearing review state: {e}")

    await artifact.delete()
    return {"message": "Artifact deleted", "artifact_id": artifact_id}


# --- flashcard review state --------------------------------------------------
@router.get("/creation/artifacts/{artifact_id}/review")
async def get_review_state(artifact_id: str):
    reviews = await FlashcardReview.list_for_artifact(artifact_id)
    return {r.card_id: r.state for r in reviews}


@router.put("/creation/artifacts/{artifact_id}/review")
async def put_review_state(artifact_id: str, request: ReviewStateRequest):
    for card_id, state in request.states.items():
        await FlashcardReview.upsert(artifact_id, card_id, state)
    return {"message": "Review state saved", "count": len(request.states)}
