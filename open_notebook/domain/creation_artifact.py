"""Domain models for the Creation plugin system.

``CreationArtifact`` is the single, generic record for every creator's output —
lifecycle metadata + a plugin-specific ``data`` blob + produced ``files``. The
plugin-specific shape lives in ``data`` (validated against the SDK schema named
by ``schema_id``); the DB doesn't need to know it.

``FlashcardReview`` holds per-(artifact, card, user) spaced-repetition state, kept
out of the generic artifact so it stays queryable rather than a JSON dump.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Union

from pydantic import ConfigDict, Field, field_validator
from surrealdb import RecordID

from open_notebook.database.repository import ensure_record_id, repo_query
from open_notebook.domain.base import ObjectModel


class CreationArtifact(ObjectModel):
    table_name: ClassVar[str] = "creation_artifact"
    nullable_fields: ClassVar[set[str]] = {
        "notebook_id",
        "command",
        "error_message",
        "creator_version",
        "sdk_version",
        "schema_id",
        "user_message",
    }

    model_config = ConfigDict(arbitrary_types_allowed=True)

    notebook_id: Optional[str] = None
    creator_key: str
    creator_version: Optional[str] = None
    sdk_version: Optional[str] = None
    schema_id: Optional[str] = None
    name: str
    status: str = "submitted"  # submitted | running | completed | partial | failed
    data: Dict[str, Any] = Field(default_factory=dict)
    files: List[Dict[str, Any]] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    command: Optional[Union[str, RecordID]] = None
    error_message: Optional[str] = None
    user_message: Optional[str] = None

    @field_validator("command", mode="before")
    @classmethod
    def parse_command(cls, value):
        if isinstance(value, str):
            return ensure_record_id(value)
        return value

    def _prepare_save_data(self) -> dict:
        data = super()._prepare_save_data()
        if data.get("command") is not None:
            data["command"] = ensure_record_id(data["command"])
        return data

    async def get_job_detail(self) -> dict:
        """Status + error_message from the linked surreal-commands job."""
        if not self.command:
            return {"status": None, "error_message": None}
        try:
            from surreal_commands import get_command_status

            status = await get_command_status(str(self.command))
            if not status:
                return {"status": "unknown", "error_message": None}
            return {
                "status": status.status,
                "error_message": getattr(status, "error_message", None),
            }
        except Exception:
            return {"status": "unknown", "error_message": None}

    @classmethod
    async def list_for(
        cls,
        notebook_id: Optional[str] = None,
        creator_key: Optional[str] = None,
    ) -> List["CreationArtifact"]:
        clauses = []
        params: Dict[str, Any] = {}
        if notebook_id:
            clauses.append("notebook_id = $notebook_id")
            params["notebook_id"] = notebook_id
        if creator_key:
            clauses.append("creator_key = $creator_key")
            params["creator_key"] = creator_key
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = await repo_query(
            f"SELECT * FROM {cls.table_name}{where} ORDER BY created DESC", params
        )
        return [cls(**row) for row in rows]


class FlashcardReview(ObjectModel):
    table_name: ClassVar[str] = "flashcard_review"
    nullable_fields: ClassVar[set[str]] = {"user_id"}

    artifact_id: str
    card_id: str
    user_id: Optional[str] = None
    # ts-fsrs Card state (serializable); dates stored as ISO strings
    state: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    async def list_for_artifact(
        cls, artifact_id: str, user_id: Optional[str] = None
    ) -> List["FlashcardReview"]:
        clauses = ["artifact_id = $artifact_id"]
        params: Dict[str, Any] = {"artifact_id": artifact_id}
        if user_id:
            clauses.append("user_id = $user_id")
            params["user_id"] = user_id
        rows = await repo_query(
            f"SELECT * FROM {cls.table_name} WHERE " + " AND ".join(clauses), params
        )
        return [cls(**row) for row in rows]

    @classmethod
    async def upsert(
        cls,
        artifact_id: str,
        card_id: str,
        state: Dict[str, Any],
        user_id: Optional[str] = None,
    ) -> "FlashcardReview":
        existing = await cls.list_for_artifact(artifact_id, user_id)
        match = next((r for r in existing if r.card_id == card_id), None)
        if match:
            match.state = state
            await match.save()
            return match
        review = cls(
            artifact_id=artifact_id, card_id=card_id, user_id=user_id, state=state
        )
        await review.save()
        return review
