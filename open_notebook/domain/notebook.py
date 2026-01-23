import asyncio
import os
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator
from surreal_commands import submit_command

from open_notebook.database.sqlite_repository import ensure_record_id, repo_query
from open_notebook.domain.base import ObjectModel
from open_notebook.exceptions import DatabaseOperationError, InvalidInputError


class Notebook(ObjectModel):
    table_name: ClassVar[str] = "notebook"
    name: str
    description: str
    archived: Optional[bool] = False

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise InvalidInputError("Notebook name cannot be empty")
        return v

    async def get_sources(self) -> List["Source"]:
        try:
            # Extract numeric ID from 'notebook:123' format
            notebook_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            srcs = await repo_query(
                """
                SELECT s.id, s.file_path, s.url, s.title, s.topics, s.command_id, s.created, s.updated
                FROM source s
                JOIN source_notebook sn ON s.id = sn.source_id
                WHERE sn.notebook_id = ?
                ORDER BY s.updated DESC
                """,
                (notebook_id,),
            )
            result = []
            for src in srcs:
                # Convert to 'table:id' format and handle asset field
                src_data = dict(src)
                src_data["id"] = f"source:{src_data['id']}"
                # Reconstruct asset from file_path and url
                src_data["asset"] = Asset(
                    file_path=src_data.pop("file_path", None),
                    url=src_data.pop("url", None),
                )
                result.append(Source(**src_data))
            return result
        except Exception as e:
            logger.error(f"Error fetching sources for notebook {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def get_notes(self) -> List["Note"]:
        try:
            # Extract numeric ID from 'notebook:123' format
            notebook_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            notes = await repo_query(
                """
                SELECT n.id, n.title, n.note_type, n.created, n.updated
                FROM note n
                JOIN note_notebook nn ON n.id = nn.note_id
                WHERE nn.notebook_id = ?
                ORDER BY n.updated DESC
                """,
                (notebook_id,),
            )
            result = []
            for note in notes:
                note_data = dict(note)
                note_data["id"] = f"note:{note_data['id']}"
                result.append(Note(**note_data))
            return result
        except Exception as e:
            logger.error(f"Error fetching notes for notebook {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def get_chat_sessions(self) -> List["ChatSession"]:
        try:
            # Extract numeric ID from 'notebook:123' format
            notebook_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            sessions = await repo_query(
                """
                SELECT cs.id, cs.title, cs.model_override, cs.created, cs.updated
                FROM chat_session cs
                JOIN chat_session_reference csr ON cs.id = csr.chat_session_id
                WHERE csr.notebook_id = ?
                ORDER BY cs.updated DESC
                """,
                (notebook_id,),
            )
            result = []
            for session in sessions:
                session_data = dict(session)
                session_data["id"] = f"chat_session:{session_data['id']}"
                result.append(ChatSession(**session_data))
            return result
        except Exception as e:
            logger.error(
                f"Error fetching chat sessions for notebook {self.id}: {str(e)}"
            )
            logger.exception(e)
            raise DatabaseOperationError(e)


class Asset(BaseModel):
    file_path: Optional[str] = None
    url: Optional[str] = None


class SourceEmbedding(ObjectModel):
    table_name: ClassVar[str] = "source_embedding"
    content: str
    source_id: Optional[int] = None

    async def get_source(self) -> "Source":
        try:
            # Get the source_id from this embedding
            embedding_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            result = await repo_query(
                """
                SELECT s.id, s.file_path, s.url, s.title, s.topics, s.full_text,
                       s.command_id, s.created, s.updated
                FROM source s
                JOIN source_embedding se ON s.id = se.source_id
                WHERE se.id = ?
                """,
                (embedding_id,),
            )
            if result:
                src_data = dict(result[0])
                src_data["id"] = f"source:{src_data['id']}"
                src_data["asset"] = Asset(
                    file_path=src_data.pop("file_path", None),
                    url=src_data.pop("url", None),
                )
                return Source(**src_data)
            raise DatabaseOperationError(f"Source not found for embedding {self.id}")
        except Exception as e:
            logger.error(f"Error fetching source for embedding {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)


class SourceInsight(ObjectModel):
    table_name: ClassVar[str] = "source_insight"
    insight_type: str
    content: str
    source_id: Optional[int] = None

    async def get_source(self) -> "Source":
        try:
            # Get the source_id from this insight
            insight_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            result = await repo_query(
                """
                SELECT s.id, s.file_path, s.url, s.title, s.topics, s.full_text,
                       s.command_id, s.created, s.updated
                FROM source s
                JOIN source_insight si ON s.id = si.source_id
                WHERE si.id = ?
                """,
                (insight_id,),
            )
            if result:
                src_data = dict(result[0])
                src_data["id"] = f"source:{src_data['id']}"
                src_data["asset"] = Asset(
                    file_path=src_data.pop("file_path", None),
                    url=src_data.pop("url", None),
                )
                return Source(**src_data)
            raise DatabaseOperationError(f"Source not found for insight {self.id}")
        except Exception as e:
            logger.error(f"Error fetching source for insight {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def save_as_note(self, notebook_id: Optional[str] = None) -> Any:
        source = await self.get_source()
        note = Note(
            title=f"{self.insight_type} from source {source.title}",
            content=self.content,
        )
        await note.save()
        if notebook_id:
            await note.add_to_notebook(notebook_id)
        return note


class Source(ObjectModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    table_name: ClassVar[str] = "source"
    asset: Optional[Asset] = None
    title: Optional[str] = None
    topics: Optional[List[str]] = Field(default_factory=list)
    full_text: Optional[str] = None
    command: Optional[str] = Field(
        default=None, description="Link to command processing job"
    )

    @field_validator("command", mode="before")
    @classmethod
    def parse_command(cls, value):
        """Parse command field to string format"""
        if value is not None:
            return str(value)
        return value

    @field_validator("id", mode="before")
    @classmethod
    def parse_id(cls, value):
        """Parse id field to handle various input formats"""
        if value is None:
            return None
        return str(value) if value else None

    async def get_status(self) -> Optional[str]:
        """Get the processing status of the associated command"""
        if not self.command:
            return None

        try:
            from surreal_commands import get_command_status

            status = await get_command_status(str(self.command))
            return status.status if status else "unknown"
        except Exception as e:
            logger.warning(f"Failed to get command status for {self.command}: {e}")
            return "unknown"

    async def get_processing_progress(self) -> Optional[Dict[str, Any]]:
        """Get detailed processing information for the associated command"""
        if not self.command:
            return None

        try:
            from surreal_commands import get_command_status

            status_result = await get_command_status(str(self.command))
            if not status_result:
                return None

            # Extract execution metadata if available
            result = getattr(status_result, "result", None)
            execution_metadata = (
                result.get("execution_metadata", {}) if isinstance(result, dict) else {}
            )

            return {
                "status": status_result.status,
                "started_at": execution_metadata.get("started_at"),
                "completed_at": execution_metadata.get("completed_at"),
                "error": getattr(status_result, "error_message", None),
                "result": result,
            }
        except Exception as e:
            logger.warning(f"Failed to get command progress for {self.command}: {e}")
            return None

    async def get_context(
        self, context_size: Literal["short", "long"] = "short"
    ) -> Dict[str, Any]:
        insights_list = await self.get_insights()
        insights = [insight.model_dump() for insight in insights_list]
        if context_size == "long":
            return dict(
                id=self.id,
                title=self.title,
                insights=insights,
                full_text=self.full_text,
            )
        else:
            return dict(id=self.id, title=self.title, insights=insights)

    async def get_embedded_chunks(self) -> int:
        try:
            source_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)
            result = await repo_query(
                "SELECT COUNT(*) as chunks FROM source_embedding WHERE source_id = ?",
                (source_id,),
            )
            if len(result) == 0:
                return 0
            return result[0]["chunks"]
        except Exception as e:
            logger.error(f"Error fetching chunks count for source {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(f"Failed to count chunks for source: {str(e)}")

    async def get_insights(self) -> List[SourceInsight]:
        try:
            source_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)
            result = await repo_query(
                "SELECT * FROM source_insight WHERE source_id = ?",
                (source_id,),
            )
            insights = []
            for insight in result:
                insight_data = dict(insight)
                insight_data["id"] = f"source_insight:{insight_data['id']}"
                insights.append(SourceInsight(**insight_data))
            return insights
        except Exception as e:
            logger.error(f"Error fetching insights for source {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError("Failed to fetch insights for source")

    async def add_to_notebook(self, notebook_id: str) -> Any:
        if not notebook_id:
            raise InvalidInputError("Notebook ID must be provided")
        return await self.relate("reference", notebook_id)

    async def vectorize(self) -> str:
        """
        Submit vectorization as a background job using the embed_source command.

        This method leverages the job-based architecture to prevent HTTP connection
        pool exhaustion when processing large documents. The embed_source command:
        1. Detects content type from file path
        2. Chunks text using content-type aware splitter
        3. Generates all embeddings in a single API call
        4. Bulk inserts source_embedding records

        Returns:
            str: The command/job ID that can be used to track progress via the commands API

        Raises:
            ValueError: If source has no text to vectorize
            DatabaseOperationError: If job submission fails
        """
        logger.info(f"Submitting embed_source job for source {self.id}")

        try:
            if not self.full_text:
                raise ValueError(f"Source {self.id} has no text to vectorize")

            # Submit the embed_source command
            command_id = submit_command(
                "open_notebook",
                "embed_source",
                {"source_id": str(self.id)},
            )

            command_id_str = str(command_id)
            logger.info(
                f"Embed source job submitted for source {self.id}: "
                f"command_id={command_id_str}"
            )

            return command_id_str

        except Exception as e:
            logger.error(
                f"Failed to submit embed_source job for source {self.id}: {e}"
            )
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def add_insight(self, insight_type: str, content: str) -> Any:
        """
        Add an insight to this source.

        Creates the insight record without embedding, then submits an async
        embed_insight command to generate the embedding in the background.

        Args:
            insight_type: Type/category of the insight
            content: The insight content text

        Returns:
            The created insight record(s)
        """
        if not insight_type or not content:
            raise InvalidInputError("Insight type and content must be provided")
        try:
            source_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)

            # Create insight WITHOUT embedding (fire-and-forget embedding via command)
            result = await repo_query(
                """
                INSERT INTO source_insight (source_id, insight_type, content, created)
                VALUES (?, ?, ?, datetime('now'))
                RETURNING *
                """,
                (source_id, insight_type, content),
            )

            # Submit embedding command (fire-and-forget)
            if result and len(result) > 0:
                insight_id = f"source_insight:{result[0].get('id', '')}"
                if insight_id:
                    submit_command(
                        "open_notebook",
                        "embed_insight",
                        {"insight_id": insight_id},
                    )
                    logger.debug(f"Submitted embed_insight command for {insight_id}")

            return result
        except Exception as e:
            logger.error(f"Error adding insight to source {self.id}: {str(e)}")
            raise

    def _prepare_save_data(self) -> dict:
        """Override to flatten asset and handle command field"""
        data = super()._prepare_save_data()

        # Flatten asset into file_path and url for SQLite storage
        asset = data.pop("asset", None)
        if asset:
            if isinstance(asset, dict):
                data["file_path"] = asset.get("file_path")
                data["url"] = asset.get("url")
            elif isinstance(asset, Asset):
                data["file_path"] = asset.file_path
                data["url"] = asset.url

        # Ensure command is stored as string
        if data.get("command") is not None:
            data["command_id"] = str(data.pop("command"))
        else:
            data.pop("command", None)

        return data

    async def delete(self) -> bool:
        """Delete source and clean up associated file, embeddings, and insights."""
        # Clean up uploaded file if it exists
        if self.asset and self.asset.file_path:
            file_path = Path(self.asset.file_path)
            if file_path.exists():
                try:
                    os.unlink(file_path)
                    logger.info(f"Deleted file for source {self.id}: {file_path}")
                except Exception as e:
                    logger.warning(
                        f"Failed to delete file {file_path} for source {self.id}: {e}. "
                        "Continuing with database deletion."
                    )
            else:
                logger.debug(
                    f"File {file_path} not found for source {self.id}, skipping cleanup"
                )

        # Delete associated embeddings and insights to prevent orphaned records
        # Note: With ON DELETE CASCADE, this should happen automatically,
        # but we do it explicitly for safety
        try:
            source_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)
            await repo_query(
                "DELETE FROM source_embedding WHERE source_id = ?",
                (source_id,),
            )
            await repo_query(
                "DELETE FROM source_insight WHERE source_id = ?",
                (source_id,),
            )
            logger.debug(f"Deleted embeddings and insights for source {self.id}")
        except Exception as e:
            logger.warning(
                f"Failed to delete embeddings/insights for source {self.id}: {e}. "
                "Continuing with source deletion."
            )

        # Call parent delete to remove database record
        return await super().delete()


class Note(ObjectModel):
    table_name: ClassVar[str] = "note"
    title: Optional[str] = None
    note_type: Optional[Literal["human", "ai"]] = None
    content: Optional[str] = None

    @field_validator("content")
    @classmethod
    def content_must_not_be_empty(cls, v):
        if v is not None and not v.strip():
            raise InvalidInputError("Note content cannot be empty")
        return v

    async def save(self) -> Optional[str]:
        """
        Save the note and submit embedding command.

        Overrides ObjectModel.save() to submit an async embed_note command
        after saving, instead of inline embedding.

        Returns:
            Optional[str]: The command_id if embedding was submitted, None otherwise
        """
        # Call parent save (without embedding)
        await super().save()

        # Submit embedding command (fire-and-forget) if note has content
        if self.id and self.content and self.content.strip():
            command_id = submit_command(
                "open_notebook",
                "embed_note",
                {"note_id": str(self.id)},
            )
            logger.debug(f"Submitted embed_note command {command_id} for {self.id}")
            return command_id

        return None

    async def add_to_notebook(self, notebook_id: str) -> Any:
        if not notebook_id:
            raise InvalidInputError("Notebook ID must be provided")
        return await self.relate("artifact", notebook_id)

    def get_context(
        self, context_size: Literal["short", "long"] = "short"
    ) -> Dict[str, Any]:
        if context_size == "long":
            return dict(id=self.id, title=self.title, content=self.content)
        else:
            return dict(
                id=self.id,
                title=self.title,
                content=self.content[:100] if self.content else None,
            )


class ChatSession(ObjectModel):
    table_name: ClassVar[str] = "chat_session"
    nullable_fields: ClassVar[set[str]] = {"model_override"}
    title: Optional[str] = None
    model_override: Optional[str] = None

    async def relate_to_notebook(self, notebook_id: str) -> Any:
        if not notebook_id:
            raise InvalidInputError("Notebook ID must be provided")
        return await self.relate("refers_to", notebook_id)

    async def relate_to_source(self, source_id: str) -> Any:
        if not source_id:
            raise InvalidInputError("Source ID must be provided")
        return await self.relate("refers_to", source_id)


async def text_search(
    keyword: str, results: int, source: bool = True, note: bool = True
):
    if not keyword:
        raise InvalidInputError("Search keyword cannot be empty")
    try:
        from open_notebook.database.sqlite_search import text_search as sqlite_text_search

        search_results = await sqlite_text_search(
            query_text=keyword,
            match_count=results,
            search_sources=source,
            search_notes=note,
        )
        return search_results
    except Exception as e:
        logger.error(f"Error performing text search: {str(e)}")
        logger.exception(e)
        raise DatabaseOperationError(e)


async def vector_search(
    keyword: str,
    results: int,
    source: bool = True,
    note: bool = True,
    minimum_score=0.2,
):
    if not keyword:
        raise InvalidInputError("Search keyword cannot be empty")
    try:
        from open_notebook.database.sqlite_search import (
            vector_search as sqlite_vector_search,
        )
        from open_notebook.utils.embedding import generate_embedding

        # Use unified embedding function (handles chunking if query is very long)
        embed = await generate_embedding(keyword)
        search_results = await sqlite_vector_search(
            query_embedding=embed,
            match_count=results,
            search_sources=source,
            search_notes=note,
            min_similarity=minimum_score,
        )
        return search_results
    except Exception as e:
        logger.error(f"Error performing vector search: {str(e)}")
        logger.exception(e)
        raise DatabaseOperationError(e)
