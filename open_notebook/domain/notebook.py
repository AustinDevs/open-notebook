import asyncio
import os
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator

from open_notebook.database import (
    ensure_record_id,
    executor,
    is_sqlite,
    parse_id,
    repo_count_related,
    repo_count_source_embeddings,
    repo_delete_embeddings,
    repo_get_insights_for_source,
    repo_get_related,
    repo_get_sessions_for_source,
    repo_get_source_for_embedding,
    repo_get_source_for_insight,
    repo_get_sources_for_notebook,
    repo_query,
    repo_update_embedding,
    serialize_embedding,
)
from open_notebook.database.command_queue import get_command_status
from open_notebook.domain.base import ObjectModel
from open_notebook.exceptions import DatabaseOperationError, InvalidInputError

# Conditional import for SurrealDB RecordID
if not is_sqlite():
    from surrealdb import RecordID


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
            # Use unified repo method for getting related sources
            srcs = await repo_get_related(
                source_table="notebook",
                source_id=self.id,
                relation="reference",
                target_table="source",
            )
            result = []
            for src in srcs:
                src_data = dict(src)
                # Handle SQLite flattened asset format
                if "file_path" in src_data or "url" in src_data:
                    src_data["asset"] = Asset(
                        file_path=src_data.pop("file_path", None),
                        url=src_data.pop("url", None),
                    )
                # Handle SurrealDB nested format
                elif "source" in src_data:
                    src_data = src_data["source"]
                result.append(Source(**src_data))
            return result
        except Exception as e:
            logger.error(f"Error fetching sources for notebook {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def get_notes(self) -> List["Note"]:
        try:
            # Use unified repo method for getting related notes
            notes = await repo_get_related(
                source_table="notebook",
                source_id=self.id,
                relation="artifact",
                target_table="note",
            )
            result = []
            for note_data in notes:
                data = dict(note_data)
                # Handle SurrealDB nested format
                if "note" in data:
                    data = data["note"]
                result.append(Note(**data))
            return result
        except Exception as e:
            logger.error(f"Error fetching notes for notebook {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def get_chat_sessions(self) -> List["ChatSession"]:
        try:
            # Use unified repo method for getting related chat sessions
            sessions = await repo_get_related(
                source_table="notebook",
                source_id=self.id,
                relation="refers_to",
                target_table="chat_session",
            )
            result = []
            for session_data in sessions:
                data = dict(session_data)
                # Handle SurrealDB nested format
                if "chat_session" in data:
                    data = data["chat_session"]
                    if isinstance(data, list):
                        data = data[0]
                result.append(ChatSession(**data))
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

    @field_validator("source_id", mode="before")
    @classmethod
    def parse_source_id(cls, value):
        """Parse source_id to handle 'source:6' format from SurrealDB."""
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            # Handle 'source:6' format
            if ":" in value:
                return int(value.split(":")[1])
            return int(value)
        return value

    async def get_source(self) -> "Source":
        try:
            # Use unified repo function (works for both backends)
            src_data = await repo_get_source_for_embedding(self.id)
            if src_data:
                src_data = dict(src_data)
                # Handle flattened asset fields (SQLite) vs nested (SurrealDB)
                if "file_path" in src_data or "url" in src_data:
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

    @field_validator("source_id", mode="before")
    @classmethod
    def parse_source_id(cls, value):
        """Parse source_id to handle 'source:6' format from SurrealDB."""
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            # Handle 'source:6' format
            if ":" in value:
                return int(value.split(":")[1])
            return int(value)
        return value

    async def get_source(self) -> "Source":
        try:
            # Use unified repo function (works for both backends)
            src_data = await repo_get_source_for_insight(self.id)
            if src_data:
                src_data = dict(src_data)
                # Handle flattened asset fields (SQLite) vs nested (SurrealDB)
                if "file_path" in src_data or "url" in src_data:
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
    command: Optional[Any] = Field(
        default=None, description="Link to command processing job"
    )

    @field_validator("command", mode="before")
    @classmethod
    def parse_command(cls, value):
        """Parse command field - RecordID for SurrealDB, string for SQLite"""
        if value is None:
            return None
        if is_sqlite():
            return str(value)
        else:
            # SurrealDB: ensure RecordID format
            if isinstance(value, str) and value:
                return ensure_record_id(value)
            return value

    async def get_status(self) -> Optional[str]:
        """Get the processing status of the associated command"""
        if not self.command:
            return None

        try:
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
            # Use unified repo method for counting related embeddings
            full_source_id = self.id if ":" in self.id else f"source:{self.id}"
            return await repo_count_source_embeddings(full_source_id)
        except Exception as e:
            logger.error(f"Error fetching chunks count for source {self.id}: {str(e)}")
            logger.exception(e)
            raise DatabaseOperationError(f"Failed to count chunks for source: {str(e)}")

    async def get_insights(self) -> List[SourceInsight]:
        try:
            full_source_id = self.id if ":" in self.id else f"source:{self.id}"
            # Use unified repo function (works for both backends)
            result = await repo_get_insights_for_source(full_source_id)
            return [SourceInsight(**insight) for insight in result]
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

        For SQLite backend, embedding is done directly (synchronously) via the executor.
        For SurrealDB backend, embedding is submitted to the command queue.

        Returns:
            str: The command/job ID that can be used to track progress via the commands API
                 (or "direct" for SQLite synchronous embedding)

        Raises:
            ValueError: If source has no text to vectorize
            DatabaseOperationError: If job submission fails
        """
        logger.info(f"Submitting embed_source job for source {self.id}")

        try:
            if not self.full_text:
                raise ValueError(f"Source {self.id} has no text to vectorize")

            # Use executor strategy pattern (handles SQLite vs SurrealDB)
            return await executor.embed_source(self.id)

        except Exception as e:
            logger.error(
                f"Failed to submit embed_source job for source {self.id}: {e}"
            )
            logger.exception(e)
            raise DatabaseOperationError(e)

    async def add_insight(self, insight_type: str, content: str) -> Any:
        """
        Add an insight to this source.

        Creates the insight record, then generates the embedding.
        For SQLite, embedding is done directly (synchronously) via the executor.
        For SurrealDB, embedding is submitted as an async command via the executor.

        Args:
            insight_type: Type/category of the insight
            content: The insight content text

        Returns:
            The created insight record(s)
        """
        if not insight_type or not content:
            raise InvalidInputError("Insight type and content must be provided")
        try:
            # Generate embedding for SQLite (direct executor returns the embedding data)
            embedding_data = await executor.embed_insight_content(content)

            if is_sqlite():
                source_id = (
                    int(self.id.split(":")[1]) if ":" in self.id else int(self.id)
                )

                # Create insight WITH embedding for SQLite
                result = await repo_query(
                    """
                    INSERT INTO source_insight (source_id, insight_type, content, embedding, created)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    RETURNING *
                    """,
                    (source_id, insight_type, content, embedding_data),
                )

                logger.debug(f"Created insight for source {self.id} with direct embedding")
            else:
                # SurrealDB: Create insight WITHOUT embedding
                result = await repo_query(
                    """
                    CREATE source_insight CONTENT {
                            "source": $source_id,
                            "insight_type": $insight_type,
                            "content": $content,
                    };""",
                    {
                        "source_id": ensure_record_id(self.id),
                        "insight_type": insight_type,
                        "content": content,
                    },
                )

                # Submit embedding command via executor (fire-and-forget)
                if result and len(result) > 0:
                    insight_id = str(result[0].get("id", ""))
                    if insight_id:
                        await executor.embed_insight(insight_id)
                        logger.debug(f"Submitted embed_insight for {insight_id}")

            return result
        except Exception as e:
            logger.error(f"Error adding insight to source {self.id}: {str(e)}")
            raise

    def _prepare_save_data(self) -> dict:
        """Override to handle asset and command field per backend"""
        data = super()._prepare_save_data()

        if is_sqlite():
            # Flatten asset into file_path and url for SQLite storage
            asset = data.pop("asset", None)
            if asset:
                if isinstance(asset, dict):
                    data["file_path"] = asset.get("file_path")
                    data["url"] = asset.get("url")
                elif isinstance(asset, Asset):
                    data["file_path"] = asset.file_path
                    data["url"] = asset.url

            # Ensure command is stored as string (command_id column)
            if data.get("command") is not None:
                data["command_id"] = str(data.pop("command"))
            else:
                data.pop("command", None)
        else:
            # SurrealDB: Ensure command field is RecordID format if not None
            if data.get("command") is not None:
                data["command"] = ensure_record_id(data["command"])

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
        try:
            full_source_id = self.id if ":" in self.id else f"source:{self.id}"
            await repo_delete_embeddings(full_source_id)

            # Delete insights
            _, id_value = parse_id(full_source_id)
            if is_sqlite():
                await repo_query(
                    "DELETE FROM source_insight WHERE source_id = ?",
                    (id_value,),
                )
            else:
                await repo_query(
                    "DELETE source_insight WHERE source = $source_id",
                    {"source_id": ensure_record_id(self.id)},
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
        Save the note and generate embedding.

        Overrides ObjectModel.save() to generate embedding after saving.
        For SQLite, embedding is done directly (synchronously) via the executor.
        For SurrealDB, embedding is submitted as an async command via the executor.

        Returns:
            Optional[str]: The command_id if embedding was submitted (SurrealDB),
                          "direct" if embedded directly (SQLite), or None if no content
        """
        # Call parent save (without embedding)
        await super().save()

        # Generate embedding if note has content (using executor strategy)
        if self.id and self.content and self.content.strip():
            try:
                return await executor.embed_note(self.id)
            except Exception as e:
                logger.warning(f"Failed to embed note {self.id}: {e}")
                return None

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
        if is_sqlite():
            from open_notebook.database.sqlite_search import (
                text_search as sqlite_text_search,
            )

            search_results = await sqlite_text_search(
                query_text=keyword,
                match_count=results,
                search_sources=source,
                search_notes=note,
            )
        else:
            # SurrealDB: Use built-in text search function
            search_results = await repo_query(
                """
                select *
                from fn::text_search($keyword, $results, $source, $note)
                """,
                {
                    "keyword": keyword,
                    "results": results,
                    "source": source,
                    "note": note,
                },
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
        from open_notebook.utils.embedding import generate_embedding

        # Use unified embedding function (handles chunking if query is very long)
        embed = await generate_embedding(keyword)

        if is_sqlite():
            from open_notebook.database.sqlite_search import (
                vector_search as sqlite_vector_search,
            )

            search_results = await sqlite_vector_search(
                query_embedding=embed,
                match_count=results,
                search_sources=source,
                search_notes=note,
                min_similarity=minimum_score,
            )
        else:
            # SurrealDB: Use built-in vector search function
            search_results = await repo_query(
                """
                SELECT * FROM fn::vector_search($embed, $results, $source, $note, $minimum_score);
                """,
                {
                    "embed": embed,
                    "results": results,
                    "source": source,
                    "note": note,
                    "minimum_score": minimum_score,
                },
            )
        return search_results
    except Exception as e:
        logger.error(f"Error performing vector search: {str(e)}")
        logger.exception(e)
        raise DatabaseOperationError(e)
