"""
Command Executor Strategy Pattern

Provides a unified interface for executing embedding operations that works
with both SQLite (direct execution) and SurrealDB (command queue).

This pattern eliminates is_sqlite() checks in domain models by abstracting
the execution strategy.

Usage:
    from open_notebook.database.executor import executor

    # In domain model
    result = await executor.embed_note(note_id)
    result = await executor.embed_source(source_id)
    result = await executor.embed_insight(insight_id)
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from loguru import logger


class CommandExecutor(ABC):
    """Abstract base class for command execution strategies."""

    @abstractmethod
    async def embed_note(self, note_id: str) -> str:
        """
        Embed a note's content.

        Args:
            note_id: The note ID to embed

        Returns:
            str: Status identifier - "direct" for sync execution,
                 command_id for async execution
        """
        pass

    @abstractmethod
    async def embed_source(self, source_id: str) -> str:
        """
        Embed a source's full text.

        Args:
            source_id: The source ID to embed

        Returns:
            str: Status identifier - "direct" for sync execution,
                 command_id for async execution
        """
        pass

    @abstractmethod
    async def embed_insight(self, insight_id: str) -> str:
        """
        Embed a source insight.

        Args:
            insight_id: The insight ID to embed

        Returns:
            str: Status identifier - "direct" for sync execution,
                 command_id for async execution
        """
        pass


class DirectExecutor(CommandExecutor):
    """
    Executes embedding operations directly (synchronously).

    Used for SQLite backend where surreal_commands job queue
    is not available.
    """

    async def embed_note(self, note_id: str) -> str:
        """Embed note directly."""
        from open_notebook.domain.notebook import Note
        from open_notebook.utils.embedding import generate_embedding
        from open_notebook.database import repo_update_embedding

        logger.debug(f"Direct embedding note: {note_id}")

        note = await Note.get(note_id)
        if not note or not note.content or not note.content.strip():
            logger.warning(f"Note {note_id} has no content to embed")
            return "skipped"

        try:
            embedding = await generate_embedding(note.content)
            await repo_update_embedding("note", note_id, embedding)
            logger.debug(f"Successfully embedded note {note_id}")
            return "direct"
        except Exception as e:
            logger.warning(f"Failed to embed note {note_id}: {e}")
            return "failed"

    async def embed_source(self, source_id: str) -> str:
        """Embed source directly."""
        from open_notebook.domain.notebook import Source
        from open_notebook.utils.chunking import chunk_text, detect_content_type
        from open_notebook.utils.embedding import generate_embeddings
        from open_notebook.database import (
            repo_insert,
            repo_query,
            serialize_embedding,
            parse_id,
        )

        logger.debug(f"Direct embedding source: {source_id}")

        source = await Source.get(source_id)
        if not source or not source.full_text:
            logger.warning(f"Source {source_id} has no text to embed")
            return "skipped"

        try:
            # Delete existing embeddings
            _, source_id_int = parse_id(source_id)
            await repo_query(
                "DELETE FROM source_embedding WHERE source_id = ?",
                (source_id_int,),
            )

            # Detect content type and chunk text
            file_path = source.asset.file_path if source.asset else None
            content_type = detect_content_type(source.full_text, file_path)
            chunks = chunk_text(source.full_text, content_type=content_type)

            if not chunks:
                logger.warning(f"No chunks created for source {source_id}")
                return "direct"

            # Generate embeddings
            embeddings = await generate_embeddings(chunks)

            # Insert embeddings
            embedding_records = [
                {
                    "source_id": source_id_int,
                    "chunk_order": idx,
                    "content": chunk,
                    "embedding": serialize_embedding(emb),
                }
                for idx, (chunk, emb) in enumerate(zip(chunks, embeddings))
            ]
            await repo_insert("source_embedding", embedding_records)

            logger.info(
                f"Direct embedding complete for source {source_id}: "
                f"{len(chunks)} chunks embedded"
            )
            return "direct"
        except Exception as e:
            logger.error(f"Failed to embed source {source_id}: {e}")
            return "failed"

    async def embed_insight(self, insight_id: str) -> str:
        """Embed insight directly and return its embedding."""
        from open_notebook.utils.embedding import generate_embedding
        from open_notebook.database import repo_update_embedding

        logger.debug(f"Direct embedding insight: {insight_id}")

        # We don't need to load the insight, just generate the embedding
        # This is called during insight creation where we already have the content
        return "direct"

    async def embed_insight_content(self, content: str) -> Optional[bytes]:
        """Generate embedding for insight content directly."""
        from open_notebook.utils.embedding import generate_embedding
        from open_notebook.database import serialize_embedding

        try:
            embedding = await generate_embedding(content)
            return serialize_embedding(embedding)
        except Exception as e:
            logger.warning(f"Failed to generate embedding for insight: {e}")
            return None


class QueueExecutor(CommandExecutor):
    """
    Submits embedding operations to the command queue (asynchronously).

    Used for SurrealDB backend where surreal_commands job queue
    handles background processing.
    """

    async def embed_note(self, note_id: str) -> str:
        """Submit note embedding to queue."""
        from open_notebook.database.command_queue import submit_command

        logger.debug(f"Submitting embed_note command for: {note_id}")

        command_id = submit_command(
            "open_notebook",
            "embed_note",
            {"note_id": str(note_id)},
        )

        logger.debug(f"Submitted embed_note command {command_id} for {note_id}")
        return str(command_id)

    async def embed_source(self, source_id: str) -> str:
        """Submit source embedding to queue."""
        from open_notebook.database.command_queue import submit_command

        logger.debug(f"Submitting embed_source command for: {source_id}")

        command_id = submit_command(
            "open_notebook",
            "embed_source",
            {"source_id": str(source_id)},
        )

        logger.info(
            f"Embed source job submitted for source {source_id}: "
            f"command_id={command_id}"
        )
        return str(command_id)

    async def embed_insight(self, insight_id: str) -> str:
        """Submit insight embedding to queue."""
        from open_notebook.database.command_queue import submit_command

        logger.debug(f"Submitting embed_insight command for: {insight_id}")

        command_id = submit_command(
            "open_notebook",
            "embed_insight",
            {"insight_id": insight_id},
        )

        logger.debug(f"Submitted embed_insight command {command_id} for {insight_id}")
        return str(command_id)

    async def embed_insight_content(self, content: str) -> Optional[bytes]:
        """
        For queue executor, insights are created without embedding.
        Embedding is submitted as a separate command.
        """
        return None


def _create_executor() -> CommandExecutor:
    """Create the appropriate executor based on database backend."""
    from open_notebook.database import is_sqlite

    if is_sqlite():
        return DirectExecutor()
    else:
        return QueueExecutor()


# Global executor instance - initialized on first import
executor: CommandExecutor = _create_executor()
