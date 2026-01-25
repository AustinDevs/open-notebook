"""
Database backend abstraction layer.

This module provides a unified interface to the database, supporting both
SurrealDB (default) and SQLite backends. The backend is selected via the
DATABASE_BACKEND environment variable:

- DATABASE_BACKEND=surrealdb (default): Uses SurrealDB
- DATABASE_BACKEND=sqlite: Uses SQLite

Usage:
    from open_notebook.database import repo_query, repo_create, ...

All database operations are routed through this module to enable
seamless switching between backends.
"""

import os
from typing import TYPE_CHECKING

# Determine which backend to use
DATABASE_BACKEND = os.getenv("DATABASE_BACKEND", "surrealdb").lower()

if DATABASE_BACKEND == "sqlite":
    # SQLite backend
    from open_notebook.database.sqlite_repository import (
        db_connection,
        deserialize_embedding,
        ensure_record_id,
        format_id,
        format_ids,
        normalize_result,
        parse_id,
        parse_record_ids,
        repo_add_relation,
        repo_check_relation,
        repo_count_related,
        repo_create,
        repo_delete,
        repo_delete_embeddings,
        repo_get,
        repo_get_embeddings,
        repo_get_insights_for_source,
        repo_get_notebooks_for_source,
        repo_get_related,
        repo_get_sessions_for_source,
        repo_get_source_for_embedding,
        repo_get_source_for_insight,
        repo_get_sources_for_notebook,
        repo_insert,
        repo_list,
        repo_list_with_counts,
        repo_query,
        repo_relate,
        repo_remove_relation,
        repo_count_source_embeddings,
        repo_get_notebook_for_session,
        repo_singleton_get,
        repo_singleton_upsert,
        repo_update,
        repo_update_embedding,
        repo_upsert,
        serialize_embedding,
    )
    from open_notebook.database.sqlite_search import (
        text_search as db_text_search,
    )
    from open_notebook.database.sqlite_search import (
        vector_search as db_vector_search,
    )

    BACKEND_NAME = "sqlite"

else:
    # SurrealDB backend (default)
    from open_notebook.database.repository import (
        db_connection,
        ensure_record_id,
        parse_record_ids,
        repo_create,
        repo_delete,
        repo_insert,
        repo_query,
        repo_relate,
        repo_update,
        repo_upsert,
    )

    # SurrealDB doesn't have separate search module - searches use repo_query
    # with fn::text_search and fn::vector_search
    db_text_search = None
    db_vector_search = None

    # SurrealDB doesn't need embedding serialization
    def serialize_embedding(embedding):
        """Pass-through for SurrealDB (stores arrays natively)."""
        return embedding

    def deserialize_embedding(data):
        """Pass-through for SurrealDB (stores arrays natively)."""
        return data

    BACKEND_NAME = "surrealdb"

    # =============================================================================
    # SURREALDB WRAPPER FUNCTIONS
    # These provide the same interface as SQLite high-level functions
    # =============================================================================

    from typing import Any, Dict, List, Optional, Tuple

    def parse_id(record_id: str) -> Tuple[str, int]:
        """Parse 'table:id' into (table, id). For SurrealDB, id is kept as string."""
        if ":" in record_id:
            parts = record_id.split(":", 1)
            # SurrealDB IDs might not be numeric, but we return int for compatibility
            try:
                return parts[0], int(parts[1])
            except ValueError:
                return parts[0], 0  # Non-numeric ID
        return "", 0

    def format_id(table: str, numeric_id: int) -> str:
        """Format as 'table:id'."""
        return f"{table}:{numeric_id}"

    def normalize_result(row: Dict[str, Any], table: str) -> Dict[str, Any]:
        """SurrealDB already returns proper IDs, just return as-is."""
        return dict(row)

    async def repo_get(table: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Get single record by ID."""
        result = await repo_query(
            "SELECT * FROM $id", {"id": ensure_record_id(record_id)}
        )
        return result[0] if result else None

    async def repo_list(
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """List records with optional filtering/ordering."""
        query = f"SELECT * FROM {table}"

        if filters:
            conditions = " AND ".join([f"{k} = ${k}" for k in filters.keys()])
            query += f" WHERE {conditions}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit is not None:
            query += f" LIMIT {limit}"

        if offset is not None:
            query += f" START {offset}"

        return await repo_query(query, filters or {})

    async def repo_get_related(
        source_table: str,
        source_id: str,
        relation: str,
        target_table: str,
    ) -> List[Dict[str, Any]]:
        """Get related records via graph edges."""
        if relation == "reference":
            if source_table == "notebook":
                # Get sources for a notebook
                query = """
                    SELECT in.* FROM reference WHERE out = $id
                """
            else:
                # Get notebooks for a source
                query = """
                    SELECT out.* FROM reference WHERE in = $id
                """
        elif relation == "artifact":
            if source_table == "notebook":
                query = """
                    SELECT in.* FROM artifact WHERE out = $id
                """
            else:
                query = """
                    SELECT out.* FROM artifact WHERE in = $id
                """
        elif relation == "refers_to":
            if source_table == "chat_session":
                query = """
                    SELECT out.* FROM refers_to WHERE in = $id
                """
            else:
                query = """
                    SELECT in.* FROM refers_to WHERE out = $id
                """
        else:
            raise ValueError(f"Unknown relationship: {relation}")

        return await repo_query(query, {"id": ensure_record_id(source_id)})

    async def repo_count_related(
        source_table: str,
        source_id: str,
        relation: str,
    ) -> int:
        """Count related records."""
        if relation == "reference":
            col = "out" if source_table == "notebook" else "in"
            query = f"SELECT count() as count FROM reference WHERE {col} = $id GROUP ALL"
        elif relation == "artifact":
            col = "out" if source_table == "notebook" else "in"
            query = f"SELECT count() as count FROM artifact WHERE {col} = $id GROUP ALL"
        elif relation == "refers_to":
            col = "in" if source_table == "chat_session" else "out"
            query = f"SELECT count() as count FROM refers_to WHERE {col} = $id GROUP ALL"
        else:
            raise ValueError(f"Unknown relationship: {relation}")

        result = await repo_query(query, {"id": ensure_record_id(source_id)})
        return result[0]["count"] if result else 0

    async def repo_add_relation(
        source_id: str,
        relation: str,
        target_id: str,
    ) -> bool:
        """Add relationship via RELATE."""
        await repo_query(
            f"RELATE $source->{relation}->$target",
            {
                "source": ensure_record_id(source_id),
                "target": ensure_record_id(target_id),
            },
        )
        return True

    async def repo_remove_relation(
        source_id: str,
        relation: str,
        target_id: str,
    ) -> bool:
        """Remove relationship."""
        await repo_query(
            f"DELETE FROM {relation} WHERE in = $source AND out = $target",
            {
                "source": ensure_record_id(source_id),
                "target": ensure_record_id(target_id),
            },
        )
        return True

    async def repo_check_relation(
        source_id: str,
        relation: str,
        target_id: str,
    ) -> bool:
        """Check if relationship exists."""
        result = await repo_query(
            f"SELECT * FROM {relation} WHERE in = $source AND out = $target",
            {
                "source": ensure_record_id(source_id),
                "target": ensure_record_id(target_id),
            },
        )
        return len(result) > 0

    async def repo_list_with_counts(
        table: str,
        count_relations: Dict[str, Tuple[str, str]],
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List records with relationship counts using SurrealDB graph queries."""
        # Build count expressions
        count_exprs = []
        for alias, (relation, _) in count_relations.items():
            if relation == "source_notebook":
                count_exprs.append(f"count(<-reference.in) as {alias}")
            elif relation == "note_notebook":
                count_exprs.append(f"count(<-artifact.in) as {alias}")
            else:
                count_exprs.append(f"0 as {alias}")

        query = f"SELECT *, {', '.join(count_exprs)} FROM {table}"

        if filters:
            conditions = " AND ".join([f"{k} = ${k}" for k in filters.keys()])
            query += f" WHERE {conditions}"

        if order_by:
            query += f" ORDER BY {order_by}"

        return await repo_query(query, filters or {})

    async def repo_get_embeddings(
        source_id: str,
        include_content: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get embeddings for a source."""
        if include_content:
            query = "SELECT * FROM source_embedding WHERE source = $id ORDER BY chunk_order"
        else:
            query = "SELECT id, source, chunk_order FROM source_embedding WHERE source = $id ORDER BY chunk_order"

        return await repo_query(query, {"id": ensure_record_id(source_id)})

    async def repo_update_embedding(
        table: str,
        record_id: str,
        embedding: List[float],
    ) -> bool:
        """Update embedding field."""
        await repo_query(
            f"UPDATE $id SET embedding = $embedding",
            {
                "id": ensure_record_id(record_id),
                "embedding": embedding,
            },
        )
        return True

    async def repo_delete_embeddings(source_id: str) -> int:
        """Delete all embeddings for a source."""
        result = await repo_query(
            "SELECT count() as count FROM source_embedding WHERE source = $id GROUP ALL",
            {"id": ensure_record_id(source_id)},
        )
        count = result[0]["count"] if result else 0

        await repo_query(
            "DELETE source_embedding WHERE source = $id",
            {"id": ensure_record_id(source_id)},
        )

        return count

    async def repo_get_notebooks_for_source(source_id: str) -> List[str]:
        """Get notebook IDs for a source."""
        result = await repo_query(
            "SELECT VALUE out FROM reference WHERE in = $id",
            {"id": ensure_record_id(source_id)},
        )
        return [str(nb_id) for nb_id in result] if result else []

    async def repo_get_sources_for_notebook(
        notebook_id: str,
        order_by: Optional[str] = "updated DESC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get sources for a notebook."""
        query = """
            SELECT * FROM (
                SELECT in.* FROM reference WHERE out = $id
            )
        """

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit is not None:
            query += f" LIMIT {limit}"

        if offset is not None:
            query += f" START {offset}"

        return await repo_query(query, {"id": ensure_record_id(notebook_id)})

    async def repo_get_sessions_for_source(source_id: str) -> List[Dict[str, Any]]:
        """Get chat sessions for a source."""
        return await repo_query(
            """
            SELECT in.* FROM refers_to WHERE out = $id ORDER BY in.updated DESC
            """,
            {"id": ensure_record_id(source_id)},
        )

    async def repo_singleton_get(record_id: str) -> Optional[Dict[str, Any]]:
        """Get a singleton record by its record_id."""
        result = await repo_query(
            "SELECT * FROM ONLY $record_id",
            {"record_id": ensure_record_id(record_id)},
        )
        if result:
            if isinstance(result, list) and len(result) > 0:
                return result[0] if isinstance(result[0], dict) else None
            elif isinstance(result, dict):
                return result
        return None

    async def repo_singleton_upsert(
        record_id: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upsert a singleton record."""
        # For SurrealDB, we use the record_id directly
        parts = record_id.split(":")
        if len(parts) == 2:
            table_name = parts[1]
        else:
            table_name = "record"

        await repo_upsert(table_name, record_id, data)

        result = await repo_query(
            "SELECT * FROM $record_id",
            {"record_id": ensure_record_id(record_id)},
        )
        return result[0] if result else data

    async def repo_get_insights_for_source(source_id: str) -> List[Dict[str, Any]]:
        """Get all insights for a source."""
        return await repo_query(
            "SELECT * FROM source_insight WHERE source = $id",
            {"id": ensure_record_id(source_id)},
        )

    async def repo_get_source_for_embedding(embedding_id: str) -> Optional[Dict[str, Any]]:
        """Get the source record for an embedding."""
        result = await repo_query(
            "SELECT source.* FROM $id FETCH source",
            {"id": ensure_record_id(embedding_id)},
        )
        if result and "source" in result[0]:
            return result[0]["source"]
        return None

    async def repo_get_source_for_insight(insight_id: str) -> Optional[Dict[str, Any]]:
        """Get the source record for an insight."""
        result = await repo_query(
            "SELECT source.* FROM $id FETCH source",
            {"id": ensure_record_id(insight_id)},
        )
        if result and "source" in result[0]:
            return result[0]["source"]
        return None

    def format_ids(table: str, rows: List[Dict[str, Any]]) -> List[str]:
        """Format a list of rows into 'table:id' strings (SurrealDB already has proper IDs)."""
        return [str(row["id"]) for row in rows]

    async def repo_count_source_embeddings(source_id: str) -> int:
        """Count the number of embeddings for a source."""
        result = await repo_query(
            "SELECT count() as chunks FROM source_embedding WHERE source = $id GROUP ALL",
            {"id": ensure_record_id(source_id)},
        )
        if len(result) == 0:
            return 0
        return result[0]["chunks"]

    async def repo_get_notebook_for_session(session_id: str) -> Optional[str]:
        """Get the notebook ID for a chat session."""
        result = await repo_query(
            "SELECT out FROM refers_to WHERE in = $session_id",
            {"session_id": ensure_record_id(session_id)},
        )
        return str(result[0]["out"]) if result else None


def get_backend_name() -> str:
    """Return the name of the current database backend."""
    return BACKEND_NAME


def is_sqlite() -> bool:
    """Check if using SQLite backend."""
    return BACKEND_NAME == "sqlite"


def is_surrealdb() -> bool:
    """Check if using SurrealDB backend."""
    return BACKEND_NAME == "surrealdb"


# Command queue exports (work with both backends)
from open_notebook.database.command_queue import (
    CommandInput,
    CommandOutput,
    CommandStatus,
    command,
    get_command_status,
    submit_command,
)

# Command executor (strategy pattern for embedding operations)
from open_notebook.database.executor import executor


# Export all functions
__all__ = [
    # Core repository functions
    "db_connection",
    "repo_query",
    "repo_create",
    "repo_update",
    "repo_upsert",
    "repo_delete",
    "repo_insert",
    "repo_relate",
    "ensure_record_id",
    "parse_record_ids",
    # High-level abstraction functions
    "parse_id",
    "format_id",
    "format_ids",
    "normalize_result",
    "repo_get",
    "repo_list",
    "repo_get_related",
    "repo_count_related",
    "repo_add_relation",
    "repo_remove_relation",
    "repo_check_relation",
    "repo_list_with_counts",
    "repo_get_embeddings",
    "repo_update_embedding",
    "repo_delete_embeddings",
    "repo_get_notebooks_for_source",
    "repo_get_sources_for_notebook",
    "repo_get_sessions_for_source",
    # Singleton record functions
    "repo_singleton_get",
    "repo_singleton_upsert",
    # Source relationship functions
    "repo_get_insights_for_source",
    "repo_get_source_for_embedding",
    "repo_get_source_for_insight",
    "repo_count_source_embeddings",
    "repo_get_notebook_for_session",
    # Embedding helpers
    "serialize_embedding",
    "deserialize_embedding",
    # Search functions (SQLite only, None for SurrealDB)
    "db_text_search",
    "db_vector_search",
    # Backend info
    "get_backend_name",
    "is_sqlite",
    "is_surrealdb",
    "BACKEND_NAME",
    # Command queue
    "submit_command",
    "get_command_status",
    "command",
    "CommandInput",
    "CommandOutput",
    "CommandStatus",
    # Command executor
    "executor",
]
