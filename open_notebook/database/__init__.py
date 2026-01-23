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
        parse_record_ids,
        repo_create,
        repo_delete,
        repo_insert,
        repo_query,
        repo_relate,
        repo_update,
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


def get_backend_name() -> str:
    """Return the name of the current database backend."""
    return BACKEND_NAME


def is_sqlite() -> bool:
    """Check if using SQLite backend."""
    return BACKEND_NAME == "sqlite"


def is_surrealdb() -> bool:
    """Check if using SurrealDB backend."""
    return BACKEND_NAME == "surrealdb"


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
]
