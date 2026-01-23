"""
SQLite repository module for Open Notebook.
Replaces SurrealDB with SQLite + sqlite-vec for vector search.
"""

import json
import os
import struct
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import aiosqlite
from loguru import logger

T = TypeVar("T", Dict[str, Any], List[Dict[str, Any]])

# Database path configuration
DEFAULT_DB_PATH = "/data/sqlite-db/open_notebook.db"


def get_database_path() -> str:
    """Get database path from environment or use default."""
    return os.getenv("SQLITE_DB_PATH", DEFAULT_DB_PATH)


def serialize_embedding(embedding: Optional[List[float]]) -> Optional[bytes]:
    """Serialize embedding list to bytes for SQLite storage."""
    if embedding is None:
        return None
    return struct.pack(f"{len(embedding)}f", *embedding)


def deserialize_embedding(data: Optional[bytes]) -> Optional[List[float]]:
    """Deserialize embedding from bytes."""
    if data is None:
        return None
    count = len(data) // 4
    return list(struct.unpack(f"{count}f", data))


def _row_to_dict(row: aiosqlite.Row) -> Dict[str, Any]:
    """Convert a sqlite Row to a dictionary, handling special types."""
    result = dict(row)

    # Parse JSON fields
    for key, value in result.items():
        if isinstance(value, str):
            # Try to parse as JSON for array/object fields
            if key in ("topics", "speakers", "youtube_preferred_languages"):
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    pass
        # Deserialize embedding fields
        elif isinstance(value, bytes) and key == "embedding":
            result[key] = deserialize_embedding(value)

    return result


def _prepare_value(key: str, value: Any) -> Any:
    """Prepare a value for SQLite storage."""
    if value is None:
        return None

    # Serialize lists/dicts to JSON
    if isinstance(value, (list, dict)):
        # Special handling for embeddings
        if key == "embedding":
            return serialize_embedding(value)
        return json.dumps(value)

    # Handle datetime
    if isinstance(value, datetime):
        return value.isoformat()

    return value


def _prepare_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare data dictionary for SQLite storage."""
    return {key: _prepare_value(key, value) for key, value in data.items()}


@asynccontextmanager
async def db_connection():
    """
    Async context manager for database connections.

    Configures:
    - Foreign key enforcement
    - WAL mode for better concurrency
    - Row factory for dict-like access
    """
    db_path = get_database_path()

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row

    try:
        # Enable foreign keys and WAL mode
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("PRAGMA journal_mode = WAL")

        # Try to load sqlite-vec extension if available
        try:
            await db.enable_load_extension(True)
            import sqlite_vec

            await db.load_extension(sqlite_vec.loadable_path())
        except Exception as e:
            logger.debug(f"sqlite-vec extension not loaded: {e}")

        yield db
    finally:
        await db.close()


async def repo_query(
    query_str: str, params: Optional[Union[Dict[str, Any], Tuple]] = None
) -> List[Dict[str, Any]]:
    """
    Execute a SQL query and return the results as a list of dicts.

    Args:
        query_str: SQL query string with ? placeholders or :named params
        params: Query parameters (tuple for ?, dict for :named)

    Returns:
        List of result rows as dictionaries
    """
    async with db_connection() as db:
        try:
            if params is None:
                params = ()

            # Handle dict params by converting to named parameter format
            if isinstance(params, dict):
                # aiosqlite supports :name syntax with dict
                cursor = await db.execute(query_str, params)
            else:
                cursor = await db.execute(query_str, params)

            rows = await cursor.fetchall()
            return [_row_to_dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.debug(f"Query: {query_str}")
            raise


async def repo_create(table: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new record in the specified table.

    Args:
        table: Table name
        data: Record data (id field will be removed if present)

    Returns:
        The created record with generated ID
    """
    data = dict(data)  # Copy to avoid mutation
    data.pop("id", None)

    # Add timestamps
    now = datetime.now(timezone.utc).isoformat()
    data["created"] = now
    data["updated"] = now

    prepared = _prepare_data(data)

    columns = ", ".join(prepared.keys())
    placeholders = ", ".join(["?" for _ in prepared])

    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    async with db_connection() as db:
        try:
            cursor = await db.execute(query, tuple(prepared.values()))
            await db.commit()
            row_id = cursor.lastrowid

            # Fetch the created record
            result = await db.execute(f"SELECT * FROM {table} WHERE id = ?", (row_id,))
            row = await result.fetchone()

            if row:
                return _row_to_dict(row)
            else:
                return {"id": row_id, **data}
        except Exception as e:
            logger.error(f"Failed to create record in {table}: {e}")
            raise RuntimeError(f"Failed to create record: {str(e)}")


async def repo_update(
    table: str, id: Union[str, int], data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Update an existing record by ID.

    Args:
        table: Table name
        id: Record ID (can be 'table:id' format or just id)

    Returns:
        List containing the updated record
    """
    # Handle SurrealDB-style IDs (table:id)
    if isinstance(id, str) and ":" in id:
        id = id.split(":")[1]

    data = dict(data)
    data.pop("id", None)
    data["updated"] = datetime.now(timezone.utc).isoformat()

    prepared = _prepare_data(data)

    set_clause = ", ".join([f"{k} = ?" for k in prepared.keys()])
    query = f"UPDATE {table} SET {set_clause} WHERE id = ?"

    async with db_connection() as db:
        try:
            await db.execute(query, (*prepared.values(), id))
            await db.commit()

            # Fetch the updated record
            result = await db.execute(f"SELECT * FROM {table} WHERE id = ?", (id,))
            row = await result.fetchone()

            if row:
                return [_row_to_dict(row)]
            else:
                raise RuntimeError(f"Record not found after update: {table}:{id}")
        except Exception as e:
            logger.error(f"Failed to update {table}:{id}: {e}")
            raise RuntimeError(f"Failed to update record: {str(e)}")


async def repo_upsert(
    table: str,
    id: Optional[str],
    data: Dict[str, Any],
    add_timestamp: bool = False,
) -> List[Dict[str, Any]]:
    """
    Create or update a record (upsert operation).

    For SQLite, we use INSERT OR REPLACE.
    """
    data = dict(data)
    data.pop("id", None)

    if add_timestamp:
        data["updated"] = datetime.now(timezone.utc).isoformat()

    # Handle record_id format (e.g., 'open_notebook:content_settings')
    if id and ":" in id:
        record_id = id
        # For singleton tables, we use a fixed ID
        table_id = 1
    else:
        record_id = id
        table_id = int(id) if id else None

    prepared = _prepare_data(data)

    if table_id:
        prepared["id"] = table_id

    columns = ", ".join(prepared.keys())
    placeholders = ", ".join(["?" for _ in prepared])

    query = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"

    async with db_connection() as db:
        try:
            await db.execute(query, tuple(prepared.values()))
            await db.commit()

            # Fetch the record
            fetch_id = table_id or 1
            result = await db.execute(f"SELECT * FROM {table} WHERE id = ?", (fetch_id,))
            row = await result.fetchone()

            if row:
                return [_row_to_dict(row)]
            else:
                return [{"id": fetch_id, **data}]
        except Exception as e:
            logger.error(f"Failed to upsert {table}: {e}")
            raise


async def repo_delete(record_id: Union[str, int]) -> bool:
    """
    Delete a record by record ID.

    Args:
        record_id: Can be 'table:id' format or just id
    """
    # Parse table:id format
    if isinstance(record_id, str) and ":" in record_id:
        table, id_part = record_id.split(":", 1)
        record_id_int = int(id_part)
    else:
        raise ValueError(f"Invalid record_id format: {record_id}. Expected 'table:id'")

    async with db_connection() as db:
        try:
            await db.execute(f"DELETE FROM {table} WHERE id = ?", (record_id_int,))
            await db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to delete {record_id}: {e}")
            raise RuntimeError(f"Failed to delete record: {str(e)}")


async def repo_relate(
    source: str,
    relationship: str,
    target: str,
    data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Create a relationship between two records.

    Maps SurrealDB relationships to SQLite join tables:
    - reference: source -> notebook (source_notebook table)
    - artifact: note -> notebook (note_notebook table)
    - refers_to: chat_session -> notebook/source (chat_session_reference table)
    """
    # Parse IDs
    source_table, source_id = source.split(":", 1) if ":" in source else (None, source)
    target_table, target_id = target.split(":", 1) if ":" in target else (None, target)

    # Map relationship to join table
    relation_tables = {
        "reference": ("source_notebook", "source_id", "notebook_id"),
        "artifact": ("note_notebook", "note_id", "notebook_id"),
        "refers_to": (
            "chat_session_reference",
            "chat_session_id",
            f"{target_table}_id" if target_table else "notebook_id",
        ),
    }

    if relationship not in relation_tables:
        raise ValueError(f"Unknown relationship: {relationship}")

    table_name, source_col, target_col = relation_tables[relationship]

    query = f"""
        INSERT OR IGNORE INTO {table_name} ({source_col}, {target_col})
        VALUES (?, ?)
    """

    async with db_connection() as db:
        try:
            await db.execute(query, (int(source_id), int(target_id)))
            await db.commit()
            return [{"source": source, "relationship": relationship, "target": target}]
        except Exception as e:
            logger.error(f"Failed to create relationship: {e}")
            raise


async def repo_insert(
    table: str,
    data: List[Dict[str, Any]],
    ignore_duplicates: bool = False,
) -> List[Dict[str, Any]]:
    """
    Bulk insert records into a table.

    Args:
        table: Table name
        data: List of records to insert
        ignore_duplicates: If True, silently ignore duplicate key errors
    """
    if not data:
        return []

    results = []
    insert_type = "INSERT OR IGNORE" if ignore_duplicates else "INSERT"

    async with db_connection() as db:
        try:
            for record in data:
                record = dict(record)
                record.pop("id", None)

                prepared = _prepare_data(record)
                columns = ", ".join(prepared.keys())
                placeholders = ", ".join(["?" for _ in prepared])

                query = f"{insert_type} INTO {table} ({columns}) VALUES ({placeholders})"
                cursor = await db.execute(query, tuple(prepared.values()))

                if cursor.lastrowid:
                    results.append({"id": cursor.lastrowid, **record})

            await db.commit()
            return results
        except Exception as e:
            if ignore_duplicates and "UNIQUE constraint" in str(e):
                return []
            logger.error(f"Failed to insert into {table}: {e}")
            raise RuntimeError("Failed to create record")


# Compatibility functions for SurrealDB-style IDs


def ensure_record_id(value: Union[str, Any]) -> str:
    """
    Ensure a value is in record ID format.

    For SQLite, we keep the 'table:id' string format for compatibility
    with the rest of the codebase.
    """
    if value is None:
        return value
    return str(value)


def parse_record_ids(obj: Any) -> Any:
    """
    Recursively convert any special ID types to strings.

    For SQLite, this is mostly a no-op but maintains API compatibility.
    """
    if isinstance(obj, dict):
        return {k: parse_record_ids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [parse_record_ids(item) for item in obj]
    return obj
