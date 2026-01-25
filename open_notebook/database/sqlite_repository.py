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

    # Known JSON fields that should always be parsed
    # Also try to auto-detect JSON for strings starting with { or [
    known_json_fields = {
        "topics",
        "speakers",
        "youtube_preferred_languages",
        "episode_profile",
        "speaker_profile",
        "transcript",
        "outline",
    }

    for key, value in result.items():
        if isinstance(value, str):
            # Parse known JSON fields
            if key in known_json_fields:
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    pass
            # Auto-detect JSON for strings starting with { or [
            elif value.startswith(("{", "[")):
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    pass
        # Deserialize embedding fields
        elif isinstance(value, bytes) and key == "embedding":
            result[key] = deserialize_embedding(value)

    return result


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _prepare_value(key: str, value: Any) -> Any:
    """Prepare a value for SQLite storage."""
    if value is None:
        return None

    # Serialize lists/dicts to JSON
    if isinstance(value, (list, dict)):
        # Special handling for embeddings
        if key == "embedding":
            return serialize_embedding(value)
        # Use custom encoder to handle datetime objects in nested structures
        return json.dumps(value, cls=DateTimeEncoder)

    # Handle datetime
    if isinstance(value, datetime):
        return value.isoformat()

    return value


def _map_fields_for_table(table: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map model field names to SQLite column names for specific tables.

    This handles the mismatch between model field names and SQLite column names,
    e.g., 'command' field -> 'command_id' column for source/episode tables.
    """
    result = dict(data)

    # For source and episode tables, command -> command_id
    if table in ("source", "episode") and "command" in result:
        command_val = result.pop("command")
        if command_val is not None:
            result["command_id"] = str(command_val)

    return result


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

            # Commit for write operations (INSERT, UPDATE, DELETE)
            query_upper = query_str.strip().upper()
            if query_upper.startswith(("INSERT", "UPDATE", "DELETE")):
                await db.commit()

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
        The created record with generated ID in 'table:id' format
    """
    data = dict(data)  # Copy to avoid mutation
    data.pop("id", None)

    # Add timestamps
    now = datetime.now(timezone.utc).isoformat()
    data["created"] = now
    data["updated"] = now

    # Map model field names to SQLite column names
    data = _map_fields_for_table(table, data)
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
                # Always normalize to 'table:id' format for consistency
                return normalize_result(_row_to_dict(row), table)
            else:
                return {"id": format_id(table, row_id), **data}
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
        List containing the updated record with 'table:id' format ID
    """
    # Handle SurrealDB-style IDs (table:id)
    numeric_id = id
    if isinstance(id, str) and ":" in id:
        numeric_id = id.split(":")[1]

    data = dict(data)
    data.pop("id", None)
    data["updated"] = datetime.now(timezone.utc).isoformat()

    # Map model field names to SQLite column names
    data = _map_fields_for_table(table, data)
    prepared = _prepare_data(data)

    set_clause = ", ".join([f"{k} = ?" for k in prepared.keys()])
    query = f"UPDATE {table} SET {set_clause} WHERE id = ?"

    async with db_connection() as db:
        try:
            await db.execute(query, (*prepared.values(), numeric_id))
            await db.commit()

            # Fetch the updated record
            result = await db.execute(f"SELECT * FROM {table} WHERE id = ?", (numeric_id,))
            row = await result.fetchone()

            if row:
                # Always normalize to 'table:id' format for consistency
                return [normalize_result(_row_to_dict(row), table)]
            else:
                raise RuntimeError(f"Record not found after update: {table}:{numeric_id}")
        except Exception as e:
            logger.error(f"Failed to update {table}:{numeric_id}: {e}")
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


# =============================================================================
# HIGH-LEVEL ABSTRACTION METHODS
# These methods encapsulate SQLite-specific logic to minimize is_sqlite() checks
# throughout the codebase.
# =============================================================================


def parse_id(record_id: str) -> Tuple[str, int]:
    """
    Parse 'table:123' or '123' into (table, numeric_id).

    Args:
        record_id: Record ID in 'table:id' format or just 'id'

    Returns:
        Tuple of (table_name, numeric_id)
    """
    if ":" in record_id:
        parts = record_id.split(":", 1)
        return parts[0], int(parts[1])
    return "", int(record_id)


def format_id(table: str, numeric_id: int) -> str:
    """Format as 'table:123' for API compatibility."""
    return f"{table}:{numeric_id}"


def normalize_result(row: Dict[str, Any], table: str) -> Dict[str, Any]:
    """
    Ensure 'id' field is in 'table:id' format and handle field name mappings.

    Args:
        row: Database row as dictionary
        table: Table name to use for ID formatting

    Returns:
        Row with normalized ID field and mapped field names
    """
    result = dict(row)
    if "id" in result and not str(result["id"]).startswith(f"{table}:"):
        result["id"] = format_id(table, int(result["id"]))

    # Handle field name mappings (SQLite column names -> model field names)
    # For source and episode tables, command_id -> command
    if table in ("source", "episode") and "command_id" in result:
        result["command"] = result.pop("command_id")

    return result


async def repo_get(table: str, record_id: str) -> Optional[Dict[str, Any]]:
    """
    Get single record by ID, returns dict with normalized id.

    Args:
        table: Table name
        record_id: Record ID (can be 'table:id' or just 'id')

    Returns:
        Record as dictionary with normalized ID, or None if not found
    """
    _, id_value = parse_id(record_id) if ":" in record_id else ("", int(record_id))

    result = await repo_query(f"SELECT * FROM {table} WHERE id = ?", (id_value,))
    if result:
        return normalize_result(result[0], table)
    return None


async def repo_list(
    table: str,
    filters: Optional[Dict[str, Any]] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    List records with optional filtering/ordering.

    Args:
        table: Table name
        filters: Dictionary of field=value filters (uses AND)
        order_by: ORDER BY clause (e.g., "updated DESC")
        limit: Maximum number of records
        offset: Number of records to skip

    Returns:
        List of records as dictionaries with normalized IDs
    """
    query = f"SELECT * FROM {table}"
    params: List[Any] = []

    if filters:
        conditions = []
        for key, value in filters.items():
            conditions.append(f"{key} = ?")
            params.append(value)
        query += " WHERE " + " AND ".join(conditions)

    if order_by:
        query += f" ORDER BY {order_by}"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    if offset is not None:
        query += " OFFSET ?"
        params.append(offset)

    result = await repo_query(query, tuple(params) if params else None)
    return [normalize_result(row, table) for row in result]


# Mapping of SurrealDB relationship names to SQLite junction table configurations
# Format: relationship_name -> (junction_table, source_column, target_column)
RELATION_MAPPING = {
    "reference": {
        "source": ("source_notebook", "source_id", "notebook_id"),
        "note": ("note_notebook", "note_id", "notebook_id"),
    },
    "artifact": {
        "note": ("note_notebook", "note_id", "notebook_id"),
    },
    "refers_to": {
        "chat_session": {
            "notebook": ("chat_session_reference", "chat_session_id", "notebook_id"),
            "source": ("chat_session_reference", "chat_session_id", "source_id"),
        },
    },
}


def _get_relation_config(
    source_table: str, relation: str, target_table: Optional[str] = None
) -> Tuple[str, str, str]:
    """
    Get junction table configuration for a relationship.

    Args:
        source_table: Source table name
        relation: Relationship name (reference, artifact, refers_to)
        target_table: Target table name (optional, for polymorphic relations)

    Returns:
        Tuple of (junction_table, source_column, target_column)
    """
    if relation == "reference":
        return ("source_notebook", "source_id", "notebook_id")
    elif relation == "artifact":
        return ("note_notebook", "note_id", "notebook_id")
    elif relation == "refers_to":
        if target_table == "source":
            return ("chat_session_reference", "chat_session_id", "source_id")
        else:
            return ("chat_session_reference", "chat_session_id", "notebook_id")
    else:
        raise ValueError(f"Unknown relationship: {relation}")


async def repo_get_related(
    source_table: str,
    source_id: str,
    relation: str,
    target_table: str,
) -> List[Dict[str, Any]]:
    """
    Get related records via junction table.

    Maps SurrealDB edge queries to JOIN queries.
    Example: Get sources for notebook:123
    - SurrealDB: SELECT * FROM source WHERE <-reference<-(notebook:123)
    - SQLite: SELECT s.* FROM source s JOIN source_notebook sn ON s.id = sn.source_id WHERE sn.notebook_id = ?

    Args:
        source_table: Table with the ID we're querying from (e.g., "notebook")
        source_id: ID of the record (e.g., "notebook:123")
        relation: Relationship name (reference, artifact, refers_to)
        target_table: Table of records to return (e.g., "source")

    Returns:
        List of related records with normalized IDs
    """
    _, id_value = parse_id(source_id)

    # Determine junction table configuration based on relationship
    if relation == "reference":
        # notebook -> source (via source_notebook)
        junction_table = "source_notebook"
        if source_table == "notebook":
            # Get sources for a notebook
            source_col = "notebook_id"
            target_col = "source_id"
        else:
            # Get notebooks for a source
            source_col = "source_id"
            target_col = "notebook_id"
    elif relation == "artifact":
        # notebook -> note (via note_notebook)
        junction_table = "note_notebook"
        if source_table == "notebook":
            source_col = "notebook_id"
            target_col = "note_id"
        else:
            source_col = "note_id"
            target_col = "notebook_id"
    elif relation == "refers_to":
        # chat_session -> notebook/source (via chat_session_reference)
        junction_table = "chat_session_reference"
        if source_table == "chat_session":
            source_col = "chat_session_id"
            target_col = f"{target_table}_id"
        else:
            source_col = f"{source_table}_id"
            target_col = "chat_session_id"
    else:
        raise ValueError(f"Unknown relationship: {relation}")

    query = f"""
        SELECT t.* FROM {target_table} t
        JOIN {junction_table} j ON t.id = j.{target_col}
        WHERE j.{source_col} = ?
        ORDER BY t.updated DESC
    """

    result = await repo_query(query, (id_value,))
    return [normalize_result(row, target_table) for row in result]


async def repo_count_related(
    source_table: str,
    source_id: str,
    relation: str,
) -> int:
    """
    Count related records.

    Args:
        source_table: Table with the ID we're querying from
        source_id: ID of the record
        relation: Relationship name

    Returns:
        Count of related records
    """
    _, id_value = parse_id(source_id)

    if relation == "reference":
        junction_table = "source_notebook"
        source_col = "notebook_id" if source_table == "notebook" else "source_id"
    elif relation == "artifact":
        junction_table = "note_notebook"
        source_col = "notebook_id" if source_table == "notebook" else "note_id"
    elif relation == "refers_to":
        junction_table = "chat_session_reference"
        source_col = f"{source_table}_id"
    else:
        raise ValueError(f"Unknown relationship: {relation}")

    result = await repo_query(
        f"SELECT COUNT(*) as count FROM {junction_table} WHERE {source_col} = ?",
        (id_value,),
    )
    return result[0]["count"] if result else 0


async def repo_add_relation(
    source_id: str,
    relation: str,
    target_id: str,
) -> bool:
    """
    Add relationship (insert into junction table).

    Args:
        source_id: Source record ID (e.g., "source:123")
        relation: Relationship name
        target_id: Target record ID (e.g., "notebook:456")

    Returns:
        True if relationship was added or already exists
    """
    source_table, source_int = parse_id(source_id)
    target_table, target_int = parse_id(target_id)

    if relation == "reference":
        # source -> notebook
        await repo_query(
            "INSERT OR IGNORE INTO source_notebook (source_id, notebook_id) VALUES (?, ?)",
            (source_int, target_int),
        )
    elif relation == "artifact":
        # note -> notebook
        await repo_query(
            "INSERT OR IGNORE INTO note_notebook (note_id, notebook_id) VALUES (?, ?)",
            (source_int, target_int),
        )
    elif relation == "refers_to":
        # chat_session -> notebook or source
        if target_table == "source":
            await repo_query(
                "INSERT OR IGNORE INTO chat_session_reference (chat_session_id, source_id) VALUES (?, ?)",
                (source_int, target_int),
            )
        else:
            await repo_query(
                "INSERT OR IGNORE INTO chat_session_reference (chat_session_id, notebook_id) VALUES (?, ?)",
                (source_int, target_int),
            )
    else:
        raise ValueError(f"Unknown relationship: {relation}")

    return True


async def repo_remove_relation(
    source_id: str,
    relation: str,
    target_id: str,
) -> bool:
    """
    Remove relationship.

    Args:
        source_id: Source record ID
        relation: Relationship name
        target_id: Target record ID

    Returns:
        True if relationship was removed
    """
    source_table, source_int = parse_id(source_id)
    target_table, target_int = parse_id(target_id)

    if relation == "reference":
        await repo_query(
            "DELETE FROM source_notebook WHERE source_id = ? AND notebook_id = ?",
            (source_int, target_int),
        )
    elif relation == "artifact":
        await repo_query(
            "DELETE FROM note_notebook WHERE note_id = ? AND notebook_id = ?",
            (source_int, target_int),
        )
    elif relation == "refers_to":
        if target_table == "source":
            await repo_query(
                "DELETE FROM chat_session_reference WHERE chat_session_id = ? AND source_id = ?",
                (source_int, target_int),
            )
        else:
            await repo_query(
                "DELETE FROM chat_session_reference WHERE chat_session_id = ? AND notebook_id = ?",
                (source_int, target_int),
            )
    else:
        raise ValueError(f"Unknown relationship: {relation}")

    return True


async def repo_check_relation(
    source_id: str,
    relation: str,
    target_id: str,
) -> bool:
    """
    Check if relationship exists.

    Args:
        source_id: Source record ID
        relation: Relationship name
        target_id: Target record ID

    Returns:
        True if relationship exists
    """
    source_table, source_int = parse_id(source_id)
    target_table, target_int = parse_id(target_id)

    if relation == "reference":
        result = await repo_query(
            "SELECT 1 FROM source_notebook WHERE source_id = ? AND notebook_id = ?",
            (source_int, target_int),
        )
    elif relation == "artifact":
        result = await repo_query(
            "SELECT 1 FROM note_notebook WHERE note_id = ? AND notebook_id = ?",
            (source_int, target_int),
        )
    elif relation == "refers_to":
        if target_table == "source":
            result = await repo_query(
                "SELECT 1 FROM chat_session_reference WHERE chat_session_id = ? AND source_id = ?",
                (source_int, target_int),
            )
        else:
            result = await repo_query(
                "SELECT 1 FROM chat_session_reference WHERE chat_session_id = ? AND notebook_id = ?",
                (source_int, target_int),
            )
    else:
        raise ValueError(f"Unknown relationship: {relation}")

    return len(result) > 0


async def repo_list_with_counts(
    table: str,
    count_relations: Dict[str, Tuple[str, str]],
    filters: Optional[Dict[str, Any]] = None,
    order_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List records with relationship counts.

    Args:
        table: Table name
        count_relations: Dictionary mapping column alias to (junction_table, id_column)
            Example: {"source_count": ("source_notebook", "notebook_id")}
        filters: Optional filters
        order_by: Optional order clause

    Returns:
        List of records with count columns added
    """
    # Build subquery for each count
    count_selects = []
    count_joins = []

    for alias, (junction_table, id_col) in count_relations.items():
        subquery_alias = f"{alias}_sq"
        count_selects.append(f"COALESCE({subquery_alias}.{alias}, 0) as {alias}")
        count_joins.append(f"""
            LEFT JOIN (
                SELECT {id_col}, COUNT(*) as {alias}
                FROM {junction_table}
                GROUP BY {id_col}
            ) {subquery_alias} ON {table}.id = {subquery_alias}.{id_col}
        """)

    query = f"SELECT {table}.*, {', '.join(count_selects)} FROM {table}"
    query += " ".join(count_joins)

    params: List[Any] = []
    if filters:
        conditions = []
        for key, value in filters.items():
            conditions.append(f"{table}.{key} = ?")
            params.append(value)
        query += " WHERE " + " AND ".join(conditions)

    if order_by:
        query += f" ORDER BY {table}.{order_by}"

    result = await repo_query(query, tuple(params) if params else None)
    return [normalize_result(row, table) for row in result]


# =============================================================================
# EMBEDDING OPERATIONS
# =============================================================================


async def repo_get_embeddings(
    source_id: str,
    include_content: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get embeddings for a source.

    Args:
        source_id: Source ID
        include_content: Whether to include chunk content

    Returns:
        List of embedding records
    """
    _, id_value = parse_id(source_id)

    if include_content:
        result = await repo_query(
            "SELECT * FROM source_embedding WHERE source_id = ? ORDER BY chunk_order",
            (id_value,),
        )
    else:
        result = await repo_query(
            "SELECT id, source_id, chunk_order FROM source_embedding WHERE source_id = ? ORDER BY chunk_order",
            (id_value,),
        )

    return [normalize_result(row, "source_embedding") for row in result]


async def repo_update_embedding(
    table: str,
    record_id: str,
    embedding: List[float],
) -> bool:
    """
    Update embedding field (handles serialization).

    Args:
        table: Table name (note, source_insight)
        record_id: Record ID
        embedding: Embedding vector

    Returns:
        True if update succeeded
    """
    _, id_value = parse_id(record_id)
    serialized = serialize_embedding(embedding)

    await repo_query(
        f"UPDATE {table} SET embedding = ? WHERE id = ?",
        (serialized, id_value),
    )
    return True


async def repo_delete_embeddings(source_id: str) -> int:
    """
    Delete all embeddings for a source.

    Args:
        source_id: Source ID

    Returns:
        Number of deleted records
    """
    _, id_value = parse_id(source_id)

    # Get count first
    result = await repo_query(
        "SELECT COUNT(*) as count FROM source_embedding WHERE source_id = ?",
        (id_value,),
    )
    count = result[0]["count"] if result else 0

    await repo_query(
        "DELETE FROM source_embedding WHERE source_id = ?",
        (id_value,),
    )

    return count


async def repo_get_notebooks_for_source(source_id: str) -> List[str]:
    """
    Get notebook IDs for a source.

    Args:
        source_id: Source ID

    Returns:
        List of notebook IDs in 'notebook:id' format
    """
    _, id_value = parse_id(source_id)

    result = await repo_query(
        "SELECT notebook_id FROM source_notebook WHERE source_id = ?",
        (id_value,),
    )

    return [f"notebook:{row['notebook_id']}" for row in result]


async def repo_get_sources_for_notebook(
    notebook_id: str,
    order_by: Optional[str] = "updated DESC",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get sources for a notebook with optional pagination.

    Args:
        notebook_id: Notebook ID
        order_by: ORDER BY clause
        limit: Maximum records
        offset: Records to skip

    Returns:
        List of source records
    """
    _, id_value = parse_id(notebook_id)

    query = """
        SELECT s.* FROM source s
        JOIN source_notebook sn ON s.id = sn.source_id
        WHERE sn.notebook_id = ?
    """

    if order_by:
        query += f" ORDER BY s.{order_by}"

    params: List[Any] = [id_value]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    if offset is not None:
        query += " OFFSET ?"
        params.append(offset)

    result = await repo_query(query, tuple(params))
    return [normalize_result(row, "source") for row in result]


async def repo_get_sessions_for_source(source_id: str) -> List[Dict[str, Any]]:
    """
    Get chat sessions for a source.

    Args:
        source_id: Source ID

    Returns:
        List of chat session records
    """
    _, id_value = parse_id(source_id)

    result = await repo_query(
        """
        SELECT cs.* FROM chat_session cs
        JOIN chat_session_reference csr ON cs.id = csr.chat_session_id
        WHERE csr.source_id = ?
        ORDER BY cs.updated DESC
        """,
        (id_value,),
    )

    return [normalize_result(row, "chat_session") for row in result]


# =============================================================================
# SINGLETON RECORD OPERATIONS
# =============================================================================


async def repo_singleton_get(record_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a singleton record by its record_id (e.g., 'open_notebook:content_settings').

    Singleton records always use id=1 in SQLite.

    Args:
        record_id: Record ID in 'namespace:table' format

    Returns:
        Record as dictionary, or None if not found
    """
    # Parse record_id to get table name (e.g., 'open_notebook:content_settings')
    parts = record_id.split(":")
    if len(parts) == 2:
        table_name = parts[1]
    else:
        table_name = record_id

    result = await repo_query(f"SELECT * FROM {table_name} WHERE id = ?", (1,))
    return result[0] if result else None


async def repo_singleton_upsert(
    record_id: str,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Upsert a singleton record.

    Singleton records always use id=1 in SQLite.

    Args:
        record_id: Record ID in 'namespace:table' format
        data: Data to upsert

    Returns:
        The upserted record
    """
    # Parse record_id to get table name
    parts = record_id.split(":")
    if len(parts) == 2:
        table_name = parts[1]
    else:
        table_name = record_id

    data = dict(data)
    data.pop("id", None)

    prepared = _prepare_data(data)
    prepared["id"] = 1  # Singleton always uses id=1

    columns = ", ".join(prepared.keys())
    placeholders = ", ".join(["?" for _ in prepared])

    query = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

    async with db_connection() as db:
        await db.execute(query, tuple(prepared.values()))
        await db.commit()

        # Fetch the record
        result = await db.execute(f"SELECT * FROM {table_name} WHERE id = ?", (1,))
        row = await result.fetchone()

        return _row_to_dict(row) if row else {"id": 1, **data}


# =============================================================================
# SOURCE RELATIONSHIP QUERIES
# =============================================================================


async def repo_get_insights_for_source(source_id: str) -> List[Dict[str, Any]]:
    """
    Get all insights for a source.

    Args:
        source_id: Source ID

    Returns:
        List of insight records with normalized IDs
    """
    _, id_value = parse_id(source_id)

    result = await repo_query(
        "SELECT * FROM source_insight WHERE source_id = ?",
        (id_value,),
    )

    return [normalize_result(row, "source_insight") for row in result]


async def repo_get_source_for_embedding(embedding_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the source record for an embedding.

    Args:
        embedding_id: Embedding ID

    Returns:
        Source record with normalized ID and asset fields, or None
    """
    _, id_value = parse_id(embedding_id)

    result = await repo_query(
        """
        SELECT s.id, s.file_path, s.url, s.title, s.topics, s.full_text,
               s.command_id, s.created, s.updated
        FROM source s
        JOIN source_embedding se ON s.id = se.source_id
        WHERE se.id = ?
        """,
        (id_value,),
    )

    if result:
        src_data = normalize_result(result[0], "source")
        return src_data
    return None


async def repo_get_source_for_insight(insight_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the source record for an insight.

    Args:
        insight_id: Insight ID

    Returns:
        Source record with normalized ID and asset fields, or None
    """
    _, id_value = parse_id(insight_id)

    result = await repo_query(
        """
        SELECT s.id, s.file_path, s.url, s.title, s.topics, s.full_text,
               s.command_id, s.created, s.updated
        FROM source s
        JOIN source_insight si ON s.id = si.source_id
        WHERE si.id = ?
        """,
        (id_value,),
    )

    if result:
        src_data = normalize_result(result[0], "source")
        return src_data
    return None


async def repo_get_notebook_for_session(session_id: str) -> Optional[str]:
    """
    Get the notebook ID for a chat session.

    Args:
        session_id: Chat session ID

    Returns:
        Notebook ID in 'notebook:id' format, or None if not found
    """
    _, id_value = parse_id(session_id)

    result = await repo_query(
        "SELECT notebook_id FROM chat_session_reference WHERE chat_session_id = ?",
        (id_value,),
    )

    if result:
        return f"notebook:{result[0]['notebook_id']}"
    return None


async def repo_count_source_embeddings(source_id: str) -> int:
    """
    Count the number of embeddings for a source.

    Args:
        source_id: Source ID

    Returns:
        Number of embedding chunks
    """
    _, id_value = parse_id(source_id)

    result = await repo_query(
        "SELECT COUNT(*) as chunks FROM source_embedding WHERE source_id = ?",
        (id_value,),
    )

    if len(result) == 0:
        return 0
    return result[0]["chunks"]


# =============================================================================
# BATCH ID FORMATTING
# =============================================================================


def format_ids(table: str, rows: List[Dict[str, Any]]) -> List[str]:
    """
    Format a list of rows into 'table:id' strings.

    Args:
        table: Table name
        rows: List of row dictionaries with 'id' field

    Returns:
        List of formatted IDs in 'table:id' format
    """
    return [f"{table}:{row['id']}" for row in rows]
