# Database Module

Dual-backend database layer supporting both SurrealDB (graph) and SQLite (relational) with unified repository patterns.

## Purpose

Abstracts database operations behind unified functions so domain models remain backend-agnostic. All `is_sqlite()` checks should be isolated to the `database/` module.

## Backend Detection

```python
from open_notebook.database import is_sqlite, is_surrealdb

if is_sqlite():  # DATABASE_BACKEND=sqlite
    # SQLite-specific behavior (should ONLY appear in database/ module)
```

**Environment Variable**: `DATABASE_BACKEND=sqlite` or `DATABASE_BACKEND=surrealdb` (default)

## Key Principle: Backend Isolation

**NEVER add `is_sqlite()` checks to:**
- `open_notebook/domain/*.py`
- `open_notebook/podcasts/*.py`
- `api/routers/*.py`
- `commands/*.py` (except infrastructure setup)

**ALWAYS handle backend differences in:**
- `open_notebook/database/__init__.py`
- `open_notebook/database/sqlite_repository.py`
- `open_notebook/database/executor.py`

See **[sqlite-translation-guide.md](sqlite-translation-guide.md)** for detailed translation patterns.

## Component Catalog

### `__init__.py` - Unified Interface

Exports functions that work for both backends:

**Core CRUD:**
- `repo_query(query, params)` - Execute raw query
- `repo_create(table, data)` - Insert record with auto-timestamps
- `repo_update(table, id, data)` - Update record
- `repo_delete(record_id)` - Delete record
- `repo_upsert(table, id, data)` - Create or update

**ID Utilities:**
- `parse_id(record_id)` - Parse `"table:123"` → `("table", 123)`
- `format_id(table, id)` - Format `123` → `"table:123"`
- `format_ids(table, rows)` - Batch format list of rows
- `normalize_result(row, table)` - Ensure `id` field is `"table:id"` format
- `ensure_record_id(value)` - Coerce to valid record ID string

**Relationship Queries:**
- `repo_get_related(source_table, source_id, relation, target_table)` - Get related records
- `repo_count_related(source_table, source_id, relation)` - Count related records
- `repo_add_relation(source_id, relation, target_id)` - Create relationship
- `repo_remove_relation(source_id, relation, target_id)` - Remove relationship
- `repo_check_relation(source_id, relation, target_id)` - Check if relationship exists

**Specialized Queries:**
- `repo_get(table, record_id)` - Get single record by ID
- `repo_list(table, filters, order_by, limit, offset)` - List with filtering
- `repo_list_with_counts(table, count_relations, filters, order_by)` - List with relationship counts

**Source/Notebook Helpers:**
- `repo_get_sources_for_notebook(notebook_id)` - Get sources in a notebook
- `repo_get_notebooks_for_source(source_id)` - Get notebooks containing source
- `repo_get_sessions_for_source(source_id)` - Get chat sessions for source
- `repo_get_insights_for_source(source_id)` - Get insights for source
- `repo_get_source_for_embedding(embedding_id)` - Get source from embedding
- `repo_get_source_for_insight(insight_id)` - Get source from insight
- `repo_get_notebook_for_session(session_id)` - Get notebook for chat session

**Singleton Records:**
- `repo_singleton_get(record_id)` - Get singleton config (e.g., `"open_notebook:content_settings"`)
- `repo_singleton_upsert(record_id, data)` - Upsert singleton config

**Embedding Operations:**
- `repo_get_embeddings(source_id, include_content)` - Get embedding chunks
- `repo_update_embedding(table, record_id, embedding)` - Update embedding field
- `repo_delete_embeddings(source_id)` - Delete all embeddings for source
- `repo_count_source_embeddings(source_id)` - Count embedding chunks
- `serialize_embedding(list)` - Convert to storage format
- `deserialize_embedding(blob)` - Convert from storage format

**Search (SQLite only, None for SurrealDB):**
- `db_text_search` - FTS5 full-text search
- `db_vector_search` - Cosine similarity vector search

### `sqlite_repository.py` - SQLite Implementation

SQLite-specific implementations of all repository functions.

**Key Functions:**
- `normalize_result(row, table)` - Ensures IDs are in `table:id` format
- `_map_fields_for_table(table, data)` - Maps model fields to SQLite columns
- `_prepare_data(data)` - Serializes complex types (JSON, embeddings, datetime)
- `_row_to_dict(row)` - Converts SQLite Row with JSON parsing

**Field Mapping:**
| Model Field | SQLite Column | Tables |
|-------------|---------------|--------|
| `command` | `command_id` | source, episode |
| `source` (foreign key) | `source_id` | source_embedding, source_insight |

### `sqlite_search.py` - Search Implementation

- `text_search(query, match_count, search_sources, search_notes)` - FTS5 full-text search
- `vector_search(query_embedding, match_count, search_sources, search_notes, min_similarity)` - Cosine similarity search

**Note:** Vector search loads all embeddings into memory for Python-side cosine calculation (sqlite-vec optional).

### `sqlite_migrate.py` - Migration System

- `run_migrations()` - Execute pending migrations from `sqlite_migrations/`
- Creates `_migrations` table to track applied migrations
- Migrations are numbered: `001_initial_schema.sql`, `002_default_data.sql`, etc.

### `sqlite_migrations/` - SQL Migration Files

Sequential SQL files:
- `001_initial_schema.sql` - All tables, indexes, FTS5 virtual tables, triggers
- `002_default_data.sql` - Default transformations
- `003_command_queue.sql` - Additional command queue indexes

### `executor.py` - Command Execution Strategy

Abstracts embedding operation execution to eliminate `is_sqlite()` checks in domain models:

```python
from open_notebook.database import executor

# In domain model - no backend check needed
result = await executor.embed_note(note_id)
result = await executor.embed_source(source_id)
result = await executor.embed_insight(insight_id)
```

**Strategies:**
- `DirectExecutor` (SQLite) - Runs embedding inline synchronously
- `QueueExecutor` (SurrealDB) - Submits to surreal_commands job queue

### `command_queue.py` / `command_worker.py` - Async Job System

Local command queue for SQLite (replaces surreal_commands):
- `submit_command(app, command_name, params)` - Submit async job
- `get_command_status(command_id)` - Poll job status
- `@command` decorator - Register command handlers

### `repository.py` - SurrealDB Implementation

Original SurrealDB repository (used when `DATABASE_BACKEND=surrealdb`):
- `db_connection()` - Async context manager for SurrealDB
- `repo_query()`, `repo_create()`, etc. - SurrealQL operations

### `async_migrate.py` - SurrealDB Migrations

SurrealDB migration system using `.surrealql` files in `/migrations/`.

## Relationship Mapping

SurrealDB graph edges map to SQLite junction tables:

| SurrealDB Relation | SQLite Junction Table | Source Column | Target Column |
|-------------------|----------------------|---------------|---------------|
| `source->reference->notebook` | `source_notebook` | `source_id` | `notebook_id` |
| `note->artifact->notebook` | `note_notebook` | `note_id` | `notebook_id` |
| `chat_session->refers_to->notebook` | `chat_session_reference` | `chat_session_id` | `notebook_id` |
| `chat_session->refers_to->source` | `chat_session_reference` | `chat_session_id` | `source_id` |

## Common Patterns

### Async-First Design
All operations are async. Use `await` for all repo functions:
```python
result = await repo_get("notebook", "notebook:123")
notebooks = await repo_list("notebook", order_by="updated DESC")
```

### ID Format Consistency
All IDs returned from repository functions are in `"table:id"` format:
```python
record = await repo_create("notebook", {"name": "Test"})
# record["id"] == "notebook:1"
```

### Auto-Timestamping
`repo_create()` sets `created` and `updated`, `repo_update()` sets `updated`:
```python
await repo_create("notebook", {"name": "Test"})  # created/updated auto-set
await repo_update("notebook", "notebook:1", {"name": "Updated"})  # updated auto-set
```

## Integration Points

**API Startup** (`api/main.py`):
- Runs migrations on startup
- Starts command worker thread for SQLite

**Domain Models** (`domain/*.py`):
- Call repo functions for persistence
- Use `executor` for embedding operations

**Commands** (`commands/*.py`):
- Background jobs use repo functions
- Embedding commands registered with `@command` decorator

## Important Quirks & Gotchas

1. **No connection pooling**: Each repo operation opens/closes connection
2. **JSON field auto-detection**: Fields starting with `{` or `[` auto-parsed as JSON
3. **Embedding serialization**: SQLite stores as BLOB, SurrealDB stores as array
4. **FTS5 sync via triggers**: Full-text indexes auto-updated on INSERT/UPDATE/DELETE
5. **Singleton ID convention**: Config tables use `id=1` in SQLite, `namespace:table` in SurrealDB
6. **Vector search in-memory**: SQLite loads all embeddings for Python-side similarity

## How to Extend

### Add New Table

1. Create migration: `sqlite_migrations/NNN_description.sql`
2. If relationships needed, add junction table
3. Update `_map_fields_for_table()` if field names differ from columns
4. Add specialized repo functions if complex queries needed

### Add New Relationship Type

1. Add junction table to migration
2. Update `_get_relation_config()` in `sqlite_repository.py`
3. Update `repo_get_related()`, `repo_add_relation()`, etc.
4. Add corresponding SurrealDB wrapper in `__init__.py`

See **[SURREAL_TO_SQLITE.md](SURREAL_TO_SQLITE.md)** for complete translation patterns.
