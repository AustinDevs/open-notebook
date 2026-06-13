# Database Module

SurrealDB abstraction layer providing repository pattern for CRUD operations and async migration management.

## Purpose

Encapsulates all database interactions: connection pooling, async CRUD operations, relationship management, and schema migrations. Provides clean interface for domain models and API endpoints to interact with SurrealDB without direct query knowledge.

## Architecture Overview

Two-tier system:
1. **Repository Layer** (repository.py): Raw async CRUD operations on SurrealDB via AsyncSurreal client
2. **Migration Layer** (async_migrate.py): Schema versioning and migration execution

Both leverage connection context manager for lifecycle management and automatic cleanup.

## Component Catalog

### repository.py

**Connection Management**
- `get_database_url()`: Resolves `SURREAL_URL` or constructs from `SURREAL_ADDRESS`/`SURREAL_PORT` (backward compatible)
- `get_database_password()`: Falls back from `SURREAL_PASSWORD` to legacy `SURREAL_PASS` env var
- `db_connection()`: Async context manager handling sign-in, namespace/database selection, and cleanup
  - Opens AsyncSurreal, authenticates, selects namespace/database, yields connection, closes on exit

**Query Operations**
- `repo_query(query_str, vars)`: Execute raw SurrealQL with parameter substitution; returns list of dicts
- `repo_create(table, data)`: Insert record; auto-adds `created`/`updated` timestamps; removes any existing `id` field
- `repo_insert(table, data_list, ignore_duplicates)`: Bulk insert multiple records; optionally ignores "already contains" errors
- `repo_upsert(table, id, data, add_timestamp)`: MERGE operation for create-or-update; optionally adds `updated` timestamp
- `repo_update(table, id, data)`: Update existing record by table+id or full record_id; auto-adds `updated`, parses ISO dates
- `repo_delete(record_id)`: Delete record by RecordID
- `repo_relate(source, relationship, target, data)`: Create graph relationship; optional relationship data

**Utilities**
- `parse_record_ids(obj)`: Recursively converts SurrealDB RecordID objects to strings (deep tree traversal)
- `ensure_record_id(value)`: Coerces string or RecordID to RecordID type

### async_migrate.py

**Migration Classes**
- `AsyncMigration`: Single migration wrapper
  - `from_file(path)`: Load .surrealql file; strips comments and whitespace
  - `run(bump)`: Execute SQL; call bump_version() on success (bump=True) or lower_version() (bump=False)

- `AsyncMigrationRunner`: Sequences multiple migrations
  - `run_all()`: Execute pending migrations from current_version to end
  - `run_one_up()`: Run next migration
  - `run_one_down()`: Rollback latest migration

- `AsyncMigrationManager`: Main orchestrator
  - **Auto-discovers** all `migrations/*.surrealql` via `discover_migrations()` (no hard-coded list), ordering by integer id. Files are `<id>[_<description>].surrealql`; ids are either legacy sequential (`1`..`15`) or unix timestamps for new migrations (`1718305200_add_x.surrealql`, created via `make migration name=add_x`).
  - Tracks an **applied set**: each applied migration's id is recorded in `_sbl_migrations` (id stored in the `version` field). Pending = discovered ids not in the applied set. Backward compatible — existing rows 1..N are treated as the applied ids.
  - `get_current_version()`: Highest applied id (display/logging only)
  - `needs_migration()`: True if any discovered id is unapplied
  - `run_migration_up()`: Apply each pending migration (ascending id), recording its id
  - `run_migration_down()`: Roll back the highest applied migration that has a `_down` file

**Version Tracking**
- `get_latest_version()`: Query max version; returns 0 if _sbl_migrations table missing
- `get_all_versions()`: Fetch all migration records; returns empty list on error
- `bump_version()`: INSERT new entry into _sbl_migrations with version + applied_at timestamp
- `lower_version()`: DELETE latest migration record (rollback)

### migrate.py

**Backward Compatibility**
- `MigrationManager`: Sync wrapper around AsyncMigrationManager
  - `get_current_version()`: Wraps async call with asyncio.run()
  - `needs_migration` property: Checks if migration pending
  - `run_migration_up()`: Execute migrations synchronously

## Common Patterns

- **Async-first design**: All operations async via AsyncSurreal; sync wrapper provided for legacy code
- **Connection per operation**: Each repo_* function opens/closes connection (no pooling); designed for serverless/stateless API
- **Auto-timestamping**: repo_create() and repo_update() auto-set `created`/`updated` fields
- **Error resilience**: RuntimeError for transaction conflicts (retriable, logged at DEBUG level); catches and re-raises other exceptions
- **RecordID polymorphism**: Functions accept string or RecordID; coerced to consistent type
- **Graceful degradation**: Migration queries catch exceptions and treat table-not-found as version 0

## Key Dependencies

- `surrealdb`: AsyncSurreal client, RecordID type
- `loguru`: Logging with context (debug/error/success levels)
- Python stdlib: `os` (env vars), `datetime` (timestamps), `contextlib` (async context manager)

## Important Quirks & Gotchas

- **No connection pooling**: Each repo_* operation creates new connection; adequate for HTTP request-scoped operations but inefficient for bulk workloads
- **Auto-discovered migrations**: AsyncMigrationManager scans the `migrations/` dir; dropping in a `<id>[_desc].surrealql` file is enough (no code change). Prefer unix-timestamp ids for new migrations (`make migration name=...`) to avoid sequential-number collisions between branches.
- **Record ID format inconsistency**: repo_update() accepts both `table:id` format and full RecordID; path handling can be subtle
- **ISO date parsing**: repo_update() parses `created` field from string to datetime if present; assumes ISO format
- **Timestamp overwrite risk**: repo_create() always sets new timestamps; can't preserve original created time on reimport
- **Transaction conflict handling**: RuntimeError from transaction conflicts logged at DEBUG level without stack trace (prevents log spam during concurrent operations)
- **Graceful null returns**: get_all_versions() returns [] on table missing; allows migration system to bootstrap cleanly

## How to Extend

1. **Add new CRUD operation**: Follow repo_* pattern (open connection, execute query, handle errors, close)
2. **Add migration**: Run `make migration name=<desc>` (creates `migrations/<unix_ts>_<desc>.surrealql` + `_down`), fill in the SurrealQL. Auto-discovered on next startup — no code change. (Legacy sequential `N.surrealql` files still work.)
3. **Change timestamp behavior**: Modify repo_create()/repo_update() to not auto-set `updated` field if caller-provided
4. **Implement connection pooling**: Replace db_connection context manager with pool.acquire() pattern (for high-throughput scenarios)

## Integration Points

- **API startup** (api/main.py): FastAPI lifespan handler calls AsyncMigrationManager.run_migration_up() on server start
- **Domain models** (domain/*.py): All models call repo_* functions for persistence
- **Commands** (commands/*.py): Background jobs use repo_* for state updates
- **Streamlit UI** (pages/*.py): Deprecated migration check; relies on API to run migrations

## Usage Example

```python
from open_notebook.database.repository import repo_create, repo_query, repo_update

# Create
record = await repo_create("notebooks", {"title": "Research"})

# Query
results = await repo_query("SELECT * FROM notebooks WHERE title = $title", {"title": "Research"})

# Update
await repo_update("notebooks", record["id"], {"title": "Updated Research"})
```
