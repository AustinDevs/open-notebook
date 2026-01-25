# SurrealDB to SQLite Translation Guide

Quick reference for translating upstream SurrealDB patterns to SQLite. Use this when pulling in changes from the main branch.

## Core Translation Philosophy

1. **Backend logic stays in `database/`** - Never add `is_sqlite()` to domain models or routers
2. **Use unified repo functions** - They work for both backends
3. **IDs are always `table:id` format** - Repository layer handles normalization

---

## ID Handling

| SurrealDB | SQLite | Notes |
|-----------|--------|-------|
| `RecordID("table", "uuid")` | Integer autoincrement | Always format as `table:id` in API responses |
| `result["id"]` (returns RecordID) | `result["id"]` (returns int) | Use `normalize_result()` to ensure consistent format |

### Parsing IDs

```python
from open_notebook.database import parse_id, format_id

# Parse "notebook:123" → ("notebook", 123)
table, numeric_id = parse_id("notebook:123")

# Format 123 → "notebook:123"
record_id = format_id("notebook", 123)
```

### Normalizing Query Results

```python
from open_notebook.database import normalize_result

# Raw SQLite row: {"id": 123, "name": "Test"}
# After normalize: {"id": "notebook:123", "name": "Test"}
normalized = normalize_result(row, "notebook")
```

### Batch ID Formatting

```python
from open_notebook.database import format_ids

# Convert list of rows to ID strings
rows = [{"id": 1}, {"id": 2}, {"id": 3}]
ids = format_ids("source", rows)  # ["source:1", "source:2", "source:3"]
```

---

## Query Translation

### Simple Select

```python
# SurrealDB
await repo_query("SELECT * FROM notebook WHERE id = $id", {"id": record_id})

# SQLite
await repo_query("SELECT * FROM notebook WHERE id = ?", (numeric_id,))

# BETTER: Use unified function (works for both)
from open_notebook.database import repo_get
result = await repo_get("notebook", "notebook:123")
```

### List with Filters

```python
# SurrealDB
await repo_query(
    "SELECT * FROM notebook WHERE archived = false ORDER BY updated DESC",
    {}
)

# BETTER: Use unified function
from open_notebook.database import repo_list
results = await repo_list(
    "notebook",
    filters={"archived": 0},
    order_by="updated DESC"
)
```

### Relationship Queries (Graph → JOIN)

```python
# SurrealDB (graph traversal)
SELECT * FROM source WHERE <-reference<-(notebook:123)

# SQLite (junction table)
SELECT s.* FROM source s
JOIN source_notebook sn ON s.id = sn.source_id
WHERE sn.notebook_id = ?

# BETTER: Use unified function
from open_notebook.database import repo_get_related
sources = await repo_get_related("notebook", "notebook:123", "reference", "source")
```

### Counting Related Records

```python
# SurrealDB
SELECT *, count(<-reference<-source) as source_count FROM notebook

# SQLite
SELECT n.*, COUNT(sn.source_id) as source_count
FROM notebook n
LEFT JOIN source_notebook sn ON n.id = sn.notebook_id
GROUP BY n.id

# BETTER: Use unified function
from open_notebook.database import repo_list_with_counts
notebooks = await repo_list_with_counts(
    "notebook",
    count_relations={
        "source_count": ("source_notebook", "notebook_id"),
        "note_count": ("note_notebook", "notebook_id"),
    },
    order_by="updated DESC"
)
```

### Creating Relationships

```python
# SurrealDB
RELATE source:123->reference->notebook:456

# SQLite
INSERT OR IGNORE INTO source_notebook (source_id, notebook_id) VALUES (123, 456)

# BETTER: Use unified function
from open_notebook.database import repo_add_relation
await repo_add_relation("source:123", "reference", "notebook:456")
```

### Removing Relationships

```python
# SurrealDB
DELETE FROM reference WHERE in = source:123 AND out = notebook:456

# SQLite
DELETE FROM source_notebook WHERE source_id = 123 AND notebook_id = 456

# BETTER: Use unified function
from open_notebook.database import repo_remove_relation
await repo_remove_relation("source:123", "reference", "notebook:456")
```

---

## Field Name Mapping

Some models have field names that differ from SQLite columns:

| Model Field | SQLite Column | Tables | Direction |
|-------------|---------------|--------|-----------|
| `command` | `command_id` | source, episode | Model → DB |
| `source` (FK) | `source_id` | source_embedding, source_insight | Model → DB |

**How it works:**
- `_map_fields_for_table()` converts model fields → SQLite columns on write
- `normalize_result()` converts SQLite columns → model fields on read

```python
# In sqlite_repository.py
def _map_fields_for_table(table: str, data: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(data)
    if table in ("source", "episode") and "command" in result:
        command_val = result.pop("command")
        if command_val is not None:
            result["command_id"] = str(command_val)
    return result
```

---

## Embedding Storage

| SurrealDB | SQLite |
|-----------|--------|
| Native array field | BLOB (serialized floats) |
| `vector::similarity::cosine()` | Manual Python calculation |

### Serialization

```python
from open_notebook.database import serialize_embedding, deserialize_embedding

# Store embedding
blob = serialize_embedding([0.1, 0.2, 0.3, ...])
await repo_update_embedding("note", "note:123", embedding_list)

# Retrieve embedding
embedding_list = deserialize_embedding(blob)
```

### Vector Search

SQLite vector search is implemented in `sqlite_search.py`:
- Loads all embeddings into memory
- Computes cosine similarity in Python
- Falls back gracefully if sqlite-vec not available

---

## Singleton Records

Singleton config tables (DefaultModels, ContentSettings) use different ID conventions:

| SurrealDB | SQLite |
|-----------|--------|
| `open_notebook:content_settings` | Row with `id = 1` |

```python
from open_notebook.database import repo_singleton_get, repo_singleton_upsert

# Get singleton
settings = await repo_singleton_get("open_notebook:content_settings")

# Upsert singleton
await repo_singleton_upsert("open_notebook:content_settings", {"key": "value"})
```

---

## Embedding Operations (Executor Pattern)

Domain models use the executor pattern to avoid `is_sqlite()` checks:

```python
# OLD (with is_sqlite check) - DON'T DO THIS
if is_sqlite():
    await embed_directly(note_id)
else:
    submit_command("embed_note", {"note_id": note_id})

# NEW (executor pattern) - DO THIS
from open_notebook.database import executor
await executor.embed_note(note_id)
```

**Available executor methods:**
- `executor.embed_note(note_id)` - Embed a note's content
- `executor.embed_source(source_id)` - Embed source with chunking
- `executor.embed_insight(insight_id)` - Embed an insight
- `executor.embed_insight_content(content)` - Generate embedding bytes for content

---

## Junction Table Reference

| Relationship | Junction Table | Columns |
|--------------|---------------|---------|
| Source ↔ Notebook | `source_notebook` | `source_id`, `notebook_id` |
| Note ↔ Notebook | `note_notebook` | `note_id`, `notebook_id` |
| ChatSession ↔ Notebook/Source | `chat_session_reference` | `chat_session_id`, `notebook_id`, `source_id` |

---

## Adding New Tables

1. **Create migration file**: `sqlite_migrations/NNN_description.sql`

```sql
-- Migration NNN: Add new_table
CREATE TABLE IF NOT EXISTS new_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    data TEXT,  -- JSON field
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_new_table_name ON new_table(name);
```

2. **If relationship needed, add junction table**:

```sql
CREATE TABLE IF NOT EXISTS new_table_notebook (
    new_table_id INTEGER NOT NULL,
    notebook_id INTEGER NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (new_table_id, notebook_id),
    FOREIGN KEY (new_table_id) REFERENCES new_table(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE
);
```

3. **If FTS needed, add virtual table and triggers**:

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS new_table_fts USING fts5(
    name,
    data,
    content='new_table',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- Sync triggers
CREATE TRIGGER IF NOT EXISTS new_table_fts_insert AFTER INSERT ON new_table BEGIN
    INSERT INTO new_table_fts(rowid, name, data) VALUES (new.id, new.name, new.data);
END;
-- ... delete and update triggers
```

4. **Update repository if field mappings needed**:

```python
# In _map_fields_for_table()
if table == "new_table" and "some_field" in result:
    result["some_column"] = result.pop("some_field")
```

5. **Add repo functions for complex queries**:

```python
# In sqlite_repository.py
async def repo_get_new_tables_for_notebook(notebook_id: str) -> List[Dict]:
    _, id_value = parse_id(notebook_id)
    result = await repo_query(
        """
        SELECT nt.* FROM new_table nt
        JOIN new_table_notebook ntn ON nt.id = ntn.new_table_id
        WHERE ntn.notebook_id = ?
        ORDER BY nt.updated DESC
        """,
        (id_value,),
    )
    return [normalize_result(row, "new_table") for row in result]
```

6. **Export from `__init__.py`** and add SurrealDB wrapper if needed.

---

## Checklist: Translating Upstream Feature

When pulling in new SurrealDB code from main branch:

- [ ] **New table?** → Add migration SQL in `sqlite_migrations/NNN_*.sql`
- [ ] **New relationship?** → Add junction table + update `repo_get_related()` etc.
- [ ] **New query in router?** → Use existing repo functions, don't add `is_sqlite()` check
- [ ] **New field with different column name?** → Update `_map_fields_for_table()`
- [ ] **New embedding field?** → Use BLOB storage with `serialize_embedding()`
- [ ] **New singleton config?** → Add table with `id INTEGER PRIMARY KEY CHECK (id = 1)`
- [ ] **New background job?** → Use executor pattern or `submit_command()`
- [ ] **Test both backends**: `DATABASE_BACKEND=sqlite uv run pytest tests/`

---

## Common Mistakes to Avoid

### ❌ Adding is_sqlite() to domain models

```python
# BAD - in notebook.py
async def get_sources(self):
    if is_sqlite():
        return await sqlite_query(...)
    else:
        return await surreal_query(...)
```

### ✅ Use unified repo function

```python
# GOOD - in notebook.py
async def get_sources(self):
    return await repo_get_related("notebook", self.id, "reference", "source")
```

### ❌ Hardcoding SQL in routers

```python
# BAD - in routers/notebooks.py
if is_sqlite():
    sources = await repo_query("SELECT s.* FROM source s JOIN...")
```

### ✅ Call repo helper function

```python
# GOOD - in routers/notebooks.py
sources = await repo_get_sources_for_notebook(notebook_id)
```

### ❌ Manual ID formatting scattered around

```python
# BAD - everywhere
formatted_id = f"{table}:{row['id']}" if is_sqlite() else str(row['id'])
```

### ✅ Use normalize_result or format_id

```python
# GOOD - once in repository layer
return normalize_result(row, table)
```

---

## Testing

Run tests with SQLite backend:

```bash
DATABASE_BACKEND=sqlite uv run pytest tests/
```

Run tests with SurrealDB backend (requires running SurrealDB):

```bash
DATABASE_BACKEND=surrealdb uv run pytest tests/
```

Manual verification checklist:
1. Create notebook
2. Add source from URL
3. Wait for embedding to complete
4. Generate insights (transformation)
5. Create note
6. Run semantic search
7. Start chat session
8. Create podcast episode (if applicable)
