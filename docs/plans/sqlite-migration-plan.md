# Database Backend Abstraction: SurrealDB + Optional SQLite

## Executive Summary

This document outlines the implementation of a configurable database backend for Open Notebook, supporting both SurrealDB (default) and SQLite as storage options. Users can select their preferred backend via the `DATABASE_BACKEND` environment variable.

**Key Design Decision**: SurrealDB remains the default and fully supported backend. SQLite is provided as an alternative for users who prefer a simpler, file-based deployment without a separate database service.

**Implementation Status**: ✅ Completed

---

## Table of Contents

1. [Design Goals](#1-design-goals)
2. [Backend Comparison](#2-backend-comparison)
3. [Configuration](#3-configuration)
4. [Architecture](#4-architecture)
5. [Schema Design](#5-schema-design)
6. [Implementation Details](#6-implementation-details)
7. [Deployment Options](#7-deployment-options)
8. [Testing](#8-testing)

---

## 1. Design Goals

### Primary Goals

| Goal | Description |
|------|-------------|
| **Backward Compatibility** | SurrealDB remains the default; existing deployments work unchanged |
| **User Choice** | Provide SQLite as a simpler alternative for certain use cases |
| **Code Maintainability** | Single codebase with abstraction layer, not forked implementations |
| **Feature Parity** | Both backends support all application features |

### When to Use Each Backend

| Use Case | Recommended Backend |
|----------|-------------------|
| Production deployment with scaling needs | SurrealDB |
| Development and testing | Either (SQLite simpler) |
| Single-user local deployment | SQLite |
| Edge/embedded deployments | SQLite |
| Docker Compose multi-service | SurrealDB |
| Single-container deployment | SQLite |

---

## 2. Backend Comparison

### Feature Comparison

| Feature | SurrealDB | SQLite |
|---------|-----------|--------|
| **Vector Search** | Native built-in | `sqlite-vec` extension |
| **Full-Text Search** | Custom BM25 analyzer | FTS5 (different tokenization) |
| **Graph Relations** | First-class RELATE syntax | Join tables with foreign keys |
| **Async Driver** | Native async | `aiosqlite` wrapper |
| **Deployment** | Separate service | Single file |
| **Scaling** | Horizontal | Vertical only |
| **RecordID Format** | Native `table:id` | Emulated `table:id` string |

### Trade-offs

**SurrealDB Advantages:**
- Native graph database features
- Built-in vector search (no extension loading)
- Better concurrent write handling
- Designed for distributed deployments

**SQLite Advantages:**
- No separate database service required
- Single-file storage (easy backup/restore)
- Widely supported and battle-tested
- Lower resource usage
- Simpler local development

---

## 3. Configuration

### Environment Variable

```bash
# Default: SurrealDB
DATABASE_BACKEND=surrealdb

# Alternative: SQLite
DATABASE_BACKEND=sqlite
```

### SurrealDB Configuration (Default)

```bash
SURREAL_URL=ws://localhost:8000/rpc
SURREAL_USER=root
SURREAL_PASSWORD=root
SURREAL_NAMESPACE=open_notebook
SURREAL_DATABASE=open_notebook
```

### SQLite Configuration

```bash
DATABASE_BACKEND=sqlite
SQLITE_DB_PATH=/data/sqlite-db/open_notebook.db
```

---

## 4. Architecture

### Abstraction Layer

The database abstraction is implemented in `open_notebook/database/__init__.py`:

```python
import os

DATABASE_BACKEND = os.getenv("DATABASE_BACKEND", "surrealdb").lower()

if DATABASE_BACKEND == "sqlite":
    from open_notebook.database.sqlite_repository import (
        repo_query, repo_create, repo_update, repo_delete,
        repo_relate, repo_insert, repo_upsert,
        serialize_embedding, deserialize_embedding,
    )
    from open_notebook.database.sqlite_search import (
        vector_search, text_search,
    )
    BACKEND_NAME = "sqlite"

    def ensure_record_id(value):
        return str(value) if value else None
else:
    from open_notebook.database.repository import (
        repo_query, repo_create, repo_update, repo_delete,
        repo_relate, repo_insert, repo_upsert, ensure_record_id,
    )
    BACKEND_NAME = "surrealdb"

    def serialize_embedding(embedding):
        return embedding  # SurrealDB stores arrays natively

    def deserialize_embedding(data):
        return data

def is_sqlite() -> bool:
    return BACKEND_NAME == "sqlite"

def is_surrealdb() -> bool:
    return BACKEND_NAME == "surrealdb"
```

### Domain Layer Pattern

Domain models use conditional logic for backend-specific queries:

```python
from open_notebook.database import is_sqlite, repo_query, ensure_record_id

class Source(ObjectModel):
    async def get_insights(self) -> List[SourceInsight]:
        if is_sqlite():
            source_id = int(self.id.split(":")[1])
            result = await repo_query(
                "SELECT * FROM source_insight WHERE source_id = ?",
                (source_id,)
            )
            # Format results for SQLite
        else:
            result = await repo_query(
                "SELECT * FROM source_insight WHERE source=$id",
                {"id": ensure_record_id(self.id)}
            )
            # SurrealDB returns properly formatted results
```

---

## 5. Schema Design

### SQLite Schema

The SQLite schema is defined in `open_notebook/database/sqlite_migrations/`:

#### Core Tables

```sql
-- Notebooks
CREATE TABLE notebook (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    archived INTEGER DEFAULT 0,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Sources
CREATE TABLE source (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT,
    url TEXT,
    title TEXT,
    topics TEXT,  -- JSON array
    full_text TEXT,
    command_id TEXT,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Notes
CREATE TABLE note (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    note_type TEXT CHECK(note_type IN ('human', 'ai')),
    content TEXT,
    embedding BLOB,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Source Embeddings (chunks)
CREATE TABLE source_embedding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    chunk_order INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding BLOB,
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE
);

-- Source Insights
CREATE TABLE source_insight (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    insight_type TEXT NOT NULL,
    content TEXT NOT NULL,
    embedding BLOB,
    created TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE
);
```

#### Relationship Tables (Replacing Graph Edges)

```sql
-- source -> notebook (was: reference relation)
CREATE TABLE source_notebook (
    source_id INTEGER NOT NULL,
    notebook_id INTEGER NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (source_id, notebook_id),
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE
);

-- note -> notebook (was: artifact relation)
CREATE TABLE note_notebook (
    note_id INTEGER NOT NULL,
    notebook_id INTEGER NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (note_id, notebook_id),
    FOREIGN KEY (note_id) REFERENCES note(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE
);

-- chat_session -> notebook/source (was: refers_to relation)
CREATE TABLE chat_session_reference (
    chat_session_id INTEGER NOT NULL,
    notebook_id INTEGER,
    source_id INTEGER,
    created TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (chat_session_id) REFERENCES chat_session(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE,
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE
);
```

#### Full-Text Search (FTS5)

```sql
CREATE VIRTUAL TABLE source_fts USING fts5(
    title, full_text,
    content='source', content_rowid='id',
    tokenize='porter unicode61'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER source_ai AFTER INSERT ON source BEGIN
    INSERT INTO source_fts(rowid, title, full_text)
    VALUES (new.id, new.title, new.full_text);
END;
```

### SurrealDB Schema

SurrealDB schema remains in `open_notebook/database/migrations/*.surql` and uses:
- Native RecordID type (`source:123`)
- RELATE syntax for graph edges
- Built-in vector search functions
- Custom BM25 text search

---

## 6. Implementation Details

### Files Modified

| File | Changes |
|------|---------|
| `open_notebook/database/__init__.py` | **New** - Abstraction layer |
| `open_notebook/database/sqlite_repository.py` | **New** - SQLite repository functions |
| `open_notebook/database/sqlite_migrate.py` | **New** - SQLite migration manager |
| `open_notebook/database/sqlite_search.py` | **New** - Vector and text search |
| `open_notebook/database/sqlite_migrations/*.sql` | **New** - SQLite schema |
| `open_notebook/domain/base.py` | Dual backend support in base classes |
| `open_notebook/domain/notebook.py` | Backend-specific queries |
| `api/main.py` | Conditional migration manager |
| `commands/embedding_commands.py` | Backend-specific embedding storage |
| `pyproject.toml` | Both `surrealdb` and `aiosqlite`/`sqlite-vec` |
| `docker-compose.*.yml` | Support for both backends |

### ID Format Compatibility

Both backends use the `table:id` format externally for API compatibility:

- **SurrealDB**: Native `source:abc123` RecordID
- **SQLite**: Emulated `source:123` string (integer ID with table prefix)

Internal SQLite operations extract the numeric ID:

```python
source_id = int(self.id.split(":")[1]) if ":" in self.id else int(self.id)
```

### Vector Search Implementation

**SurrealDB**: Uses built-in `fn::vector_search()` function

**SQLite**: Pure Python cosine similarity with optional `sqlite-vec` extension:

```python
def cosine_similarity(a: List[float], b: List[float]) -> float:
    dot_product = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    return dot_product / (norm_a * norm_b) if norm_a and norm_b else 0.0
```

---

## 7. Deployment Options

### Option 1: SurrealDB (Default)

```yaml
# docker-compose.full.yml
services:
  surrealdb:
    image: surrealdb/surrealdb:v2
    volumes:
      - ./surreal_data:/mydata
    ports:
      - "8000:8000"
    command: start --log info --user root --pass root rocksdb:/mydata/mydatabase.db

  open_notebook:
    image: lfnovo/open_notebook:v1-latest
    environment:
      - DATABASE_BACKEND=surrealdb  # or omit (default)
    depends_on:
      - surrealdb
```

### Option 2: SQLite

```yaml
# docker-compose.single.yml
services:
  open_notebook_single:
    image: lfnovo/open_notebook:v1-latest-single
    environment:
      - DATABASE_BACKEND=sqlite
      - SQLITE_DB_PATH=/app/data/sqlite-db/open_notebook.db
    volumes:
      - ./notebook_data:/app/data
```

### Option 3: Local Development

```bash
# SurrealDB (default)
export DATABASE_BACKEND=surrealdb
# Start SurrealDB separately
surreal start --user root --pass root file:./surreal_data/mydb.db

# SQLite
export DATABASE_BACKEND=sqlite
export SQLITE_DB_PATH=./data/open_notebook.db
# No separate database process needed
```

---

## 8. Testing

### Running Tests

Tests automatically use the configured backend:

```bash
# Test with SurrealDB (default)
pytest tests/

# Test with SQLite
DATABASE_BACKEND=sqlite pytest tests/
```

### Backend-Specific Test Considerations

- **ID format tests**: Both backends should produce `table:id` formatted IDs
- **Search tests**: Results may differ slightly due to tokenization differences
- **Relationship tests**: Join table queries vs RELATE syntax

---

## Appendix A: Dependencies

Both backends are included in `pyproject.toml`:

```toml
dependencies = [
    # SurrealDB (default backend)
    "surrealdb>=1.0.4",

    # SQLite (alternative backend)
    "aiosqlite>=0.19.0",
    "sqlite-vec>=0.1.0",

    # ... other dependencies
]
```

---

## Appendix B: Migration Between Backends

A data migration script can be used to transfer data between backends:

```bash
# Export from SurrealDB, import to SQLite
python scripts/migrate_surreal_to_sqlite.py

# Export from SQLite, import to SurrealDB
python scripts/migrate_sqlite_to_surreal.py
```

**Note**: These scripts are for data portability and are not required for normal operation.

---

*Document Version: 2.0*
*Updated: January 2026*
*Author: Claude Code Assistant*
