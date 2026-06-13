"""
Async migration system for SurrealDB using the official Python client.

Migrations are auto-discovered from the ``migrations/`` directory and tracked
individually by id in the ``_sbl_migrations`` table (an "applied set" model).

A migration file is named ``<id>[_<description>].surrealql`` where ``<id>`` is an
integer. Two id styles are supported and interleave correctly because they sort
numerically:

* **Legacy sequential**: ``1.surrealql`` … ``15.surrealql`` (the original core
  migrations; already applied on existing databases).
* **Unix timestamp** (preferred for new migrations): ``1718305200_add_x.surrealql``
  — created with ``date +%s`` (or ``make migration name=add_x``). Timestamps are
  far larger than the legacy ids, so they always run after them, and two authors
  never collide on "the next number".

Each migration may have an optional rollback file with the same stem plus
``_down`` (e.g. ``1718305200_add_x_down.surrealql``).
"""

from pathlib import Path
from typing import List, Optional, Set

from loguru import logger

from .repository import db_connection, repo_query

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _clean_sql(raw_content: str) -> str:
    """Strip comments/blank lines and collapse to a single executable string."""
    lines = []
    for line in raw_content.split("\n"):
        line = line.strip()
        if line and not line.startswith("--"):
            lines.append(line)
    return " ".join(lines)


class AsyncMigration:
    """A single migration: an integer id, its up SQL, and optional down SQL."""

    def __init__(self, migration_id: int, sql: str, down_sql: Optional[str] = None):
        self.id = migration_id
        self.sql = sql
        self.down_sql = down_sql

    @classmethod
    def from_files(cls, migration_id: int, up_path: Path, down_path: Optional[Path]):
        sql = _clean_sql(up_path.read_text(encoding="utf-8"))
        down_sql = (
            _clean_sql(down_path.read_text(encoding="utf-8")) if down_path else None
        )
        return cls(migration_id, sql, down_sql)

    async def apply(self) -> None:
        """Run the up SQL and record this id as applied."""
        try:
            async with db_connection() as connection:
                await connection.query(self.sql)
            await mark_applied(self.id)
        except Exception as e:
            logger.error(f"Migration {self.id} failed: {str(e)}")
            raise

    async def revert(self) -> None:
        """Run the down SQL and remove this id from the applied set."""
        if not self.down_sql:
            raise ValueError(f"Migration {self.id} has no rollback (_down) file")
        try:
            async with db_connection() as connection:
                await connection.query(self.down_sql)
            await mark_reverted(self.id)
        except Exception as e:
            logger.error(f"Rollback of migration {self.id} failed: {str(e)}")
            raise


def discover_migrations() -> List[AsyncMigration]:
    """Find and order all migrations on disk by their integer id (ascending)."""
    by_stem = {p.stem: p for p in MIGRATIONS_DIR.glob("*.surrealql")}
    migrations: List[AsyncMigration] = []
    for stem, path in by_stem.items():
        if stem.endswith("_down"):
            continue
        prefix = stem.split("_", 1)[0]
        try:
            migration_id = int(prefix)
        except ValueError:
            logger.warning(
                f"Skipping migration file with non-integer id prefix: {path.name}"
            )
            continue
        down_path = by_stem.get(f"{stem}_down")
        migrations.append(AsyncMigration.from_files(migration_id, path, down_path))

    migrations.sort(key=lambda m: m.id)

    seen: Set[int] = set()
    for m in migrations:
        if m.id in seen:
            raise ValueError(f"Duplicate migration id detected: {m.id}")
        seen.add(m.id)
    return migrations


class AsyncMigrationManager:
    """Applies pending migrations and reports migration state."""

    def __init__(self):
        self.migrations = discover_migrations()

    async def get_current_version(self) -> int:
        """Highest applied migration id (0 if none). For display/logging."""
        return await get_latest_version()

    async def get_pending(self) -> List[AsyncMigration]:
        applied = await get_applied_ids()
        return [m for m in self.migrations if m.id not in applied]

    async def needs_migration(self) -> bool:
        return len(await self.get_pending()) > 0

    async def run_migration_up(self) -> None:
        """Apply every discovered migration whose id is not yet applied."""
        pending = await self.get_pending()
        if not pending:
            logger.info("Database is already at the latest version")
            return

        logger.info(
            f"Applying {len(pending)} pending migration(s): "
            f"{[m.id for m in pending]}"
        )
        for migration in pending:
            logger.info(f"Running migration {migration.id}")
            await migration.apply()
        logger.info(
            f"Migration successful. Current version: {await self.get_current_version()}"
        )

    async def run_migration_down(self) -> None:
        """Roll back the most recently applied migration that has a _down file."""
        applied = await get_applied_ids()
        revertible = sorted(
            (m for m in self.migrations if m.id in applied and m.down_sql),
            key=lambda m: m.id,
        )
        if not revertible:
            logger.info("No revertible migration to roll back")
            return
        target = revertible[-1]
        logger.info(f"Rolling back migration {target.id}")
        await target.revert()


# --- applied-set tracking (_sbl_migrations) ----------------------------------
async def get_all_versions() -> List[dict]:
    """All applied migration records (id stored in the `version` field)."""
    try:
        return await repo_query("SELECT * FROM _sbl_migrations ORDER BY version;")
    except Exception:
        # Table doesn't exist yet -> nothing applied.
        return []


async def get_applied_ids() -> Set[int]:
    """Set of migration ids already applied (backward compatible: legacy rows
    store sequential ids 1..N in the same `version` field)."""
    try:
        return {int(row["version"]) for row in await get_all_versions()}
    except Exception:
        return set()


async def get_latest_version() -> int:
    """Highest applied migration id, or 0 if none."""
    applied = await get_applied_ids()
    return max(applied) if applied else 0


async def mark_applied(migration_id: int) -> None:
    await repo_query(
        "CREATE type::thing('_sbl_migrations', $id) "
        "SET version = $id, applied_at = time::now();",
        {"id": migration_id},
    )


async def mark_reverted(migration_id: int) -> None:
    await repo_query(
        "DELETE type::thing('_sbl_migrations', $id);",
        {"id": migration_id},
    )
