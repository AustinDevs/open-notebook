"""
SQLite migration system for Open Notebook.
Handles schema versioning and migrations.
"""

from pathlib import Path
from typing import List, Optional

import aiosqlite
from loguru import logger

from .sqlite_repository import db_connection, get_database_path

MIGRATIONS_DIR = Path(__file__).parent / "sqlite_migrations"


class SQLiteMigration:
    """Handles individual migration operations."""

    def __init__(self, sql: str, version: int) -> None:
        """Initialize migration with SQL content and version number."""
        self.sql = sql
        self.version = version

    @classmethod
    def from_file(cls, file_path: Path) -> "SQLiteMigration":
        """Create migration from SQL file."""
        # Extract version from filename (e.g., '001_initial.sql' -> 1)
        version = int(file_path.stem.split("_")[0])

        with open(file_path, "r", encoding="utf-8") as file:
            sql = file.read()

        return cls(sql, version)


class SQLiteMigrationManager:
    """
    Main migration manager for SQLite.

    Automatically discovers and runs migrations from the sqlite_migrations directory.
    """

    def __init__(self, migrations_dir: Optional[Path] = None):
        """Initialize migration manager."""
        self.migrations_dir = migrations_dir or MIGRATIONS_DIR
        self._migrations: Optional[List[SQLiteMigration]] = None

    @property
    def migrations(self) -> List[SQLiteMigration]:
        """Lazy-load migrations from the migrations directory."""
        if self._migrations is None:
            self._migrations = self._discover_migrations()
        return self._migrations

    def _discover_migrations(self) -> List[SQLiteMigration]:
        """Discover migration files and load them in order."""
        migrations = []

        if not self.migrations_dir.exists():
            logger.warning(f"Migrations directory not found: {self.migrations_dir}")
            return migrations

        # Find all .sql files
        migration_files = sorted(self.migrations_dir.glob("*.sql"))

        for file_path in migration_files:
            # Skip down migrations
            if "_down" in file_path.stem:
                continue

            try:
                migration = SQLiteMigration.from_file(file_path)
                migrations.append(migration)
                logger.debug(f"Discovered migration {migration.version}: {file_path.name}")
            except Exception as e:
                logger.error(f"Failed to load migration {file_path}: {e}")

        # Sort by version
        migrations.sort(key=lambda m: m.version)
        return migrations

    async def _ensure_migrations_table(self, db: aiosqlite.Connection) -> None:
        """Create the migrations tracking table if it doesn't exist."""
        await db.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()

    async def get_current_version(self) -> int:
        """Get the current database version."""
        try:
            async with db_connection() as db:
                await self._ensure_migrations_table(db)

                cursor = await db.execute(
                    "SELECT MAX(version) FROM _migrations"
                )
                row = await cursor.fetchone()

                if row and row[0] is not None:
                    return row[0]
                return 0
        except Exception as e:
            logger.debug(f"Could not get version (database may be new): {e}")
            return 0

    async def needs_migration(self) -> bool:
        """Check if any migrations need to be run."""
        current_version = await self.get_current_version()
        max_version = max((m.version for m in self.migrations), default=0)
        return current_version < max_version

    async def run_migration_up(self) -> None:
        """Run all pending migrations."""
        current_version = await self.get_current_version()
        logger.info(f"Current database version: {current_version}")

        pending = [m for m in self.migrations if m.version > current_version]

        if not pending:
            logger.info("No pending migrations")
            return

        logger.info(f"Found {len(pending)} pending migration(s)")

        async with db_connection() as db:
            await self._ensure_migrations_table(db)

            for migration in pending:
                logger.info(f"Running migration {migration.version}...")

                try:
                    # Execute the migration SQL
                    await db.executescript(migration.sql)

                    # Record the migration
                    await db.execute(
                        "INSERT INTO _migrations (version) VALUES (?)",
                        (migration.version,),
                    )
                    await db.commit()

                    logger.info(f"Migration {migration.version} completed")

                except Exception as e:
                    logger.error(f"Migration {migration.version} failed: {e}")
                    raise

        new_version = await self.get_current_version()
        logger.info(f"Migration complete. Database is now at version {new_version}")

    async def run_migration_down(self, target_version: int = 0) -> None:
        """
        Run down migrations to revert to a target version.

        Args:
            target_version: The version to revert to (0 = clean slate)
        """
        current_version = await self.get_current_version()

        if current_version <= target_version:
            logger.info(f"Already at version {current_version}, nothing to rollback")
            return

        # Find down migrations
        down_migrations = []
        for version in range(current_version, target_version, -1):
            down_file = self.migrations_dir / f"{version:03d}_down.sql"
            if down_file.exists():
                down_migrations.append((version, down_file))
            else:
                logger.warning(f"No down migration found for version {version}")

        if not down_migrations:
            logger.warning("No down migrations available")
            return

        async with db_connection() as db:
            for version, file_path in down_migrations:
                logger.info(f"Rolling back migration {version}...")

                try:
                    sql = file_path.read_text()
                    await db.executescript(sql)

                    await db.execute(
                        "DELETE FROM _migrations WHERE version = ?",
                        (version,),
                    )
                    await db.commit()

                    logger.info(f"Rolled back migration {version}")

                except Exception as e:
                    logger.error(f"Rollback of migration {version} failed: {e}")
                    raise


# Standalone functions for compatibility with existing code


async def get_latest_version() -> int:
    """Get the latest version from the migrations table."""
    manager = SQLiteMigrationManager()
    return await manager.get_current_version()


async def get_all_versions() -> List[dict]:
    """Get all applied versions from the migrations table."""
    try:
        async with db_connection() as db:
            cursor = await db.execute(
                "SELECT version, applied_at FROM _migrations ORDER BY version"
            )
            rows = await cursor.fetchall()
            return [{"version": row[0], "applied_at": row[1]} for row in rows]
    except Exception:
        return []
