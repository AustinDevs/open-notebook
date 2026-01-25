"""
SQLite-based command queue for background job processing.

This module provides a command queue implementation that works with SQLite,
enabling background job processing (podcasts, embeddings) without requiring
SurrealDB. When DATABASE_BACKEND=surrealdb, it delegates to surreal_commands.

API Compatibility:
- submit_command(namespace, command_name, args) -> str (job_id)
- get_command_status(job_id) -> Optional[CommandStatus]
- command decorator for registering handlers
"""

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from loguru import logger

from open_notebook.database import is_sqlite

# Command registry for SQLite backend
# Maps (app_name, command_name) -> handler function
COMMAND_REGISTRY: Dict[Tuple[str, str], Callable] = {}


@dataclass
class CommandStatus:
    """Status information for a command job (compatible with surreal_commands)."""

    job_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    progress: Optional[Dict[str, Any]] = None


@dataclass
class CommandJob:
    """Internal representation of a command job."""

    id: int
    job_id: str
    namespace: str
    command_name: str
    args: Dict[str, Any]
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


# Schema for the command_queue table
COMMAND_QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS command_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    namespace TEXT NOT NULL,
    command_name TEXT NOT NULL,
    args TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_command_queue_status ON command_queue (status, created_at);
CREATE INDEX IF NOT EXISTS idx_command_queue_job_id ON command_queue (job_id);
"""


async def initialize_command_queue_schema():
    """
    Initialize the command_queue table schema.
    Should be called during database initialization.
    """
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        for statement in COMMAND_QUEUE_SCHEMA.strip().split(";"):
            statement = statement.strip()
            if statement:
                try:
                    await db.execute(statement)
                except Exception as e:
                    # Index might already exist
                    if "already exists" not in str(e).lower():
                        logger.debug(f"Schema statement note: {e}")
        await db.commit()
        logger.debug("Command queue schema initialized")


# =============================================================================
# SQLite Implementation
# =============================================================================


async def sqlite_submit_command(
    namespace: str, command_name: str, args: Dict[str, Any]
) -> str:
    """
    Submit a command job to the SQLite queue.

    Args:
        namespace: Application namespace (e.g., "open_notebook")
        command_name: Name of the command to execute
        args: Arguments to pass to the command handler

    Returns:
        str: The job_id (UUID) for tracking the job
    """
    from open_notebook.database.sqlite_repository import db_connection

    job_id = str(uuid.uuid4())
    args_json = json.dumps(args)

    async with db_connection() as db:
        await db.execute(
            """
            INSERT INTO command_queue (job_id, namespace, command_name, args, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', datetime('now'))
            """,
            (job_id, namespace, command_name, args_json),
        )
        await db.commit()

    logger.debug(f"Submitted command {namespace}.{command_name} with job_id={job_id}")
    return job_id


async def sqlite_get_command_status(job_id: str) -> Optional[CommandStatus]:
    """
    Get the status of a command job from SQLite.

    Args:
        job_id: The UUID of the job

    Returns:
        CommandStatus if found, None otherwise
    """
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        cursor = await db.execute(
            """
            SELECT job_id, status, result, error_message, created_at, completed_at
            FROM command_queue
            WHERE job_id = ?
            """,
            (job_id,),
        )
        row = await cursor.fetchone()

        if not row:
            return None

        result = None
        if row[2]:  # result column
            try:
                result = json.loads(row[2])
            except json.JSONDecodeError:
                result = {"raw": row[2]}

        created = None
        if row[4]:
            try:
                created = datetime.fromisoformat(row[4])
            except (ValueError, TypeError):
                pass

        updated = None
        if row[5]:  # completed_at
            try:
                updated = datetime.fromisoformat(row[5])
            except (ValueError, TypeError):
                pass

        return CommandStatus(
            job_id=row[0],
            status=row[1],
            result=result,
            error_message=row[3],
            created=created,
            updated=updated,
        )


async def acquire_next_job() -> Optional[CommandJob]:
    """
    Atomically acquire the next pending job for processing.

    Uses BEGIN IMMEDIATE to prevent race conditions when multiple
    workers might be running (though typically single-instance for SQLite).

    Returns:
        CommandJob if a job was acquired, None if queue is empty
    """
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        try:
            # Use BEGIN IMMEDIATE for atomic read-modify-write
            await db.execute("BEGIN IMMEDIATE")

            cursor = await db.execute(
                """
                SELECT id, job_id, namespace, command_name, args, status, created_at
                FROM command_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()

            if not row:
                await db.execute("ROLLBACK")
                return None

            job_id = row[0]

            # Update status to processing
            await db.execute(
                """
                UPDATE command_queue
                SET status = 'processing', started_at = datetime('now')
                WHERE id = ?
                """,
                (job_id,),
            )
            await db.commit()

            args = {}
            try:
                args = json.loads(row[4])
            except (json.JSONDecodeError, TypeError):
                args = {}

            created_at = datetime.now(timezone.utc)
            try:
                created_at = datetime.fromisoformat(row[6])
            except (ValueError, TypeError):
                pass

            return CommandJob(
                id=row[0],
                job_id=row[1],
                namespace=row[2],
                command_name=row[3],
                args=args,
                status="processing",
                created_at=created_at,
            )

        except Exception as e:
            try:
                await db.execute("ROLLBACK")
            except Exception:
                pass
            logger.error(f"Failed to acquire job: {e}")
            raise


async def update_job_completed(job_id: str, result: Dict[str, Any]):
    """Mark a job as completed with its result."""
    from open_notebook.database.sqlite_repository import db_connection

    result_json = json.dumps(result)

    async with db_connection() as db:
        await db.execute(
            """
            UPDATE command_queue
            SET status = 'completed', completed_at = datetime('now'), result = ?
            WHERE job_id = ?
            """,
            (result_json, job_id),
        )
        await db.commit()


async def update_job_failed(job_id: str, error_message: str):
    """Mark a job as failed with an error message."""
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        await db.execute(
            """
            UPDATE command_queue
            SET status = 'failed', completed_at = datetime('now'), error_message = ?
            WHERE job_id = ?
            """,
            (error_message, job_id),
        )
        await db.commit()


async def recover_stuck_jobs(timeout_minutes: int = 30):
    """
    Recover jobs stuck in 'processing' state (e.g., worker crash).

    Jobs that have been processing for longer than timeout_minutes
    are reset to 'pending' status.

    Args:
        timeout_minutes: Minutes after which a processing job is considered stuck
    """
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        cursor = await db.execute(
            """
            UPDATE command_queue
            SET status = 'pending', started_at = NULL
            WHERE status = 'processing'
              AND started_at < datetime('now', ?)
            RETURNING job_id
            """,
            (f"-{timeout_minutes} minutes",),
        )
        recovered = await cursor.fetchall()
        await db.commit()

        if recovered:
            job_ids = [row[0] for row in recovered]
            logger.warning(f"Recovered {len(recovered)} stuck jobs: {job_ids}")

        return len(recovered) if recovered else 0


async def get_queue_stats() -> Dict[str, int]:
    """Get statistics about the command queue."""
    from open_notebook.database.sqlite_repository import db_connection

    async with db_connection() as db:
        cursor = await db.execute(
            """
            SELECT status, COUNT(*) as count
            FROM command_queue
            GROUP BY status
            """
        )
        rows = await cursor.fetchall()

        stats = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
        for row in rows:
            stats[row[0]] = row[1]

        return stats


# =============================================================================
# Command Decorator (Registry)
# =============================================================================


from pydantic import BaseModel


class ExecutionContext(BaseModel):
    """Execution context containing job metadata."""

    command_id: str


class CommandInput(BaseModel):
    """Base class for command input (compatible with surreal_commands)."""

    execution_context: Optional[ExecutionContext] = None

    class Config:
        arbitrary_types_allowed = True


class CommandOutput(BaseModel):
    """Base class for command output (compatible with surreal_commands)."""

    class Config:
        arbitrary_types_allowed = True


def command(
    name: str,
    app: str = "open_notebook",
    retry: Optional[Dict[str, Any]] = None,
):
    """
    Decorator to register a command handler.

    For SQLite backend, registers the handler in the local registry.
    For SurrealDB backend, delegates to surreal_commands.

    Args:
        name: Command name (e.g., "generate_podcast")
        app: Application namespace (e.g., "open_notebook")
        retry: Retry configuration (ignored for SQLite, used by surreal_commands)

    Usage:
        @command("my_command", app="open_notebook")
        async def my_command_handler(input_data: MyInput) -> MyOutput:
            ...
    """

    def decorator(func: Callable) -> Callable:
        # Always register in local registry for SQLite support
        COMMAND_REGISTRY[(app, name)] = func
        logger.debug(f"Registered command handler: {app}.{name}")

        # If using SurrealDB, also register with surreal_commands
        if not is_sqlite():
            try:
                from surreal_commands import command as surreal_command

                # Apply surreal_commands decorator
                return surreal_command(name, app=app, retry=retry)(func)
            except ImportError:
                logger.warning(
                    "surreal_commands not available, using local registry only"
                )

        return func

    return decorator


def get_command_handler(
    namespace: str, command_name: str
) -> Optional[Callable]:
    """Get a registered command handler."""
    return COMMAND_REGISTRY.get((namespace, command_name))


# =============================================================================
# Public API (Compatibility Layer)
# =============================================================================


def submit_command(namespace: str, command_name: str, args: Dict[str, Any]) -> str:
    """
    Submit a command for background execution.

    This is the main entry point for submitting jobs. It routes to either
    the SQLite implementation or surreal_commands based on DATABASE_BACKEND.

    Args:
        namespace: Application namespace (e.g., "open_notebook")
        command_name: Name of the command to execute
        args: Arguments to pass to the command handler

    Returns:
        str: Job ID for tracking the job status

    Example:
        job_id = submit_command("open_notebook", "generate_podcast", {"content": "..."})
    """
    if is_sqlite():
        # SQLite: Use our implementation
        # Since this needs to be sync for compatibility, use asyncio.run
        # if no event loop, or create_task if in async context
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context - need to handle differently
            # Create a future that will be awaited
            future = asyncio.ensure_future(
                sqlite_submit_command(namespace, command_name, args)
            )
            # For sync callers in async context, they need to await separately
            # Return a blocking call
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                return executor.submit(
                    asyncio.run,
                    sqlite_submit_command(namespace, command_name, args),
                ).result()
        except RuntimeError:
            # No running event loop, safe to use asyncio.run
            return asyncio.run(sqlite_submit_command(namespace, command_name, args))
    else:
        # SurrealDB: Delegate to surreal_commands
        from surreal_commands import submit_command as surreal_submit

        result = surreal_submit(namespace, command_name, args)
        return str(result)


async def get_command_status(job_id: str) -> Optional[CommandStatus]:
    """
    Get the status of a command job.

    Args:
        job_id: The job ID returned from submit_command

    Returns:
        CommandStatus with job details, or None if not found
    """
    if is_sqlite():
        return await sqlite_get_command_status(job_id)
    else:
        from surreal_commands import get_command_status as surreal_get_status

        return await surreal_get_status(job_id)


@dataclass
class CommandResult:
    """Result of a synchronous command execution."""

    success: bool
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def is_success(self) -> bool:
        """Check if the command was successful."""
        return self.success


async def _execute_command_async(
    namespace: str, command_name: str, args: Dict[str, Any]
) -> CommandResult:
    """Execute a command synchronously (internal async implementation)."""
    import inspect
    import traceback

    handler = get_command_handler(namespace, command_name)
    if not handler:
        return CommandResult(
            success=False,
            error_message=f"No handler registered for command: {namespace}.{command_name}",
        )

    try:
        # Get the handler's input type from its signature
        sig = inspect.signature(handler)
        params = list(sig.parameters.values())

        if not params:
            return CommandResult(
                success=False,
                error_message=f"Handler {namespace}.{command_name} has no parameters",
            )

        # First parameter should be the input type
        input_param = params[0]
        input_type = input_param.annotation

        # Create input instance
        if input_type and input_type != inspect.Parameter.empty:
            input_data = input_type(**args)
        else:
            input_data = args

        # Execute the handler
        if asyncio.iscoroutinefunction(handler):
            result = await handler(input_data)
        else:
            result = await asyncio.to_thread(handler, input_data)

        # Convert result to dict
        if hasattr(result, "model_dump"):
            result_dict = result.model_dump()
        elif hasattr(result, "__dict__"):
            result_dict = result.__dict__
        elif isinstance(result, dict):
            result_dict = result
        else:
            result_dict = {"result": result}

        # Check for success flag in result
        success = result_dict.get("success", True)
        error_msg = result_dict.get("error_message") if not success else None

        return CommandResult(
            success=success,
            result=result_dict,
            error_message=error_msg,
        )

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return CommandResult(
            success=False,
            error_message=error_msg,
        )


def execute_command_sync(
    namespace: str,
    command_name: str,
    args: Dict[str, Any],
    timeout: int = 300,
) -> CommandResult:
    """
    Execute a command synchronously (blocking).

    This runs the command directly without queuing, waiting for completion.
    Useful for operations that must complete before continuing.

    Args:
        namespace: Application namespace (e.g., "open_notebook")
        command_name: Name of the command to execute
        args: Arguments to pass to the command handler
        timeout: Maximum seconds to wait (default: 300)

    Returns:
        CommandResult with success status and result/error

    Example:
        result = execute_command_sync("open_notebook", "process_source", {...})
        if result.is_success():
            print("Success:", result.result)
        else:
            print("Failed:", result.error_message)
    """
    if is_sqlite():
        # SQLite: Execute directly using our local registry
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context - use thread pool
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    asyncio.wait_for(
                        _execute_command_async(namespace, command_name, args),
                        timeout=timeout,
                    ),
                )
                return future.result(timeout=timeout + 5)
        except RuntimeError:
            # No running event loop, safe to use asyncio.run
            return asyncio.run(
                asyncio.wait_for(
                    _execute_command_async(namespace, command_name, args),
                    timeout=timeout,
                )
            )
        except asyncio.TimeoutError:
            return CommandResult(
                success=False,
                error_message=f"Command timed out after {timeout} seconds",
            )
    else:
        # SurrealDB: Delegate to surreal_commands
        from surreal_commands import execute_command_sync as surreal_execute_sync

        return surreal_execute_sync(namespace, command_name, args, timeout=timeout)


# Export public API
__all__ = [
    # Core functions
    "submit_command",
    "get_command_status",
    "execute_command_sync",
    "command",
    # Base classes
    "CommandInput",
    "CommandOutput",
    "CommandStatus",
    "CommandResult",
    "ExecutionContext",
    # SQLite-specific (for worker)
    "acquire_next_job",
    "update_job_completed",
    "update_job_failed",
    "recover_stuck_jobs",
    "get_queue_stats",
    "get_command_handler",
    "initialize_command_queue_schema",
    # Registry
    "COMMAND_REGISTRY",
]
