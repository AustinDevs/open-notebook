"""
Background worker for processing SQLite command queue jobs.

This worker polls the command_queue table for pending jobs and executes
them using registered command handlers. It runs as a background task
within the API process when DATABASE_BACKEND=sqlite.

Usage:
    # Start worker in background (from api/main.py lifespan)
    worker_task = asyncio.create_task(start_worker())

    # Or run standalone
    asyncio.run(start_worker())
"""

import asyncio
import inspect
import traceback
from typing import Any, Dict, Optional

from loguru import logger

from open_notebook.database.command_queue import (
    CommandJob,
    acquire_next_job,
    get_command_handler,
    get_queue_stats,
    recover_stuck_jobs,
    update_job_completed,
    update_job_failed,
)

# Worker configuration
DEFAULT_POLL_INTERVAL = 2  # seconds
DEFAULT_RECOVERY_INTERVAL = 300  # 5 minutes
DEFAULT_STUCK_TIMEOUT = 30  # minutes

# Global worker state
_worker_running = False
_worker_task: Optional[asyncio.Task] = None


async def execute_job(job: CommandJob) -> Dict[str, Any]:
    """
    Execute a command job using its registered handler.

    Args:
        job: The CommandJob to execute

    Returns:
        Dict containing the result from the handler

    Raises:
        ValueError: If no handler is registered for the command
        Exception: Any exception raised by the handler
    """
    handler = get_command_handler(job.namespace, job.command_name)

    if not handler:
        raise ValueError(
            f"No handler registered for command: {job.namespace}.{job.command_name}"
        )

    logger.info(
        f"Executing job {job.job_id}: {job.namespace}.{job.command_name}"
    )

    # Construct input from args
    # Get the handler's input type from its signature
    sig = inspect.signature(handler)
    params = list(sig.parameters.values())

    if not params:
        raise ValueError(f"Handler {job.namespace}.{job.command_name} has no parameters")

    # First parameter should be the input type
    input_param = params[0]
    input_type = input_param.annotation

    # Create input instance
    if input_type and input_type != inspect.Parameter.empty:
        # Pydantic model or dataclass
        try:
            # Add execution_context with command_id
            args_with_context = dict(job.args)

            # Check if input type has execution_context field (Pydantic model)
            if hasattr(input_type, "model_fields"):
                # Pydantic v2
                if "execution_context" in input_type.model_fields:
                    # Create execution context with command_id
                    from open_notebook.database.command_queue import ExecutionContext
                    args_with_context["execution_context"] = ExecutionContext(
                        command_id=job.job_id
                    )

            input_data = input_type(**args_with_context)
        except Exception as e:
            logger.error(f"Failed to construct input for {job.command_name}: {e}")
            raise ValueError(f"Invalid input arguments: {e}")
    else:
        input_data = job.args

    # Execute the handler
    if asyncio.iscoroutinefunction(handler):
        result = await handler(input_data)
    else:
        # Sync handler - run in thread pool to avoid blocking
        result = await asyncio.to_thread(handler, input_data)

    # Convert result to dict
    if hasattr(result, "model_dump"):
        return result.model_dump()
    elif hasattr(result, "__dict__"):
        return result.__dict__
    elif isinstance(result, dict):
        return result
    else:
        return {"result": result}


async def process_one_job() -> bool:
    """
    Attempt to acquire and process one job from the queue.

    Returns:
        True if a job was processed, False if queue was empty
    """
    job = await acquire_next_job()

    if not job:
        return False

    try:
        result = await execute_job(job)
        await update_job_completed(job.job_id, result)
        logger.info(f"Job {job.job_id} completed successfully")
        return True

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        await update_job_failed(job.job_id, error_msg)
        logger.error(f"Job {job.job_id} failed: {e}")
        return True  # Job was processed (even if failed)


async def worker_loop(
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    recovery_interval: int = DEFAULT_RECOVERY_INTERVAL,
    stuck_timeout: int = DEFAULT_STUCK_TIMEOUT,
):
    """
    Main worker loop that continuously processes jobs from the queue.

    Args:
        poll_interval: Seconds to wait between polling when queue is empty
        recovery_interval: Seconds between stuck job recovery checks
        stuck_timeout: Minutes after which a processing job is considered stuck
    """
    global _worker_running
    _worker_running = True

    logger.info(
        f"Command worker started (poll={poll_interval}s, "
        f"recovery={recovery_interval}s, stuck_timeout={stuck_timeout}min)"
    )

    last_recovery = 0
    iteration = 0

    try:
        while _worker_running:
            iteration += 1

            # Periodic stuck job recovery
            if iteration * poll_interval >= last_recovery + recovery_interval:
                try:
                    recovered = await recover_stuck_jobs(stuck_timeout)
                    if recovered > 0:
                        logger.info(f"Recovered {recovered} stuck jobs")
                except Exception as e:
                    logger.error(f"Failed to recover stuck jobs: {e}")
                last_recovery = iteration * poll_interval

            # Process jobs
            try:
                job_processed = await process_one_job()

                if job_processed:
                    # More jobs might be waiting, don't sleep
                    continue
                else:
                    # Queue empty, wait before polling again
                    await asyncio.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Worker error: {e}")
                logger.exception(e)
                # Back off on errors
                await asyncio.sleep(poll_interval * 2)

    except asyncio.CancelledError:
        logger.info("Command worker cancelled")
        raise
    finally:
        _worker_running = False
        logger.info("Command worker stopped")


async def start_worker(
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    recovery_interval: int = DEFAULT_RECOVERY_INTERVAL,
    stuck_timeout: int = DEFAULT_STUCK_TIMEOUT,
) -> asyncio.Task:
    """
    Start the command worker as a background task.

    Returns:
        The asyncio Task running the worker
    """
    global _worker_task

    if _worker_task and not _worker_task.done():
        logger.warning("Worker already running")
        return _worker_task

    # Import commands to register handlers
    try:
        import commands  # noqa: F401

        logger.info("Command handlers loaded")
    except ImportError as e:
        logger.warning(f"Could not import commands module: {e}")

    # Initialize schema before starting
    from open_notebook.database.command_queue import initialize_command_queue_schema

    await initialize_command_queue_schema()

    # Recover any stuck jobs from previous runs
    try:
        recovered = await recover_stuck_jobs(stuck_timeout)
        if recovered > 0:
            logger.info(f"Recovered {recovered} stuck jobs from previous run")
    except Exception as e:
        logger.warning(f"Could not recover stuck jobs: {e}")

    # Log queue stats
    try:
        stats = await get_queue_stats()
        logger.info(f"Queue stats: {stats}")
    except Exception as e:
        logger.warning(f"Could not get queue stats: {e}")

    # Start worker loop
    _worker_task = asyncio.create_task(
        worker_loop(poll_interval, recovery_interval, stuck_timeout)
    )

    return _worker_task


async def stop_worker():
    """Stop the running worker gracefully."""
    global _worker_running, _worker_task

    if not _worker_task:
        return

    _worker_running = False

    if not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass

    _worker_task = None
    logger.info("Worker stopped")


def is_worker_running() -> bool:
    """Check if the worker is currently running."""
    return _worker_running and _worker_task is not None and not _worker_task.done()


# Export public API
__all__ = [
    "start_worker",
    "stop_worker",
    "is_worker_running",
    "worker_loop",
    "execute_job",
    "process_one_job",
]
