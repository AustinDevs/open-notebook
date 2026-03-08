import asyncio
import hashlib
import os
import re
from contextvars import ContextVar
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from open_notebook.utils.encryption import get_secret_from_env

# ContextVar for per-request user identity (set by JWTAuthMiddleware)
current_user_id: ContextVar[Optional[str]] = ContextVar(
    "current_user_id", default=None
)


def set_user_context(user_id: Optional[str]) -> None:
    """Set the current user context. Used by command handlers to restore user context."""
    if user_id:
        current_user_id.set(user_id)


class PasswordAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to check password authentication for all API requests.
    Always active with default password if OPEN_NOTEBOOK_PASSWORD is not set.
    Supports Docker secrets via OPEN_NOTEBOOK_PASSWORD_FILE.
    """

    def __init__(self, app, excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.password = get_secret_from_env("OPEN_NOTEBOOK_PASSWORD")
        self.excluded_paths = excluded_paths or [
            "/",
            "/health",
            "/docs",
            "/openapi.json",
            "/redoc",
        ]

    async def dispatch(self, request: Request, call_next):
        # Skip authentication if no password is set
        if not self.password:
            return await call_next(request)

        # Skip authentication for excluded paths
        if request.url.path in self.excluded_paths:
            return await call_next(request)

        # Skip authentication for CORS preflight requests (OPTIONS)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Check authorization header
        auth_header = request.headers.get("Authorization")

        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Expected format: "Bearer {password}"
        try:
            scheme, credentials = auth_header.split(" ", 1)
            if scheme.lower() != "bearer":
                raise ValueError("Invalid authentication scheme")
        except ValueError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid authorization header format"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Check password
        if credentials != self.password:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid password"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Password is correct, proceed with the request
        response = await call_next(request)
        return response


# Optional: HTTPBearer security scheme for OpenAPI documentation
security = HTTPBearer(auto_error=False)


def check_api_password(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """
    Utility function to check API password.
    Can be used as a dependency in individual routes if needed.
    Supports Docker secrets via OPEN_NOTEBOOK_PASSWORD_FILE.
    Returns True without checking credentials if OPEN_NOTEBOOK_PASSWORD is not configured.
    Raises 401 if credentials are missing or don't match the configured password.
    """
    password = get_secret_from_env("OPEN_NOTEBOOK_PASSWORD")

    # No password configured - skip authentication
    if not password:
        return True

    # No credentials provided
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="Missing authorization",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check password
    if credentials.credentials != password:
        raise HTTPException(
            status_code=401,
            detail="Invalid password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware for JWT-based multiuser authentication.
    Decodes JWT from Authorization header, sets current_user_id ContextVar,
    and auto-migrates new user databases on first request.
    """

    def __init__(self, app, excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.jwt_secret = os.environ.get("JWT_SECRET")
        self.jwt_algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
        if not self.jwt_secret:
            raise ValueError(
                "JWT_SECRET environment variable is required when AUTH_MODE=jwt"
            )
        self.excluded_paths = excluded_paths or [
            "/",
            "/health",
            "/docs",
            "/openapi.json",
            "/redoc",
        ]
        # Track which user databases have been migrated (in-memory cache)
        self._migrated_users: set[str] = set()
        # Per-user locks to prevent concurrent migration of the same user
        self._migration_locks: dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()

    def _sanitize_user_id(self, user_id: str) -> str:
        """Sanitize user_id for use in database name."""
        return re.sub(r"[^a-zA-Z0-9_]", "_", user_id)

    def _get_user_db_name(self, user_id: str) -> str:
        """Generate the per-user database name."""
        sanitized = self._sanitize_user_id(user_id)
        hash_suffix = hashlib.sha256(user_id.encode()).hexdigest()[:8]
        return f"user_{sanitized}_{hash_suffix}"

    async def _ensure_user_db_migrated(self, user_id: str) -> None:
        """Run migrations for a user's database if not already done."""
        if user_id in self._migrated_users:
            return

        # Get or create a per-user lock
        async with self._locks_lock:
            if user_id not in self._migration_locks:
                self._migration_locks[user_id] = asyncio.Lock()
            lock = self._migration_locks[user_id]

        async with lock:
            # Double-check after acquiring lock
            if user_id in self._migrated_users:
                return

            db_name = self._get_user_db_name(user_id)
            logger.info(f"Running migrations for new user database: {db_name}")

            try:
                # Set user context so migrations use the correct database
                current_user_id.set(user_id)

                from open_notebook.database.async_migrate import AsyncMigrationManager

                migration_manager = AsyncMigrationManager()
                if await migration_manager.needs_migration():
                    await migration_manager.run_migration_up()
                    new_version = await migration_manager.get_current_version()
                    logger.success(
                        f"Migrations completed for user {user_id} (db: {db_name}), "
                        f"version: {new_version}"
                    )
                else:
                    logger.debug(
                        f"User database {db_name} already at latest version"
                    )

                # Also run podcast profile migration for new users
                try:
                    from open_notebook.podcasts.migration import (
                        migrate_podcast_profiles,
                    )

                    await migrate_podcast_profiles()
                except Exception as e:
                    logger.warning(
                        f"Podcast profile migration for user {user_id}: {e}"
                    )

                self._migrated_users.add(user_id)

            except Exception as e:
                logger.error(
                    f"Failed to migrate database for user {user_id}: {e}"
                )
                raise

    async def dispatch(self, request: Request, call_next):
        # Skip authentication for excluded paths
        if request.url.path in self.excluded_paths:
            return await call_next(request)

        # Skip authentication for CORS preflight requests (OPTIONS)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Check authorization header
        auth_header = request.headers.get("Authorization")

        if not auth_header:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Expected format: "Bearer {jwt_token}"
        try:
            scheme, token = auth_header.split(" ", 1)
            if scheme.lower() != "bearer":
                raise ValueError("Invalid authentication scheme")
        except ValueError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid authorization header format"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Decode JWT
        try:
            payload = jwt.decode(
                token, self.jwt_secret, algorithms=[self.jwt_algorithm]
            )
        except jwt.ExpiredSignatureError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Token has expired"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        except jwt.InvalidTokenError as e:
            return JSONResponse(
                status_code=401,
                content={"detail": f"Invalid token: {e}"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Extract user ID from 'sub' claim
        user_id = payload.get("sub")
        if not user_id:
            return JSONResponse(
                status_code=401,
                content={"detail": "Token missing 'sub' claim"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Set user context
        current_user_id.set(user_id)

        # Ensure user database is migrated
        try:
            await self._ensure_user_db_migrated(user_id)
        except Exception:
            return JSONResponse(
                status_code=500,
                content={"detail": "Failed to initialize user database"},
            )

        # Proceed with the request
        response = await call_next(request)
        return response
