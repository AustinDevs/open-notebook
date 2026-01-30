"""
JWT Authentication middleware for multi-tenant namespace isolation.

Allows external applications (e.g., Laravel/Filament) to pass a JWT token
containing a namespace claim, enabling per-user database isolation.

JWT Payload expected format:
{
    "namespace": "user_namespace",
    "database": "open_notebook",  // optional, defaults to env SURREAL_DATABASE
    "exp": 1234567890,  // optional expiration timestamp
    "iat": 1234567890,  // optional issued-at timestamp
    "sub": "user_id"    // optional subject (user identifier)
}
"""

import os
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import jwt
from fastapi import Request
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


@dataclass
class TenantContext:
    """Holds the current tenant's database connection details."""

    namespace: str
    database: str
    user_id: Optional[str] = None


# Context variable to hold the current tenant context
# This allows the database layer to access the namespace without passing it through every function
_tenant_context: ContextVar[Optional[TenantContext]] = ContextVar(
    "tenant_context", default=None
)


def get_tenant_context() -> Optional[TenantContext]:
    """Get the current tenant context from the context variable."""
    return _tenant_context.get()


def set_tenant_context(context: Optional[TenantContext]) -> None:
    """Set the current tenant context."""
    _tenant_context.set(context)


def get_current_namespace() -> str:
    """
    Get the current namespace to use for database operations.
    Returns the JWT namespace if set, otherwise falls back to environment variable.
    """
    ctx = get_tenant_context()
    if ctx and ctx.namespace:
        return ctx.namespace
    return os.environ.get("SURREAL_NAMESPACE", "open_notebook")


def get_current_database() -> str:
    """
    Get the current database to use for database operations.
    Returns the JWT database if set, otherwise falls back to environment variable.
    """
    ctx = get_tenant_context()
    if ctx and ctx.database:
        return ctx.database
    return os.environ.get("SURREAL_DATABASE", "open_notebook")


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to validate JWT tokens and extract tenant namespace.

    When JWT_AUTH_ENABLED=true:
    - Validates the JWT signature using JWT_SECRET
    - Extracts namespace/database from JWT claims
    - Sets the tenant context for database operations

    When JWT_AUTH_ENABLED=false (default):
    - Passes through without validation (uses env namespace)
    """

    def __init__(self, app, excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.jwt_secret = os.environ.get("JWT_SECRET")
        self.jwt_algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
        self.jwt_enabled = os.environ.get("JWT_AUTH_ENABLED", "false").lower() == "true"
        self.default_namespace = os.environ.get("SURREAL_NAMESPACE", "open_notebook")
        self.default_database = os.environ.get("SURREAL_DATABASE", "open_notebook")
        self.excluded_paths = excluded_paths or [
            "/",
            "/health",
            "/docs",
            "/openapi.json",
            "/redoc",
            "/api/auth/status",
            "/api/config",
        ]

    async def dispatch(self, request: Request, call_next):
        # Reset tenant context at the start of each request
        set_tenant_context(None)

        # Skip JWT processing if not enabled
        if not self.jwt_enabled:
            return await call_next(request)

        # Skip for excluded paths
        if request.url.path in self.excluded_paths:
            return await call_next(request)

        # Skip for CORS preflight
        if request.method == "OPTIONS":
            return await call_next(request)

        # Check for JWT in Authorization header or query parameter
        token = self._extract_token(request)

        if not token:
            # No token provided - use default namespace (backward compatible)
            logger.debug("No JWT token provided, using default namespace")
            return await call_next(request)

        # Validate and decode JWT
        try:
            payload = self._decode_token(token)
            if payload is None:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid or expired JWT token"},
                    headers={"WWW-Authenticate": "Bearer"},
                )

            # Extract tenant information
            namespace = payload.get("namespace", self.default_namespace)
            database = payload.get("database", self.default_database)
            user_id = payload.get("sub")

            # Set tenant context for this request
            tenant_ctx = TenantContext(
                namespace=namespace, database=database, user_id=user_id
            )
            set_tenant_context(tenant_ctx)

            logger.debug(
                f"JWT authenticated: namespace={namespace}, database={database}, user={user_id}"
            )

            # Store tenant info in request state for access in route handlers
            request.state.tenant = tenant_ctx

        except Exception as e:
            logger.error(f"JWT processing error: {e}")
            return JSONResponse(
                status_code=401,
                content={"detail": f"JWT authentication failed: {str(e)}"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        response = await call_next(request)
        return response

    def _extract_token(self, request: Request) -> Optional[str]:
        """
        Extract JWT token from request.
        Checks: Authorization header, then 'token' query parameter.
        """
        # Check Authorization header first
        auth_header = request.headers.get("Authorization")
        if auth_header:
            parts = auth_header.split(" ", 1)
            if len(parts) == 2 and parts[0].lower() == "bearer":
                # Check if it looks like a JWT (has two dots)
                if parts[1].count(".") == 2:
                    return parts[1]

        # Check query parameter (useful for iframe embedding)
        token = request.query_params.get("token")
        if token and token.count(".") == 2:
            return token

        return None

    def _decode_token(self, token: str) -> Optional[dict]:
        """
        Decode and validate JWT token.
        Returns payload dict if valid, None if invalid.
        """
        if not self.jwt_secret:
            logger.error("JWT_SECRET not configured but JWT auth is enabled")
            return None

        try:
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm],
                options={
                    "verify_exp": True,  # Verify expiration if present
                    "require": ["namespace"],  # Require namespace claim
                },
            )
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return None
