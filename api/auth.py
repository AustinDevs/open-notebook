import os
from pathlib import Path
from contextvars import ContextVar
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# Context variable for current user ID - used for row-level multitenancy
# This is set by the auth middleware and read by domain/repository functions
current_user_id: ContextVar[Optional[str]] = ContextVar("current_user_id", default=None)


from loguru import logger

def _get_secret_from_env(var_name: str) -> Optional[str]:
    """
    Get a secret from environment, supporting Docker secrets pattern.

    Checks for VAR_FILE first (Docker secrets), then falls back to VAR.
    """
    # Check for _FILE variant first (Docker secrets)
    file_path = os.environ.get(f"{var_name}_FILE")
    if file_path:
        path = Path(file_path)
        if path.exists() and path.is_file():
            secret = path.read_text().strip()
            if secret:
                return secret

    # Fall back to direct environment variable
    return os.environ.get(var_name)


class PasswordAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to check password authentication for all API requests.
    Always active with default password if OPEN_NOTEBOOK_PASSWORD is not set.
    Supports Docker secrets via OPEN_NOTEBOOK_PASSWORD_FILE.
    """

    def __init__(self, app, excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.password = _get_secret_from_env("OPEN_NOTEBOOK_PASSWORD")
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


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to authenticate requests using JWT tokens.
    Used for SaaS embedding where users are authenticated by a parent application.

    Extracts the 'sub' claim from the JWT and auto-creates User records
    on first authentication. Sets current_user_id context variable for
    row-level multitenancy filtering.
    """

    def __init__(self, app, excluded_paths: Optional[list] = None):
        super().__init__(app)
        self.jwt_secret = os.environ.get("JWT_SECRET")
        self.jwt_public_key = os.environ.get("JWT_PUBLIC_KEY")
        self.jwt_algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
        self.excluded_paths = excluded_paths or [
            "/",
            "/health",
            "/docs",
            "/openapi.json",
            "/redoc",
        ]

        # Validate configuration
        if not self.jwt_secret and not self.jwt_public_key:
            logger.warning(
                "JWT authentication enabled but no JWT_SECRET or JWT_PUBLIC_KEY set. "
                "All authenticated requests will fail."
            )

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

        # Decode and validate JWT
        try:
            # Use public key for RS256/RS384/RS512, secret for HS256/HS384/HS512
            key = self.jwt_public_key if self.jwt_public_key else self.jwt_secret
            if not key:
                return JSONResponse(
                    status_code=500,
                    content={"detail": "JWT authentication not configured"},
                )

            payload = jwt.decode(
                token,
                key,
                algorithms=[self.jwt_algorithm],
                audience="open-notebook",
                options={"verify_exp": True, "verify_aud": True},
            )

            # Extract user identifier from 'sub' claim
            sub = payload.get("sub")
            logger.info(f"JWT auth: sub claim = {sub}")
            if not sub:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "JWT missing 'sub' claim"},
                    headers={"WWW-Authenticate": "Bearer"},
                )

            # Auto-create user if not exists
            # The 'sub' claim becomes the user's ID (user:<sub>)
            from open_notebook.domain.user import User

            user = await User.get_or_create(sub)
            logger.info(f"JWT auth: user.id = {user.id}")

            # Store user_id in request state for endpoint access
            request.state.user_id = user.id

            # Set context variable for repository filtering
            current_user_id.set(user.id)
            logger.info(f"JWT auth: current_user_id set to {user.id}")

        except jwt.ExpiredSignatureError:
            return JSONResponse(
                status_code=401,
                content={"detail": "JWT token has expired"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid JWT token"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # JWT is valid, proceed with the request
        response = await call_next(request)
        return response


def get_current_user_id(request: Request) -> Optional[str]:
    """
    FastAPI dependency to get the current user ID from request state.

    Usage in endpoints:
        @router.get("/items")
        async def get_items(user_id: str = Depends(get_current_user_id)):
            ...
    """
    return getattr(request.state, "user_id", None)


# Optional: HTTPBearer security scheme for OpenAPI documentation
security = HTTPBearer(auto_error=False)


def check_api_password(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """
    Utility function to check API password.
    Can be used as a dependency in individual routes if needed.
    Supports Docker secrets via OPEN_NOTEBOOK_PASSWORD_FILE.
    Uses default password if not configured.
    """
    password = _get_secret_from_env("OPEN_NOTEBOOK_PASSWORD")

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
