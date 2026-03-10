"""
Authentication router for Open Notebook API.
Provides endpoints to check authentication status.
"""

import os

from fastapi import APIRouter

from open_notebook.utils.encryption import get_secret_from_env

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/status")
async def get_auth_status():
    """
    Check if authentication is enabled.
    Returns whether a password is required to access the API.
    Supports Docker secrets via OPEN_NOTEBOOK_PASSWORD_FILE.
    """
    auth_mode = os.environ.get("AUTH_MODE", "password")
    auth_enabled = auth_mode == "jwt" or bool(
        get_secret_from_env("OPEN_NOTEBOOK_PASSWORD")
    )

    return {
        "auth_enabled": auth_enabled,
        "auth_mode": auth_mode,
        "message": "Authentication is required"
        if auth_enabled
        else "Authentication is disabled",
    }