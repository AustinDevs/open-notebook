"""
Field-level encryption for sensitive data.

This module provides encryption/decryption for credentials stored in the database.
Fernet uses AES-128-CBC with HMAC-SHA256 for authenticated encryption.

Encryption is OPTIONAL:
- If OPEN_NOTEBOOK_ENCRYPTION_KEY is set, values are encrypted at rest
- If not set, values are stored as plain text (with warning logged)

Usage:
    # Encrypt before storing (returns plain text if encryption not configured)
    encrypted = encrypt_value(api_key)

    # Decrypt when reading (returns original value if not encrypted)
    decrypted = decrypt_value(encrypted)

    # Generate a new key for OPEN_NOTEBOOK_ENCRYPTION_KEY
    new_key = generate_key()
"""

import base64
import os
from pathlib import Path
from typing import Optional

from loguru import logger

# Lazy import to avoid startup failure if cryptography not installed
_fernet_instance: Optional["Fernet"] = None
_encryption_key: Optional[str] = None
_encryption_checked: bool = False


def get_secret_from_env(var_name: str) -> Optional[str]:
    """
    Get a secret from environment, supporting Docker secrets pattern.

    Checks for VAR_FILE first (Docker secrets), then falls back to VAR.

    Args:
        var_name: Base name of the environment variable

    Returns:
        The secret value, or None if not configured.
    """
    # Check for _FILE variant first (Docker secrets)
    file_path = os.environ.get(f"{var_name}_FILE")
    if file_path:
        try:
            path = Path(file_path)
            if path.exists() and path.is_file():
                secret = path.read_text().strip()
                if secret:
                    logger.debug(f"Loaded {var_name} from file: {file_path}")
                    return secret
                else:
                    logger.warning(f"{var_name}_FILE points to empty file: {file_path}")
            else:
                logger.warning(f"{var_name}_FILE path does not exist: {file_path}")
        except Exception as e:
            logger.error(f"Failed to read {var_name} from file {file_path}: {e}")

    # Fall back to direct environment variable
    return os.environ.get(var_name)


def _get_encryption_key() -> Optional[str]:
    """
    Get encryption key from environment if configured.

    Priority:
    1. OPEN_NOTEBOOK_ENCRYPTION_KEY_FILE (Docker secrets)
    2. OPEN_NOTEBOOK_ENCRYPTION_KEY (environment variable)

    Returns:
        Encryption key string, or None if not configured.
    """
    return get_secret_from_env("OPEN_NOTEBOOK_ENCRYPTION_KEY")


def is_encryption_enabled() -> bool:
    """
    Check if encryption is enabled (key is configured).

    Returns:
        True if OPEN_NOTEBOOK_ENCRYPTION_KEY is set, False otherwise.
    """
    global _encryption_checked, _encryption_key

    if not _encryption_checked:
        _encryption_key = _get_encryption_key()
        _encryption_checked = True

        if not _encryption_key:
            logger.warning(
                "OPEN_NOTEBOOK_ENCRYPTION_KEY is not set. "
                "Credentials will be stored as plain text. "
                "For production, generate a key with: "
                'python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
            )

    return _encryption_key is not None


def get_fernet() -> Optional["Fernet"]:
    """
    Get Fernet instance with the configured encryption key.

    Returns:
        Fernet instance, or None if encryption is not configured.
    """
    global _fernet_instance

    if not is_encryption_enabled():
        return None

    if _fernet_instance is None:
        try:
            from cryptography.fernet import Fernet

            _fernet_instance = Fernet(_encryption_key.encode())
        except ImportError:
            logger.error(
                "cryptography package not installed. "
                "Install with: pip install cryptography"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to initialize encryption: {e}")
            return None

    return _fernet_instance


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value using Fernet symmetric encryption.

    If encryption is not configured, returns the original value.

    Args:
        value: The plain text string to encrypt.

    Returns:
        Base64-encoded encrypted string, or original value if encryption disabled.
    """
    fernet = get_fernet()
    if fernet is None:
        return value

    try:
        return fernet.encrypt(value.encode()).decode()
    except Exception as e:
        logger.error(f"Encryption failed: {e}")
        return value


def _looks_like_fernet_token(s: str) -> bool:
    """Check if string looks like a Fernet encrypted token."""
    if len(s) < 40:  # Minimum length for Fernet token
        return False
    # Fernet tokens use URL-safe base64
    try:
        decoded = base64.urlsafe_b64decode(s)
        # Fernet tokens have a specific structure
        return len(decoded) >= 57  # Version (1) + timestamp (8) + IV (16) + data (32+)
    except Exception:
        return False


def decrypt_value(value: str) -> str:
    """
    Decrypt a Fernet-encrypted string value.

    Handles graceful fallback for:
    - Unencrypted legacy data
    - Data stored when encryption was disabled

    Args:
        value: The encrypted string (or plain text for legacy data).

    Returns:
        Decrypted plain text string, or original value if not encrypted.

    Raises:
        ValueError: If decryption fails for what appears to be encrypted data
            (likely wrong key).
    """
    # If it doesn't look like encrypted data, return as-is
    if not _looks_like_fernet_token(value):
        return value

    fernet = get_fernet()
    if fernet is None:
        # Encryption not configured but data looks encrypted
        logger.warning(
            "Data appears to be encrypted but OPEN_NOTEBOOK_ENCRYPTION_KEY is not set. "
            "Returning encrypted value as-is."
        )
        return value

    try:
        from cryptography.fernet import InvalidToken

        return fernet.decrypt(value.encode()).decode()
    except InvalidToken:
        # Looks like encrypted data but failed to decrypt - likely wrong key
        raise ValueError(
            "Decryption failed: data appears to be encrypted but key is incorrect. "
            "Check OPEN_NOTEBOOK_ENCRYPTION_KEY configuration."
        )
    except ImportError:
        logger.error("cryptography package not installed for decryption")
        return value
    except Exception as e:
        logger.error(f"Decryption failed: {e}")
        return value


def generate_key() -> str:
    """
    Generate a new Fernet encryption key.

    Use this to create a value for OPEN_NOTEBOOK_ENCRYPTION_KEY.

    Returns:
        Base64-encoded Fernet key.

    Example:
        >>> from open_notebook.utils.encryption import generate_key
        >>> print(generate_key())
        'your-generated-key-here'
    """
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()
