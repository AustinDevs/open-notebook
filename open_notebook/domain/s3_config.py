"""S3 configuration domain model for storing S3 credentials.

Credentials are encrypted at rest when OPEN_NOTEBOOK_ENCRYPTION_KEY is configured.
If not set, credentials are stored as plain text (with warning logged).

In multitenancy mode, each user has their own S3 configuration.
"""

import asyncio
from typing import Any, ClassVar, Dict, Optional

from loguru import logger
from pydantic import Field, SecretStr

from open_notebook.database.repository import ensure_record_id, repo_query, repo_upsert
from open_notebook.domain.base import RecordModel
from open_notebook.utils.encryption import decrypt_value, encrypt_value


def _get_current_user_id() -> Optional[str]:
    """Get the current user ID from context, if available."""
    try:
        from api.auth import current_user_id

        user_id = current_user_id.get()
        if user_id:
            # Normalize to string format
            user_id_str = str(user_id)
            if not user_id_str.startswith("user:"):
                user_id_str = f"user:{user_id_str}"
            return user_id_str
    except Exception:
        pass
    return None


def _get_s3_config_record_id(user_id: Optional[str] = None) -> str:
    """
    Get the S3 config record ID for the given user.

    Args:
        user_id: User ID (e.g., "user:1"). If None, tries to get from context.

    Returns:
        Record ID like "user:1:s3_config" or fallback "open_notebook:s3_config"
    """
    if user_id is None:
        user_id = _get_current_user_id()

    if user_id:
        # Extract numeric part for cleaner record ID
        if user_id.startswith("user:"):
            user_num = user_id.replace("user:", "")
            return f"user:{user_num}:s3_config"
        return f"user:{user_id}:s3_config"

    # Fallback for non-multitenancy mode
    return "open_notebook:s3_config"


class S3Config(RecordModel):
    """
    Per-user configuration for S3 storage credentials.

    In multitenancy mode, each user has their own S3 configuration stored
    with a user-specific record ID (e.g., "user:1:s3_config").

    These settings take priority over environment variables when configured.
    Credentials are encrypted at rest when OPEN_NOTEBOOK_ENCRYPTION_KEY is set.
    If not set, credentials are stored as plain text (with warning logged).
    """

    # Fields that contain secrets and should be encrypted
    _secret_fields: ClassVar[set] = {"access_key_id", "secret_access_key"}

    # Note: record_id is now dynamic per-user, not a class variable
    # Use _get_s3_config_record_id() to get the appropriate record ID

    # Credentials (use SecretStr in memory to avoid accidental logging)
    access_key_id: Optional[SecretStr] = Field(
        None, description="AWS Access Key ID or S3-compatible service key"
    )
    secret_access_key: Optional[SecretStr] = Field(
        None, description="AWS Secret Access Key or S3-compatible service secret"
    )

    # Configuration
    bucket_name: Optional[str] = Field(None, description="S3 bucket name")
    region: Optional[str] = Field(
        "us-east-1", description="AWS region (e.g., us-east-1, eu-west-1)"
    )
    endpoint_url: Optional[str] = Field(
        None,
        description="Custom endpoint URL for S3-compatible services (MinIO, DigitalOcean Spaces, etc.)",
    )
    public_url: Optional[str] = Field(
        None, description="Public URL prefix for serving files (optional)"
    )
    use_path_style: bool = Field(
        False,
        description="Use path-style URLs (required for some S3-compatible services)",
    )

    def _prepare_save_data(self, include_none: bool = False) -> Dict[str, Any]:
        """
        Prepare data for database storage.

        SecretStr values are extracted, encrypted, and stored as strings.
        Encryption is performed if OPEN_NOTEBOOK_ENCRYPTION_KEY is configured.

        Args:
            include_none: If True, include None values (needed for clearing fields)
        """
        data: Dict[str, Any] = {}
        for field_name, field_info in self.model_fields.items():
            if str(field_info.annotation).startswith("typing.ClassVar"):
                continue
            value = getattr(self, field_name)
            if value is not None:
                # Convert SecretStr to encrypted string for storage
                if isinstance(value, SecretStr):
                    data[field_name] = encrypt_value(value.get_secret_value())
                else:
                    data[field_name] = value
            elif include_none:
                # Include None values when clearing
                data[field_name] = None
        return data

    async def update(self, user_id: Optional[str] = None) -> "S3Config":
        """
        Save the configuration to the database for the current user.

        Uses _prepare_save_data() to properly handle SecretStr encryption.

        Args:
            user_id: Optional user ID. If None, uses current context.
        """
        record_id = _get_s3_config_record_id(user_id)
        data = self._prepare_save_data()
        await repo_upsert(
            self.__class__.table_name
            if hasattr(self.__class__, "table_name")
            else "record",
            record_id,
            data,
        )
        logger.debug(f"Saved S3 config for record: {record_id}")
        return self

    @classmethod
    async def get_instance(cls, user_id: Optional[str] = None) -> "S3Config":
        """
        Fetch configuration from database for the current user.

        Always fetches fresh data (no caching) to ensure we get the latest
        configuration values. Decrypts secret fields when loading.

        Args:
            user_id: Optional user ID. If None, uses current context.

        Returns:
            S3Config: Fresh instance with current database values
        """
        record_id = _get_s3_config_record_id(user_id)

        result = await repo_query(
            "SELECT * FROM ONLY $record_id",
            {"record_id": ensure_record_id(record_id)},
        )

        if result:
            if isinstance(result, list) and len(result) > 0:
                data = result[0]
            elif isinstance(result, dict):
                data = result
            else:
                data = {}
        else:
            data = {}

        # Convert encrypted string values back to SecretStr for secret fields
        for field_name in cls._secret_fields:
            if field_name in data and data[field_name] is not None:
                # Decrypt and wrap in SecretStr
                if isinstance(data[field_name], str):
                    decrypted = decrypt_value(data[field_name])
                    data[field_name] = SecretStr(decrypted)

        # Create new instance with fresh data (bypass singleton cache)
        instance = object.__new__(cls)
        object.__setattr__(instance, "__dict__", {})
        super(S3Config, instance).__init__(**data)
        return instance

    def is_configured(self) -> bool:
        """Check if S3 is configured with required fields."""
        return bool(self.bucket_name and self.access_key_id and self.secret_access_key)

    def get_config_source(self) -> str:
        """Return the source of configuration (database, env, or none)."""
        if self.is_configured():
            return "database"
        # Check environment variables
        from open_notebook.config import AWS_ACCESS_KEY_ID, AWS_BUCKET

        if AWS_BUCKET and AWS_ACCESS_KEY_ID:
            return "environment"
        return "none"

    @classmethod
    def get_sync(cls, user_id: Optional[str] = None) -> Optional["S3Config"]:
        """
        Synchronously get the S3 config instance.

        This is used by the storage module which may be called from sync contexts.
        Returns None if database is not available or config doesn't exist.

        Args:
            user_id: Optional user ID. If None, tries to get from context.
        """
        try:
            # Try to get existing event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                # We're in an async context - can't use asyncio.run()
                # Return None and let storage module fall back to env vars
                return None

            # No running loop - safe to use asyncio.run()
            return asyncio.run(cls.get_instance(user_id))
        except Exception:
            return None

    async def clear_credentials(self, user_id: Optional[str] = None) -> None:
        """Clear all S3 credentials from the database by deleting the record."""
        from open_notebook.database.repository import repo_query

        record_id = _get_s3_config_record_id(user_id)

        # Delete the record from database
        await repo_query(
            "DELETE $record_id",
            {"record_id": ensure_record_id(record_id)},
        )
        logger.debug(f"Cleared S3 config for record: {record_id}")

        # Reset instance state
        self.access_key_id = None
        self.secret_access_key = None
        self.bucket_name = None
        self.region = "us-east-1"
        self.endpoint_url = None
        self.public_url = None
        self.use_path_style = False
