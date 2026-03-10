"""
Per-user S3 configuration domain model.

Each user can have one S3Config record in their database, storing S3-compatible
storage credentials. Credentials are encrypted at rest using the same Fernet
encryption as Credential API keys.

Priority for S3 configuration:
1. User's database config (set via Settings UI) — highest priority
2. Environment variables (AWS_*) — system-wide fallback
3. Disabled — use local storage only

Usage:
    config = S3Config(
        bucket_name="my-bucket",
        access_key_id=SecretStr("AKIAIOSFODNN7EXAMPLE"),
        secret_access_key=SecretStr("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
    )
    await config.save()
"""

from typing import Any, ClassVar, Dict, List, Optional

from loguru import logger
from pydantic import Field, SecretStr

from open_notebook.database.repository import repo_query
from open_notebook.domain.base import ObjectModel
from open_notebook.utils.encryption import decrypt_value, encrypt_value


class S3Config(ObjectModel):
    """
    Per-user S3 storage configuration.

    Each user has at most one S3Config record in their database.
    Uses upsert semantics on save to maintain this invariant.
    Credentials are encrypted at rest via Fernet.
    """

    table_name: ClassVar[str] = "s3_config"
    _secret_fields: ClassVar[set] = {"access_key_id", "secret_access_key"}
    nullable_fields: ClassVar[set[str]] = {
        "access_key_id",
        "secret_access_key",
        "bucket_name",
        "region",
        "endpoint_url",
        "public_url",
    }

    # Credentials (SecretStr prevents accidental logging)
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

    def _prepare_save_data(self) -> Dict[str, Any]:
        """Override to encrypt secret fields before storage."""
        data = {}
        for key, value in self.model_dump().items():
            if key in self.__class__._secret_fields:
                # Handle SecretStr: extract, encrypt, store
                secret_val = getattr(self, key)
                if secret_val is not None:
                    secret_value = (
                        secret_val.get_secret_value()
                        if isinstance(secret_val, SecretStr)
                        else secret_val
                    )
                    data[key] = encrypt_value(secret_value)
                else:
                    data[key] = None
            elif value is not None or key in self.__class__.nullable_fields:
                data[key] = value
        return data

    @classmethod
    async def get_for_user(cls) -> Optional["S3Config"]:
        """
        Get the S3 config for the current user (at most one per user database).

        Returns None if no config exists.
        """
        results = await repo_query("SELECT * FROM s3_config LIMIT 1")
        if not results:
            return None
        try:
            return cls._from_db_row(results[0])
        except Exception as e:
            logger.warning(f"Failed to load S3 config: {e}")
            return None

    @classmethod
    async def get(cls, id: str) -> "S3Config":
        """Override get() to handle secret field decryption."""
        instance = await super().get(id)
        for field_name in cls._secret_fields:
            raw_val = getattr(instance, field_name, None)
            if raw_val is not None:
                raw = (
                    raw_val.get_secret_value()
                    if isinstance(raw_val, SecretStr)
                    else raw_val
                )
                decrypted = decrypt_value(raw)
                object.__setattr__(instance, field_name, SecretStr(decrypted))
        return instance

    @classmethod
    async def get_all(cls, order_by=None) -> List["S3Config"]:
        """Override get_all() to handle secret field decryption."""
        instances = await super().get_all(order_by=order_by)
        for instance in instances:
            for field_name in cls._secret_fields:
                raw_val = getattr(instance, field_name, None)
                if raw_val is not None:
                    raw = (
                        raw_val.get_secret_value()
                        if isinstance(raw_val, SecretStr)
                        else raw_val
                    )
                    decrypted = decrypt_value(raw)
                    object.__setattr__(instance, field_name, SecretStr(decrypted))
        return instances

    async def save(self) -> None:
        """Save S3 config with upsert semantics (one record per user)."""
        # If no ID, check if a config already exists for this user
        if not self.id:
            existing = await self.__class__.get_for_user()
            if existing:
                self.id = existing.id
                self.created = existing.created

        # Remember original SecretStr values before save
        original_secrets = {
            field_name: getattr(self, field_name)
            for field_name in self.__class__._secret_fields
        }

        await super().save()

        # After save, DB round-trip may corrupt SecretStr values.
        # Restore the original values.
        for field_name, original_val in original_secrets.items():
            if original_val is not None:
                object.__setattr__(self, field_name, original_val)
            elif getattr(self, field_name) and isinstance(
                getattr(self, field_name), str
            ):
                decrypted = decrypt_value(getattr(self, field_name))
                object.__setattr__(self, field_name, SecretStr(decrypted))

    @classmethod
    def _from_db_row(cls, row: dict) -> "S3Config":
        """Create an S3Config from a database row, decrypting secret fields."""
        for field_name in cls._secret_fields:
            val = row.get(field_name)
            if val and isinstance(val, str):
                decrypted = decrypt_value(val)
                row[field_name] = SecretStr(decrypted)
            elif val is None:
                row[field_name] = None
        return cls(**row)

    def is_configured(self) -> bool:
        """Check if S3 is configured with required fields."""
        return bool(
            self.bucket_name and self.access_key_id and self.secret_access_key
        )

    def get_config_source(self) -> str:
        """Return the source of configuration (database, env, or none)."""
        if self.is_configured():
            return "database"
        # Check environment variables
        from open_notebook.config import AWS_ACCESS_KEY_ID, AWS_BUCKET

        if AWS_BUCKET and AWS_ACCESS_KEY_ID:
            return "environment"
        return "none"
