"""User model for multitenancy support."""

from typing import ClassVar

from loguru import logger

from open_notebook.database.repository import repo_query, repo_upsert
from open_notebook.domain.base import ObjectModel


class User(ObjectModel):
    """
    User model for row-level multitenancy.

    Users are created automatically on first valid JWT authentication.
    The user's ID is the 'sub' claim from the JWT, which identifies
    the user in the parent SaaS application.

    The ID is stored in SurrealDB format: user:<sub_claim>
    """

    table_name: ClassVar[str] = "user"

    @classmethod
    async def get_or_create(cls, id: str) -> "User":
        """
        Get an existing user or create a new one based on ID.

        This is the primary method used during JWT authentication to
        auto-provision users on their first login. The ID should be
        the 'sub' claim from the JWT.

        Args:
            id: The user ID (typically the 'sub' claim from JWT)

        Returns:
            Existing or newly created User instance
        """
        # Ensure ID has table prefix
        if not id.startswith("user:"):
            full_id = f"user:{id}"
        else:
            full_id = id

        # Extract just the ID part without table prefix for upsert
        id_part = full_id.split(":", 1)[1] if ":" in full_id else full_id

        # Try to find existing user
        try:
            logger.debug(f"Looking up user: {full_id}")
            # Use direct interpolation since we control full_id from validated JWT
            result = await repo_query(f"SELECT * FROM {full_id}")
            logger.debug(f"User lookup result: {result}")
            if result:
                logger.info(f"Found existing user: {full_id}")
                return cls(**result[0])
            else:
                logger.debug(f"User {full_id} not found (empty result)")
        except Exception as e:
            logger.debug(f"User {full_id} lookup failed with exception: {e}")

        # Create new user with specific ID using upsert
        # repo_upsert expects the full record ID (e.g., "user:1")
        logger.info(f"Creating new user: {full_id}")
        result = await repo_upsert("user", full_id, {})
        logger.debug(f"User upsert result: {result}")

        if result and len(result) > 0:
            return cls(**result[0])

        # Fallback: try to fetch the user we just created
        result = await repo_query(f"SELECT * FROM {full_id}")
        if result:
            return cls(**result[0])

        raise RuntimeError(f"Failed to create or retrieve user {full_id}")
