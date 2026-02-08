"""
Storage utility module for file operations.

Supports both local filesystem and S3-compatible storage (AWS S3, DigitalOcean Spaces, MinIO, etc.).
Configuration priority:
1. Database config (set via Settings UI) - highest priority
2. Environment variables - fallback for existing deployments
3. Disabled - use local storage only
"""

import os
from pathlib import Path
from typing import BinaryIO, Optional, Union

from loguru import logger

from open_notebook.config import (
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET,
    AWS_DEFAULT_REGION,
    AWS_ENDPOINT,
    AWS_SECRET_ACCESS_KEY,
    AWS_URL,
    AWS_USE_PATH_STYLE_ENDPOINT,
    DATA_FOLDER,
    UPLOADS_FOLDER,
    sanitize_user_id_for_path,
)

def _get_s3_credentials() -> Optional[dict]:
    """
    Get S3 credentials from database config (per-user) or environment variables.
    Returns None if S3 is not configured.

    In multitenancy mode, each user has their own S3 configuration stored in the
    database. The S3Config.get_sync() method uses the current user context.

    Priority:
    1. Database config via S3Config.get_sync() (per-user in multitenancy)
    2. Environment variables (fallback for existing deployments)
    """
    # Try to get credentials from database (per-user in multitenancy mode)
    try:
        from open_notebook.domain.s3_config import S3Config

        config = S3Config.get_sync()
        if config and config.bucket_name and config.access_key_id:
            return {
                "access_key_id": config.access_key_id.get_secret_value()
                if config.access_key_id
                else None,
                "secret_access_key": config.secret_access_key.get_secret_value()
                if config.secret_access_key
                else None,
                "bucket": config.bucket_name,
                "region": config.region or "us-east-1",
                "endpoint": config.endpoint_url,
                "use_path_style": config.use_path_style,
                "public_url": config.public_url,
            }
    except Exception as e:
        # Database config not available or error - fall back to env vars
        logger.debug(f"S3Config not available from database: {e}")

    # Fall back to environment variables
    if AWS_BUCKET and AWS_ACCESS_KEY_ID:
        return {
            "access_key_id": AWS_ACCESS_KEY_ID,
            "secret_access_key": AWS_SECRET_ACCESS_KEY,
            "bucket": AWS_BUCKET,
            "region": AWS_DEFAULT_REGION,
            "endpoint": AWS_ENDPOINT,
            "use_path_style": AWS_USE_PATH_STYLE_ENDPOINT,
            "public_url": AWS_URL,
        }

    return None


def _get_s3_client():
    """
    Create S3 client with current user's credentials.

    No caching - creates a fresh client each time to support multitenancy
    where each user may have different S3 credentials.
    """
    credentials = _get_s3_credentials()
    if not credentials:
        raise RuntimeError("S3 is not configured")

    try:
        import boto3
        from botocore.config import Config

        # Configure path-style or virtual-hosted style addressing
        config = Config(
            signature_version="s3v4",
            s3={
                "addressing_style": "path"
                if credentials.get("use_path_style")
                else "auto"
            },
        )

        return boto3.client(
            "s3",
            endpoint_url=credentials.get("endpoint") or None,
            aws_access_key_id=credentials.get("access_key_id"),
            aws_secret_access_key=credentials.get("secret_access_key"),
            region_name=credentials.get("region"),
            config=config,
        )
    except ImportError:
        raise RuntimeError(
            "boto3 is required for S3 storage. Install with: pip install boto3"
        )




def _get_s3_key(relative_path: str) -> str:
    """Convert relative path to S3 key."""
    # Normalize path separators for S3
    return relative_path.replace("\\", "/").lstrip("/")


def _get_relative_path(full_path: str, base_folder: str = DATA_FOLDER) -> str:
    """Extract relative path from full path."""
    return os.path.relpath(full_path, base_folder)


def is_s3_enabled() -> bool:
    """Check if S3 storage is enabled and configured."""
    return _get_s3_credentials() is not None


def upload_file(
    file_content: Union[bytes, BinaryIO],
    destination_path: str,
    content_type: Optional[str] = None,
) -> str:
    """
    Upload file to storage (local or S3).

    Args:
        file_content: File content as bytes or file-like object
        destination_path: Full destination path (e.g., ./data/uploads/1/file.pdf)
        content_type: Optional MIME type for S3 uploads

    Returns:
        The storage path/URL for the uploaded file
    """
    if is_s3_enabled():
        return _upload_to_s3(file_content, destination_path, content_type)
    else:
        return _upload_to_local(file_content, destination_path)


def _upload_to_local(
    file_content: Union[bytes, BinaryIO], destination_path: str
) -> str:
    """Upload file to local filesystem."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    # Write file
    if isinstance(file_content, bytes):
        with open(destination_path, "wb") as f:
            f.write(file_content)
    else:
        with open(destination_path, "wb") as f:
            f.write(file_content.read())

    logger.info(f"Saved file locally: {destination_path}")
    return destination_path


def _upload_to_s3(
    file_content: Union[bytes, BinaryIO],
    destination_path: str,
    content_type: Optional[str] = None,
) -> str:
    """Upload file to S3-compatible storage."""
    client = _get_s3_client()
    credentials = _get_s3_credentials()
    bucket = credentials["bucket"] if credentials else AWS_BUCKET

    s3_key = _get_s3_key(_get_relative_path(destination_path))

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    if isinstance(file_content, bytes):
        from io import BytesIO

        file_obj = BytesIO(file_content)
    else:
        file_obj = file_content

    client.upload_fileobj(file_obj, bucket, s3_key, ExtraArgs=extra_args or None)
    logger.info(f"Uploaded to S3: s3://{bucket}/{s3_key}")

    # Return the S3 URI for storage in database
    return f"s3://{bucket}/{s3_key}"


def download_file(storage_path: str) -> bytes:
    """
    Download file from storage (local or S3).

    Args:
        storage_path: Path returned from upload_file (local path or s3:// URI)

    Returns:
        File content as bytes
    """
    if storage_path.startswith("s3://"):
        return _download_from_s3(storage_path)
    else:
        return _download_from_local(storage_path)


def _download_from_local(file_path: str) -> bytes:
    """Download file from local filesystem."""
    with open(file_path, "rb") as f:
        return f.read()


def _download_from_s3(s3_uri: str) -> bytes:
    """Download file from S3-compatible storage."""
    from io import BytesIO

    client = _get_s3_client()

    # Parse s3://bucket/key URI
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    buffer = BytesIO()
    client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return buffer.read()


def download_to_temp_file(storage_path: str, suffix: Optional[str] = None) -> str:
    """
    Download file from S3 to a temporary local file.

    Args:
        storage_path: S3 URI (s3://bucket/key)
        suffix: Optional file suffix (e.g., ".pdf")

    Returns:
        Path to the temporary file
    """
    import tempfile

    content = download_file(storage_path)

    # Extract suffix from original path if not provided
    if suffix is None and "/" in storage_path:
        original_filename = storage_path.split("/")[-1]
        if "." in original_filename:
            suffix = "." + original_filename.split(".")[-1]

    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)

    return temp_path


def get_file_stream(storage_path: str) -> BinaryIO:
    """
    Get a file stream for streaming responses.

    Args:
        storage_path: Path returned from upload_file

    Returns:
        File-like object for streaming
    """
    if storage_path.startswith("s3://"):
        return _get_s3_stream(storage_path)
    else:
        return open(storage_path, "rb")


def _get_s3_stream(s3_uri: str) -> BinaryIO:
    """Get streaming response from S3."""
    client = _get_s3_client()

    # Parse s3://bucket/key URI
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"]


def delete_file(storage_path: str) -> bool:
    """
    Delete file from storage (local or S3).

    Args:
        storage_path: Path returned from upload_file

    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        if storage_path.startswith("s3://"):
            return _delete_from_s3(storage_path)
        else:
            return _delete_from_local(storage_path)
    except Exception as e:
        logger.error(f"Failed to delete file {storage_path}: {e}")
        return False


def _delete_from_local(file_path: str) -> bool:
    """Delete file from local filesystem."""
    if os.path.exists(file_path):
        os.unlink(file_path)
        logger.info(f"Deleted local file: {file_path}")
        return True
    return False


def _delete_from_s3(s3_uri: str) -> bool:
    """Delete file from S3-compatible storage."""
    client = _get_s3_client()

    # Parse s3://bucket/key URI
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    client.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Deleted from S3: {s3_uri}")
    return True


def file_exists(storage_path: str) -> bool:
    """
    Check if file exists in storage.

    Args:
        storage_path: Path returned from upload_file

    Returns:
        True if file exists
    """
    if storage_path.startswith("s3://"):
        return _s3_file_exists(storage_path)
    else:
        return os.path.exists(storage_path)


def _s3_file_exists(s3_uri: str) -> bool:
    """Check if file exists in S3."""
    try:
        client = _get_s3_client()

        # Parse s3://bucket/key URI
        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def get_user_upload_path(user_id: Optional[str], filename: str) -> str:
    """
    Get the full storage path for a user upload.

    Args:
        user_id: User ID (e.g., "user:1")
        filename: Original filename

    Returns:
        Full path for storing the file
    """
    safe_id = sanitize_user_id_for_path(user_id)
    if safe_id:
        return os.path.join(UPLOADS_FOLDER, safe_id, filename)
    return os.path.join(UPLOADS_FOLDER, filename)


def get_user_podcast_path(user_id: Optional[str], episode_name: str) -> str:
    """
    Get the folder path for a user's podcast episode.

    Args:
        user_id: User ID (e.g., "user:1")
        episode_name: Episode name

    Returns:
        Full folder path for the episode
    """
    safe_id = sanitize_user_id_for_path(user_id)
    base = os.path.join(DATA_FOLDER, "podcasts")
    if safe_id:
        return os.path.join(base, safe_id, "episodes", episode_name)
    return os.path.join(base, "episodes", episode_name)


def get_public_url(storage_path: str) -> Optional[str]:
    """
    Get the public URL for a file in S3 storage.

    Args:
        storage_path: S3 URI (s3://bucket/key) or local path

    Returns:
        Public URL if configured and path is S3, None otherwise
    """
    if not storage_path.startswith("s3://"):
        return None

    credentials = _get_s3_credentials()
    public_url = credentials.get("public_url") if credentials else AWS_URL

    if not public_url:
        return None

    # Extract the key from s3://bucket/key
    parts = storage_path.replace("s3://", "").split("/", 1)
    key = parts[1] if len(parts) > 1 else ""

    # Construct public URL
    base_url = public_url.rstrip("/")
    return f"{base_url}/{key}"


def test_s3_connection() -> tuple[bool, str]:
    """
    Test S3 connection with current credentials (sync version).

    Note: This may not work in async contexts. Use test_s3_connection_async instead.

    Returns:
        Tuple of (success, message)
    """
    try:
        if not is_s3_enabled():
            return False, "S3 is not configured"

        client = _get_s3_client()
        credentials = _get_s3_credentials()
        bucket = credentials["bucket"] if credentials else AWS_BUCKET

        # Try to list bucket (head_bucket requires different permissions)
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)

        return True, f"Successfully connected to bucket: {bucket}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"


def test_s3_connection_with_credentials(credentials: dict) -> tuple[bool, str]:
    """
    Test S3 connection with provided credentials.

    Args:
        credentials: Dict with access_key_id, secret_access_key, bucket, region,
                     endpoint, use_path_style keys

    Returns:
        Tuple of (success, message)
    """
    try:
        import boto3
        from botocore.config import Config

        if not credentials or not credentials.get("bucket"):
            return False, "S3 is not configured"

        # Configure path-style or virtual-hosted style addressing
        config = Config(
            signature_version="s3v4",
            s3={
                "addressing_style": "path"
                if credentials.get("use_path_style")
                else "auto"
            },
        )

        client = boto3.client(
            "s3",
            endpoint_url=credentials.get("endpoint") or None,
            aws_access_key_id=credentials.get("access_key_id"),
            aws_secret_access_key=credentials.get("secret_access_key"),
            region_name=credentials.get("region"),
            config=config,
        )

        bucket = credentials["bucket"]

        # Try to list bucket (head_bucket requires different permissions)
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)

        return True, f"Successfully connected to bucket: {bucket}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"
