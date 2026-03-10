"""
Storage utility module for file operations.

Supports both local filesystem and S3-compatible storage (AWS S3, DigitalOcean
Spaces, MinIO, etc.). Uses ContextVar-based per-request isolation so concurrent
requests for different users never share S3 credentials.

Configuration priority per request:
1. ContextVar credentials (set via ensure_s3_credentials_cached from user's DB)
2. Environment variables (AWS_*) — system-wide fallback
3. Disabled — use local storage only
"""

import os
from contextvars import ContextVar
from pathlib import Path
from typing import Any, BinaryIO, Optional, Union

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
)

# ContextVar-based per-request S3 state.
# Each async task / coroutine gets its own value — no cross-user leakage.
_s3_credentials: ContextVar[Optional[dict]] = ContextVar(
    "s3_credentials", default=None
)
_s3_client_var: ContextVar[Any] = ContextVar("s3_client", default=None)


def set_s3_credentials_for_request(credentials: Optional[dict]) -> None:
    """
    Set S3 credentials for the current request/coroutine.

    Called from async context (e.g., ensure_s3_credentials_cached) to make
    credentials available to sync storage functions within the same request.
    """
    _s3_credentials.set(credentials)
    # Reset client so next call creates one with new credentials
    _s3_client_var.set(None)
    if credentials:
        logger.debug(
            f"S3 credentials set for request (bucket: {credentials.get('bucket')})"
        )


def clear_s3_context() -> None:
    """Reset S3 ContextVars for the current request."""
    _s3_credentials.set(None)
    _s3_client_var.set(None)


async def ensure_s3_credentials_cached() -> None:
    """
    Load S3 credentials from the current user's DB and set the ContextVar.

    Call from async contexts before sync storage operations.
    No-op if credentials are already set for this request.
    """
    # Already set for this request
    if _s3_credentials.get() is not None:
        return

    try:
        from open_notebook.domain.s3_config import S3Config

        config = await S3Config.get_for_user()
        if config and config.is_configured():
            set_s3_credentials_for_request(
                {
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
            )
    except Exception as e:
        logger.debug(f"S3Config not available for current user: {e}")


def _get_s3_credentials() -> Optional[dict]:
    """
    Get S3 credentials for the current request.

    Priority:
    1. ContextVar (set from async context for this request)
    2. Environment variables (system-wide fallback)
    """
    # Check ContextVar first
    creds = _s3_credentials.get()
    if creds is not None:
        return creds

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
    """Get or create S3 client for the current request."""
    client = _s3_client_var.get()
    if client is not None:
        return client

    credentials = _get_s3_credentials()
    if not credentials:
        raise RuntimeError("S3 is not configured")

    try:
        import boto3
        from botocore.config import Config

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
        _s3_client_var.set(client)
        return client
    except ImportError:
        raise RuntimeError(
            "boto3 is required for S3 storage. Install with: pip install boto3"
        )


def _get_s3_key(relative_path: str) -> str:
    """Convert relative path to S3 key."""
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
        destination_path: Full destination path (e.g., ./data/uploads/file.pdf)
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
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

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

        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def get_upload_path(filename: str) -> str:
    """Get the full storage path for an upload."""
    return os.path.join(UPLOADS_FOLDER, filename)


def get_podcast_path(episode_name: str) -> str:
    """Get the folder path for a podcast episode."""
    return os.path.join(DATA_FOLDER, "podcasts", "episodes", episode_name)


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

    parts = storage_path.replace("s3://", "").split("/", 1)
    key = parts[1] if len(parts) > 1 else ""

    base_url = public_url.rstrip("/")
    return f"{base_url}/{key}"


def test_s3_connection() -> tuple[bool, str]:
    """
    Test S3 connection with current credentials.

    Returns:
        Tuple of (success, message)
    """
    try:
        if not is_s3_enabled():
            return False, "S3 is not configured"

        client = _get_s3_client()
        credentials = _get_s3_credentials()
        bucket = credentials["bucket"] if credentials else AWS_BUCKET

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
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)

        return True, f"Successfully connected to bucket: {bucket}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"
