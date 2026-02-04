"""
Storage utility module for file operations.

Supports both local filesystem and S3-compatible storage (AWS S3, DigitalOcean Spaces, etc.).
Configure via environment variables:
- S3_ENABLED=true to use S3 storage
- S3_BUCKET, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION
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
    S3_ENABLED,
    UPLOADS_FOLDER,
    sanitize_user_id_for_path,
)

# Lazy-load boto3 only when S3 is enabled
_s3_client = None


def _get_s3_client():
    """Get or create S3 client (lazy initialization)."""
    global _s3_client
    if _s3_client is None:
        try:
            import boto3
            from botocore.config import Config

            # Configure path-style or virtual-hosted style addressing
            config = Config(
                signature_version="s3v4",
                s3={"addressing_style": "path" if AWS_USE_PATH_STYLE_ENDPOINT else "auto"},
            )

            _s3_client = boto3.client(
                "s3",
                endpoint_url=AWS_ENDPOINT or None,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_DEFAULT_REGION,
                config=config,
            )
        except ImportError:
            raise RuntimeError(
                "boto3 is required for S3 storage. Install with: pip install boto3"
            )
    return _s3_client


def _get_s3_key(relative_path: str) -> str:
    """Convert relative path to S3 key."""
    # Normalize path separators for S3
    return relative_path.replace("\\", "/").lstrip("/")


def _get_relative_path(full_path: str, base_folder: str = DATA_FOLDER) -> str:
    """Extract relative path from full path."""
    return os.path.relpath(full_path, base_folder)


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
    if S3_ENABLED:
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
    s3_key = _get_s3_key(_get_relative_path(destination_path))

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    if isinstance(file_content, bytes):
        from io import BytesIO

        file_obj = BytesIO(file_content)
    else:
        file_obj = file_content

    client.upload_fileobj(file_obj, AWS_BUCKET, s3_key, ExtraArgs=extra_args or None)
    logger.info(f"Uploaded to S3: s3://{AWS_BUCKET}/{s3_key}")

    # Return the S3 URI for storage in database
    return f"s3://{AWS_BUCKET}/{s3_key}"


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


def is_s3_enabled() -> bool:
    """Check if S3 storage is enabled."""
    return S3_ENABLED


def get_public_url(storage_path: str) -> Optional[str]:
    """
    Get the public URL for a file in S3 storage.

    Args:
        storage_path: S3 URI (s3://bucket/key) or local path

    Returns:
        Public URL if AWS_URL is configured and path is S3, None otherwise
    """
    if not storage_path.startswith("s3://") or not AWS_URL:
        return None

    # Extract the key from s3://bucket/key
    parts = storage_path.replace("s3://", "").split("/", 1)
    key = parts[1] if len(parts) > 1 else ""

    # Construct public URL
    base_url = AWS_URL.rstrip("/")
    return f"{base_url}/{key}"
