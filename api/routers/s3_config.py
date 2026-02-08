"""API router for S3 storage configuration."""

from typing import Optional

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field, SecretStr

from open_notebook.config import (
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET,
    AWS_DEFAULT_REGION,
    AWS_ENDPOINT,
    AWS_SECRET_ACCESS_KEY,
    AWS_USE_PATH_STYLE_ENDPOINT,
)
from open_notebook.domain.s3_config import S3Config
from open_notebook.utils.storage import test_s3_connection_with_credentials

router = APIRouter()


class S3ConfigRequest(BaseModel):
    """Request model for saving S3 configuration."""

    access_key_id: str = Field(..., description="AWS Access Key ID")
    secret_access_key: str = Field(..., description="AWS Secret Access Key")
    bucket_name: str = Field(..., description="S3 bucket name")
    region: Optional[str] = Field("us-east-1", description="AWS region")
    endpoint_url: Optional[str] = Field(
        None, description="Custom endpoint for S3-compatible services"
    )
    public_url: Optional[str] = Field(
        None, description="Public URL prefix for serving files"
    )
    use_path_style: bool = Field(
        False, description="Use path-style URLs (for MinIO, etc.)"
    )


class S3ConfigResponse(BaseModel):
    """Response model for S3 configuration (no secrets)."""

    bucket_name: Optional[str] = None
    region: Optional[str] = None
    endpoint_url: Optional[str] = None
    public_url: Optional[str] = None
    use_path_style: bool = False
    has_credentials: bool = False


class S3StatusResponse(BaseModel):
    """Response model for S3 configuration status."""

    configured: bool
    source: str  # "database", "environment", or "none"
    bucket_name: Optional[str] = None
    region: Optional[str] = None
    endpoint_url: Optional[str] = None


class S3TestResponse(BaseModel):
    """Response model for S3 connection test."""

    success: bool
    message: str


@router.get("/s3-config/status", response_model=S3StatusResponse)
async def get_s3_status():
    """Check if S3 is configured and get the configuration source."""
    try:
        # Get database config
        config = await S3Config.get_instance()
        source = config.get_config_source()

        if source == "database":
            return S3StatusResponse(
                configured=True,
                source="database",
                bucket_name=config.bucket_name,
                region=config.region,
                endpoint_url=config.endpoint_url,
            )
        elif source == "environment":
            return S3StatusResponse(
                configured=True,
                source="environment",
                bucket_name=AWS_BUCKET,
                endpoint_url=None,  # Don't expose env endpoint
            )
        else:
            return S3StatusResponse(configured=False, source="none")

    except Exception as e:
        logger.error(f"Error checking S3 status: {e}")
        # Fall back to checking environment variables
        if AWS_BUCKET and AWS_ACCESS_KEY_ID:
            return S3StatusResponse(
                configured=True, source="environment", bucket_name=AWS_BUCKET
            )
        return S3StatusResponse(configured=False, source="none")


@router.get("/s3-config", response_model=S3ConfigResponse)
async def get_s3_config():
    """Get current S3 configuration (secrets are never returned)."""
    try:
        config = await S3Config.get_instance()

        return S3ConfigResponse(
            bucket_name=config.bucket_name,
            region=config.region,
            endpoint_url=config.endpoint_url,
            public_url=config.public_url,
            use_path_style=config.use_path_style,
            has_credentials=bool(config.access_key_id and config.secret_access_key),
        )

    except Exception as e:
        logger.error(f"Error getting S3 config: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting S3 config: {e}")


@router.post("/s3-config", response_model=S3ConfigResponse)
async def save_s3_config(request: S3ConfigRequest):
    """Save S3 configuration to the database."""
    try:
        config = await S3Config.get_instance()

        # Update configuration
        config.access_key_id = SecretStr(request.access_key_id)
        config.secret_access_key = SecretStr(request.secret_access_key)
        config.bucket_name = request.bucket_name
        config.region = request.region or "us-east-1"
        config.endpoint_url = request.endpoint_url
        config.public_url = request.public_url
        config.use_path_style = request.use_path_style

        await config.update()

        logger.info(f"S3 configuration saved for bucket: {request.bucket_name}")

        return S3ConfigResponse(
            bucket_name=config.bucket_name,
            region=config.region,
            endpoint_url=config.endpoint_url,
            public_url=config.public_url,
            use_path_style=config.use_path_style,
            has_credentials=True,
        )

    except Exception as e:
        logger.error(f"Error saving S3 config: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving S3 config: {e}")


@router.delete("/s3-config")
async def delete_s3_config():
    """Remove S3 configuration from the database."""
    try:
        config = await S3Config.get_instance()
        await config.clear_credentials()

        logger.info("S3 configuration deleted")

        return {"message": "S3 configuration deleted successfully"}

    except Exception as e:
        logger.error(f"Error deleting S3 config: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting S3 config: {e}")


@router.post("/s3-config/test", response_model=S3TestResponse)
async def test_s3_config():
    """Test the current S3 connection."""
    try:
        # Get credentials from database first, then fall back to env vars
        config = await S3Config.get_instance()

        if config.is_configured():
            # Use database credentials
            credentials = {
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
            }
        elif AWS_BUCKET and AWS_ACCESS_KEY_ID:
            # Fall back to environment variables
            credentials = {
                "access_key_id": AWS_ACCESS_KEY_ID,
                "secret_access_key": AWS_SECRET_ACCESS_KEY,
                "bucket": AWS_BUCKET,
                "region": AWS_DEFAULT_REGION,
                "endpoint": AWS_ENDPOINT,
                "use_path_style": AWS_USE_PATH_STYLE_ENDPOINT,
            }
        else:
            return S3TestResponse(success=False, message="S3 is not configured")

        success, message = test_s3_connection_with_credentials(credentials)
        return S3TestResponse(success=success, message=message)

    except Exception as e:
        logger.error(f"Error testing S3 connection: {e}")
        return S3TestResponse(success=False, message=f"Error: {str(e)}")
