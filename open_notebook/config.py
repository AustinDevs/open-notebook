import os
import re
from typing import Optional


# ROOT DATA FOLDER
DATA_FOLDER = "./data"

# AWS S3-COMPATIBLE STORAGE CONFIGURATION
# Storage is enabled when AWS_BUCKET is set
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_BUCKET = os.getenv("AWS_BUCKET", "")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "")  # e.g., https://nyc3.digitaloceanspaces.com
AWS_URL = os.getenv("AWS_URL", "")  # Public URL base for accessing files
AWS_USE_PATH_STYLE_ENDPOINT = os.getenv("AWS_USE_PATH_STYLE_ENDPOINT", "false").lower() == "true"

# S3 is enabled when bucket is configured
S3_ENABLED = bool(AWS_BUCKET)

# LANGGRAPH CHECKPOINT FILE
sqlite_folder = f"{DATA_FOLDER}/sqlite-db"
os.makedirs(sqlite_folder, exist_ok=True)
LANGGRAPH_CHECKPOINT_FILE = f"{sqlite_folder}/checkpoints.sqlite"

# UPLOADS FOLDER
UPLOADS_FOLDER = f"{DATA_FOLDER}/uploads"
os.makedirs(UPLOADS_FOLDER, exist_ok=True)

# TIKTOKEN CACHE FOLDER
TIKTOKEN_CACHE_DIR = f"{DATA_FOLDER}/tiktoken-cache"
os.makedirs(TIKTOKEN_CACHE_DIR, exist_ok=True)


def sanitize_user_id_for_path(user_id: Optional[str]) -> Optional[str]:
    """
    Extract numeric ID from user:N format for filesystem use.

    Handles formats: "user:1", "user:⟨1⟩", "1"
    Returns: "1" or None
    """
    if not user_id:
        return None
    match = re.search(r"(\d+)", str(user_id))
    return match.group(1) if match else None


def get_user_uploads_folder(user_id: Optional[str]) -> str:
    """
    Get uploads folder for a user, creates if needed.

    Returns: ./data/uploads/{user_id}/ if user_id provided
             ./data/uploads/ if no user_id (legacy mode)
    """
    safe_id = sanitize_user_id_for_path(user_id)
    if safe_id:
        path = os.path.join(UPLOADS_FOLDER, safe_id)
        os.makedirs(path, exist_ok=True)
        return path
    return UPLOADS_FOLDER


def get_user_podcasts_folder(user_id: Optional[str]) -> str:
    """
    Get podcasts folder for a user, creates if needed.

    Returns: ./data/podcasts/{user_id}/episodes/ if user_id provided
             ./data/podcasts/episodes/ if no user_id (legacy mode)
    """
    safe_id = sanitize_user_id_for_path(user_id)
    base = os.path.join(DATA_FOLDER, "podcasts")
    if safe_id:
        path = os.path.join(base, safe_id, "episodes")
    else:
        path = os.path.join(base, "episodes")
    os.makedirs(path, exist_ok=True)
    return path
