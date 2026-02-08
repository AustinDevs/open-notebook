import os

# ROOT DATA FOLDER
DATA_FOLDER = "./data"

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

# AWS/S3 STORAGE CONFIGURATION
# These can be overridden by database config (set via Settings UI)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_BUCKET = os.getenv("AWS_BUCKET", "")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "")  # For S3-compatible services (MinIO, DO Spaces, etc.)
AWS_URL = os.getenv("AWS_URL", "")  # Public URL prefix for serving files
AWS_USE_PATH_STYLE_ENDPOINT = (
    os.getenv("AWS_USE_PATH_STYLE_ENDPOINT", "false").lower() == "true"
)
