import os
import shutil
import sys
import logging
from db_tables import PDF_ARCHIVE_TABLE, SOURCE_REPORTS_TABLE
from secret_env import load_workspace_secret_env_defaults

def _load_secret_env_defaults():
    """Load workspace-local secret defaults without overriding explicit env."""
    load_workspace_secret_env_defaults()

_load_secret_env_defaults()

def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}

class Config:
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "ssh_reports_hub")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "ssh_reports_hub")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

    SOURCE_TABLE = SOURCE_REPORTS_TABLE
    META_TABLE = PDF_ARCHIVE_TABLE
    PDF_STATUS_COL = "pdf_sync_status"
    LEGACY_STATUS_COL = "sync_status"
    PDF_HASH_COL = "pdf_hash"

    LOCAL_BUFFER_DIR = os.getenv("LOCAL_BUFFER_DIR", os.path.expanduser("~/downloads/pdf_archive_temp"))
    RCLONE_BIN = (
        os.getenv("RCLONE_BIN")
        or (os.path.expanduser("~/.local/bin/rclone") if os.path.exists(os.path.expanduser("~/.local/bin/rclone")) else None)
        or shutil.which("rclone")
        or "/usr/bin/rclone"
    )
    RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "gdrive:/archive/pdf")
    RCLONE_CONFIG = os.getenv("RCLONE_CONFIG", os.path.expanduser("~/.config/rclone/rclone.conf"))
    LOCK_FILE = "/tmp/pdf_archiver_async.lock"
    ZOMBIE_TIMEOUT_SECONDS = int(os.getenv("PDF_ARCHIVER_ZOMBIE_TIMEOUT", "600"))

    BATCH_SIZE = 10
    DOWNLOAD_CONCURRENCY = 10
    FETCH_RETRY_LIMIT = int(os.getenv("FETCH_RETRY_LIMIT", "8"))
    FETCH_ONLY = _env_flag("PDF_ARCHIVER_FETCH_ONLY")
    RCLONE_TRANSFERS = int(os.getenv("RCLONE_TRANSFERS", "8"))
    RCLONE_CHECKERS = int(os.getenv("RCLONE_CHECKERS", "16"))
    RCLONE_RETRIES = int(os.getenv("RCLONE_RETRIES", "3"))
    RCLONE_LOW_LEVEL_RETRIES = int(os.getenv("RCLONE_LOW_LEVEL_RETRIES", "10"))
    ONEDRIVE_CHUNK_SIZE = os.getenv("ONEDRIVE_CHUNK_SIZE", "64000k")
    DBFI_DOWNLOAD_CONCURRENCY = int(os.getenv("DBFI_DOWNLOAD_CONCURRENCY", "1"))
    DBFI_REQUEST_DELAY_SECONDS = float(os.getenv("DBFI_REQUEST_DELAY_SECONDS", "2"))

    EXCLUDED_FIRMS = ('미래에셋증권', '유진투자증권', '상상인증권', 'BNK투자증권')
    DBFI_FIRM_ORDER = 19
    WARP_PROXY = os.getenv("WARP_PROXY", "127.0.0.1:9091")
    LOG_FILE = os.getenv("LOG_FILE", os.path.expanduser("~/logs/pdf_archiver_async.log"))

# 로깅 설정
os.makedirs(os.path.dirname(Config.LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(Config.LOG_FILE), logging.StreamHandler(sys.stdout)]
)
