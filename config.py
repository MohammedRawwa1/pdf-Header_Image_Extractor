import os

# Core
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
# WEBHOOK_URL: explicitly set, or auto-derived from Railway's RAILWAY_PUBLIC_DOMAIN
WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "")
if not WEBHOOK_URL:
    _railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if _railway_domain:
        WEBHOOK_URL = f"https://{_railway_domain}"
USE_POLLING: bool = os.getenv("USE_POLLING", "false").lower() in (
    "1",
    "true",
    "yes",
)
HOST: str = os.getenv("HOST", "0.0.0.0")  # nosec B104 - intentional bind to all interfaces for web serving
PORT: int = int(os.getenv("PORT", "8000"))

# ── Owner & Admin ──────────────────────────────────────────────────────────────
# OWNER_ID: single Telegram user ID that has full control over sensitive /s
# commands (/setwebhook, /setcommands, /set_commands, /delete_webhook).
# Falls back to ADMIN_USERS when OWNER_ID is not set.
OWNER_ID: int = 0
_owner_raw = os.getenv("OWNER_ID", "")
if _owner_raw:
    try:
        OWNER_ID = int(_owner_raw)
    except ValueError:
        pass

# Admins: comma-separated Telegram user ids (e.g. "12345,67890"). If empty, admin commands require ADMIN_SECRET.
ADMIN_USERS_RAW = os.getenv("ADMIN_USERS", "")
ADMIN_USERS: set[int] = set()
if ADMIN_USERS_RAW:
    for part in ADMIN_USERS_RAW.split(","):
        part = part.strip()
        if part:
            try:
                ADMIN_USERS.add(int(part))
            except ValueError:
                pass

# Admin HTTP secret for API endpoints
ADMIN_SECRET: str = os.getenv("ADMIN_SECRET", "")

# Optional logging / integrations
LOG_CHANNEL: str = os.getenv("LOG_CHANNEL", "")
SENTRY_DSN: str = os.getenv("SENTRY_DSN", "")

# Webhook secret token for CSRF protection (X-Telegram-Bot-Api-Secret-Token)
# If not set, a random token is auto-generated on startup
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")

# Background queue (optional)
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Limits
MAX_FILE_SIZE: int = int(
    os.getenv("MAX_FILE_SIZE", str(0))
)  # bytes, 0 = unlimited
BOT_API_MAX_MB: int = int(
    os.getenv("BOT_API_MAX_MB", "50")
)  # Telegram Bot API max in MB

# Temp directory for downloads (optional)
TMP_DIR: str = os.getenv("TMP_DIR", "")

# PDF compression quality preset (used by tools.compress_pdf)
PDF_COMPRESS_QUALITY: str = os.getenv("PDF_COMPRESS_QUALITY", "/ebook")

# ── Storage backend ────────────────────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STORAGE_BACKEND: str = os.getenv(
    "STORAGE_BACKEND", "local"
)  # 'local', 's3', or 'r2'
STORAGE_PATH: str = os.getenv(
    "STORAGE_PATH", os.path.join(ROOT_DIR, "storage")
)
INPUT_PATH: str = os.getenv("INPUT_PATH", os.path.join(STORAGE_PATH, "input"))
OUTPUT_PATH: str = os.getenv(
    "OUTPUT_PATH", os.path.join(STORAGE_PATH, "output")
)
TEMP_PATH: str = os.getenv("TEMP_PATH", os.path.join(STORAGE_PATH, "temp"))
THUMBNAIL_PATH: str = os.getenv(
    "THUMBNAIL_PATH", os.path.join(STORAGE_PATH, "thumbnails")
)

# Optional S3 fallback settings
ENABLE_S3_FALLBACK: bool = os.getenv(
    "ENABLE_S3_FALLBACK", "false"
).lower() in ("1", "true", "yes")
S3_BUCKET: str = os.getenv("S3_BUCKET", "")
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_REGION: str = os.getenv("S3_REGION", "")
S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "")
S3_PRESIGNED_EXPIRY: int = int(os.getenv("S3_PRESIGNED_EXPIRY", "3600"))
S3_SIGNATURE_VERSION: str = os.getenv("S3_SIGNATURE_VERSION", "s3v4")
S3_USE_SSL: bool = os.getenv("S3_USE_SSL", "1") not in (
    "0",
    "false",
    "False",
    "no",
)
PRESIGN_EXPIRES: int = int(os.getenv("PRESIGN_EXPIRES", "3600"))

# ── Userbot / relay for big files ──────────────────────────────────────────────
RELAY_CHAT_ID: str = os.getenv("RELAY_CHAT_ID", "")

# API_ID / API_HASH are read by utils/telethon_session at runtime.
# They are listed here for documentation clarity.
# API_ID: int = int(os.getenv("API_ID", "0"))
# API_HASH: str = os.getenv("API_HASH", "")
# PYROGRAM_SESSION: str = os.getenv("PYROGRAM_SESSION", "")


# ── Helpers ────────────────────────────────────────────────────────────────────


def is_admin_user(user_id: int) -> bool:
    """Return True if user_id is in ADMIN_USERS or matches OWNER_ID."""
    if user_id is None:
        return False
    uid = int(user_id)
    if OWNER_ID and uid == OWNER_ID:
        return True
    if ADMIN_USERS:
        return uid in ADMIN_USERS
    return False


def is_owner(user_id: int) -> bool:
    """Return True only if user_id matches OWNER_ID.

    Used to guard sensitive webhook commands (/s) so that only the
    designated owner can touch them. Falls back to ADMIN_USERS when
    OWNER_ID is not configured.
    """
    if user_id is None:
        return False
    if OWNER_ID:
        return int(user_id) == OWNER_ID
    # No owner configured — fall back to admin check
    return is_admin_user(user_id)
