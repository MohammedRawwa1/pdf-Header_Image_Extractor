import json
import logging
import os
import re

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None


def _load_environment_file() -> None:
    """Load values from a .env file when available, without overriding existing env vars."""
    if load_dotenv is None:
        return
    _root = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(_root, ".env"),
        os.path.join(os.getcwd(), ".env"),
        os.path.join(_root, ".env.local"),
    ]
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.exists(candidate):
            try:
                load_dotenv(candidate, override=False)
            except Exception:  # nosec B110
                pass


_load_environment_file()

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
# Effective Bot API upload/download limit in bytes: honors BOT_API_MAX_MB
# (floored at 1MB so a misconfigured 0 can't silently disable routing) and
# caps at MAX_FILE_SIZE when that app-level limit is smaller.
_BOT_API_MB_EFFECTIVE = max(1, BOT_API_MAX_MB)
BOT_API_UPLOAD_LIMIT_BYTES: int = min(
    _BOT_API_MB_EFFECTIVE * 1024 * 1024,
    (
        MAX_FILE_SIZE
        if MAX_FILE_SIZE and MAX_FILE_SIZE > 0
        else _BOT_API_MB_EFFECTIVE * 1024 * 1024
    ),
)

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

# Whether the userbot fallback is enabled. Defaults to enabled when API
# credentials are present (root's effective gate); can be force-disabled
# by setting ENABLE_USERBOT=false or force-enabled with true/1/yes.
_ENABLE_USERBOT_EXPLICIT = os.getenv("ENABLE_USERBOT", "").lower()
ENABLE_USERBOT: bool = (
    _ENABLE_USERBOT_EXPLICIT in ("1", "true", "yes")
    or bool(
        os.getenv("API_ID")
        or os.getenv("api_id")
        or os.getenv("USERBOT_API_ID")
        or os.getenv("userbot_api_id")
    )
)


# ── Access Control List (ACL) ───────────────────────────────────────────────────
# ALLOWED_USER_IDS: comma-separated Telegram user ids allowed to use the bot.
# When empty, the bot is open to everyone (useful while letting users try it).
# ADMIN_USER_ID: a single Telegram user id allowed to run admin commands
# (e.g. /admin add|remove|list) and receive health alerts.
_allowed_file = os.path.join(STORAGE_PATH, "allowed_users.json")


def _load_allowed_users() -> set[int]:
    s: set[int] = set()
    # First, read from ALLOWED_USER_IDS env var if present
    env_val = os.getenv("ALLOWED_USER_IDS", "")
    if env_val:
        for part in env_val.split(","):
            try:
                v = int(part.strip())
                s.add(v)
            except ValueError:
                continue

    # Next, read persisted file if exists
    try:
        if os.path.exists(_allowed_file):
            with open(_allowed_file, encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    for v in data:
                        try:
                            s.add(int(v))
                        except ValueError:
                            continue
    except Exception:
        logger.debug("Failed to load allowed users from file")

    return s


ALLOWED_USER_IDS: set[int] = _load_allowed_users()


def persist_allowed_users() -> None:
    """Persist current `ALLOWED_USER_IDS` to the storage file."""
    try:
        os.makedirs(STORAGE_PATH, exist_ok=True)
        with open(_allowed_file, "w", encoding="utf-8") as fh:
            json.dump(sorted(list(ALLOWED_USER_IDS)), fh)
    except Exception:
        logger.debug("Failed to persist allowed users to file")


def _parse_optional_int(val: str | None) -> int | None:
    try:
        if val is None or val == "":
            return None
        return int(val)
    except ValueError:
        return None


ADMIN_USER_ID: int | None = _parse_optional_int(os.getenv("ADMIN_USER_ID", ""))


def is_user_allowed(user_id: int) -> bool:
    """Return True if user is allowed by ACL or if ACL is empty (open bot).

    Admin user is always allowed.  When ALLOWED_USER_IDS is empty, all
    users are permitted (default, so users can try the bot).
    """
    try:
        if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
            return True
        if not ALLOWED_USER_IDS:
            return True
        return user_id in ALLOWED_USER_IDS
    except Exception:
        logger.warning("ACL check failed for user %s; defaulting to allowed", user_id)
        return True


def _resolve_env_reference(val: str | None) -> str | None:
    """Resolve simple $VAR or ${VAR} environment references (Railway-style)."""
    if not val:
        return val
    v = val.strip()
    m = re.match(r"^\$(\w+)$", v) or re.match(r"^\$\{(\w+)\}$", v)
    if m:
        return os.getenv(m.group(1)) or None

    def _repl(m):
        return os.getenv(m.group(1), "")

    try:
        substituted = re.sub(r"\$\{?(\w+)\}?", _repl, v)
    except Exception:
        substituted = v
    return substituted or None


# Normalize MongoDB environment variable names so all modules find the URI.
_canonical_mongo = None
for _key in ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL"):
    _raw = os.getenv(_key)
    if not _raw:
        continue
    _resolved = _resolve_env_reference(_raw)
    if _resolved:
        _canonical_mongo = _resolved
        break

if _canonical_mongo:
    os.environ.setdefault("MONGO_URI", _canonical_mongo)
    os.environ.setdefault("MONGODB_URL", _canonical_mongo)
    os.environ.setdefault("MONGO_URL", _canonical_mongo)
    os.environ.setdefault("MONGODB_URI", _canonical_mongo)


# ── Helpers ────────────────────────────────────────────────────────────────────


def is_admin_user(user_id: int) -> bool:
    """Return True if user_id is in ADMIN_USERS, matches OWNER_ID, or matches ADMIN_USER_ID."""
    if user_id is None:
        return False
    uid = int(user_id)
    if OWNER_ID and uid == OWNER_ID:
        return True
    if ADMIN_USER_ID and uid == ADMIN_USER_ID:
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
