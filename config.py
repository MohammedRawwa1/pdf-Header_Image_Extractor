import os
from typing import Set

# Core
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "")
USE_POLLING: bool = os.getenv("USE_POLLING", "false").lower() in ("1", "true", "yes")
HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "8000"))

# Admins: comma-separated Telegram user ids (e.g. "12345,67890"). If empty, admin commands require ADMIN_SECRET.
ADMIN_USERS_RAW = os.getenv("ADMIN_USERS", "")
ADMIN_USERS: Set[int] = set()
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

# Background queue (optional)
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Limits
MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", str(0)))  # bytes, 0 = unlimited

# Temp directory for downloads (optional)
TMP_DIR: str = os.getenv("TMP_DIR", "")


def is_admin_user(user_id: int) -> bool:
    if user_id is None:
        return False
    if ADMIN_USERS:
        return int(user_id) in ADMIN_USERS
    return False
