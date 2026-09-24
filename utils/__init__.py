# ruff: noqa: E402
# The DeprecationWarning filter below must run before the submodule imports,
# so the imports intentionally follow a module-level statement.
import warnings as _warnings

_warnings.filterwarnings(
    "ignore",
    message=r"There is no current event loop",
    category=DeprecationWarning,
)
del _warnings

from .cache import RedisCache, close_cache, get_cache
from .db import (
    close_db,
    delete_forward_batch,
    get_db,
    get_forward_batch,
    get_job_metadata,
    get_user_session,
    save_forward_batch,
    save_job_metadata,
    save_telethon_forward,
    save_user_session,
    update_job_metadata,
)
from .error_handler import (
    BotErrorHandler,
    async_error_handler,
    get_error_handler,
    handle_bot_error,
)
from .rate_limiter import (
    ConversionRateLimiter,
    RateLimiter,
    TelegramAPIRateLimiter,
)
from .telethon_session import (
    build_pyrogram_client,
    build_telethon_client,
    get_pyrogram_session_string,
    get_userbot_credentials,
    has_usable_telethon_session,
    normalize_target,
)

__all__ = [
    "BotErrorHandler",
    "ConversionRateLimiter",
    "RateLimiter",
    "RedisCache",
    "TelegramAPIRateLimiter",
    "async_error_handler",
    "build_pyrogram_client",
    "build_telethon_client",
    "close_cache",
    "close_db",
    "delete_forward_batch",
    "get_cache",
    "get_db",
    "get_error_handler",
    "get_forward_batch",
    "get_job_metadata",
    "get_pyrogram_session_string",
    "get_user_session",
    "get_userbot_credentials",
    "handle_bot_error",
    "has_usable_telethon_session",
    "normalize_target",
    "save_forward_batch",
    "save_job_metadata",
    "save_telethon_forward",
    "save_user_session",
    "update_job_metadata",
]
