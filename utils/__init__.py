# utils/__init__.py
"""Utilities package for PDF header image extractor bot."""

# ruff: noqa: F401 - re-exports are intentionally imported for package API surface

from .cache import (
    RedisCache,
    close_cache,
    get_cache,
)
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
