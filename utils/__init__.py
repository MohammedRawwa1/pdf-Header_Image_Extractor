# utils/__init__.py
"""Utilities package for PDF header image extractor bot."""

import warnings as _warnings

# pyrogram's sync wrapper calls asyncio.get_event_loop() at import time;
# under Python 3.12+ that emits a DeprecationWarning when no loop is running
# (normal during module import). Suppress just that third-party message so
# imports stay clean without masking other warnings.
_warnings.filterwarnings(
    "ignore",
    message=r"There is no current event loop",
    category=DeprecationWarning,
)

del _warnings

from .cache import (  # noqa: E402, F401
    RedisCache,
    close_cache,
    get_cache,
)
from .db import (  # noqa: E402, F401
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
from .error_handler import (  # noqa: E402, F401
    BotErrorHandler,
    async_error_handler,
    get_error_handler,
    handle_bot_error,
)
from .rate_limiter import (  # noqa: E402, F401
    ConversionRateLimiter,
    RateLimiter,
    TelegramAPIRateLimiter,
)
from .telethon_session import (  # noqa: E402, F401
    build_pyrogram_client,
    build_telethon_client,
    get_pyrogram_session_string,
    get_userbot_credentials,
    has_usable_telethon_session,
    normalize_target,
)
