# utils/__init__.py
"""Utilities package for PDF header image extractor bot."""

from .telethon_session import (
    build_telethon_client,
    build_pyrogram_client,
    get_userbot_credentials,
    get_pyrogram_session_string,
    has_usable_telethon_session,
    normalize_target,
)
from .db import (
    get_db,
    close_db,
    save_job_metadata,
    get_job_metadata,
    update_job_metadata,
    save_user_session,
    get_user_session,
    save_forward_batch,
    get_forward_batch,
    delete_forward_batch,
    save_telethon_forward,
)
from .cache import (
    get_cache,
    close_cache,
    RedisCache,
)
