"""Userbot uploader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot upload files >50MB.
"""

import logging
import os
from collections.abc import Callable

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:
    TelegramClient = None
    StringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:
    PyrogramClient = None

logger = logging.getLogger(__name__)

# Telethon upload chunk size in KB.  Larger chunks mean fewer upload
# requests (Telethon caps upload parts at 512KB).  Configurable via
# TELETHON_UPLOAD_PART_SIZE_KB.
TELETHON_UPLOAD_PART_SIZE_KB = int(
    os.getenv("TELETHON_UPLOAD_PART_SIZE_KB", "512")
)


async def _normalize_target(chat_id: int | str, client=None):
    try:
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            return chat_id
        try:
            return int(chat_id)
        except Exception:
            return chat_id
    except Exception:
        return chat_id


async def _send_with_telethon(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    thumb_path: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bool:
    """Send a file using Telethon.

    Args:
        thumb_path: Optional path to a thumbnail image to attach.
        user_id: Optional Telegram user ID for per-user session resolution.
    """
    if TelegramClient is None:
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_userbot_credentials,
        resolve_session_string,
    )

    # Use the caller's pre-resolved session string when provided; fall back to
    # resolving it here (direct callers) so it is never resolved twice.
    session_str = await resolve_session_string(
        "telethon", session_str=session_str, user_id=user_id
    )
    if not session_str:
        logger.info(
            "userbot: Telethon session not configured; skipping Telethon upload"
        )
        return False

    api_id, api_hash = get_userbot_credentials()
    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    try:

        async def _no_phone():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")

        await client.start(phone=_no_phone)
        target = await _normalize_target(chat_id, client)
        kwargs = {
            "file": file_path,
            "caption": caption or "",
            "part_size_kb": TELETHON_UPLOAD_PART_SIZE_KB,
        }
        if thumb_path and os.path.exists(thumb_path):
            kwargs["thumb"] = thumb_path
        if progress_callback is not None:
            kwargs["progress_callback"] = progress_callback
        await client.send_file(target, **kwargs)
        logger.info("userbot: Telethon sent file %s to %s", file_path, target)
        return True
    except Exception:
        logger.exception("userbot: Telethon failed to send file %s", file_path)
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:  # nosec B110
            pass


async def _send_with_pyrogram(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    thumb_path: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bool:
    """Send a file using Pyrogram (session string fallback).

    Args:
        thumb_path: Optional path to a thumbnail image to attach.
        user_id: Optional Telegram user ID for per-user session resolution.
    """
    if PyrogramClient is None:
        return False

    from utils.telethon_session import (
        build_pyrogram_client,
        get_userbot_credentials,
        resolve_session_string,
    )

    api_id, api_hash = get_userbot_credentials()

    # Use the caller's pre-resolved session string when provided; fall back to
    # resolving it here (direct callers) so it is never resolved twice.
    session_str = await resolve_session_string(
        "pyrogram", session_str=session_str, user_id=user_id
    )
    client = build_pyrogram_client(api_id, api_hash, session_str=session_str)
    if client is None:
        return False

    try:
        await client.start()
        target = await _normalize_target(chat_id)
        kwargs = {"caption": caption or ""}
        if thumb_path and os.path.exists(thumb_path):
            kwargs["thumb"] = thumb_path
        if progress_callback is not None:
            kwargs["progress"] = progress_callback

        await client.send_document(target, file_path, **kwargs)
        logger.info(
            "userbot: Pyrogram sent document %s to %s", file_path, target
        )
        return True
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return False
    finally:
        try:
            await client.stop()
        except Exception:  # nosec B110
            pass


async def send_file_via_userbot(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    thumb_path: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bool:
    """Send a file using a user account.

    Tries Telethon first (when a session is available), then falls back to
    Pyrogram if a session string is configured.

    Args:
        thumb_path: Optional path to a thumbnail image to attach.
        user_id: Optional Telegram user ID for per-user session resolution.

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string_for_user,
        get_telethon_session_string_for_user,
    )

    _tele_session = await get_telethon_session_string_for_user(user_id=user_id)
    if TelegramClient is not None and _tele_session:
        try:
            result = await _send_with_telethon(
                chat_id,
                file_path,
                caption,
                thumb_path,
                progress_callback=progress_callback,
                user_id=user_id,
                session_str=_tele_session,
            )
            if result:
                return True
            logger.info(
                "userbot: Telethon send failed; trying Pyrogram fallback"
            )
        except Exception as e:
            logger.warning(
                "userbot: Telethon send error (%s); trying Pyrogram fallback",
                e,
            )
    elif TelegramClient is not None:
        logger.info(
            "userbot: Telethon session not configured; skipping Telethon upload"
        )

    _pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id)
    if PyrogramClient is not None and _pyro_session:
        result = await _send_with_pyrogram(
            chat_id,
            file_path,
            caption,
            thumb_path,
            progress_callback=progress_callback,
            user_id=user_id,
            session_str=_pyro_session,
        )
        if result:
            return True

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return False
