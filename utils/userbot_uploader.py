"""Userbot uploader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot upload files >50MB.
"""

import logging
import os
from collections.abc import Callable
from typing import Any

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
        # Telethon's send_file() does not forward part_size_kb to
        # upload_file(), so upload explicitly with the desired chunk size and
        # pass the returned InputFile handle (send_file accepts pre-uploaded
        # handles directly).
        file_handle = await client.upload_file(
            file_path,
            file_name=os.path.basename(file_path),
            part_size_kb=TELETHON_UPLOAD_PART_SIZE_KB,
            progress_callback=progress_callback,
        )
        kwargs = {
            "file": file_handle,
            "caption": caption or "",
        }
        if thumb_path and os.path.exists(thumb_path):
            kwargs["thumb"] = thumb_path
        sent = await client.send_file(target, **kwargs)
        logger.info("userbot: Telethon sent file %s to %s", file_path, target)
        # Return the sent message (truthy) so callers can locate the delivered
        # copy (chat_id + message_id) for later chat-based downloads.
        return sent
    except Exception:
        logger.exception("userbot: Telethon failed to send file %s", file_path)
        return None
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

        sent = await client.send_document(target, file_path, **kwargs)
        logger.info(
            "userbot: Pyrogram sent document %s to %s", file_path, target
        )
        # Return the sent message (truthy) so callers can locate the delivered
        # copy (chat_id + message_id) for later chat-based downloads.
        return sent
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return None
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
                return result
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
            return result

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return None


async def send_file_via_userbot_with_fallback(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    thumb_path: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> tuple[Any | None, int | str]:
    """Send a file via the userbot, retrying to Saved Messages on failure.

    Used for large-file delivery: when ``chat_id`` is the bot's user ID the
    file lands in the requesting user's DM with the bot; if that send fails
    (e.g. the userbot can't resolve the bot's entity — a known production
    failure), it retries to the userbot's Saved Messages (``'me'``) so the
    file is still delivered instead of being lost.

    Shares the retry logic between the web process (``bot.py``) and the
    worker (``tasks.py``) so the delivery-target fallback stays in one place.

    Returns ``(sent_message, chat_used)`` — the sent message object (truthy;
    carries the delivered copy's ``id``/``chat_id``) and the chat the file
    actually landed in (``chat_id``, or ``'me'`` for Saved Messages), so
    callers can point a later chat-based download at the copy.  ``(None,
    chat_id)`` when no userbot session is available or both attempts fail.
    """
    sent = await send_file_via_userbot(
        chat_id=chat_id,
        file_path=file_path,
        caption=caption,
        thumb_path=thumb_path,
        progress_callback=progress_callback,
        user_id=user_id,
    )
    if not sent and str(chat_id) != "me":
        logger.warning(
            "userbot: upload to %s failed; retrying to Saved Messages ('me') user_id=%s",
            chat_id,
            user_id,
        )
        sent = await send_file_via_userbot(
            chat_id="me",
            file_path=file_path,
            caption=caption,
            thumb_path=thumb_path,
            progress_callback=progress_callback,
            user_id=user_id,
        )
        if sent:
            return sent, "me"
        return None, chat_id
    return sent, chat_id
