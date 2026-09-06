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

TELETHON_UPLOAD_PART_SIZE_KB = int(os.getenv("TELETHON_UPLOAD_PART_SIZE_KB", "512"))


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


async def _send_with_telethon(chat_id: int | str, file_path: str, caption: str | None = None, thumb_path: str | None = None,
                               progress_callback: Callable[[int, int], None] | None = None, user_id: int | None = None,
                               session_str: str | None = None) -> bool:
    if TelegramClient is None:
        return False
    from utils.telethon_session import build_telethon_client, get_userbot_credentials, resolve_session_string
    session_str = await resolve_session_string("telethon", session_str=session_str, user_id=user_id)
    if not session_str:
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")
        return False
    api_id, api_hash = get_userbot_credentials()
    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    try:
        async def _no_phone():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")
        await client.start(phone=_no_phone)
        target = await _normalize_target(chat_id, client)
        file_handle = await client.upload_file(file_path, file_name=os.path.basename(file_path), part_size_kb=TELETHON_UPLOAD_PART_SIZE_KB, progress_callback=progress_callback)
        kwargs = {"file": file_handle, "caption": caption or ""}
        if thumb_path and os.path.exists(thumb_path):
            kwargs["thumb"] = thumb_path
        sent = await client.send_file(target, **kwargs)
        logger.info("userbot: Telethon sent file %s to %s", file_path, target)
        return sent
    except Exception:
        logger.exception("userbot: Telethon failed to send file %s", file_path)
        return None
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _send_with_pyrogram(chat_id: int | str, file_path: str, caption: str | None = None, thumb_path: str | None = None,
                               progress_callback: Callable[[int, int], None] | None = None, user_id: int | None = None,
                               session_str: str | None = None) -> bool:
    if PyrogramClient is None:
        return False
    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials, resolve_session_string
    api_id, api_hash = get_userbot_credentials()
    session_str = await resolve_session_string("pyrogram", session_str=session_str, user_id=user_id)
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
        logger.info("userbot: Pyrogram sent document %s to %s", file_path, target)
        return sent
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return None
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def _forward_with_telethon(target_chat_id: int | str, src_chat_id: int | str, src_message_id: int,
                                  user_id: int | None = None, session_str: str | None = None) -> bool:
    if TelegramClient is None:
        return False
    from utils.telethon_session import build_telethon_client, get_userbot_credentials, resolve_session_string
    session_str = await resolve_session_string("telethon", session_str=session_str, user_id=user_id)
    if not session_str:
        return False
    api_id, api_hash = get_userbot_credentials()
    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    try:
        async def _no_phone():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")
        await client.start(phone=_no_phone)
        target = await _normalize_target(target_chat_id, client)
        src = await _normalize_target(src_chat_id, client)
        await client.forward_messages(target, messages=src_message_id, from_peer=src)
        logger.info("userbot: Telethon forwarded %s/%s to %s", src, src_message_id, target)
        return True
    except Exception:
        logger.exception("userbot: Telethon failed to forward %s/%s", src_chat_id, src_message_id)
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _forward_with_pyrogram(target_chat_id: int | str, src_chat_id: int | str, src_message_id: int,
                                  user_id: int | None = None, session_str: str | None = None) -> bool:
    if PyrogramClient is None:
        return False
    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials, resolve_session_string
    api_id, api_hash = get_userbot_credentials()
    session_str = await resolve_session_string("pyrogram", session_str=session_str, user_id=user_id)
    client = build_pyrogram_client(api_id, api_hash, session_str=session_str)
    if client is None:
        return False
    try:
        await client.start()
        target = await _normalize_target(target_chat_id)
        src = await _normalize_target(src_chat_id)
        await client.forward_messages(target, from_chat_id=src, message_ids=src_message_id)
        logger.info("userbot: Pyrogram forwarded %s/%s to %s", src, src_message_id, target)
        return True
    except Exception:
        logger.exception("userbot: Pyrogram failed to forward %s/%s", src_chat_id, src_message_id)
        return False
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def forward_message_via_userbot(target_chat_id: int | str, src_chat_id: int | str, src_message_id: int,
                                       user_id: int | None = None) -> bool:
    if TelegramClient is None and PyrogramClient is None:
        return False
    if not src_message_id:
        return False
    from utils.telethon_session import get_pyrogram_session_string_for_user, get_telethon_session_string_for_user
    _tele_session = await get_telethon_session_string_for_user(user_id=user_id)
    if TelegramClient is not None and _tele_session:
        try:
            if await _forward_with_telethon(target_chat_id, src_chat_id, src_message_id, user_id=user_id, session_str=_tele_session):
                return True
            logger.info("userbot: Telethon forward failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon forward error (%s); trying Pyrogram fallback", e)
    _pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id)
    if PyrogramClient is not None and _pyro_session:
        return await _forward_with_pyrogram(target_chat_id, src_chat_id, src_message_id, user_id=user_id, session_str=_pyro_session)
    return False


async def send_file_via_userbot(chat_id: int | str, file_path: str, caption: str | None = None, thumb_path: str | None = None,
                                 progress_callback: Callable[[int, int], None] | None = None, user_id: int | None = None) -> bool:
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError("Neither Telethon nor Pyrogram are installed. Install at least one: pip install telethon or pip install pyrogram")
    from utils.telethon_session import get_pyrogram_session_string_for_user, get_telethon_session_string_for_user
    _tele_session = await get_telethon_session_string_for_user(user_id=user_id)
    if TelegramClient is not None and _tele_session:
        try:
            result = await _send_with_telethon(chat_id, file_path, caption, thumb_path, progress_callback=progress_callback, user_id=user_id, session_str=_tele_session)
            if result:
                return result
            logger.info("userbot: Telethon send failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon send error (%s); trying Pyrogram fallback", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")
    _pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id)
    if PyrogramClient is not None and _pyro_session:
        result = await _send_with_pyrogram(chat_id, file_path, caption, thumb_path, progress_callback=progress_callback, user_id=user_id, session_str=_pyro_session)
        if result:
            return result
    logger.warning("userbot: all send methods failed for %s", chat_id)
    return None


async def send_file_via_userbot_with_fallback(chat_id: int | str, file_path: str, caption: str | None = None, thumb_path: str | None = None,
                                               progress_callback: Callable[[int, int], None] | None = None, user_id: int | None = None) -> tuple[Any | None, int | str]:
    sent = await send_file_via_userbot(chat_id=chat_id, file_path=file_path, caption=caption, thumb_path=thumb_path, progress_callback=progress_callback, user_id=user_id)
    if not sent and str(chat_id) != "me":
        logger.warning("userbot: upload to %s failed; retrying to Saved Messages ('me') user_id=%s", chat_id, user_id)
        sent = await send_file_via_userbot(chat_id="me", file_path=file_path, caption=caption, thumb_path=thumb_path, progress_callback=progress_callback, user_id=user_id)
        if sent:
            return sent, "me"
        return None, chat_id
    return sent, chat_id
