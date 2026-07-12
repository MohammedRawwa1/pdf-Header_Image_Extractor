"""Userbot downloader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot download files >50MB.
"""

import io
import os
import logging
import shutil
from typing import Union, Optional
from datetime import datetime
import asyncio
import json

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


async def _normalize_target(chat_id: Union[int, str], client=None):
    """Return a compatible target entity for ``chat_id``."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


async def _download_with_telethon(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
) -> bool:
    """Download using Telethon client."""
    if TelegramClient is None:
        logger.debug("Telethon not installed; skipping Telethon download")
        return False

    from utils.telethon_session import build_telethon_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_telethon_client(api_id, api_hash)
    try:
        logger.info("userbot: starting Telethon client for download")
        await client.start()
        logger.info("userbot: Telethon client started successfully")
    except Exception as e:
        logger.exception("userbot: failed to start Telethon client: %s", e)
        return False

    try:
        target = await _normalize_target(chat_id, client)

        try:
            msgs = await client.get_messages(target, ids=message_id)
        except Exception as e:
            logger.exception("userbot: get_messages direct by id failed: %s", e)
            msgs = None

        if msgs:
            msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
            if getattr(msg, "media", None):
                logger.info("userbot: message found; downloading %s/%s to %s", target, message_id, dest_path)
                for attempt in range(3):
                    try:
                        await client.download_media(msg, file=dest_path)
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                            return True
                        logger.warning("userbot: downloaded file empty (attempt %s) %s", attempt + 1, dest_path)
                        try:
                            os.remove(dest_path)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.exception("userbot: download attempt %s failed: %s", attempt + 1, e)

        # Scan recent messages as fallback
        try:
            async for m in client.iter_messages(target, limit=200):
                if getattr(m, "media", None):
                    for attempt in range(3):
                        try:
                            await client.download_media(m, file=dest_path)
                            if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                return True
                        except Exception:
                            pass
        except Exception:
            pass

        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _download_bytes_with_pyrogram(
    chat_id: Union[int, str],
    message_id: int,
) -> Optional[bytes]:
    """Download a message's media into memory (bytes) using Pyrogram."""
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; cannot do in-memory download")
        return None

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured; cannot do in-memory download")
        return None

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for in-memory download")

        target = await _normalize_target(chat_id)

        _candidates = [target]
        _bot_token = os.getenv("BOT_TOKEN", "")
        if _bot_token and ":" in _bot_token:
            try:
                _bot_id = int(_bot_token.split(":")[0])
                if _bot_id != target:
                    _candidates.append(_bot_id)
            except (ValueError, IndexError):
                pass

        for _peer in _candidates:
            try:
                messages = await client.get_messages(_peer, message_ids=[message_id])
                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg and getattr(msg, "media", None):
                        data = await client.download_media(msg, in_memory=True)
                        if data is not None and isinstance(data, bytes) and len(data) > 0:
                            logger.info("userbot: Pyrogram in-memory download succeeded: %d bytes", len(data))
                            return data
            except Exception as e:
                logger.warning("userbot: Pyrogram in-memory error with peer=%s msg=%s: %s", _peer, message_id, e)

        return None
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def _download_with_pyrogram(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
) -> bool:
    """Download using Pyrogram client (session string fallback)."""
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; skipping")
        return False

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured")
        return False

    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception as e:
            logger.warning("userbot: could not create dest dir %s: %s", _dest_dir, e)

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for download")

        target = await _normalize_target(chat_id)

        _candidates = [target]
        _bot_token = os.getenv("BOT_TOKEN", "")
        if _bot_token and ":" in _bot_token:
            try:
                _bot_id = int(_bot_token.split(":")[0])
                if _bot_id != target:
                    _candidates.append(_bot_id)
            except (ValueError, IndexError):
                pass

        for _peer in _candidates:
            try:
                messages = await client.get_messages(_peer, message_ids=[message_id])
                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg and getattr(msg, "media", None):
                        _dl = await client.download_media(msg, file_name=dest_path)
                        if _dl:
                            _dl_path = str(_dl)
                            _abs_dest = os.path.abspath(dest_path)
                            if _dl_path != _abs_dest and not os.path.exists(dest_path):
                                if os.path.exists(_dl_path):
                                    shutil.move(_dl_path, _abs_dest)
                            if os.path.exists(_abs_dest) and os.path.getsize(_abs_dest) > 0:
                                return True
            except Exception as e:
                logger.warning("userbot: Pyrogram error with peer=%s msg=%s: %s", _peer, message_id, e)

        return False
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def download_forward_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
) -> bool:
    """Download a message media using a user account.

    Tries Telethon first (with string session or file-based session),
    then falls back to Pyrogram if a session string is configured.

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string,
        has_usable_telethon_session,
    )

    pyrogram_session_configured = bool(get_pyrogram_session_string())

    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            result = await _download_with_pyrogram(chat_id, message_id, dest_path)
            if result:
                return True
            logger.info("userbot: Pyrogram download failed; trying Telethon fallback")
        except Exception as e:
            logger.warning("userbot: Pyrogram download error (%s); trying Telethon fallback", e)

    if TelegramClient is not None and has_usable_telethon_session():
        try:
            result = await _download_with_telethon(
                chat_id, message_id, dest_path, msg_date, file_unique_id
            )
            if result:
                return True
        except Exception as e:
            logger.warning("userbot: Telethon download error (%s)", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon download")

    logger.warning("userbot: all download methods failed for %s/%s", chat_id, message_id)
    return False


async def download_bytes_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
) -> Optional[bytes]:
    """Download a message media into memory (bytes) using userbot.

    Tries Pyrogram with ``in_memory=True`` first.
    Falls back to Telethon (BytesIO) if Pyrogram fails.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string,
        has_usable_telethon_session,
    )

    pyrogram_session_configured = bool(get_pyrogram_session_string())

    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            data = await _download_bytes_with_pyrogram(chat_id, message_id)
            if data is not None:
                return data
        except Exception as e:
            logger.warning("userbot: Pyrogram in-memory download error (%s)", e)

    if TelegramClient is not None and has_usable_telethon_session():
        try:
            from utils.telethon_session import build_telethon_client, get_userbot_credentials as _get_creds

            _api_id, _api_hash = _get_creds()
            _client = build_telethon_client(_api_id, _api_hash)
            if _client is not None:
                await _client.start()
                target = await _normalize_target(chat_id, _client)
                msgs = await _client.get_messages(target, ids=message_id)
                if msgs:
                    msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
                    if getattr(msg, "media", None):
                        buf = io.BytesIO()
                        await _client.download_media(msg, file=buf)
                        data = buf.getvalue()
                        if data and len(data) > 0:
                            return data
                await _client.disconnect()
        except Exception as e:
            logger.warning("userbot: Telethon in-memory download error (%s)", e)

    logger.warning("userbot: all in-memory download methods failed for %s/%s", chat_id, message_id)
    return None
