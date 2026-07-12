"""Userbot downloader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot download files >50MB.
"""

import io
import os
import logging
import shutil
from typing import Union, Optional, Callable
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


async def _resolve_pyrogram_peer(client, peer_id: Union[int, str]) -> int:
    """Resolve a peer ID to get Pyrogram's cached entity (with access_hash).

    Pyrogram needs the ``access_hash`` for a peer before it can call
    ``get_messages()`` or similar.  For user IDs the userbot has never
    interacted with, Pyrogram raises ``[400 PEER_ID_INVALID]`` because
    it lacks the hash.  This function resolves the peer via
    ``get_chat()`` / ``get_users()``, which fetches and caches the hash.

    Adapted from the media_conersion_bot reference implementation.

    Args:
        client: An active Pyrogram Client.
        peer_id: Numeric chat/user ID or @username.

    Returns:
        The resolved peer (usually the same numeric ID, now cached).
    """
    if not isinstance(peer_id, int):
        return peer_id

    # Try get_chat first (covers groups, channels, and users)
    try:
        resolved = await client.get_chat(peer_id)
        if resolved is not None:
            cached_id = getattr(resolved, "id", None)
            if cached_id is not None:
                logger.debug(
                    "resolve_pyrogram_peer: get_chat(%s) -> id=%s type=%s",
                    peer_id, cached_id,
                    getattr(resolved, "_", type(resolved).__name__),
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "resolve_pyrogram_peer: get_chat(%s) failed: %s", peer_id, e,
        )

    # Fall back to get_users (only works for users, not groups/channels)
    try:
        resolved = await client.get_users(peer_id)
        if resolved is not None:
            cached_id = getattr(resolved, "id", None)
            if cached_id is not None:
                logger.debug(
                    "resolve_pyrogram_peer: get_users(%s) -> id=%s",
                    peer_id, cached_id,
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "resolve_pyrogram_peer: get_users(%s) failed: %s", peer_id, e,
        )

    # Could not resolve; return original ID (get_messages will fail gracefully)
    logger.info(
        "resolve_pyrogram_peer: could not resolve %s, will try as-is", peer_id,
    )
    return peer_id


async def _download_with_telethon(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Download using Telethon client.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.
    """
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
                        kwargs = {"file": dest_path}
                        if progress_callback is not None:
                            kwargs["progress_callback"] = progress_callback
                        await client.download_media(msg, **kwargs)
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
                            kwargs = {"file": dest_path}
                            if progress_callback is not None:
                                kwargs["progress_callback"] = progress_callback
                            await client.download_media(m, **kwargs)
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
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Optional[bytes]:
    """Download a message's media into memory (bytes) using Pyrogram.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.
    """
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

        # Resolve the peer to cache its access_hash (prevents PEER_ID_INVALID)
        _candidates = [await _resolve_pyrogram_peer(client, target)]

        for _peer in _candidates:
            try:
                messages = await client.get_messages(_peer, message_ids=[message_id])
                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg and getattr(msg, "media", None):
                        kwargs = {"in_memory": True}
                        if progress_callback is not None:
                            kwargs["progress"] = progress_callback
                        data = await client.download_media(msg, **kwargs)
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
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Download using Pyrogram client (session string fallback).

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.
    """
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

        # Resolve the peer to cache its access_hash (prevents PEER_ID_INVALID)
        # Note: userbots cannot interact with bot peers, so we only resolve
        # the original chat_id (skip the bot's user ID entirely).
        _candidates = [await _resolve_pyrogram_peer(client, target)]

        for _peer in _candidates:
            try:
                messages = await client.get_messages(_peer, message_ids=[message_id])
                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg and getattr(msg, "media", None):
                        kwargs = {"file_name": dest_path}
                        if progress_callback is not None:
                            kwargs["progress"] = progress_callback
                        _dl = await client.download_media(msg, **kwargs)
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


async def download_bytes_by_file_id_via_userbot(
    file_id: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Optional[bytes]:
    """Download a file directly by bot-API file_id using userbot.

    Uses Telethon's ``resolve_bot_file_id`` + ``download_file`` to get the raw
    bytes without needing a chat_id/message_id. Falls back to Pyrogram if
    Telethon is unavailable.

    Args:
        file_id: Telegram bot API file_id (the ``file_id`` field on a Document).
        progress_callback: Optional callable(current_bytes, total_bytes).

    Returns:
        Raw bytes of the file, or None on failure.
    """
    if TelegramClient is not None:
        try:
            from telethon.utils import resolve_bot_file_id
            from utils.telethon_session import (
                build_telethon_client,
                get_userbot_credentials,
                has_usable_telethon_session,
            )

            if not has_usable_telethon_session():
                logger.info("userbot: Telethon session not configured; cannot download by file_id")
            else:
                api_id, api_hash = get_userbot_credentials()
                client = build_telethon_client(api_id, api_hash)
                if client is not None:
                    try:
                        await client.start()
                        resolved = resolve_bot_file_id(file_id)
                        if resolved is not None:
                            location, file_size = resolved
                            # download_file expects input_location as first arg,
                            # file_size and progress_callback as kwargs.
                            dl_kwargs = {}
                            if progress_callback is not None:
                                dl_kwargs["progress_callback"] = progress_callback
                            data = await client.download_file(
                                location, file_size=file_size, **dl_kwargs
                            )
                            if data and len(data) > 0:
                                logger.info(
                                    "userbot: Telethon file_id download succeeded: %d bytes",
                                    len(data),
                                )
                                return data
                            logger.warning("userbot: Telethon file_id download returned empty")
                        else:
                            logger.warning("userbot: resolve_bot_file_id returned None for file_id")
                    except Exception as e:
                        logger.warning("userbot: Telethon file_id download error: %s", e)
                    finally:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("userbot: Telethon file_id setup error: %s", e)

    # Pyrogram does not have a resolve_bot_file_id equivalent for direct file_id
    # downloads.  The chat+message_id-based download functions remain available
    # (download_forward_via_userbot etc.) for Pyrogram users.  This file_id-only
    # path relies on Telethon's resolve_bot_file_id utility.
    if PyrogramClient is not None:
        logger.warning(
            "userbot: Pyrogram file_id download not supported (no resolve_bot_file_id); "
            "use download_forward_via_userbot with chat_id+message_id instead"
        )

    logger.warning("userbot: all file_id download methods failed for file_id=%s", file_id[:16])
    return None


async def download_forward_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Download a message media using a user account.

    Tries Telethon first (with string session or file-based session),
    then falls back to Pyrogram if a session string is configured.

    If ``progress_callback`` is provided, it will be forwarded to the
    underlying download method for real-time progress updates.

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

    # ── 1) Telethon (preferred: faster, better large-file support) ──
    if TelegramClient is not None and has_usable_telethon_session():
        try:
            result = await _download_with_telethon(
                chat_id, message_id, dest_path,
                msg_date, file_unique_id,
                progress_callback=progress_callback,
            )
            if result:
                return True
            logger.info("userbot: Telethon download failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon download error (%s); trying Pyrogram fallback", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon download")

    # ── 2) Pyrogram fallback ──
    pyrogram_session_configured = bool(get_pyrogram_session_string())
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            result = await _download_with_pyrogram(chat_id, message_id, dest_path, progress_callback=progress_callback)
            if result:
                return True
        except Exception as e:
            logger.warning("userbot: Pyrogram download error (%s)", e)

    logger.warning("userbot: all download methods failed for %s/%s", chat_id, message_id)
    return False


async def download_bytes_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Optional[bytes]:
    """Download a message media into memory (bytes) using userbot.

    Tries **Telethon** first (faster, better large-file support),
    falls back to Pyrogram with ``in_memory=True`` if Telethon is unavailable.

    If ``progress_callback`` is provided, it will be forwarded to the
    underlying download method for real-time progress updates.
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

    # ── 1) Telethon (preferred: faster, better large-file support) ──
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
                        kwargs = {"file": buf}
                        if progress_callback is not None:
                            kwargs["progress_callback"] = progress_callback
                        await _client.download_media(msg, **kwargs)
                        data = buf.getvalue()
                        if data and len(data) > 0:
                            logger.info(
                                "userbot: Telethon in-memory download succeeded: %d bytes",
                                len(data),
                            )
                            return data
                await _client.disconnect()
        except Exception as e:
            logger.warning("userbot: Telethon in-memory download error (%s); trying Pyrogram fallback", e)

    # ── 2) Pyrogram fallback ──
    pyrogram_session_configured = bool(get_pyrogram_session_string())
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            data = await _download_bytes_with_pyrogram(chat_id, message_id, progress_callback=progress_callback)
            if data is not None:
                return data
        except Exception as e:
            logger.warning("userbot: Pyrogram in-memory download error (%s)", e)

    logger.warning("userbot: all in-memory download methods failed for %s/%s", chat_id, message_id)
    return None
