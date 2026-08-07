"""Userbot downloader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot download files >50MB.
"""

import asyncio
import io
import logging
import os
import shutil
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

# Module-level default timeout for Telethon download operations.
# Configurable via TELETHON_DOWNLOAD_TIMEOUT env var (default 600s = 10 min).
TELETHON_DOWNLOAD_TIMEOUT = int(os.getenv("TELETHON_DOWNLOAD_TIMEOUT", "600"))

# Module-level default timeout for Pyrogram download operations.
# Configurable via PYROGRAM_DOWNLOAD_TIMEOUT env var (default 600s = 10 min).
PYROGRAM_DOWNLOAD_TIMEOUT = int(os.getenv("PYROGRAM_DOWNLOAD_TIMEOUT", "600"))

# Check if PyMuPDF (fitz) is available for PDF validation after download.
# If not installed, PDF validation is skipped and all downloads are accepted
# at the file-exists level (graceful degradation — the error will surface later
# when thumbnail creation is attempted).
try:
    import fitz as _fitz

    _FITZ_AVAILABLE = True
except ImportError:
    _FITZ_AVAILABLE = False


def _get_bot_user_id() -> int | None:
    """Extract the bot's user ID from the BOT_TOKEN environment variable.

    When the Bot API reports ``chat_id == user_id`` (i.e. the user's ID in a DM),
    MTProto clients (Telethon/Pyrogram) need the **bot's user ID** to access
    those same messages from the bot's chat.  This helper extracts the bot's
    numeric ID from the first segment of the BOT_TOKEN.

    Returns:
        The bot user ID (int), or None if BOT_TOKEN is not set or malformed.
    """
    token = os.getenv("BOT_TOKEN", "")
    if ":" in token:
        try:
            return int(token.split(":")[0])
        except (ValueError, IndexError):
            pass
    return None


def _is_user_dm_chat(chat_id: int | str) -> bool:
    """Return True if ``chat_id`` looks like a user-to-bot DM chat.

    In the Bot API, DMs use the user's Telegram ID as the ``chat_id``,
    which is always a positive integer.  Negative IDs are groups/channels.
    """
    try:
        cid = int(chat_id)
        return cid > 0
    except (TypeError, ValueError):
        return False


def _extract_file_dc_id(msg) -> int | None:
    """Extract the Telegram DC ID where the file in ``msg`` is stored.

    Telethon message media objects contain a ``dc_id`` attribute that tells
    which Telegram DC (data center) the file resides on.  By migrating the
    client to that DC before downloading, we avoid ``FILE_MIGRATE_X`` errors
    and the associated timeout/retry storms that happen during cross-DC file
    transfers.

    Args:
        msg: A Telethon ``Message`` object with ``media``.

    Returns:
        The DC ID (int) if found, else None.
    """
    if msg is None:
        return None
    media = getattr(msg, "media", None)
    if media is None:
        return None

    # Document (files, stickers, voice, video)
    doc = getattr(media, "document", None)
    if doc is not None:
        dc_id = getattr(doc, "dc_id", None)
        if dc_id:
            return dc_id

    # Photo
    photo = getattr(media, "photo", None)
    if photo is not None:
        dc_id = getattr(photo, "dc_id", None)
        if dc_id:
            return dc_id

    # WebPage (link previews with media)
    webpage = getattr(media, "webpage", None)
    if webpage is not None:
        for attr in ("photo", "document"):
            sub = getattr(webpage, attr, None)
            if sub is not None:
                dc_id = getattr(sub, "dc_id", None)
                if dc_id:
                    return dc_id

    return None


async def _normalize_target(chat_id: int | str, client=None):
    """Return a compatible target entity for ``chat_id``."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


async def _resolve_pyrogram_peer(client, peer_id: int | str) -> int:
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
                    peer_id,
                    cached_id,
                    getattr(resolved, "_", type(resolved).__name__),
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "resolve_pyrogram_peer: get_chat(%s) failed: %s",
            peer_id,
            e,
        )

    # Fall back to get_users (only works for users, not groups/channels)
    try:
        resolved = await client.get_users(peer_id)
        if resolved is not None:
            cached_id = getattr(resolved, "id", None)
            if cached_id is not None:
                logger.debug(
                    "resolve_pyrogram_peer: get_users(%s) -> id=%s",
                    peer_id,
                    cached_id,
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "resolve_pyrogram_peer: get_users(%s) failed: %s",
            peer_id,
            e,
        )

    # Could not resolve; return original ID (get_messages will fail gracefully)
    logger.info(
        "resolve_pyrogram_peer: could not resolve %s, will try as-is",
        peer_id,
    )
    return peer_id


# ── Large Bot API channel helpers ─────────────────────────────────
# Pyrogram 2.0.106's ``get_peer_type()`` has a hardcoded range check that
# only accepts channel IDs whose raw ``channel_id <= 2147483647``.
# Channels with larger raw IDs (e.g. 4367325292) are rejected with
# ``Peer id invalid`` BEFORE any network request is made.
# The helpers below bypass this via raw MTProto API.
# Adapted from the media_conersion_bot reference implementation.


def _is_likely_pdf(path: str) -> bool:
    """Return True if the file path has a .pdf extension."""
    return path.lower().endswith(".pdf")


def _validate_downloaded_pdf(path: str) -> bool:
    """Validate a downloaded PDF file by attempting to open it with PyMuPDF.

    Returns True if the file is a valid PDF, False otherwise.
    Logs a warning if validation fails.

    If PyMuPDF (fitz) is not installed, skips validation and returns True
    (assumes valid — the error will surface later during thumbnail creation).
    """
    if not os.path.exists(path):
        return False
    if not _FITZ_AVAILABLE:
        return True  # can't validate, assume valid
    try:
        doc = _fitz.open(path)
        doc.close()
        return True
    except Exception as e:
        logger.warning(
            "userbot: downloaded PDF validation failed for %s: %s", path, e
        )
        return False


def _is_large_bot_api_channel(peer_id) -> bool:
    """Return True if ``peer_id`` is a Bot API channel ID whose raw
    channel_id exceeds Pyrogram's 32-bit range check (2147483647)."""
    if not isinstance(peer_id, int) or peer_id >= 0:
        return False
    s = str(peer_id)
    if not s.startswith("-100"):
        return False
    raw_id = abs(peer_id) - 1000000000000
    return raw_id > 2147483647


async def _resolve_bot_api_channel_raw(client, bot_api_chat_id: int):
    """Resolve a Bot API channel ID (-100xxxxx...) via raw MTProto API.

    Invokes `channels.GetChannels` with ``access_hash=0`` so the server
    responds with the correct access_hash, bypassing Pyrogram's peer
    type validation.

    Returns an ``InputPeerChannel`` on success, or ``None`` on failure.
    """
    from pyrogram import raw

    raw_channel_id = abs(bot_api_chat_id) - 1000000000000
    try:
        result = await client.invoke(
            raw.functions.channels.GetChannels(
                id=[
                    raw.types.InputChannel(
                        channel_id=raw_channel_id,
                        access_hash=0,
                    )
                ]
            )
        )
        if result and result.chats:
            chat = result.chats[0]
            access_hash = getattr(chat, "access_hash", 0)
            logger.info(
                "userbot: resolved large channel %s -> channel_id=%s access_hash=%s",
                bot_api_chat_id,
                raw_channel_id,
                access_hash,
            )
            return raw.types.InputPeerChannel(
                channel_id=raw_channel_id,
                access_hash=access_hash,
            )
    except Exception as e:
        logger.warning(
            "userbot: failed to resolve large channel %s via raw API: %s",
            bot_api_chat_id,
            e,
        )
    return None


async def _get_message_via_raw_channel_api(
    client, channel_peer, message_id: int
):
    """Get a single message from a resolved channel peer using raw MTProto API.

    Returns the Pyrogram ``Message`` object on success, or ``None``.
    """
    from pyrogram import raw
    from pyrogram import types as pyro_types

    try:
        r = await client.invoke(
            raw.functions.channels.GetMessages(
                channel=channel_peer,
                id=[raw.types.InputMessageID(id=message_id)],
            )
        )
        if r and r.messages:
            users = {i.id: i for i in r.users}
            chats = {i.id: i for i in r.chats}
            msg = await pyro_types.Message._parse(
                client,
                r.messages[0],
                users,
                chats,
                replies=0,
            )
            return msg
    except Exception as e:
        logger.warning(
            "userbot: GetMessages via raw API failed for msg %s: %s",
            message_id,
            e,
        )
    return None


async def _download_from_raw_channel(
    client,
    bot_api_chat_id: int,
    message_id: int,
    dest_path: str,
    progress_callback=None,
) -> bool:
    """Try to download a message from a large Bot API channel via raw API.

    Handles path reconciliation (Pyrogram may resolve relative paths
    differently) and returns True on success, False on failure.

    Features exponential backoff between retries.
    """
    for attempt in range(3):
        channel_peer = await _resolve_bot_api_channel_raw(
            client, bot_api_chat_id
        )
        if channel_peer is None:
            await asyncio.sleep(2**attempt)
            continue
        msg = await _get_message_via_raw_channel_api(
            client, channel_peer, message_id
        )
        if msg is None or not getattr(msg, "media", None):
            await asyncio.sleep(2**attempt)
            continue
        kwargs = {"file_name": dest_path}
        if progress_callback is not None:
            kwargs["progress"] = progress_callback
        try:
            _dl = await asyncio.wait_for(
                client.download_media(msg, **kwargs),
                timeout=PYROGRAM_DOWNLOAD_TIMEOUT,
            )
        except Exception as e:
            logger.warning(
                "userbot: raw channel download attempt %s failed: %s",
                attempt + 1,
                e,
            )
            await asyncio.sleep(2**attempt)
            continue
        if not _dl:
            await asyncio.sleep(2**attempt)
            continue
        _dl_path = str(_dl)
        _abs_dest = os.path.abspath(dest_path)
        if _dl_path != _abs_dest and not os.path.exists(dest_path):
            if os.path.exists(_dl_path):
                try:
                    shutil.move(_dl_path, _abs_dest)
                except Exception:  # nosec B110
                    pass
        if os.path.exists(_abs_dest) and os.path.getsize(_abs_dest) > 0:
            # Validate PDF files to catch corrupted/incomplete downloads
            if _is_likely_pdf(_abs_dest) and not _validate_downloaded_pdf(
                _abs_dest
            ):
                logger.warning(
                    "userbot: raw channel PDF is corrupted/invalid (attempt %s), removing and retrying",
                    attempt + 1,
                )
                try:
                    os.remove(_abs_dest)
                except Exception:  # nosec B110
                    pass
                await asyncio.sleep(2**attempt)
                continue
            return True
        await asyncio.sleep(2**attempt)
    return False


async def _download_bytes_from_raw_channel(
    client,
    bot_api_chat_id: int,
    message_id: int,
    progress_callback=None,
) -> bytes | None:
    """Try to in-memory download a message from a large Bot API channel via raw API.

    Returns bytes on success, or None.

    Features exponential backoff between retries.
    """
    for attempt in range(3):
        channel_peer = await _resolve_bot_api_channel_raw(
            client, bot_api_chat_id
        )
        if channel_peer is None:
            await asyncio.sleep(2**attempt)
            continue
        msg = await _get_message_via_raw_channel_api(
            client, channel_peer, message_id
        )
        if msg is None or not getattr(msg, "media", None):
            await asyncio.sleep(2**attempt)
            continue
        try:
            data = await asyncio.wait_for(
                client.download_media(msg, in_memory=True),
                timeout=PYROGRAM_DOWNLOAD_TIMEOUT,
            )
        except Exception as e:
            logger.warning(
                "userbot: raw channel bytes download attempt %s failed: %s",
                attempt + 1,
                e,
            )
            await asyncio.sleep(2**attempt)
            continue
        if data is not None and isinstance(data, bytes) and len(data) > 0:
            return data
        await asyncio.sleep(2**attempt)
    return None


async def _download_file_by_file_id(
    file_id: str,
    dest_path: str,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bool:
    """Download a file directly by Bot API file_id using Telethon's resolve_bot_file_id.

    This bypasses chat/message resolution entirely and is the fastest path for
    downloading files. Works regardless of whether the userbot has joined the
    source chat, because it uses the raw file location embedded in the Bot API
    file_id.

    Args:
        file_id: Telegram Bot API file_id.
        dest_path: Local path to save the downloaded file.
        progress_callback: Optional callable(current_bytes, total_bytes).

    Returns:
        True on success, False on failure.
    """
    if TelegramClient is None:
        logger.debug(
            "userbot: Telethon not installed; cannot download by file_id"
        )
        return False

    from telethon.utils import resolve_bot_file_id

    from utils.telethon_session import (
        build_telethon_client,
        get_telethon_session_string_for_user,
        get_userbot_credentials,
    )

    api_id, api_hash = get_userbot_credentials()

    _session_str = await get_telethon_session_string_for_user(user_id=user_id)
    client = build_telethon_client(api_id, api_hash, session_str=_session_str)
    try:
        await client.start()
    except Exception as e:
        logger.warning(
            "userbot: failed to start Telethon client for file_id download: %s",
            e,
        )
        return False

    try:
        resolved = resolve_bot_file_id(file_id)
        if resolved is None:
            logger.warning(
                "userbot: resolve_bot_file_id returned None for file_id (may be unsupported version)"
            )
            return False

        location, file_size = resolved
        logger.info(
            "userbot: file_id resolved to location (size=%s), downloading to %s",
            file_size,
            dest_path,
        )

        _dest_dir = os.path.dirname(dest_path)
        if _dest_dir:
            try:
                os.makedirs(_dest_dir, exist_ok=True)
            except Exception as e:
                logger.warning(
                    "userbot: could not create dest dir %s: %s", _dest_dir, e
                )

        # download_file writes directly to the file path
        dl_kwargs = {"file": dest_path}
        if progress_callback is not None:
            dl_kwargs["progress_callback"] = progress_callback

        await client.download_file(location, **dl_kwargs)

        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            # Validate PDF files to catch corrupted/incomplete downloads
            if _is_likely_pdf(dest_path) and not _validate_downloaded_pdf(
                dest_path
            ):
                logger.warning(
                    "userbot: file_id-downloaded PDF is corrupted/invalid, removing"
                )
                try:
                    os.remove(dest_path)
                except Exception:  # nosec B110
                    pass
                return False
            logger.info(
                "userbot: file_id download succeeded: %s (%d bytes)",
                dest_path,
                os.path.getsize(dest_path),
            )
            return True

        logger.warning(
            "userbot: file_id download produced empty file at %s", dest_path
        )
        return False
    except Exception as e:
        logger.warning("userbot: file_id download error: %s", e)
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:  # nosec B110
            pass


async def _resolve_telethon_entity(client, chat_id: int | str):
    """Resolve a chat/peer entity for Telethon with multiple fallback strategies.

    Telethon needs a cached entity (from ``get_entity`` or dialog iteration)
    to download messages from a chat. This function tries several approaches:
    1. Direct ``client.get_entity()`` with the original ID
    2. For channel IDs, try with ``-100`` prefix normalization
    3. Iterate through recent dialogs and match by ID

    Args:
        client: An active Telethon client.
        chat_id: Numeric chat ID or @username.

    Returns:
        Resolved entity on success, or None on failure.
    """
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        try:
            return await client.get_entity(chat_id)
        except Exception as e:
            logger.debug(
                "userbot: get_entity(@) failed for %s: %s", chat_id, e
            )
            return None

    # Strategy 1: Try direct get_entity with the raw ID
    try:
        return await client.get_entity(chat_id)
    except ValueError as e:
        err_str = str(e)
        if "Could not find the input entity" in err_str:
            logger.debug(
                "userbot: get_entity(%s) entity not found, trying alternative strategies",
                chat_id,
            )
        else:
            logger.debug("userbot: get_entity(%s) failed: %s", chat_id, e)
    except Exception as e:
        logger.debug("userbot: get_entity(%s) failed: %s", chat_id, e)

    # Strategy 2: For Bot API channel IDs (e.g. -100xxxxxxxxx), try resolving
    # by constructing the canonical peer and using raw API
    if isinstance(chat_id, int) and chat_id < 0:
        s = str(chat_id)
        if s.startswith("-100"):
            raw_id = abs(chat_id) - 1000000000000
            try:
                from telethon import types as t_types
                from telethon.tl.functions.channels import GetChannelsRequest

                peer = t_types.InputChannel(channel_id=raw_id, access_hash=0)
                result = await client(GetChannelsRequest(id=[peer]))
                if result and result.chats:
                    entity = result.chats[0]
                    logger.info(
                        "userbot: resolved channel via raw API: %s (id=%s)",
                        type(entity).__name__,
                        getattr(entity, "id", None),
                    )
                    return entity
            except Exception as e2:
                logger.debug(
                    "userbot: raw channel resolution failed for %s: %s",
                    chat_id,
                    e2,
                )

    # Strategy 3: Scan recent dialogs for a matching entity
    try:
        async for dialog in client.iter_dialogs(limit=200):
            if dialog and dialog.entity:
                eid = getattr(dialog.entity, "id", None)
                if eid and eid == abs(chat_id):
                    logger.info(
                        "userbot: resolved entity via dialog scan: %s (id=%s)",
                        type(dialog.entity).__name__,
                        eid,
                    )
                    return dialog.entity
    except Exception as e3:
        logger.debug("userbot: dialog scan failed: %s", e3)

    logger.warning("userbot: could not resolve entity for chat_id=%s", chat_id)
    return None


async def _download_with_telethon(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    msg_date: str | None = None,
    file_unique_id: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    file_id: str | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bool:
    """Download using Telethon client.

    If ``file_id`` is provided, tries direct file_id-based download first
    (fastest path, bypasses chat resolution entirely). Falls back to
    chat-based download with smart entity resolution if file_id is not
    available or fails.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Features exponential backoff between retries and a total retry budget
    to prevent infinite retry storms when Telegram's DC is having issues.
    """
    if TelegramClient is None:
        logger.debug("Telethon not installed; skipping Telethon download")
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_userbot_credentials,
        resolve_session_string,
    )

    api_id, api_hash = get_userbot_credentials()

    # Use the caller's pre-resolved session string when provided; fall back to
    # resolving it here (direct callers) so it is never resolved twice.
    session_str = await resolve_session_string(
        "telethon", session_str=session_str, user_id=user_id
    )
    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    try:
        logger.info("userbot: starting Telethon client for download")
        await client.start()
        logger.info("userbot: Telethon client started successfully")
    except Exception as e:
        logger.exception("userbot: failed to start Telethon client: %s", e)
        return False

    # Cap total download attempts to prevent infinite retry storms.
    MAX_TOTAL_ATTEMPTS = int(os.getenv("TELETHON_MAX_RETRY_ATTEMPTS", "20"))
    # Total timeout for the entire download operation (used with asyncio.wait_for).
    # 600s = 10 minutes for files up to ~200MB. Adjust via env for faster/slower connections.
    DOWNLOAD_TOTAL_TIMEOUT = TELETHON_DOWNLOAD_TIMEOUT
    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception:  # nosec B110
            pass

    try:
        target = await _normalize_target(chat_id, client)

        total_attempts = 0

        # Use smart entity resolution for better channel/chat handling
        resolved_entity = await _resolve_telethon_entity(client, chat_id)
        if resolved_entity is not None:
            try:
                msgs = await client.get_messages(
                    resolved_entity, ids=message_id
                )
            except Exception as e:
                logger.warning(
                    "userbot: get_messages via resolved entity failed: %s; trying raw target",
                    e,
                )
                msgs = None
        else:
            msgs = None

        # If entity resolution didn't work, fall back to direct get_messages
        if msgs is None:
            try:
                msgs = await client.get_messages(target, ids=message_id)
            except Exception as e:
                logger.warning(
                    "userbot: get_messages direct by id failed: %s", e
                )
                msgs = None

        # ── DM fallback: Bot API chat_id maps to user ID in DMs, but MTProto
        # needs the **bot's** user ID.  Try resolving the bot from BOT_TOKEN.
        if msgs is None and _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                try:
                    logger.info(
                        "userbot: DM chat detected (chat_id=%s), trying bot entity (bot_id=%s)",
                        chat_id,
                        bot_user_id,
                    )
                    bot_entity = await client.get_entity(bot_user_id)
                    if bot_entity is not None:
                        logger.info(
                            "userbot: resolved bot entity, trying get_messages from bot DM"
                        )
                        msgs = await client.get_messages(
                            bot_entity, ids=message_id
                        )
                except Exception as e:
                    logger.warning(
                        "userbot: bot entity resolution failed: %s", e
                    )
                    msgs = None

        if msgs:
            msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
            if getattr(msg, "media", None):
                logger.info(
                    "userbot: message found; downloading %s/%s to %s",
                    target,
                    message_id,
                    dest_path,
                )

                # ── Pre-migrate to the file's DC before downloading ──
                # Cross-DC GetFileRequest timeouts are the #1 cause of download
                # failures for large files.  Extract the file's DC from the
                # message media and migrate the client there first.
                try:
                    _file_dc = _extract_file_dc_id(msg)
                    if _file_dc is not None:
                        logger.info(
                            "userbot: file DC ID=%s, ensuring client is on correct DC",
                            _file_dc,
                        )
                        await client._set_connection_dc(_file_dc)
                except Exception as dc_err:
                    logger.debug(
                        "userbot: DC pre-migration skipped: %s", dc_err
                    )

                for attempt in range(3):
                    total_attempts += 1
                    if total_attempts > MAX_TOTAL_ATTEMPTS:
                        logger.warning(
                            "userbot: hit max total attempts (%d), giving up on direct download",
                            MAX_TOTAL_ATTEMPTS,
                        )
                        break
                    try:
                        kwargs = {"file": dest_path}
                        if progress_callback is not None:
                            kwargs["progress_callback"] = progress_callback
                        # Wrap in asyncio.wait_for to enforce a total-download timeout
                        # and prevent hanging on large files that span multiple DCs.
                        # This is more portable than Telethon's native timeout param
                        # (which was added in a later version).
                        await asyncio.wait_for(
                            client.download_media(msg, **kwargs),
                            timeout=DOWNLOAD_TOTAL_TIMEOUT,
                        )
                        if (
                            os.path.exists(dest_path)
                            and os.path.getsize(dest_path) > 0
                        ):
                            if _is_likely_pdf(
                                dest_path
                            ) and not _validate_downloaded_pdf(dest_path):
                                logger.warning(
                                    "userbot: downloaded PDF is corrupted/invalid (attempt %s), removing and retrying",
                                    attempt + 1,
                                )
                                try:
                                    os.remove(dest_path)
                                except Exception:  # nosec B110
                                    pass
                                await asyncio.sleep(2**attempt)
                                continue
                            return True
                        logger.warning(
                            "userbot: downloaded file empty (attempt %s) %s",
                            attempt + 1,
                            dest_path,
                        )
                        try:
                            os.remove(dest_path)
                        except Exception:  # nosec B110
                            pass
                        await asyncio.sleep(2**attempt)
                    except Exception as e:
                        logger.exception(
                            "userbot: download attempt %s failed: %s",
                            attempt + 1,
                            e,
                        )
                        await asyncio.sleep(2**attempt)

        # Scan recent messages as fallback
        try:
            async for m in client.iter_messages(target, limit=200):
                if total_attempts > MAX_TOTAL_ATTEMPTS:
                    logger.warning(
                        "userbot: hit max total attempts (%d), giving up scan-fallback",
                        MAX_TOTAL_ATTEMPTS,
                    )
                    break
                if getattr(m, "media", None):
                    for attempt in range(3):
                        total_attempts += 1
                        if total_attempts > MAX_TOTAL_ATTEMPTS:
                            logger.warning(
                                "userbot: hit max total attempts (%d), giving up scan-fallback",
                                MAX_TOTAL_ATTEMPTS,
                            )
                            break
                        try:
                            kwargs = {"file": dest_path}
                            if progress_callback is not None:
                                kwargs["progress_callback"] = progress_callback
                            # Wrap in asyncio.wait_for to enforce total-download timeout
                            await asyncio.wait_for(
                                client.download_media(m, **kwargs),
                                timeout=DOWNLOAD_TOTAL_TIMEOUT,
                            )
                            if (
                                os.path.exists(dest_path)
                                and os.path.getsize(dest_path) > 0
                            ):
                                if _is_likely_pdf(
                                    dest_path
                                ) and not _validate_downloaded_pdf(dest_path):
                                    logger.warning(
                                        "userbot: scan-fallback downloaded PDF is corrupted/invalid, removing",
                                    )
                                    try:
                                        os.remove(dest_path)
                                    except Exception:  # nosec B110
                                        pass
                                    await asyncio.sleep(2**attempt)
                                    continue
                                return True
                            await asyncio.sleep(2**attempt)
                        except Exception:
                            await asyncio.sleep(2**attempt)
        except Exception:  # nosec B110
            pass

        logger.warning(
            "userbot: Telethon download failed after %d attempts (chat=%s msg=%s)",
            total_attempts,
            chat_id,
            message_id,
        )
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:  # nosec B110
            pass


async def _download_bytes_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bytes | None:
    """Download a message's media into memory (bytes) using Pyrogram.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Features exponential backoff between retries and a total retry budget
    to prevent infinite retry storms when Telegram's DC is having issues.
    """
    if PyrogramClient is None:
        logger.info(
            "userbot: Pyrogram not installed; cannot do in-memory download"
        )
        return None

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
        logger.info(
            "userbot: Pyrogram session string not configured; cannot do in-memory download"
        )
        return None

    MAX_TOTAL_ATTEMPTS = int(os.getenv("PYROGRAM_MAX_RETRY_ATTEMPTS", "6"))

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for in-memory download")

        target = await _normalize_target(chat_id)

        # Resolve the peer to cache its access_hash (prevents PEER_ID_INVALID)
        _candidates = [await _resolve_pyrogram_peer(client, target)]

        # ── DM fallback: if chat_id looks like a user ID (Bot API DM),
        # also try the bot's user ID so Pyrogram can access the bot's chat.
        if _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                bot_resolved = await _resolve_pyrogram_peer(
                    client, bot_user_id
                )
                if bot_resolved not in _candidates:
                    _candidates.append(bot_resolved)
                    logger.info(
                        "userbot: added bot user ID %s as candidate for in-memory DM download",
                        bot_user_id,
                    )

        total_attempts = 0

        for _peer in _candidates:
            for attempt in range(3):
                total_attempts += 1
                if total_attempts > MAX_TOTAL_ATTEMPTS:
                    logger.warning(
                        "userbot: Pyrogram in-memory hit max total attempts (%d), giving up",
                        MAX_TOTAL_ATTEMPTS,
                    )
                    break
                try:
                    messages = await client.get_messages(
                        _peer, message_ids=[message_id]
                    )
                    if messages:
                        msg = (
                            messages[0]
                            if isinstance(messages, list)
                            else messages
                        )
                        if msg and getattr(msg, "media", None):
                            kwargs = {"in_memory": True}
                            if progress_callback is not None:
                                kwargs["progress"] = progress_callback
                            data = await asyncio.wait_for(
                                client.download_media(msg, **kwargs),
                                timeout=PYROGRAM_DOWNLOAD_TIMEOUT,
                            )
                            if (
                                data is not None
                                and isinstance(data, bytes)
                                and len(data) > 0
                            ):
                                logger.info(
                                    "userbot: Pyrogram in-memory download succeeded: %d bytes",
                                    len(data),
                                )
                                return data
                        else:
                            break
                except ValueError as e:
                    err_str = str(e)
                    if (
                        "Peer id invalid" in err_str
                        and isinstance(_peer, int)
                        and _is_large_bot_api_channel(_peer)
                    ):
                        logger.info(
                            "userbot: large channel ID %s for in-memory, trying raw API (attempt %s)",
                            _peer,
                            attempt + 1,
                        )
                        data = await _download_bytes_from_raw_channel(
                            client,
                            _peer,
                            message_id,
                            progress_callback,
                        )
                        if data is not None:
                            return data
                    else:
                        logger.warning(
                            "userbot: Pyrogram in-memory error with peer=%s msg=%s: %s",
                            _peer,
                            message_id,
                            e,
                        )
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    logger.warning(
                        "userbot: Pyrogram in-memory error with peer=%s msg=%s: %s",
                        _peer,
                        message_id,
                        e,
                    )
                    await asyncio.sleep(2**attempt)

        logger.warning(
            "userbot: Pyrogram in-memory download failed after %d attempts (chat=%s msg=%s)",
            total_attempts,
            chat_id,
            message_id,
        )
        return None
    finally:
        try:
            await client.stop()
        except Exception:  # nosec B110
            pass


async def _download_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bool:
    """Download using Pyrogram client (session string fallback).

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Features exponential backoff between retries and a total retry budget
    to prevent infinite retry storms when Telegram's DC is having issues.
    """
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; skipping")
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
        logger.info("userbot: Pyrogram session string not configured")
        return False

    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception as e:
            logger.warning(
                "userbot: could not create dest dir %s: %s", _dest_dir, e
            )

    MAX_TOTAL_ATTEMPTS = int(os.getenv("PYROGRAM_MAX_RETRY_ATTEMPTS", "6"))

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for download")

        target = await _normalize_target(chat_id)

        # Resolve the peer to cache its access_hash (prevents PEER_ID_INVALID)
        _candidates = [await _resolve_pyrogram_peer(client, target)]

        # ── DM fallback: if chat_id looks like a user ID (Bot API DM),
        # also try the bot's user ID so Pyrogram can access the bot's chat.
        if _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                bot_resolved = await _resolve_pyrogram_peer(
                    client, bot_user_id
                )
                if bot_resolved not in _candidates:
                    _candidates.append(bot_resolved)
                    logger.info(
                        "userbot: added bot user ID %s as candidate for DM download",
                        bot_user_id,
                    )

        total_attempts = 0

        for _peer in _candidates:
            for attempt in range(3):
                total_attempts += 1
                if total_attempts > MAX_TOTAL_ATTEMPTS:
                    logger.warning(
                        "userbot: Pyrogram hit max total attempts (%d), giving up",
                        MAX_TOTAL_ATTEMPTS,
                    )
                    break
                try:
                    messages = await client.get_messages(
                        _peer, message_ids=[message_id]
                    )
                    if messages:
                        msg = (
                            messages[0]
                            if isinstance(messages, list)
                            else messages
                        )
                        if msg and getattr(msg, "media", None):
                            kwargs = {"file_name": dest_path}
                            if progress_callback is not None:
                                kwargs["progress"] = progress_callback
                            _dl = await asyncio.wait_for(
                                client.download_media(msg, **kwargs),
                                timeout=PYROGRAM_DOWNLOAD_TIMEOUT,
                            )
                            if _dl:
                                _dl_path = str(_dl)
                                _abs_dest = os.path.abspath(dest_path)
                                if (
                                    _dl_path != _abs_dest
                                    and not os.path.exists(dest_path)
                                ):
                                    if os.path.exists(_dl_path):
                                        shutil.move(_dl_path, _abs_dest)
                                if (
                                    os.path.exists(_abs_dest)
                                    and os.path.getsize(_abs_dest) > 0
                                ):
                                    if _is_likely_pdf(
                                        _abs_dest
                                    ) and not _validate_downloaded_pdf(
                                        _abs_dest
                                    ):
                                        logger.warning(
                                            "userbot: Pyrogram downloaded PDF is corrupted/invalid "
                                            "(attempt %s), removing and retrying",
                                            attempt + 1,
                                        )
                                        try:
                                            os.remove(_abs_dest)
                                        except Exception:  # nosec B110
                                            pass
                                        await asyncio.sleep(2**attempt)
                                        continue
                                    return True
                            if os.path.exists(dest_path):
                                try:
                                    os.remove(dest_path)
                                except Exception:  # nosec B110
                                    pass
                        else:
                            break
                except ValueError as e:
                    err_str = str(e)
                    if (
                        "Peer id invalid" in err_str
                        and isinstance(_peer, int)
                        and _is_large_bot_api_channel(_peer)
                    ):
                        logger.info(
                            "userbot: large channel ID %s, trying raw API (attempt %s)",
                            _peer,
                            attempt + 1,
                        )
                        if await _download_from_raw_channel(
                            client,
                            _peer,
                            message_id,
                            dest_path,
                            progress_callback,
                        ):
                            return True
                    else:
                        logger.warning(
                            "userbot: Pyrogram error with peer=%s msg=%s: %s",
                            _peer,
                            message_id,
                            e,
                        )
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    logger.warning(
                        "userbot: Pyrogram error with peer=%s msg=%s: %s",
                        _peer,
                        message_id,
                        e,
                    )
                    await asyncio.sleep(2**attempt)

        logger.warning(
            "userbot: Pyrogram download failed after %d attempts (chat=%s msg=%s)",
            total_attempts,
            chat_id,
            message_id,
        )
        return False
    finally:
        try:
            await client.stop()
        except Exception:  # nosec B110
            pass


async def download_bytes_by_file_id_via_userbot(
    file_id: str,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bytes | None:
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
                get_telethon_session_string_for_user,
                get_userbot_credentials,
            )

            _session_str = await get_telethon_session_string_for_user(
                user_id=user_id
            )
            if not _session_str:
                logger.info(
                    "userbot: Telethon session not configured; cannot download by file_id"
                )
            else:
                api_id, api_hash = get_userbot_credentials()
                client = build_telethon_client(
                    api_id, api_hash, session_str=_session_str
                )
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
                                dl_kwargs["progress_callback"] = (
                                    progress_callback
                                )
                            data = await client.download_file(
                                location, file_size=file_size, **dl_kwargs
                            )
                            if data and len(data) > 0:
                                logger.info(
                                    "userbot: Telethon file_id download succeeded: %d bytes",
                                    len(data),
                                )
                                return data
                            logger.warning(
                                "userbot: Telethon file_id download returned empty"
                            )
                        else:
                            logger.warning(
                                "userbot: resolve_bot_file_id returned None for file_id"
                            )
                    except Exception as e:
                        logger.warning(
                            "userbot: Telethon file_id download error: %s", e
                        )
                    finally:
                        try:
                            await client.disconnect()
                        except Exception:  # nosec B110
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

    logger.warning(
        "userbot: all file_id download methods failed for file_id=%s",
        file_id[:16],
    )
    return None


async def download_forward_via_userbot(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    msg_date: str | None = None,
    file_unique_id: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    file_id: str | None = None,
    user_id: int | None = None,
) -> bool:
    """Download a message media using a user account.

    Tries file_id-based download first (if ``file_id`` provided, this bypasses
    chat resolution entirely and is the fastest path), then Telethon, then
    Pyrogram session string fallback.

    When called with ``chat_id=0, message_id=0`` (sentinel values used when
    only ``file_id`` is available), the chat-based fallbacks are skipped
    entirely to avoid wasting time on invalid IDs.

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
        get_pyrogram_session_string_for_user,
        get_telethon_session_string_for_user,
    )

    # Note: file_id-based download via _download_file_by_file_id() was removed
    # because modern Bot API file_id formats (v4+) are not supported by
    # Telethon's resolve_bot_file_id utility.  The function still exists for
    # potential future use or manual invocation.

    # ── Sentinel check: if chat_id=0 and message_id=0, no chat context is
    # available — skip all chat-based downloads.
    _only_file_id = (chat_id == 0 or str(chat_id) == "0") and (
        message_id == 0 or str(message_id) == "0"
    )
    if _only_file_id:
        logger.info(
            "userbot: sentinel chat_id/message_id detected, no chat context available"
        )
        logger.warning(
            "userbot: all download methods failed (no chat context)"
        )
        return False

    # ── 1) Telethon (preferred: faster, better large-file support) ──
    _tele_session = await get_telethon_session_string_for_user(user_id=user_id)
    if TelegramClient is not None and _tele_session:
        try:
            result = await _download_with_telethon(
                chat_id,
                message_id,
                dest_path,
                msg_date,
                file_unique_id,
                progress_callback=progress_callback,
                file_id=file_id,
                user_id=user_id,
                session_str=_tele_session,
            )
            if result:
                return True
            logger.info(
                "userbot: Telethon download failed; trying Pyrogram fallback"
            )
        except Exception as e:
            logger.warning(
                "userbot: Telethon download error (%s); trying Pyrogram fallback",
                e,
            )
    elif TelegramClient is not None:
        logger.info(
            "userbot: Telethon session not configured; skipping Telethon download"
        )

    # ── 2) Pyrogram fallback (if configured) ──
    _pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id)
    if PyrogramClient is not None and _pyro_session:
        try:
            result = await _download_with_pyrogram(
                chat_id,
                message_id,
                dest_path,
                progress_callback=progress_callback,
                user_id=user_id,
                session_str=_pyro_session,
            )
            if result:
                return True
        except Exception as e:
            logger.warning("userbot: Pyrogram download error (%s)", e)

    logger.warning(
        "userbot: all download methods failed for %s/%s", chat_id, message_id
    )
    return False


async def download_bytes_via_userbot(
    chat_id: int | str,
    message_id: int,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bytes | None:
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
        get_pyrogram_session_string_for_user,
        get_telethon_session_string_for_user,
    )

    # ── 1) Telethon (preferred: faster, better large-file support) ──
    _tele_session = await get_telethon_session_string_for_user(user_id=user_id)
    if TelegramClient is not None and _tele_session:
        try:
            from utils.telethon_session import build_telethon_client
            from utils.telethon_session import (
                get_userbot_credentials as _get_creds,
            )

            _api_id, _api_hash = _get_creds()
            # Reuse the session string already resolved by the gate.
            _session_str = _tele_session
            _client = build_telethon_client(
                _api_id, _api_hash, session_str=_session_str
            )
            if _client is not None:
                try:
                    await _client.start()
                    target = await _normalize_target(chat_id, _client)
                    try:
                        msgs = await _client.get_messages(
                            target, ids=message_id
                        )
                    except Exception as e:
                        logger.warning(
                            "userbot: Telethon in-memory get_messages failed: %s",
                            e,
                        )
                        msgs = None

                    # ── DM fallback: try bot entity for in-memory download too
                    if not msgs and _is_user_dm_chat(chat_id):
                        bot_user_id = _get_bot_user_id()
                        if bot_user_id is not None and bot_user_id != abs(
                            int(chat_id)
                        ):
                            try:
                                bot_entity = await _client.get_entity(
                                    bot_user_id
                                )
                                if bot_entity is not None:
                                    logger.info(
                                        "userbot: in-memory DM fallback, trying bot entity %s",
                                        bot_user_id,
                                    )
                                    msgs = await _client.get_messages(
                                        bot_entity, ids=message_id
                                    )
                            except Exception as e:
                                logger.warning(
                                    "userbot: in-memory bot entity resolution failed: %s",
                                    e,
                                )
                                msgs = None

                    if msgs:
                        msg = (
                            msgs[0]
                            if isinstance(msgs, (list, tuple))
                            else msgs
                        )
                        if getattr(msg, "media", None):
                            # ── Pre-migrate to the file's DC before downloading ──
                            try:
                                _file_dc = _extract_file_dc_id(msg)
                                if _file_dc is not None:
                                    logger.info(
                                        "userbot: in-memory file DC ID=%s, migrating",
                                        _file_dc,
                                    )
                                    await _client._set_connection_dc(_file_dc)
                            except Exception:  # nosec B110
                                pass

                            for attempt in range(3):
                                try:
                                    buf = io.BytesIO()
                                    kwargs = {"file": buf}
                                    if progress_callback is not None:
                                        kwargs["progress_callback"] = (
                                            progress_callback
                                        )
                                    await asyncio.wait_for(
                                        _client.download_media(msg, **kwargs),
                                        timeout=TELETHON_DOWNLOAD_TIMEOUT,
                                    )
                                    data = buf.getvalue()
                                    if data and len(data) > 0:
                                        logger.info(
                                            "userbot: Telethon in-memory download succeeded: %d bytes",
                                            len(data),
                                        )
                                        return data
                                    logger.warning(
                                        "userbot: Telethon in-memory download empty (attempt %s)",
                                        attempt + 1,
                                    )
                                except Exception as e:
                                    logger.warning(
                                        "userbot: Telethon in-memory download attempt %s failed: %s",
                                        attempt + 1,
                                        e,
                                    )
                                await asyncio.sleep(2**attempt)
                finally:
                    try:
                        await _client.disconnect()
                    except Exception:  # nosec B110
                        pass
        except Exception as e:
            logger.warning(
                "userbot: Telethon in-memory download error (%s); trying Pyrogram fallback",
                e,
            )

    # ── 2) Pyrogram fallback ──
    _pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id)
    if PyrogramClient is not None and _pyro_session:
        try:
            data = await _download_bytes_with_pyrogram(
                chat_id,
                message_id,
                progress_callback=progress_callback,
                user_id=user_id,
                session_str=_pyro_session,
            )
            if data is not None:
                return data
        except Exception as e:
            logger.warning(
                "userbot: Pyrogram in-memory download error (%s)", e
            )

    logger.warning(
        "userbot: all in-memory download methods failed for %s/%s",
        chat_id,
        message_id,
    )
    return None
