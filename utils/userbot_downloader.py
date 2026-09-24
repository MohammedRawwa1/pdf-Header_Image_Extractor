"""Userbot downloader for large PDF files via Telethon/Pyrogram.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
Used when Telegram Bot API cannot download files >20MB.
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


TELETHON_DOWNLOAD_TIMEOUT = int(os.getenv("TELETHON_DOWNLOAD_TIMEOUT", "600"))


PYROGRAM_DOWNLOAD_TIMEOUT = int(os.getenv("PYROGRAM_DOWNLOAD_TIMEOUT", "600"))


TELETHON_DOWNLOAD_PART_SIZE_KB = int(
    os.getenv("TELETHON_DOWNLOAD_PART_SIZE_KB", "512")
)


async def _download_media_with_part_size(client, msg, **kwargs):

    part_size_kb = kwargs.pop("part_size_kb", None)
    location = None
    if part_size_kb:
        try:
            from telethon.utils import get_input_location

            dc_id, location = get_input_location(msg)
        except Exception:
            location = None
    if location is not None:
        kwargs["part_size_kb"] = part_size_kb
        kwargs["dc_id"] = dc_id

        return await client.download_file(location, **kwargs)
    return await client.download_media(msg, **kwargs)


try:
    import fitz as _fitz

    _FITZ_AVAILABLE = True
except ImportError:
    _FITZ_AVAILABLE = False


def _get_bot_user_id() -> int | None:

    token = os.getenv("BOT_TOKEN", "")
    if ":" in token:
        try:
            return int(token.split(":")[0])
        except (ValueError, IndexError):
            pass
    return None


def _is_user_dm_chat(chat_id: int | str) -> bool:

    try:
        cid = int(chat_id)
        return cid > 0
    except (TypeError, ValueError):
        return False


def _extract_file_dc_id(msg) -> int | None:

    if msg is None:
        return None
    media = getattr(msg, "media", None)
    if media is None:
        return None

    doc = getattr(media, "document", None)
    if doc is not None:
        dc_id = getattr(doc, "dc_id", None)
        if dc_id:
            return dc_id

    photo = getattr(media, "photo", None)
    if photo is not None:
        dc_id = getattr(photo, "dc_id", None)
        if dc_id:
            return dc_id

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

    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


async def _resolve_pyrogram_peer(client, peer_id: int | str) -> int:

    if not isinstance(peer_id, int):
        return peer_id

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

    try:
        _dialogs = await client.get_dialogs(limit=200)
        for _d in _dialogs:
            _chat = getattr(_d, "chat", None)
            if _chat is not None and getattr(_chat, "id", None) == peer_id:
                logger.info(
                    "resolve_pyrogram_peer: found %s via dialog scan",
                    peer_id,
                )
                return getattr(_chat, "id", None) or peer_id
    except Exception as e:
        logger.debug("resolve_pyrogram_peer: dialog scan failed: %s", e)

    logger.info(
        "resolve_pyrogram_peer: could not resolve %s, will try as-is",
        peer_id,
    )
    return peer_id


def _is_likely_pdf(path: str) -> bool:

    return path.lower().endswith(".pdf")


def _validate_downloaded_pdf(path: str) -> bool:

    if not os.path.exists(path):
        return False
    if not _FITZ_AVAILABLE:
        return True
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

    if not isinstance(peer_id, int) or peer_id >= 0:
        return False
    s = str(peer_id)
    if not s.startswith("-100"):
        return False
    raw_id = abs(peer_id) - 1000000000000
    return raw_id > 2147483647


async def _resolve_bot_api_channel_raw(client, bot_api_chat_id: int):

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
                except Exception:
                    pass
        if os.path.exists(_abs_dest) and os.path.getsize(_abs_dest) > 0:
            if _is_likely_pdf(_abs_dest) and not _validate_downloaded_pdf(
                _abs_dest
            ):
                logger.warning(
                    "userbot: raw channel PDF is corrupted/invalid (attempt %s), removing and retrying",
                    attempt + 1,
                )
                try:
                    os.remove(_abs_dest)
                except Exception:
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

        dl_kwargs = {
            "file": dest_path,
            "part_size_kb": TELETHON_DOWNLOAD_PART_SIZE_KB,
        }
        if progress_callback is not None:
            dl_kwargs["progress_callback"] = progress_callback

        await client.download_file(location, **dl_kwargs)

        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            if _is_likely_pdf(dest_path) and not _validate_downloaded_pdf(
                dest_path
            ):
                logger.warning(
                    "userbot: file_id-downloaded PDF is corrupted/invalid, removing"
                )
                try:
                    os.remove(dest_path)
                except Exception:
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
        except Exception:
            pass


async def _resolve_telethon_entity(client, chat_id: int | str):

    if isinstance(chat_id, str) and chat_id.startswith("@"):
        try:
            return await client.get_entity(chat_id)
        except Exception as e:
            logger.debug(
                "userbot: get_entity(@) failed for %s: %s", chat_id, e
            )
            return None

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


async def _resolve_bot_entity(client):

    bot_user_id = _get_bot_user_id()
    if bot_user_id is None:
        return None

    try:
        entity = await client.get_entity(bot_user_id)
        if entity is not None:
            return entity
    except Exception:
        pass

    try:
        async for dialog in client.iter_dialogs(limit=300):
            ent = getattr(dialog, "entity", None)
            if ent is not None and getattr(ent, "id", None) == bot_user_id:
                logger.info(
                    "userbot: resolved bot entity %s via dialog scan",
                    bot_user_id,
                )
                return ent
    except Exception:
        pass

    _username = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
    if _username:
        try:
            return await client.get_entity(_username)
        except Exception:
            pass
    logger.warning("userbot: could not resolve bot entity %s", bot_user_id)
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

    if TelegramClient is None:
        logger.debug("Telethon not installed; skipping Telethon download")
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_userbot_credentials,
        resolve_session_string,
    )

    api_id, api_hash = get_userbot_credentials()

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

    MAX_TOTAL_ATTEMPTS = int(os.getenv("TELETHON_MAX_RETRY_ATTEMPTS", "20"))

    DOWNLOAD_TOTAL_TIMEOUT = TELETHON_DOWNLOAD_TIMEOUT
    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception:
            pass

    try:
        target = await _normalize_target(chat_id, client)

        total_attempts = 0

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

        if msgs is None:
            try:
                msgs = await client.get_messages(target, ids=message_id)
            except Exception as e:
                logger.warning(
                    "userbot: get_messages direct by id failed: %s", e
                )
                msgs = None

        if msgs is None and _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                logger.info(
                    "userbot: DM chat detected (chat_id=%s), resolving bot entity (bot_id=%s)",
                    chat_id,
                    bot_user_id,
                )
                bot_entity = await _resolve_bot_entity(client)
                if bot_entity is not None:
                    try:
                        logger.info(
                            "userbot: resolved bot entity, trying get_messages from bot DM"
                        )
                        msgs = await client.get_messages(
                            bot_entity, ids=message_id
                        )
                    except Exception as e:
                        logger.warning(
                            "userbot: get_messages from bot DM failed: %s", e
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
                        kwargs = {
                            "file": dest_path,
                            "part_size_kb": TELETHON_DOWNLOAD_PART_SIZE_KB,
                        }
                        if progress_callback is not None:
                            kwargs["progress_callback"] = progress_callback

                        await asyncio.wait_for(
                            _download_media_with_part_size(
                                client, msg, **kwargs
                            ),
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
                                except Exception:
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
                        except Exception:
                            pass
                        await asyncio.sleep(2**attempt)
                    except Exception as e:
                        logger.exception(
                            "userbot: download attempt %s failed: %s",
                            attempt + 1,
                            e,
                        )
                        await asyncio.sleep(2**attempt)

        _self_id = getattr(client, "_self_id", None)
        if _self_id is None:
            try:
                _me = await client.get_me()
                _self_id = getattr(_me, "id", None)
            except Exception:
                pass
        _target_is_self = False
        try:
            _target_is_self = _self_id is not None and str(
                abs(int(target))
            ) == str(abs(int(_self_id)))
        except (TypeError, ValueError):
            pass
        if _target_is_self:
            logger.warning(
                "userbot: scan target %s is the userbot's own account; "
                "skipping Saved Messages scan (would download the wrong file)",
                target,
            )
        else:
            _scan_ok = await _scan_fallback_download(
                client,
                target,
                dest_path,
                progress_callback,
                MAX_TOTAL_ATTEMPTS,
                DOWNLOAD_TOTAL_TIMEOUT,
            )
            if _scan_ok:
                return True

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
        except Exception:
            pass


async def _scan_fallback_download(
    client,
    target,
    dest_path: str,
    progress_callback: Callable[[int, int], None] | None,
    max_attempts: int,
    timeout: int,
) -> bool:

    total_attempts = 0
    try:
        async for m in client.iter_messages(target, limit=200):
            if total_attempts > max_attempts:
                logger.warning(
                    "userbot: hit max total attempts (%d), giving up scan-fallback",
                    max_attempts,
                )
                break
            if getattr(m, "media", None):
                for attempt in range(3):
                    total_attempts += 1
                    if total_attempts > max_attempts:
                        logger.warning(
                            "userbot: hit max total attempts (%d), giving up scan-fallback",
                            max_attempts,
                        )
                        break
                    try:
                        kwargs = {
                            "file": dest_path,
                            "part_size_kb": TELETHON_DOWNLOAD_PART_SIZE_KB,
                        }
                        if progress_callback is not None:
                            kwargs["progress_callback"] = progress_callback

                        await asyncio.wait_for(
                            _download_media_with_part_size(
                                client, m, **kwargs
                            ),
                            timeout=timeout,
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
                                except Exception:
                                    pass
                                await asyncio.sleep(2**attempt)
                                continue
                            return True
                        await asyncio.sleep(2**attempt)
                    except Exception:
                        await asyncio.sleep(2**attempt)
    except Exception:
        pass
    return False


async def _download_bytes_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bytes | None:

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

        _candidates = [await _resolve_pyrogram_peer(client, target)]

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
        except Exception:
            pass


async def _download_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
    session_str: str | None = None,
) -> bool:

    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; skipping")
        return False

    from utils.telethon_session import (
        build_pyrogram_client,
        get_userbot_credentials,
        resolve_session_string,
    )

    api_id, api_hash = get_userbot_credentials()

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

        _candidates = [await _resolve_pyrogram_peer(client, target)]

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
                                        except Exception:
                                            pass
                                        await asyncio.sleep(2**attempt)
                                        continue
                                    return True
                            if os.path.exists(dest_path):
                                try:
                                    os.remove(dest_path)
                                except Exception:
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
        except Exception:
            pass


async def download_bytes_by_file_id_via_userbot(
    file_id: str,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bytes | None:

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
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("userbot: Telethon file_id setup error: %s", e)

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

    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string_for_user,
        get_telethon_session_string_for_user,
    )

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
            from utils.telethon_session import build_telethon_client
            from utils.telethon_session import (
                get_userbot_credentials as _get_creds,
            )

            _api_id, _api_hash = _get_creds()

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

                    if not msgs and _is_user_dm_chat(chat_id):
                        bot_user_id = _get_bot_user_id()
                        if bot_user_id is not None and bot_user_id != abs(
                            int(chat_id)
                        ):
                            try:
                                bot_entity = await _resolve_bot_entity(_client)
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
                            try:
                                _file_dc = _extract_file_dc_id(msg)
                                if _file_dc is not None:
                                    logger.info(
                                        "userbot: in-memory file DC ID=%s, migrating",
                                        _file_dc,
                                    )
                                    await _client._set_connection_dc(_file_dc)
                            except Exception:
                                pass

                            for attempt in range(3):
                                try:
                                    buf = io.BytesIO()
                                    kwargs = {
                                        "file": buf,
                                        "part_size_kb": TELETHON_DOWNLOAD_PART_SIZE_KB,
                                    }
                                    if progress_callback is not None:
                                        kwargs["progress_callback"] = (
                                            progress_callback
                                        )
                                    await asyncio.wait_for(
                                        _download_media_with_part_size(
                                            _client, msg, **kwargs
                                        ),
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
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(
                "userbot: Telethon in-memory download error (%s); trying Pyrogram fallback",
                e,
            )

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
