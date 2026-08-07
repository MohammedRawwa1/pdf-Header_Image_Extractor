import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import aiofiles
import aiohttp
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
)
from telegram import BotCommand, InputFile, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Security helpers ────────────────────────────────────────────
_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._\- ]")
_MAX_FILENAME_LENGTH = 255


def _sanitize_filename(filename: str, default: str = "file") -> str:
    """Sanitize a user-supplied filename to prevent path traversal and injection.

    - Removes path separators and directory traversal sequences
    - Strips dangerous characters (only allows a-z, A-Z, 0-9, ., _, -, space)
    - Limits length to 255 characters
    - Returns a safe default if the result is empty
    """
    if not filename or not isinstance(filename, str):
        return default
    # Remove any path separators
    filename = filename.replace("\\", "_").replace("/", "_")
    # Remove null bytes and control characters
    filename = "".join(c for c in filename if c >= " ")
    # Strip directory traversal sequences
    filename = filename.replace("..", "_")
    # Remove any remaining dangerous characters
    filename = _SAFE_FILENAME_RE.sub("_", filename)
    # Limit length
    filename = filename[:_MAX_FILENAME_LENGTH]
    # Strip leading dots and spaces
    filename = filename.lstrip(". ")
    # Default if empty
    return filename if filename else default


from PIL import Image  # noqa: E402

import config  # noqa: E402
from config import OWNER_ID  # noqa: E402
from tools import (  # noqa: E402
    create_thumbnail_from_image,
    create_thumbnail_from_pdf,
    extract_pdf_metadata,
    is_supported_format,
    is_valid_pdf,
)
from utils.bigfile_pipeline import BigFilePipeline  # noqa: E402
from utils.error_handler import (  # noqa: E402
    get_error_handler,
    handle_bot_error,
)
from utils.progress_tracker import (  # noqa: E402
    _format_size,
    progress_tracker,
    send_progress_update,
)
from utils.rate_limiter import (  # noqa: E402
    RedisSlidingWindowRateLimiter,
    telegram_api_limiter,  # shared singleton (bot.py + progress tracker)
)
from utils.redis_client import get_sync_redis  # noqa: E402
from utils.session_healthcheck import (  # noqa: E402
    get_session_healthchecker,
    source_label,
    start_session_healthcheck,
    stop_session_healthcheck,
)
from utils.url_validation import _validate_url_safe  # noqa: E402
from utils.userbot_uploader import send_file_via_userbot  # noqa: E402

# ── Logging configuration (must be before any logger usage) ──
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level)
# keep httpx at least INFO to avoid leaking full request URLs in DEBUG logs
logging.getLogger("httpx").setLevel(max(numeric_level, logging.INFO))
logging.getLogger("rq").setLevel(numeric_level)
logging.getLogger("telegram").setLevel(numeric_level)
# Suppress Pyrogram's noisy transport retries (BrokenPipe, timeout, etc.)
# These are benign retries that Pyrogram handles automatically.
logging.getLogger("pyrogram.session.session").setLevel(logging.WARNING)
logging.getLogger("pyrogram.connection.transport.tcp.tcp").setLevel(
    logging.WARNING
)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Global error handler instance
# NOTE: `telegram_api_limiter` is the shared singleton from utils.rate_limiter,
# reused by send_progress_update so the global 30/s + per-user 1/s budgets
# stay accurate across all outbound sends in this process.
bot_error_handler = get_error_handler()

# Global BigFilePipeline instance for large file ingestion
_bigfile_pipeline = None
try:
    _bigfile_pipeline = BigFilePipeline()
    logger.info("BigFilePipeline initialized")
except Exception as e:
    logger.warning("BigFilePipeline init failed (non-fatal): %s", e)


# ── Userbot availability cache (checked once per handler call) ──
def _has_mongo_session_sync(user_id: int | None) -> bool:
    """Return True if MongoDB (sync) holds any session string for *user_id*.

    Covers per-user sessions that only exist in MongoDB — e.g. after a redeploy
    wiped the ephemeral per-user JSON files.  Mirrors the async per-user
    resolvers so ``_check_userbot_available`` agrees with what downloads
    actually resolve.
    """
    if user_id is None:
        return False
    try:
        from utils.db import (  # noqa: PLC0415
            COL_SESSIONS,
            get_sync_db,
            sync_query,
        )

        db = get_sync_db()
        if db is None:
            return False
        doc = (
            sync_query(COL_SESSIONS, db)
            .where("user_id", "=", int(user_id))
            .first()
        )
        if not doc:
            return False
        return bool(
            doc.get("telethon_session")
            or doc.get("pyrogram_session")
            or doc.get("string_session")
        )
    except Exception:
        return False


def _check_userbot_available(user_id: int | None = None) -> bool:
    """Return True if a Telethon or Pyrogram userbot session is configured.

    When ``user_id`` is provided, checks that user's own sessions first
    (per-user login), falling back to the global/admin session, and finally
    to MongoDB (sync) — so a per-user session that only survives in MongoDB
    after a redeploy still enables the userbot fallback.

    Honors ``ENABLE_USERBOT``: when explicitly set to false/0/no, the
    userbot fallback is disabled entirely (mirrors the reference's gate).
    """
    try:
        if not config.ENABLE_USERBOT:
            return False
        from utils.telethon_session import (  # noqa: PLC0415
            get_pyrogram_session_string,
            has_usable_telethon_session,
        )

        if has_usable_telethon_session(user_id=user_id) or bool(
            get_pyrogram_session_string(user_id=user_id)
        ):
            return True
        return _has_mongo_session_sync(user_id)
    except Exception:
        return False


def _make_progress_cb(task_id: str, loop: asyncio.AbstractEventLoop):
    """Build a sync progress callback that updates the progress_tracker asynchronously.

    Both Telethon's ``progress_callback`` and Pyrogram's ``progress`` accept
    a sync callable ``(current_bytes, total_bytes)``. This wraps the async
    ``update_task_progress`` so it can be called from those contexts.
    """

    def _cb(current: int, total: int):
        try:
            asyncio.run_coroutine_threadsafe(
                progress_tracker.update_task_progress(task_id, current),
                loop,
            )
        except Exception:  # nosec B110
            pass

    return _cb


async def _send_with_upload_progress(
    bot,
    chat_id: int,
    file_path: str,
    caption: str,
    thumb_path: str | None,
    user_id: int,
    filename: str,
    file_size: int,
    loop: asyncio.AbstractEventLoop,
    target_chat_id: int | str = None,
) -> bool:
    """Send a file via userbot with upload progress tracking.

    ``chat_id`` is used for the **progress message** (shown in the DM with the bot).
    ``target_chat_id`` controls where the actual file is uploaded.
    When ``target_chat_id`` is ``'me'``, the file lands in the userbot's Saved Messages.
    Defaults to ``chat_id`` when not provided (backward-compatible).

    Creates a progress task, shows 'uploading' status with a progress bar,
    then calls send_file_via_userbot with a progress callback that updates
    the task in real time. On success, marks the task as completed and
    edits the message. On failure, marks as failed and re-raises.

    Returns True on success, raises on failure.
    """
    task_id = uuid.uuid4().hex[:12]
    task = progress_tracker.create_task(
        task_id, user_id or 0, filename, file_size
    )
    task.status = "uploading"
    task.start()
    progress_msg_id = await send_progress_update(chat_id, bot, task)
    _cb = _make_progress_cb(task.task_id, loop)

    try:
        _upload_target = (
            target_chat_id if target_chat_id is not None else chat_id
        )
        success = await send_file_via_userbot(
            chat_id=_upload_target,
            file_path=file_path,
            caption=caption,
            thumb_path=thumb_path,
            progress_callback=_cb,
            user_id=user_id,
        )
        if success:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                try:
                    await send_progress_update(
                        chat_id, bot, task, progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
            logger.info(
                "Upload complete: %s (%s)", filename, _format_size(file_size)
            )
            return True
        else:
            await progress_tracker.fail_task(
                task.task_id, "Userbot upload returned False"
            )
            if progress_msg_id:
                try:
                    await send_progress_update(
                        chat_id, bot, task, progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
            raise RuntimeError(f"Userbot upload failed for {filename}")
    except Exception as e:
        await progress_tracker.fail_task(task.task_id, str(e))
        if progress_msg_id:
            try:
                await send_progress_update(chat_id, bot, task, progress_msg_id)
            except Exception:  # nosec B110
                pass
        raise


async def _notify_download_failed(msg, file_size=None):
    """Send a helpful error message when all download methods fail for a large file."""
    try:
        relay_chat = config.RELAY_CHAT_ID
        options = []
        if relay_chat:
            options.append(
                "- Add the userbot account to a group where the bot is also a member, then send the file there."
            )
        else:
            options.append(
                "- Set RELAY_CHAT_ID env var to a group where both bot and userbot are members."
            )
        options.append("- Send a public HTTPS URL to the file instead.")
        if file_size:
            mb_size = file_size // (1024 * 1024)
            options.append(
                f"- Upload a smaller file (under 50MB). Your file is ~{mb_size} MB."
            )
        else:
            options.append("- Upload a smaller file (under 50MB).")
        await msg.reply_text(
            "Failed to download file. All methods tried:\n"
            "1. Relay group (forward + userbot download)\n"
            "2. Direct chat download (userbot in same chat)\n"
            "3. S3 pipeline (if configured)\n\n"
            "Options:\n" + "\n".join(options)
        )
    except Exception:  # nosec B110
        pass


async def _userbot_download_fallback(
    msg,
    file_path: str,
    filename: str,
    mime: str,
    file_size: int,
    file_unique_id,
    user_id,
    chat_id,
    loop,
    forward_info: dict | None = None,
    file_id: str | None = None,
):
    """Try file_id -> forward source -> relay group -> direct chat -> BigFilePipeline.

    Order of attempts:
    0. File_id-based download (fastest, no chat resolution needed)
    1. Forward source (original chat, when available)
    2. Relay group (forward to relay chat -> download via userbot)
    3. Direct chat download (userbot in same chat)
    4. BigFilePipeline (S3 pipeline)

    ``file_id`` is the Telegram Bot API ``file_id`` from the document. When provided,
    Telethon's ``resolve_bot_file_id`` is used to download directly by file location,
    bypassing chat/message resolution entirely.

    Creates a progress task and tries each download method in sequence.

    Args:
        forward_info: Dict with keys 'chat_id', 'message_id', 'user_id' from forwarded messages.
        file_id: Telegram Bot API ``file_id`` for direct file location download.

    Returns:
        "local"    -> file downloaded to file_path, caller should thumbnail + send
        "pipeline" -> handled async by BigFilePipeline, caller should return
        False      -> all methods failed, user already notified
    """
    from utils.userbot_downloader import download_forward_via_userbot

    task_id = uuid.uuid4().hex[:12]
    task = progress_tracker.create_task(
        task_id, user_id or 0, filename, file_size or 0
    )
    task.status = "downloading"
    task.start()
    progress_msg_id = await send_progress_update(
        msg.chat.id, msg.get_bot(), task
    )
    _cb = _make_progress_cb(task.task_id, loop)

    dl_ok = False

    # Note: file_id-based download (via resolve_bot_file_id) was removed because
    # modern Bot API file_id formats (v4+) are not supported by Telethon's
    # resolve_bot_file_id utility.  The file_id parameter is still accepted and
    # passed to chat-based download methods as a fallback hint.

    # 1) Forward source (original chat, if available and different from current chat)
    if not dl_ok and forward_info:
        fwd_chat_id = forward_info.get("chat_id")
        fwd_msg_id = forward_info.get("message_id")
        if fwd_chat_id and fwd_msg_id and fwd_chat_id != msg.chat.id:
            try:
                logger.info(
                    "forward source: trying userbot download from %s/%s",
                    fwd_chat_id,
                    fwd_msg_id,
                )
                dl_ok = await download_forward_via_userbot(
                    chat_id=fwd_chat_id,
                    message_id=fwd_msg_id,
                    dest_path=file_path,
                    progress_callback=_cb,
                    file_id=file_id,
                    user_id=user_id,
                )
                if dl_ok and filename.lower().endswith(".pdf"):
                    if not is_valid_pdf(file_path):
                        logger.warning(
                            "forward source: downloaded PDF is corrupted, trying next method"
                        )
                        dl_ok = False
                        try:
                            os.remove(file_path)
                        except Exception:  # nosec B110
                            pass
            except Exception as fwd_err:
                logger.warning("forward source download failed: %s", fwd_err)

    # 2) Relay group: forward to relay chat -> download via userbot
    if not dl_ok:
        relay_chat = config.RELAY_CHAT_ID
        if relay_chat:
            try:
                relay_chat_id = int(relay_chat)
                # Try PTB-based forward first (preferred when bot context is available)
                _bot = getattr(msg, "get_bot", None)
                if _bot is not None:
                    _forwarded = await _bot().forward_message(
                        chat_id=relay_chat_id,
                        from_chat_id=msg.chat.id,
                        message_id=msg.message_id,
                    )
                    if _forwarded and getattr(_forwarded, "message_id", None):
                        relay_msg_id = _forwarded.message_id
                        logger.info(
                            "relay (PTB): forwarded %s/%s to %s/%s",
                            msg.chat.id,
                            msg.message_id,
                            relay_chat_id,
                            relay_msg_id,
                        )
                        dl_ok = await download_forward_via_userbot(
                            chat_id=relay_chat_id,
                            message_id=relay_msg_id,
                            dest_path=file_path,
                            progress_callback=_cb,
                            file_id=file_id,
                            user_id=user_id,
                        )
                        if dl_ok and filename.lower().endswith(".pdf"):
                            if not is_valid_pdf(file_path):
                                logger.warning(
                                    "relay: downloaded PDF is corrupted, trying next method"
                                )
                                dl_ok = False
                                try:
                                    os.remove(file_path)
                                except Exception:  # nosec B110
                                    pass
                    else:
                        raise Exception(
                            "PTB forward_message returned no message_id"
                        )
                else:
                    raise Exception("msg.get_bot() not available")
            except Exception as _relay_ptb_err:
                # ── If PTB relay failed, try HTTP-based relay (same approach as tasks.py) ──
                if not dl_ok:
                    try:
                        logger.warning(
                            "relay (PTB) failed (%s); trying HTTP-based relay forward",
                            _relay_ptb_err,
                        )
                        bot_token = config.BOT_TOKEN
                        fwd_url = f"https://api.telegram.org/bot{bot_token}/forwardMessage"
                        import requests as _requests

                        fwd_resp = _requests.post(
                            fwd_url,
                            data={
                                "chat_id": relay_chat_id,
                                "from_chat_id": chat_id,
                                "message_id": msg.message_id,
                            },
                            timeout=30,
                        )
                        if fwd_resp.status_code == 200:
                            fwd_data = fwd_resp.json()
                            relay_msg_id = fwd_data["result"]["message_id"]
                            logger.info(
                                "relay (HTTP): forwarded %s/%s to %s/%s",
                                msg.chat.id,
                                msg.message_id,
                                relay_chat_id,
                                relay_msg_id,
                            )
                            dl_ok = await download_forward_via_userbot(
                                chat_id=relay_chat_id,
                                message_id=relay_msg_id,
                                dest_path=file_path,
                                progress_callback=_cb,
                                file_id=file_id,
                                user_id=user_id,
                            )
                            if dl_ok and filename.lower().endswith(".pdf"):
                                if not is_valid_pdf(file_path):
                                    logger.warning(
                                        "relay (HTTP): downloaded PDF is corrupted, trying next method"
                                    )
                                    dl_ok = False
                                    try:
                                        os.remove(file_path)
                                    except Exception:  # nosec B110
                                        pass
                        else:
                            raise Exception(
                                f"HTTP forwardMessage failed: {fwd_resp.status_code} "
                                f"{fwd_resp.text[:200]}"
                            )
                    except Exception as _relay_http_err:
                        logger.warning(
                            "relay (HTTP) also failed for %s/%s: %s",
                            msg.chat.id,
                            msg.message_id,
                            _relay_http_err,
                        )

    # 3) Direct chat download (group chat where userbot is a member)
    if not dl_ok:
        logger.info(
            "relay failed or not configured, trying direct chat download"
        )
        dl_ok = await download_forward_via_userbot(
            chat_id=msg.chat.id,
            message_id=msg.message_id,
            dest_path=file_path,
            progress_callback=_cb,
            file_id=file_id,
            user_id=user_id,
        )
        if dl_ok and filename.lower().endswith(".pdf"):
            if not is_valid_pdf(file_path):
                logger.warning(
                    "direct chat: downloaded PDF is corrupted, will try BigFilePipeline"
                )
                dl_ok = False
                try:
                    os.remove(file_path)
                except Exception:  # nosec B110
                    pass

    # 4) BigFilePipeline (S3 pipeline)
    if not dl_ok and _bigfile_pipeline is not None:
        logger.info("direct download failed, trying BigFilePipeline")
        try:
            _ingest = await _bigfile_pipeline.ingest_large_file(
                chat_id=msg.chat.id,
                message_id=msg.message_id,
                file_size=file_size or 0,
                file_unique_id=file_unique_id,
                user_id=user_id,
                original_filename=filename,
                progress_callback=_cb,
            )
            if _ingest.ok:
                await progress_tracker.complete_task(task.task_id)
                if progress_msg_id:
                    await send_progress_update(
                        msg.chat.id, msg.get_bot(), task, progress_msg_id
                    )
                await msg.reply_text(
                    f"Large file ({file_size // (1024 * 1024)} MB) queued for processing.\n"
                    f"Job: {_ingest.job_id[:8]}... You'll receive the result when ready."
                )
                return "pipeline"
            else:
                logger.warning("BigFilePipeline failed: %s", _ingest.error)
        except Exception as pipe_err:
            logger.warning("BigFilePipeline error: %s", pipe_err)

    if dl_ok:
        actual_size = os.path.getsize(file_path)
        await progress_tracker.update_task_progress(task.task_id, actual_size)
        await progress_tracker.complete_task(task.task_id)
        if progress_msg_id:
            await send_progress_update(
                msg.chat.id, msg.get_bot(), task, progress_msg_id
            )
        return "local"
    else:
        await _notify_download_failed(msg, file_size)
        if task:
            await progress_tracker.fail_task(
                task.task_id, "All download methods failed"
            )
            if progress_msg_id:
                try:
                    await send_progress_update(
                        msg.chat.id, msg.get_bot(), task, progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
        return False


# ── User session tracking (Redis + MongoDB) ──────────────────
# Cached imports for _track_user_session (avoids re-importing on every call)
_cache_get_cache = None
_db_save_user_session = None


def _init_session_imports():
    global _cache_get_cache, _db_save_user_session
    try:
        from utils.cache import get_cache as _gc

        _cache_get_cache = _gc
    except Exception:  # nosec B110
        pass
    try:
        from utils.db import save_user_session as _ss

        _db_save_user_session = _ss
    except Exception:  # nosec B110
        pass


_init_session_imports()


async def _track_user_session(update: Update, action: str = "message"):
    """Best-effort record user session data in Redis and MongoDB."""
    try:
        user = update.effective_user
        if not user:
            return
        uid = user.id
        chat = update.effective_chat
        session_data = {
            "user_id": uid,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "chat_id": chat.id if chat else None,
            "chat_type": chat.type if chat else "",
            "last_action": action,
            "last_seen": time.time(),
            "is_owner": config.is_owner(uid),
            "is_admin": config.is_admin_user(uid),
        }
        # Redis cache (fast lookups)
        if _cache_get_cache is not None:
            try:
                cache = await _cache_get_cache()
                await cache.cache_user_session(str(uid), session_data)
            except Exception:  # nosec B110
                pass
        # MongoDB (durable history)
        if _db_save_user_session is not None:
            try:
                await _db_save_user_session(uid, session_data)
            except Exception:  # nosec B110
                pass
    except Exception:  # nosec B110
        pass


# Optional RQ enqueue helper (import only when needed)
def enqueue_job(func_name: str, *args, **kwargs):
    """Enqueue a job on the RQ 'default' queue.

    Returns the RQ job id (so /canceljob can cancel it) or None on failure.

    Uses a NON-decoding Redis connection (same as ``_cancel_rq_job`` and the
    RQ worker): RQ stores job payloads pickled as raw bytes, so the
    decode_responses=True singleton must never back an RQ Queue — reads
    through it would UnicodeDecodeError.
    """
    try:
        from rq import Queue

        import tasks
        from utils.redis_client import get_sync_redis_raw

        redis_conn = get_sync_redis_raw()
        if not redis_conn:
            logger.error("Redis not available for enqueue_job")
            return None
        q = Queue("default", connection=redis_conn)
        # lookup function from tasks
        func = getattr(tasks, func_name)
        job = q.enqueue(func, *args, **kwargs)
        return getattr(job, "id", None)
    except Exception:
        logger.exception("Failed to enqueue job for %s", func_name)
        return None


BOT_TOKEN = config.BOT_TOKEN
if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set")
    raise SystemExit("Missing BOT_TOKEN")

WEBHOOK_URL = config.WEBHOOK_URL
USE_POLLING = config.USE_POLLING

# ── Webhook secret token for CSRF protection ────────────────
# This is sent as X-Telegram-Bot-Api-Secret-Token by Telegram when calling
# the webhook endpoint. It protects against fake requests from malicious actors.
# If not configured via WEBHOOK_SECRET env var, generate a random one on startup.
WEBHOOK_SECRET = config.WEBHOOK_SECRET
if not WEBHOOK_SECRET:

    WEBHOOK_SECRET = secrets.token_urlsafe(32)
    logger.warning(
        "WEBHOOK_SECRET not set in env! Auto-generated to %s... "
        "This secret changes on every restart, which will break the webhook. "
        "Set the WEBHOOK_SECRET env var for a persistent secret across restarts.",
        WEBHOOK_SECRET[:8],
    )

# Build async Application (python-telegram-bot v20+)
application = ApplicationBuilder().token(BOT_TOKEN).build()

# ── Session healthcheck background task ─────────────────────
# Started inside the FastAPI lifespan startup (on_startup), where a
# running event loop exists — see the lifespan handler above.
_shc_task = None

# Batch-forward collection helpers (Redis-backed with local fallback)
local_forward_batches = {}


def _batch_keys(chat_id: int, user_id: int) -> tuple:
    base = f"forward_batch:{chat_id}:{user_id}"
    return base + ":active", base + ":items"


def start_forward_batch(chat_id: int, user_id: int) -> bool:
    r = get_sync_redis()
    if r:
        active_key, items_key = _batch_keys(chat_id, user_id)
        r.set(active_key, "1")
        r.delete(items_key)
        return True
    # fallback: use local in-memory list
    key = (chat_id, user_id)
    local_forward_batches.pop(key, None)
    local_forward_batches[key] = []
    return True


def append_forward_item(chat_id: int, user_id: int, item: dict) -> bool:
    r = get_sync_redis()
    if r:
        _, items_key = _batch_keys(chat_id, user_id)
        try:
            r.rpush(items_key, json.dumps(item))
            return True
        except Exception:
            logger.exception("Failed to push forward item to Redis list")
            return False
    key = (chat_id, user_id)
    local_forward_batches.setdefault(key, []).append(item)
    return True


def get_forward_items(chat_id: int, user_id: int) -> list:
    r = get_sync_redis()
    if r:
        _, items_key = _batch_keys(chat_id, user_id)
        try:
            raw = r.lrange(items_key, 0, -1)
            return (
                [
                    json.loads(x.decode() if isinstance(x, bytes) else x)
                    for x in raw
                ]
                if raw
                else []
            )
        except Exception:
            logger.exception("Failed to read forward items from Redis")
            return []
    key = (chat_id, user_id)
    return list(local_forward_batches.get(key, []))


def clear_forward_batch(chat_id: int, user_id: int) -> bool:
    r = get_sync_redis()
    if r:
        active_key, items_key = _batch_keys(chat_id, user_id)
        try:
            r.delete(active_key)
            r.delete(items_key)
            return True
        except Exception:
            logger.exception("Failed to clear forward batch keys in Redis")
            return False
    key = (chat_id, user_id)
    local_forward_batches.pop(key, None)
    return True


def is_batch_active(chat_id: int, user_id: int) -> bool:
    r = get_sync_redis()
    if r:
        active_key, _ = _batch_keys(chat_id, user_id)
        try:
            return bool(r.exists(active_key))
        except Exception:
            return False
    key = (chat_id, user_id)
    return key in local_forward_batches


async def handle_document(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "document")
    msg = update.effective_message
    if not msg or not msg.document:
        return

    doc = msg.document
    # ── ACL check (open bot when ALLOWED_USER_IDS is empty) ──
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await msg.reply_text("Access denied. This bot is private.")
        return

    # ── Respect Telegram API rate limits (global 30/s + per-user 1/s) ──
    try:
        await telegram_api_limiter.wait_if_needed(
            str(getattr(update.effective_user, "id", 0))
        )
    except Exception:  # nosec B110 - throttling is best-effort
        pass

    # If REDIS_URL provided, enqueue background job and return immediately
    chat_id = msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
    filename = _sanitize_filename(doc.file_name, f"file_{doc.file_id}")
    mime = getattr(doc, "mime_type", "") or ""

    # ── Early format validation: check → validate → compare → process ──
    # Reject unsupported formats (video files like MKV, AVI, etc.) BEFORE any
    # download, relay forwarding, enqueue, or thumbnail processing.
    if not is_supported_format(filename, mime):
        logger.info(
            "Rejected unsupported format: filename=%s mime=%s chat_id=%s user_id=%s",
            filename,
            mime,
            chat_id,
            getattr(update.effective_user, "id", None),
        )
        await msg.reply_text(
            "\u274c Unsupported file format.\n\n"
            "This bot only processes **PDF documents** and **images** (JPEG, PNG, WEBP, GIF).\n"
            "Video files (MKV, AVI, MP4, MOV, etc.) and other formats are not supported."
        )
        return

    # ── Capture forward metadata (for userbot fallback + batch items) ──
    forward_info = None
    try:
        fch = getattr(msg, "forward_from_chat", None)
        f_msg_id = getattr(msg, "forward_from_message_id", None)
        f_user = getattr(msg, "forward_from", None)
        if fch or f_msg_id or f_user:
            forward_info = {}
            if fch:
                forward_info["chat_id"] = fch.id
            if f_msg_id:
                forward_info["message_id"] = f_msg_id
            if f_user:
                forward_info["user_id"] = f_user.id
    except Exception:  # nosec B110
        pass

    # If this was forwarded and a forward-batch is active for this sender, store metadata and return
    is_forwarded = bool(
        getattr(msg, "forward_from", None)
        or getattr(msg, "forward_from_chat", None)
        or getattr(msg, "forward_date", None)
    )
    user_id = getattr(update.effective_user, "id", None)
    if is_forwarded and await asyncio.to_thread(
        is_batch_active, chat_id, user_id
    ):
        item = {
            "file_id": doc.file_id,
            "file_unique_id": getattr(doc, "file_unique_id", None),
            "filename": filename,
            "mime": mime,
            "message_id": msg.message_id,
            "forward_info": forward_info,
            "file_size": getattr(doc, "file_size", None),
        }
        await asyncio.to_thread(append_forward_item, chat_id, user_id, item)
        await msg.reply_text(f"Added forwarded file to batch: {filename}")
        return

    # If Telegram reports a file_size on the Document, check it against the configured
    # upload limit before attempting to enqueue or download. Telegram's Bot API will
    # reject downloads for files larger than the bot's allowed size (returns 400 "file is too big").
    file_size = getattr(doc, "file_size", None)
    upload_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
    use_userbot_download = False
    if file_size and upload_limit and file_size > upload_limit:
        _userbot_ok = _check_userbot_available(user_id)
        if _userbot_ok:
            use_userbot_download = True
            logger.info(
                "file too large for Bot API (%d MB), falling back to userbot download",
                file_size // (1024 * 1024),
            )
        else:
            # Inform the user
            try:
                mb_limit = upload_limit // (1024 * 1024)
                mb_size = file_size // (1024 * 1024)
                await msg.reply_text(
                    f"I can't download files larger than {mb_limit} MB via the Bot API. "
                    f"Your file is approximately {mb_size} MB.\n\n"
                    "Options:\n"
                    "- Upload a smaller file (under the limit).\n"
                    "- Send a public HTTPS URL to the file (I can download and process URLs).\n"
                    "- Use a user account client (Pyrogram user) which supports larger uploads.\n"
                    "If you want automatic external-hosting fallback, enable S3 fallback in the bot config."
                )
            except Exception:
                logger.exception("Failed to notify user about large file")
            return

    if config.REDIS_URL:
        # When file is too large (>50MB) and userbot is available, route through
        # userbot/BigFilePipeline instead. process_document_job uses the Bot API
        # (getFile) which cannot handle files >50MB and will fail with "file is too big".
        if not use_userbot_download:
            # Pass message_id + forward_info + file_size so the worker has context for userbot fallback
            ok = await asyncio.to_thread(
                enqueue_job,
                "process_document_job",
                chat_id,
                doc.file_id,
                filename,
                mime,
                getattr(doc, "file_unique_id", None),
                msg.message_id,
                forward_info,
                file_size,
                user_id,
            )
            if ok:
                await msg.reply_text(
                    "Queued your file for background processing; I'll send the result when ready.\n"
                    f"Job ID: `{ok}` — use /canceljob {ok} to cancel it."
                )
                return
            # fall through to inline processing on enqueue failure

    tmpdir = (
        tempfile.mkdtemp(dir=config.TMP_DIR)
        if config.TMP_DIR
        else tempfile.mkdtemp()
    )
    progress_msg_id = None
    task = None
    _dl_success = False
    _loop = asyncio.get_running_loop()
    try:
        file_path = os.path.join(tmpdir, filename)

        if use_userbot_download:
            # ── Big file download: forward source → relay → direct → pipeline ──
            _dl_result = await _userbot_download_fallback(
                msg,
                file_path,
                filename,
                mime,
                file_size or 0,
                getattr(doc, "file_unique_id", None),
                user_id,
                chat_id,
                _loop,
                forward_info=forward_info,
                file_id=doc.file_id,
            )
            if _dl_result == "pipeline":
                return
            if not _dl_result:
                return
        else:
            # ── Normal Bot API download ──
            file = await context.bot.get_file(doc.file_id)
            if (
                file_size and file_size > 1024 * 1024
            ):  # only show progress for files >1MB
                task_id = uuid.uuid4().hex[:12]
                task = progress_tracker.create_task(
                    task_id, user_id or 0, filename, file_size
                )
                progress_msg_id = await send_progress_update(
                    msg.chat.id, context.bot, task
                )
                task.start()
                task.status = "downloading"

                await file.download_to_drive(
                    custom_path=file_path, read_timeout=300, write_timeout=300
                )
                task.update_progress(file_size or os.path.getsize(file_path))
                if progress_msg_id:
                    await send_progress_update(
                        msg.chat.id, context.bot, task, progress_msg_id
                    )
            else:
                await file.download_to_drive(
                    custom_path=file_path, read_timeout=300, write_timeout=300
                )
            _dl_success = True

        # Validate PDF downloaded via Bot API — catch corrupted files before thumbnail creation
        if (
            filename.lower().endswith(".pdf") or mime == "application/pdf"
        ) and not is_valid_pdf(file_path):
            logger.warning(
                "Bot API download: downloaded PDF is corrupted/invalid, will retry via userbot"
            )
            _dl_success = False
            try:
                os.remove(file_path)
            except Exception:  # nosec B110
                pass
            raise RuntimeError(
                "Bot API download produced invalid PDF, falling back to userbot"
            )

        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        lower = filename.lower()

        # Return original file unchanged but attach generated thumbnail as header
        if lower.endswith(".pdf") or mime == "application/pdf":
            create_thumbnail_from_pdf(file_path, thumb_path)
        elif mime.startswith("image/"):
            create_thumbnail_from_image(file_path, thumb_path)
        else:
            # generic placeholder thumbnail
            im = Image.new("RGB", (320, 320), (240, 240, 240))
            im.save(thumb_path, "JPEG", quality=85)

        chat_id = msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
        _dl_size = os.path.getsize(file_path)
        _ul_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
        # ── Full PDF metadata retrieval (surfaced in the caption) ──
        caption = "Here is your file with an auto-generated cover preview."
        if lower.endswith(".pdf") or mime == "application/pdf":
            meta = extract_pdf_metadata(file_path)
            if meta.get("extracted"):
                meta_bits = []
                if meta.get("title"):
                    meta_bits.append(f"📄 {meta['title']}")
                if meta.get("author"):
                    meta_bits.append(f"✍️ {meta['author']}")
                meta_bits.append(f"📑 {meta['pages']} pages")
                if meta.get("file_size"):
                    meta_bits.append(_format_size(meta["file_size"]))
                caption += "\n\n" + " · ".join(meta_bits)
        if _dl_size > _ul_limit and _check_userbot_available(user_id):
            await _send_with_upload_progress(
                bot=context.bot,
                chat_id=chat_id,
                file_path=file_path,
                caption=caption,
                thumb_path=thumb_path,
                user_id=user_id,
                filename=filename,
                file_size=_dl_size,
                loop=_loop,
                target_chat_id="me",
            )
        else:
            with (
                open(file_path, "rb") as f_doc,
                open(thumb_path, "rb") as f_thumb,
            ):
                input_doc = InputFile(f_doc, filename=filename)
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=input_doc,
                    thumbnail=f_thumb,
                    caption=caption,
                )
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                await send_progress_update(
                    msg.chat.id, context.bot, task, progress_msg_id
                )
    except Exception as e:
        # Try userbot fallback if Bot API download failed
        if not _dl_success and _check_userbot_available(user_id):
            logger.info(
                "Bot API download failed, falling back to userbot download for %s",
                filename,
            )
            try:
                _dl_result = await _userbot_download_fallback(
                    msg,
                    file_path,
                    filename,
                    mime,
                    file_size or 0,
                    getattr(doc, "file_unique_id", None),
                    user_id,
                    chat_id,
                    _loop,
                    forward_info=forward_info,
                    file_id=doc.file_id,
                )
                if _dl_result == "local":
                    # Retry thumbnail + send
                    thumb_path = os.path.join(tmpdir, "thumb.jpg")
                    if (
                        filename.lower().endswith(".pdf")
                        or mime == "application/pdf"
                    ):
                        create_thumbnail_from_pdf(file_path, thumb_path)
                    elif mime.startswith("image/"):
                        create_thumbnail_from_image(file_path, thumb_path)
                    else:
                        im = Image.new("RGB", (320, 320), (240, 240, 240))
                        im.save(thumb_path, "JPEG", quality=85)

                    chat_id = (
                        msg.chat.id
                        if getattr(msg, "chat", None)
                        else msg.chat.id
                    )
                    _fb_size = os.path.getsize(file_path)
                    _fb_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
                    if _fb_size > _fb_limit and _check_userbot_available(user_id):
                        await _send_with_upload_progress(
                            bot=context.bot,
                            chat_id=chat_id,
                            file_path=file_path,
                            caption="Here is your file (downloaded via userbot) with an auto-generated cover preview.",
                            thumb_path=thumb_path,
                            user_id=user_id,
                            filename=filename,
                            file_size=_fb_size,
                            loop=_loop,
                            target_chat_id="me",
                        )
                    else:
                        with (
                            open(file_path, "rb") as f_doc,
                            open(thumb_path, "rb") as f_thumb,
                        ):
                            input_doc = InputFile(f_doc, filename=filename)
                            await context.bot.send_document(
                                chat_id=chat_id,
                                document=input_doc,
                                thumbnail=f_thumb,
                                caption="Here is your file (downloaded via userbot) with an auto-generated cover preview.",
                            )
                    return
            except Exception as ub_err:
                logger.exception(
                    "Userbot download fallback also failed: %s", ub_err
                )

        # Normal error handling
        error_info = await handle_bot_error(
            e, "PDF Document Processing", update=update
        )
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(
                        msg.chat.id, context.bot, task, progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
        try:
            await msg.reply_text(error_info["user_message"])
        except Exception:  # nosec B110
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def handle_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "photo")
    msg = update.effective_message
    if not msg or not msg.photo:
        return

    # ── ACL check (open bot when ALLOWED_USER_IDS is empty) ──
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await msg.reply_text("Access denied. This bot is private.")
        return

    # ── Respect Telegram API rate limits (global 30/s + per-user 1/s) ──
    try:
        await telegram_api_limiter.wait_if_needed(
            str(getattr(update.effective_user, "id", 0))
        )
    except Exception:  # nosec B110 - throttling is best-effort
        pass

    photo = msg.photo[-1]
    photo_size = getattr(photo, "file_size", None) or 0

    # If REDIS_URL configured, enqueue background job and return immediately
    chat_id = msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
    filename = _sanitize_filename(f"photo_{photo.file_id}.jpg", "photo.jpg")
    # ── Capture forward metadata (for userbot fallback + batch items) ──
    photo_forward_info = None
    try:
        fch = getattr(msg, "forward_from_chat", None)
        f_msg_id = getattr(msg, "forward_from_message_id", None)
        f_user = getattr(msg, "forward_from", None)
        if fch or f_msg_id or f_user:
            photo_forward_info = {}
            if fch:
                photo_forward_info["chat_id"] = fch.id
            if f_msg_id:
                photo_forward_info["message_id"] = f_msg_id
            if f_user:
                photo_forward_info["user_id"] = f_user.id
    except Exception:  # nosec B110
        pass

    # If this photo was forwarded and batch collection is active, append to batch
    is_forwarded = bool(
        getattr(msg, "forward_from", None)
        or getattr(msg, "forward_from_chat", None)
        or getattr(msg, "forward_date", None)
    )
    user_id = getattr(update.effective_user, "id", None)
    if is_forwarded and await asyncio.to_thread(
        is_batch_active, chat_id, user_id
    ):
        item = {
            "file_id": photo.file_id,
            "file_unique_id": getattr(photo, "file_unique_id", None),
            "filename": filename,
            "mime": "image/jpeg",
            "message_id": msg.message_id,
            "forward_info": photo_forward_info,
            "file_size": photo_size,
        }
        await asyncio.to_thread(append_forward_item, chat_id, user_id, item)
        await msg.reply_text(f"Added forwarded photo to batch: {filename}")
        return

    # Thumbnail caching disabled

    if config.REDIS_URL:
        ok = await asyncio.to_thread(
            enqueue_job,
            "process_document_job",
            chat_id,
            photo.file_id,
            filename,
            "image/jpeg",
            getattr(photo, "file_unique_id", None),
            msg.message_id,
            photo_forward_info,
            photo_size,
            user_id,
        )
        if ok:
            await msg.reply_text(
                "Queued your photo for background processing; I'll send the result when ready.\n"
                f"Job ID: `{ok}` — use /canceljob {ok} to cancel it."
            )
            return

    tmpdir = (
        tempfile.mkdtemp(dir=config.TMP_DIR)
        if config.TMP_DIR
        else tempfile.mkdtemp()
    )
    progress_msg_id = None
    task = None
    _dl_success = False
    _loop = asyncio.get_running_loop()
    try:
        file_path = os.path.join(tmpdir, filename)

        upload_limit = config.BOT_API_UPLOAD_LIMIT_BYTES

        if photo_size > upload_limit and _check_userbot_available(user_id):
            # ── Userbot download path for large photos ──
            _dl_result = await _userbot_download_fallback(
                msg,
                file_path,
                filename,
                "image/jpeg",
                photo_size,
                getattr(photo, "file_unique_id", None),
                user_id,
                chat_id,
                _loop,
                forward_info=photo_forward_info,
                file_id=photo.file_id,
            )
            if _dl_result == "pipeline":
                return
            if not _dl_result:
                return
        else:
            # ── Normal Bot API download ──
            file = await context.bot.get_file(photo.file_id)
            if photo_size > 1024 * 1024:
                task_id = uuid.uuid4().hex[:12]
                task = progress_tracker.create_task(
                    task_id, user_id or 0, filename, photo_size
                )
                progress_msg_id = await send_progress_update(
                    msg.chat.id, context.bot, task
                )
                task.start()
                task.status = "downloading"

            await file.download_to_drive(
                custom_path=file_path, read_timeout=300, write_timeout=300
            )

            if task:
                task.update_progress(os.path.getsize(file_path))
                task.status = "processing"
                if progress_msg_id:
                    await send_progress_update(
                        msg.chat.id, context.bot, task, progress_msg_id
                    )
            _dl_success = True

        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        create_thumbnail_from_image(
            file_path, thumb_path
        )  # send original image back as document to preserve original bytes, attach thumbnail
        chat_id = msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
        _ph_size = os.path.getsize(file_path)
        _ph_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
        if _ph_size > _ph_limit and _check_userbot_available(user_id):
            await _send_with_upload_progress(
                bot=context.bot,
                chat_id=chat_id,
                file_path=file_path,
                caption="Here is your image with an auto-generated thumbnail.",
                thumb_path=thumb_path,
                user_id=user_id,
                filename=filename,
                file_size=_ph_size,
                loop=_loop,
                target_chat_id="me",
            )
        else:
            with (
                open(file_path, "rb") as f_doc,
                open(thumb_path, "rb") as f_thumb,
            ):
                input_doc = InputFile(
                    f_doc, filename=os.path.basename(file_path)
                )
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=input_doc,
                    thumbnail=f_thumb,
                    caption="Here is your image with an auto-generated thumbnail.",
                )
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                await send_progress_update(
                    msg.chat.id, context.bot, task, progress_msg_id
                )
    except Exception as e:
        # If Bot API download failed but userbot is available, try fallback
        if not _dl_success and _check_userbot_available(user_id):
            logger.info(
                "Bot API download failed for photo, falling back to userbot"
            )
            try:
                _dl_result = await _userbot_download_fallback(
                    msg,
                    file_path,
                    filename,
                    "image/jpeg",
                    photo_size or 0,
                    getattr(photo, "file_unique_id", None),
                    user_id,
                    chat_id,
                    _loop,
                    forward_info=photo_forward_info,
                    file_id=photo.file_id,
                )
                if _dl_result == "local":
                    thumb_path = os.path.join(tmpdir, "thumb.jpg")
                    create_thumbnail_from_image(file_path, thumb_path)
                    _ph_fb_size = os.path.getsize(file_path)
                    _ph_fb_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
                    if (
                        _ph_fb_size > _ph_fb_limit
                        and _check_userbot_available(user_id)
                    ):
                        await _send_with_upload_progress(
                            bot=context.bot,
                            chat_id=msg.chat.id,
                            file_path=file_path,
                            caption="Here is your image (downloaded via userbot) with an auto-generated thumbnail.",
                            thumb_path=thumb_path,
                            user_id=user_id,
                            filename=filename,
                            file_size=_ph_fb_size,
                            loop=_loop,
                            target_chat_id="me",
                        )
                    else:
                        with (
                            open(file_path, "rb") as f_doc,
                            open(thumb_path, "rb") as f_thumb,
                        ):
                            input_doc = InputFile(
                                f_doc, filename=os.path.basename(file_path)
                            )
                            await context.bot.send_document(
                                chat_id=msg.chat.id,
                                document=input_doc,
                                thumbnail=f_thumb,
                                caption="Here is your image (downloaded via userbot) with an auto-generated thumbnail.",
                            )
                    return
            except Exception as ub_err:
                logger.exception(
                    "Userbot download fallback for photo also failed: %s",
                    ub_err,
                )

        error_info = await handle_bot_error(
            e, "Photo Processing", update=update
        )
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(
                        msg.chat.id, context.bot, task, progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
        try:
            await msg.reply_text(error_info["user_message"])
        except Exception:  # nosec B110
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
application.add_handler(MessageHandler(filters.PHOTO, handle_photo))


async def cmd_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/start")
    user_id = getattr(update.effective_user, "id", None)
    if not config.is_user_allowed(user_id):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user_name = getattr(update.effective_user, "first_name", None) or "there"
    await update.effective_message.reply_text(
        f"🎉 Welcome, {user_name}!\n\n"
        "📄 Send me a **PDF** or **image** and I'll return a thumbnail "
        "(PDF first page used as cover).\n\n"
        "⚡ **Quick commands:**\n"
        "• /help — all commands\n"
        "• /login — connect **your** Telethon account (large files)\n"
        "• /loginpyro — connect **your** Pyrogram account (large files)\n"
        "• /loginstatus — check **your** session health\n"
        "• /canceljob <id> — cancel a queued/in-flight job\n"
        "• /startbatch + /endbatch — process multiple files at once\n\n"
        "_Sessions are per-user: nobody else can use your account._",
        parse_mode="Markdown",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/help")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return

    text = (
        "📚 Help — all commands\n\n"
        "📄 Core\n"
        "• /start — welcome & quick start\n"
        "• /help — this help\n"
        "• /status — bot status\n\n"
        "🔐 Your sessions (per-user)\n"
        "• /login [phone] — connect your Telethon account\n"
        "• /loginpyro [phone] — connect your Pyrogram account\n"
        "• /loginstatus — check your session health\n"
        "• /logout — disconnect your Telethon session\n"
        "• /logoutpyro — disconnect your Pyrogram session\n"
        "• /clearflood — reset a stuck login flow\n"
        "• /cancel — cancel an active login flow\n\n"
        "📦 Jobs\n"
        "• /canceljob <id> — cancel a queued/in-flight job\n\n"
        "🗂 Batch\n"
        "• /startbatch — start collecting forwarded files\n"
        "• /endbatch — process the collected batch\n"
        "• /cancelbatch — discard the collected batch\n\n"
        "⚙️ Admin / owner\n"
        "• /admin add|remove|list <user_id> — manage allowed users\n"
        "• /sessionstatus — userbot session health (owner)\n"
        "• /setwebhook <url> — set webhook (owner)\n"
        "• /delwebhook — delete webhook (owner)\n"
        "• /setcommands — push this list to Telegram (owner)\n\n"
        "Send a PDF or image any time to get its thumbnail."
    )
    await update.effective_message.reply_text(text)


async def cmd_status(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/status")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    # Minimal, non-sensitive status reply
    await update.effective_message.reply_text("active")


async def cmd_setwebhook(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/setwebhook")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text(
            "⛔ Only the bot owner can run this command."
        )
        return
    args = context.args or []
    if not args:
        await update.effective_message.reply_text(
            "Usage: /setwebhook https://example.com"
        )
        return
    url = args[0]
    # Validate URL to prevent SSRF attacks
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            await update.effective_message.reply_text(
                "\u274c URL must start with http:// or https://"
            )
            return
        if not parsed.netloc:
            await update.effective_message.reply_text(
                "\u274c Invalid URL: no hostname"
            )
            return
        # Block internal/private IP ranges to prevent SSRF
        import ipaddress

        try:
            hostname = parsed.netloc.split(":")[0].split("@")[-1]
            # Only check if it looks like an IP address
            try:
                ip = ipaddress.ip_address(hostname)
                if (
                    ip.is_private
                    or ip.is_loopback
                    or ip.is_link_local
                    or ip.is_multicast
                ):
                    await update.effective_message.reply_text(
                        "\u274c URL points to an internal/private IP address. This is not allowed."
                    )
                    return
            except ValueError:
                pass  # Hostname, not an IP - allow through
        except Exception:  # nosec B110
            pass  # Best-effort validation
    except Exception:
        await update.effective_message.reply_text("\u274c Invalid URL format")
        return
    webhook_path = f"/webhook/{BOT_TOKEN}"
    full_url = url.rstrip("/") + webhook_path
    try:
        # Register webhook with secret token for CSRF protection
        # Telegram will send X-Telegram-Bot-Api-Secret-Token header on each request
        await context.bot.set_webhook(
            url=full_url,
            secret_token=WEBHOOK_SECRET,
        )
        await update.effective_message.reply_text(
            f"Webhook set to {full_url}\n"
            f"\ud83d\udd12 CSRF protection enabled (secret token configured)"
        )
    except Exception:
        logger.exception("Failed to set webhook")
        await update.effective_message.reply_text(
            "\u274c Failed to set webhook. Check the URL and try again."
        )


async def cmd_delwebhook(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/delwebhook")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text(
            "⛔ Only the bot owner can run this command."
        )
        return
    try:
        await context.bot.delete_webhook()
        await update.effective_message.reply_text("Webhook deleted")
    except Exception:
        logger.exception("Failed to delete webhook")
        await update.effective_message.reply_text(
            "\u274c Failed to delete webhook."
        )


async def cmd_setcommands(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/setcommands")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text(
            "⛔ Only the bot owner can run this command."
        )
        return
    # Default command set
    commands = [
        BotCommand("start", "Start interaction with the bot"),
        BotCommand("help", "Show help and available commands"),
        BotCommand("status", "Get bot status"),
        BotCommand("login", "Login your Telethon userbot"),
        BotCommand("loginpyro", "Login your Pyrogram userbot"),
        BotCommand("loginstatus", "Check your live session health"),
        BotCommand("logout", "Logout your Telethon session"),
        BotCommand("logoutpyro", "Logout your Pyrogram session"),
        BotCommand("clearflood", "Clear an active login flow"),
        BotCommand("admin", "Manage allowed users"),
        BotCommand("startbatch", "Start collecting forwarded files"),
        BotCommand("endbatch", "Process collected batch"),
        BotCommand("cancelbatch", "Cancel batch collection"),
        BotCommand("canceljob", "Cancel a queued/in-flight job"),
        BotCommand("cancel", "Cancel an active login flow"),
        BotCommand("setcommands", "(owner) Update the command list"),
        BotCommand("sessionstatus", "(owner) Check userbot session health"),
        BotCommand("setwebhook", "(owner) Set webhook URL"),
        BotCommand("delwebhook", "(owner) Delete webhook"),
    ]
    try:
        await context.bot.set_my_commands(commands)
        await update.effective_message.reply_text("Commands updated")
    except Exception:
        logger.exception("Failed to set commands")
        await update.effective_message.reply_text(
            "\u274c Failed to update commands."
        )


application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("help", cmd_help))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("setwebhook", cmd_setwebhook))
application.add_handler(CommandHandler("delwebhook", cmd_delwebhook))
application.add_handler(CommandHandler("setcommands", cmd_setcommands))


async def cmd_startbatch(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/startbatch")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text(
            "Unable to start batch here."
        )
        return
    await asyncio.to_thread(start_forward_batch, chat_id, user_id)
    await update.effective_message.reply_text(
        "Started forward-collection batch. Forward messages now; when finished run /endbatch to process them."
    )


async def cmd_endbatch(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/endbatch")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text(
            "Unable to finish batch here."
        )
        return
    items = await asyncio.to_thread(
        get_forward_items, chat_id, user_id
    )
    if not items:
        await update.effective_message.reply_text(
            "No forwarded items were collected in the batch."
        )
        return

    # enqueue a single batch job which processes items in order
    if config.REDIS_URL:
        ok = await asyncio.to_thread(
            enqueue_job, "process_document_batch_job", chat_id, items, user_id
        )
        if ok:
            await asyncio.to_thread(clear_forward_batch, chat_id, user_id)
            await update.effective_message.reply_text(
                f"Queued batch with {len(items)} items for processing.\n"
                f"Job ID: `{ok}` — use /canceljob {ok} to cancel it."
            )
            return
        # fall through to inline execution on failure

    # fallback: run batch processing inline in background
    try:
        import tasks

        # run in executor to avoid blocking
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, tasks.process_document_batch_job, chat_id, items, user_id
        )
        await asyncio.to_thread(clear_forward_batch, chat_id, user_id)
        await update.effective_message.reply_text(
            f"Processed batch with {len(items)} items."
        )
    except Exception:
        logger.exception("Failed to process batch inline")
        await update.effective_message.reply_text(
            "\u274c Error processing batch. Check server logs for details."
        )


async def cmd_cancelbatch(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "/cancelbatch")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text(
            "Unable to cancel batch here."
        )
        return
    await asyncio.to_thread(clear_forward_batch, chat_id, user_id)
    await update.effective_message.reply_text(
        "Cancelled and cleared forwarded batch."
    )


application.add_handler(CommandHandler("startbatch", cmd_startbatch))
application.add_handler(CommandHandler("endbatch", cmd_endbatch))
application.add_handler(CommandHandler("cancelbatch", cmd_cancelbatch))


async def cmd_sessionstatus(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Show userbot session health status (owner only)."""
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text(
            "\u26d4 Only the bot owner can run this command."
        )
        return

    try:
        checker = get_session_healthchecker()
        # Run a fresh check (doesn't wait for interval)
        await checker.run_once()
        text = checker.format_status_text()
        await update.effective_message.reply_text(text, parse_mode="Markdown")
    except Exception as exc:
        logger.exception("/sessionstatus failed: %s", exc)
        await update.effective_message.reply_text(
            "Failed to check session health. Check server logs for details."
        )


application.add_handler(CommandHandler("sessionstatus", cmd_sessionstatus))

# ── Telethon / Userbot Login Commands (per-user) ──────────────
# Login flows live in utils/login_handler.py and use the background-task +
# asyncio.Future pattern. Each user logs in with their OWN Telethon or
# Pyrogram session; sessions are persisted per-user (JSON files + MongoDB)
# and every download/upload resolves that user's session.

from utils.login_handler import (  # noqa: E402
    cleanup_login_flow,
    register_login_handlers,
)


async def cmd_logout(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Log out the calling user's Telethon session (per-user)."""
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user_id = update.effective_user.id

    # Clean up any active login flow before logging out
    futures_map = context.application.bot_data.get("login_futures", {})
    if (
        user_id in futures_map
        and futures_map[user_id].get("task") is not None
        and not futures_map[user_id]["task"].done()
    ):
        await cleanup_login_flow(context, user_id)

    try:
        from utils.telethon_session import (
            _get_persisted_session_path,
            _invalidate_session_cache,
            get_telethon_session_path,
        )

        session_path = get_telethon_session_path()
        removed = []

        # 1. Remove global session files (backward-compatible)
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
                removed.append(session_path)
            except Exception:  # nosec B110
                pass
        for suffix in (
            ".session",
            ".session-journal",
            ".session.lock",
            ".session.json",
        ):
            path_with_suffix = session_path + suffix
            if os.path.exists(path_with_suffix):
                try:
                    os.remove(path_with_suffix)
                    removed.append(path_with_suffix)
                except Exception:  # nosec B110
                    pass

        # 2. Remove per-user JSON session file
        per_user_json = _get_persisted_session_path(user_id=user_id)
        if os.path.exists(per_user_json):
            try:
                os.remove(per_user_json)
                removed.append(
                    f"Per-user JSON ({os.path.basename(per_user_json)})"
                )
            except Exception as exc:
                logger.debug(
                    "logout: failed to remove per-user JSON %s: %s",
                    per_user_json,
                    exc,
                )

        # 3. Remove per-user Telethon .session file on disk
        per_user_session = f"{session_path}.{user_id}.session"
        if os.path.exists(per_user_session):
            try:
                os.remove(per_user_session)
                removed.append(
                    f"Per-user .session ({os.path.basename(per_user_session)})"
                )
            except Exception as exc:
                logger.debug(
                    "logout: failed to remove per-user session %s: %s",
                    per_user_session,
                    exc,
                )

        # 4. Clear Telethon session from MongoDB (keep Pyrogram)
        try:
            db_model = context.application.bot_data.get("db_model")
            if db_model is not None and hasattr(db_model, "delete_session"):
                await db_model.delete_session(user_id)
                removed.append("MongoDB session")
            else:
                from utils.db import save_user_session

                await save_user_session(
                    user_id,
                    {
                        "telethon_session": "",
                        "string_session": "",
                        "logged_out": True,
                        "logged_out_at": time.time(),
                    },
                )
        except Exception:  # nosec B110
            pass

        # 5. Clear in-memory session cache for this user
        try:
            _invalidate_session_cache(user_id=user_id)
        except Exception:  # nosec B110
            pass

        if removed:
            await update.message.reply_text(
                "✅ Logged out and removed Telethon session files:\n"
                + "\n".join(removed)
            )
        else:
            await update.message.reply_text(
                "No local Telethon session file was found to remove."
            )
    except Exception as exc:
        logger.exception("/logout failed: %s", exc)
        await update.message.reply_text(
            "Failed to remove the Telethon session. Check server logs for details."
        )


async def cmd_logoutpyro(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Log out the calling user's Pyrogram session (per-user)."""
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user_id = update.effective_user.id

    # Clean up any active login flow before logging out
    futures_map = context.application.bot_data.get("login_futures", {})
    if (
        user_id in futures_map
        and futures_map[user_id].get("task") is not None
        and not futures_map[user_id]["task"].done()
    ):
        await cleanup_login_flow(context, user_id)

    try:
        removed = []

        # 1. Clear Pyrogram session from JSON file (per-user, then global)
        try:
            from utils.telethon_session import (
                save_session_string_to_file_async,
            )

            if await save_session_string_to_file_async(
                "", client_type="pyrogram", user_id=user_id
            ):
                removed.append("JSON file (pyrogram_session cleared)")
        except Exception as exc:
            logger.debug("logoutpyro: JSON per-user clear failed: %s", exc)
        try:
            if await save_session_string_to_file_async("", client_type="pyrogram"):
                removed.append("Global JSON file (pyrogram_session cleared)")
        except Exception as exc:
            logger.debug("logoutpyro: JSON global clear failed: %s", exc)

        # 2. Clear Pyrogram session from MongoDB
        try:
            db_model = context.application.bot_data.get("db_model")
            if db_model is not None and hasattr(db_model, "save_session"):
                await db_model.save_session(user_id, {"pyrogram_session": ""})
                removed.append("MongoDB (pyrogram_session cleared)")
            else:
                from utils.db import save_user_session

                await save_user_session(user_id, {"pyrogram_session": ""})
                removed.append("MongoDB (pyrogram_session cleared)")
        except Exception as exc:
            logger.debug("logoutpyro: MongoDB clear failed: %s", exc)

        if removed:
            await update.message.reply_text(
                "✅ Logged out of Pyrogram and cleared session:\n"
                + "\n".join(removed)
            )
        else:
            await update.message.reply_text(
                "No Pyrogram session was found to clear."
            )
    except Exception as exc:
        logger.exception("/logoutpyro failed: %s", exc)
        await update.message.reply_text(
            "Failed to clear the Pyrogram session. Check server logs for details."
        )


def _fmt_ts(ts) -> str:
    """Format a stored epoch timestamp (float) or datetime as a short human string."""
    if not ts:
        return "never"
    if hasattr(ts, "timestamp"):  # datetime / BSON date
        try:
            ts = ts.timestamp()
        except Exception:  # nosec B110
            return "?"
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return "?"
    age = time.time() - ts
    if age < 1:
        return "just now"
    if age < 60:
        return f"{int(age)}s ago"
    if age < 3600:
        return f"{int(age // 60)}m ago"
    if age < 86400:
        return f"{int(age // 3600)}h ago"
    if age < 86400 * 30:
        return f"{int(age // 86400)}d ago"
    try:
        return time.strftime("%b %d", time.localtime(ts))
    except (OverflowError, OSError, ValueError):
        return "?"


async def cmd_loginstatus(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Live session health check for the calling user (per-user)."""
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    await update.message.reply_text(
        "🩺 Testing sessions live — connecting to Telegram..."
    )

    calling_user_id = update.effective_user.id
    try:
        checker = get_session_healthchecker()
        health = await checker.run_once(user_id=calling_user_id)
    except Exception as exc:
        logger.exception("loginstatus: health check failed: %s", exc)
        health = {}

    pyro = health.get("pyrogram", {})
    tele = health.get("telethon", {})

    # ── Where the user's sessions are stored: per-user JSON file (ephemeral,
    # wiped on redeploy) + MongoDB (durable) ──
    per_user_json_session = None
    try:
        from utils.telethon_session import (
            _get_persisted_session_path,
            _load_all_sessions_from_file_async,
        )

        _per_user_path = _get_persisted_session_path(user_id=calling_user_id)
        per_user_json_session = await _load_all_sessions_from_file_async(
            user_id=calling_user_id
        )
    except Exception:
        logger.debug("bot: Per-user JSON file check failed")

    tele_per_user = bool(
        per_user_json_session and per_user_json_session.get("telethon_session")
    )
    pyro_per_user = bool(
        per_user_json_session and per_user_json_session.get("pyrogram_session")
    )

    mongo_session = None
    try:
        from utils.db import get_user_session  # noqa: PLC0415

        mongo_session = await get_user_session(calling_user_id)
    except Exception:
        logger.debug("bot: Mongo session state check failed")
    tele_mongo = bool(
        mongo_session and mongo_session.get("telethon_session")
    )
    pyro_mongo = bool(
        mongo_session and mongo_session.get("pyrogram_session")
    )
    mongo_legacy_only = bool(
        mongo_session
        and mongo_session.get("string_session")
        and not mongo_session.get("telethon_session")
        and not mongo_session.get("pyrogram_session")
    )

    has_api_id = bool(
        os.getenv("API_ID")
        or os.getenv("USERBOT_API_ID")
        or os.getenv("api_id")
        or os.getenv("userbot_api_id")
    )
    has_api_hash = bool(
        os.getenv("API_HASH")
        or os.getenv("USERBOT_API_HASH")
        or os.getenv("api_hash")
        or os.getenv("userbot_api_hash")
    )

    def _session_line(name: str, result: dict) -> str:
        if not result:
            return f"❌ **{name}** — Check failed (no result)"
        alive = result.get("alive", False)
        source = source_label(result.get("source"))
        if alive:
            phone = result.get("phone") or "?"
            dc = result.get("dc_id") or "?"
            latency = result.get("latency_ms") or "?"
            return (
                f"✅ **{name}** — Working\n"
                f"   Phone: `{phone}`\n"
                f"   DC: `{dc}` | Latency: `{latency}ms`\n"
                f"   Source: {source}"
            )
        else:
            err = (result.get("error") or "Not configured").replace("`", "")
            return f"❌ **{name}** — `{err}`\n   Source: {source}"

    # Get healthcheck interval + admin alert state from the checker
    check_interval = getattr(checker, "check_interval", 3600)
    admin_alerts = (
        "Enabled" if getattr(checker, "admin_user_id", None) else "Disabled"
    )
    userbot_enabled = config.ENABLE_USERBOT

    _tele_mongo_mark = (
        "⚠️ (legacy)" if mongo_legacy_only else ("✅" if tele_mongo else "❌")
    )
    storage_lines = [
        f"**Stored for you (user {calling_user_id}):**",
        f"  Telethon: JSON {'✅' if tele_per_user else '❌'} · MongoDB {_tele_mongo_mark}",
        f"  Pyrogram: JSON {'✅' if pyro_per_user else '❌'} · MongoDB {'✅' if pyro_mongo else '❌'}",
    ]
    if mongo_legacy_only:
        storage_lines.append(
            "  ⚠️ only a legacy `string_session` is in MongoDB (client type unknown)"
        )

    # Doc-level activity timestamps (last_active is set on every save; last_seen
    # on every bot interaction; created_at on first sighting).
    if mongo_session is not None:
        _ts_bits = []
        for _label, _key in (
            ("last active", "last_active"),
            ("last seen", "last_seen"),
            ("created", "created_at"),
        ):
            _v = mongo_session.get(_key)
            if _v:
                _ts_bits.append(f"{_label} {_fmt_ts(_v)}")
        if _ts_bits:
            storage_lines.append("  " + " · ".join(_ts_bits))

    lines = [
        "\U0001f510 **Live Session Status**",
        "",
        "**Userbot enabled:** " + ("✅ Yes" if userbot_enabled else "❌ No"),
        "**API credentials:** "
        + ("✅ Set" if has_api_id and has_api_hash else "⚠️ Missing API_ID/API_HASH"),
        "",
        _session_line("Telethon", tele),
        "",
        _session_line("Pyrogram", pyro),
        "",
        *storage_lines,
        "",
        f"🔔 Admin alerts: `{admin_alerts}`",
        f"🔄 Background check: every `{check_interval}s`",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_admin(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Manage allowed users (admin only): /admin add|remove|list <user_id>."""
    await _track_user_session(update, "/admin")
    user_id = update.effective_user.id
    is_admin = config.is_admin_user(user_id)
    if not is_admin:
        await update.message.reply_text("Unauthorized: admin only")
        return

    args = context.args if hasattr(context, "args") else []
    if not args:
        await update.message.reply_text("Usage: /admin add|remove|list <user_id>")
        return

    cmd = args[0].lower()
    if cmd == "list":
        users = sorted(list(config.ALLOWED_USER_IDS))
        await update.message.reply_text(f"Allowed users: {users}")
        return

    if len(args) < 2:
        await update.message.reply_text("Specify a user id")
        return

    try:
        target = int(args[1])
    except Exception:
        await update.message.reply_text("Invalid user id")
        return

    if cmd == "add":
        config.ALLOWED_USER_IDS.add(target)
        config.persist_allowed_users()
        await update.message.reply_text(f"Added {target} to allowed users")
        return
    if cmd == "remove":
        config.ALLOWED_USER_IDS.discard(target)
        config.persist_allowed_users()
        await update.message.reply_text(f"Removed {target} from allowed users")
        return

    await update.message.reply_text("Unknown admin command")


async def cmd_clearflood(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Clear the calling user's active login flow (FloodWait cleanup)."""
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user_id = update.effective_user.id
    futures_map = context.application.bot_data.get("login_futures", {})
    if (
        user_id in futures_map
        and futures_map[user_id].get("task") is not None
    ):
        await cleanup_login_flow(context, user_id)
        await update.message.reply_text(
            "✅ Cleared active login flow. You can run /login or /loginpyro again."
        )
    else:
        await update.message.reply_text("No active login flow found to clear.")


async def cmd_cancel(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Cancel an active login flow (or report nothing to cancel)."""
    await _track_user_session(update, "/cancel")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    user_id = update.effective_user.id
    futures_map = context.application.bot_data.get("login_futures", {})
    if (
        user_id in futures_map
        and futures_map[user_id].get("task") is not None
        and not futures_map[user_id]["task"].done()
    ):
        await cleanup_login_flow(context, user_id)
        await update.message.reply_text("❌ Login cancelled.")
        return
    await update.message.reply_text(
        "❌ Operation cancelled.\n\nSend /help to see available commands."
    )


# ── Job cancellation (progress task / RQ job / pipeline job) ───


def _wipe_job_redis_keys(job_id: str) -> None:
    """Delete all Redis keys associated with a job id (progress, io, cancel flag).

    Covers BOTH background pipes for consistency:
      - shared bookkeeping keys (progress/io/cancel)
      - BigFile pipeline keys (pdf:job:<id> hash, pdf:progress:<id>)
    (RQ job hashes/registries are handled by ``_cancel_rq_job`` itself.)
    """
    try:
        r = get_sync_redis()
        if not r:
            return
        for key in (
            f"progress:{job_id}",
            f"io:in:{job_id}",
            f"io:out:{job_id}",
            f"cancel:{job_id}",
            f"pdf:job:{job_id}",
            f"pdf:progress:{job_id}",
        ):
            try:
                r.delete(key)
            except Exception:  # nosec B110
                pass
    except Exception:  # nosec B110
        pass


def _cancel_rq_job(job_id: str, chat_id: int | None) -> bool:
    """Best-effort cancel of an RQ job by id, but only when the job originated
    from the caller's chat (ownership check for shared group chats).

    Robust against a known RQ 2.x race: ``job.cancel()`` can raise
    ``ValueError: Execution {id} not found in Redis`` when the job is in the
    started registry but its execution record is missing (or was already
    cleaned up). The ``cancel:<job_id>`` flag is set FIRST — that is the
    signal the RQ worker honours at its checkpoints (tasks.py) to abort the
    job even mid-flight — so cancellation is guaranteed even if RQ's own
    bookkeeping fails. ``job.cancel()`` is still attempted for clean
    bookkeeping; on failure the job is dropped from the queue/started
    registries manually and marked canceled so it can never run.
    """
    try:
        from rq.job import Job, JobStatus

        from utils.redis_client import get_sync_redis_raw

        # RQ stores job payloads pickled as raw bytes, so RQ operations must
        # use a NON-decoding connection (the decode_responses=True singleton
        # would UnicodeDecodeError on Job.fetch and silently fail to cancel).
        r = get_sync_redis_raw()
        if not r:
            return False
        job = Job.fetch(job_id, connection=r)
        # All enqueued jobs pass chat_id as the first positional argument.
        args = list(getattr(job, "args", None) or [])
        if chat_id is not None and (not args or args[0] != chat_id):
            return False

        # Belt: the worker aborts jobs on this flag, so set it BEFORE RQ
        # bookkeeping — a failed job.cancel() must never lose the cancel.
        # Use the SAME raw connection that just succeeded at Job.fetch (a
        # second connection may be in a transient error state and would
        # silently lose the flag inside the best-effort guard below).
        try:
            r.setex(f"cancel:{job_id}", 3600, "1")
        except Exception:  # nosec B110 - flag is best-effort
            pass

        def _drop_from_registries() -> None:
            """Fallback: remove the job from queue/started registries and mark
            it canceled, plus (re)set the cancel flag."""
            origin = getattr(job, "origin", None) or "default"
            # Wrong-type errors are impossible (queue is a list, wip is a
            # zset); each key op is independently guarded.
            try:
                r.lrem(f"rq:queue:{origin}", 0, job_id)
            except Exception:  # nosec B110
                pass
            try:
                r.zrem(f"rq:wip:{origin}", job_id)
            except Exception:  # nosec B110
                pass
            try:
                job.set_status(JobStatus.CANCELED)
            except Exception:  # nosec B110
                pass
            # Re-set the abort flag in case the belt attempt above failed.
            try:
                r.setex(f"cancel:{job_id}", 3600, "1")
            except Exception:  # nosec B110
                pass

        try:
            job.cancel()
            return True
        except Exception:  # nosec B110 - RQ 2.x execution-registry race etc.
            _drop_from_registries()
            return True
    except Exception:
        return False


def _cancel_pipeline_job(job_id: str, user_id: int | None) -> bool:
    """Best-effort cancel of a BigFilePipeline job: set the cancel flag and
    remove any queued entry from the ``pdf:jobs`` Redis list — but only for
    jobs owned by ``user_id`` (multi-user isolation)."""
    removed = False
    try:
        from utils.job_queue import JOB_LIST

        r = get_sync_redis()
        if not r:
            return False
        raw_items = r.lrange(JOB_LIST, 0, -1)
        for item in raw_items:
            raw = item.decode() if isinstance(item, bytes) else item
            try:
                d = json.loads(raw)
            except Exception:  # nosec B112 - skip non-JSON entries in the queue
                continue
            if d.get("job_id") == job_id and (
                user_id is None or d.get("user_id") == user_id
            ):
                try:
                    r.hset(f"pdf:job:{job_id}", mapping={"cancel": "1"})
                except Exception:  # nosec B110
                    pass
                try:
                    r.lrem(JOB_LIST, 0, item)
                    removed = True
                except Exception:  # nosec B110
                    pass
        return removed
    except Exception:
        return False


async def cmd_canceljob(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Cancel a queued or in-flight job by id (progress task, RQ job, or pipeline job)."""
    await _track_user_session(update, "/canceljob")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    args = context.args if hasattr(context, "args") else []
    if not args:
        await update.effective_message.reply_text(
            "Usage: /canceljob <job_id>\n\n"
            "You can find the job id in the 'Queued...' reply or the progress "
            "message (ID: xxxxxxxx)."
        )
        return
    job_id = args[0].strip()
    uid = getattr(update.effective_user, "id", None)
    chat_id = update.effective_chat.id if update.effective_chat else None

    actions = []
    owned = False

    # 1) Inline progress task — only the owning user may cancel it
    task_id = progress_tracker.find_task_id_by_prefix(job_id)
    if task_id:
        task = progress_tracker.get_task(task_id)
        if task is not None and task.user_id == uid:
            owned = True
            if await progress_tracker.cancel_task(task_id):
                actions.append(f"progress task `{task_id[:8]}`")

    # 2) RQ job (queued/started Bot API pipeline) — caller's chat only
    if await asyncio.to_thread(_cancel_rq_job, job_id, chat_id):
        owned = True
        actions.append(f"RQ job `{job_id}`")

    # 3) BigFilePipeline job — caller's own only
    if _cancel_pipeline_job(job_id, uid):
        owned = True
        actions.append(f"pipeline job `{job_id}`")

    if owned:
        # Ownership verified: wipe Redis keys + set the in-flight abort flag
        _wipe_job_redis_keys(job_id)
        try:
            r = get_sync_redis()
            if r:
                r.setex(f"cancel:{job_id}", 3600, "1")
        except Exception:  # nosec B110
            pass
        await update.effective_message.reply_text(
            "✅ Cancelled: " + ", ".join(actions)
        )
    else:
        await update.effective_message.reply_text(
            f"No active job found for you with id `{job_id}`. "
            "It may have already finished."
        )


# ── Register per-user login + auth commands ────────────────────
register_login_handlers(application)
application.add_handler(CommandHandler("logout", cmd_logout))
application.add_handler(CommandHandler("logoutpyro", cmd_logoutpyro))
application.add_handler(CommandHandler("loginstatus", cmd_loginstatus))
application.add_handler(CommandHandler("admin", cmd_admin))
application.add_handler(CommandHandler("clearflood", cmd_clearflood))
application.add_handler(CommandHandler("cancel", cmd_cancel))
application.add_handler(CommandHandler("canceljob", cmd_canceljob))


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """FastAPI lifespan: run startup logic and graceful shutdown teardown.

    Replaces the deprecated ``@app.on_event`` startup/shutdown handlers.
    ``on_startup`` / ``_on_shutdown`` / ``on_shutdown`` are module-level
    coroutines defined below and resolved at runtime (when the server starts),
    so forward references are safe.
    """
    await on_startup()
    try:
        yield
    finally:
        # Deliberate order: stop the session healthcheck BEFORE the worker/
        # DB/application shutdown so the checker never sends admin messages
        # during teardown.
        await _on_shutdown()
        await on_shutdown()


app = FastAPI(lifespan=_lifespan)


@app.middleware("http")
async def _security_headers_middleware(request: Request, call_next):
    """Add baseline hardening headers to every HTTP response.

    Flagged by the pre-deployment VulnClaw security audit (missing
    X-Content-Type-Options / X-Frame-Options / Referrer-Policy / HSTS).
    HSTS is only emitted when the request arrived over HTTPS (direct or via
    a trusted proxy) so local/plain-HTTP testing is unaffected.
    """
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault(
        "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
    )
    # Proxies may send a comma-separated list (e.g. "https, http"); the first
    # entry is the scheme the client used.
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",")[0]
    if request.url.scheme == "https" or forwarded_proto.strip().lower() == "https":
        response.headers.setdefault(
            "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
        )
    return response


# ── Application shutdown handler ────────────────────────────
async def _on_shutdown():
    """Graceful shutdown: stop session healthcheck and cleanup."""
    logger.info("Shutting down bot application...")
    try:
        stop_session_healthcheck()
        logger.info("Session healthcheck stopped")
    except Exception:  # nosec B110
        pass


# Background tasks references for graceful shutdown
_keep_alive_task = None
_worker_task = None
_worker_proc = None  # subprocess.Popen handle for the RQ worker
_cleanup_task = None
_longpoll_task = None
_shutdown_event = asyncio.Event()


async def on_startup() -> None:
    global \
        _keep_alive_task, \
        _worker_task, \
        _worker_proc, \
        _cleanup_task, \
        _longpoll_task, \
        _shc_task
    # Ensure storage directories exist
    for d in (
        config.STORAGE_PATH,
        config.INPUT_PATH,
        config.OUTPUT_PATH,
        config.TEMP_PATH,
        config.THUMBNAIL_PATH,
    ):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:  # nosec B110
            pass
    # Initialize application so handlers, bot, and context are ready
    await application.initialize()

    # ── Start the session healthcheck loop (needs a running loop) ──
    try:
        _admin_id = (
            OWNER_ID
            if OWNER_ID
            else (list(config.ADMIN_USERS)[0] if config.ADMIN_USERS else None)
        )
        if _admin_id:
            _shc_task = start_session_healthcheck(
                admin_user_id=_admin_id,
                bot_app=application,
                db_model=None,  # uses utils.db fallback for MongoDB persistence
                check_interval=int(
                    os.getenv("SESSION_HEALTHCHECK_INTERVAL", "3600")
                ),
            )
            logger.info("Session healthcheck started for admin %s", _admin_id)
        else:
            logger.info("No admin configured; session healthcheck disabled")
    except Exception as e:
        logger.warning("Session healthcheck init failed (non-fatal): %s", e)

    # ── Log cached user sessions on startup ──
    try:
        from utils.cache import get_cache

        cache = await get_cache()
        if cache:
            client = await cache._get_client()
            if client:
                keys = await client.keys("cache:user:*")
                logger.info(
                    "Startup: %d user sessions loaded from Redis cache",
                    len(keys) if keys else 0,
                )
    except Exception:
        logger.debug("Could not enumerate cached user sessions on startup")

    # ── Eagerly persist env-var session strings to per-user JSON + MongoDB ──
    # Mirrors the reference (media_conversion_bot/main.py): after a redeploy
    # the persisted per-user JSON files are empty, so /loginstatus shows the
    # owner's env session as missing and per-user resolution falls back to env
    # every time. Persisting here populates the owner's per-user file right away.
    try:
        from utils.telethon_session import (
            _load_all_sessions_from_file_async,
            save_session_string_to_file_async,
        )

        _admin_persist_id = (
            config.ADMIN_USER_ID
            or config.OWNER_ID
            or (sorted(config.ADMIN_USERS)[0] if config.ADMIN_USERS else None)
        )
        _existing_json = await _load_all_sessions_from_file_async()

        # Pyrogram session from env var
        _pyro_env = os.getenv("PYROGRAM_SESSION") or os.getenv(
            "USERBOT_PYROGRAM_SESSION"
        )
        if _pyro_env and _existing_json.get("pyrogram_session") != _pyro_env:
            await save_session_string_to_file_async(
                _pyro_env, client_type="pyrogram"
            )
        if _pyro_env and _admin_persist_id:
            await save_session_string_to_file_async(
                _pyro_env, client_type="pyrogram", user_id=_admin_persist_id
            )
            try:
                from utils.db import save_user_session

                await save_user_session(
                    _admin_persist_id, {"pyrogram_session": _pyro_env}
                )
            except Exception:  # nosec B110
                pass

        # Telethon session from env var
        _telethon_env = None
        for _k in (
            "API_SESSION",
            "SESSION",
            "api_session",
            "USERBOT_SESSION",
            "userbot_session",
            "TELETHON_SESSION",
            "telethon_session",
        ):
            _v = os.getenv(_k)
            if _v:
                _telethon_env = _v
                break
        if _telethon_env and _existing_json.get("telethon_session") != _telethon_env:
            await save_session_string_to_file_async(
                _telethon_env, client_type="telethon"
            )
        if _telethon_env and _admin_persist_id:
            await save_session_string_to_file_async(
                _telethon_env, client_type="telethon", user_id=_admin_persist_id
            )
            try:
                from utils.db import save_user_session

                await save_user_session(
                    _admin_persist_id, {"telethon_session": _telethon_env}
                )
            except Exception:  # nosec B110
                pass

        logger.info(
            "Startup: persisted env-var sessions to per-user JSON (admin=%s)",
            _admin_persist_id,
        )
    except Exception as exc:
        logger.debug("Startup env->per-user JSON persistence skipped: %s", exc)

    # ── Restore per-user JSON session files from MongoDB ──
    # The per-user JSON session files live on an ephemeral filesystem and are
    # wiped on every redeploy.  Re-materialize each user's JSON file from the
    # durable MongoDB store so per-user sessions (created via /login or
    # /loginpyro) keep working immediately after a deployment.
    try:
        from utils.telethon_session import restore_per_user_session_files

        await restore_per_user_session_files()
    except Exception as exc:
        logger.debug("Startup: per-user session file restore skipped: %s", exc)

    # ── Background worker subprocess (with auto-restart supervision) ──
    _worker_proc = None
    if os.getenv("RUN_WORKER_IN_PROC", "false").lower() in (
        "1",
        "true",
        "yes",
    ):

        async def _worker_supervisor():
            """Monitor the RQ worker subprocess and restart it if it crashes."""
            import subprocess as _sub  # nosec - B404: needed for worker process supervision with list form (no shell)
            import sys as _sys

            worker_path = os.path.join(os.getcwd(), "worker.py")
            restart_delay = 5

            while not _shutdown_event.is_set():
                try:
                    global _worker_proc
                    _worker_proc = _sub.Popen(  # nosec - B603: list form, no shell=True, fixed worker path
                        [_sys.executable, worker_path],
                        env=os.environ.copy(),
                        close_fds=True,
                    )
                    logger.info(
                        "Worker subprocess started pid=%s", _worker_proc.pid
                    )
                    loop = asyncio.get_running_loop()
                    rc = await loop.run_in_executor(None, _worker_proc.wait)
                    logger.warning(
                        "Worker subprocess exited (rc=%s), restarting in %ds...",
                        rc,
                        restart_delay,
                    )
                except Exception as e:
                    logger.exception("Worker supervisor error: %s", e)

                if _shutdown_event.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        _shutdown_event.wait(),
                        timeout=restart_delay,
                    )
                    break
                except TimeoutError:
                    continue

        _worker_task = asyncio.create_task(_worker_supervisor())
        logger.info("Worker supervisor started")

    # ── Long-poller (when USE_POLLING=true and no webhook) ──
    if USE_POLLING:
        await application.start()
        logger.info("Started polling mode")

        async def _longpoll_loop():
            """Background long-poller: fetch updates via getUpdates and dispatch."""
            offset = None
            poll_interval = int(os.getenv("LONGPOLL_INTERVAL", "1"))
            logger.info("Long-poller started (interval=%ds)", poll_interval)

            while not _shutdown_event.is_set():
                try:
                    updates = await application.bot.get_updates(
                        offset=offset,
                        timeout=30,
                        allowed_updates=[
                            "message",
                            "callback_query",
                            "edited_message",
                        ],
                    )
                    if updates:
                        for u in updates:
                            if getattr(u, "update_id", None) is not None:
                                offset = int(u.update_id) + 1
                            try:
                                await application.process_update(u)
                            except Exception:
                                logger.exception(
                                    "Failed to dispatch polled update"
                                )
                except TimeoutError:
                    # Normal timeout — no updates, keep polling
                    pass
                except Exception as e:
                    logger.warning("Long-poller error: %s", e)
                    try:
                        await asyncio.wait_for(
                            _shutdown_event.wait(),
                            timeout=poll_interval,
                        )
                        break
                    except TimeoutError:
                        continue

            logger.info("Long-poller stopped")

        _longpoll_task = asyncio.create_task(_longpoll_loop())

    elif WEBHOOK_URL:
        webhook_path = f"/webhook/{BOT_TOKEN}"
        full_url = WEBHOOK_URL.rstrip("/") + webhook_path
        await application.bot.set_webhook(
            url=full_url, secret_token=WEBHOOK_SECRET
        )
        try:
            masked_url = full_url.rsplit("/", 1)[0] + "/<REDACTED_BOT_TOKEN>"
        except Exception:
            masked_url = "<webhook_url_redacted>"
        logger.info("Webhook set to %s", masked_url)
    else:
        logger.warning(
            "No WEBHOOK_URL provided and USE_POLLING is false; bot won't receive updates."
        )

    # ── Webhook health monitor (optional) ──
    if WEBHOOK_URL:
        try:
            from utils.webhook_monitor import WebhookRecoveryManager

            _webhook_recovery = WebhookRecoveryManager(
                application, WEBHOOK_URL, WEBHOOK_SECRET
            )
            await _webhook_recovery.start()
            logger.info("Webhook recovery monitor started")
        except Exception as _wh_err:
            logger.debug("Webhook monitor not started: %s", _wh_err)

    # ── Periodic cleanup task ──
    try:
        from utils.cleanup import cleanup_manager as _cm

        async def _cleanup_loop():
            await _cm.start()

        _cleanup_task = asyncio.create_task(_cleanup_loop())
        logger.info(
            "Cleanup manager started (interval=%ds)", _cm.cleanup_interval
        )
    except Exception as e:
        logger.warning("Cleanup manager not started: %s", e)

    # ── Keep-alive heartbeat to prevent free-tier spin-down ──
    try:
        _ka_disabled = os.getenv("KEEP_ALIVE_DISABLED", "").lower() in (
            "1",
            "true",
            "yes",
        )
        if not _ka_disabled:
            _ka_url = os.getenv("KEEP_ALIVE_URL") or ""
            if not _ka_url:
                _railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
                if _railway_domain:
                    _ka_url = f"https://{_railway_domain}"
            if not _ka_url and WEBHOOK_URL:
                try:
                    parsed_ka = urlparse(WEBHOOK_URL)
                    if parsed_ka.netloc:
                        _ka_url = f"{parsed_ka.scheme}://{parsed_ka.netloc}"
                except Exception:  # nosec B110
                    pass

            if _ka_url:
                _ka_url = _ka_url.rstrip("/")
                _health_url = f"{_ka_url}/health"
                _ka_interval = max(
                    60, min(840, int(os.getenv("KEEP_ALIVE_INTERVAL", "600")))
                )

                async def _keep_alive_loop():
                    logger.info(
                        "Keep-alive heartbeat started: pinging %s every %ds",
                        _health_url,
                        _ka_interval,
                    )
                    try:
                        async with aiohttp.ClientSession() as _session:
                            while True:
                                try:
                                    async with _session.get(
                                        _health_url,
                                        timeout=aiohttp.ClientTimeout(
                                            total=10
                                        ),
                                    ) as _resp:
                                        logger.debug(
                                            "Keep-alive ping: %s", _resp.status
                                        )
                                except (
                                    TimeoutError,
                                    aiohttp.ClientError,
                                    OSError,
                                ) as _e:
                                    logger.debug(
                                        "Keep-alive ping failed (harmless): %s",
                                        _e,
                                    )
                                try:
                                    await asyncio.wait_for(
                                        _shutdown_event.wait(),
                                        timeout=_ka_interval,
                                    )
                                    break
                                except TimeoutError:
                                    continue
                                except asyncio.CancelledError:
                                    break
                    except asyncio.CancelledError:
                        pass
                    logger.info("Keep-alive heartbeat stopped")

                _keep_alive_task = asyncio.create_task(_keep_alive_loop())
                logger.info("Keep-alive heartbeat scheduled")
            else:
                logger.info(
                    "Keep-alive heartbeat disabled: no public URL available (set KEEP_ALIVE_URL, WEBHOOK_URL, or RAILWAY_PUBLIC_DOMAIN)"
                )
    except Exception as _ka_err:
        logger.warning("Failed to start keep-alive heartbeat: %s", _ka_err)


async def on_shutdown() -> None:
    _shutdown_event.set()
    try:
        # Cancel background tasks
        for task, name in [
            (_keep_alive_task, "keep-alive"),
            (_worker_task, "worker-supervisor"),
            (_cleanup_task, "cleanup"),
            (_longpoll_task, "long-poller"),
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                logger.info("Shutdown: %s task stopped", name)

        # Terminate worker subprocess
        if _worker_proc is not None:
            try:
                _worker_proc.terminate()
                try:
                    _worker_proc.wait(timeout=5)
                except Exception:
                    _worker_proc.kill()
                    _worker_proc.wait(timeout=3)
                logger.info("Shutdown: worker subprocess terminated")
            except Exception:  # nosec B110
                pass

        # Stop cleanup manager
        try:
            from utils.cleanup import cleanup_manager as _cm

            _cm.stop()
        except Exception:  # nosec B110
            pass

        # Close MongoDB connections
        try:
            from utils.db import close_db

            await close_db()
        except Exception:  # nosec B110
            pass

        # CRITICAL: Do NOT delete the webhook on shutdown.
        # On free-tier platforms, the webhook must persist so Telegram can
        # wake the service back up on the next incoming message.
        if USE_POLLING:
            await application.stop()
        await application.shutdown()
    except Exception:
        logger.exception("Error during shutdown")


@app.post("/webhook/{token}")
async def telegram_webhook(
    token: str,
    request: Request,
    background_tasks: BackgroundTasks,
    x_telegram_bot_api_secret_token: str | None = Header(
        default=None, alias="X-Telegram-Bot-Api-Secret-Token"
    ),
):
    """Receive incoming updates from Telegram via webhook.

    CSRF Protection:
    - URL path token must match BOT_TOKEN (prevents path guessing)
    - X-Telegram-Bot-Api-Secret-Token header must match WEBHOOK_SECRET
      (set when registering the webhook via /setwebhook or /set_webhook)
      This is Telegram's official CSRF protection mechanism.
    """
    # Layer 1: URL path token validation (constant-time comparison)
    if not secrets.compare_digest(token, BOT_TOKEN):
        logger.warning("Received webhook with invalid token")
        return {"ok": False}

    # Layer 2: Secret token header validation (CSRF protection)
    # Telegram sends this header when secret_token is configured in setWebhook
    if not x_telegram_bot_api_secret_token or not secrets.compare_digest(
        x_telegram_bot_api_secret_token, WEBHOOK_SECRET
    ):
        logger.warning(
            "Received webhook with invalid X-Telegram-Bot-Api-Secret-Token "
            "(expected=%s..., got=%s...)",
            WEBHOOK_SECRET[:8] if WEBHOOK_SECRET else "None",
            str(x_telegram_bot_api_secret_token)[:8]
            if x_telegram_bot_api_secret_token
            else "None",
        )
        return {"ok": False}

    data = await request.json()
    update = Update.de_json(data, application.bot)

    async def _run_update() -> None:
        # Guard against unobserved task exceptions from malformed updates.
        try:
            await application.process_update(update)
        except Exception:
            logger.exception("Failed to process webhook update")

    # Schedule processing in the running event loop to avoid threadpool issues
    asyncio.create_task(_run_update())
    return {"ok": True}


URL_RE = re.compile(r"https?://[^\s'\)\]\>]+", re.IGNORECASE)


async def download_url_to_file(url: str, dest_path: str) -> None:
    # SSRF prevention: validate URL before fetching and disable redirects
    if not _validate_url_safe(url):
        raise RuntimeError(f"SSRF prevention: blocked unsafe URL: {url[:100]}")
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, allow_redirects=False) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Download failed: {resp.status}")
            # Stream to file
            async with aiofiles.open(dest_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    await f.write(chunk)


async def handle_text_with_url(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await _track_user_session(update, "url")
    msg = update.effective_message
    if not msg or not msg.text:
        return
    # ── ACL check (open bot when ALLOWED_USER_IDS is empty) ──
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await msg.reply_text("Access denied. This bot is private.")
        return
    urls = URL_RE.findall(msg.text)
    if not urls:
        return

    user_id = getattr(update.effective_user, "id", None)
    _loop = asyncio.get_running_loop()

    for url in urls:
        url = url.rstrip(".,;!?)]")
        # quick check by extension
        if url.lower().endswith(".pdf"):
            # If redis available, enqueue background job to download and process URL
            chat_id = (
                msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
            )
            parsed = urlparse(url)
            base = os.path.basename(parsed.path) or "download.pdf"
            if not base.lower().endswith(".pdf"):
                base = base + ".pdf"
            if config.REDIS_URL:
                # ── Respect Telegram API rate limits (global 30/s + per-user 1/s) ──
                try:
                    await telegram_api_limiter.wait_if_needed(
                        str(getattr(update.effective_user, "id", 0))
                    )
                except Exception:  # nosec B110 - throttling is best-effort
                    pass
                ok = await asyncio.to_thread(
                    enqueue_job, "process_url_job", chat_id, url, base
                )
                if ok:
                    await msg.reply_text(
                        "Queued your PDF URL for background processing; I'll send the result when ready."
                    )
                    return
                # fall back to inline processing on enqueue failure

            tmpdir = (
                tempfile.mkdtemp(dir=config.TMP_DIR)
                if config.TMP_DIR
                else tempfile.mkdtemp()
            )
            try:
                file_path = os.path.join(tmpdir, base)
                await download_url_to_file(url, file_path)
                thumb_path = os.path.join(tmpdir, "thumb.jpg")
                create_thumbnail_from_pdf(file_path, thumb_path)
                _url_file_size = os.path.getsize(file_path)
                _url_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
                if _url_file_size > _url_limit and _check_userbot_available(user_id):
                    await _send_with_upload_progress(
                        bot=context.bot,
                        chat_id=msg.chat.id
                        if getattr(msg, "chat", None)
                        else msg.chat_id,
                        file_path=file_path,
                        caption="Generated thumbnail from URL",
                        thumb_path=thumb_path,
                        user_id=getattr(update.effective_user, "id", None),
                        filename=base,
                        file_size=_url_file_size,
                        loop=_loop,
                        target_chat_id="me",
                    )
                else:
                    with (
                        open(file_path, "rb") as f_doc,
                        open(thumb_path, "rb") as f_thumb,
                    ):
                        input_doc = InputFile(f_doc, filename=base)
                        chat_id = (
                            msg.chat.id
                            if getattr(msg, "chat", None)
                            else msg.chat_id
                        )
                        await context.bot.send_document(
                            chat_id=chat_id,
                            document=input_doc,
                            thumbnail=f_thumb,
                            caption="Generated thumbnail from URL",
                        )
            except Exception as e:
                error_info = await handle_bot_error(
                    e, "URL PDF Processing", update=update
                )
                try:
                    await msg.reply_text(error_info["user_message"])
                except Exception:  # nosec B110
                    pass
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
            return
        else:
            # HEAD to detect content-type (disabled redirects for SSRF prevention)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(
                        url, allow_redirects=False
                    ) as resp:
                        ctype = resp.headers.get("Content-Type", "")
                        if "pdf" in ctype.lower():
                            tmpdir = (
                                tempfile.mkdtemp(dir=config.TMP_DIR)
                                if config.TMP_DIR
                                else tempfile.mkdtemp()
                            )
                            try:
                                parsed = urlparse(url)
                                base = (
                                    os.path.basename(parsed.path)
                                    or "download.pdf"
                                )
                                if not base.lower().endswith(".pdf"):
                                    base = base + ".pdf"
                                file_path = os.path.join(tmpdir, base)
                                await download_url_to_file(url, file_path)
                                thumb_path = os.path.join(tmpdir, "thumb.jpg")
                                create_thumbnail_from_pdf(
                                    file_path, thumb_path
                                )
                                with (
                                    open(file_path, "rb") as f_doc,
                                    open(thumb_path, "rb") as f_thumb,
                                ):
                                    input_doc = InputFile(f_doc, filename=base)
                                    chat_id = (
                                        msg.chat.id
                                        if getattr(msg, "chat", None)
                                        else msg.chat_id
                                    )
                                    await context.bot.send_document(
                                        chat_id=chat_id,
                                        document=input_doc,
                                        thumbnail=f_thumb,
                                        caption="Generated thumbnail from URL",
                                    )
                            except Exception as e:
                                error_info = await handle_bot_error(
                                    e,
                                    "URL PDF Processing (HEAD detect)",
                                    update=update,
                                )
                                try:
                                    await msg.reply_text(
                                        error_info["user_message"]
                                    )
                                except Exception:  # nosec B110
                                    pass
                            finally:
                                shutil.rmtree(tmpdir, ignore_errors=True)
                            return
            except Exception:  # nosec B112
                continue


application.add_handler(
    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_with_url)
)


def _verify_admin_header(admin_token: str) -> bool:
    if not config.ADMIN_SECRET or not admin_token:
        return False
    return secrets.compare_digest(admin_token, config.ADMIN_SECRET)


@app.get("/status")
async def status() -> str:
    # Return a minimal, non-sensitive status string
    return "active"


# ── Rate limiter for admin HTTP endpoints ─────────────────────
# Limits per client IP to prevent abuse of sensitive webhook operations
_admin_api_rate_limiter = RedisSlidingWindowRateLimiter(
    max_requests=5, window_seconds=60
)


def _get_client_ip(request: Request) -> str:
    """Extract client IP from request, respecting reverse proxy headers."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host or "unknown"
    return "unknown"


async def _rate_limit_admin_api(request: Request) -> None:
    """FastAPI dependency: rate limit by client IP, returns rate limit headers."""
    client_ip = _get_client_ip(request)
    allowed, remaining, reset_seconds = await _admin_api_rate_limiter.acquire(
        user_id=f"admin_api:{client_ip}"
    )

    headers = {
        "X-RateLimit-Limit": "5",
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Reset": str(int(reset_seconds)),
    }

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Max 5 requests per 60 seconds.",
            headers=headers,
        )


@app.get("/commands")
async def get_commands(
    admin_token: str | None = Header(default=None),
    _: None = Depends(_rate_limit_admin_api),
) -> dict:
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Admin token required")
    try:
        cmds = await application.bot.get_my_commands()
        return {"ok": True, "commands": [c.to_dict() for c in cmds]}
    except Exception:
        logger.exception("Failed to fetch commands")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch commands. Check server logs for details.",
        )


@app.post("/set_webhook")
async def set_webhook(
    request: Request,
    admin_token: str | None = Header(default=None),
    owner_id: str | None = Header(default=None),
    _: None = Depends(_rate_limit_admin_api),
) -> dict:
    # OWNER_ID header check: only the bot owner can call this endpoint
    if OWNER_ID and (not owner_id or str(owner_id) != str(OWNER_ID)):
        raise HTTPException(
            status_code=403, detail="Only the bot owner can set the webhook"
        )
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    body = await request.json()
    url = body.get("url") if isinstance(body, dict) else None
    if not url:
        raise HTTPException(
            status_code=400, detail="Missing 'url' in JSON body"
        )
    webhook_path = f"/webhook/{BOT_TOKEN}"
    full_url = url.rstrip("/") + webhook_path
    try:
        await application.bot.set_webhook(
            url=full_url,
            secret_token=WEBHOOK_SECRET,
        )
        return {"ok": True, "webhook": full_url, "csrf_protected": True}
    except Exception:
        logger.exception("Failed to set webhook")
        raise HTTPException(
            status_code=500,
            detail="Failed to set webhook. Check server logs for details.",
        )


@app.post("/delete_webhook")
async def delete_webhook(
    request: Request,
    admin_token: str | None = Header(default=None),
    owner_id: str | None = Header(default=None),
    _: None = Depends(_rate_limit_admin_api),
) -> dict:
    if OWNER_ID and (not owner_id or str(owner_id) != str(OWNER_ID)):
        raise HTTPException(
            status_code=403, detail="Only the bot owner can delete the webhook"
        )
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    try:
        await application.bot.delete_webhook()
        return {"ok": True}
    except Exception:
        logger.exception("Failed to delete webhook")
        raise HTTPException(
            status_code=500,
            detail="Failed to delete webhook. Check server logs for details.",
        )


@app.post("/admin/purge_s3")
async def admin_purge_s3(
    request: Request,
    admin_token: str | None = Header(default=None),
    owner_id: str | None = Header(default=None),
    _: None = Depends(_rate_limit_admin_api),
) -> dict:
    # OWNER_ID header check: only the bot owner can purge S3
    if OWNER_ID and (not owner_id or str(owner_id) != str(OWNER_ID)):
        raise HTTPException(
            status_code=403, detail="Only the bot owner can purge S3"
        )
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    body = await request.json()
    ttl = int(body.get("ttl_seconds", 0)) if isinstance(body, dict) else 0
    prefix = (
        body.get("prefix", "pdf-bot/")
        if isinstance(body, dict)
        else "pdf-bot/"
    )
    if ttl <= 0:
        raise HTTPException(status_code=400, detail="ttl_seconds must be > 0")

    try:
        from storage import purge_objects_older_than
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="storage.purge_objects_older_than is unavailable",
        )

    # run in executor to avoid blocking
    try:
        loop = asyncio.get_running_loop()
        deleted = await loop.run_in_executor(
            None, purge_objects_older_than, ttl, prefix
        )
        return {"ok": True, "deleted": deleted}
    except Exception:
        logger.exception("Failed purging S3 objects")
        raise HTTPException(
            status_code=500,
            detail="Failed to purge S3 objects. Check server logs for details.",
        )


@app.post("/set_commands")
async def set_commands(
    request: Request,
    admin_token: str | None = Header(default=None),
    owner_id: str | None = Header(default=None),
    _: None = Depends(_rate_limit_admin_api),
) -> dict:
    # OWNER_ID header check: only the bot owner can call this endpoint
    if OWNER_ID and (not owner_id or str(owner_id) != str(OWNER_ID)):
        raise HTTPException(
            status_code=403, detail="Only the bot owner can set commands"
        )
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    body = await request.json()
    items = body.get("commands") if isinstance(body, dict) else None
    try:
        if items and isinstance(items, list):
            cmds = [
                BotCommand(it.get("command"), it.get("description", ""))
                for it in items
            ]
            await application.bot.set_my_commands(cmds)
        else:
            # fallback to default commands
            await application.bot.set_my_commands(
                [
                    BotCommand("start", "Start interaction with the bot"),
                    BotCommand("help", "Show help and available commands"),
                    BotCommand("status", "Get bot status"),
                ]
            )
        return {"ok": True}
    except Exception:
        logger.exception("Failed to set commands")
        raise HTTPException(
            status_code=500,
            detail="Failed to set commands. Check server logs for details.",
        )


@app.get("/")
async def root() -> str:
    return "active"


@app.get("/health")
async def health() -> str:
    return "active"


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")  # nosec - B104: container deployment, configurable via HOST env var
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host=host, port=port, log_level="info")
