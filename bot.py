import os
import logging
import tempfile
import shutil
from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks
import asyncio
import re
import aiohttp
import aiofiles
import json
import time
from urllib.parse import urlparse
from telegram import InputFile
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    CommandHandler,
    filters,
)

from tools import create_thumbnail_from_pdf, create_thumbnail_from_image
import config
from config import OWNER_ID
from utils.progress_tracker import progress_tracker, send_progress_update
from utils.error_handler import (
    BotErrorHandler,
    get_error_handler,
    handle_bot_error,
    async_error_handler,
)
from utils.rate_limiter import TelegramAPIRateLimiter
from utils.bigfile_pipeline import BigFilePipeline

# ── Logging configuration (must be before any logger usage) ──
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level)
# keep httpx at least INFO to avoid leaking full request URLs in DEBUG logs
logging.getLogger("httpx").setLevel(max(numeric_level, logging.INFO))
logging.getLogger("rq").setLevel(numeric_level)
logging.getLogger("telegram").setLevel(numeric_level)
logger = logging.getLogger(__name__)

# Global error handler and rate limiter instances
bot_error_handler = get_error_handler()
telegram_api_limiter = TelegramAPIRateLimiter()

# Global BigFilePipeline instance for large file ingestion
_bigfile_pipeline = None
try:
    _bigfile_pipeline = BigFilePipeline()
    logger.info("BigFilePipeline initialized")
except Exception as e:
    logger.warning("BigFilePipeline init failed (non-fatal): %s", e)


# ── Userbot availability cache (checked once per handler call) ──
def _check_userbot_available() -> bool:
    """Return True if a Telethon or Pyrogram userbot session is configured."""
    try:
        from utils.telethon_session import has_usable_telethon_session, get_pyrogram_session_string
        return has_usable_telethon_session() or bool(get_pyrogram_session_string())
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
        except Exception:
            pass
    return _cb


async def _notify_download_failed(msg, file_size=None):
    """Send a helpful error message when all download methods fail for a large file."""
    try:
        relay_chat = config.RELAY_CHAT_ID
        options = []
        if relay_chat:
            options.append("- Add the userbot account to a group where the bot is also a member, then send the file there.")
        else:
            options.append("- Set RELAY_CHAT_ID env var to a group where both bot and userbot are members.")
        options.append("- Send a public HTTPS URL to the file instead.")
        if file_size:
            mb_size = file_size // (1024 * 1024)
            options.append(f"- Upload a smaller file (under 50MB). Your file is ~{mb_size} MB.")
        else:
            options.append("- Upload a smaller file (under 50MB).")
        await msg.reply_text(
            "Failed to download file. All methods tried:\n"
            "1. Relay group (forward + userbot download)\n"
            "2. Direct chat download (userbot in same chat)\n"
            "3. S3 pipeline (if configured)\n\n"
            "Options:\n" + "\n".join(options)
        )
    except Exception:
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
):
    """Try relay group -> forward source -> direct chat -> BigFilePipeline for userbot download.

    Follows the reference pattern from media_conersion_bot: tries the original
    forward source (when available) before relay/direct chat, since the userbot
    may have access to the original chat.

    Creates a progress task and tries each download method in sequence.

    Args:
        forward_info: Dict with keys 'chat_id', 'message_id', 'user_id' from forwarded messages.
                      When available, the userbot tries the original forward source first.

    Returns:
        "local"    -> file downloaded to file_path, caller should thumbnail + send
        "pipeline" -> handled async by BigFilePipeline, caller should return
        False      -> all methods failed, user already notified
    """
    import uuid as _uuid
    from utils.userbot_downloader import download_forward_via_userbot

    task_id = _uuid.uuid4().hex[:12]
    task = progress_tracker.create_task(task_id, user_id or 0, filename, file_size or 0)
    task.status = "downloading"
    task.start()
    progress_msg_id = await send_progress_update(msg.chat.id, msg.get_bot(), task)
    _cb = _make_progress_cb(task.task_id, loop)

    dl_ok = False

    # 0) Forward source (original chat, if available and different from current chat)
    if forward_info:
        fwd_chat_id = forward_info.get("chat_id")
        fwd_msg_id = forward_info.get("message_id")
        if fwd_chat_id and fwd_msg_id and fwd_chat_id != msg.chat.id:
            try:
                logger.info(
                    "forward source: trying userbot download from %s/%s",
                    fwd_chat_id, fwd_msg_id,
                )
                dl_ok = await download_forward_via_userbot(
                    chat_id=fwd_chat_id,
                    message_id=fwd_msg_id,
                    dest_path=file_path,
                    progress_callback=_cb,
                )
            except Exception as fwd_err:
                logger.warning("forward source download failed: %s", fwd_err)

    # 1) Relay group: forward to relay chat -> download via userbot
    if not dl_ok:
        relay_chat = config.RELAY_CHAT_ID
        if relay_chat:
            try:
                relay_chat_id = int(relay_chat)
                _forwarded = await msg.get_bot().forward_message(
                    chat_id=relay_chat_id,
                    from_chat_id=msg.chat.id,
                    message_id=msg.message_id,
                )
                if _forwarded and getattr(_forwarded, "message_id", None):
                    relay_msg_id = _forwarded.message_id
                    logger.info(
                        "relay: forwarded %s/%s to %s/%s",
                        msg.chat.id, msg.message_id, relay_chat_id, relay_msg_id,
                    )
                    dl_ok = await download_forward_via_userbot(
                        chat_id=relay_chat_id,
                        message_id=relay_msg_id,
                        dest_path=file_path,
                        progress_callback=_cb,
                    )
            except Exception as relay_err:
                logger.warning("relay: forward or download failed: %s", relay_err)

    # 2) Direct chat download (group chat where userbot is a member)
    if not dl_ok:
        logger.info("relay failed or not configured, trying direct chat download")
        dl_ok = await download_forward_via_userbot(
            chat_id=msg.chat.id,
            message_id=msg.message_id,
            dest_path=file_path,
            progress_callback=_cb,
        )

    # 3) BigFilePipeline (S3 pipeline)
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
                    await send_progress_update(msg.chat.id, msg.get_bot(), task, progress_msg_id)
                await msg.reply_text(
                    f"Large file ({file_size // (1024*1024)} MB) queued for processing.\n"
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
            await send_progress_update(msg.chat.id, msg.get_bot(), task, progress_msg_id)
        return "local"
    else:
        await _notify_download_failed(msg, file_size)
        if task:
            await progress_tracker.fail_task(task.task_id, "All download methods failed")
            if progress_msg_id:
                try:
                    await send_progress_update(msg.chat.id, msg.get_bot(), task, progress_msg_id)
                except Exception:
                    pass
        return False


# ── Login flow state ───────────────────────────────────────────
# Tracks which users are currently in the /login flow
LOGIN_PENDING_USERS: set = set()


class AwaitingLoginFilter(filters.MessageFilter):
    """Filter text messages only for users in the Telethon login flow."""

    def filter(self, message):
        try:
            user = getattr(message, "from_user", None)
            return bool(user and user.id in LOGIN_PENDING_USERS)
        except Exception:
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
    except Exception:
        pass
    try:
        from utils.db import save_user_session as _ss
        _db_save_user_session = _ss
    except Exception:
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
            except Exception:
                pass
        # MongoDB (durable history)
        if _db_save_user_session is not None:
            try:
                await _db_save_user_session(uid, session_data)
            except Exception:
                pass
    except Exception:
        pass

# Optional RQ enqueue helper (import only when needed)
def enqueue_job(func_name: str, *args, **kwargs):
    try:
        from redis import Redis
        from rq import Queue
        import tasks

        redis_conn = Redis.from_url(config.REDIS_URL)
        q = Queue('default', connection=redis_conn)
        # lookup function from tasks
        func = getattr(tasks, func_name)
        q.enqueue(func, *args, **kwargs)
        return True
    except Exception:
        logger.exception("Failed to enqueue job for %s", func_name)
        return False

BOT_TOKEN = config.BOT_TOKEN
if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set")
    raise SystemExit("Missing BOT_TOKEN")

WEBHOOK_URL = config.WEBHOOK_URL
USE_POLLING = config.USE_POLLING

# Build async Application (python-telegram-bot v20+)
application = ApplicationBuilder().token(BOT_TOKEN).build()

# Batch-forward collection helpers (Redis-backed with local fallback)
local_forward_batches = {}


def _get_redis_conn():
    if not config.REDIS_URL:
        return None
    try:
        from redis import Redis

        return Redis.from_url(config.REDIS_URL)
    except Exception:
        logger.exception("Redis not available for forward-batch storage")
        return None


def _batch_keys(chat_id: int, user_id: int) -> tuple:
    base = f"forward_batch:{chat_id}:{user_id}"
    return base + ":active", base + ":items"


def start_forward_batch(chat_id: int, user_id: int) -> bool:
    r = _get_redis_conn()
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
    r = _get_redis_conn()
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
    r = _get_redis_conn()
    if r:
        _, items_key = _batch_keys(chat_id, user_id)
        try:
            raw = r.lrange(items_key, 0, -1)
            return [json.loads(x.decode() if isinstance(x, bytes) else x) for x in raw] if raw else []
        except Exception:
            logger.exception("Failed to read forward items from Redis")
            return []
    key = (chat_id, user_id)
    return list(local_forward_batches.get(key, []))


def clear_forward_batch(chat_id: int, user_id: int) -> bool:
    r = _get_redis_conn()
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
    r = _get_redis_conn()
    if r:
        active_key, _ = _batch_keys(chat_id, user_id)
        try:
            return bool(r.exists(active_key))
        except Exception:
            return False
    key = (chat_id, user_id)
    return key in local_forward_batches



async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "document")
    msg = update.effective_message
    if not msg or not msg.document:
        return

    doc = msg.document
    # If REDIS_URL provided, enqueue background job and return immediately
    chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
    filename = doc.file_name or f"file_{doc.file_id}"
    mime = getattr(doc, "mime_type", "") or ""
    # If this was forwarded and a forward-batch is active for this sender, store metadata and return
    is_forwarded = bool(getattr(msg, "forward_from", None) or getattr(msg, "forward_from_chat", None) or getattr(msg, "forward_date", None))
    user_id = getattr(update.effective_user, "id", None)
    if is_forwarded and is_batch_active(chat_id, user_id):
        item = {"file_id": doc.file_id, "file_unique_id": getattr(doc, 'file_unique_id', None), "filename": filename, "mime": mime}
        append_forward_item(chat_id, user_id, item)
        await msg.reply_text(f"Added forwarded file to batch: {filename}")
        return

    # ── Capture forward metadata (for userbot fallback) ──────────────
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
    except Exception:
        pass

    # If Telegram reports a file_size on the Document, check it against the configured
    # upload limit before attempting to enqueue or download. Telegram's Bot API will
    # reject downloads for files larger than the bot's allowed size (returns 400 "file is too big").
    file_size = getattr(doc, 'file_size', None)
    upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
    use_userbot_download = False
    if file_size and upload_limit and file_size > upload_limit:
        _userbot_ok = _check_userbot_available()
        if _userbot_ok:
            use_userbot_download = True
            logger.info(
                "file too large for Bot API (%d MB), falling back to userbot download",
                file_size // (1024 * 1024)
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
            # Pass message_id + forward_info so the worker has context for userbot fallback
            ok = enqueue_job(
                'process_document_job',
                chat_id, doc.file_id, filename, mime,
                getattr(doc, 'file_unique_id', None),
                msg.message_id, forward_info,
            )
            if ok:
                await msg.reply_text("Queued your file for background processing; I'll send the result when ready.")
                return
            # fall through to inline processing on enqueue failure

    tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
    progress_msg_id = None
    task = None
    _dl_success = False
    _loop = asyncio.get_running_loop()
    try:
        file_path = os.path.join(tmpdir, filename)

        if use_userbot_download:
            # ── Big file download: forward source → relay → direct → pipeline ──
            _dl_result = await _userbot_download_fallback(
                msg, file_path, filename, mime,
                file_size or 0, getattr(doc, 'file_unique_id', None),
                user_id, chat_id, _loop,
                forward_info=forward_info,
            )
            if _dl_result == "pipeline":
                return
            if not _dl_result:
                return
        else:
            # ── Normal Bot API download ──
            file = await context.bot.get_file(doc.file_id)
            if file_size and file_size > 1024 * 1024:  # only show progress for files >1MB
                import uuid as _uuid
                task_id = _uuid.uuid4().hex[:12]
                task = progress_tracker.create_task(task_id, user_id or 0, filename, file_size)
                progress_msg_id = await send_progress_update(msg.chat.id, context.bot, task)
                task.start()
                task.status = "downloading"

                await file.download_to_drive(custom_path=file_path, read_timeout=300, write_timeout=300)
                task.update_progress(file_size or os.path.getsize(file_path))
                if progress_msg_id:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
            else:
                await file.download_to_drive(custom_path=file_path)
            _dl_success = True

        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        lower = filename.lower()
        mime = mime

        # Return original file unchanged but attach generated thumbnail as header
        if lower.endswith('.pdf') or mime == 'application/pdf':
            create_thumbnail_from_pdf(file_path, thumb_path)
        elif mime.startswith('image/'):
            create_thumbnail_from_image(file_path, thumb_path)
        else:
            # generic placeholder thumbnail
            from PIL import Image
            im = Image.new('RGB', (320, 320), (240, 240, 240))
            im.save(thumb_path, 'JPEG', quality=85)

        with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
            input_doc = InputFile(f_doc, filename=filename)
            chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
            await context.bot.send_document(chat_id=chat_id, document=input_doc, thumbnail=f_thumb,
                                           caption="Here is your file with an auto-generated cover preview.")
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
    except Exception as e:
        # Try userbot fallback if Bot API download failed
        if not _dl_success and _check_userbot_available():
            logger.info("Bot API download failed, falling back to userbot download for %s", filename)
            try:
                _dl_result = await _userbot_download_fallback(
                    msg, file_path, filename, mime,
                    file_size or 0, getattr(doc, 'file_unique_id', None),
                    user_id, chat_id, _loop,
                    forward_info=forward_info,
                )
                if _dl_result == "local":
                    # Retry thumbnail + send
                    thumb_path = os.path.join(tmpdir, "thumb.jpg")
                    if filename.lower().endswith('.pdf') or mime == 'application/pdf':
                        create_thumbnail_from_pdf(file_path, thumb_path)
                    elif mime.startswith('image/'):
                        create_thumbnail_from_image(file_path, thumb_path)
                    else:
                        from PIL import Image
                        im = Image.new('RGB', (320, 320), (240, 240, 240))
                        im.save(thumb_path, 'JPEG', quality=85)

                    with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                        input_doc = InputFile(f_doc, filename=filename)
                        await context.bot.send_document(
                            chat_id=chat_id, document=input_doc, thumbnail=f_thumb,
                            caption="Here is your file (downloaded via userbot) with an auto-generated cover preview."
                        )
                    return
            except Exception as ub_err:
                logger.exception("Userbot download fallback also failed: %s", ub_err)

        # Normal error handling
        error_info = await handle_bot_error(e, "PDF Document Processing", update=update)
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
                except Exception:
                    pass
        try:
            await msg.reply_text(error_info["user_message"])
        except Exception:
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "photo")
    msg = update.effective_message
    if not msg or not msg.photo:
        return

    photo = msg.photo[-1]

    # If REDIS_URL configured, enqueue background job and return immediately
    chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
    filename = f"photo_{photo.file_id}.jpg"
    # If this photo was forwarded and batch collection is active, append to batch
    is_forwarded = bool(getattr(msg, "forward_from", None) or getattr(msg, "forward_from_chat", None) or getattr(msg, "forward_date", None))
    user_id = getattr(update.effective_user, "id", None)
    if is_forwarded and is_batch_active(chat_id, user_id):
        item = {"file_id": photo.file_id, "file_unique_id": getattr(photo, 'file_unique_id', None), "filename": filename, "mime": "image/jpeg"}
        append_forward_item(chat_id, user_id, item)
        await msg.reply_text(f"Added forwarded photo to batch: {filename}")
        return

    # ── Capture forward metadata (for userbot fallback) ──────────────
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
    except Exception:
        pass

    # Thumbnail caching disabled

    if config.REDIS_URL:
        ok = enqueue_job(
            'process_document_job',
            chat_id, photo.file_id, filename, 'image/jpeg',
            getattr(photo, 'file_unique_id', None),
            msg.message_id, photo_forward_info,
        )
        if ok:
            await msg.reply_text("Queued your photo for background processing; I'll send the result when ready.")
            return

    tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
    progress_msg_id = None
    task = None
    _dl_success = False
    _loop = asyncio.get_running_loop()
    _userbot_ok = _check_userbot_available()
    try:
        file_path = os.path.join(tmpdir, filename)

        # Check if photo is too large for Bot API
        photo_size = getattr(photo, 'file_size', None) or 0
        upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024

        if photo_size > upload_limit and _userbot_ok:
            # ── Userbot download path for large photos ──
            _dl_result = await _userbot_download_fallback(
                msg, file_path, filename, "image/jpeg",
                photo_size, getattr(photo, 'file_unique_id', None),
                user_id, chat_id, _loop,
                forward_info=photo_forward_info,
            )
            if _dl_result == "pipeline":
                return
            if not _dl_result:
                return
        else:
            # ── Normal Bot API download ──
            file = await context.bot.get_file(photo.file_id)
            if photo_size > 1024 * 1024:
                import uuid as _uuid
                task_id = _uuid.uuid4().hex[:12]
                task = progress_tracker.create_task(task_id, user_id or 0, filename, photo_size)
                progress_msg_id = await send_progress_update(msg.chat.id, context.bot, task)
                task.start()
                task.status = "downloading"

            await file.download_to_drive(custom_path=file_path)

            if task:
                task.update_progress(os.path.getsize(file_path))
                task.status = "processing"
                if progress_msg_id:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
            _dl_success = True

        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        create_thumbnail_from_image(file_path, thumb_path)            # send original image back as document to preserve original bytes, attach thumbnail
        with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
            input_doc = InputFile(f_doc, filename=os.path.basename(file_path))
            chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
            await context.bot.send_document(chat_id=chat_id, document=input_doc, thumbnail=f_thumb,
                                           caption="Here is your image with an auto-generated thumbnail.")
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
    except Exception as e:
        # If Bot API download failed but userbot is available, try fallback
        if not _dl_success and _userbot_ok:
            logger.info("Bot API download failed for photo, falling back to userbot")
            try:
                _dl_result = await _userbot_download_fallback(
                    msg, file_path, filename, "image/jpeg",
                    photo_size or 0, getattr(photo, 'file_unique_id', None),
                    user_id, chat_id, _loop,
                    forward_info=photo_forward_info,
                )
                if _dl_result == "local":
                    thumb_path = os.path.join(tmpdir, "thumb.jpg")
                    create_thumbnail_from_image(file_path, thumb_path)
                    with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                        input_doc = InputFile(f_doc, filename=os.path.basename(file_path))
                        await context.bot.send_document(
                            chat_id=msg.chat.id,
                            document=input_doc, thumbnail=f_thumb,
                            caption="Here is your image (downloaded via userbot) with an auto-generated thumbnail."
                        )
                    return
            except Exception as ub_err:
                logger.exception("Userbot download fallback for photo also failed: %s", ub_err)

        error_info = await handle_bot_error(e, "Photo Processing", update=update)
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
                except Exception:
                    pass
        try:
            await msg.reply_text(error_info["user_message"])
        except Exception:
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
application.add_handler(MessageHandler(filters.PHOTO, handle_photo))


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/start")
    await update.effective_message.reply_text(
        "Hello! Send me a PDF or image and I'll return a thumbnail (PDF first page as cover)."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/help")
    text = (
        "/start - start\n"
        "/help - this help\n"
        "/status - get bot status\n"
        "/login - (owner) login Telethon/Pyrogram userbot\n"
        "/loginstatus - (owner) show login status\n"
        "/logout - (owner) logout and clear session\n"
        "/clearflood - (owner) clear flood wait / resend code\n"
        "/setwebhook <url> - (admin) set webhook to URL\n"
        "/delwebhook - (admin) delete webhook\n"
        "/setcommands - (admin) set bot command list\n"
        "/startbatch - start collecting forwarded files\n"
        "/endbatch - process collected batch\n"
        "/cancelbatch - cancel batch collection\n"
    )
    await update.effective_message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/status")
    # Minimal, non-sensitive status reply
    await update.effective_message.reply_text("active")


async def cmd_setwebhook(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/setwebhook")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("⛔ Only the bot owner can run this command.")
        return
    args = context.args or []
    if not args:
        await update.effective_message.reply_text("Usage: /setwebhook https://example.com")
        return
    url = args[0]
    webhook_path = f"/webhook/{BOT_TOKEN}"
    full_url = url.rstrip("/") + webhook_path
    try:
        await context.bot.set_webhook(full_url)
        await update.effective_message.reply_text(f"Webhook set to {full_url}")
    except Exception as e:
        logger.exception("Failed to set webhook")
        await update.effective_message.reply_text(f"Error setting webhook: {e}")


async def cmd_delwebhook(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/delwebhook")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("⛔ Only the bot owner can run this command.")
        return
    try:
        await context.bot.delete_webhook()
        await update.effective_message.reply_text("Webhook deleted")
    except Exception as e:
        logger.exception("Failed to delete webhook")
        await update.effective_message.reply_text(f"Error deleting webhook: {e}")


async def cmd_setcommands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/setcommands")
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("⛔ Only the bot owner can run this command.")
        return
    # Default command set
    commands = [
        BotCommand("start", "Start interaction with the bot"),
        BotCommand("help", "Show help and available commands"),
        BotCommand("status", "Get bot status"),
        BotCommand("login", "(owner) Login Telethon userbot"),
        BotCommand("loginstatus", "(owner) Show login status"),
        BotCommand("logout", "(owner) Logout and clear session"),
        BotCommand("clearflood", "(owner) Clear flood wait / resend code"),
        BotCommand("startbatch", "Start collecting forwarded files"),
        BotCommand("endbatch", "Process collected batch"),
        BotCommand("cancelbatch", "Cancel batch collection"),
        BotCommand("setwebhook", "(admin) Set webhook URL"),
        BotCommand("delwebhook", "(admin) Delete webhook"),
    ]
    try:
        await context.bot.set_my_commands(commands)
        await update.effective_message.reply_text("Commands updated")
    except Exception as e:
        logger.exception("Failed to set commands")
        await update.effective_message.reply_text(f"Error setting commands: {e}")


application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("help", cmd_help))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("setwebhook", cmd_setwebhook))
application.add_handler(CommandHandler("delwebhook", cmd_delwebhook))
application.add_handler(CommandHandler("setcommands", cmd_setcommands))


async def cmd_startbatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/startbatch")
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text("Unable to start batch here.")
        return
    start_forward_batch(chat_id, user_id)
    await update.effective_message.reply_text("Started forward-collection batch. Forward messages now; when finished run /endbatch to process them.")


async def cmd_endbatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/endbatch")
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text("Unable to finish batch here.")
        return
    items = get_forward_items(chat_id, user_id)
    if not items:
        await update.effective_message.reply_text("No forwarded items were collected in the batch.")
        return

    # enqueue a single batch job which processes items in order
    if config.REDIS_URL:
        ok = enqueue_job('process_document_batch_job', chat_id, items)
        if ok:
            clear_forward_batch(chat_id, user_id)
            await update.effective_message.reply_text(f"Queued batch with {len(items)} items for processing.")
            return
        # fall through to inline execution on failure

    # fallback: run batch processing inline in background
    try:
        import tasks
        # run in executor to avoid blocking
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, tasks.process_document_batch_job, chat_id, items)
        clear_forward_batch(chat_id, user_id)
        await update.effective_message.reply_text(f"Processed batch with {len(items)} items.")
    except Exception as e:
        logger.exception("Failed to process batch inline")
        await update.effective_message.reply_text(f"Error processing batch: {e}")


async def cmd_cancelbatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "/cancelbatch")
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text("Unable to cancel batch here.")
        return
    clear_forward_batch(chat_id, user_id)
    await update.effective_message.reply_text("Cancelled and cleared forwarded batch.")


application.add_handler(CommandHandler("startbatch", cmd_startbatch))
application.add_handler(CommandHandler("endbatch", cmd_endbatch))
application.add_handler(CommandHandler("cancelbatch", cmd_cancelbatch))

# ── Telethon / Userbot Login Commands ────────────────────────


def _clear_login_flow(user_id, context):
    """Clean up all login flow state for a user."""
    try:
        LOGIN_PENDING_USERS.discard(user_id)
    except Exception:
        pass
    if context is not None and getattr(context, "user_data", None) is not None:
        try:
            login_task = context.user_data.get("login_start_task")
            if login_task is not None and not login_task.done():
                login_task.cancel()
        except Exception:
            pass
        try:
            fut = context.user_data.get("login_pending_future")
            if fut is not None and not fut.done():
                fut.cancel()
        except Exception:
            pass
        try:
            client = context.user_data.get("login_client")
            if client is not None:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(client.disconnect())
                except RuntimeError:
                    pass
        except Exception:
            pass
        for key in (
            "awaiting_login_phone",
            "awaiting_login_code",
            "awaiting_login_password",
            "login_phone",
            "login_client",
            "login_session_path",
            "login_code_sent_at",
            "login_code_sent_repr",
            "login_code_hash",
            "login_code_type",
            "login_flood_wait_until",
            "login_resend_count",
            "login_password_retry_count",
            "login_pending_future",
            "login_pending_type",
            "login_start_task",
            "login_flow_started",
        ):
            context.user_data.pop(key, None)


async def cmd_login(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start Telethon userbot login flow."""
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("\u26d4 Only the bot owner can run this command.")
        return

    try:
        from telethon import TelegramClient
    except ImportError:
        await update.effective_message.reply_text(
            "Telethon is not installed. Install telethon to use /login:\n"
            "pip install telethon"
        )
        return

    api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
    api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
    if not api_id or not api_hash:
        await update.effective_message.reply_text(
            "Missing Telethon credentials. Set API_ID and API_HASH in the environment."
        )
        return

    await update.effective_message.reply_text(
        "Please send the phone number for the userbot session in international format, e.g. +1234567890."
    )
    context.user_data["awaiting_login_phone"] = True
    LOGIN_PENDING_USERS.add(uid)


async def cmd_loginstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current Telethon/Pyrogram login status (owner only)."""
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("\u26d4 Only the bot owner can run this command.")
        return

    data = context.user_data
    sent_at = data.get("login_code_sent_at")
    resend_count = data.get("login_resend_count", 0)
    code_hash = data.get("login_code_hash")
    session_path = data.get("login_session_path")
    awaiting = {
        "phone": bool(data.get("awaiting_login_phone")),
        "code": bool(data.get("awaiting_login_code")),
        "password": bool(data.get("awaiting_login_password")),
    }

    telethon_ready = False
    try:
        from utils.telethon_session import has_usable_telethon_session
        telethon_ready = has_usable_telethon_session()
    except Exception:
        pass

    pyrogram_ready = False
    try:
        from utils.telethon_session import get_pyrogram_session_string
        pyrogram_ready = bool(get_pyrogram_session_string())
    except Exception:
        pass

    masked_hash = None
    try:
        if code_hash:
            s = str(code_hash)
            masked_hash = s[:4] + "..." + s[-4:]
    except Exception:
        pass

    lines = [
        "\U0001f510 **Login Status**",
        "",
        "**Telethon session:** " + ("\u2705 Available" if telethon_ready else "\u274c Not configured"),
        "**Pyrogram session:** " + ("\u2705 Available" if pyrogram_ready else "\u274c Not configured"),
        "",
        "**Active login flow:**",
        "  awaiting: " + str(awaiting),
        "  sent_at: " + str(sent_at),
        "  resend_count: " + str(resend_count),
        "  code_hash: " + str(masked_hash),
        "  session_path: " + str(session_path),
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove Telethon session files and clear login state."""
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("\u26d4 Only the bot owner can run this command.")
        return

    try:
        from utils.telethon_session import get_telethon_session_path
        session_path = get_telethon_session_path()
        removed = []
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
                removed.append(session_path)
            except Exception:
                pass
        for suffix in (".session", ".session-journal", ".session.lock"):
            path_with_suffix = session_path + suffix
            if os.path.exists(path_with_suffix):
                try:
                    os.remove(path_with_suffix)
                    removed.append(path_with_suffix)
                except Exception:
                    pass

        # Clean up temp session files
        try:
            import glob as _glob
            for f in _glob.glob(os.path.join(config.TEMP_PATH or "/tmp", "userbot_session*")):
                try:
                    os.remove(f)
                    removed.append(f)
                except Exception:
                    pass
        except Exception:
            pass

        # Clear Redis-cached session if any
        try:
            from utils.cache import get_cache
            cache = await get_cache()
            await cache.delete("telethon:session_string")
        except Exception:
            pass

        _clear_login_flow(uid, context)

        if removed:
            await update.effective_message.reply_text(
                "\u2705 Logged out and removed Telethon session files:\n" + "\n".join(removed)
            )
        else:
            await update.effective_message.reply_text(
                "No local Telethon session file was found to remove."
            )
    except Exception as exc:
        logger.exception("/logout failed: %s", exc)
        await update.effective_message.reply_text(
            "Failed to remove the Telethon session. Check server logs for details."
        )


async def _process_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle text messages during the Telethon login flow (phone, code, password)."""
    user_id = getattr(update.effective_user, "id", None)
    if not user_id:
        return

    # Respect FloodWait
    try:
        flood_until = context.user_data.get("login_flood_wait_until")
        if flood_until:
            now = time.time()
            if now < flood_until:
                remaining = int(flood_until - now)
                await update.message.reply_text(
                    f"Too many login attempts. Please wait {remaining} seconds before retrying."
                )
                return
            else:
                context.user_data.pop("login_flood_wait_until", None)
    except Exception:
        pass

    if context.user_data.get("awaiting_login_phone"):
        context.user_data["awaiting_login_phone"] = False
        phone = update.message.text.strip()
        await update.message.reply_text("Got phone number. Please wait while I generate the Telethon session...")

        try:
            from telethon import TelegramClient
        except ImportError:
            await update.message.reply_text(
                "Telethon is not installed. Install telethon to use /login."
            )
            _clear_login_flow(user_id, context)
            return

        api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
        api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
        try:
            api_id = int(api_id)
        except Exception:
            await update.message.reply_text("Configured API_ID is invalid. It must be an integer.")
            _clear_login_flow(user_id, context)
            return

        # Use the same session path that utils.telethon_session expects,
        # so build_telethon_client() and has_usable_telethon_session()
        # can detect and reuse the session after login completes.
        from utils.telethon_session import get_telethon_session_path
        session_path = get_telethon_session_path()
        os.makedirs(os.path.dirname(session_path) or ".", exist_ok=True)

        # Use file-based session so Telethon manages the .session file natively.
        # This way has_usable_telethon_session() and build_telethon_client()
        # can detect and reuse it across bot restarts.
        client = TelegramClient(session_path, api_id, api_hash)
        try:
            await client.connect()

            if await client.is_user_authorized():
                await update.message.reply_text(
                    f"Telethon session is already authorized and saved to {session_path}."
                )
                await client.disconnect()
                _clear_login_flow(user_id, context)
                return

            async def _do_start():
                from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError, FloodWaitError

                try:
                    loop = asyncio.get_running_loop()
                    context.user_data["login_phone"] = phone
                    context.user_data["login_client"] = client
                    context.user_data["login_session_path"] = session_path
                    context.user_data["awaiting_login_code"] = True

                    async def _code_callback():
                        _future = loop.create_future()
                        context.user_data["login_pending_future"] = _future
                        context.user_data["login_pending_type"] = "code"
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text="Please enter the login code you received on your Telegram app:",
                        )
                        _code = await _future
                        try:
                            _trans = str.maketrans({
                                "\u0660": "0", "\u0661": "1", "\u0662": "2", "\u0663": "3", "\u0664": "4",
                                "\u0665": "5", "\u0666": "6", "\u0667": "7", "\u0668": "8", "\u0669": "9",
                                "\u06F0": "0", "\u06F1": "1", "\u06F2": "2", "\u06F3": "3", "\u06F4": "4",
                                "\u06F5": "5", "\u06F6": "6", "\u06F7": "7", "\u06F8": "8", "\u06F9": "9",
                            })
                            _code = (_code or "").translate(_trans)
                            _code = "".join(c for c in _code if c.isdigit())
                        except Exception:
                            pass
                        return _code

                    async def _password_callback():
                        _pw_future = loop.create_future()
                        context.user_data["login_pending_future"] = _pw_future
                        context.user_data["login_pending_type"] = "password"
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text="Two-step verification is enabled. Please enter your account password:",
                        )
                        return await _pw_future

                    for _attempt in range(2):
                        try:
                            logger.info("Login via client.start() for %s (attempt %d/2)", phone, _attempt + 1)
                            await client.start(
                                phone=phone,
                                code_callback=_code_callback,
                            )
                            logger.info("Login successful for %s via client.start()", phone)
                            break
                        except PhoneCodeExpiredError:
                            if _attempt == 1:
                                raise
                            logger.warning("Code expired for %s; waiting 5s then retrying", phone)
                            await asyncio.sleep(5)
                            continue
                        except SessionPasswordNeededError:
                            _password = await _password_callback()
                            await client.sign_in(password=_password)
                            break

                    if await client.is_user_authorized():
                        # Telethon already saved the session to its native .session file
                        # at session_path. has_usable_telethon_session() and
                        # build_telethon_client() will find it automatically.
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=f"\u2705 Telethon userbot login successful.\nSession saved to: {session_path}",
                        )
                        logger.info("Telethon login successful for %s", phone)
                    else:
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text="Login completed but session is not authorized. Please run /login again.",
                        )

                except Exception as start_exc:
                    logger.exception("_do_start() failed: %s", start_exc)
                    try:
                        if isinstance(start_exc, FloodWaitError):
                            wait = getattr(start_exc, "seconds", None) or getattr(start_exc, "timeout", None) or 60
                            await context.bot.send_message(
                                chat_id=update.effective_chat.id,
                                text=f"Too many attempts. Please wait {int(wait)} seconds before retrying.",
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=update.effective_chat.id,
                                text=f"Login failed: {start_exc.__class__.__name__}.\nPlease run /login again.",
                            )
                    except Exception:
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text="Login failed unexpectedly. Please run /login again.",
                        )
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    try:
                        fut = context.user_data.get("login_pending_future")
                        if fut and not fut.done():
                            fut.cancel()
                    except Exception:
                        pass
                    _clear_login_flow(user_id, context)

            context.user_data["login_phone"] = phone
            context.user_data["login_client"] = client
            context.user_data["login_session_path"] = session_path
            # Set guard flag to prevent fall-through cleanup from firing
            # while _do_start() is setting up the pending future
            context.user_data["login_flow_started"] = True
            login_task = asyncio.create_task(_do_start())
            context.user_data["login_start_task"] = login_task
            logger.info("Login background task started for %s", phone)
            return

        except Exception as exc:
            logger.exception("/login phone step failed: %s", exc)
            await update.message.reply_text(
                "Failed to start Telethon login. Check API_ID/API_HASH and the phone number."
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            _clear_login_flow(user_id, context)
            return

    # Check if there's a pending future to resolve (code or password from client.start())
    pending_future = context.user_data.get("login_pending_future")
    pending_type = context.user_data.get("login_pending_type")
    if pending_future is not None and not pending_future.done():
        client = context.user_data.get("login_client")
        phone = context.user_data.get("login_phone")
        if client is None or not phone:
            await update.message.reply_text(
                "Session state lost. Please run /login again to start a fresh login."
            )
            _clear_login_flow(user_id, context)
            return

        try:
            _input = update.message.text.strip()
            if pending_type == "code":
                trans_digits = str.maketrans({
                    "\u0660": "0", "\u0661": "1", "\u0662": "2", "\u0663": "3", "\u0664": "4",
                    "\u0665": "5", "\u0666": "6", "\u0667": "7", "\u0668": "8", "\u0669": "9",
                    "\u06F0": "0", "\u06F1": "1", "\u06F2": "2", "\u06F3": "3", "\u06F4": "4",
                    "\u06F5": "5", "\u06F6": "6", "\u06F7": "7", "\u06F8": "8", "\u06F9": "9",
                })
                norm_code = (_input or "").translate(trans_digits)
                norm_code = "".join([c for c in norm_code if c.isdigit()])
                resolved_value = norm_code
            else:
                resolved_value = _input
            pending_future.set_result(resolved_value)
            logger.info("Telethon login %s resolved for user=%s", pending_type or "input", user_id)
            return
        except Exception as exc:
            logger.exception("Failed to resolve pending future for user=%s: %s", user_id, exc)
            try:
                if not pending_future.done():
                    pending_future.set_exception(exc)
            except Exception:
                pass
            await update.message.reply_text(
                "Failed to send your input to the login process. Please run /login again."
            )
            return

    # Only clear if no active login flow was started
    if not context.user_data.get("login_flow_started"):
        _clear_login_flow(user_id, context)


application.add_handler(CommandHandler("login", cmd_login))
application.add_handler(CommandHandler("loginstatus", cmd_loginstatus))
application.add_handler(CommandHandler("logout", cmd_logout))


async def cmd_clearflood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear FloodWait block for current login flow and optionally resend code.

    Usage: /clearflood [resend]
    - Without args: clears flood wait for your current login attempt.
    - With 'resend': attempts a best-effort resend of the login code.
    """
    user = update.effective_user
    uid = getattr(user, "id", None)
    if not config.is_owner(uid):
        await update.effective_message.reply_text("\u26d4 Only the bot owner can run this command.")
        return

    # Clear flood wait state
    try:
        cleared = False
        if context.user_data.pop("login_flood_wait_until", None) is not None:
            cleared = True
    except Exception:
        cleared = False

    # Disconnect any lingering Telethon client
    try:
        client = context.user_data.get("login_client")
        if client is not None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(client.disconnect())
            except RuntimeError:
                pass
    except Exception:
        pass

    args = context.args if hasattr(context, "args") else []
    want_resend = len(args) > 0 and args[0].lower() in ("resend", "r")

    if not want_resend:
        await update.effective_message.reply_text(
            "\u2705 FloodWait state cleared." if cleared else "No FloodWait state found."
        )
        return

    # Attempt resend using stored Telethon client/phone
    client = context.user_data.get("login_client")
    phone = context.user_data.get("login_phone")
    if client is None or not phone:
        await update.effective_message.reply_text(
            "No active login session found to resend for. Start /login first."
        )
        return

    try:
        sent = await client.send_code_request(phone)
    except Exception as e:
        try:
            from telethon.errors import FloodWaitError
            if isinstance(e, FloodWaitError):
                wait = getattr(e, "seconds", None) or getattr(e, "timeout", None) or 60
                context.user_data["login_flood_wait_until"] = time.time() + int(wait)
                await update.effective_message.reply_text(
                    f"Too many requests; please wait {int(wait)} seconds before retrying."
                )
                logger.warning("FloodWait during clearflood resend for %s: wait=%s", phone, wait)
                return
        except Exception:
            pass
        logger.exception("Resend via /clearflood failed")
        await update.effective_message.reply_text(
            "Failed to resend login code. See server logs for details."
        )
        return

    # Store new code context
    try:
        context.user_data["login_code_sent_at"] = time.time()
        context.user_data["login_code_sent_repr"] = repr(sent)
        new_hash = getattr(sent, "phone_code_hash", None)
        if new_hash:
            context.user_data["login_code_hash"] = new_hash
    except Exception:
        pass

    await update.effective_message.reply_text(
        "Cleared FloodWait and resent login code (best-effort). Check your Telegram app for the code."
    )


application.add_handler(CommandHandler("clearflood", cmd_clearflood))

# Login text handler - captures phone/code/password during login flow
_login_text_filter = filters.TEXT & ~filters.COMMAND & AwaitingLoginFilter()
application.add_handler(MessageHandler(_login_text_filter, _process_login_text))

app = FastAPI()

# Background tasks references for graceful shutdown
_keep_alive_task = None
_worker_task = None
_worker_proc = None       # subprocess.Popen handle for the RQ worker
_cleanup_task = None
_longpoll_task = None
_shutdown_event = asyncio.Event()


@app.on_event("startup")
async def on_startup() -> None:
    global _keep_alive_task, _worker_task, _worker_proc, _cleanup_task, _longpoll_task
    # Ensure storage directories exist
    for d in (config.STORAGE_PATH, config.INPUT_PATH, config.OUTPUT_PATH, config.TEMP_PATH, config.THUMBNAIL_PATH):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass
    # Initialize application so handlers, bot, and context are ready
    await application.initialize()

    # ── Log cached user sessions on startup ──
    try:
        from utils.cache import get_cache
        cache = await get_cache()
        if cache:
            client = await cache._get_client()
            if client:
                keys = await client.keys("cache:user:*")
                logger.info("Startup: %d user sessions loaded from Redis cache", len(keys) if keys else 0)
    except Exception:
        logger.debug("Could not enumerate cached user sessions on startup")

    # ── Background worker subprocess (with auto-restart supervision) ──
    _worker_proc = None
    if os.getenv("RUN_WORKER_IN_PROC", "false").lower() in ("1", "true", "yes"):
        async def _worker_supervisor():
            """Monitor the RQ worker subprocess and restart it if it crashes."""
            import sys as _sys
            import subprocess as _sub
            worker_path = os.path.join(os.getcwd(), "worker.py")
            restart_delay = 5

            while not _shutdown_event.is_set():
                try:
                    global _worker_proc
                    _worker_proc = _sub.Popen(
                        [_sys.executable, worker_path],
                        env=os.environ.copy(),
                        close_fds=True,
                    )
                    logger.info("Worker subprocess started pid=%s", _worker_proc.pid)
                    loop = asyncio.get_running_loop()
                    rc = await loop.run_in_executor(None, _worker_proc.wait)
                    logger.warning(
                        "Worker subprocess exited (rc=%s), restarting in %ds...",
                        rc, restart_delay,
                    )
                except Exception as e:
                    logger.exception("Worker supervisor error: %s", e)

                if _shutdown_event.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        _shutdown_event.wait(), timeout=restart_delay,
                    )
                    break
                except asyncio.TimeoutError:
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
                        allowed_updates=["message", "callback_query", "edited_message"],
                    )
                    if updates:
                        for u in updates:
                            if getattr(u, "update_id", None) is not None:
                                offset = int(u.update_id) + 1
                            try:
                                await application.process_update(u)
                            except Exception:
                                logger.exception("Failed to dispatch polled update")
                except asyncio.TimeoutError:
                    # Normal timeout — no updates, keep polling
                    pass
                except Exception as e:
                    logger.warning("Long-poller error: %s", e)
                    try:
                        await asyncio.wait_for(
                            _shutdown_event.wait(), timeout=poll_interval,
                        )
                        break
                    except asyncio.TimeoutError:
                        continue

            logger.info("Long-poller stopped")

        _longpoll_task = asyncio.create_task(_longpoll_loop())

    elif WEBHOOK_URL:
        webhook_path = f"/webhook/{BOT_TOKEN}"
        full_url = WEBHOOK_URL.rstrip("/") + webhook_path
        await application.bot.set_webhook(full_url)
        try:
            masked_url = full_url.rsplit('/', 1)[0] + '/<REDACTED_BOT_TOKEN>'
        except Exception:
            masked_url = '<webhook_url_redacted>'
        logger.info("Webhook set to %s", masked_url)
    else:
        logger.warning("No WEBHOOK_URL provided and USE_POLLING is false; bot won't receive updates.")

    # ── Webhook health monitor (optional) ──
    if WEBHOOK_URL:
        try:
            from utils.webhook_monitor import WebhookRecoveryManager
            _webhook_recovery = WebhookRecoveryManager(application, WEBHOOK_URL)
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
        logger.info("Cleanup manager started (interval=%ds)", _cm.cleanup_interval)
    except Exception as e:
        logger.warning("Cleanup manager not started: %s", e)

    # ── Keep-alive heartbeat to prevent Render free-tier spin-down ──
    try:
        _ka_disabled = os.getenv("KEEP_ALIVE_DISABLED", "").lower() in ("1", "true", "yes")
        if not _ka_disabled:
            _ka_url = (
                os.getenv("KEEP_ALIVE_URL")
                or os.getenv("RENDER_EXTERNAL_URL")
                or ""
            )
            if not _ka_url and WEBHOOK_URL:
                try:
                    parsed_ka = urlparse(WEBHOOK_URL)
                    if parsed_ka.netloc:
                        _ka_url = f"{parsed_ka.scheme}://{parsed_ka.netloc}"
                except Exception:
                    pass

            if _ka_url:
                _ka_url = _ka_url.rstrip("/")
                _health_url = f"{_ka_url}/health"
                _ka_interval = max(60, min(840, int(os.getenv("KEEP_ALIVE_INTERVAL", "600"))))

                async def _keep_alive_loop():
                    logger.info("Keep-alive heartbeat started: pinging %s every %ds", _health_url, _ka_interval)
                    try:
                        async with aiohttp.ClientSession() as _session:
                            while True:
                                try:
                                    async with _session.get(_health_url, timeout=aiohttp.ClientTimeout(total=10)) as _resp:
                                        logger.debug("Keep-alive ping: %s", _resp.status)
                                except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as _e:
                                    logger.debug("Keep-alive ping failed (harmless): %s", _e)
                                try:
                                    await asyncio.wait_for(_shutdown_event.wait(), timeout=_ka_interval)
                                    break
                                except asyncio.TimeoutError:
                                    continue
                                except asyncio.CancelledError:
                                    break
                    except asyncio.CancelledError:
                        pass
                    logger.info("Keep-alive heartbeat stopped")

                _keep_alive_task = asyncio.create_task(_keep_alive_loop())
                logger.info("Keep-alive heartbeat scheduled")
            else:
                logger.info("Keep-alive heartbeat disabled: no public URL available (set KEEP_ALIVE_URL, RENDER_EXTERNAL_URL, or WEBHOOK_URL)")
    except Exception as _ka_err:
        logger.warning("Failed to start keep-alive heartbeat: %s", _ka_err)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _keep_alive_task, _worker_task, _worker_proc, _cleanup_task, _longpoll_task
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
            except Exception:
                pass

        # Stop cleanup manager
        try:
            from utils.cleanup import cleanup_manager as _cm
            _cm.stop()
        except Exception:
            pass

        # Close MongoDB connections
        try:
            from utils.db import close_db
            await close_db()
        except Exception:
            pass

        # CRITICAL: Do NOT delete the webhook on shutdown.
        # On Render free tier, the webhook must persist so Telegram can
        # wake the service back up on the next incoming message.
        if USE_POLLING:
            await application.stop()
        await application.shutdown()
    except Exception:
        logger.exception("Error during shutdown")


@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request, background_tasks: BackgroundTasks):
    if token != BOT_TOKEN:
        logger.warning("Received webhook with invalid token")
        return {"ok": False}

    data = await request.json()
    update = Update.de_json(data, application.bot)
    # Schedule processing in the running event loop to avoid threadpool issues
    asyncio.create_task(application.process_update(update))
    return {"ok": True}


URL_RE = re.compile(r"https?://[^\s'\)\]\>]+", re.IGNORECASE)


async def download_url_to_file(url: str, dest_path: str) -> None:
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Download failed: {resp.status}")
            # Stream to file
            async with aiofiles.open(dest_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    await f.write(chunk)


async def handle_text_with_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _track_user_session(update, "url")
    msg = update.effective_message
    if not msg or not msg.text:
        return
    urls = URL_RE.findall(msg.text)
    if not urls:
        return

    for url in urls:
        url = url.rstrip('.,;!?)]')
        # quick check by extension
        if url.lower().endswith('.pdf'):
            # If redis available, enqueue background job to download and process URL
            chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
            parsed = urlparse(url)
            base = os.path.basename(parsed.path) or "download.pdf"
            if not base.lower().endswith('.pdf'):
                base = base + ".pdf"
            if config.REDIS_URL:
                ok = enqueue_job('process_url_job', chat_id, url, base)
                if ok:
                    await msg.reply_text("Queued your PDF URL for background processing; I'll send the result when ready.")
                    return
                # fall back to inline processing on enqueue failure

            tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
            try:
                file_path = os.path.join(tmpdir, base)
                await download_url_to_file(url, file_path)
                thumb_path = os.path.join(tmpdir, "thumb.jpg")
                create_thumbnail_from_pdf(file_path, thumb_path)
                with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                    input_doc = InputFile(f_doc, filename=base)
                    chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
                    await context.bot.send_document(chat_id=chat_id, document=input_doc, thumbnail=f_thumb, caption=f"Generated thumbnail from URL")
            except Exception as e:
                error_info = await handle_bot_error(e, "URL PDF Processing", update=update)
                try:
                    await msg.reply_text(error_info["user_message"])
                except Exception:
                    pass
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
            return
        else:
            # HEAD to detect content-type
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url, allow_redirects=True) as resp:
                        ctype = resp.headers.get('Content-Type', '')
                        if 'pdf' in ctype.lower():
                            tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
                            try:
                                parsed = urlparse(url)
                                base = os.path.basename(parsed.path) or "download.pdf"
                                if not base.lower().endswith('.pdf'):
                                    base = base + ".pdf"
                                file_path = os.path.join(tmpdir, base)
                                await download_url_to_file(url, file_path)
                                thumb_path = os.path.join(tmpdir, "thumb.jpg")
                                create_thumbnail_from_pdf(file_path, thumb_path)
                                with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                                    input_doc = InputFile(f_doc, filename=base)
                                    chat_id = msg.chat.id if getattr(msg, 'chat', None) else msg.chat_id
                                    await context.bot.send_document(chat_id=chat_id, document=input_doc, thumbnail=f_thumb, caption=f"Generated thumbnail from URL")
                            except Exception as e:
                                error_info = await handle_bot_error(e, "URL PDF Processing (HEAD detect)", update=update)
                                try:
                                    await msg.reply_text(error_info["user_message"])
                                except Exception:
                                    pass
                            finally:
                                shutil.rmtree(tmpdir, ignore_errors=True)
                            return
            except Exception:
                continue


application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_with_url))



def _verify_admin_header(admin_token: str) -> bool:
    if not config.ADMIN_SECRET:
        return False
    return admin_token == config.ADMIN_SECRET


@app.get("/status")
async def status(admin_token: str | None = Header(default=None)) -> str:
    # Return a minimal, non-sensitive status string
    return "active"


@app.get("/commands")
async def get_commands(admin_token: str | None = Header(default=None)) -> dict:
    if admin_token and not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    try:
        cmds = await application.bot.get_my_commands()
        return {"ok": True, "commands": [c.to_dict() for c in cmds]}
    except Exception as e:
        logger.exception("Failed to fetch commands")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/set_webhook")
async def set_webhook(request: Request, admin_token: str | None = Header(default=None), owner_id: str | None = Header(default=None)) -> dict:
    # OWNER_ID header check: only the bot owner can call this endpoint
    if OWNER_ID and (not owner_id or int(owner_id) != OWNER_ID):
        raise HTTPException(status_code=403, detail="Only the bot owner can set the webhook")
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    body = await request.json()
    url = body.get("url") if isinstance(body, dict) else None
    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url' in JSON body")
    webhook_path = f"/webhook/{BOT_TOKEN}"
    full_url = url.rstrip("/") + webhook_path
    try:
        await application.bot.set_webhook(full_url)
        return {"ok": True, "webhook": full_url}
    except Exception as e:
        logger.exception("Failed to set webhook")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete_webhook")
async def delete_webhook(request: Request, admin_token: str | None = Header(default=None), owner_id: str | None = Header(default=None)) -> dict:
    if OWNER_ID and (not owner_id or int(owner_id) != OWNER_ID):
        raise HTTPException(status_code=403, detail="Only the bot owner can delete the webhook")
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    try:
        await application.bot.delete_webhook()
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to delete webhook")
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/admin/recache_thumbs')
async def admin_recache_thumbs(request: Request, admin_token: str | None = Header(default=None)) -> dict:
    """Thumbnail recache endpoint — disabled. Thumbnail caching was removed."""
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail='Invalid admin token')
    return {'ok': False, 'error': 'Thumbnail caching is disabled. This endpoint is no-op.'}


@app.post('/admin/purge_s3')
async def admin_purge_s3(request: Request, admin_token: str | None = Header(default=None)) -> dict:
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail='Invalid admin token')
    body = await request.json()
    ttl = int(body.get('ttl_seconds', 0)) if isinstance(body, dict) else 0
    prefix = body.get('prefix', 'pdf-bot/') if isinstance(body, dict) else 'pdf-bot/'
    if ttl <= 0:
        raise HTTPException(status_code=400, detail='ttl_seconds must be > 0')

    try:
        from storage import purge_objects_older_than
    except Exception:
        raise HTTPException(status_code=500, detail='storage.purge_objects_older_than is unavailable')

    # run in executor to avoid blocking
    try:
        loop = asyncio.get_running_loop()
        deleted = await loop.run_in_executor(None, purge_objects_older_than, ttl, prefix)
        return {'ok': True, 'deleted': deleted}
    except Exception as e:
        logger.exception('Failed purging S3 objects')
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/set_commands")
async def set_commands(request: Request, admin_token: str | None = Header(default=None), owner_id: str | None = Header(default=None)) -> dict:
    # OWNER_ID header check: only the bot owner can call this endpoint
    if OWNER_ID and (not owner_id or int(owner_id) != OWNER_ID):
        raise HTTPException(status_code=403, detail="Only the bot owner can set commands")
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail="Invalid admin token")
    body = await request.json()
    items = body.get("commands") if isinstance(body, dict) else None
    try:
        if items and isinstance(items, list):
            cmds = [BotCommand(it.get("command"), it.get("description", "")) for it in items]
            await application.bot.set_my_commands(cmds)
        else:
            # fallback to default commands
            await application.bot.set_my_commands([
                BotCommand("start", "Start interaction with the bot"),
                BotCommand("help", "Show help and available commands"),
                BotCommand("status", "Get bot status"),
            ])
        return {"ok": True}
    except Exception as e:
        logger.exception("Failed to set commands")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health() -> str:
    return "active"


if __name__ == '__main__':
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host=host, port=port, log_level="info")