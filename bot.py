import os
import logging
import tempfile
import shutil
from typing import Dict, Any

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
from io import BytesIO
import config
from config import OWNER_ID
from utils.progress_tracker import progress_tracker, send_progress_update, _format_size

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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level)
# keep httpx at least INFO to avoid leaking full request URLs in DEBUG logs
import logging as _logging
logging.getLogger("httpx").setLevel(max(numeric_level, _logging.INFO))
logging.getLogger("rq").setLevel(numeric_level)
logging.getLogger("telegram").setLevel(numeric_level)
logger = logging.getLogger(__name__)

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

    # Thumbnail caching disabled

    # If Telegram reports a file_size on the Document, check it against the configured
    # upload limit before attempting to enqueue or download. Telegram's Bot API will
    # reject downloads for files larger than the bot's allowed size (returns 400 "file is too big").
    file_size = getattr(doc, 'file_size', None)
    upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
    if file_size and upload_limit and file_size > upload_limit:
        # Inform the user with actionable options
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
        ok = enqueue_job('process_document_job', chat_id, doc.file_id, filename, mime, getattr(doc, 'file_unique_id', None))
        if ok:
            await msg.reply_text("Queued your file for background processing; I'll send the result when ready.")
            return
        # fall through to inline processing on enqueue failure

    tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
    progress_msg_id = None
    task = None
    try:
        file_path = os.path.join(tmpdir, filename)
        file = await context.bot.get_file(doc.file_id)

        # Set up progress tracking for download
        if file_size and file_size > 1024 * 1024:  # only show progress for files >1MB
            import uuid as _uuid
            task_id = _uuid.uuid4().hex[:12]
            task = progress_tracker.create_task(task_id, user_id or 0, filename, file_size)
            progress_msg_id = await send_progress_update(msg.chat.id, context.bot, task)
            task.start()
            task.status = "downloading"

            # Download with progress callback
            downloaded = 0
            async def _progress_cb(current, total):
                nonlocal downloaded
                downloaded = current
                task.update_progress(current)
                # Throttle updates to max once per 2 seconds
                if task._last_update is None or time.time() - task._last_update >= 2.0:
                    task._last_update = time.time()
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
            file._file_size = file_size  # hint for progress
            await file.download_to_drive(custom_path=file_path, read_timeout=300, write_timeout=300)
            task.update_progress(file_size or os.path.getsize(file_path))
            if progress_msg_id:
                await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
        else:
            await file.download_to_drive(custom_path=file_path)

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
        logger.exception("Failed to process document")
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
                except Exception:
                    pass
        try:
            await msg.reply_text(f"Error processing file: {e}")
        except Exception:
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    # Thumbnail caching disabled

    if config.REDIS_URL:
        ok = enqueue_job('process_document_job', chat_id, photo.file_id, filename, 'image/jpeg', getattr(photo, 'file_unique_id', None))
        if ok:
            await msg.reply_text("Queued your photo for background processing; I'll send the result when ready.")
            return

    tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
    progress_msg_id = None
    task = None
    try:
        file_path = os.path.join(tmpdir, filename)
        file = await context.bot.get_file(photo.file_id)

        # Progress tracking for photos >1MB
        photo_size = getattr(photo, 'file_size', None) or 0
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
        logger.exception("Failed to process photo")
        if task:
            await progress_tracker.fail_task(task.task_id, str(e))
            if progress_msg_id:
                try:
                    await send_progress_update(msg.chat.id, context.bot, task, progress_msg_id)
                except Exception:
                    pass
        try:
            await msg.reply_text(f"Error processing photo: {e}")
        except Exception:
            pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
application.add_handler(MessageHandler(filters.PHOTO, handle_photo))


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Hello! Send me a PDF or image and I'll return a thumbnail (PDF first page as cover)."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "/start - start\n"
        "/help - this help\n"
        "/status - get bot status\n"
        "/setwebhook <url> - (admin) set webhook to URL\n"
        "/delwebhook - (admin) delete webhook\n"
        "/setcommands - (admin) set bot command list\n"
    )
    await update.effective_message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Minimal, non-sensitive status reply
    await update.effective_message.reply_text("active")


async def cmd_setwebhook(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = getattr(user, "id", None)
    if not chat_id or not user_id:
        await update.effective_message.reply_text("Unable to start batch here.")
        return
    start_forward_batch(chat_id, user_id)
    await update.effective_message.reply_text("Started forward-collection batch. Forward messages now; when finished run /endbatch to process them.")


async def cmd_endbatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

app = FastAPI()

# Background tasks references for graceful shutdown
_keep_alive_task = None
_worker_task = None
_shutdown_event = asyncio.Event()


@app.on_event("startup")
async def on_startup() -> None:
    global _keep_alive_task, _worker_task
    # Ensure storage directories exist
    for d in (config.STORAGE_PATH, config.INPUT_PATH, config.OUTPUT_PATH, config.TEMP_PATH, config.THUMBNAIL_PATH):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass
    # Initialize application so handlers, bot, and context are ready
    await application.initialize()

    # Optionally start an external worker subprocess to avoid running a separate
    # paid worker service while allowing the worker to install signal handlers.
    # Enable this by setting the environment variable RUN_WORKER_IN_PROC=true
    try:
        if os.getenv("RUN_WORKER_IN_PROC", "false").lower() in ("1", "true", "yes"):
            try:
                import sys
                import subprocess
                worker_path = os.path.join(os.getcwd(), "worker.py")
                # Start worker as a separate process so it can register signal handlers
                proc = subprocess.Popen([sys.executable, worker_path], env=os.environ.copy(), close_fds=True)
                logger.info("Started worker subprocess pid=%s", proc.pid)
            except Exception:
                logger.exception("Failed to start worker subprocess")
    except Exception:
        logger.exception("Error while attempting to start worker subprocess")

    if USE_POLLING:
        # start polling in background for local/dev
        await application.start()
        logger.info("Started polling mode")
    elif WEBHOOK_URL:
        webhook_path = f"/webhook/{BOT_TOKEN}"
        full_url = WEBHOOK_URL.rstrip("/") + webhook_path
        await application.bot.set_webhook(full_url)
        # redact the bot token when logging the webhook URL
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

    # ── Keep-alive heartbeat to prevent Render free-tier spin-down ──
    # Periodically pings /health so the service stays awake.
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
    global _keep_alive_task, _worker_task
    _shutdown_event.set()
    try:
        if _keep_alive_task and not _keep_alive_task.done():
            _keep_alive_task.cancel()
            try:
                await _keep_alive_task
            except asyncio.CancelledError:
                pass
        if _worker_task and not _worker_task.done():
            _worker_task.cancel()
            try:
                await _worker_task
            except asyncio.CancelledError:
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
                    await context.bot.send_document(chat_id=chat_id, document=input_doc, thumb=f_thumb, caption=f"Generated thumbnail from URL")
            except Exception as e:
                logger.exception("Failed to process PDF URL")
                try:
                    await msg.reply_text(f"Error processing URL: {e}")
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
                                    await context.bot.send_document(chat_id=chat_id, document=input_doc, thumb=f_thumb, caption=f"Generated thumbnail from URL")
                            except Exception as e:
                                logger.exception("Failed to process PDF URL")
                                try:
                                    await msg.reply_text(f"Error processing URL: {e}")
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
    if not admin_token or not _verify_admin_header(admin_token):
        raise HTTPException(status_code=403, detail='Invalid admin token')
    body = await request.json()
    limit = body.get('limit') if isinstance(body, dict) else None
    dry_run = bool(body.get('dry_run')) if isinstance(body, dict) else False
    notify_chat = body.get('notify_chat') if isinstance(body, dict) else None

    # try to enqueue via RQ if possible
    try:
        ok = enqueue_job('recache_thumbs_job', notify_chat, limit, dry_run)
        if ok:
            return {'ok': True, 'queued': True}
    except Exception:
        logger.exception('Failed to enqueue recache job')

    # fallback: run inline in background executor
    try:
        import tasks
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, tasks.recache_thumbs_job, notify_chat, limit, dry_run)
        return {'ok': True, 'queued': False, 'result': res}
    except Exception as e:
        logger.exception('Failed running recache job inline')
        raise HTTPException(status_code=500, detail=str(e))


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