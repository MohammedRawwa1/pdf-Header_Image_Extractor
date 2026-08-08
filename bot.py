import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import sys
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
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
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
    extract_pdf_embedded_thumbnail,
    extract_pdf_metadata,
    infer_extension,
    is_supported_format,
    is_valid_pdf,
    thumbnail_is_usable,
)
from utils.bigfile_pipeline import BigFilePipeline  # noqa: E402
from utils.cache_cleanup import run_cache_clear  # noqa: E402
from utils.ebook_converter import (  # noqa: E402
    calibre_available,
    conversion_targets_for,
    is_book_format,
)
from utils.error_handler import (  # noqa: E402
    get_error_handler,
    handle_bot_error,
)
from utils.markdown_utils import escape_markdown, safe_code_span  # noqa: E402
from utils.ocr import (  # noqa: E402
    is_ocr_source,
    ocr_enabled,
    ocr_pdf_available,
)
from utils.processed_cache import (  # noqa: E402
    _get_pdf_checks,
    _store_fuid_binding,
    _store_pdf_checks,
    get_processed_by_file_unique_id,
    get_processed_op,
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
from utils.redis_client import (  # noqa: E402
    get_sync_redis,
    get_sync_redis_raw,
)
from utils.session_healthcheck import (  # noqa: E402
    get_session_healthchecker,
    source_label,
    start_session_healthcheck,
    stop_session_healthcheck,
)
from utils.tg_http import (  # noqa: E402
    COMPRESS_PDF_ACTION,
    OCR_ACTION,
    _attach_pending_buttons,
    _tg_forward_message,
    _tg_send_document,
    _tg_send_document_by_id,
    _tg_send_pending_prompt,
)
from utils.url_validation import _validate_url_safe  # noqa: E402
from utils.user_settings import (  # noqa: E402
    get_user_setting,
    set_user_setting,
)
from utils.userbot_downloader import _get_bot_user_id  # noqa: E402
from utils.userbot_uploader import (  # noqa: E402
    send_file_via_userbot_with_fallback,
)

# ── Logging configuration (must be before any logger usage) ──
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level, stream=sys.stdout)
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

    def _cb(current: int, total: int, *args):
        try:
            asyncio.run_coroutine_threadsafe(
                progress_tracker.update_task_progress(task_id, current),
                loop,
            )
        except Exception:  # nosec B110
            pass

    return _cb


def _register_progress_edit_cb(
    bot,
    chat_id: int,
    task_id: str,
    message_id: int | None,
) -> None:
    """Register a callback that live-edits the task's progress message.

    ``update_task_progress`` fires on every download/upload chunk, but by
    itself only updates the in-memory task + Redis — the Telegram message
    is only edited through this callback.  Edits are throttled (>=2s apart
    or >=2% progress jump) so the shared rate limiter is not saturated;
    final states always push through.  The callback removes itself on the
    final state so the tracker does not leak entries.
    """
    if not task_id or not message_id:
        return
    _state = {"last_edit": 0.0, "last_pct": -1.0}

    async def _edit(task) -> None:
        now = time.time()
        pct = task.progress_percentage
        is_final = task.status in ("completed", "failed", "cancelled")
        if is_final:
            progress_tracker.unregister_callback(task_id)
        elif pct - _state["last_pct"] < 2 and now - _state["last_edit"] < 2.0:
            return
        _state["last_edit"] = now
        _state["last_pct"] = pct
        try:
            await send_progress_update(chat_id, bot, task, message_id)
        except Exception:  # nosec B110
            pass

    progress_tracker.register_callback(task_id, _edit)


async def _begin_upload_phase(
    bot,
    chat_id: int,
    task,
    loop: asyncio.AbstractEventLoop,
    progress_msg_id: int | None = None,
):
    """Switch a (possibly merged download->upload) tracker to the upload phase.

    Shared by ``_send_with_upload_progress`` (userbot path) and
    ``_send_document_via_bot_api`` (raw Bot API path) so both show identical
    upload-progress behaviour:

    * ``task.start()`` runs FIRST - it resets status to "processing" and
      (re)sets start_time (also clearing end_time / error_message / throttle
      state from any previous lifecycle), so a fresh tracker reads
      "uploading" and a merged download->upload tracker's speed/ETA reflect
      the UPLOAD phase only, with its bar climbing from 0 again.
    * The phase switch is shown IMMEDIATELY (new progress message for a
      fresh tracker, in-place edit for a merged one) before the first
      streamed chunk lands.
    * The progress callback and live-edit callback are wired up.

    Returns ``(progress_msg_id, progress_cb)``.
    """
    task.start()
    task.processed_size = 0
    task.status = "uploading"
    try:
        if progress_msg_id is None:
            progress_msg_id = await send_progress_update(chat_id, bot, task)
        else:
            await send_progress_update(chat_id, bot, task, progress_msg_id)
    except Exception:  # nosec B110 - progress is best-effort
        pass
    _cb = _make_progress_cb(task.task_id, loop)
    _register_progress_edit_cb(bot, chat_id, task.task_id, progress_msg_id)
    return progress_msg_id, _cb


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
    task=None,
    progress_msg_id: int | None = None,
) -> bool:
    """Send a file via userbot with upload progress tracking.

    ``chat_id`` is used for the **progress message** (shown in the DM with the bot).
    ``target_chat_id`` controls where the actual file is uploaded.
    Delivery call sites pass the bot's user ID (``BOT_USER_ID``) so large
    results land in the user's DM with the bot; ``'me'`` sends to the userbot's
    Saved Messages.  Defaults to ``chat_id`` when not provided (backward-compatible).

    Creates a progress task, shows 'uploading' status with a progress bar,
    then calls send_file_via_userbot_with_fallback with a progress callback
    that updates the task in real time. On success, marks the task as completed and
    DELETES the progress message (same auto-removal as the worker flow).
    On failure, marks as failed and re-raises.

    ``task`` and ``progress_msg_id`` may be passed to REUSE an existing
    tracker (e.g. the download tracker from ``_userbot_download_fallback``)
    so the big-file flow shows ONE merged progress message
    (download -> upload) instead of two separate ones.

    Returns True on success, raises on failure.
    """
    # A blank/white (or missing) preview is worse than none — drop it so the
    # userbot delivers without a white cover thumbnail.
    if not thumbnail_is_usable(thumb_path):
        logger.info(
            "_send_with_upload_progress: skipping unusable thumbnail "
            "for %s",
            filename,
        )
        thumb_path = None
    if task is None:
        task_id = uuid.uuid4().hex[:12]
        task = progress_tracker.create_task(
            task_id, user_id or 0, filename, file_size
        )
    # Switch the (possibly merged download->upload) tracker to the upload
    # phase and show it immediately (post or in-place edit) before the
    # userbot's first progress chunk lands.
    progress_msg_id, _cb = await _begin_upload_phase(
        bot, chat_id, task, loop, progress_msg_id
    )

    try:
        _upload_target = (
            target_chat_id if target_chat_id is not None else chat_id
        )
        # Shared helper: retries to the userbot's Saved Messages ('me') when
        # the send to the preferred target fails (e.g. the userbot can't
        # resolve the bot's entity — a known production failure).
        _sent_msg, _used_chat = await send_file_via_userbot_with_fallback(
            chat_id=_upload_target,
            file_path=file_path,
            caption=caption,
            thumb_path=thumb_path,
            progress_callback=_cb,
            user_id=user_id,
        )
        if _sent_msg:
            # The bot cannot edit the userbot's delivered message, so oversized
            # results get a bot-API prompt pointing at the delivered copy —
            # web-process parity with the worker flow.  PDFs: [🗜 Compress PDF]
            # + [🔎 OCR]; raster images: [🔎 OCR].  Best-effort by contract:
            # _tg_send_pending_prompt never raises.
            _name = filename or ""
            _is_pdf = _name.lower().endswith(".pdf")
            _has_ocr = is_ocr_source(_name) and ocr_enabled()
            if _is_pdf or _has_ocr:
                _msg_chat = getattr(_sent_msg, "chat_id", None)
                if _msg_chat is None:
                    _msg_chat = getattr(
                        getattr(_sent_msg, "chat", None), "id", None
                    )
                _src_chat = (
                    "me"
                    if str(_used_chat) == "me"
                    else (_msg_chat if _msg_chat is not None else _used_chat)
                )
                _sent_id = getattr(_sent_msg, "id", None)
                if _sent_id:
                    if _is_pdf:
                        await asyncio.to_thread(
                            _tg_send_pending_prompt,
                            *COMPRESS_PDF_ACTION,
                            chat_id=chat_id,
                            filename=filename,
                            user_id=user_id,
                            file_size=file_size,
                            src_chat_id=_src_chat,
                            src_message_id=_sent_id,
                            extra_action=(
                                (OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2])
                                if ocr_enabled()
                                else None
                            ),
                        )
                    else:
                        await asyncio.to_thread(
                            _tg_send_pending_prompt,
                            *OCR_ACTION,
                            chat_id=chat_id,
                            filename=filename,
                            user_id=user_id,
                            file_size=file_size,
                            src_chat_id=_src_chat,
                            src_message_id=_sent_id,
                        )
            await progress_tracker.complete_task(task.task_id)
            # Auto-remove the transient progress message after delivery.
            if progress_msg_id:
                try:
                    await bot.delete_message(
                        chat_id=chat_id, message_id=progress_msg_id
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


async def _send_document_via_bot_api(
    bot,
    chat_id: int,
    file_path: str,
    filename: str,
    thumb_path: str,
    caption: str,
    task=None,
    progress_msg_id: int | None = None,
    user_id: int | None = None,
) -> None:
    """Send a document via the Bot API with LIVE upload progress.

    PTB's ``send_document`` exposes no upload-progress hook, and its
    ``InputFile`` reads the whole file into memory up front (``load_file`` ->
    ``obj.read()``), so wrapping the file object can only ever report 0% then
    100%.  When a progress tracker exists, this instead streams the multipart
    body via raw Bot API HTTP in a worker thread
    (``utils.tg_http._tg_send_document`` + its ``_ProgressFileReader``) and
    feeds the streamed byte counts into the shared progress tracker, which
    live-edits the existing progress message (throttled, exactly like the
    worker flow).  Retries on 429/5xx are handled inside ``_tg_send_document``.

    When no tracker exists (tiny files that upload in under a second) the plain
    PTB path is kept, so behaviour is unchanged for those.
    """
    # A blank/white (or missing) preview is worse than none — skip it so a
    # white placeholder is never attached to a delivered document.
    if not thumbnail_is_usable(thumb_path):
        thumb_path = None
    _thumb_fh = None
    try:
        if thumb_path:
            _thumb_fh = open(thumb_path, "rb")
    except Exception:  # nosec B110 - thumb is optional
        _thumb_fh = None
    if task is None or progress_msg_id is None:
        try:
            with open(file_path, "rb") as f_doc:
                _sent = await bot.send_document(
                    chat_id=chat_id,
                    document=InputFile(f_doc, filename=filename),
                    thumbnail=_thumb_fh,
                    caption=caption,
                )
        finally:
            if _thumb_fh is not None:
                _thumb_fh.close()
        # Attach the result buttons (🗜 Compress + 🔎 OCR for PDFs, 🔎 OCR for
        # images) — parity with the raw-HTTP path in _tg_send_document.
        if user_id:
            try:
                _doc = getattr(_sent, "document", None)
                _actions: list[tuple[str, str, str]] = []
                if filename.lower().endswith(".pdf"):
                    _actions.append(
                        (
                            COMPRESS_PDF_ACTION[0],
                            COMPRESS_PDF_ACTION[1],
                            COMPRESS_PDF_ACTION[2],
                        )
                    )
                if is_ocr_source(filename) and ocr_enabled():
                    _actions.append(
                        (OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2])
                    )
                if _actions:
                    _attach_pending_buttons(
                        chat_id,
                        getattr(_sent, "message_id", None),
                        getattr(_doc, "file_id", None),
                        getattr(_doc, "file_unique_id", None),
                        filename,
                        user_id,
                        None,
                        tuple(_actions),
                    )
            except Exception:  # nosec B110 - best-effort button
                pass
        return

    # Switch the (possibly merged download->upload) tracker to the upload phase
    # so the SAME progress message climbs from 0% with an "uploading" label.
    _loop = asyncio.get_running_loop()
    progress_msg_id, _cb = await _begin_upload_phase(
        bot, chat_id, task, _loop, progress_msg_id
    )
    try:
        with open(file_path, "rb") as f_doc:
            await asyncio.to_thread(
                _tg_send_document,
                config.BOT_TOKEN,
                chat_id,
                f_doc,
                filename,
                _thumb_fh,
                caption,
                _cb,
                user_id,
                ocr_user_id=user_id,
            )
    finally:
        if _thumb_fh is not None:
            _thumb_fh.close()


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
        _dl_limit_mb = config.BOT_API_DOWNLOAD_LIMIT_BYTES // (1024 * 1024)
        if file_size:
            mb_size = file_size // (1024 * 1024)
            options.append(
                f"- Upload a smaller file (under {_dl_limit_mb}MB). "
                f"Your file is ~{mb_size} MB."
            )
        else:
            options.append(
                f"- Upload a smaller file (under {_dl_limit_mb}MB)."
            )
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
        ("local", task, progress_msg_id)  -> file downloaded to file_path;
            caller should thumbnail + send, REUSING the returned tracker and
            its progress message for the upload phase (merged single message)
        ("pipeline", None, None)          -> handled async by BigFilePipeline,
            caller should return
        (False, None, None)                -> all methods failed, user notified
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
    _register_progress_edit_cb(
        msg.get_bot(), msg.chat.id, task.task_id, progress_msg_id
    )

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
                        relay_msg_id = _tg_forward_message(
                            bot_token,
                            relay_chat_id,
                            chat_id,
                            msg.message_id,
                        )
                        if relay_msg_id:
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
                            raise Exception("HTTP forwardMessage failed")
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
                # Download is done and the pipeline worker takes over with its
                # OWN progress message — remove this tracker's message now so
                # only one live progress message exists at a time.
                if progress_msg_id:
                    try:
                        await msg.get_bot().delete_message(
                            chat_id=msg.chat.id, message_id=progress_msg_id
                        )
                    except Exception:  # nosec B110
                        pass
                queued_msg = await msg.reply_text(
                    f"Large file ({file_size // (1024 * 1024)} MB) queued for processing.\n"
                    f"Job: {_ingest.job_id[:8]}... You'll receive the result when ready.",
                    reply_markup=_queued_cancel_kb(user_id, _ingest.job_id),
                )
                _store_queued_message(
                    _ingest.job_id,
                    chat_id,
                    getattr(queued_msg, "message_id", None),
                )
                return ("pipeline", None, None)
            else:
                logger.warning("BigFilePipeline failed: %s", _ingest.error)
        except Exception as pipe_err:
            logger.warning("BigFilePipeline error: %s", pipe_err)

    if dl_ok:
        actual_size = os.path.getsize(file_path)
        await progress_tracker.update_task_progress(task.task_id, actual_size)
        # MERGED TRACKER: hand the download tracker and its progress message to
        # the caller so the upload phase (``_send_with_upload_progress``) reuses
        # the SAME message — one download -> upload progress message that is
        # auto-deleted once the output is delivered.
        return ("local", task, progress_msg_id)
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
        return (False, None, None)


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
QUEUED_MSG_KEY = "queued_msg:{}"
QUEUED_MSG_TTL = 7 * 24 * 3600


def _store_queued_message(job_id, chat_id, message_id) -> None:
    """Remember a "Queued..." confirmation message so the worker can
    auto-delete it once the job output is delivered (see tasks.py)."""
    if not job_id or not message_id:
        return
    try:
        r = get_sync_redis()
        if r:
            r.setex(
                QUEUED_MSG_KEY.format(job_id),
                QUEUED_MSG_TTL,
                json.dumps(
                    {"chat_id": chat_id, "message_ids": [message_id]}
                ),
            )
    except Exception:  # nosec B110
        pass


def _warm_fuid_binding(file_unique_id: str | None) -> None:
    """Best-effort: refresh the durable ``pfuid:<fuid> -> content_hash`` index.

    bot.py never has the file bytes, so it cannot COMPUTE the content hash
    itself — but the pdfcheck record (written by the worker) already carries
    the ``content_hash`` binding.  Re-writing the durable index from it at
    ENQUEUE time keeps the surface fast-path warm even before the worker
    runs: it heals any transient pfuid write failure and re-arms the 30-day
    TTL whenever the file is re-sent while the pdfcheck binding is alive.
    """
    if not file_unique_id:
        return
    _checks = _get_pdf_checks(file_unique_id)
    if not _checks:
        return
    _ch = _checks.get("content_hash")
    if _ch:
        _store_fuid_binding(file_unique_id, _ch)


def _resend_cached_result(
    chat_id: int,
    file_unique_id: str | None,
    filename: str,
    op: str,
    user_id: int,
    caption: str,
    target: str | None = None,
) -> bool:
    """Re-send a previously delivered result by its cached Bot API file_id.

    Returns True when a cached ``done`` copy existed and was re-sent; False
    when there is nothing cacheable/usable (callers fall back to a fresh
    job).  Re-sends reuse Telegram's stored copy — Bot API deliveries by
    file_id (keeps the thumbnail), userbot deliveries (big files) by
    forwarding the delivered message via the userbot (server-side media
    copy) — so it works even after the user deleted the bot's earlier
    messages.

    Resolution goes through the file's Telegram ``file_unique_id`` (the
    worker bound it to the content hash in the pdfcheck record) — the web
    process never has the file bytes, so the dedup record is keyed by the
    CONTENT hash, never name+size.

    ``target`` (book conversion) selects the per-target op entry
    (``ops:convert:pdf`` etc) — each format keeps its own cached copy, so
    converting the same book to a second format never evicts the first.
    """
    rec = get_processed_by_file_unique_id(file_unique_id)
    if not rec:
        return False
    entry = get_processed_op(rec, op, target)
    if not entry or entry.get("status") != "done":
        return False
    if entry.get("delivery") == "userbot":
        # Big-file results have no Bot API file_id — forward the delivered
        # copy via the userbot (server-side media copy, no re-upload).
        # Try the re-send chat first (correct for groups, where ids match
        # between the bot and the userbot), then fall back to the bot's DM /
        # Saved Messages (the original big-file delivery target).
        if not (entry.get("src_chat_id") and entry.get("src_message_id")):
            return False
        try:
            import asyncio as _asyncio

            from utils.userbot_downloader import _get_bot_user_id
            from utils.userbot_uploader import forward_message_via_userbot

            _targets: list[int | str] = [chat_id]
            _targets.append(_get_bot_user_id() or "me")
            for _t in _targets:
                _ok = _asyncio.run(
                    forward_message_via_userbot(
                        _t,
                        entry["src_chat_id"],
                        int(entry["src_message_id"]),
                        user_id=user_id,
                    )
                )
                if _ok:
                    return True
            return False
        except Exception:
            logger.warning(
                "Failed to re-send cached %s result for %s", op, filename
            )
            return False
    if not entry.get("file_id"):
        return False
    try:
        _res = _tg_send_document_by_id(
            None,
            chat_id,
            entry["file_id"],
            rec.get("filename") or filename,
            caption=caption,
            compress_user_id=user_id,
            ocr_user_id=user_id,
        )
        return bool(_res and _res.get("ok"))
    except Exception:
        logger.warning(
            "Failed to re-send cached %s result for %s", op, filename
        )
        return False


def enqueue_job(
    func_name: str,
    *args,
    job_timeout: int | None = None,
    owner_user_id: int | None = None,
    **kwargs,
):
    """Enqueue a job on the RQ 'default' queue.

    Returns the RQ job id (so /canceljob can cancel it) or None on failure.

    ``job_timeout`` overrides RQ's default 180s job timeout — pass a value
    larger than the job's own long-running phase (e.g. Calibre conversions)
    so the death penalty can't kill it mid-work.  Defaults to RQ's 180s when
    None.

    ``owner_user_id`` (optional) tags the queued job's meta with its owner
    at ENQUEUE time (not job-start time, like ``_attach_job_user_meta``) so
    /canceljob's per-user gate also protects still-queued jobs in shared
    group chats.  It is NOT forwarded to the task — task functions must
    keep receiving ``user_id`` positionally in ``*args``.

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
        job = q.enqueue(func, *args, **kwargs, job_timeout=job_timeout)
        # Tag the job with its owner now so /canceljob's per-user gate works
        # even before the worker starts (queued jobs).  Best-effort: a failed
        # meta write just leaves the chat-scoped fallback in place.
        if owner_user_id is not None:
            try:
                job.meta["user_id"] = owner_user_id
                job.save_meta()
            except Exception:  # nosec B110 - best-effort ownership tag
                logger.debug(
                    "Could not tag job %s with owner %s (chat-scoped fallback stays in place)",
                    getattr(job, "id", "?"),
                    owner_user_id,
                )
        return getattr(job, "id", None)
    except Exception:
        logger.exception("Failed to enqueue job for %s", func_name)
        return None


BOT_TOKEN = config.BOT_TOKEN
if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set")
    raise SystemExit("Missing BOT_TOKEN")

# Numeric user ID of the bot (first segment of BOT_TOKEN).  Used as the
# userbot's delivery target for large results (> Bot API upload cap) so the
# file lands in the user's DM with the bot instead of the userbot's own
# Saved Messages ("me").  None when the token is malformed — callers then
# fall back to "me".
BOT_USER_ID: int | None = _get_bot_user_id()

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
    # Telegram documents may arrive WITHOUT a filename (only ``file_<id>``).
    # Book formats are detected by extension, so derive one from the MIME
    # type before validation/classification — a nameless EPUB must not be
    # rejected just because it lacks the extension it needs to convert.
    filename = infer_extension(filename, mime)

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
            "I work with **PDFs, images** (JPEG, PNG, WEBP, GIF) and **e-books** "
            "(EPUB, MOBI, AZW3, FB2, DOCX, TXT, RTF, HTML, ODT and more).\n"
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

    # ── Media detection → context builder menu ─────────────────────────────
    # Every supported file (PDF, image, e-book) is detected FIRST and an
    # action menu is shown before ANY processing — nothing is downloaded,
    # echoed, or piped until the user taps an option (Thumbnail / Compress /
    # OCR / Convert).  This removes the old book echo-then-convert double pass
    # and the auto-thumbnail pipeline: one file, one download, one result.
    file_size = getattr(doc, "file_size", None)
    if config.REDIS_URL:
        _kind = _classify_media(filename, mime)
        if _kind is None:
            # Validated as supported above; treat an undetectable one as
            # unsupported rather than silently dropping the file.
            try:
                await msg.reply_text(
                    "\u274c Couldn't detect what this file is. Try sending "
                    "a PDF, image, or e-book."
                )
            except Exception:  # nosec B110
                pass
            return

        # ── Cached-result re-send (already processed) ───────────────────
        # The same file (name+size) was already delivered with a thumbnail:
        # re-send the cached copy instead of starting a new job — even if the
        # user deleted the bot's earlier messages (Telegram keeps the file
        # behind the file_id).  Only the PRIMARY thumbnail deliverable is
        # auto-resent here (other operations like OCR stay behind the menu so
        # the user can still pick a different action).  Falls through to the
        # menu when nothing is cached or the cached copy expired.
        if await asyncio.to_thread(
            _resend_cached_result,
            chat_id,
            getattr(doc, "file_unique_id", None),
            filename,
            "thumb",
            user_id,
            "\U0001f5bc\ufe0f Here is your file (cached result — "
            "already processed).",
        ):
            try:
                await msg.reply_text(
                    "\u267b\ufe0f Already processed this file before — "
                    "re-sent the cached result, no new job was started.",
                )
            except Exception:  # nosec B110
                pass
            return
        _token = _store_ctx_record(
            chat_id=chat_id,
            user_id=user_id,
            message_id=msg.message_id,
            file_id=doc.file_id,
            file_unique_id=getattr(doc, "file_unique_id", None),
            filename=filename,
            mime=mime,
            file_size=file_size,
            forward_info=forward_info,
            kind=_kind,
        )
        if not _token:
            try:
                await msg.reply_text(
                    "\u274c Couldn't start a processing session. "
                    "Try again in a moment."
                )
            except Exception:  # nosec B110
                pass
            return
        _kb = _ctx_menu_kb(user_id or 0, _token, filename, _kind)
        _icon = {
            "book": "\U0001f4d6",
            "pdf": "\U0001f4c4",
            "image": "\U0001f5bc\ufe0f",
        }.get(_kind, "\U0001f4dd")
        if not _kb:
            try:
                await msg.reply_text(
                    f"{_icon} `{safe_code_span(filename)}` — no actions are "
                    "available for this file right now (a required feature "
                    "is disabled or not installed).",
                    parse_mode="Markdown",
                )
            except Exception:  # nosec B110
                pass
            return
        try:
            await msg.reply_text(
                f"{_icon} `{safe_code_span(filename)}` — what would you like "
                "to do?\n_Nothing is processed until you tap an option._",
                reply_markup=_kb,
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            logger.exception("Failed to show context menu for %s", filename)
        return
    # ── No Redis: legacy inline pipeline (no persistent menu possible). ──
    # E-books still need the queue (their Convert token lives in Redis), so
    # reject them clearly instead of mis-routing them into thumbnails.
    if is_book_format(filename) and not filename.lower().endswith(".pdf"):
        try:
            await msg.reply_text(
                "\u274c Book conversion needs the job queue (Redis), which "
                "isn't configured on this instance.",
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            pass
        return

    # If Telegram reports a file_size on the Document, check it against the Bot API
    # DOWNLOAD limit (getFile cap, 20MB) before attempting to enqueue or download.
    # Telegram's Bot API rejects downloads above that cap with 400 "file is too big",
    # so such files must route through the userbot / BigFilePipeline instead.
    download_limit = config.BOT_API_DOWNLOAD_LIMIT_BYTES
    use_userbot_download = False
    if file_size and download_limit and file_size > download_limit:
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
                mb_limit = download_limit // (1024 * 1024)
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
            _dl_result, _dl_task, _dl_msg_id = await _userbot_download_fallback(
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
            # MERGED TRACKER: the download helper hands back its tracker and
            # progress message so the upload phase below reuses the SAME
            # message — one download -> upload progress, auto-deleted on
            # delivery (mirrors the worker flow's auto-removal).
            task = _dl_task
            progress_msg_id = _dl_msg_id
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
            # Validator: reuse the PDF's embedded thumbnail when it has one
            # (skips the 2x first-page render entirely).
            if not extract_pdf_embedded_thumbnail(file_path, thumb_path):
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
                target_chat_id=BOT_USER_ID or "me",
                task=task,
                progress_msg_id=progress_msg_id,
            )
            # The upload helper completed the tracker and auto-deleted the
            # merged progress message; nothing left to clean up below.
            task = None
            progress_msg_id = None
        else:
            await _send_document_via_bot_api(
                bot=context.bot,
                chat_id=chat_id,
                file_path=file_path,
                filename=filename,
                thumb_path=thumb_path,
                caption=caption,
                task=task,
                progress_msg_id=progress_msg_id,
                user_id=user_id,
            )
        # Both send helpers raise on failure, so reaching here means the file
        # (with its preview) was delivered — publish has_thumb=True so repeat
        # sends / button taps short-circuit at enqueue instead of re-rendering
        # (PDFs only: the deliberate gray placeholder for other types must
        # never set the flag).
        if (
            lower.endswith(".pdf") or mime == "application/pdf"
        ) and thumbnail_is_usable(thumb_path):
            _store_pdf_checks(
                getattr(doc, "file_unique_id", None), has_thumb=True
            )
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                try:
                    await context.bot.delete_message(
                        chat_id=msg.chat.id, message_id=progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
    except Exception as e:
        # Try userbot fallback if Bot API download failed
        if not _dl_success and _check_userbot_available(user_id):
            logger.info(
                "Bot API download failed, falling back to userbot download for %s",
                filename,
            )
            try:
                _dl_result, _dl_task, _dl_msg_id = (
                    await _userbot_download_fallback(
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
                )
                if _dl_result == "pipeline":
                    # Handed off to the BigFilePipeline; progress message was
                    # already removed by the fallback helper. Clean up the
                    # ORIGINAL Bot-API progress task/message (if any) that the
                    # failed download left behind.
                    if task:
                        await progress_tracker.fail_task(
                            task.task_id, "Superseded by userbot fallback"
                        )
                        if progress_msg_id:
                            try:
                                await context.bot.delete_message(
                                    chat_id=msg.chat.id,
                                    message_id=progress_msg_id,
                                )
                            except Exception:  # nosec B110
                                pass
                    return
                if _dl_result == "local":
                    # Clean up the ORIGINAL Bot-API progress task/message (if
                    # any) before continuing with the merged tracker below.
                    if task:
                        await progress_tracker.fail_task(
                            task.task_id, "Superseded by userbot fallback"
                        )
                        if progress_msg_id:
                            try:
                                await context.bot.delete_message(
                                    chat_id=msg.chat.id,
                                    message_id=progress_msg_id,
                                )
                            except Exception:  # nosec B110
                                pass
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
                            target_chat_id=BOT_USER_ID or "me",
                            task=_dl_task,
                            progress_msg_id=_dl_msg_id,
                        )
                    else:
                        await _send_document_via_bot_api(
                            bot=context.bot,
                            chat_id=chat_id,
                            file_path=file_path,
                            filename=filename,
                            thumb_path=thumb_path,
                            caption="Here is your file (downloaded via userbot) with an auto-generated cover preview.",
                            task=_dl_task,
                            progress_msg_id=_dl_msg_id,
                            user_id=user_id,
                        )
                        # Auto-remove the transient download progress message
                        # now that the output was delivered.
                        if _dl_task:
                            await progress_tracker.complete_task(
                                _dl_task.task_id
                            )
                            if _dl_msg_id:
                                try:
                                    await context.bot.delete_message(
                                        chat_id=msg.chat.id,
                                        message_id=_dl_msg_id,
                                    )
                                except Exception:  # nosec B110
                                    pass
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

    # ── Media detection → context builder menu ─────────────────────────────
    # Photos are detected as images and offered an action menu (Thumbnail /
    # OCR) before anything is processed — nothing is piped until tapped.
    if config.REDIS_URL:
        _token = _store_ctx_record(
            chat_id=chat_id,
            user_id=user_id,
            message_id=msg.message_id,
            file_id=photo.file_id,
            file_unique_id=getattr(photo, "file_unique_id", None),
            filename=filename,
            mime="image/jpeg",
            file_size=photo_size,
            forward_info=photo_forward_info,
            kind="image",
        )
        if not _token:
            try:
                await msg.reply_text(
                    "\u274c Couldn't start a processing session. "
                    "Try again in a moment."
                )
            except Exception:  # nosec B110
                pass
            return
        _kb = _ctx_menu_kb(user_id or 0, _token, filename, "image")
        if not _kb:
            try:
                await msg.reply_text(
                    f"\U0001f5bc\ufe0f `{safe_code_span(filename)}` — no "
                    "actions are available for this image right now.",
                    parse_mode="Markdown",
                )
            except Exception:  # nosec B110
                pass
            return
        try:
            await msg.reply_text(
                f"\U0001f5bc\ufe0f `{safe_code_span(filename)}` — what "
                "would you like to do?\n_Nothing is processed until you tap "
                "an option._",
                reply_markup=_kb,
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            logger.exception("Failed to show context menu for %s", filename)
        return
    # No Redis → legacy inline pipeline below.

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

        download_limit = config.BOT_API_DOWNLOAD_LIMIT_BYTES

        if photo_size > download_limit and _check_userbot_available(user_id):
            # ── Userbot download path for large photos ──
            _dl_result, _dl_task, _dl_msg_id = (
                await _userbot_download_fallback(
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
            )
            if _dl_result == "pipeline":
                return
            if not _dl_result:
                return
            # MERGED TRACKER: reuse the download tracker + progress message for
            # the upload phase below (one message, auto-deleted on delivery).
            task = _dl_task
            progress_msg_id = _dl_msg_id
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
                target_chat_id=BOT_USER_ID or "me",
                task=task,
                progress_msg_id=progress_msg_id,
            )
            # The upload helper completed the tracker and auto-deleted the
            # merged progress message; nothing left to clean up below.
            task = None
            progress_msg_id = None
        else:
            await _send_document_via_bot_api(
                bot=context.bot,
                chat_id=chat_id,
                file_path=file_path,
                filename=os.path.basename(file_path),
                thumb_path=thumb_path,
                caption="Here is your image with an auto-generated thumbnail.",
                task=task,
                progress_msg_id=progress_msg_id,
                user_id=user_id,
            )
        if task:
            await progress_tracker.complete_task(task.task_id)
            if progress_msg_id:
                try:
                    await context.bot.delete_message(
                        chat_id=msg.chat.id, message_id=progress_msg_id
                    )
                except Exception:  # nosec B110
                    pass
    except Exception as e:
        # If Bot API download failed but userbot is available, try fallback
        if not _dl_success and _check_userbot_available(user_id):
            logger.info(
                "Bot API download failed for photo, falling back to userbot"
            )
            try:
                _dl_result, _dl_task, _dl_msg_id = (
                    await _userbot_download_fallback(
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
                )
                if _dl_result == "pipeline":
                    # Handed off to the BigFilePipeline; progress message was
                    # already removed by the fallback helper. Clean up the
                    # ORIGINAL Bot-API progress task/message (if any) that the
                    # failed download left behind.
                    if task:
                        await progress_tracker.fail_task(
                            task.task_id, "Superseded by userbot fallback"
                        )
                        if progress_msg_id:
                            try:
                                await context.bot.delete_message(
                                    chat_id=msg.chat.id,
                                    message_id=progress_msg_id,
                                )
                            except Exception:  # nosec B110
                                pass
                    return
                if _dl_result == "local":
                    # Clean up the ORIGINAL Bot-API progress task/message (if
                    # any) before continuing with the merged tracker below.
                    if task:
                        await progress_tracker.fail_task(
                            task.task_id, "Superseded by userbot fallback"
                        )
                        if progress_msg_id:
                            try:
                                await context.bot.delete_message(
                                    chat_id=msg.chat.id,
                                    message_id=progress_msg_id,
                                )
                            except Exception:  # nosec B110
                                pass
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
                            target_chat_id=BOT_USER_ID or "me",
                            task=_dl_task,
                            progress_msg_id=_dl_msg_id,
                        )
                    else:
                        await _send_document_via_bot_api(
                            bot=context.bot,
                            chat_id=msg.chat.id,
                            file_path=file_path,
                            filename=os.path.basename(file_path),
                            thumb_path=thumb_path,
                            caption="Here is your image (downloaded via userbot) with an auto-generated thumbnail.",
                            task=_dl_task,
                            progress_msg_id=_dl_msg_id,
                            user_id=user_id,
                        )
                        # Auto-remove the transient download progress message
                        # now that the output was delivered.
                        if _dl_task:
                            await progress_tracker.complete_task(
                                _dl_task.task_id
                            )
                            if _dl_msg_id:
                                try:
                                    await context.bot.delete_message(
                                        chat_id=msg.chat.id,
                                        message_id=_dl_msg_id,
                                    )
                                except Exception:  # nosec B110
                                    pass
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
    # Prefer the Telegram username (e.g. @mohammad) over the display name
    # so the welcome matches the name the user is known by on Telegram.
    _eff_user = update.effective_user
    if _eff_user and getattr(_eff_user, "username", None):
        user_name = "@" + _eff_user.username
    else:
        user_name = getattr(_eff_user, "first_name", None) or "there"
    await update.effective_message.reply_text(
        f"🎉 Welcome, {escape_markdown(user_name)}!\n\n"
        "📄 Send a **PDF**, **image**, or **e-book** and I'll show you an "
        "**action menu** — tap what you want and I process only that:\n"
        "• **PDF**: 🖼 **Thumbnail** · 🔎🖼 **OCR & Thumbnail** (all-in-one) · 🗜🖼 **Compress & Thumbnail**\n"
        "• **Images**: 🖼 **Thumbnail** · 🔎 **OCR**\n"
        "• **E-books**: 🔁 **Convert** · 🗜 **Compress PDF** · 🔎 **OCR PDF**\n\n"
        "⚡ **Quick commands:**\n"
        "• /help — all commands\n"
        "• /login — connect **your** Telethon account (large files)\n"
        "• /loginpyro — connect **your** Pyrogram account (large files)\n"
        "• /loginstatus — check **your** session health\n"
        "• /ocr [pdf|txt|picker] — pick your **OCR output default** (skip the picker)\n"
        "• /canceljob <id> — cancel a queued/in-flight job (asks to confirm)\n"
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
        "• /status — bot status, queue depth & your active/queued jobs\n\n"
        "🔐 Your sessions (per-user)\n"
        "• /login [phone] — connect your Telethon account\n"
        "• /loginpyro [phone] — connect your Pyrogram account\n"
        "• /loginstatus — check your session health\n"
        "• /logout — disconnect your Telethon session\n"
        "• /logoutpyro — disconnect your Pyrogram session\n"
        "• /clearflood — reset a stuck login flow\n"
        "• /cancel — cancel an active login flow\n\n"
        "📦 Jobs\n"
        "• /canceljob <id> — cancel a queued/in-flight job (asks to confirm)\n\n"
        "🗂 Batch\n"
        "• /startbatch — start collecting forwarded files\n"
        "• /endbatch — process the collected batch\n"
        "• /cancelbatch — discard the collected batch\n\n"
        "⚙️ Admin / owner — these commands are restricted\n"
        "• /admin add|remove|list <user_id> — manage allowed users "
        "(admin)\n"
        "• /sessionstatus — userbot session health (owner)\n"
        "• /clear_cache — wipe cached results, leftovers & thumbnails "
        "(admin)\n"
        "• /cancelall — cancel ALL queued/running jobs (admin, asks to "
        "confirm)\n"
        "• /setwebhook <url> — set webhook (owner)\n"
        "• /delwebhook — delete webhook (owner)\n"
        "• /setcommands — push this list to Telegram (owner)\n\n"
        "📖 Media → action menu\n"
        "• Send a PDF / image / e-book → I detect it and show an **action "
        "menu**; nothing is processed until you tap an option\n"
        "• **PDF**: 🖼 Thumbnail · 🔎🖼 OCR & Thumbnail (all-in-one) · "
        "🗜🖼 Compress & Thumbnail\n"
        "• **Image**: 🖼 Thumbnail · 🔎 OCR\n"
        "• **E-book**: 🔁 Convert · 🗜 Compress PDF (convert→PDF, then "
        "shrink) · 🔎 OCR PDF (converted to PDF first)\n"
        "• /ocr [pdf|txt|picker] — pin your OCR output so the picker is skipped\n"
        "\n"
        "Send any supported file to get its action menu."
    )
    await update.effective_message.reply_text(text)


def _ago(ts: float | None) -> str:
    """Human-readable age for a unix timestamp, e.g. '2m ago'."""
    if not ts:
        return ""
    s = int(time.time() - ts)
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def _job_user_id(job: object) -> int | None:
    """Attribute an RQ job to the requesting user (job.meta first, then args).

    Covers every enqueue path from bot.py / telethon_ingest:
      process_input_key_job(job_dict), process_document_job(...),
      process_url_job(...), process_document_batch_job(...).
    """
    try:
        meta_uid = (getattr(job, "meta", None) or {}).get("user_id")
        if meta_uid:
            return meta_uid
        args = list(getattr(job, "args", None) or [])
        if not args:
            return None
        if isinstance(args[0], dict):  # process_input_key_job(job dict)
            return args[0].get("user_id")
        if len(args) >= 9 and isinstance(args[8], int):  # process_document_job
            return args[8]
        if len(args) >= 7 and isinstance(args[6], dict):  # forward_info fallback
            return args[6].get("user_id")
        if len(args) >= 4 and isinstance(args[3], int):  # process_url_job
            return args[3]
        if len(args) >= 3 and isinstance(args[2], int):  # batch job
            return args[2]
    except Exception:  # nosec B110 - best-effort
        pass
    return None


def _job_label(job: object) -> str:
    """Best-effort human label for a job: filename or a short description."""
    try:
        args = list(getattr(job, "args", None) or [])
        if not args:
            return getattr(job, "func_name", "job") or "job"
        if isinstance(args[0], dict):  # pipeline job dict
            return (
                args[0].get("original_filename")
                or args[0].get("filename")
                or "pipeline job"
            )
        if len(args) >= 2:
            if isinstance(args[1], list):  # batch job
                return f"batch ({len(args[1])} items)"
            if isinstance(args[1], str):
                if args[1].startswith(("http://", "https://")):  # URL job
                    return str(args[2]) if len(args) >= 3 else "URL job"
                if len(args[1]) > 40:  # Telegram file_id -> document job
                    return str(args[2]) if len(args) >= 3 else "document"
    except Exception:  # nosec B110 - best-effort
        pass
    return getattr(job, "func_name", "job") or "job"


def _job_cancel_id(job: object, fallback: str) -> str:
    """Cancel token for a job: pipeline dicts carry their own ``job_id``.

    RQ jobs use their registry id; BigFilePipeline jobs use the id inside the
    job dict (that is what /canceljob resolves via ``_cancel_pipeline_job``).
    """
    try:
        args = list(getattr(job, "args", None) or [])
        if args and isinstance(args[0], dict):
            return str(args[0].get("job_id") or fallback)
    except Exception:  # nosec B110
        pass
    return fallback


def _cancel_all_kb() -> InlineKeyboardMarkup:
    """The one-tap cancel-all button shown under the /status job list."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\U0001f5d1 Cancel all jobs", callback_data="cancelall"
                )
            ]
        ]
    )


def _cancel_status_reply(
    uid: int | None, header: str
) -> tuple[str, InlineKeyboardMarkup | None]:
    """Build a post-cancel reply: ``header`` + refreshed /status summary.

    Shared by the /cancelall and /canceljob confirm flows so a successful
    cancel shows the updated job list instead of a one-line confirmation.
    The cancel-all button (admin-only) is re-attached when the caller still
    has queued/running jobs; ``None`` is returned otherwise. Callers that
    EDIT a message must substitute an empty keyboard for ``None`` — Telegram
    keeps the existing inline keyboard when ``reply_markup`` is omitted, so
    plain ``None`` would leave stale confirmation buttons behind. New replies
    can pass ``None`` as-is (never send an empty keyboard on sendMessage: it
    serializes to ``{}``, which Telegram rejects for a required field).
    """
    summary, has_jobs = _build_status_summary(uid, config.is_owner(uid))
    text = header + "\n\n" + summary
    kb = (
        _cancel_all_kb()
        if has_jobs and config.is_admin_user(uid)
        else None
    )
    return text, kb

# ── Token-based pending-action state ───────────────────────────────────
# Input context menus (``ctxfile:<token>``) and delivered-file buttons
# (``bookconvert`` / ``bookcompress`` / ``bookocr``) share one model: a
# pending file record keyed by a short token stored with the button callback,
# consumed atomically on the final action so double-taps are inert.


def _peek_pending_record(key: str) -> str | None:
    """Non-consuming peek at a pending button record (raw JSON).

    Used by the OCR button reveal: the record is read so the picker can be
    shown, but is only consumed atomically on the FINAL format tap (so a
    double-tap can never enqueue the same job twice).
    """
    if not key:
        return None
    try:
        r = get_sync_redis()
        if not r:
            return None
        raw = r.get(key)
        if raw is None:
            return None
        return raw if isinstance(raw, str) else raw.decode(errors="replace")
    except Exception:  # nosec B110
        return None


def _consume_pending_record(key: str) -> str | None:
    """Atomically fetch-and-delete a pending button record (raw JSON).

    Uses Redis ``GETDEL`` (>= 6.2) with a Lua fallback, so a double-tap can
    never read the same record twice — without this, two concurrent callbacks
    (a fast double-tap, a webhook redelivery, or multiple bot instances)
    could both GET before either DELETEs and enqueue the same job twice.
    """
    try:
        r = get_sync_redis()
        if not r:
            return None
        try:
            raw = r.getdel(key)
        except Exception:  # nosec B110 - older Redis: Lua fallback
            try:
                raw = r.eval(
                    "local v = redis.call('get', KEYS[1]); "
                    "if v then redis.call('del', KEYS[1]) end; "
                    "return v",
                    1,
                    key,
                )
            except Exception:  # nosec B110
                return None
        if raw is None:
            return None
        return raw if isinstance(raw, str) else raw.decode(errors="replace")
    except Exception:  # nosec B110
        return None



def _book_conv_kb(
    uid: int, token: str, filename: str
) -> InlineKeyboardMarkup | None:
    """Format-picker keyboard (conversion-only) for ``filename``.

    Callback data ``bookconv:<uid>:<token>:<fmt>`` — same-user bound; the
    token resolves the pending file record on the final tap.  Returns None
    when there are no valid targets.
    """
    if not calibre_available():
        return None
    targets = conversion_targets_for(os.path.splitext(filename)[1])
    if not targets:
        return None
    rows = [
        [
            InlineKeyboardButton(
                fmt.upper(),
                callback_data=f"bookconv:{uid}:{token}:{fmt}",
            )
            for fmt in targets[i : i + 3]
        ]
        for i in range(0, len(targets), 3)
    ]
    # ✖ Cancel: closes the format picker without queuing a conversion.
    rows.append(
        [
            InlineKeyboardButton(
                "\u2716\ufe0f Cancel",
                callback_data=f"bookcancel:{uid}:{token}",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


# ── Media detection → context builder menu ─────────────────────────────
# Shown at INPUT time (before any processing): the bot detects the media
# type, stores a pending record keyed by a short token, and offers
# type-appropriate actions as inline buttons.  Nothing is downloaded or piped
# until the user taps an option — one file, one download, one result.  This
# kills the old book echo-then-convert double pass and the auto-thumbnail
# pipeline for PDFs/images.
CTX_KEY = "ctxfile:{}"


def _classify_media(filename: str, mime: str) -> str | None:
    """Detect the media kind: ``pdf`` | ``image`` | ``book`` | None."""
    lower = (filename or "").lower()
    if lower.endswith(".pdf") or (mime or "") == "application/pdf":
        return "pdf"
    if (mime or "").startswith("image/") or lower.endswith(
        (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff")
    ):
        return "image"
    if is_book_format(filename):
        return "book"
    return None


def _load_pending_token(
    token: str, consume: bool, *prefixes: str
) -> dict | None:
    """Resolve a pending button record across record prefixes (best-effort).

    The input context menu stores under ``ctxfile:<token>`` while the
    delivered-file buttons store under their own prefixes (``bookconvert`` /
    ``bookcompress`` / ``bookocr``).  Handlers try both so a single token
    space serves input menus and delivered buttons alike.  ``consume=True``
    GETDELs the FIRST matching record atomically, so double-taps can never
    enqueue the same job twice.
    """
    if not token:
        return None
    for prefix in prefixes:
        key = f"{prefix}:{token}"
        if consume:
            raw = _consume_pending_record(key)
        else:
            raw = _peek_pending_record(key)
        if raw:
            try:
                rec = json.loads(raw)
            except Exception:  # nosec B110 - corrupt record = expired
                rec = None
            if rec:
                return rec
    return None


def _store_ctx_record(
    *,
    chat_id: int,
    user_id: int | None,
    message_id: int | None,
    file_id: str,
    file_unique_id: str | None,
    filename: str,
    mime: str,
    file_size: int | None,
    forward_info: dict | None,
    kind: str,
) -> str | None:
    """Persist the pending input-file record; returns its token or None."""
    token = uuid.uuid4().hex[:10]
    try:
        r = get_sync_redis()
        if not r:
            return None
        try:
            import config as _cfg

            _ttl = getattr(_cfg, "BOOK_ASK_TTL_SECONDS", 600)
        except Exception:  # nosec B110
            _ttl = 600
        r.setex(
            CTX_KEY.format(token),
            _ttl,
            json.dumps(
                {
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "filename": filename,
                    "mime": mime,
                    "chat_id": chat_id,
                    # The file lives at the input message in the user's chat,
                    # so re-downloads use chat_id + message_id (source None).
                    "source_chat_id": None,
                    "message_id": message_id,
                    "file_size": file_size,
                    "forward_info": forward_info,
                    "user_id": user_id,
                    "kind": kind,
                }
            ),
        )
        return token
    except Exception:  # nosec B110 - record store is best-effort
        logger.exception("Failed to store context-menu record for %s", filename)
        return None


def _ctx_menu_kb(
    uid: int, token: str, filename: str, kind: str
) -> InlineKeyboardMarkup | None:
    """The input context menu for a detected file (type-appropriate actions).

    Callback data reuses the delivered-file prefixes (``bookconvert`` /
    ``compresspdf`` / ``ocr``) so ONE set of handlers serves both flows; the
    new ``ctxthumb`` prefix starts the normal thumbnail pipeline.  Returns
    None when no action is available (feature disabled / engines missing) —
    callers then reply with a clear message instead of a dead menu.
    """
    rows: list[list[InlineKeyboardButton]] = []
    if kind == "pdf":
        # PDFs get a dedicated menu-interface: 🖼 Thumbnail, 🔎+🖼 OCR &
        # Thumbnail (all-in-one — no standalone OCR), and 🗜+🖼 Compress &
        # Thumbnail (compress_pdf_job already delivers a cover thumbnail).
        rows.append(
            [
                InlineKeyboardButton(
                    "\U0001f5bc\ufe0f Thumbnail",
                    callback_data=f"ctxthumb:{uid}:{token}",
                )
            ]
        )
        if is_ocr_source(filename) and ocr_enabled():
            rows.append(
                [
                    InlineKeyboardButton(
                        "\U0001f50e\U0001f5bc\ufe0f OCR & Thumbnail",
                        callback_data=f"ctxthumbocr:{uid}:{token}",
                    )
                ]
            )
        rows.append(
            [
                InlineKeyboardButton(
                    "\U0001f5dc\ufe0f\U0001f5bc\ufe0f Compress & Thumbnail",
                    callback_data=f"compresspdf:{uid}:{token}",
                )
            ]
        )
    elif kind == "image":
        rows.append(
            [
                InlineKeyboardButton(
                    "\U0001f5bc\ufe0f Thumbnail",
                    callback_data=f"ctxthumb:{uid}:{token}",
                )
            ]
        )
        if is_ocr_source(filename) and ocr_enabled():
            rows.append(
                [
                    InlineKeyboardButton(
                        "\U0001f50e OCR",
                        callback_data=f"ocr:{uid}:{token}",
                    )
                ]
            )
    elif kind == "book":
        if getattr(config, "ENABLE_BOOK_CONVERSION", False) and calibre_available():
            rows.append(
                [
                    InlineKeyboardButton(
                        "\U0001f501 Convert",
                        callback_data=f"bookconvert:{uid}:{token}",
                    )
                ]
            )
        # 🗜 Compress for a book = convert-to-PDF then shrink (needs Calibre).
        if getattr(config, "ENABLE_BOOK_CONVERSION", False) and calibre_available():
            rows.append(
                [
                    InlineKeyboardButton(
                        "\U0001f5dc\ufe0f Compress PDF",
                        callback_data=f"bookcomp:{uid}:{token}",
                    )
                ]
            )
        # Books aren't OCR-able directly — ocr_job converts them to PDF first
        # (needs Calibre), so the button is gated on Calibre being present.
        if ocr_enabled() and calibre_available():
            rows.append(
                [
                    InlineKeyboardButton(
                        "\U0001f50e OCR PDF",
                        callback_data=f"ocr:{uid}:{token}",
                    )
                ]
            )
    # ✖ Close on every input context menu: discard the pending record and
    # clear the menu without queuing anything.
    if rows:
        rows.append(
            [
                InlineKeyboardButton(
                    "\u2716\ufe0f Close",
                    callback_data=f"ctxclose:{uid}:{token}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows) if rows else None


def _queued_cancel_kb(user_id: int | None, job_id: str) -> InlineKeyboardMarkup | None:
    """The ❌ cancel button attached to a 'Queued...' reply.

    ``callback_data`` is ``canceljob:<user_id>:<job_id>`` — tapping it arms a
    same-user-bound confirmation (see ``handle_canceljob_arm_callback``), so
    a user can cancel a just-queued job without typing /canceljob. Returns
    None when the owner or job id is missing (no button then).
    """
    if not user_id or not job_id:
        return None
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "❌ Cancel this job",
                    callback_data=f"canceljob:{user_id}:{job_id[:32]}",
                )
            ]
        ]
    )


def _build_status_summary(uid: int | None, is_owner: bool) -> str:
    """Per-user /status summary: global depth + the caller's jobs.

    Reads the RQ registries directly from Redis using the NON-decoding
    connection (same requirement as the RQ worker — job payloads are pickled
    raw bytes). The per-user section is private to the caller; the owner
    additionally sees a breakdown of who else is loading the bot.
    """
    try:
        from rq.job import Job

        from utils.redis_client import get_sync_redis_raw

        r = get_sync_redis_raw()
        if not r:
            return (
                "Bot: active\n(Redis unreachable \u2014 job summary unavailable)",
                False,
            )

        now = time.time()

        def _list_ids(key: str) -> list[str]:
            try:
                return [
                    m.decode() if isinstance(m, bytes) else str(m)
                    for m in r.lrange(key, 0, -1)
                ]
            except Exception:  # nosec B110
                return []

        def _zset_since(
            key: str, since: float, num: int = 50
        ) -> list[tuple[str, float]]:
            """Most recent ``num`` members of a registry zset (id, score)."""
            try:
                return [
                    (m.decode() if isinstance(m, bytes) else str(m), float(score))
                    for m, score in r.zrevrangebyscore(
                        key, now, since, start=0, num=num, withscores=True
                    )
                ]
            except Exception:  # nosec B110
                return []

        def _zcount(key: str, since: float) -> int:
            """Full registry count since a timestamp (for the header line)."""
            try:
                return int(r.zcount(key, since, now))
            except Exception:  # nosec B110
                return 0

        def _fetch(job_id: str):
            try:
                return Job.fetch(job_id, connection=r)
            except Exception:  # nosec B110 - job may have expired mid-scan
                return None

        queued_ids = _list_ids("rq:queue:default")
        started = _zset_since("rq:wip:default", 0)
        finished_24h = _zset_since("rq:finished:default", now - 86400)
        failed_24h = _zset_since("rq:failed:default", now - 86400)
        finished_total = _zcount("rq:finished:default", now - 86400)
        failed_total = _zcount("rq:failed:default", now - 86400)

        per_user: dict[int, dict[str, int]] = {}
        mine: list[tuple[str, str, str, float | None]] = []  # (label, status, cancel_id, ts)

        def _tally(job_id: str, status: str, ts: float | None = None) -> None:
            job = _fetch(job_id)
            if job is None:
                return
            u = _job_user_id(job)
            if u is not None:
                bucket = per_user.setdefault(
                    u, {"queued": 0, "running": 0, "finished": 0, "failed": 0}
                )
                bucket[status] += 1
                if u == uid and status in ("queued", "running"):
                    mine.append(
                        (_job_label(job), status, _job_cancel_id(job, job_id), ts)
                    )

        for jid in queued_ids:
            _tally(jid, "queued")
        for jid, ts in started:
            _tally(jid, "running", ts)
        for jid, ts in finished_24h:
            _tally(jid, "finished", ts)
        for jid, ts in failed_24h:
            _tally(jid, "failed", ts)

        lines = ["\u2705 Bot: active"]
        parts = []
        if queued_ids:
            parts.append(f"{len(queued_ids)} queued")
        if started:
            parts.append(f"{len(started)} running")
        if finished_total:
            parts.append(f"{finished_total} finished (24h)")
        if failed_total:
            parts.append(f"{failed_total} failed (24h)")
        lines.append(" \u00b7 ".join(parts) if parts else "No jobs in the last 24h")

        if uid is not None:
            b = per_user.get(uid, {})
            lines.append("")
            if mine:
                lines.append("\U0001f464 Your jobs:")
                for label, status, cid, ts in mine:
                    icon = "\U0001f4e5" if status == "queued" else "\U0001f504"
                    age = f" \u00b7 {_ago(ts)}" if ts else ""
                    lines.append(
                        # Filenames can contain `_` (a Markdown italic
                        # delimiter) — escape the raw-text label so the
                        # summary is safe under parse_mode="Markdown".
                        f"\u2022 {icon} {escape_markdown(label)} \u2014 {status}{age}"
                        f" \u00b7 `/canceljob {cid[:8]}`"
                    )
            else:
                lines.append("\U0001f464 Your jobs: none active/queued")
            extra = []
            if b.get("finished"):
                extra.append(f"{b['finished']} finished")
            if b.get("failed"):
                extra.append(f"{b['failed']} failed")
            if extra:
                lines.append("   (" + ", ".join(extra) + " in the last 24h)")

        if is_owner and uid is not None:
            others = [
                (u, b)
                for u, b in per_user.items()
                if u != uid and (b["queued"] or b["running"])
            ]
            if others:
                lines.append("")
                lines.append("\U0001f465 Other users (queued/running):")
                for u, b in sorted(
                    others, key=lambda x: -(x[1]["queued"] + x[1]["running"])
                ):
                    lines.append(
                        f"\u2022 {u}: {b['queued']} queued, {b['running']} running"
                    )

        # Show the cancel button when the user has something to cancel: any
        # queued/running RQ job OR an active inline progress task.
        has_jobs = bool(mine) or any(
            getattr(t, "user_id", None) == uid
            for t in progress_tracker.tasks.values()
        )
        return "\n".join(lines), has_jobs
    except Exception:
        logger.exception("Failed to build /status summary")
        return "Bot: active", False


async def cmd_status(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    uid = getattr(update.effective_user, "id", None)
    logger.info("/status: user_id=%s", uid)
    await _track_user_session(update, "/status")
    if not config.is_user_allowed(uid):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    # Per-user summary: global queue depth + the caller's active/queued jobs.
    # Falls back to "Bot: active" if Redis is unreachable.
    summary, has_jobs = _build_status_summary(uid, config.is_owner(uid))
    if has_jobs:
        # The queue-wide cancel button is admin-only: only attach it for
        # admins (the confirm flow itself also enforces the admin gate).
        _kb = _cancel_all_kb() if config.is_admin_user(uid) else None
        await update.effective_message.reply_text(
            summary, reply_markup=_kb, parse_mode="Markdown"
        )
    else:
        await update.effective_message.reply_text(
            summary, parse_mode="Markdown"
        )


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
            f"\U0001f512 CSRF protection enabled (secret token configured)"
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
        BotCommand("status", "Bot status: queue & your jobs"),
        BotCommand("login", "Login your Telethon userbot"),
        BotCommand("loginpyro", "Login your Pyrogram userbot"),
        BotCommand("loginstatus", "Check your live session health"),
        BotCommand("logout", "Logout your Telethon session"),
        BotCommand("logoutpyro", "Logout your Pyrogram session"),
        BotCommand("clearflood", "Clear an active login flow"),
        BotCommand("admin", "Manage allowed users"),
        BotCommand(
            "clear_cache", "(admin) Clear cached results & thumbnails"
        ),
        BotCommand("startbatch", "Start collecting forwarded files"),
        BotCommand("endbatch", "Process collected batch"),
        BotCommand("cancelbatch", "Cancel batch collection"),
        BotCommand("ocr", "Set OCR output default (pdf/txt/picker)"),
        BotCommand("canceljob", "Cancel a job (asks to confirm)"),
        BotCommand("cancelall", "(admin) Cancel all queued jobs"),
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
            enqueue_job, "process_document_batch_job",            chat_id, items, user_id,
            owner_user_id=user_id,
            # job_timeout > RQ's 180s default: batches can contain large
            # e-books whose inline deliver_book_job (download + echo) must not
            # be killed by the death penalty mid-run.
            job_timeout=1800,
        )
        if ok:
            await asyncio.to_thread(clear_forward_batch, chat_id, user_id)
            queued_msg = await update.effective_message.reply_text(
                f"Queued batch with {len(items)} items for processing.\n"
                f"Job ID: `{ok}` — use /canceljob {ok} to cancel it.",
                reply_markup=_queued_cancel_kb(user_id, ok),
                parse_mode="Markdown",
            )
            _store_queued_message(
                ok,
                chat_id,
                getattr(queued_msg, "message_id", None),
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


# ── Admin: /clear_cache (cached results, leftovers & thumbnails) ──


def _format_clearcache_result(res: dict) -> str:
    """Human-readable summary of a ``run_cache_clear`` result dict."""
    r = res.get("redis", {})
    f = res.get("files", {})
    lines = ["\u2705 Cache cleared"]
    deleted = r.get("deleted", 0) or 0
    skipped = r.get("skipped_live", 0) or 0
    lines.append(
        f"\u2022 Redis: {deleted:,} key(s) removed"
        + (f" \u00b7 {skipped:,} live-job key(s) kept" if skipped else "")
    )
    keys_before = r.get("keys_before")
    keys_after = r.get("keys_after")
    if keys_before is not None and keys_after is not None:
        lines.append(f"  cache keys: {keys_before:,} \u2192 {keys_after:,}")
    freed = f.get("freed_bytes", 0) or 0
    lines.append(
        f"\u2022 Files: {f.get('deleted_files', 0):,} deleted / "
        f"{_format_size(freed) or '0 B'}"
    )
    for name in ("thumbnails", "temp", "input", "output"):
        by_dir = f.get("by_dir", {}).get(name)
        if by_dir:
            lines.append(
                f"  {name}: {by_dir.get('files', 0):,} / "
                f"{_format_size(by_dir.get('bytes', 0)) or '0 B'}"
            )
    lines.append(
        "\nQueued & running jobs were left untouched. "
        "You can run /clear_cache again anytime."
    )
    return "\n".join(lines)


async def _run_clearcache(update: Update) -> None:
    """Run the cache clear on a worker thread and report the result."""
    try:
        result = await asyncio.to_thread(run_cache_clear)
    except Exception:
        logger.exception("clear_cache failed")
        try:
            await update.effective_message.reply_text(
                "\u274c Cache clear failed \u2014 check the logs."
            )
        except Exception:  # nosec B110
            pass
        return
    try:
        await update.effective_message.reply_text(
            _format_clearcache_result(result)
        )
    except Exception:
        logger.exception("clear_cache: failed to send result")


async def cmd_clearcache(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Clear cached results, job leftovers and cached files (admin only)."""
    await _track_user_session(update, "/clear_cache")
    if not config.is_admin_user(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Unauthorized: admin only"
        )
        return
    uid = getattr(update.effective_user, "id", None)
    args = context.args if hasattr(context, "args") else []
    if args and args[0].strip().lower() == "confirm":
        await _run_clearcache(update)
        return
    confirm_kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\U0001f5d1\ufe0f Yes, clear caches",
                    callback_data=f"clearcache_confirm:{uid}",
                )
            ],
            [
                InlineKeyboardButton(
                    "\u274c No", callback_data=f"clearcache_abort:{uid}"
                )
            ],
        ]
    )
    try:
        await update.effective_message.reply_text(
            "\U0001f9f9 Clear all caches?\n\n"
            "This wipes:\n"
            "\u2022 Redis caches \u2014 re-send/dedup records, pdfcheck, "
            "file/session caches, pending menus, batch state\n"
            "\u2022 Job leftovers \u2014 progress / io / cancel / pipeline "
            "bookkeeping (only finished jobs; active jobs are preserved)\n"
            "\u2022 Cached files \u2014 thumbnails, temp, input, output "
            "folders (skipping files in use by active jobs)\n\n"
            "Queued & running jobs are NOT cancelled.\n"
            "Reply with /clear_cache confirm, or tap the button below.",
            reply_markup=confirm_kb,
        )
    except Exception:
        logger.exception("clear_cache: failed to show confirmation")


async def handle_clearcache_confirm_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Second tap: actually clear the caches (same-user only)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        armer = int(str(query.data).split(":", 1)[1])
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can confirm.", show_alert=True
        )
        return
    try:
        await query.answer()
    except Exception:  # nosec B110
        pass
    try:
        await query.edit_message_text("\U0001f9f9 Clearing caches\u2026")
    except Exception:  # nosec B110
        pass
    try:
        result = await asyncio.to_thread(run_cache_clear)
    except Exception:
        logger.exception("clear_cache failed")
        try:
            await query.edit_message_text(
                "\u274c Cache clear failed \u2014 check the logs."
            )
        except Exception:  # nosec B110
            pass
        return
    try:
        await query.edit_message_text(_format_clearcache_result(result))
    except Exception:
        logger.exception("clear_cache: failed to report result")


async def handle_clearcache_abort_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Second tap on \"No\": keep everything (same-user only)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        armer = int(str(query.data).split(":", 1)[1])
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can abort it.",
            show_alert=True,
        )
        return
    try:
        await query.answer()
        await query.edit_message_text(
            "Cache clear aborted \u2014 nothing was changed."
        )
    except Exception:  # nosec B110
        pass


async def handle_clearcache_stale_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Old unsuffixed buttons from before the same-user binding."""
    query = update.callback_query
    if query is None:
        return
    try:
        await query.answer(
            "This button is outdated \u2014 run /clear_cache instead",
            show_alert=True,
        )
    except Exception:  # nosec B110
        pass


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
      - the queued_msg:<id> record (the Telegram "Queued..." message is
        deleted separately by the caller before this runs, so the record is
        still readable when the message ids are fetched)
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
            f"queued_msg:{job_id}",
        ):
            try:
                r.delete(key)
            except Exception:  # nosec B110
                pass
    except Exception:  # nosec B110
        pass


def _rq_job_owned_by(job, user_id: int | None) -> bool:
    """True when ``job`` may be cancelled by ``user_id`` (per-user gate).

    Every RQ job type tags ``meta['user_id']`` via ``_attach_job_user_meta``,
    so tagged jobs are strictly user-scoped — another group member can no
    longer cancel them.  Untagged jobs (e.g. CLI test enqueues that pass no
    user) fall back to the legacy chat-scoped behaviour (owned by anyone in
    the caller's chat), so isolation never blocks legacy jobs.

    When the user tag cannot be read, the job is treated as NOT owned
    (fail-closed) — never allow a cancel we cannot verify.
    """
    if user_id is None:
        return True
    try:
        meta_uid = (getattr(job, "meta", None) or {}).get("user_id")
    except Exception:  # nosec B110 - meta read failure = not user-verified
        return False
    if meta_uid is None:
        return True
    try:
        return str(meta_uid) == str(user_id)
    except Exception:
        return False


def _resolve_rq_job(
    job_id: str, chat_id: int | None, user_id: int | None = None, r=None
):
    """Resolve an RQ job id/prefix to a Job the caller may cancel, or None.

    Non-mutating — used by the /canceljob confirmation prompt to describe
    what WOULD be cancelled, and by ``_cancel_rq_job`` which then performs
    the actual cancel. Ownership is enforced here: the job is only returned
    when it originated from ``chat_id`` (enqueues pass chat_id as the first
    positional argument) AND — when ``user_id`` is provided and the job
    carries a ``meta['user_id']`` tag — when that tag matches the caller
    (per-user isolation in shared group chats).

    ``r`` is an optional raw Redis connection to reuse; ``_cancel_rq_job``
    passes its own so the fetch and the cancel flag share ONE connection
    (a transiently-broken second connection must not silently lose the
    flag). When omitted a fresh connection is opened.
    """
    try:
        from rq.job import Job

        from utils.redis_client import get_sync_redis_raw

        # A full RQ id is 32-char uuid4 hex; refuse unusably short inputs so
        # prefix matching can never accidentally match everything ("").
        if len(job_id) < 4:
            return None
        if r is None:
            r = get_sync_redis_raw()
        if not r:
            return None
        job = None
        try:
            job = Job.fetch(job_id, connection=r)
        except Exception:  # nosec B110 - fall through to prefix resolution
            pass
        if job is None:
            for candidate in _rq_ids_by_prefix(r, job_id):
                try:
                    cand = Job.fetch(candidate, connection=r)
                except Exception:
                    # A corrupt/stale job key shouldn't abort the scan —
                    # fall through and check the next prefix candidate.
                    cand = None
                if cand is None:
                    continue
                # Ownership: only cancel jobs from the caller's chat — keep
                # looking if the first prefix candidate belongs to someone else.
                c_args = list(getattr(cand, "args", None) or [])
                if chat_id is not None and (not c_args or c_args[0] != chat_id):
                    continue
                if not _rq_job_owned_by(cand, user_id):
                    continue
                job = cand
                break
        if job is None:
            return None
        # All enqueued jobs pass chat_id as the first positional argument.
        args = list(getattr(job, "args", None) or [])
        if chat_id is not None and (not args or args[0] != chat_id):
            return None
        if not _rq_job_owned_by(job, user_id):
            return None
        return job
    except Exception:
        return None


def _cancel_rq_job(
    job_id: str, chat_id: int | None, user_id: int | None = None
) -> str | None:
    """Best-effort cancel of an RQ job by id, but only when the job originated
    from the caller's chat (chat ownership for shared group chats) AND, when
    the job carries a user tag, from the caller's user (per-user isolation —
    see ``_resolve_rq_job`` / ``_rq_job_owned_by``).

    Returns the RESOLVED full job id on success (RQ ids are 36-char dashed
    UUIDs, but the queued-button payload embeds ``job_id[:32]`` — callers
    must clean up Redis keys under the full id, not the possibly-truncated
    input) or None on failure.

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
        from rq.job import JobStatus

        from utils.redis_client import get_sync_redis_raw

        # RQ stores job payloads pickled as raw bytes, so RQ operations must
        # use a NON-decoding connection (the decode_responses=True singleton
        # would UnicodeDecodeError on Job.fetch and silently fail to cancel).
        r = get_sync_redis_raw()
        if not r:
            return None
        job = _resolve_rq_job(job_id, chat_id, user_id=user_id, r=r)
        if job is None:
            return None
        # The flag uses the RESOLVED full id (prefix inputs resolve to it).
        full_id = getattr(job, "id", None) or job_id

        # Belt: the worker aborts jobs on this flag, so set it BEFORE RQ
        # bookkeeping — a failed job.cancel() must never lose the cancel.
        try:
            r.setex(f"cancel:{full_id}", 3600, "1")
        except Exception:  # nosec B110 - flag is best-effort
            pass

        def _drop_from_registries() -> None:
            """Fallback: remove the job from queue/started registries and mark
            it canceled, plus (re)set the cancel flag."""
            origin = getattr(job, "origin", None) or "default"
            # Wrong-type errors are impossible (queue is a list, wip is a
            # zset); each key op is independently guarded.
            try:
                r.lrem(f"rq:queue:{origin}", 0, full_id)
            except Exception:  # nosec B110
                pass
            try:
                r.zrem(f"rq:wip:{origin}", full_id)
            except Exception:  # nosec B110
                pass
            try:
                job.set_status(JobStatus.CANCELED)
            except Exception:  # nosec B110
                pass
            # Re-set the abort flag in case the belt attempt above failed.
            try:
                r.setex(f"cancel:{full_id}", 3600, "1")
            except Exception:  # nosec B110
                pass

        try:
            job.cancel()
            return full_id
        except Exception:  # nosec B110 - RQ 2.x execution-registry race etc.
            _drop_from_registries()
            return full_id
    except Exception:
        return None


def _rq_ids_by_prefix(r, prefix: str) -> list[str]:
    """Job ids in the default queue/started registries starting with ``prefix``."""
    found = []
    try:
        for m in r.lrange("rq:queue:default", 0, -1):
            s = m.decode() if isinstance(m, bytes) else str(m)
            if s.startswith(prefix):
                found.append(s)
    except Exception:  # nosec B110
        pass
    try:
        for m, _ in r.zrange("rq:wip:default", 0, -1, withscores=True):
            s = m.decode() if isinstance(m, bytes) else str(m)
            if s.startswith(prefix):
                found.append(s)
    except Exception:  # nosec B110
        pass
    return found


def _pipeline_hash_cancellable(h: dict) -> bool:
    """True when a ``pdf:job:<id>`` hash is a live, not-yet-cancelled job.

    Shared by ``_cancel_pipeline_job`` and ``_pipeline_job_exists`` so the
    terminal/already-cancelled guards can't drift.  Terminal statuses are
    written by the worker on completion/cancel/failure (``done``,
    ``s3_fallback``, ``too_large``, ``cancelled``, ``failed``, ``error``); a
    hash whose cancel flag is already set has nothing new to cancel.
    """
    _st = h.get("status") or h.get(b"status") or ""
    if isinstance(_st, bytes):
        _st = _st.decode()
    if _st in (
        "done",
        "s3_fallback",
        "too_large",
        "cancelled",
        "failed",
        "error",
    ):
        return False
    if (h.get("cancel") or h.get(b"cancel") or "") in ("1", b"1"):
        return False
    return True


def _cancel_pipeline_job(job_id: str, user_id: int | None) -> bool:
    """Best-effort cancel of a BigFilePipeline job — queued or in-flight.

    Sets the ``pdf:job:<id>`` hash ``cancel`` flag and removes any queued
    entry from the ``pdf:jobs`` Redis list — but only for jobs owned by
    ``user_id`` (multi-user isolation).  Jobs already popped by the pipeline
    worker are cancelled via the hash flag (the worker's mid-flight checks
    abort the S3 download); ownership is verified against the ``user_id``
    field stored on the hash at enqueue time.
    """
    removed = False
    flag_set = False
    try:
        from utils.job_queue import JOB_LIST

        r = get_sync_redis()
        if not r:
            return False
        # Refuse unusably short inputs so prefix matching can't match everything.
        if len(job_id) < 4:
            return False
        # 1) Queued: remove the entry from pdf:jobs (ownership via job dict).
        raw_items = r.lrange(JOB_LIST, 0, -1)
        for item in raw_items:
            raw = item.decode() if isinstance(item, bytes) else item
            try:
                d = json.loads(raw)
            except Exception:  # nosec B112 - skip non-JSON entries in the queue
                continue
            _dj = str(d.get("job_id") or "")
            if (_dj == job_id or _dj.startswith(job_id)) and (
                user_id is None or d.get("user_id") == user_id
            ):
                try:
                    r.hset(f"pdf:job:{job_id}", mapping={"cancel": "1"})
                    flag_set = True
                except Exception:  # nosec B110
                    pass
                try:
                    r.lrem(JOB_LIST, 0, item)
                    removed = True
                except Exception:  # nosec B110
                    pass
        # 2) In-flight: the job was already popped; verify ownership via the
        #    pdf:job:<id> hash (user_id stored by enqueue_job) and set the
        #    cancel flag so the worker's mid-flight checks abort it.  Terminal
        #    statuses and an already-set cancel flag mean nothing NEW to cancel.
        #    ``user_id=None`` (admin /cancelall) bypasses the ownership gate.
        if not removed:
            try:
                h = r.hgetall(f"pdf:job:{job_id}") or {}
            except Exception:  # nosec B110
                h = {}
            if h:
                if not _pipeline_hash_cancellable(h):
                    return removed or flag_set
                howner = h.get("user_id") or h.get(b"user_id") or ""
                if isinstance(howner, bytes):
                    howner = howner.decode()
                if user_id is None or str(howner) == str(user_id):
                    try:
                        r.hset(f"pdf:job:{job_id}", mapping={"cancel": "1"})
                        flag_set = True
                    except Exception:  # nosec B110
                        pass
        return removed or flag_set
    except Exception:
        return False


def _pipeline_job_exists(job_id: str, user_id: int | None) -> bool:
    """Non-mutating: does a BigFilePipeline job owned by ``user_id`` exist?

    Mirrors ``_cancel_pipeline_job``'s matching (exact or prefix, with the
    per-user ownership gate) but performs no cancellation — used by the
    /canceljob confirmation prompt.  Also recognizes in-flight jobs via the
    ``pdf:job:<id>`` hash (ownership from the stored ``user_id`` field).
    """
    if len(job_id) < 4:
        return False
    try:
        from utils.job_queue import JOB_LIST

        r = get_sync_redis()
        if not r:
            return False
        for item in r.lrange(JOB_LIST, 0, -1):
            raw = item.decode() if isinstance(item, bytes) else item
            try:
                d = json.loads(raw)
            except Exception:  # nosec B112 - skip non-JSON entries in the queue
                continue
            _dj = str(d.get("job_id") or "")
            if (_dj == job_id or _dj.startswith(job_id)) and (
                user_id is None or d.get("user_id") == user_id
            ):
                return True
        # In-flight check: the hash exists (job was popped) and is owned by
        # the caller (user_id stored by enqueue_job).  Terminal statuses and
        # already-cancelled hashes are not offered for cancellation.
        try:
            h = r.hgetall(f"pdf:job:{job_id}") or {}
        except Exception:  # nosec B110
            h = {}
        if h:
            if not _pipeline_hash_cancellable(h):
                return False
            howner = h.get("user_id") or h.get(b"user_id") or ""
            if isinstance(howner, bytes):
                howner = howner.decode()
            if user_id is None or str(howner) == str(user_id):
                return True
        return False
    except Exception:
        return False


def _owned_progress_task_id(job_id: str, uid: int | None) -> str | None:
    """Resolve a progress task id/prefix owned by ``uid``, or None.

    Shared by the /canceljob arm (describe what WOULD be cancelled) and the
    actual cancel so both agree on the ownership rule.
    """
    task_id = progress_tracker.find_task_id_by_prefix(job_id)
    if not task_id:
        return None
    task = progress_tracker.get_task(task_id)
    if task is not None and getattr(task, "user_id", None) == uid:
        return task_id
    return None


def _resolve_cancel_targets(
    job_id: str, uid: int | None, chat_id: int | None
) -> list[str]:
    """Describe what /canceljob WOULD cancel for this caller (no mutation).

    Returns the same action labels ``_do_cancel_job`` reports, so the
    confirmation prompt never offers to cancel a dead job or someone else's.
    """
    targets = []
    task_id = _owned_progress_task_id(job_id, uid)
    if task_id:
        targets.append(f"progress task `{task_id[:8]}`")
    rq_job = _resolve_rq_job(job_id, chat_id, user_id=uid)
    if rq_job is not None:
        # Label with the FULL resolved id (RQ ids are 36-char dashed UUIDs;
        # the caller may have passed a truncated 32-char prefix).
        targets.append(f"RQ job `{getattr(rq_job, 'id', None) or job_id}`")
    if _pipeline_job_exists(job_id, uid):
        targets.append(f"pipeline job `{job_id}`")
    return targets


async def _do_cancel_job(
    job_id: str, uid: int | None, chat_id: int | None
) -> tuple[bool, list[str]]:
    """Perform the actual /canceljob cancellation (progress, RQ, pipeline).

    Mirrors the pre-confirmation cancel body: every path enforces its own
    ownership gate (progress by user_id, RQ by chat + user, pipeline by
    user_id).
    On any owned hit, the per-job cleanup runs — auto-delete the "Queued..."
    confirmations, wipe the io/queued_msg keys, re-arm the in-flight abort
    flag — and ``(owned, action_labels)`` is returned.
    """
    actions = []
    owned = False
    # The Redis keys (queued_msg:<id>, cancel:<id>, ...) were stored under the
    # FULL job id at queue time. The queued-button payload truncates RQ ids to
    # 32 chars (they are 36-char dashed UUIDs), so cleanup must use the
    # resolved full id — otherwise the "Queued..." message and keys survive.
    cleanup_id: str | None = job_id

    # 1) Inline progress task — only the owning user may cancel it
    task_id = _owned_progress_task_id(job_id, uid)
    if task_id:
        owned = True
        if await progress_tracker.cancel_task(task_id):
            actions.append(f"progress task `{task_id[:8]}`")

    # 2) RQ job (queued/started Bot API pipeline) — caller's chat + user only.
    # _cancel_rq_job resolves the full id internally and returns it.
    full_rq_id = await asyncio.to_thread(
        _cancel_rq_job, job_id, chat_id, uid
    )
    if full_rq_id:
        owned = True
        cleanup_id = full_rq_id
        actions.append(f"RQ job `{full_rq_id}`")

    # 3) BigFilePipeline job — caller's own only
    if _cancel_pipeline_job(job_id, uid):
        owned = True
        actions.append(f"pipeline job `{job_id}`")

    if owned and cleanup_id:
        # Auto-delete the "Queued..." confirmation(s) for this job BEFORE
        # wiping keys (the wipe would remove the queued_msg:<id> record that
        # _delete_queued_messages needs to read the message ids).
        try:
            import tasks

            tasks._delete_queued_messages(cleanup_id)
        except Exception:  # nosec B110
            pass
        # Ownership verified: wipe Redis keys + set the in-flight abort flag
        _wipe_job_redis_keys(cleanup_id)
        try:
            r = get_sync_redis()
            if r:
                r.setex(f"cancel:{cleanup_id}", 3600, "1")
        except Exception:  # nosec B110
            pass
    return owned, actions


async def cmd_canceljob(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Cancel a queued or in-flight job by id (progress task, RQ job, or pipeline job).

    Requires an explicit confirmation — ``/canceljob <id> confirm`` or the
    inline button — so an accidental cancel can't kill a job.
    """
    await _track_user_session(update, "/canceljob")
    if not config.is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    args = context.args if hasattr(context, "args") else []
    if not args:
        await update.effective_message.reply_text(
            "Usage: /canceljob <job_id> [confirm]\n\n"
            "You can find the job id in the 'Queued...' reply, the progress "
            "message (ID: xxxxxxxx), or the /status job list.\n"
            "The first run asks for confirmation; append 'confirm' to skip it."
        )
        return
    uid = getattr(update.effective_user, "id", None)
    chat_id = update.effective_chat.id if update.effective_chat else None

    if args[0].strip().lower() == "confirm":
        if len(args) < 2:
            await update.effective_message.reply_text(
                "Usage: /canceljob <job_id> confirm"
            )
            return
        job_id = args[1].strip()
        owned, actions = await _do_cancel_job(job_id, uid, chat_id)
        if owned:
            header = "✅ Cancelled: " + ", ".join(actions)
        else:
            header = (
                f"No active job found for you with id "
                f"`{safe_code_span(job_id)}`. "
                "It may have already finished."
            )
        # Append the refreshed /status summary (mirrors the confirm callback
        # and cancel-all): the cancel-all button is offered when jobs remain.
        text, kb = _cancel_status_reply(uid, header)
        await update.effective_message.reply_text(
            text, reply_markup=kb, parse_mode="Markdown"
        )
        return

    job_id = args[0].strip()
    targets = _resolve_cancel_targets(job_id, uid, chat_id)
    if not targets:
        await update.effective_message.reply_text(
            f"No active job found for you with id `{safe_code_span(job_id)}`. "
            "It may have already finished.",
            parse_mode="Markdown",
        )
        return
    # Same-user-bound confirm/abort buttons (well under the 64-byte callback
    # data limit: 17 + uid + ':' + job_id capped at 32 chars).
    confirm_kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Yes, cancel job",
                    callback_data=f"canceljob_confirm:{uid}:{job_id[:32]}",
                ),
                InlineKeyboardButton(
                    "❌ No",
                    callback_data=f"canceljob_abort:{uid}:{job_id[:32]}",
                ),
            ]
        ]
    )
    await update.effective_message.reply_text(
        "⚠️ This will cancel:\n• "
        + "\n• ".join(targets)
        + "\n\nReply with /canceljob <id> confirm, or tap the button below.",
        reply_markup=confirm_kb,
        parse_mode="Markdown",
    )


async def _cancel_all_stalled() -> tuple[int, int, int]:
    """Cancel ALL queued/in-flight jobs across both pipelines (admin only).

    Admin /cancelall: unlike the old per-user ``_cancel_all_for``, the
    ownership gates are bypassed (``user_id=None`` / ``chat_id=None``) so the
    ENTIRE queue is cleared — every user's progress tasks, RQ jobs (queued +
    in-flight + scheduled/deferred) and BigFile pipeline jobs (queued +
    in-flight).  Each job goes through the same machinery as /canceljob:
    cancel flags the worker honours mid-flight, registry cleanup, and
    Queued-message removal.

    Returns (progress, RQ, pipeline) counts.
    """
    tasks_cancelled = 0
    rq_cancelled = 0
    pipe_cancelled = 0
    cleaned: list[str] = []

    # 1) Progress tasks — every user, in-memory + Redis.
    cancelled_tids: set[str] = set()
    try:
        for tid in list(progress_tracker.tasks.keys()):
            if await progress_tracker.cancel_task(tid):
                tasks_cancelled += 1
                cancelled_tids.add(tid)
        r = get_sync_redis()
        if r:
            for key in r.scan_iter(
                f"{progress_tracker.PREFIX_PROGRESS}*", count=100
            ):
                k = key.decode() if isinstance(key, bytes) else key
                tid = k[len(progress_tracker.PREFIX_PROGRESS):]
                # Skip tasks the in-memory loop already cancelled (their
                # Redis record may still exist) and stale orphans with no
                # task record at all.
                if tid in cancelled_tids or tid in progress_tracker.tasks:
                    continue
                t = progress_tracker.get_task(tid)
                if t is not None:
                    if await progress_tracker.cancel_task(tid):
                        tasks_cancelled += 1
                        cancelled_tids.add(tid)
    except Exception:
        logger.exception("cancelall: progress-task cancellation failed")

    # 2) RQ jobs — queued + in-flight + scheduled/deferred, every user.
    try:
        r = get_sync_redis_raw()
        if r:
            candidates: list[str] = []
            for key in r.keys("rq:queue:*"):
                for m in r.lrange(key, 0, -1):
                    candidates.append(
                        m.decode() if isinstance(m, bytes) else str(m)
                    )
            for key in (
                r.keys("rq:wip:*")
                + r.keys("rq:started:*")
                + r.keys("rq:scheduled:*")
                + r.keys("rq:deferred:*")
            ):
                for m, _score in r.zrange(key, 0, -1, withscores=True):
                    candidates.append(
                        m.decode() if isinstance(m, bytes) else str(m)
                    )
            seen: set[str] = set()
            for cid in candidates:
                if cid in seen:
                    continue
                seen.add(cid)
                # None ownership bypasses the chat/user gates.
                _full = await asyncio.to_thread(
                    _cancel_rq_job, cid, None, None
                )
                if _full:
                    rq_cancelled += 1
                    cleaned.append(_full)
    except Exception:
        logger.exception("cancelall: RQ cancellation failed")

    # 3) BigFilePipeline jobs — queued + in-flight, every user.
    try:
        from utils.cache_cleanup import PIPELINE_TERMINAL_STATUSES
        from utils.job_queue import DELAYED_SET, JOB_LIST

        r = get_sync_redis()
        if r:
            # Queued: every entry in the pdf:jobs list + pdf:delayed zset.
            pipe_ids: set[str] = set()
            delayed_ids: set[str] = set()
            for key in (JOB_LIST, DELAYED_SET):
                try:
                    _type = r.type(key)
                    if isinstance(_type, bytes):
                        _type = _type.decode()
                    _type = str(_type)
                except Exception:
                    _type = ""
                try:
                    if _type == "list":
                        items = r.lrange(key, 0, -1)
                    elif _type == "zset":
                        items = r.zrange(key, 0, -1)
                    else:
                        items = []
                except Exception:
                    items = []
                for item in items:
                    raw = item.decode() if isinstance(item, bytes) else item
                    try:
                        d = json.loads(raw)
                    except Exception:  # nosec B112 - skip non-JSON entries
                        continue
                    _jid = str(d.get("job_id") or "")
                    if _jid:
                        pipe_ids.add(_jid)
                    if _type == "zset":
                        # Delayed jobs are cancelled by removing their entry
                        # (they will never be promoted); flag the metadata
                        # hash too so the job reads as cancelled.
                        delayed_ids.add(_jid)
                        try:
                            r.zrem(key, item)
                        except Exception:  # nosec B110
                            pass
                        if _jid:
                            pipe_cancelled += 1
                            cleaned.append(_jid)
                            try:
                                r.hset(
                                    f"pdf:job:{_jid}",
                                    mapping={"cancel": "1"},
                                )
                            except Exception:  # nosec B110
                                pass
            for _jid in pipe_ids:
                # Delayed entries are fully handled above (zrem + flag); the
                # generic cancel below would only re-set the same flag.
                if _jid in delayed_ids:
                    continue
                if _cancel_pipeline_job(_jid, None):
                    pipe_cancelled += 1
                    cleaned.append(_jid)
            # In-flight: the worker popped the queue entry, so only the
            # pdf:job:<id> hash remains (non-terminal status).
            try:
                for key in r.scan_iter("pdf:job:*", count=200):
                    k = key.decode() if isinstance(key, bytes) else key
                    _jid = k[len("pdf:job:"):]
                    if not _jid or _jid in pipe_ids:
                        continue
                    try:
                        h = r.hgetall(key) or {}
                    except Exception:  # nosec B110
                        h = {}
                    if not h:
                        continue
                    _status = h.get("status") or h.get(b"status") or ""
                    if isinstance(_status, bytes):
                        _status = _status.decode()
                    if _status in PIPELINE_TERMINAL_STATUSES:
                        continue
                    if _cancel_pipeline_job(_jid, None):
                        pipe_cancelled += 1
                        cleaned.append(_jid)
            except Exception:
                logger.debug(
                    "cancelall: pipeline in-flight scan failed", exc_info=True
                )
    except Exception:
        logger.exception("cancelall: pipeline cancellation failed")

    # Mirror cmd_canceljob's per-job cleanup: delete the "Queued..."
    # confirmation FIRST (the wipe would remove the queued_msg:<id> record
    # it needs), then wipe bookkeeping keys, then re-arm the abort flag.
    for cid in cleaned:
        try:
            import tasks  # noqa: PLC0415 - same pattern as cmd_canceljob

            tasks._delete_queued_messages(cid)
        except Exception:  # nosec B110
            pass
        _wipe_job_redis_keys(cid)
        try:
            r = get_sync_redis()
            if r:
                r.setex(f"cancel:{cid}", 3600, "1")
        except Exception:  # nosec B110
            pass

    return tasks_cancelled, rq_cancelled, pipe_cancelled


async def cmd_cancelall(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Cancel ALL queued/in-flight jobs in both pipelines (admin only).

    Clears the whole queue — every user's progress tasks, RQ jobs and
    BigFile pipeline jobs.  Requires an explicit ``/cancelall confirm`` so
    an accidental tap can't wipe every job at once.
    """
    await _track_user_session(update, "/cancelall")
    if not config.is_admin_user(getattr(update.effective_user, "id", None)):
        await update.effective_message.reply_text(
            "Unauthorized: admin only"
        )
        return
    uid = getattr(update.effective_user, "id", None)
    args = context.args if hasattr(context, "args") else []
    if not (args and args[0].strip().lower() == "confirm"):
        confirm_kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "\u2705 Yes, cancel all",
                        callback_data=f"cancelall_confirm:{uid}",
                    )
                ]
            ]
        )
        await update.effective_message.reply_text(
            "⚠️ This will cancel ALL queued/running jobs for ALL users "
            "(documents, batches, URL jobs, and large-file pipeline jobs).\n\n"
            "Reply with /cancelall confirm, or tap the button below.",
            reply_markup=confirm_kb,
        )
        return
    tasks_cancelled, rq_cancelled, pipe_cancelled = (
        await _cancel_all_stalled()
    )
    total = tasks_cancelled + rq_cancelled + pipe_cancelled
    if total:
        bits = []
        if rq_cancelled:
            bits.append(f"{rq_cancelled} queued/running")
        if pipe_cancelled:
            bits.append(f"{pipe_cancelled} pipeline")
        if tasks_cancelled:
            bits.append(f"{tasks_cancelled} progress")
        await update.effective_message.reply_text(
            f"✅ Cancelled {total} job(s): " + ", ".join(bits) + "."
        )
    else:
        await update.effective_message.reply_text(
            "✅ Nothing to cancel \u2014 the queue is empty."
        )


async def handle_cancelall_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """First tap on the /status cancel-all button: show a confirmation."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_admin_user(uid):
        await query.answer("Admin only", show_alert=True)
        return
    try:
        await query.answer()
    except Exception:  # nosec B110
        pass
    confirm_kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\u2705 Yes, cancel all",
                    callback_data=f"cancelall_confirm:{uid}",
                ),
                InlineKeyboardButton(
                    "\u274c No",
                    callback_data=f"cancelall_abort:{uid}",
                ),
            ]
        ]
    )
    try:
        await query.edit_message_text(
            "⚠️ Cancel ALL queued/running jobs for ALL users?",
            reply_markup=confirm_kb,
        )
    except Exception:
        logger.exception("cancelall: failed to show confirmation")


async def handle_cancelall_confirm_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Second tap: actually cancel the whole queue (admin, same-user only)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_admin_user(uid):
        await query.answer("Admin only", show_alert=True)
        return
    try:
        armer = int(str(query.data).split(":", 1)[1])
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can confirm.", show_alert=True
        )
        return
    # Answer immediately so the button never shows as stuck while the
    # blocking cancellation work runs; the result lands in the edit below.
    try:
        await query.answer()
    except Exception:  # nosec B110
        pass
    await _track_user_session(update, "/cancelall")
    try:
        tasks_cancelled, rq_cancelled, pipe_cancelled = (
            await _cancel_all_stalled()
        )
    except Exception:
        logger.exception("cancelall: confirm-callback cancellation failed")
        try:
            await query.answer("Something went wrong", show_alert=True)
        except Exception:  # nosec B110
            pass
        return
    total = tasks_cancelled + rq_cancelled + pipe_cancelled
    header = (
        f"✅ Cancelled {total} job(s)."
        if total
        else "✅ Nothing was cancelled."
    )
    # Refresh the /status message with the updated job list instead of a
    # one-line confirmation: after a successful cancel the list is (usually)
    # empty, but any jobs that could not be cancelled stay visible with the
    # cancel-all button re-attached. An empty keyboard (for None) removes the
    # stale confirmation buttons from the message.
    text, kb = _cancel_status_reply(uid, header)
    try:
        await query.edit_message_text(
            text,
            reply_markup=kb if kb is not None else InlineKeyboardMarkup([]),
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("cancelall: failed to edit refreshed status message")


async def handle_cancelall_stale_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Old unsuffixed cancelall buttons from before the same-user binding."""
    query = update.callback_query
    if query is None:
        return
    try:
        await query.answer(
            "This button is outdated \u2014 run /cancelall instead", show_alert=True
        )
    except Exception:  # nosec B110
        pass


async def handle_cancelall_abort_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Second tap on the confirmation: keep everything (admin, same-user)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_admin_user(uid):
        await query.answer("Admin only", show_alert=True)
        return
    try:
        armer = int(str(query.data).split(":", 1)[1])
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can abort it.", show_alert=True
        )
        return
    await _track_user_session(update, "/cancelall")
    try:
        await query.answer("Nothing was cancelled")
    except Exception:  # nosec B110
        pass
    try:
        # Empty keyboard: clear the stale confirmation buttons.
        await query.edit_message_text(
            "✅ Nothing was cancelled.", reply_markup=InlineKeyboardMarkup([])
        )
    except Exception:  # nosec B110
        pass


async def handle_canceljob_arm_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Tap on a 'Queued...' reply's ❌ Cancel button: arm the confirmation.

    Same-user bound to the person who QUEUED the job (embedded in the
    callback data) so another group member can't trigger the flow. Replies a
    fresh confirmation message with ✅/❌ buttons, reusing the existing
    canceljob_confirm/canceljob_abort handlers, and removes the queued
    message's button so it can't be re-armed.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        parts = str(query.data).split(":", 2)
        armer = int(parts[1])
        job_id = parts[2]
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who queued this job can cancel it.",
            show_alert=True,
        )
        return
    if len(job_id) < 4:
        await query.answer("Invalid job id", show_alert=True)
        return
    # Consistent with cmd_canceljob's arm: don't offer to cancel a job that
    # already finished (the confirm handler would only say 'not found').
    chat_id = query.message.chat.id if query.message else None
    if not _resolve_cancel_targets(job_id, uid, chat_id):
        try:
            await query.answer(
                "This job is no longer active.", show_alert=True
            )
        except Exception:  # nosec B110
            pass
        return
    # Answer immediately so the button never shows as stuck.
    try:
        await query.answer()
    except Exception:  # nosec B110
        pass
    await _track_user_session(update, "/canceljob")
    confirm_kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Yes, cancel job",
                    callback_data=f"canceljob_confirm:{uid}:{job_id[:32]}",
                ),
                InlineKeyboardButton(
                    "❌ No",
                    callback_data=f"canceljob_abort:{uid}:{job_id[:32]}",
                ),
            ]
        ]
    )
    # Remove the queued reply's button so it can't be re-armed (the confirm
    # flow edits the NEW confirmation message, which is never deleted by the
    # cancel cleanup — only the original 'Queued...' message is).
    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
    except Exception as e:
        logger.debug(
            "canceljob: could not remove queued cancel button: %s", e
        )
    try:
        if query.message is not None:
            await query.message.reply_text(
                f"⚠️ Cancel job `{job_id[:8]}`?",
                reply_markup=confirm_kb,
                parse_mode="Markdown",
            )
    except Exception:
        logger.exception("canceljob: failed to show queue-cancel confirmation")


async def handle_canceljob_confirm_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Confirm tap on the /canceljob prompt: cancel the single job (same-user only)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        parts = str(query.data).split(":", 2)
        armer = int(parts[1])
        job_id = parts[2]
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can confirm.", show_alert=True
        )
        return
    if len(job_id) < 4:
        await query.answer("Invalid job id", show_alert=True)
        return
    # Answer immediately so the button never shows as stuck while the
    # blocking cancellation work runs; the result lands in the edit below.
    try:
        await query.answer()
    except Exception:  # nosec B110
        pass
    await _track_user_session(update, "/canceljob")
    chat_id = query.message.chat.id if query.message else None
    try:
        owned, actions = await _do_cancel_job(job_id, uid, chat_id)
    except Exception:
        logger.exception("canceljob: confirm-callback cancellation failed")
        try:
            await query.answer("Something went wrong", show_alert=True)
        except Exception:  # nosec B110
            pass
        return
    if owned:
        header = "✅ Cancelled: " + ", ".join(actions)
    else:
        header = (
            f"No active job found for you with id `{job_id}`. "
            "It may have already finished."
        )
    # Show the refreshed /status job list below the result (same treatment as
    # cancel-all): the cancel-all button re-appears when jobs remain, and an
    # empty keyboard clears the stale confirmation buttons otherwise.
    text, kb = _cancel_status_reply(uid, header)
    try:
        await query.edit_message_text(
            text,
            reply_markup=kb if kb is not None else InlineKeyboardMarkup([]),
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("canceljob: failed to edit confirmation result")


async def handle_canceljob_abort_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Abort tap on the /canceljob prompt: keep the job (same-user only)."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        armer = int(str(query.data).split(":", 2)[1])
    except Exception:
        await query.answer("Invalid confirmation", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who started this can abort it.", show_alert=True
        )
        return
    await _track_user_session(update, "/canceljob")
    try:
        await query.answer("Nothing was cancelled")
    except Exception:  # nosec B110
        pass
    try:
        # Empty keyboard: clear the stale confirmation buttons.
        await query.edit_message_text(
            "✅ Nothing was cancelled.", reply_markup=InlineKeyboardMarkup([])
        )
    except Exception:  # nosec B110
        pass


async def handle_ctx_thumb_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🖼 Thumbnail button on the input context menu: ``ctxthumb:<uid>:<token>``.

    Consumes the pending record atomically (``ctxfile:<token>``) and enqueues
    ``process_document_job`` — the normal thumbnail pipeline, now running only
    when the user asks for it (no more auto-processing on send).  Same-user
    bound.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "ctxthumb":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who sent the file can process it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, True, "ctxfile")
    if not rec:
        await query.answer(
            "This menu has expired. Send the file again.", show_alert=True
        )
        return
    await _track_user_session(update, "ctx_thumbnail")
    chat_id = rec.get("chat_id")
    filename = rec.get("filename") or "file"
    if not chat_id:
        await query.answer(
            "This action is invalid. Send the file again.", show_alert=True
        )
        return
    # ── Already-thumbed / already-processed gate (skip the job entirely) ──
    # Cached validator: a PDF that ships an embedded thumbnail needs nothing
    # re-added.  Cached record: the same CONTENT (resolved via the file's
    # Telegram file_unique_id) was already delivered — re-send the cached copy
    # instead of queueing a new job (even if the user deleted the bot's
    # earlier messages).
    if rec.get("file_unique_id"):
        _fchecks = _get_pdf_checks(rec.get("file_unique_id"))
        if _fchecks is not None and _fchecks.get("has_thumb") is True:
            _msg = "\u2705 This PDF already has a thumbnail — nothing to add."
            try:
                await query.answer(_msg)
            except Exception:  # nosec B110
                pass
            await _replace_tapped_text(query, _msg, InlineKeyboardMarkup([]))
            return
        _rec = get_processed_by_file_unique_id(rec.get("file_unique_id"))
        if _rec:
            _entry = (_rec.get("ops") or {}).get("thumb")
            if _entry is not None:
                if _entry.get("status") == "skipped":
                    _msg = (
                        "\u2705 This PDF already has a thumbnail — "
                        "nothing to add."
                    )
                    try:
                        await query.answer(_msg)
                    except Exception:  # nosec B110
                        pass
                    await _replace_tapped_text(
                        query, _msg, InlineKeyboardMarkup([])
                    )
                    return
                if await asyncio.to_thread(
                    _resend_cached_result,
                    chat_id,
                    rec.get("file_unique_id"),
                    filename,
                    "thumb",
                    armer,
                    "\U0001f5bc\ufe0f Here is your file (cached result — "
                    "already processed).",
                ):
                    _msg = (
                        "\u267b\ufe0f Already processed — re-sent the cached "
                        "result. No new job was started."
                    )
                    try:
                        await query.answer(_msg)
                    except Exception:  # nosec B110
                        pass
                    await _replace_tapped_text(
                        query, _msg, InlineKeyboardMarkup([])
                    )
                    return
                # Cached copy expired (Telegram dropped the file): fall
                # through and process fresh so the user still gets the file.
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before this job's
    # worker run (heals transient pfuid write failures).
    _warm_fuid_binding(rec.get("file_unique_id"))
    # job_timeout > RQ's 180s default: a large file's userbot download +
    # thumbnail pass can legitimately outlive the death penalty.
    ok = await asyncio.to_thread(
        enqueue_job,
        "process_document_job",
        chat_id,
        rec.get("file_id"),
        filename,
        rec.get("mime", ""),
        rec.get("file_unique_id"),
        rec.get("message_id"),
        rec.get("forward_info"),
        rec.get("file_size"),
        armer,
        owner_user_id=armer,
        job_timeout=1800,
    )
    if ok:
        try:
            await query.answer("\U0001f5bc\ufe0f Thumbnail queued")
        except Exception:  # nosec B110
            pass
        # Replace the tapped message in place when it's a text message (input
        # context menu -> no leftover prompt); media-message buttons fall back
        # inside the helper to clear-button + new text message.
        _qid = await _replace_tapped_text(
            query,
            f"\U0001f5bc\ufe0f Building the thumbnail for "
            f"`{safe_code_span(filename)}`...\n"
            f"Job ID: `{ok}` — use /canceljob {ok} to cancel it.",
            _queued_cancel_kb(armer, ok),
        )
        _store_queued_message(ok, chat_id, _qid)
    else:
        try:
            await query.answer(
                "\u274c Couldn't queue the job. Try again in a moment.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass


async def handle_ctx_thumb_ocr_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🔎+🖼 OCR & Thumbnail button on the input context menu: ``ctxthumbocr:<uid>:<token>``.

    All-in-one for PDFs: consumes the pending record atomically
    (``ctxfile:<token>``) and enqueues BOTH ``process_document_job`` (cover
    thumbnail) and ``ocr_job`` (searchable PDF / plain text) in one tap — so
    there is no standalone OCR button for PDFs.  Same-user bound.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "ctxthumbocr":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who sent the file can process it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, True, "ctxfile")
    if not rec:
        await query.answer(
            "This menu has expired. Send the file again.", show_alert=True
        )
        return
    await _track_user_session(update, "ctx_thumb_ocr")
    chat_id = rec.get("chat_id")
    filename = rec.get("filename") or "file"
    if not chat_id:
        await query.answer(
            "This action is invalid. Send the file again.", show_alert=True
        )
        return
    # ── Already-processed gate: queue only the parts not yet done ──
    # Re-sends of the same CONTENT (resolved via the file's Telegram
    # file_unique_id) skip the parts already delivered (cached file_id
    # re-sent instead of a new job) and the parts the PDF validator already
    # proved pointless (embedded thumb / text layer).
    _fchecks = _get_pdf_checks(rec.get("file_unique_id"))
    _f_thumb = _fchecks.get("has_thumb") if _fchecks is not None else None
    _rec = (
        get_processed_by_file_unique_id(rec.get("file_unique_id"))
        if rec.get("file_unique_id")
        else None
    )
    _ops = (_rec or {}).get("ops") or {}
    _thumb_entry = _ops.get("thumb")
    _ocr_entry = _ops.get("ocr")
    _thumb_handled = _thumb_entry is not None or _f_thumb is True
    # ``has_text_layer`` no longer counts as "OCR handled": an already-
    # searchable PDF still needs the OCR job — the worker now DELIVERS the
    # searchable file back in one tap instead of messaging "nothing to add".
    # Only a real cached delivery (a done ``ocr`` entry) skips the enqueue.
    _ocr_handled = _ocr_entry is not None
    if _thumb_handled and _ocr_handled:
        # Both parts are already done: re-send one cached copy if available.
        _resent = False
        if _thumb_entry and _thumb_entry.get("status") == "done":
            _resent = await asyncio.to_thread(
                _resend_cached_result,
                chat_id,
                rec.get("file_unique_id"),
                filename,
                "thumb",
                armer,
                "\U0001f5bc\ufe0f Here is your file (cached result — "
                "already processed).",
            )
        if not _resent and _ocr_entry and _ocr_entry.get("status") == "done":
            _resent = await asyncio.to_thread(
                _resend_cached_result,
                chat_id,
                rec.get("file_unique_id"),
                filename,
                "ocr",
                armer,
                "\U0001f50e Here is the cached OCR result (already "
                "processed).",
            )
        _msg = (
            "\u267b\ufe0f Already processed — re-sent the cached result, "
            "no new job."
            if _resent
            else "\u2705 Already processed — nothing new to add."
        )
        try:
            await query.answer(_msg)
        except Exception:  # nosec B110
            pass
        await _replace_tapped_text(query, _msg, InlineKeyboardMarkup([]))
        return
    _want_thumb = not _thumb_handled
    _want_ocr = not _ocr_handled
    # Cached text-layer check: an already-searchable PDF's OCR "output" is
    # the file itself (the worker delivers it back, no engine needed).
    _already_searchable = (
        filename.lower().endswith(".pdf")
        and _fchecks is not None
        and _fchecks.get("has_text_layer") is True
    )
    # OCR target: honor the user's pinned default; else, when the cached
    # text-layer check already proves the PDF is searchable, enqueue
    # target="pdf" — the worker delivers the already-searchable file back in
    # one tap (no OCR engine needed), so the engine check is skipped; else
    # searchable PDF when the engine is available, else plain text.
    _target = (get_user_setting(armer, "ocr_target", "") or "").lower()
    if _target not in ("pdf", "txt"):
        _target = (
            "pdf"
            if (_already_searchable or ocr_pdf_available())
            else "txt"
        )
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before the worker
    # runs (heals transient pfuid write failures).
    _warm_fuid_binding(rec.get("file_unique_id"))
    ok_thumb = None
    if _want_thumb:
        ok_thumb = await asyncio.to_thread(
            enqueue_job,
            "process_document_job",
            chat_id,
            rec.get("file_id"),
            filename,
            rec.get("mime", ""),
            rec.get("file_unique_id"),
            rec.get("message_id"),
            rec.get("forward_info"),
            rec.get("file_size"),
            armer,
            owner_user_id=armer,
            job_timeout=1800,
        )
    # Double-delivery guard: when the thumbnail job actually queued AND the
    # OCR target is the deliver-back (already-searchable PDF → target=pdf),
    # the thumbnail job will deliver the file back with its cover — re-running
    # ocr_job would re-send identical bytes.  Suppress it so the all-in-one
    # tap yields exactly one delivery.  (target=txt still runs: it produces a
    # different artifact, the extracted text.)  Only suppressed once the thumb
    # enqueue succeeded — a failed thumb enqueue keeps the OCR deliver-back
    # as the fallback, so the tap never ends with nothing queued.
    _ocr_suppressed_dupe = False
    if (
        _want_thumb
        and ok_thumb is not None
        and _want_ocr
        and _target == "pdf"
        and _already_searchable
    ):
        _want_ocr = False
        _ocr_suppressed_dupe = True
    ok_ocr = None
    if _want_ocr:
        ok_ocr = await asyncio.to_thread(
            enqueue_job,
            "ocr_job",
            chat_id,
            rec.get("file_id"),
            filename,
            armer,
            rec.get("file_unique_id"),
            rec.get("message_id"),
            None,  # forward_info is not stored in the pending record
            rec.get("file_size"),
            owner_user_id=armer,
            source_chat_id=rec.get("source_chat_id"),
            target=_target,
            job_timeout=7200,
        )
    if ok_thumb or ok_ocr:
        _queued_parts = []
        _kb_rows: list[list[InlineKeyboardButton]] = []
        if ok_thumb:
            _queued_parts.append(f"🖼 Thumbnail — `{ok_thumb}`")
            _kb_t = _queued_cancel_kb(armer, ok_thumb)
            if _kb_t:
                _kb_rows.extend(_kb_t.inline_keyboard)
        if ok_ocr:
            _queued_parts.append(f"🔎 OCR — `{ok_ocr}`")
            _kb_o = _queued_cancel_kb(armer, ok_ocr)
            if _kb_o:
                _kb_rows.extend(_kb_o.inline_keyboard)
        _skipped_parts = []
        if not _want_thumb:
            _skipped_parts.append("\U0001f5bc\ufe0f thumbnail already done")
        if not _want_ocr:
            _skipped_parts.append(
                "\U0001f50e OCR skipped — already searchable "
                "(delivered with the thumbnail)"
                if _ocr_suppressed_dupe
                else "\U0001f50e OCR already done"
            )
        _failed_part = (ok_thumb is None and _want_thumb) or (
            ok_ocr is None and _want_ocr
        )
        _msg = (
            f"\U0001f50e\U0001f5bc\ufe0f Building thumbnail + OCR for "
            f"`{safe_code_span(filename)}`...\n"
            + "\n".join(_queued_parts)
            + (
                f"\n_skipping: {', '.join(_skipped_parts)}._"
                if _skipped_parts
                else ""
            )
            + (
                "\n\u26a0\ufe0f One of the jobs failed to queue — try again."
                if _failed_part
                else ""
            )
        )
        try:
            await query.answer(
                "\U0001f50e\U0001f5bc\ufe0f Thumbnail + OCR queued"
            )
        except Exception:  # nosec B110
            pass
        # Explicit empty keyboard (not None) when no cancel rows exist so the
        # tapped menu's buttons are fully removed either way (PTB omits the
        # reply_markup field for None, leaving the old row stuck).
        _qid = await _replace_tapped_text(
            query,
            _msg,
            InlineKeyboardMarkup(_kb_rows) if _kb_rows else InlineKeyboardMarkup([]),
        )
        # Store the confirmation under the LONGER-running job (OCR) so it is
        # not deleted when the thumbnail finishes first; the second job's
        # _delete_queued_messages is then a no-op on the missing record.
        if ok_ocr:
            _store_queued_message(ok_ocr, chat_id, _qid)
        elif ok_thumb:
            _store_queued_message(ok_thumb, chat_id, _qid)
    else:
        try:
            await query.answer(
                "\u274c Couldn't queue the jobs. Try again in a moment.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass


async def _replace_tapped_text(
    query, text: str, reply_markup
) -> int | None:
    """Replace a tapped callback message's text in place when possible.

    Input context menus are plain TEXT messages, so the action confirmation
    or picker replaces the menu text directly — no leftover "what would you
    like to do?" prompt.  Buttons on DELIVERED files sit on MEDIA messages
    that Telegram cannot text-edit (400), so for those the helper clears the
    button and posts a fresh text message instead.  Returns the message_id
    that now shows ``text`` (or None when nothing could be sent).

    ``reply_markup`` is the keyboard to show; ``None`` is treated as "clear
    the whole context builder" (explicit ``InlineKeyboardMarkup([])``).  PTB
    omits the reply_markup field entirely when it's None, which would leave
    the old button row stuck on the edited message — so None can never be
    passed through as-is.
    """
    # Centralized guard: None would be omitted by PTB and leave stale buttons.
    if reply_markup is None:
        reply_markup = InlineKeyboardMarkup([])
    try:
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
        return getattr(query.message, "message_id", None)
    except Exception:  # nosec B110 - media message: fall back below
        pass
    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup([])
        )
    except Exception:  # nosec B110
        pass
    try:
        _sent = await query.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
        return getattr(_sent, "message_id", None)
    except Exception:  # nosec B110
        return None


async def handle_book_convert_button_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🔁 Convert button on a delivered e-book: reveals the format picker.

    ``callback_data`` is ``bookconvert:<uid>:<token>`` — same-user bound, so a
    tap in a group from anyone but the receiver is rejected.  The pending file
    record (``bookconvert:<token>``) is peeked here and consumed on the final
    format tap, keeping conversion a single-purpose interface.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "bookconvert":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who sent or received the book can convert it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, False, "bookconvert", "ctxfile")
    if not rec:
        try:
            await query.answer(
                "⏰ This Convert button has expired. Send the book again.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass
        # Replace the leftover menu text too (not just clear the buttons) so
        # the input menu doesn't keep asking "what would you like to do?".
        # An explicit empty keyboard (not None) is required: PTB omits the
        # reply_markup field when it's None, which would leave the whole
        # context-builder button row stuck on the message.
        await _replace_tapped_text(
            query,
            "⏰ This choice expired. Send the book again.",
            InlineKeyboardMarkup([]),
        )
        return
    await _track_user_session(update, "book_convert")
    filename = rec.get("filename") or "file"
    kb = _book_conv_kb(armer, token, filename)
    if not kb:
        try:
            await query.answer(
                f"\u274c No convertible target formats for `{safe_code_span(filename)}`.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass
        # Same as above: replace the menu text instead of leaving a dead prompt
        # (explicit empty keyboard so the button row is fully removed).
        await _replace_tapped_text(
            query,
            f"\u274c No convertible target formats for "
            f"`{safe_code_span(filename)}`.",
            InlineKeyboardMarkup([]),
        )
        return
    try:
        await query.answer()
    except Exception:  # nosec B110 - stale/redelivered query
        pass
    # Replace the tapped message in place when it's a text message (input
    # context menu -> no leftover "what would you like to do?" prompt);
    # delivered-book buttons sit on media messages, which fall back inside the
    # helper to clear-button + new text message.
    await _replace_tapped_text(
        query,
        f"\U0001f501 Convert `{safe_code_span(filename)}` to:",
        kb,
    )


async def handle_book_compress_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🗜 Compress PDF button on the book input menu: ``bookcomp:<uid>:<token>``.

    Compress for a book = convert-to-PDF then shrink: enqueues
    ``convert_book_job`` with ``target_fmt="pdf"`` and ``compress=True`` so
    the delivered PDF is already compressed.  Consumes the pending record
    atomically.  Same-user bound.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "bookcomp":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who sent the book can compress it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, True, "bookconvert", "ctxfile")
    if not rec:
        await query.answer(
            "This menu has expired. Send the book again.", show_alert=True
        )
        return
    await _track_user_session(update, "book_compress")
    chat_id = rec.get("chat_id")
    filename = rec.get("filename") or "file"
    if not chat_id:
        await query.answer(
            "This action is invalid. Send the book again.", show_alert=True
        )
        return
    # ── Already-converted gate (compress = convert-to-PDF + shrink) ──
    # Same content re-sent: re-send the cached compressed copy (target
    # "pdf:compress") instead of re-running Calibre + Ghostscript.
    _fuid = rec.get("file_unique_id")
    if _fuid:
        _rec = get_processed_by_file_unique_id(_fuid)
        _conv = get_processed_op(_rec, "convert", "pdf:compress")
        if (
            _conv
            and _conv.get("status") == "done"
            and await asyncio.to_thread(
                _resend_cached_result,
                chat_id,
                _fuid,
                filename,
                "convert",
                armer,
                "\U0001f5dc\ufe0f Here is your compressed book (cached "
                "result — already processed).",
                "pdf:compress",
            )
        ):
            _msg = (
                "\u267b\ufe0f Already compressed — re-sent the cached "
                "result. No new job was started."
            )
            try:
                await query.answer(_msg)
            except Exception:  # nosec B110
                pass
            await _replace_tapped_text(query, _msg, InlineKeyboardMarkup([]))
            return
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before this job's
    # worker run (heals transient pfuid write failures).
    _warm_fuid_binding(rec.get("file_unique_id"))
    # job_timeout covers the full convert-to-PDF + compress + deliver chain.
    _conv_timeout = getattr(config, "BOOK_CONVERT_TIMEOUT_SECONDS", 600)
    ok = await asyncio.to_thread(
        enqueue_job,
        "convert_book_job",
        chat_id,
        rec.get("file_id"),
        filename,
        rec.get("mime", ""),
        "pdf",  # compress target is always PDF
        armer,
        rec.get("file_unique_id"),
        rec.get("message_id"),
        rec.get("forward_info"),
        rec.get("file_size"),
        rec.get("source_chat_id"),
        owner_user_id=armer,
        compress=True,
        # Two-leg conversions (direct + EPUB pivot) can each use the full
        # BOOK_CONVERT_TIMEOUT_SECONDS; give the death penalty headroom.
        job_timeout=2 * int(_conv_timeout) + 300,
    )
    if ok:
        try:
            await query.answer("\U0001f5dc\ufe0f Compress queued")
        except Exception:  # nosec B110
            pass
        _qid = await _replace_tapped_text(
            query,
            f"\U0001f5dc\ufe0f Converting `{safe_code_span(filename)}` to "
            f"PDF and compressing...\n"
            f"Job ID: `{ok}` — use /canceljob {ok} to cancel it.",
            _queued_cancel_kb(armer, ok),
        )
        _store_queued_message(ok, chat_id, _qid)
    else:
        try:
            await query.answer(
                "\u274c Couldn't queue the compression. Try again in a moment.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass


async def handle_book_convert_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Format-picker buttons: ``bookconv:<uid>:<token>:<target>``.

    Enqueues ``convert_book_job`` for the token's file.  Same-user bound; the
    pending record is consumed once, so double-taps are inert.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 4 or parts[0] != "bookconv":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    target = parts[3].lower()
    if uid != armer:
        await query.answer(
            "Only the person who sent or received the book can convert it.",
            show_alert=True,
        )
        return
    pending = _load_pending_token(token, True, "bookconvert", "ctxfile")
    if not pending:
        try:
            await query.edit_message_text(
                "⏰ This choice expired. Send the book again.",
                reply_markup=InlineKeyboardMarkup([]),
            )
        except Exception:  # nosec B110
            pass
        return
    await _track_user_session(update, "book_convert")
    chat_id = pending.get("chat_id")
    filename = pending.get("filename") or "file"
    # ── Already-converted gate (same target format only) ──
    # A re-send of the same book resolves the cached convert record via its
    # Telegram file_unique_id and re-sends the cached copy (Bot API file_id
    # or a userbot forward for big results) instead of queueing a fresh
    # Calibre job.  Only when the cached TARGET matches — converting to a
    # different format must run fresh.
    _fuid = pending.get("file_unique_id")
    if _fuid:
        _rec = get_processed_by_file_unique_id(_fuid)
        _conv = get_processed_op(_rec, "convert", target)
        if (
            _conv
            and _conv.get("status") == "done"
            and await asyncio.to_thread(
                _resend_cached_result,
                chat_id,
                _fuid,
                filename,
                "convert",
                armer,
                "\U0001f4da Here is your converted book (cached result — "
                "already processed).",
                target,
            )
        ):
            try:
                await query.edit_message_text(
                    "\u267b\ufe0f Already converted to this format — re-sent "
                    "the cached result. No new job was started.",
                    reply_markup=InlineKeyboardMarkup([]),
                )
            except Exception:  # nosec B110
                pass
            return
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before this job's
    # worker run (heals transient pfuid write failures).
    _warm_fuid_binding(pending.get("file_unique_id"))
    # job_timeout caps the ENTIRE RQ job — download + convert + deliver — so
    # it must exceed RQ's 180s default AND cover a slow userbot re-download
    # of a large book on top of the Calibre conversion window.
    _conv_timeout = getattr(config, "BOOK_CONVERT_TIMEOUT_SECONDS", 600)
    ok = await asyncio.to_thread(
        enqueue_job,
        "convert_book_job",
        chat_id,
        pending.get("file_id"),
        filename,
        pending.get("mime", ""),
        target,
        armer,
        pending.get("file_unique_id"),
        pending.get("message_id"),
        pending.get("forward_info"),
        pending.get("file_size"),
        pending.get("source_chat_id"),
        owner_user_id=armer,
        # Two-leg conversions (direct + EPUB pivot) can each use the full
        # BOOK_CONVERT_TIMEOUT_SECONDS — give the death penalty headroom
        # (matches handle_book_compress_callback).
        job_timeout=2 * int(_conv_timeout) + 300,
    )
    if ok:
        try:
            await query.edit_message_text(
                f"\U0001f501 Converting `{safe_code_span(filename)}` to "
                f"**{target.upper()}**...\nJob ID: `{ok}` — use /canceljob {ok} to cancel it.",
                reply_markup=_queued_cancel_kb(armer, ok),
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            pass
        _store_queued_message(
            ok,
            chat_id,
            getattr(query.message, "message_id", None),
        )
    else:
        try:
            await query.edit_message_text(
                "\u274c Couldn't queue the conversion. Try again in a moment.",
                reply_markup=InlineKeyboardMarkup([]),
            )
        except Exception:  # nosec B110
            pass


async def handle_compress_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🗜 Compress PDF button on a delivered result: ``compresspdf:<uid>:<token>``.

    Looks up the delivered file in Redis (``bookcompress:<token>``), verifies
    ownership, and enqueues ``compress_pdf_job``.  Same-user bound.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "compresspdf":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who received the file can compress it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, True, "bookcompress", "ctxfile")
    if not rec:
        await query.answer(
            "This compress link has expired. Send the file again.", show_alert=True
        )
        return
    await _track_user_session(update, "compress_pdf")
    chat_id = rec.get("chat_id")
    filename = rec.get("filename") or "file.pdf"
    if not chat_id:
        await query.answer(
            "This compress link is invalid. Send the file again.",
            show_alert=True,
        )
        return
    # ── Already-processed gate (skip the job entirely) ──
    if rec.get("file_unique_id"):
        _rec = get_processed_by_file_unique_id(rec.get("file_unique_id"))
        if _rec:
            _entry = (_rec.get("ops") or {}).get("compress")
            if _entry is not None:
                if _entry.get("status") == "skipped":
                    _msg = (
                        "\u2705 Already processed — this PDF was already "
                        "well-compressed (kept the original)."
                    )
                    try:
                        await query.answer(_msg)
                    except Exception:  # nosec B110
                        pass
                    await _replace_tapped_text(
                        query, _msg, InlineKeyboardMarkup([])
                    )
                    return
                if await asyncio.to_thread(
                    _resend_cached_result,
                    chat_id,
                    rec.get("file_unique_id"),
                    filename,
                    "compress",
                    armer,
                    "\U0001f5dc\ufe0f Here is the cached compressed result "
                    "(already processed).",
                ):
                    _msg = (
                        "\u267b\ufe0f Already processed — re-sent the "
                        "cached result. No new job was started."
                    )
                    try:
                        await query.answer(_msg)
                    except Exception:  # nosec B110
                        pass
                    await _replace_tapped_text(
                        query, _msg, InlineKeyboardMarkup([])
                    )
                    return
                # Cached copy expired: fall through and compress fresh.
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before this job's
    # worker run (heals transient pfuid write failures).
    _warm_fuid_binding(rec.get("file_unique_id"))
    # job_timeout > RQ's 180s default: gs compression of a large PDF can
    # legitimately outlive the death penalty (mirrors convert_book_job).
    ok = await asyncio.to_thread(
        enqueue_job,
        "compress_pdf_job",
        chat_id,
        rec.get("file_id"),
        filename,
        armer,
        rec.get("file_unique_id"),
        rec.get("message_id"),
        rec.get("file_size"),
        owner_user_id=armer,
        source_chat_id=rec.get("source_chat_id"),
        job_timeout=1800,
    )
    if ok:
        try:
            await query.answer("\U0001f5dc\ufe0f Compression queued")
        except Exception:  # nosec B110
            pass
        # Replace the tapped message in place when it's a text message (input
        # context menu -> no leftover prompt); media-message buttons fall back
        # inside the helper to clear-button + new text message.
        _qid = await _replace_tapped_text(
            query,
            f"\U0001f5dc\ufe0f Compressing `{safe_code_span(filename)}`...\n"
            f"Job ID: `{ok}` — use /canceljob {ok} to cancel it.",
            _queued_cancel_kb(armer, ok),
        )
        _store_queued_message(ok, chat_id, _qid)
    else:
        try:
            await query.answer(
                "\u274c Couldn't queue the compression.", show_alert=True
            )
        except Exception:  # nosec B110
            pass


def _ocr_pick_kb(uid: int, token: str) -> InlineKeyboardMarkup:
    """OCR output picker: 📄 Searchable PDF (primary) + 📝 Plain text.

    Callback data ``ocrpick:<uid>:<token>:<fmt>`` — same-user bound; the token
    resolves the pending file record, which is consumed atomically on the
    final tap so double-taps are inert.
    """
    rows = []
    if ocr_pdf_available():
        rows.append(
            [
                InlineKeyboardButton(
                    "\U0001f4c4 Searchable PDF",
                    callback_data=f"ocrpick:{uid}:{token}:pdf",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "\U0001f4dd Plain text (.txt)",
                callback_data=f"ocrpick:{uid}:{token}:txt",
            )
        ]
    )
    # ✖ Cancel: closes the picker without queuing anything.
    rows.append(
        [
            InlineKeyboardButton(
                "\u2716\ufe0f Cancel",
                callback_data=f"ocrcancel:{uid}:{token}",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


_OCR_TARGET_LABELS = {
    "": "\U0001f500 Always ask (picker)",
    "pdf": "\U0001f4c4 Searchable PDF",
    "txt": "\U0001f4dd Plain text (.txt)",
}


def _ocr_settings_kb(uid: int) -> InlineKeyboardMarkup:
    """One-tap OCR default picker for ``/ocr`` (same-user bound)."""
    _cur = (get_user_setting(uid, "ocr_target", "") or "").lower()
    rows = []
    for _val, _label in (
        ("pdf", _OCR_TARGET_LABELS["pdf"]),
        ("txt", _OCR_TARGET_LABELS["txt"]),
        ("", _OCR_TARGET_LABELS[""]),
    ):
        if _val == "pdf" and not ocr_pdf_available():
            continue  # engine missing → don't offer the PDF default
        _mark = " ✅" if _cur == _val else ""
        rows.append(
            [
                InlineKeyboardButton(
                    f"{_label}{_mark}",
                    callback_data=f"ocrset:{uid}:{_val or 'picker'}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "\u2716\ufe0f Close",
                callback_data=f"ocrset:{uid}:close",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


async def cmd_ocr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/ocr [pdf|txt|picker] — set your per-user OCR output default.

    With no argument, shows the current default plus one-tap buttons.  With an
    argument, pins the output so the 🔎 OCR button skips the picker and runs
    directly (Searchable PDF for ``pdf``, plain text for ``txt``, always ask
    for ``picker``).
    """
    await _track_user_session(update, "/ocr")
    uid = getattr(update.effective_user, "id", None)
    if not config.is_user_allowed(uid):
        await update.effective_message.reply_text(
            "Access denied. This bot is private."
        )
        return
    if not ocr_enabled():
        try:
            await update.effective_message.reply_text(
                "\u274c OCR is currently disabled on this instance.",
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            pass
        return
    _arg = ((context.args or [""])[0] or "").lower()
    if _arg in ("pdf", "txt", "picker"):
        _val = "" if _arg == "picker" else _arg
        if _val == "pdf" and not ocr_pdf_available():
            try:
                await update.effective_message.reply_text(
                    "\u274c Searchable PDF isn't available on this instance "
                    "(ocrmypdf not installed).",
                    parse_mode="Markdown",
                )
            except Exception:  # nosec B110
                pass
            return
        set_user_setting(uid, "ocr_target", _val)
        _label = _OCR_TARGET_LABELS.get(_val, _OCR_TARGET_LABELS[""])
        try:
            await update.effective_message.reply_text(
                f"\u2705 OCR default set to **{_label}** \u2014 the \U0001f50e OCR "
                "button will now run it directly (no picker).\n"
                "Change it any time with /ocr or the buttons below.",
                reply_markup=_ocr_settings_kb(uid),
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            pass
        return
    # No/invalid arg → show the current default + one-tap buttons.
    _cur = (get_user_setting(uid, "ocr_target", "") or "").lower()
    _cur_label = _OCR_TARGET_LABELS.get(_cur, _OCR_TARGET_LABELS[""])
    try:
        await update.effective_message.reply_text(
            f"\U0001f50e **OCR output default**\n\n"
            f"Current: **{_cur_label}**\n\n"
            "Tap a button to pin it (the \U0001f50e OCR button then skips the "
            "picker), or use `/ocr pdf`, `/ocr txt`, `/ocr picker`.",
            reply_markup=_ocr_settings_kb(uid),
            parse_mode="Markdown",
        )
    except Exception:  # nosec B110
        pass


async def handle_ocr_set_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """One-tap OCR default buttons: ``ocrset:<uid>:<pdf|txt|picker>``."""
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "ocrset":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    _arg = parts[2].lower()
    if uid != armer:
        await query.answer(
            "Only you can change your OCR default.", show_alert=True
        )
        return
    if _arg == "close":
        # ✖ Close on the /ocr settings menu: dismiss without changing anything.
        try:
            await query.answer("Closed")
        except Exception:  # nosec B110
            pass
        await _track_user_session(update, "ocr_settings_close")
        try:
            await query.edit_message_text(
                "\u2705 Settings closed.",
                reply_markup=InlineKeyboardMarkup([]),
            )
        except Exception:  # nosec B110
            pass
        return
    if _arg not in ("pdf", "txt", "picker"):
        await query.answer("Invalid action", show_alert=True)
        return
    _val = "" if _arg == "picker" else _arg
    if _val == "pdf" and not ocr_pdf_available():
        await query.answer(
            "Searchable PDF isn't available on this instance.", show_alert=True
        )
        return
    set_user_setting(armer, "ocr_target", _val)
    await _track_user_session(update, "ocr_set")
    _label = _OCR_TARGET_LABELS.get(_val, _OCR_TARGET_LABELS[""])
    try:
        await query.edit_message_text(
            f"\u2705 OCR default set to **{_label}** \u2014 the \U0001f50e OCR "
            "button will now run it directly (no picker).",
            reply_markup=_ocr_settings_kb(armer),
            parse_mode="Markdown",
        )
    except Exception:  # nosec B110
        pass


async def handle_ocr_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """🔎 OCR button on a delivered result: ``ocr:<uid>:<token>``.

    Verifies ownership, PEEKS the pending record (``bookocr:<token>``), and
    reveals the output picker (📄 Searchable PDF / 📝 Plain text).  The record
    is consumed atomically only on the final pick, so a double-tap can never
    enqueue the same job twice.  Same-user bound.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 3 or parts[0] != "ocr":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    if uid != armer:
        await query.answer(
            "Only the person who received the file can OCR it.",
            show_alert=True,
        )
        return
    rec = _load_pending_token(token, False, "bookocr", "ctxfile")
    if not rec:
        await query.answer(
            "This OCR link has expired. Send the file again.", show_alert=True
        )
        return
    if not ocr_enabled():
        await query.answer(
            "OCR is currently disabled on this instance.", show_alert=True
        )
        return
    await _track_user_session(update, "ocr")
    # Per-user default (set via /ocr or the settings buttons): when the user
    # pinned an output, skip the picker entirely and run it in one tap.  The
    # record is consumed atomically here so a double-tap can't double-enqueue.
    _default = (get_user_setting(armer, "ocr_target", "") or "").lower()
    if _default in ("pdf", "txt"):
        if _default == "pdf" and not ocr_pdf_available():
            # Pinned to PDF but the engine is missing → tell the user why the
            # picker is reappearing, then fall through to it (record still
            # peeked, never consumed).
            try:
                await query.answer(
                    "Searchable PDF isn't available on this instance — "
                    "showing the picker instead.",
                    show_alert=False,
                )
            except Exception:  # nosec B110
                pass
            _default = ""
        else:
            _rec2 = _load_pending_token(token, True, "bookocr", "ctxfile")
            if not _rec2:
                await query.answer(
                    "This OCR link has expired. Send the file again.",
                    show_alert=True,
                )
                return
            await _enqueue_ocr_job(query, _rec2, armer, _default)
            return
    filename = rec.get("filename") or "file"
    # ── Already-searchable direct PDF: skip the picker, one tap ──────────
    # When the pdfcheck cache (written by the thumbnail/OCR validator over
    # this same immutable content) already proves the PDF carries a searchable
    # text layer, the OCR button's promise is just the file itself — the
    # worker re-sends the searchable PDF back with an explanatory caption,
    # and no OCR engine is needed.  Consumed atomically so a double-tap can't
    # double-enqueue.  Falls through to the picker when the layer is unknown
    # (the worker re-checks and decides there).
    _fchecks = _get_pdf_checks(rec.get("file_unique_id"))
    if (
        filename.lower().endswith(".pdf")
        and _fchecks is not None
        and _fchecks.get("has_text_layer") is True
    ):
        _rec2 = _load_pending_token(token, True, "bookocr", "ctxfile")
        if not _rec2:
            await query.answer(
                "This OCR link has expired. Send the file again.",
                show_alert=True,
            )
            return
        await _enqueue_ocr_job(query, _rec2, armer, "pdf")
        return
    try:
        await query.answer()
    except Exception:  # nosec B110 - stale/redelivered query
        pass
    # Replace the tapped message in place when it's a text message (input
    # context menu -> no leftover prompt); media-message buttons fall back
    # inside the helper to clear-button + new text message.  A None return
    # means even the fallback failed (message deleted elsewhere) — tell the
    # user the reveal didn't stick.
    _revealed = await _replace_tapped_text(
        query,
        f"\U0001f50e OCR `{safe_code_span(filename)}` as:",
        _ocr_pick_kb(armer, token),
    )
    if _revealed is None:
        try:
            await query.answer(
                "This OCR link has expired. Send the file again.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass


async def handle_ocr_pick_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """OCR output picker: ``ocrpick:<uid>:<token>:<fmt>`` (fmt = pdf|txt).

    Consumes the pending record atomically (``bookocr:<token>``) and enqueues
    ``ocr_job`` with ``target=<fmt>`` — 📄 Searchable PDF (ocrmypdf text
    layer) or 📝 plain text (.txt).  Same-user bound; double-taps are inert
    because the record is GETDEL'd on the first tap.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(update.effective_user, "id", None)
    parts = str(query.data or "").split(":")
    if len(parts) != 4 or parts[0] != "ocrpick":
        await query.answer("Invalid action", show_alert=True)
        return
    try:
        armer = int(parts[1])
    except ValueError:
        await query.answer("Invalid action", show_alert=True)
        return
    token = parts[2]
    target = parts[3].lower()
    if uid != armer:
        await query.answer(
            "Only the person who received the file can OCR it.",
            show_alert=True,
        )
        return
    if target not in ("pdf", "txt"):
        await query.answer("Invalid action", show_alert=True)
        return
    rec = _load_pending_token(token, True, "bookocr", "ctxfile")
    if not rec:
        await query.answer(
            "This OCR link has expired. Send the file again.", show_alert=True
        )
        return
    if not ocr_enabled():
        await query.answer(
            "OCR is currently disabled on this instance.", show_alert=True
        )
        return
    if target == "pdf" and not ocr_pdf_available():
        await query.answer(
            "Searchable PDF isn't available on this instance. Use Plain text.",
            show_alert=True,
        )
        return
    await _track_user_session(update, "ocr_pick")
    await _enqueue_ocr_job(query, rec, armer, target)


async def _enqueue_ocr_job(
    query, rec: dict, uid: int, target: str
) -> bool:
    """Enqueue ``ocr_job`` for an already-validated pending record.

    Shared by the format-picker tap and the direct (default-skip) path so both
    post the identical queued reply + cancel button.  ``target`` is "pdf" or
    "txt".  Returns True when the job was queued.
    """
    chat_id = rec.get("chat_id")
    filename = rec.get("filename") or "file"
    if not chat_id:
        try:
            await query.answer(
                "This OCR link is invalid. Send the file again.",
                show_alert=True,
            )
        except Exception:  # nosec B110
            pass
        return False
    # Warm the durable fuid->content_hash index from the worker's pdfcheck
    # binding so the SURFACE fast-path stays alive even before this job's
    # worker run (heals transient pfuid write failures).
    _warm_fuid_binding(rec.get("file_unique_id"))
    # job_timeout > RQ's 180s default: OCR of a multi-page PDF at 200-300 DPI
    # (and ocrmypdf's text-layer pass) can legitimately outlive the death
    # penalty (mirrors convert_book_job).
    ok = await asyncio.to_thread(
        enqueue_job,
        "ocr_job",
        chat_id,
        rec.get("file_id"),
        filename,
        uid,
        rec.get("file_unique_id"),
        rec.get("message_id"),
        None,  # forward_info is not stored in the pending record
        rec.get("file_size"),
        owner_user_id=uid,
        source_chat_id=rec.get("source_chat_id"),
        target=target,
        # job_timeout > RQ's 180s default: OCR of a multi-page PDF at 200-300
        # DPI (plus ocrmypdf's text-layer pass) can outlive the death penalty.
        # E-book sources first run a Calibre conversion (up to
        # BOOK_CONVERT_TIMEOUT_SECONDS) before OCR, so allow generous headroom.
        job_timeout=7200,
    )
    if ok:
        try:
            await query.answer("\U0001f50e OCR queued")
        except Exception:  # nosec B110
            pass
        _label = (
            "building the searchable PDF"
            if target == "pdf"
            else "extracting the text"
        )
        # Replace the tapped message in place when it's a text message (input
        # context menu -> no leftover prompt); media-message buttons fall back
        # inside the helper to clear-button + new text message.
        _qid = await _replace_tapped_text(
            query,
            f"\U0001f50e OCR started — {_label} on "
            f"`{safe_code_span(filename)}`...\n"
            f"Job ID: `{ok}` — use /canceljob {ok} to cancel it.",
            _queued_cancel_kb(uid, ok),
        )
        _store_queued_message(ok, chat_id, _qid)
    else:
        try:
            await query.answer(
                "\u274c Couldn't queue the OCR.", show_alert=True
            )
        except Exception:  # nosec B110
            pass
    return bool(ok)


async def handle_menu_cancel_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """✖ Cancel/Close on a context menu or format picker.

    Handles ``ocrcancel:<uid>:<token>`` (OCR output picker),
    ``bookcancel:<uid>:<token>`` (Convert format picker) and
    ``ctxclose:<uid>:<token>`` (input context menu): consumes the pending
    record atomically (so every option on the menu becomes inert — a stale
    format tap can never enqueue a job afterwards) and replaces the menu
    with a short "cancelled" note.  Same-user bound like every other action.
    """
    query = update.callback_query
    if query is None:
        return
    uid = getattr(query.from_user, "id", None)
    if not config.is_user_allowed(uid):
        await query.answer("Access denied", show_alert=True)
        return
    try:
        parts = str(query.data).split(":", 2)
        armer = int(parts[1])
        token = parts[2]
    except Exception:
        await query.answer("Invalid action", show_alert=True)
        return
    if parts[0] not in ("ctxclose", "ocrcancel", "bookcancel"):
        await query.answer("Invalid action", show_alert=True)
        return
    if uid != armer:
        await query.answer(
            "Only the person who opened this menu can close it.",
            show_alert=True,
        )
        return
    try:
        await query.answer("Cancelled")
    except Exception:  # nosec B110
        pass
    await _track_user_session(update, "ctx_menu_close")
    # Consume the record from whichever store holds it (input menu vs
    # delivered-file buttons) so no option can re-fire afterwards.
    _load_pending_token(
        token, True, "bookconvert", "bookcompress", "bookocr", "ctxfile"
    )
    # Close the context builder FULLY: an explicit empty keyboard (not None)
    # is required — PTB omits reply_markup when it's None, so the old buttons
    # would stay glued to the "Cancelled" message instead of disappearing.
    await _replace_tapped_text(
        query,
        "\u2716\ufe0f Cancelled — nothing was queued.",
        InlineKeyboardMarkup([]),
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
application.add_handler(CommandHandler("cancelall", cmd_cancelall))
application.add_handler(
    CallbackQueryHandler(handle_cancelall_callback, pattern="^cancelall$")
)
application.add_handler(
    CallbackQueryHandler(
        handle_cancelall_confirm_callback, pattern=r"^cancelall_confirm:\d+$"
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_cancelall_abort_callback, pattern=r"^cancelall_abort:\d+$"
    )
)
# /clear_cache confirmation buttons (same-user bound, id in callback data).
application.add_handler(CommandHandler("clear_cache", cmd_clearcache))
application.add_handler(
    CallbackQueryHandler(
        handle_clearcache_confirm_callback,
        pattern=r"^clearcache_confirm:\d+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_clearcache_abort_callback,
        pattern=r"^clearcache_abort:\d+$",
    )
)
# Legacy unsuffixed buttons (pre same-user binding) — tell the user what to do.
application.add_handler(
    CallbackQueryHandler(
        handle_clearcache_stale_callback,
        pattern=r"^clearcache_(confirm|abort)$",
    )
)
# Legacy unsuffixed buttons (pre same-user binding) — tell the user what to do.
application.add_handler(
    CallbackQueryHandler(
        handle_cancelall_stale_callback,
        pattern=r"^cancelall_(confirm|abort)$",
    )
)
# 'Queued...' reply cancel button (arms the confirmation, same-user bound).
application.add_handler(
    CallbackQueryHandler(
        handle_canceljob_arm_callback,
        pattern=r"^canceljob:\d+:\S+$",
    )
)
# /canceljob confirmation buttons (same-user bound, id in callback data).
application.add_handler(
    CallbackQueryHandler(
        handle_canceljob_confirm_callback,
        pattern=r"^canceljob_confirm:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_canceljob_abort_callback,
        pattern=r"^canceljob_abort:\d+:\S+$",
    )
)
# Book conversion: the 🔁 Convert button on delivered e-books, the format
# picker it reveals, and the 🗜 Compress PDF button on delivered results —
# all same-user bound, and conversion never mixes with the thumbnail flow.
application.add_handler(
    CallbackQueryHandler(
        handle_book_convert_button_callback,
        pattern=r"^bookconvert:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_book_convert_callback,
        pattern=r"^bookconv:\d+:\S+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_compress_callback,
        pattern=r"^compresspdf:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_ocr_callback,
        pattern=r"^ocr:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_ocr_pick_callback,
        pattern=r"^ocrpick:\d+:\S+:(pdf|txt)$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_ocr_set_callback,
        pattern=r"^ocrset:\d+:(pdf|txt|picker|close)$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_ctx_thumb_callback,
        pattern=r"^ctxthumb:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_ctx_thumb_ocr_callback,
        pattern=r"^ctxthumbocr:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_book_compress_callback,
        pattern=r"^bookcomp:\d+:\S+$",
    )
)
# ✖ Cancel/Close buttons on the input context menu and the OCR/Convert
# format pickers — same-user bound, consume the pending record, clear the
# menu without queuing anything.
application.add_handler(
    CallbackQueryHandler(
        handle_menu_cancel_callback,
        pattern=r"^ctxclose:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_menu_cancel_callback,
        pattern=r"^ocrcancel:\d+:\S+$",
    )
)
application.add_handler(
    CallbackQueryHandler(
        handle_menu_cancel_callback,
        pattern=r"^bookcancel:\d+:\S+$",
    )
)
application.add_handler(CommandHandler("ocr", cmd_ocr))


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
_watchdog_task = None
_shutdown_event = asyncio.Event()


async def on_startup() -> None:
    global \
        _keep_alive_task, \
        _worker_task, \
        _worker_proc, \
        _cleanup_task, \
        _longpoll_task, \
        _watchdog_task, \
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

    # ── Stale-task watchdog: auto-fail zombie progress trackers ──
    try:
        _wd_interval = int(
            os.getenv("PROGRESS_WATCHDOG_INTERVAL_SECONDS", "300")
        )
        _wd_stale = int(
            os.getenv("PROGRESS_WATCHDOG_STALE_SECONDS", "1800")
        )
        # 0 disables the watchdog; otherwise enforce a sane floor so a
        # misconfigured tiny interval can't hammer Redis/Mongo.
        if _wd_interval > 0:
            _wd_interval = max(60, _wd_interval)

            async def _progress_watchdog_loop():
                logger.info(
                    "Progress watchdog started (interval=%ss, stale=%ss)",
                    _wd_interval,
                    _wd_stale,
                )
                while True:
                    try:
                        await asyncio.wait_for(
                            _shutdown_event.wait(), timeout=_wd_interval
                        )
                        break
                    except TimeoutError:
                        pass
                    except asyncio.CancelledError:
                        break
                    try:
                        _failed = (
                            await progress_tracker.watchdog_stale_tasks(
                                _wd_stale
                            )
                        )
                        if _failed:
                            logger.warning(
                                "Progress watchdog auto-failed %d stale "
                                "task(s): %s",
                                len(_failed),
                                _failed,
                            )
                    except Exception:
                        logger.exception(
                            "Progress watchdog iteration failed"
                        )
                logger.info("Progress watchdog stopped")

            _watchdog_task = asyncio.create_task(_progress_watchdog_loop())
    except Exception as _wd_err:
        logger.warning("Failed to start progress watchdog: %s", _wd_err)


async def on_shutdown() -> None:
    _shutdown_event.set()
    try:
        # Cancel background tasks
        for task, name in [
            (_keep_alive_task, "keep-alive"),
            (_worker_task, "worker-supervisor"),
            (_cleanup_task, "cleanup"),
            (_longpoll_task, "long-poller"),
            (_watchdog_task, "progress-watchdog"),
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
    _seen: set[str] = set()
    # Confirmation labels for URLs successfully enqueued this message — sent
    # as ONE summary reply after the loop instead of one reply per URL.
    _queued_labels: list[str] = []

    for url in urls:
        url = url.rstrip(".,;!?)]")
        if url in _seen:
            continue  # the same URL listed twice in one message
        _seen.add(url)
        # quick check by extension — PDFs and e-books only
        parsed = urlparse(url)
        base = os.path.basename(parsed.path) or "download"
        _url_ext = os.path.splitext(base)[1].lower()
        if not _url_ext:
            base = base + ".pdf"
            _url_ext = ".pdf"
        _is_pdf_url = _url_ext == ".pdf"
        _is_ebook_url = (not _is_pdf_url) and is_book_format(base)
        if not (_is_pdf_url or _is_ebook_url):
            continue  # silently ignore non-book URLs (unchanged behavior)
        if _is_ebook_url and not getattr(
            config, "ENABLE_BOOK_CONVERSION", False
        ):
            try:
                await msg.reply_text(
                    "\U0001f4d5 E-book conversion is currently disabled on this "
                    "instance.\nSend a **PDF** or **image** to get a thumbnail "
                    "cover.",
                    parse_mode="Markdown",
                )
            except Exception:
                logger.exception(
                    "Failed to notify about disabled book conversion"
                )
            continue  # keep processing the other URLs in the message
        chat_id = (
            msg.chat.id if getattr(msg, "chat", None) else msg.chat_id
        )
        if config.REDIS_URL:
            # ── Respect Telegram API rate limits (global 30/s + per-user 1/s) ──
            try:
                await telegram_api_limiter.wait_if_needed(
                    str(getattr(update.effective_user, "id", 0))
                )
            except Exception:  # nosec B110 - throttling is best-effort
                pass
            ok = await asyncio.to_thread(
                enqueue_job,
                "process_url_job",
                chat_id,
                url,
                base,
                user_id,
                owner_user_id=user_id,
                # job_timeout > RQ's 180s default: large URL downloads (PDF or
                # e-book echo) can legitimately outlive the death penalty.
                job_timeout=1800,
            )
            if ok:
                # Truncate pathological URL basenames so a long filename can't
                # balloon the batched confirmation reply.
                _label_base = base if len(base) <= 60 else base[:57] + "..."
                _queued_labels.append(
                    f"`{safe_code_span(_label_base)}` \u2014 "
                    + ("PDF URL" if _is_pdf_url else "e-book URL")
                )
                continue  # process the remaining URLs in the message
            # fall back to inline processing on enqueue failure

        tmpdir = (
            tempfile.mkdtemp(dir=config.TMP_DIR)
            if config.TMP_DIR
            else tempfile.mkdtemp()
        )
        try:
            file_path = os.path.join(tmpdir, base)
            await download_url_to_file(url, file_path)
            _url_file_size = os.path.getsize(file_path)
            _url_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
            if _is_ebook_url:
                # Conversion-only interface: echo the book back with a
                # 🔁 Convert button — never the thumbnail pipeline.
                _convert_uid = user_id if calibre_available() else None
                _book_caption = (
                    "\U0001f4da Here's your book. Tap the Convert button "
                    "to re-format it."
                    if _convert_uid
                    else "\U0001f4da Here's your book."
                )
                if (
                    _url_file_size > _url_limit
                    and _check_userbot_available(user_id)
                ):
                    await _send_with_upload_progress(
                        bot=context.bot,
                        chat_id=chat_id,
                        file_path=file_path,
                        caption=_book_caption,
                        thumb_path=None,
                        user_id=getattr(
                            update.effective_user, "id", None
                        ),
                        filename=base,
                        file_size=_url_file_size,
                        loop=_loop,
                        target_chat_id=BOT_USER_ID or "me",
                    )
                else:
                    with open(file_path, "rb") as f_doc:
                        await asyncio.to_thread(
                            _tg_send_document,
                            config.BOT_TOKEN,
                            chat_id,
                            f_doc,
                            base,
                            None,
                            _book_caption,
                            None,
                            None,
                            _convert_uid,
                        )
                continue  # next URL in the message (finally cleans tmpdir)
            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            create_thumbnail_from_pdf(file_path, thumb_path)
            if _url_file_size > _url_limit and _check_userbot_available(user_id):
                await _send_with_upload_progress(
                    bot=context.bot,
                    chat_id=chat_id,
                    file_path=file_path,
                    caption="Generated thumbnail from URL",
                    thumb_path=thumb_path,
                    user_id=getattr(update.effective_user, "id", None),
                    filename=base,
                    file_size=_url_file_size,
                    loop=_loop,
                    target_chat_id=BOT_USER_ID or "me",
                )
            else:
                _url_task = None
                _url_msg_id = None
                if _url_file_size and _url_file_size > 1024 * 1024:
                    _url_task = progress_tracker.create_task(
                        uuid.uuid4().hex[:12],
                        user_id or 0,
                        base,
                        _url_file_size,
                    )
                    _url_msg_id = await send_progress_update(
                        chat_id, context.bot, _url_task
                    )
                try:
                    await _send_document_via_bot_api(
                        bot=context.bot,
                        chat_id=chat_id,
                        file_path=file_path,
                        filename=base,
                        thumb_path=thumb_path,
                        caption="Generated thumbnail from URL",
                        task=_url_task,
                        progress_msg_id=_url_msg_id,
                        user_id=user_id,
                    )
                except Exception:
                    if _url_task:
                        await progress_tracker.fail_task(
                            _url_task.task_id, "Telegram send failed"
                        )
                    raise
                else:
                    if _url_task:
                        await progress_tracker.complete_task(
                            _url_task.task_id
                        )
                finally:
                    if _url_msg_id:
                        try:
                            await context.bot.delete_message(
                                chat_id=chat_id,
                                message_id=_url_msg_id,
                            )
                        except Exception:  # nosec B110
                            pass
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
        # Continue with the remaining URLs in the message.
        continue

    # One summary confirmation for every successfully queued URL instead of
    # one reply per URL (avoids spam on multi-URL messages).  Best-effort: a
    # failed reply must not surface as an error.
    if _queued_labels:
        _n = len(_queued_labels)
        _plural = "s" if _n != 1 else ""
        _summary = (
            f"\U0001f4e5 Queued {_n} URL{_plural} for background processing; "
            f"I'll send the result{_plural} when ready."
        )
        _summary += "\n\n" + "\n".join(
            f"\u2022 {label}" for label in _queued_labels
        )
        try:
            await msg.reply_text(_summary, parse_mode="Markdown")
        except Exception:  # nosec B110 - confirmation is best-effort
            logger.exception("Failed to send batched URL confirmation")


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
                    BotCommand("status", "Bot status: queue & your jobs"),
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
