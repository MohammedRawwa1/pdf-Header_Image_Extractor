import io
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable

import requests

logger = logging.getLogger(__name__)


def _job_cancelled(job_id: str | None) -> bool:
    """Return True if a cancel flag exists in Redis for this job id.

    Set by /canceljob (``cancel:<job_id>`` key with a 1h TTL). The RQ worker
    and pipeline worker check this flag so in-flight jobs abort cleanly.
    """
    if not job_id:
        return False
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return False
        return bool(r.exists(f"cancel:{job_id}"))
    except Exception:
        return False


def _pipeline_cancel_flag(job_id: str | None) -> bool:
    """Return True if the pipeline job hash has its cancel flag set."""
    if not job_id:
        return False
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return False
        flag = r.hget(f"pdf:job:{job_id}", "cancel")
        return flag in (b"1", "1")
    except Exception:
        return False


class _JobCancelledError(Exception):
    """Raised to abort a BigFilePipeline job mid-flight after /canceljob fires."""


def _check_cancel_flags(job_id: str | None) -> bool:
    """True when either pipeline cancel flag (``cancel:<id>`` or the
    ``pdf:job:<id>`` hash) is set — used by the mid-flight abort checks."""
    return _job_cancelled(job_id) or _pipeline_cancel_flag(job_id)


def _mark_pipeline_hash_status(job_id: str | None, status: str) -> None:
    """Best-effort update of the ``pdf:job:<id>`` hash ``status`` field.

    Kept in sync as jobs finish/cancel so /canceljob's existence checks can
    tell live jobs apart from completed ones (the hash survives up to
    JOB_METADATA_TTL after completion).  Terminal statuses are ``done``,
    ``s3_fallback``, ``too_large``, ``cancelled``, ``failed``, ``error``,
    ``already_processed``.
    """
    if not job_id:
        return
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return
        # Never resurrect a wiped hash (e.g. one already removed by
        # _do_cancel_job's cleanup): only update an existing job record.
        if not r.exists(f"pdf:job:{job_id}"):
            return
        r.hset(f"pdf:job:{job_id}", mapping={"status": status})
    except Exception:  # nosec B110
        pass


# Direct Telegram Bot HTTP API calls (getFile, file downloads, forwardMessage,
# sendMessage/editMessageText/deleteMessage, sendDocument and progress) live in
# utils/tg_http.py — imported below.  Only S3-presigned and user-supplied URL
# downloads still use requests here.


def _live_edit(
    state: dict,
    chat_id: int,
    filename: str,
    stage: str,
    detail: str,
    recv: int,
    total: int,
) -> None:
    """Throttled live edit of the progress message (>=2% jumps or >=2s apart)."""
    if not total:
        return
    if not state.get("msg_id"):
        # No progress message to edit (the initial post failed); posting one
        # here would create an untracked message that cleanup couldn't remove.
        return
    pct = int(recv * 100 / total)
    now = time.time()
    if pct - state["last_pct"] < 2 and now - state["last_t"] < 2.0:
        return
    state["last_pct"] = pct
    state["last_t"] = now
    new_id = _tg_send_progress(
        chat_id,
        filename,
        stage,
        detail=detail,
        file_size=total,
        message_id=state["msg_id"],
        progress_pct=pct,
    )
    if new_id:
        state["msg_id"] = new_id


# ── Transient-message auto-delete helpers ────────────────────────
# bot.py records the "Queued your file..." confirmation message under
# ``queued_msg:<job_id>`` when a job is enqueued.  Once the output has been
# delivered, the worker deletes that confirmation AND the live progress
# message so the chat only keeps the final result.

QUEUED_MSG_KEY = "queued_msg:{}"
QUEUED_MSG_TTL = 7 * 24 * 3600


def _append_queued_message(job_id: str, message_id: int) -> None:
    """Append a message id to a job's auto-delete record (best-effort).

    Used when an RQ job hands off to the BigFilePipeline worker so the
    pipeline's cleanup also removes the RQ worker's progress message.
    """
    if not job_id or not message_id:
        return
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return
        key = QUEUED_MSG_KEY.format(job_id)
        raw = r.get(key)
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {}
        if not data.get("chat_id"):
            return  # no valid record to extend — don't create a half-baked one
        ids = list(data.get("message_ids", []))
        if message_id not in ids:
            ids.append(message_id)
        r.setex(
            key,
            QUEUED_MSG_TTL,
            json.dumps({"chat_id": data.get("chat_id"), "message_ids": ids}),
        )
    except Exception:  # nosec B110
        pass


def _delete_queued_messages(job_id: str | None) -> None:
    """Delete the recorded "Queued..." message(s) for a job (best-effort)."""
    if not job_id:
        return
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return
        key = QUEUED_MSG_KEY.format(job_id)
        raw = r.get(key)
        if not raw:
            return
        try:
            data = json.loads(raw)
            chat_id = data.get("chat_id")
            for mid in data.get("message_ids", []):
                _tg_delete_message(chat_id, mid)
        finally:
            try:
                r.delete(key)
            except Exception:  # nosec B110
                pass
    except Exception:  # nosec B110
        pass


def _clear_queued_message_buttons(job_id: str | None) -> None:
    """Strip the cancel button from a job's recorded "Queued..." message(s).

    Used when an RQ job hands off to the BigFilePipeline: the RQ job is done
    and its cancel button would be stale (the live job now runs under the
    pipeline job id). The handoff progress message carries the pipeline's own
    live cancel button instead, so a file never shows two cancel controls.
    Best-effort — the queued_msg record is left intact for delivery cleanup.
    """
    if not job_id:
        return
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return
        raw = r.get(QUEUED_MSG_KEY.format(job_id))
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:  # nosec B112 - non-JSON record is not ours to fix
            return
        chat_id = data.get("chat_id")
        for mid in data.get("message_ids", []):
            _tg_edit_message_reply_markup(chat_id, mid)
    except Exception:  # nosec B110
        pass


def _transfer_queued_messages(
    src_job_id: str | None, dst_job_id: str | None
) -> None:
    """Move the auto-delete record from one job id to another (RQ -> pipeline)."""
    if not src_job_id or not dst_job_id or src_job_id == dst_job_id:
        return
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if not r:
            return
        src_key = QUEUED_MSG_KEY.format(src_job_id)
        raw = r.get(src_key)
        if not raw:
            return
        try:
            r.setex(QUEUED_MSG_KEY.format(dst_job_id), QUEUED_MSG_TTL, raw)
        finally:
            r.delete(src_key)
    except Exception:  # nosec B110
        pass


def _deliver_result_via_userbot(
    chat_id: int,
    file_path: str,
    filename: str,
    caption: str,
    thumb_path: str | None,
    user_id: int | None,
) -> tuple | None:
    """Deliver a too-large result via the userbot using the user's session.

    Sends to the bot's user ID so the file lands in the requesting user's DM
    with the bot (mirroring the web process's big-file delivery); falls back
    to the userbot's Saved Messages ('me') when the bot entity can't be
    resolved.

    Returns ``(sent_message, source_chat)`` on delivery, where
    ``sent_message`` is the userbot's sent message (carries the delivered
    copy's id) and ``source_chat`` is the chat the copy landed in (the DM as
    the userbot sees it, or ``'me'`` for Saved Messages) — enough for a later
    chat-based download of the delivered copy.  ``None`` when no userbot
    session is available (caller then falls back to S3 URL / error message).
    """
    import asyncio as _asyncio

    try:
        from utils.userbot_downloader import _get_bot_user_id
        from utils.userbot_uploader import (
            send_file_via_userbot_with_fallback,
        )
    except Exception:
        return None
    target = _get_bot_user_id() or "me"
    # A blank/white (or missing) preview is worse than none — drop it so the
    # userbot delivers without a white cover thumbnail.
    if not thumbnail_is_usable(thumb_path):
        logger.info(
            "worker: skipping unusable thumbnail for %s "
            "(userbot delivery)",
            filename,
        )
        thumb_path = None
    try:
        _sent, _used = _asyncio.run(
            send_file_via_userbot_with_fallback(
                chat_id=target,
                file_path=file_path,
                caption=caption,
                thumb_path=thumb_path,
                progress_callback=None,
                user_id=user_id,
            )
        )
    except Exception:
        # Never let a userbot send failure escape: the caller degrades to the
        # S3 URL / error-message fallback instead of failing the whole job.
        logger.exception(
            "worker: userbot result delivery raised (chat=%s target=%s user_id=%s)",
            chat_id,
            target,
            user_id,
        )
        return None
    if not _sent:
        return None
    # Where the copy actually landed, as the userbot resolves it: Saved
    # Messages when the 'me' fallback fired, else the sent message's own chat
    # (the DM with the bot for the primary target).
    if str(_used) == "me":
        _src = "me"
    else:
        _msg_chat = getattr(_sent, "chat_id", None)
        if _msg_chat is None:
            _msg_chat = getattr(getattr(_sent, "chat", None), "id", None)
        _src = _msg_chat if _msg_chat is not None else target
    return (_sent, _src)


def _publish_thumb_ready(
    file_unique_id: str | None,
    is_pdf: bool,
    thumb_path: str | None = None,
    thumb_bytes: bytes | None = None,
) -> None:
    """Best-effort: cache ``has_thumb=True`` after a PDF's rendered preview was
    actually DELIVERED to Telegram.

    The pdfcheck ``has_thumb`` flag short-circuits repeat sends at enqueue
    time, so it must only flip once the thumbnail really reached the user — a
    failed send must never mark the file as "already has a thumbnail".  PDFs
    only, usable preview only (``thumb_bytes`` is already blank-filtered by
    the caller; ``thumb_path`` is re-checked with
    :func:`thumbnail_is_usable`).
    """
    if not is_pdf:
        return
    if thumb_bytes is not None:
        if not thumb_bytes:
            return
    elif not thumbnail_is_usable(thumb_path):
        return
    # ``_store_pdf_checks`` swallows its own errors — best-effort by design.
    _store_pdf_checks(file_unique_id, has_thumb=True)


def _safe_local_filename(filename: str) -> str:
    """Filesystem-safe local name for a user-supplied filename.

    Defense-in-depth on top of the bot-side ``_sanitize_filename`` (which
    already strips separators and ``..``): a filename such as ``../x.epub``
    or ``a/b.epub`` must never escape the job's private tmpdir when joined.
    Basenames the name, drops control characters, rejects dots-only names,
    and caps the length (preserving the extension) so every worker download
    stays inside its own temp dir.
    """
    name = (filename or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    name = "".join(c for c in name if c >= " ")
    if not name or set(name) <= {"."}:
        return "file"
    if len(name) > 200:
        _base, _ext = os.path.splitext(name)
        _ext = _ext[:20]
        name = (_base[: 200 - len(_ext)] or "file") + _ext
    return name


def _cleanup_after_success(
    chat_id: int,
    job_id: str | None,
    progress_msg_id: int | None,
    skip_queued_delete: bool = False,
) -> None:
    """Delete the transient progress + "Queued..." messages after delivery.

    ``skip_queued_delete`` keeps the "Queued..." confirmation (used by batch
    items; the batch job deletes its own confirmation once all items are done).
    """
    try:
        _tg_delete_message(chat_id, progress_msg_id)
    except Exception:  # nosec B110
        pass
    if not skip_queued_delete:
        _delete_queued_messages(job_id)


def _cleanup_after_failure(
    chat_id: int,
    job_id: str | None,
    progress_msg_id: int | None,
    skip_queued_delete: bool = False,
) -> None:
    """Delete transient progress + "Queued..." messages after a failed job.

    Mirrors ``_cleanup_after_success`` so a failure leaves no stale progress
    bar or confirmation in the chat; the caller sends its own user-facing
    error notification.
    """
    _cleanup_after_success(chat_id, job_id, progress_msg_id, skip_queued_delete)


def _cancel_pipeline_cleanup(
    job_id: str | None,
    chat_id: int | None,
    progress_msg_id: int | None,
    out_meta: dict | None,
    unique_key: str,
) -> dict:
    """Mark a pipeline job cancelled and remove its transient messages.

    Deletes the live progress message and the queued_msg record — which may
    hold the RQ handoff progress message AND the "Large file queued via S3
    pipeline..." confirmation (transferred to the pipeline job id at
    handoff) — then persists ``status=cancelled`` into io:out.
    """
    _cleanup_after_failure(chat_id, job_id, progress_msg_id)
    _mark_pipeline_hash_status(job_id, "cancelled")
    if out_meta is not None:
        try:
            out_meta.setdefault("status", "cancelled")
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass
    return {"status": "cancelled"}


def _short_error(exc: BaseException, limit: int = 120) -> str:
    """First line of an exception message, truncated for a Telegram reply."""
    try:
        text = str(exc).strip()
    except Exception:  # nosec B110
        text = ""
    first = (text.splitlines() or ["unknown error"])[0]
    if len(first) > limit:
        first = first[: limit - 3] + "..."
    return first


import config  # noqa: E402
from tools import (  # noqa: E402
    compress_pdf,
    create_thumbnail_from_image,
    create_thumbnail_from_image_bytes,
    create_thumbnail_from_pdf,
    create_thumbnail_from_pdf_bytes,
    extract_pdf_embedded_thumbnail,
    extract_pdf_embedded_thumbnail_bytes,
    extract_pdf_metadata,
    is_supported_format,
    pdf_has_text_layer,
    thumbnail_bytes_is_blank,
    thumbnail_is_usable,
)
from utils.ebook_converter import (  # noqa: E402
    ConversionCancelledError,
    calibre_available,
    convert_book_to_pdf_with_thumbnail,
    convert_ebook_robust,
    is_book_format,
    safe_target_name,
)
from utils.ocr import (  # noqa: E402
    OCRCancelledError,
    _extract_pdf_text,  # noqa: PLC2701 - same package, reused as-is
    is_ocr_source,
    ocr_enabled,
    run_ocr,
    run_ocr_pdf,
)


def _is_ebook(filename: str | None) -> bool:
    """True for non-PDF book formats — the conversion-only interface.

    PDFs stay on the thumbnail flow; every other allowed book format gets the
    deliver-back-with-Convert-button treatment, uniformly across the document,
    batch and URL entry points.
    """
    return bool(filename) and is_book_format(filename) and not filename.lower().endswith(".pdf")


def _book_conversion_enabled() -> bool:
    """The ENABLE_BOOK_CONVERSION master switch (default on)."""
    return bool(getattr(config, "ENABLE_BOOK_CONVERSION", False))
from utils.progress_tracker import _format_size  # noqa: E402
from utils.storage import _TransferProgress  # noqa: E402,F401
from utils.tg_http import (  # noqa: E402
    BOOK_CONVERT_ACTION,
    COMPRESS_PDF_ACTION,
    OCR_ACTION,
    _tg_delete_message,
    _tg_download_to_bytes,
    _tg_download_to_file,
    _tg_edit_message_reply_markup,
    _tg_forward_message,
    _tg_get_file_path,
    _tg_send_document,
    _tg_send_document_by_id,
    _tg_send_message,
    _tg_send_pending_prompt,
    _tg_send_progress,
)


def _maybe_attach_result_prompt(
    chat_id: int,
    file_path: str,
    filename: str,
    user_id: int | None,
    src_chat: int | str,
    src_message: int | None,
) -> None:
    """Post one-tap result prompts after a userbot-delivered file.

    The bot cannot edit the userbot's delivered message, so oversized results
    delivered through the userbot get a bot-API prompt pointing at the
    delivered copy (mirrors the book/convert fallback).  PDFs get
    [🗜 Compress PDF] + [🔎 OCR]; raster images get [🔎 OCR]; everything else
    is skipped.  Best-effort.
    """
    if not src_message:
        return
    _name = filename or ""
    _is_pdf = _name.lower().endswith(".pdf")
    if not (_is_pdf or is_ocr_source(_name)):
        return
    _size = 0
    try:
        if file_path:
            _size = os.path.getsize(file_path)
    except Exception:  # nosec B110
        _size = 0
    if _is_pdf:
        _tg_send_pending_prompt(
            *COMPRESS_PDF_ACTION,
            chat_id=chat_id,
            filename=filename,
            user_id=user_id,
            file_size=_size,
            src_chat_id=src_chat,
            src_message_id=src_message,
            extra_action=(
                (OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2])
                if ocr_enabled()
                else None
            ),
        )
    elif ocr_enabled():
        _tg_send_pending_prompt(
            *OCR_ACTION,
            chat_id=chat_id,
            filename=filename,
            user_id=user_id,
            file_size=_size,
            src_chat_id=src_chat,
            src_message_id=src_message,
        )

try:
    from rq import get_current_job
except Exception:
    get_current_job = None

try:
    from storage import upload_file_and_get_presigned_url
except Exception:
    upload_file_and_get_presigned_url = None


def _download_s3_key_to_file(
    key: str,
    dest_path: str,
    progress_callback: Callable[[int, int], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Download an S3 object (by key) to local `dest_path` using boto3.

    ``progress_callback(current_bytes, total_bytes)`` (optional) receives LIVE
    transfer progress.  ``cancel_check()`` (optional) is polled per chunk —
    when it returns True the transfer aborts by raising ``_JobCancelledError`` so a
    /canceljob can stop a pipeline job mid-download.  Returns True on success,
    False on failure (``_JobCancelledError`` is re-raised, never swallowed).
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        logger.exception("boto3 not available for downloading S3 key %s", key)
        return False

    bucket = getattr(config, "S3_BUCKET", None)
    if not bucket:
        logger.error("S3 bucket not configured; cannot download key %s", key)
        return False

    client_kwargs = {}
    if getattr(config, "S3_REGION", None):
        client_kwargs["region_name"] = config.S3_REGION
    if getattr(config, "S3_ENDPOINT", None):
        client_kwargs["endpoint_url"] = config.S3_ENDPOINT
    if getattr(config, "AWS_ACCESS_KEY_ID", None) or getattr(
        config, "AWS_SECRET_ACCESS_KEY", None
    ):
        client_kwargs["aws_access_key_id"] = config.AWS_ACCESS_KEY_ID or None
        client_kwargs["aws_secret_access_key"] = (
            config.AWS_SECRET_ACCESS_KEY or None
        )

    try:
        sig = getattr(config, "S3_SIGNATURE_VERSION", "s3v4")
        boto_cfg = BotoConfig(signature_version=sig)
        s3 = boto3.client("s3", config=boto_cfg, **client_kwargs)
    except Exception:
        logger.exception("Failed to create S3 client for download of %s", key)
        return False

    # Resolve the object size once so the live callback can show a percentage.
    _s3_total = 0
    if progress_callback is not None:
        try:
            _s3_total = int(
                s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
            )
        except Exception:  # nosec B110 - unknown size still shows bytes
            _s3_total = 0
    _cb = _TransferProgress(_s3_total, progress_callback)

    try:
        # ensure parent dir exists
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        if cancel_check is None:
            s3.download_file(bucket, key, dest_path, Callback=_cb)
        else:
            # Chunked get_object loop: poll cancel_check per chunk so a
            # /canceljob flag can abort a large download mid-transfer
            # (boto3's download_file can't be interrupted, and
            # _TransferProgress swallows callback exceptions).
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"]
            with open(dest_path, "wb") as fh:
                while True:
                    if cancel_check():
                        raise _JobCancelledError()
                    chunk = body.read(1024 * 1024)
                    if not chunk:
                        break
                    fh.write(chunk)
                    _cb(len(chunk))
        return True
    except _JobCancelledError:
        raise
    except Exception:
        logger.exception("Failed to download S3 key %s to %s", key, dest_path)
        # fallback: try to generate a presigned URL and download via requests
        try:
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=int(getattr(config, "S3_PRESIGNED_EXPIRY", 3600)),
            )
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            if cancel_check is not None and cancel_check():
                                raise _JobCancelledError()
                            fh.write(chunk)
                            _cb(len(chunk))
            return True
        except _JobCancelledError:
            raise
        except Exception:
            logger.exception(
                "Presigned GET fallback failed for S3 key %s", key
            )
            return False


def process_input_key_job(job: dict) -> dict:
    """Process a job dict produced by telethon_ingest._upload_and_enqueue.

    Expected keys: 'job_id', 'input_key' (S3 key), 'original_filename', 'size', 'chat_id', 'message_id', 'cleanup_input'
    Optional: 'file_unique_id' (carried by the bot's BigFilePipeline path; when
    present it is bound to the content hash so re-sends can be resolved at
    enqueue time — telethon_ingest jobs without it still dedup in-worker).
    This will download the object to a temp dir and run the disk-mode flow (thumbnail, compress, send).
    Returns the Telegram send response or an error dict.
    """
    job_id = job.get("job_id") or uuid.uuid4().hex
    input_key = job.get("input_key")
    filename = (
        job.get("original_filename")
        or os.path.basename(input_key or "")
        or f"{job_id}.bin"
    )
    chat_id = job.get("chat_id")
    user_id = job.get("user_id")
    cleanup_input = job.get("cleanup_input", True)

    unique_key = job_id

    logger.info(
        "process_input_key_job: start chat_id=%s user_id=%s filename=%s size=%s",
        chat_id,
        user_id,
        filename,
        job.get("size") or job.get("file_size"),
    )

    # Honour /canceljob: abort before downloading when the flag is set.
    # (/canceljob's "\u2705 Cancelled..." reply is the user-facing
    # confirmation, so no lingering extra message is posted here.)
    if _job_cancelled(job_id) or _pipeline_cancel_flag(job_id):
        # The shared cancelled-cleanup removes the queued_msg record — which
        # may hold the RQ handoff progress message and the "Large file queued
        # via S3 pipeline..." confirmation — in the race where the flag was
        # set after the job was popped but before this pre-flight check ran,
        # and marks the job hash status as cancelled (when the hash still
        # exists).
        return _cancel_pipeline_cleanup(
            job_id, chat_id, None, None, unique_key
        )

    # write input metadata for observability
    try:
        input_meta = {
            "job_id": job_id,
            "input_key": input_key,
            "filename": filename,
            "size": job.get("size") or job.get("file_size"),
            "chat_id": chat_id,
            "user_id": user_id,
            "enqueued_at": int(time.time()),
        }
        _set_io_keys(unique_key, input_meta=input_meta)
    except Exception:
        logger.exception("Failed to write io:in for job %s", unique_key)

    out_meta = {
        "status": "processing",
        "timestamps": {"start": int(time.time())},
        "durations": {},
        "sizes": {},
        "user_id": user_id,
    }
    try:
        _set_io_keys(unique_key, output_meta=out_meta)
    except Exception:  # nosec B110
        pass

    tmpdir = None
    _progress_msg_id = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        dest_path = os.path.join(tmpdir, _safe_local_filename(filename))

        # Send initial progress
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "downloading",
            detail="\U0001f4e5 Downloading from S3 storage...",
            file_size=job.get("size") or job.get("file_size") or 0,
        )
        # Register the live progress message in the job's auto-delete record so
        # /canceljob removes it alongside the "Large file queued..." message.
        if job_id and _progress_msg_id:
            _append_queued_message(job_id, _progress_msg_id)

        # Live byte progress while pulling the object from S3 (throttled edits).
        _live_state = {
            "msg_id": _progress_msg_id,
            "last_pct": -1,
            "last_t": 0.0,
        }

        def _live_download_cb(recv: int, total: int) -> None:
            _live_edit(
                _live_state,
                chat_id,
                filename,
                "downloading",
                f"\U0001f4e5 Downloading from S3: "
                f"{_format_size(recv)} / {_format_size(total)}",
                recv,
                total,
            )

        dl_start = time.time()
        ok = False
        try:
            if input_key:
                ok = _download_s3_key_to_file(
                    input_key,
                    dest_path,
                    progress_callback=_live_download_cb,
                    cancel_check=lambda: _check_cancel_flags(job_id),
                )
        except _JobCancelledError:
            # /canceljob fired mid-download: the S3 transfer was aborted, so
            # remove the transient messages and record the cancellation.
            return _cancel_pipeline_cleanup(
                job_id, chat_id, _progress_msg_id, out_meta, unique_key
            )
        if not ok:
            # Notify the user and clear the transient progress + "Queued..."
            # messages instead of leaving a stale "failed" bar in the chat.
            _tg_send_message(
                None,
                chat_id,
                "\u274c Failed to download from S3 storage.",
            )
            _cleanup_after_failure(chat_id, job_id, _progress_msg_id)
            out_meta.setdefault("status", "download_failed")
            out_meta.setdefault("error", "s3_download_failed")
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass
            _mark_pipeline_hash_status(job_id, "failed")
            return {"error": "s3_download_failed"}
        # ── Honour /canceljob fired between the last chunk and here ──
        if _check_cancel_flags(job_id):
            return _cancel_pipeline_cleanup(
                job_id, chat_id, _progress_msg_id, out_meta, unique_key
            )
        dl_elapsed = time.time() - dl_start
        out_meta.setdefault("durations", {})["download_ms"] = int(
            dl_elapsed * 1000
        )
        out_meta.setdefault("timestamps", {})["download_end"] = int(
            time.time()
        )
        _dl_size_post = os.path.getsize(dest_path)
        try:
            out_meta.setdefault("sizes", {})["orig_bytes"] = _dl_size_post
        except Exception:  # nosec B110
            pass
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass

        # Update progress: download complete
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "downloaded",
            detail=f"\u2705 Download complete ({_format_size(_dl_size_post)})",
            file_size=_dl_size_post,
            message_id=_progress_msg_id,
        )

        # ── Content-hash dedup: same bytes already thumbed/delivered ──────
        # SHA-256 of the DOWNLOADED bytes (never name+size): a re-sent copy of
        # the same content re-sends the cached result instead of re-running
        # the whole pipeline.  Keys on the INPUT bytes — a re-upload of the
        # original matches even though the delivered file may be compressed.
        _content_hash = None
        try:
            _content_hash = content_sha256_file(dest_path)
        except Exception:  # nosec B110 - dedup unavailable; run normally
            logger.debug("Failed to hash %s", dest_path)
        _bind_fuid_content(job.get("file_unique_id"), _content_hash)
        _dedup_src = (
            _shortcircuit_if_processed(
                _content_hash,
                "thumb",
                chat_id,
                filename=filename,
                user_id=user_id,
                note=(
                    "\u267b\ufe0f Already processed this file before — "
                    "re-sent the cached result, no new job was started."
                ),
            )
            if _content_hash
            else None
        )
        if _dedup_src:
            out_meta.setdefault("status", "already_processed")
            # Observability: how the re-send happened (bot file_id /
            # userbot forward / skipped) for io:out inspection.
            out_meta.setdefault("resend_source", _dedup_src)
            out_meta.setdefault("skipped", True)
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, job_id, _progress_msg_id)
            _mark_pipeline_hash_status(job_id, "already_processed")
            return {"status": "already_processed", "skipped": True}

        # Now reuse disk-mode flow: thumbnail, compress, s3-fallback if needed, send
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "thumbnailing",
            detail="\U0001f5bc\ufe0f Creating cover preview...",
            file_size=_dl_size_post,
            message_id=_progress_msg_id,
        )
        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        if filename.lower().endswith(".pdf"):
            # Validator: reuse the PDF's embedded thumbnail when it has one
            # (skips the 2x first-page render entirely).  A PDF that already
            # ships a cover preview needs nothing re-added — short-circuit,
            # mirroring the main process_document_job flow.
            _has_thumb = extract_pdf_embedded_thumbnail(dest_path, thumb_path)
            if _has_thumb:
                _skip_already_thumbed(
                    _content_hash,
                    filename,
                    job.get("size") or job.get("file_size"),
                    user_id=user_id,
                    chat_id=chat_id,
                )
                _cleanup_after_failure(chat_id, job_id, _progress_msg_id)
                out_meta.setdefault("status", "already_thumbed")
                out_meta.setdefault("timestamps", {})["finished"] = int(
                    time.time()
                )
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                return {"status": "already_thumbed", "skipped": True}
            create_thumbnail_from_pdf(dest_path, thumb_path)
        else:
            create_thumbnail_from_image(dest_path, thumb_path)

        # ── Full PDF metadata retrieval (persisted into io:out) ──
        if filename.lower().endswith(".pdf"):
            pdf_meta = extract_pdf_metadata(dest_path)
            if pdf_meta.get("extracted"):
                out_meta["pdf_metadata"] = pdf_meta
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass

        upload_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
        upload_path = dest_path
        try:
            orig_size = os.path.getsize(dest_path)
        except Exception:
            orig_size = None

        compress_total = 0.0
        if orig_size and upload_limit and orig_size > upload_limit:
            _progress_msg_id = _tg_send_progress(
                chat_id,
                filename,
                "compressing",
                detail="\U0001f5dc\ufe0f Compressing with /ebook quality...",
                file_size=orig_size,
                message_id=_progress_msg_id,
            )
            # first attempt
            try:
                a_start = time.time()
                c1 = dest_path + ".compressed.pdf"
                ok1 = compress_pdf(dest_path, c1, gs_quality="/ebook")
                a_elapsed = time.time() - a_start
                compress_total += a_elapsed
                out_meta.setdefault("durations", {})["compress_ms"] = int(
                    compress_total * 1000
                )
                out_meta.setdefault("timestamps", {})[
                    "compress_attempt_1_end"
                ] = int(time.time())
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                if ok1:
                    try:
                        csize = os.path.getsize(c1)
                    except Exception:
                        csize = None
                    if csize and csize <= upload_limit:
                        upload_path = c1
                        out_meta.setdefault("sizes", {})[
                            "compressed_bytes"
                        ] = csize
            except Exception:  # nosec B110
                pass

            if upload_path == dest_path:
                _progress_msg_id = _tg_send_progress(
                    chat_id,
                    filename,
                    "compressing",
                    detail="\U0001f5dc\ufe0f /ebook too large; trying /screen...",
                    file_size=orig_size,
                    message_id=_progress_msg_id,
                )
                try:
                    b_start = time.time()
                    c2 = dest_path + ".compressed.screen.pdf"
                    ok2 = compress_pdf(dest_path, c2, gs_quality="/screen")
                    b_elapsed = time.time() - b_start
                    compress_total += b_elapsed
                    out_meta.setdefault("durations", {})["compress_ms"] = int(
                        compress_total * 1000
                    )
                    out_meta.setdefault("timestamps", {})[
                        "compress_attempt_2_end"
                    ] = int(time.time())
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass
                    if ok2:
                        try:
                            c2size = os.path.getsize(c2)
                        except Exception:
                            c2size = None
                        if c2size and c2size <= upload_limit:
                            upload_path = c2
                            out_meta.setdefault("sizes", {})[
                                "compressed_bytes"
                            ] = c2size
                except Exception:  # nosec B110
                    pass

        # ── Honour /canceljob before the userbot delivery attempt ──
        if _check_cancel_flags(job_id):
            return _cancel_pipeline_cleanup(
                job_id, chat_id, _progress_msg_id, out_meta, unique_key
            )

        # If still too large, deliver via userbot first, then S3 fallback
        if (
            upload_path == dest_path
            and orig_size
            and upload_limit
            and orig_size > upload_limit
        ):
            # ── Userbot delivery first: puts the actual file into the user's
            # DM with the bot before degrading to an S3 URL / error. ──
            try:
                _progress_msg_id = _tg_send_progress(
                    chat_id,
                    filename,
                    "sending",
                    detail="\U0001f4e4 Sending via userbot (large file)...",
                    file_size=os.path.getsize(upload_path),
                    message_id=_progress_msg_id,
                )
            except Exception:  # nosec B110
                pass
            _ub_res = _deliver_result_via_userbot(
                chat_id,
                upload_path,
                filename,
                "Here is your file with an auto-generated cover preview.",
                thumb_path,
                user_id,
            )
            if _ub_res:
                _sent, _src_chat = _ub_res
                # Oversized PDFs delivered via the userbot get a one-tap
                # 🗜 Compress prompt pointing at the delivered copy (mirrors
                # the book/convert fallback).
                _maybe_attach_result_prompt(
                    chat_id,
                    upload_path,
                    filename,
                    user_id,
                    _src_chat,
                    getattr(_sent, "id", None),
                )
                _cache_userbot_delivered_copy(
                    _content_hash,
                    filename,
                    job.get("size") or job.get("file_size"),
                    "thumb",
                    _src_chat,
                    getattr(_sent, "id", None),
                    user_id=user_id,
                    chat_id=chat_id,
                )
                _publish_thumb_ready(
                    job.get("file_unique_id"),
                    filename.lower().endswith(".pdf"),
                    thumb_path=thumb_path,
                )
                try:
                    out_meta.setdefault("status", "done")
                    out_meta.setdefault("delivery", "userbot")
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                _cleanup_after_success(chat_id, job_id, _progress_msg_id)
                _mark_pipeline_hash_status(job_id, "done")
                return {"status": "done", "delivery": "userbot"}
            if (
                getattr(config, "ENABLE_S3_FALLBACK", False)
                and getattr(config, "S3_BUCKET", None)
                and upload_file_and_get_presigned_url
            ):
                try:
                    up_start = time.time()
                    url = upload_file_and_get_presigned_url(
                        dest_path, filename
                    )
                    up_elapsed = time.time() - up_start
                    if url:
                        try:
                            _tg_send_message(
                                None,
                                chat_id,
                                "\U0001f4ce File was too large for Telegram; uploaded to external storage.",
                            )
                        except Exception:  # nosec B110
                            pass
                        out_meta.setdefault("durations", {})[
                            "s3_upload_ms"
                        ] = int(up_elapsed * 1000)
                        out_meta.setdefault("timestamps", {})[
                            "s3_upload_end"
                        ] = int(time.time())
                        out_meta.setdefault("status", "s3_fallback")
                        out_meta.setdefault("s3", {})["url"] = url
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:  # nosec B110
                            pass
                        _cleanup_after_success(
                            chat_id, job_id, _progress_msg_id
                        )
                        _mark_pipeline_hash_status(job_id, "s3_fallback")
                        return {"s3_url": url}
                except Exception:
                    logger.exception("S3 fallback failed for job %s", job_id)

            # fallback notify and persist
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\U0001f4e6 File too large to upload via bot; compression couldn't reduce it enough. Try a smaller file or external storage.",
                )
            except Exception:  # nosec B110
                pass
            # The job is over: clear the transient progress + "Queued..."
            # messages (the notification above remains as feedback).
            _cleanup_after_failure(chat_id, job_id, _progress_msg_id)
            _mark_pipeline_hash_status(job_id, "too_large")
            out_meta.setdefault("status", "too_large_after_compress")
            out_meta.setdefault("sizes", {})["orig_bytes"] = orig_size
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass
            return {"error": "file too large after compression"}

        # ── Honour /canceljob while the job is in flight ──
        if _check_cancel_flags(job_id):
            return _cancel_pipeline_cleanup(
                job_id, chat_id, _progress_msg_id, out_meta, unique_key
            )

        # send final document via Telegram
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "sending",
            detail="\U0001f4e4 Sending result to Telegram...",
            file_size=os.path.getsize(upload_path),
            message_id=_progress_msg_id,
        )
        # Live upload progress while the result is pushed to Telegram.
        _send_state = {
            "msg_id": _progress_msg_id,
            "last_pct": -1,
            "last_t": 0.0,
        }

        def _live_send_cb(recv: int, total: int) -> None:
            _live_edit(
                _send_state,
                chat_id,
                filename,
                "sending",
                f"\U0001f4e4 Sending to Telegram: "
                f"{_format_size(recv)} / {_format_size(total)}",
                recv,
                total,
            )

        send_start = time.time()
        _thumb_fh = None
        try:
            if thumbnail_is_usable(thumb_path):
                _thumb_fh = open(thumb_path, "rb")
            with open(upload_path, "rb") as f_doc:
                res = _tg_send_document(
                    None,
                    chat_id,
                    f_doc,
                    filename,
                    thumb_fileobj=_thumb_fh,
                    caption="Here is your file with an auto-generated cover preview.",
                    progress_callback=_live_send_cb,
                    compress_user_id=user_id,
                    ocr_user_id=user_id,
                )
        finally:
            if _thumb_fh is not None:
                _thumb_fh.close()
        _cache_delivered_copy(
            _content_hash,
            filename,
            job.get("size") or job.get("file_size"),
            "thumb",
            res,
            user_id=user_id,
            chat_id=chat_id,
        )
        _publish_thumb_ready(
            job.get("file_unique_id"),
            filename.lower().endswith(".pdf"),
            thumb_path=thumb_path,
        )
        send_elapsed = time.time() - send_start
        out_meta.setdefault("durations", {})["tg_send_ms"] = int(
            send_elapsed * 1000
        )
        out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
        out_meta.setdefault("status", "done")
        try:
            out_meta.setdefault("sizes", {})["out_bytes"] = os.path.getsize(
                upload_path
            )
        except Exception:  # nosec B110
            pass
        try:
            out_meta["tg_response"] = res
        except Exception:  # nosec B110
            pass
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass

        # Auto-delete the transient messages now that the output was
        # delivered: the progress bar and the "Queued..." confirmation.
        _cleanup_after_success(chat_id, job_id, _progress_msg_id)
        _mark_pipeline_hash_status(job_id, "done")

        try:
            if get_current_job is not None:
                job_obj = get_current_job()
                if job_obj is not None:
                    job_obj.meta["tg_response"] = res
                    job_obj.save_meta()
        except Exception:  # nosec B110
            pass

        return res

    except Exception as e:
        logger.exception("Error processing input_key job %s", job_id)
        # Clear the transient progress + "Queued..." messages; the standalone
        # error notification below remains as the failure message.
        _cleanup_after_failure(chat_id, job_id, _progress_msg_id)
        out_meta.setdefault("status", "error")
        out_meta.setdefault("error", str(e))
        out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error processing uploaded file: "
                f"{_short_error(e)}\n"
                "Check server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        _mark_pipeline_hash_status(job_id, "error")
        return {"error": "processing_error"}
    finally:
        try:
            if tmpdir and os.path.exists(tmpdir):
                if cleanup_input:
                    shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


# IO mapping TTL (seconds) for input/output keys stored in Redis
IO_TTL = 7 * 24 * 3600


from utils.db import COL_JOBS, get_sync_db, sync_query  # noqa: E402
from utils.processed_cache import (  # noqa: E402
    _get_pdf_checks,
    _store_fuid_binding,
    _store_pdf_checks,
    content_sha256,
    content_sha256_file,
    get_processed_op,
    get_processed_record,
    upsert_processed_record,
)
from utils.redis_client import get_sync_redis  # noqa: E402
from utils.url_validation import _validate_url_safe  # noqa: E402


def _set_io_keys(
    unique_id: str,
    input_meta: dict | None = None,
    output_meta: dict | None = None,
    ttl: int | None = None,
) -> bool:
    """Set input and/or output JSON blobs in Redis under `io:in:{id}` and `io:out:{id}`.

    Also writes a best-effort backup to MongoDB (sync) so
    metadata survives Redis key expiry or restarts.
    """
    r = get_sync_redis()
    redis_ok = False
    try:
        if r:
            if ttl is None:
                ttl = IO_TTL
            if input_meta is not None:
                r.set(f"io:in:{unique_id}", json.dumps(input_meta), ex=ttl)
            if output_meta is not None:
                r.set(f"io:out:{unique_id}", json.dumps(output_meta), ex=ttl)
            redis_ok = True
    except Exception:
        logger.exception("Failed setting IO keys for %s", unique_id)

    # Best-effort MongoDB backup (sync, cached client)
    try:
        mongo_meta = {}
        if input_meta is not None:
            mongo_meta["io_in"] = input_meta
        if output_meta is not None:
            mongo_meta["io_out"] = output_meta
        if mongo_meta:
            mongo_meta["unique_id"] = unique_id
            mongo_meta["type"] = "io_metadata"
            mongo_meta["created_at"] = time.time()
            # Use the prepared statement query builder (field whitelist + parameter binding)
            mongo_db = get_sync_db()
            if mongo_db is not None:
                sync_query(COL_JOBS, mongo_db).where(
                    "job_id", "=", f"io:{unique_id}"
                ).upsert(mongo_meta)
    except Exception:  # nosec B110
        pass

    return redis_ok


# Shared validator-result cache (``pdfcheck:<file_unique_id>``) and the
# processed-result cache (``processed:<fingerprint>``) live in
# utils/processed_cache.py — shared with bot.py so enqueue-time short-circuits
# ("already thumbed" / re-send dedup) see what the worker recorded.


def _cache_delivered_copy(
    content_hash: str | None,
    filename: str | None,
    file_size: int | None,
    op: str,
    res: dict | None,
    *,
    user_id: int | None = None,
    chat_id: int | None = None,
    target: str | None = None,
) -> None:
    """Best-effort: cache a delivered copy's reusable file_id for re-send dedup.

    ``res`` is the sendDocument response; the delivered copy's file_id lets a
    later re-send of the same CONTENT (keyed by its sha256) be re-sent
    WITHOUT a new job — even after the user deleted the bot's messages (see
    utils/processed_cache.py).
    """
    if not res or not res.get("ok"):
        return
    try:
        _doc = (res.get("result") or {}).get("document") or {}
        upsert_processed_record(
            content_hash,
            op,
            "done",
            filename=filename,
            file_size=file_size,
            file_id=_doc.get("file_id"),
            thumb_file_id=(_doc.get("thumbnail") or {}).get("file_id"),
            user_id=user_id,
            chat_id=chat_id,
            target=target,
        )
    except Exception:  # nosec B110 - cache is best-effort
        pass


def _cache_userbot_delivered_copy(
    content_hash: str | None,
    filename: str | None,
    file_size: int | None,
    op: str,
    src_chat_id: int | str | None,
    src_message_id: int | None,
    *,
    user_id: int | None = None,
    chat_id: int | None = None,
    target: str | None = None,
) -> None:
    """Best-effort: cache a USERBOT-delivered copy for re-send dedup.

    Big files delivered through the userbot have NO Bot API file_id to re-send
    from.  The record instead stores the delivered copy's location
    (``src_chat_id``/``src_message_id`` as the userbot sees it — the bot's DM
    or ``'me'``/Saved Messages); re-sends FORWARD that message via the
    userbot (server-side media copy, no re-upload).  See
    utils/processed_cache.py.
    """
    if not content_hash or not src_chat_id or not src_message_id:
        return
    try:
        upsert_processed_record(
            content_hash,
            op,
            "done",
            filename=filename,
            file_size=file_size,
            delivery="userbot",
            src_chat_id=src_chat_id,
            src_message_id=src_message_id,
            user_id=user_id,
            chat_id=chat_id,
            target=target,
        )
    except Exception:  # nosec B110 - cache is best-effort
        pass


def _forward_userbot_cached(
    src_chat_id: int | str,
    src_message_id: int,
    user_id: int | None,
    preferred_chat_id: int | str | None = None,
) -> bool:
    """Re-deliver a userbot-cached copy by forwarding its delivered message.

    Tries ``preferred_chat_id`` first (the chat where the user re-sent the
    file — correct for GROUPS, where chat ids are identical for the bot and
    the userbot), then falls back to the bot's DM / Saved Messages (where
    big-file results are originally delivered; private-chat ids differ
    between the two accounts, so the DM is the reliable target there).  A
    server-side media copy — no re-upload.
    """
    import asyncio as _asyncio

    try:
        from utils.userbot_downloader import _get_bot_user_id
        from utils.userbot_uploader import forward_message_via_userbot

        _targets: list[int | str] = []
        if preferred_chat_id:
            _targets.append(preferred_chat_id)
        _targets.append(_get_bot_user_id() or "me")
        for _t in _targets:
            if _asyncio.run(
                forward_message_via_userbot(
                    _t, src_chat_id, src_message_id, user_id=user_id
                )
            ):
                return True
    except Exception:
        logger.warning(
            "Failed to forward cached userbot result %s/%s",
            src_chat_id,
            src_message_id,
        )
        return False
    return False


def _skip_already_thumbed(
    content_hash: str | None,
    filename: str | None,
    file_size: int | None,
    *,
    user_id: int | None,
    chat_id: int | None,
) -> None:
    """Record the thumb-skip and notify the user (already-thumbed PDF).

    Best-effort: the ``processed:<content_hash>`` record lets re-sends of the
    same CONTENT short-circuit at ENQUEUE time (bot.py) instead of queueing a
    fresh job.
    """
    upsert_processed_record(
        content_hash,
        "thumb",
        "skipped",
        filename=filename,
        file_size=file_size,
        user_id=user_id,
        chat_id=chat_id,
    )
    try:
        _tg_send_message(
            None,
            chat_id,
            "✅ This PDF already has a thumbnail — nothing to add.",
        )
    except Exception:  # nosec B110
        pass


def _bind_fuid_content(
    file_unique_id: str | None, content_hash: str | None
) -> None:
    """Best-effort: bind ``file_unique_id -> content_hash`` for bot.py.

    The web process only has the fuid at enqueue time (never the bytes), so
    the worker records the mapping in TWO places — the durable ``pfuid:<fuid>``
    index (30-day TTL, matching the processed record) and the pdfcheck
    record (legacy/7-day) — letting bot.py resolve ``processed:<hash>`` and
    re-send the cached copy at the SURFACE, before any download.  The
    durable index keeps the fast path alive for the record's full 30-day
    lifetime.  Best-effort: a failed bind just means re-sends queue a job
    and the worker's own content-hash dedup handles them (after download).
    """
    if not file_unique_id or not content_hash:
        return
    # Both helpers swallow their own errors — best-effort by design.
    _store_fuid_binding(file_unique_id, content_hash)
    _store_pdf_checks(file_unique_id, content_hash=content_hash)


# Per-op re-send captions for worker-side dedup short-circuits.
_OP_DONE_CAPTIONS = {
    "thumb": (
        "\U0001f5bc\ufe0f Here is your file (cached result — "
        "already processed)."
    ),
    "ocr": (
        "\U0001f50e Here is the cached OCR result (already processed)."
    ),
    "compress": (
        "\U0001f5dc\ufe0f Here is the cached compressed result "
        "(already processed)."
    ),
    "convert": (
        "\U0001f4da Here is your converted book (cached result — "
        "already processed)."
    ),
}

_OP_SKIPPED_MSGS = {
    "thumb": "✅ This PDF already has a thumbnail — nothing to add.",
    "ocr": (
        "✅ This PDF already has a searchable text layer — no OCR needed "
        "(nothing to add)."
    ),
    "compress": (
        "✅ This PDF is already well-compressed — kept the original "
        "(nothing to add)."
    ),
    "convert": "✅ This book was already converted — nothing to add.",
}


def _resend_cached_processed(
    content_hash: str | None,
    op: str,
    chat_id: int,
    *,
    filename: str | None = None,
    user_id: int | None = None,
    expected_target: str | None = None,
) -> str | None:
    """Worker-side dedup: re-send a cached copy when ``op`` already finished.

    Called right after download (the content hash is known): if the record
    says the op was delivered before, re-send the cached copy and return the
    delivery SOURCE — ``"bot"`` (Bot API file_id re-send) or ``"userbot"``
    (forward of the userbot-delivered copy); if it was short-circuited
    before, return ``"skipped"`` (the caller posts the per-op "already ..."
    note).  Returns None when nothing is recorded or the cached copy failed
    to send — the caller then processes the job normally.  The returned
    source is what callers log into ``io:out`` for observability.

    ``expected_target`` (book conversion): when set, a cached record whose
    ``target`` differs (e.g. the book was converted to PDF but the user now
    asks for TXT) is NOT a re-send of this deliverable — return None so the
    job runs fresh.
    """
    if not content_hash:
        return None
    _rec = get_processed_record(content_hash)
    if not _rec:
        return None
    _entry = get_processed_op(_rec, op, expected_target)
    if not _entry:
        return None
    if _entry.get("status") == "skipped":
        return "skipped"
    if _entry.get("status") != "done":
        return None
    if expected_target and (_entry.get("target") or "") != expected_target:
        # Defense-in-depth: get_processed_op already matched the target (via
        # the per-target field or the legacy fallback), so this only fires on
        # inconsistent hand-written records — never re-send a DIFFERENT
        # format's deliverable.
        return None
    if _entry.get("delivery") == "userbot":
        # Big-file results have no Bot API file_id — forward the delivered
        # copy via the userbot (server-side media copy, no re-upload).
        if not (_entry.get("src_chat_id") and _entry.get("src_message_id")):
            return None
        try:
            return (
                "userbot"
                if _forward_userbot_cached(
                    _entry["src_chat_id"],
                    int(_entry["src_message_id"]),
                    user_id,
                    preferred_chat_id=chat_id,
                )
                else None
            )
        except Exception:  # nosec B110 - forward failed; process fresh
            return None
    if _entry.get("file_id"):
        try:
            _res = _tg_send_document_by_id(
                None,
                chat_id,
                _entry["file_id"],
                _rec.get("filename") or filename or "file",
                caption=_OP_DONE_CAPTIONS.get(
                    op, "♻️ Cached result (already processed)."
                ),
                compress_user_id=user_id,
                ocr_user_id=user_id,
            )
            return "bot" if _res and _res.get("ok") else None
        except Exception:  # nosec B110 - cached copy failed; process fresh
            logger.warning(
                "Failed to re-send cached %s result for %s", op, filename
            )
            return None
    return None


def _shortcircuit_if_processed(
    content_hash: str | None,
    op: str,
    chat_id: int,
    *,
    filename: str | None = None,
    user_id: int | None = None,
    note: str | None = None,
    expected_target: str | None = None,
) -> str | None:
    """Worker-side dedup: short-circuit the job and report how.

    Re-sends the cached copy when the op was already delivered (and posts
    ``note``), or posts the per-op "already ..." note when it was previously
    short-circuited.  Returns the dedup OUTCOME — ``"bot"`` (Bot API file_id
    re-send), ``"userbot"`` (userbot forward) or ``"skipped"`` (no copy;
    the per-op note was posted) — or None when the job should run normally.
    Callers log the outcome into ``io:out`` (``resend_source``) for
    observability, mark out_meta, clean up transient messages and return.

    ``expected_target`` (book conversion) is forwarded to the record check so
    a cached result of a DIFFERENT target format is never re-sent.
    """
    if not content_hash:
        return None
    _dedup = _resend_cached_processed(
        content_hash,
        op,
        chat_id,
        filename=filename,
        user_id=user_id,
        expected_target=expected_target,
    )
    if _dedup is None:
        return None
    try:
        if _dedup == "skipped":
            _tg_send_message(None, chat_id, _OP_SKIPPED_MSGS.get(op, ""))
        elif note:
            _tg_send_message(None, chat_id, note)
    except Exception:  # nosec B110 - best-effort note
        pass
    return _dedup


def _attach_job_user_meta(user_id: int | None) -> str | None:
    """Best-effort: tag the current RQ job with ``user_id`` (returns its job id).

    Mirrors the per-user observability in process_document_job so queued/running
    jobs are attributable to a user in RQ dashboards and /status.
    """
    try:
        _j = get_current_job()
        if _j is None:
            return None
        _j.meta["user_id"] = user_id
        _j.save_meta()
        return getattr(_j, "id", None)
    except Exception:  # nosec B110 - best-effort
        return None


def process_document_job(
    chat_id: int,
    file_id: str,
    filename: str,
    mime: str | None = "",
    file_unique_id: str | None = None,
    message_id: int | None = None,
    forward_info: dict | None = None,
    file_size: int | None = None,
    user_id: int | None = None,
    _skip_queued_delete: bool = False,
) -> dict | None:
    """RQ job: download a Telegram file by file_id, create thumbnail, and send back the original with thumb.

    When the Bot API cannot handle a large file (>20MB download cap), falls back through:
      1. File_id-based userbot download (fast, but may fail for modern file_id formats)
      2. Chat-based userbot download ``download_bytes_via_userbot(chat_id, message_id)``
      3. BigFilePipeline (S3 pipeline + separate worker) — only if S3 is configured

    ``user_id`` is threaded into the userbot fallback so the *requesting user's*
    own Telethon/Pyrogram session is used (per-user sessions).

    NOTE: This function reads the bot token from `config.BOT_TOKEN` internally; do NOT pass the token as a job argument.
    """
    unique_key = file_unique_id or file_id

    logger.info(
        "process_document_job: start chat_id=%s user_id=%s filename=%s size=%s",
        chat_id,
        user_id,
        filename,
        file_size,
    )

    # Capture the RQ job id (when running under the RQ worker) so /canceljob can
    # abort this job via the `cancel:<id>` Redis flag even while it is running.
    _rq_job_id = None
    try:
        import rq

        _cur_job = rq.get_current_job()
        _rq_job_id = _cur_job.id if _cur_job else None
    except Exception:  # nosec B110 - rq is optional outside the worker
        pass
    _cancel_check_id = _rq_job_id or unique_key

    # Honour an early /canceljob request before doing any heavy work.
    if _job_cancelled(_cancel_check_id):
        return {"status": "cancelled"}

    # ── Early format validation: reject unsupported formats before any processing ──
    if not is_supported_format(filename, mime or ""):
        logger.info(
            "process_document_job: rejected unsupported format: filename=%s mime=%s chat_id=%s user_id=%s",
            filename,
            mime,
            chat_id,
            user_id,
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Unsupported file format.\n\n"
                "I work with **PDFs, images** (JPEG, PNG, WEBP, GIF) and **e-books** "
                "(EPUB, MOBI, AZW3, FB2, DOCX, TXT, RTF, HTML, ODT and more).\n"
                "Video files (MKV, AVI, MP4, MOV, etc.) and other formats are not supported.",
                # Static text with **bold** markers -- no user input, so
                # Markdown parsing is safe (matches the other formatted sends).
                parse_mode="Markdown",
            )
        except Exception:  # nosec B110
            pass
        # No progress message was posted yet, but the "Queued your file..."
        # confirmation must still be cleaned up.
        _cleanup_after_failure(
            chat_id,
            _rq_job_id,
            None,
            skip_queued_delete=_skip_queued_delete,
        )
        return {"error": "unsupported format", "filename": filename, "mime": mime}

    # persist input metadata
    try:
        input_meta = {
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "filename": filename,
            "mime": mime,
            "chat_id": chat_id,
            "user_id": user_id,
            "message_id": message_id,
            "forward_info": forward_info,
            "enqueued_at": int(time.time()),
        }
        _set_io_keys(unique_key, input_meta=input_meta)
    except Exception:
        logger.exception(
            "Failed to write initial io input key for %s", unique_key
        )

    # init output meta / timings
    out_meta = {
        "status": "processing",
        "timestamps": {"start": int(time.time())},
        "durations": {},
        "sizes": {},
        "user_id": user_id,
    }
    try:
        _set_io_keys(unique_key, output_meta=out_meta)
    except Exception:  # nosec B110
        pass

    tmpdir = None
    # Flag for userbot fallback data (large files that Bot API can't handle)
    _userbot_dl_data = None
    # Calculate upload limit BEFORE getFile so the early size check can use it
    # Upload cap (sendDocument) gates compression/send decisions; the Bot API
    # DOWNLOAD cap (getFile) gates whether getFile is even attempted.
    upload_limit = config.BOT_API_UPLOAD_LIMIT_BYTES
    download_limit = config.BOT_API_DOWNLOAD_LIMIT_BYTES
    # Track progress message ID so we can edit the same message
    _progress_msg_id = None
    try:
        # 1) getFile (path) — with multi-level userbot fallback for files >20MB
        #
        # Fallback chain when Bot API cannot handle the file:
        #   a) File_id-based download  — fastest, but broken for v4+ file_ids
        #   b) Chat-based download     — works reliably with any file_id
        #   d) Relay group             — forward to relay, then userbot download
        #   c) BigFilePipeline         — S3 pipeline + separate worker (files >20MB)
        #
        gf_start = time.time()
        try:
            # If we already know the file exceeds the Bot API DOWNLOAD limit,
            # skip getFile entirely (getFile would 400 "file is too big").
            _skip_bot_api = (
                file_size and download_limit and file_size > download_limit
            )
            if _skip_bot_api:
                logger.info(
                    "file_size=%d > download_limit=%d; skipping Bot API getFile, "
                    "proceeding directly to userbot download",
                    file_size,
                    download_limit,
                )
                raise requests.HTTPError("Bad Request: file is too big")
            # For small files going through Bot API: send initial progress
            _progress_msg_id = _tg_send_progress(
                chat_id,
                filename,
                "downloading",
                detail="\U0001f4e5 Downloading via Bot API...",
                file_size=file_size or 0,
            )
            # Register the live progress message in the job's auto-delete record
            # so /canceljob (and any cleanup) can remove it too, not just the
            # "Queued..." confirmation.
            if _rq_job_id and _progress_msg_id:
                _append_queued_message(_rq_job_id, _progress_msg_id)
            tg_file_path = _tg_get_file_path(
                None,
                file_id,
                diagnostic=lambda meta: _set_io_keys(
                    file_id, output_meta=meta
                ),
            )
        except requests.HTTPError as _gf_err:
            _gf_err_str = str(_gf_err)
            if "file is too big" in _gf_err_str.lower():
                # Honor the ENABLE_USERBOT gate (mirrors the reference's
                # handlers-level gate): when explicitly disabled, skip the
                # userbot fallback chain entirely.
                try:
                    import config as _config

                    _userbot_gate = _config.ENABLE_USERBOT
                except Exception:
                    _userbot_gate = True
                if not _userbot_gate:
                    logger.info(
                        "Bot API cannot handle large file; userbot fallback disabled by ENABLE_USERBOT"
                    )
                    raise
                logger.info(
                    "Bot API cannot handle large file; trying userbot fallback chain"
                )

                # Send initial progress message (edits the existing one posted
                # before the Bot API attempt, so the user sees a single message)
                _progress_msg_id = _tg_send_progress(
                    chat_id,
                    filename,
                    "downloading",
                    detail="\U0001f504 Connecting to userbot...",
                    file_size=file_size or 0,
                    message_id=_progress_msg_id,
                )
                # Register the live progress message in the auto-delete record
                # (covers the skipped-Bot-API path too, where the message above
                # is a fresh post rather than an edit).
                if _rq_job_id and _progress_msg_id:
                    _append_queued_message(_rq_job_id, _progress_msg_id)

                import asyncio as _asyncio

                _ub_data = None
                _fallback_errors = []

                # Live byte progress during userbot downloads: a throttled
                # callback that edits the progress message with real
                # bytes-downloaded numbers (edits >=2s apart or >=2% jumps).
                _ub_state = {
                    "msg_id": _progress_msg_id,
                    "last_pct": -1,
                    "last_t": 0.0,
                    "phase": None,
                }

                def _userbot_progress_cb(
                    recv: int, total: int, phase: str = "download"
                ) -> None:
                    if not total:
                        return
                    if not _ub_state["msg_id"]:
                        # No progress message to edit (initial post failed);
                        # posting one here would create an untracked message
                        # that cleanup couldn't remove on cancel.
                        return
                    if phase != _ub_state.get("phase"):
                        # Phase switch (e.g. userbot download -> S3 upload):
                        # reset the throttle state so the new phase's bar
                        # climbs from 0 instead of being suppressed.
                        _ub_state["phase"] = phase
                        _ub_state["last_pct"] = -1
                    pct = int(recv * 100 / total)
                    now = time.time()
                    if (
                        pct - _ub_state["last_pct"] < 2
                        and now - _ub_state["last_t"] < 2.0
                    ):
                        return
                    _ub_state["last_pct"] = pct
                    _ub_state["last_t"] = now
                    if phase == "s3_upload":
                        detail = (
                            f"\U0001f4e4 Uploading to S3: "
                            f"{_format_size(recv)} / {_format_size(total)}"
                        )
                    else:
                        detail = (
                            f"\U0001f4e5 Downloading: "
                            f"{_format_size(recv)} / {_format_size(total)}"
                        )
                    new_id = _tg_send_progress(
                        chat_id,
                        filename,
                        "downloading",
                        detail=detail,
                        file_size=total,
                        message_id=_ub_state["msg_id"],
                        progress_pct=pct,
                    )
                    if new_id:
                        _ub_state["msg_id"] = new_id

                # ── Fallback (a): file_id-based download ──
                try:
                    from utils.userbot_downloader import (
                        download_bytes_by_file_id_via_userbot as _dl_file_id,
                    )

                    _ub_data = _asyncio.run(
                        _dl_file_id(
                            file_id,
                            progress_callback=_userbot_progress_cb,
                            user_id=user_id,
                        )
                    )
                    if _ub_data and len(_ub_data) > 0:
                        logger.info(
                            "Userbot file_id download succeeded: %d bytes",
                            len(_ub_data),
                        )
                    else:
                        _ub_data = None
                        raise Exception("file_id download returned empty")
                except Exception as _fb_a:
                    _fallback_errors.append(f"file_id download: {_fb_a}")
                    logger.warning(
                        "Fallback (a) file_id download failed: %s", _fb_a
                    )

                # ── Fallback (b): chat-based download (works with any file_id) ──
                if _ub_data is None and message_id:
                    try:
                        from utils.userbot_downloader import (
                            download_bytes_via_userbot as _dl_chat,
                        )

                        _progress_msg_id = _tg_send_progress(
                            chat_id,
                            filename,
                            "downloading",
                            detail="\U0001f4e5 Downloading via userbot...",
                            file_size=file_size or 0,
                            message_id=_progress_msg_id,
                        )
                        logger.info(
                            "Trying fallback (b) chat-based download: chat=%s msg=%s",
                            chat_id,
                            message_id,
                        )
                        _ub_data = _asyncio.run(
                            _dl_chat(
                                chat_id,
                                message_id,
                                progress_callback=_userbot_progress_cb,
                                user_id=user_id,
                            )
                        )
                        if _ub_data and len(_ub_data) > 0:
                            logger.info(
                                "Userbot chat-based download succeeded: %d bytes",
                                len(_ub_data),
                            )
                        else:
                            _ub_data = None
                            raise Exception("chat download returned empty")
                    except Exception as _fb_b:
                        _fallback_errors.append(f"chat download: {_fb_b}")
                        logger.warning(
                            "Fallback (b) chat-based download failed: %s",
                            _fb_b,
                        )

                # ── Fallback (d): relay group (forward -> userbot download) ──
                if _ub_data is None and message_id:
                    try:
                        relay_chat = getattr(config, "RELAY_CHAT_ID", None)
                        if relay_chat:
                            relay_chat_id = int(relay_chat)
                            bot_token = config.BOT_TOKEN
                            _progress_msg_id = _tg_send_progress(
                                chat_id,
                                filename,
                                "downloading",
                                detail="\U0001f504 Forwarding to relay group...",
                                file_size=file_size or 0,
                                message_id=_progress_msg_id,
                            )
                            logger.info(
                                "Trying fallback (d) relay group: forwarding %s/%s -> %s",
                                chat_id,
                                message_id,
                                relay_chat_id,
                            )
                            fwd_msg_id = _tg_forward_message(
                                bot_token,
                                relay_chat_id,
                                chat_id,
                                message_id,
                            )
                            if fwd_msg_id:
                                logger.info(
                                    "Forwarded to relay %s/%s, trying userbot download",
                                    relay_chat_id,
                                    fwd_msg_id,
                                )
                                from utils.userbot_downloader import (
                                    download_bytes_via_userbot as _dl_relay,
                                )

                                _ub_data = _asyncio.run(
                                    _dl_relay(
                                        relay_chat_id,
                                        fwd_msg_id,
                                        progress_callback=_userbot_progress_cb,
                                        user_id=user_id,
                                    )
                                )
                                if _ub_data and len(_ub_data) > 0:
                                    logger.info(
                                        "Relay userbot download succeeded: %d bytes",
                                        len(_ub_data),
                                    )
                                else:
                                    _ub_data = None
                                    raise Exception(
                                        "relay download returned empty"
                                    )
                            else:
                                raise Exception("forwardMessage failed")
                        else:
                            logger.info(
                                "RELAY_CHAT_ID not configured, skipping fallback (d)"
                            )
                    except Exception as _fb_d:
                        _fallback_errors.append(f"relay group: {_fb_d}")
                        logger.warning(
                            "Fallback (d) relay group download failed: %s",
                            _fb_d,
                        )

                # ── Fallback (c): BigFilePipeline (S3 pipeline) ──
                if _ub_data is None and message_id:
                    try:
                        from utils.bigfile_pipeline import (
                            BigFilePipeline as _BFP,  # noqa: N814
                        )

                        _progress_msg_id = _tg_send_progress(
                            chat_id,
                            filename,
                            "downloading",
                            detail="\U0001f504 Trying S3 pipeline...",
                            file_size=file_size or 0,
                            message_id=_progress_msg_id,
                        )
                        logger.info(
                            "Trying fallback (c) BigFilePipeline: chat=%s msg=%s size=%s",
                            chat_id,
                            message_id,
                            file_size or "unknown",
                        )
                        _pipeline = _BFP()
                        _result = _asyncio.run(
                            _pipeline.ingest_large_file(
                                chat_id=chat_id,
                                message_id=message_id,
                                file_size=file_size or 0,
                                file_unique_id=file_unique_id,
                                original_filename=filename,
                                user_id=user_id,
                                progress_callback=_userbot_progress_cb,
                            )
                        )
                        if _result and _result.ok:
                            logger.info(
                                "BigFilePipeline job enqueued: job_id=%s s3_key=%s",
                                _result.job_id,
                                _result.s3_key,
                            )
                            # The RQ job is done — strip its (now stale) cancel
                            # button from the original "Queued your file"
                            # message so this file never shows two cancel
                            # controls. Do this BEFORE the transfer moves the
                            # queued_msg record to the pipeline job id.
                            _clear_queued_message_buttons(_rq_job_id)
                            # Attach a LIVE cancel button for the pipeline job
                            # to the handoff message (same payload format as
                            # the web process's queued replies) and surface the
                            # new job id so the user can /canceljob it.
                            _handoff_kb = None
                            if user_id and _result.job_id:
                                _handoff_kb = {
                                    "inline_keyboard": [
                                        [
                                            {
                                                "text": "\u274c Cancel this job",
                                                "callback_data": (
                                                    f"canceljob:{user_id}:"
                                                    f"{_result.job_id[:32]}"
                                                ),
                                            }
                                        ]
                                    ]
                                }
                            _tg_send_progress(
                                chat_id,
                                filename,
                                "done",
                                detail=(
                                    "\u2705 Large file queued via S3 pipeline. "
                                    f"Job: {_result.job_id[:8]}... "
                                    "You'll receive the result when ready."
                                ),
                                file_size=file_size or 0,
                                message_id=_progress_msg_id,
                                reply_markup=_handoff_kb,
                            )
                            # Hand the queued-message record over to the
                            # pipeline job id so the pipeline worker cleans up
                            # the confirmation (and this progress message) once
                            # it delivers the output.
                            if _rq_job_id:
                                _append_queued_message(
                                    _rq_job_id, _progress_msg_id
                                )
                                _transfer_queued_messages(
                                    _rq_job_id, _result.job_id
                                )
                            return {"pipeline": _result.job_id}
                        else:
                            raise Exception(
                                f"BigFilePipeline failed: {_result.error if _result else 'unknown'}"
                            )
                    except Exception as _fb_c:
                        _fallback_errors.append(f"BigFilePipeline: {_fb_c}")
                        logger.warning(
                            "Fallback (c) BigFilePipeline failed: %s", _fb_c
                        )

                # ── All fallbacks exhausted ──
                if _ub_data is not None:
                    _userbot_dl_data = _ub_data
                    tg_file_path = "__userbot_fallback__"
                else:
                    logger.error(
                        "All download methods failed for file_id=%s chat=%s msg=%s user_id=%s. Errors: %s",
                        file_id,
                        chat_id,
                        message_id,
                        user_id,
                        "; ".join(_fallback_errors),
                    )
                    # The transient progress message is deleted by the outer
                    # exception handler, which also sends the error
                    # notification (with the failure reason).
                    raise _gf_err from RuntimeError(
                        f"All {len(_fallback_errors)} fallbacks exhausted: "
                        + "; ".join(_fallback_errors)
                    )
            else:
                raise
        gf_elapsed = time.time() - gf_start
        out_meta.setdefault("durations", {})["getfile_ms"] = int(
            gf_elapsed * 1000
        )
        out_meta.setdefault("timestamps", {})["getfile_end"] = int(time.time())
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass

        # (upload_limit was calculated before the getFile block above)

        if config.TMP_DIR:
            # disk-mode
            tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR)
            file_path = os.path.join(tmpdir, _safe_local_filename(filename))

            # download file (via Bot API or userbot fallback)
            dl_start = time.time()
            if _userbot_dl_data is not None:
                # Already downloaded via userbot; write bytes to disk
                with open(file_path, "wb") as fh:
                    fh.write(_userbot_dl_data)
                logger.info(
                    "Used userbot-fallback data for file_id=%s (%d bytes written)",
                    file_id,
                    len(_userbot_dl_data),
                )
            else:
                try:
                    import config as _conf

                    bot_token = _conf.BOT_TOKEN
                except Exception:
                    bot_token = None

                _bot_dl_state = {
                    "msg_id": _progress_msg_id,
                    "last_pct": -1,
                    "last_t": 0.0,
                }
                _bot_dl_total = file_size or 0

                def _bot_dl_cb(recv: int, total: int) -> None:
                    _live_edit(
                        _bot_dl_state,
                        chat_id,
                        filename,
                        "downloading",
                        f"\U0001f4e5 Downloading via Bot API: "
                        f"{_format_size(recv)} / {_format_size(total)}",
                        recv,
                        total,
                    )

                _tg_download_to_file(
                    bot_token,
                    tg_file_path,
                    file_path,
                    total=_bot_dl_total,
                    progress_callback=_bot_dl_cb,
                )
            dl_elapsed = time.time() - dl_start
            out_meta.setdefault("durations", {})["download_ms"] = int(
                dl_elapsed * 1000
            )
            out_meta.setdefault("timestamps", {})["download_end"] = int(
                time.time()
            )
            _dl_size_post = os.path.getsize(file_path)
            try:
                out_meta.setdefault("sizes", {})["orig_bytes"] = _dl_size_post
            except Exception:  # nosec B110
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass

            # Update progress: download complete
            _progress_msg_id = _tg_send_progress(
                chat_id,
                filename,
                "downloaded",
                detail=f"\u2705 Download complete ({_format_size(_dl_size_post)})",
                file_size=_dl_size_post,
                message_id=_progress_msg_id,
            )

            # ── Content-hash dedup: same bytes already processed ──────────
            # SHA-256 of the DOWNLOADED bytes — two files that merely share a
            # name and exact byte size can no longer collide.  A re-upload of
            # the same content (even one Telegram gives a fresh fuid) re-sends
            # the cached copy instead of re-rendering + re-uploading.
            _content_hash = None
            try:
                _content_hash = content_sha256_file(file_path)
            except Exception:  # nosec B110 - dedup unavailable; run normally
                logger.debug("Failed to hash %s", file_path)
            _bind_fuid_content(file_unique_id, _content_hash)
            _dedup_src = (
                _shortcircuit_if_processed(
                    _content_hash,
                    "thumb",
                    chat_id,
                    filename=filename,
                    user_id=user_id,
                    note=(
                        "\u267b\ufe0f Already processed this file before — "
                        "re-sent the cached result, no new job was started."
                    ),
                )
                if _content_hash
                else None
            )
            if _dedup_src:
                out_meta.setdefault("status", "already_processed")
                # Observability: how the re-send happened (bot file_id /
                # userbot forward / skipped) for io:out inspection.
                out_meta.setdefault("resend_source", _dedup_src)
                out_meta.setdefault("skipped", True)
                out_meta.setdefault("timestamps", {})["finished"] = int(
                    time.time()
                )
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                _cleanup_after_failure(
                    chat_id,
                    _rq_job_id,
                    _progress_msg_id,
                    skip_queued_delete=_skip_queued_delete,
                )
                return {"status": "already_processed", "skipped": True}

            # thumbnail
            _progress_msg_id = _tg_send_progress(
                chat_id,
                filename,
                "thumbnailing",
                detail="\U0001f5bc\ufe0f Creating cover preview...",
                file_size=_dl_size_post,
                message_id=_progress_msg_id,
            )
            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            if (
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower()
            ):
                # Validator: reuse the PDF's embedded thumbnail when it has one
                # (skips the 2x first-page render).  Also compute + cache BOTH
                # check results here — this job runs FIRST on the OCR &
                # Thumbnail button's FIFO queue, so ocr_job can reuse them
                # instead of re-running the text-layer pass on its own download.
                _checks = _get_pdf_checks(file_unique_id)
                _cached_layer = (
                    _checks["has_text_layer"]
                    if _checks is not None
                    else None
                )
                _has_thumb = extract_pdf_embedded_thumbnail(
                    file_path, thumb_path
                )
                _store_pdf_checks(
                    file_unique_id,
                    has_thumb=_has_thumb,
                    content_hash=_content_hash,
                    has_text_layer=(
                        _cached_layer
                        if _cached_layer is not None
                        # Only run the (potentially page-wide) text-layer pass
                        # when OCR is enabled — with OCR off, no ocr_job will
                        # ever consume it, so plain Thumbnail taps stay free
                        # of the extra pass.
                        else (
                            pdf_has_text_layer(file_path)
                            if ocr_enabled()
                            else None
                        )
                    ),
                )
                # ── Validator: "already has a thumbnail" — short-circuit ──
                # The PDF ships a cover preview embedded in its page dict, so
                # re-adding one (and re-uploading the file) adds nothing.
                # Reply and stop, mirroring the already_ocr /
                # already_compressed short-circuits.
                if _has_thumb:
                    _skip_already_thumbed(
                        _content_hash,
                        filename,
                        file_size,
                        user_id=user_id,
                        chat_id=chat_id,
                    )
                    out_meta.setdefault("status", "already_thumbed")
                    out_meta.setdefault("timestamps", {})["finished"] = int(
                        time.time()
                    )
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass
                    _cleanup_after_failure(
                        chat_id,
                        _rq_job_id,
                        _progress_msg_id,
                        skip_queued_delete=_skip_queued_delete,
                    )
                    return {"status": "already_thumbed", "skipped": True}
                create_thumbnail_from_pdf(file_path, thumb_path)

            # ── Full PDF metadata retrieval (persisted into io:out) ──
            if (
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower()
            ):
                pdf_meta = extract_pdf_metadata(file_path)
                if pdf_meta.get("extracted"):
                    out_meta["pdf_metadata"] = pdf_meta
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass

            # compression flow
            upload_path = file_path
            try:
                orig_size = os.path.getsize(file_path)
            except Exception:
                orig_size = None

            compress_total = 0.0
            if orig_size and upload_limit and orig_size > upload_limit:
                _progress_msg_id = _tg_send_progress(
                    chat_id,
                    filename,
                    "compressing",
                    detail="\U0001f5dc\ufe0f Compressing with /ebook quality...",
                    file_size=orig_size,
                    message_id=_progress_msg_id,
                )
                # attempt first pass
                try:
                    a_start = time.time()
                    c1 = file_path + ".compressed.pdf"
                    ok1 = compress_pdf(file_path, c1, gs_quality="/ebook")
                    a_elapsed = time.time() - a_start
                    compress_total += a_elapsed
                    out_meta.setdefault("durations", {})["compress_ms"] = int(
                        compress_total * 1000
                    )
                    out_meta.setdefault("timestamps", {})[
                        "compress_attempt_1_end"
                    ] = int(time.time())
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass
                    if ok1:
                        try:
                            csize = os.path.getsize(c1)
                        except Exception:
                            csize = None
                        if csize and csize <= upload_limit:
                            upload_path = c1
                            out_meta.setdefault("sizes", {})[
                                "compressed_bytes"
                            ] = csize
                except Exception:  # nosec B110
                    pass

                if upload_path == file_path:
                    # try second, more aggressive pass
                    _progress_msg_id = _tg_send_progress(
                        chat_id,
                        filename,
                        "compressing",
                        detail="\U0001f5dc\ufe0f /ebook too large; trying /screen...",
                        file_size=orig_size,
                        message_id=_progress_msg_id,
                    )
                    try:
                        b_start = time.time()
                        c2 = file_path + ".compressed.screen.pdf"
                        ok2 = compress_pdf(file_path, c2, gs_quality="/screen")
                        b_elapsed = time.time() - b_start
                        compress_total += b_elapsed
                        out_meta.setdefault("durations", {})["compress_ms"] = (
                            int(compress_total * 1000)
                        )
                        out_meta.setdefault("timestamps", {})[
                            "compress_attempt_2_end"
                        ] = int(time.time())
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:  # nosec B110
                            pass
                        if ok2:
                            try:
                                c2size = os.path.getsize(c2)
                            except Exception:
                                c2size = None
                            if c2size and c2size <= upload_limit:
                                upload_path = c2
                                out_meta.setdefault("sizes", {})[
                                    "compressed_bytes"
                                ] = c2size
                    except Exception:  # nosec B110
                        pass

            # if still too large, deliver via userbot first, then S3 fallback
            if (
                upload_path == file_path
                and orig_size
                and upload_limit
                and orig_size > upload_limit
            ):
                # ── Userbot delivery first: puts the actual file into the
                # user's DM with the bot before degrading to S3 URL / error. ──
                try:
                    _progress_msg_id = _tg_send_progress(
                        chat_id,
                        filename,
                        "sending",
                        detail="\U0001f4e4 Sending via userbot (large file)...",
                        file_size=os.path.getsize(upload_path),
                        message_id=_progress_msg_id,
                    )
                except Exception:  # nosec B110
                    pass
                _ub_res = _deliver_result_via_userbot(
                    chat_id,
                    upload_path,
                    filename,
                    "Here is your file with an auto-generated cover preview.",
                    thumb_path,
                    user_id,
                )
                if _ub_res:
                    _sent, _src_chat = _ub_res
                    # Oversized PDFs delivered via the userbot get a one-tap
                    # 🗜 Compress prompt pointing at the delivered copy (mirrors
                    # the book/convert fallback).
                    _maybe_attach_result_prompt(
                        chat_id,
                        upload_path,
                        filename,
                        user_id,
                        _src_chat,
                        getattr(_sent, "id", None),
                    )
                    _cache_userbot_delivered_copy(
                        _content_hash,
                        filename,
                        file_size,
                        "thumb",
                        _src_chat,
                        getattr(_sent, "id", None),
                        user_id=user_id,
                        chat_id=chat_id,
                    )
                    _publish_thumb_ready(
                        file_unique_id,
                        filename.lower().endswith(".pdf")
                        or "pdf" in (mime or "").lower(),
                        thumb_path=thumb_path,
                    )
                    try:
                        out_meta.setdefault("status", "done")
                        out_meta.setdefault("delivery", "userbot")
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass
                    _cleanup_after_success(
                        chat_id,
                        _rq_job_id,
                        _progress_msg_id,
                        skip_queued_delete=_skip_queued_delete,
                    )
                    return {"status": "done", "delivery": "userbot"}
                if (
                    getattr(config, "ENABLE_S3_FALLBACK", False)
                    and getattr(config, "S3_BUCKET", None)
                    and upload_file_and_get_presigned_url
                ):
                    try:
                        up_start = time.time()
                        url = upload_file_and_get_presigned_url(
                            file_path, filename
                        )
                        up_elapsed = time.time() - up_start
                        if url:
                            try:
                                _tg_send_message(
                                    None,
                                    chat_id,
                                    f"File was too large for Telegram; uploaded to external storage: {url}",
                                )
                            except Exception:  # nosec B110
                                pass
                            out_meta.setdefault("durations", {})[
                                "s3_upload_ms"
                            ] = int(up_elapsed * 1000)
                            out_meta.setdefault("timestamps", {})[
                                "s3_upload_end"
                            ] = int(time.time())
                            out_meta.setdefault("status", "s3_fallback")
                            out_meta.setdefault("s3", {})["url"] = url
                            try:
                                _set_io_keys(unique_key, output_meta=out_meta)
                            except Exception:  # nosec B110
                                pass
                            _cleanup_after_success(
                                chat_id,
                                _rq_job_id,
                                _progress_msg_id,
                                skip_queued_delete=_skip_queued_delete,
                            )
                            return {"s3_url": url}
                    except Exception:
                        logger.exception(
                            "S3 fallback failed for file_id=%s", file_id
                        )

                # otherwise notify user and persist io entry
                try:
                    _tg_send_message(
                        None,
                        chat_id,
                        "\U0001f4e6 File too large to upload via bot; compression didn't reduce it enough. Try a smaller file or external storage.",
                    )
                except Exception:  # nosec B110
                    pass
                # The job is over: clear the transient progress + "Queued..."
                # messages (the notification above remains as feedback).
                _cleanup_after_failure(
                    chat_id,
                    _rq_job_id,
                    _progress_msg_id,
                    skip_queued_delete=_skip_queued_delete,
                )
                out_meta.setdefault("status", "too_large_after_compress")
                out_meta.setdefault("sizes", {})["orig_bytes"] = orig_size
                out_meta.setdefault("timestamps", {})["finished"] = int(
                    time.time()
                )
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                return {"error": "file too large after compression"}

            # ── Honour /canceljob while the job is in flight ──
            if _job_cancelled(_cancel_check_id):
                # Auto-delete the transient progress + "Queued..." messages
                # (the /canceljob command reply already confirmed the
                # cancellation to the user).
                _tg_delete_message(chat_id, _progress_msg_id)
                _delete_queued_messages(_cancel_check_id)
                out_meta.setdefault("status", "cancelled")
                out_meta.setdefault("timestamps", {})["finished"] = int(
                    time.time()
                )
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                return {"status": "cancelled"}

            # send final document via Telegram
            _progress_msg_id = _tg_send_progress(
                chat_id,
                filename,
                "sending",
                detail="\U0001f4e4 Sending result...",
                file_size=os.path.getsize(upload_path),
                message_id=_progress_msg_id,
            )
            send_start = time.time()
            _thumb_fh = None
            try:
                if thumbnail_is_usable(thumb_path):
                    _thumb_fh = open(thumb_path, "rb")
                with open(upload_path, "rb") as f_doc:
                    res = _tg_send_document(
                        None,
                        chat_id,
                        f_doc,
                        filename,
                        thumb_fileobj=_thumb_fh,
                        caption="Here is your file with an auto-generated cover preview.",
                        compress_user_id=user_id,
                        ocr_user_id=user_id,
                    )
            finally:
                if _thumb_fh is not None:
                    _thumb_fh.close()
            _cache_delivered_copy(
                _content_hash,
                filename,
                file_size,
                "thumb",
                res,
                user_id=user_id,
                chat_id=chat_id,
            )
            _publish_thumb_ready(
                file_unique_id,
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower(),
                thumb_path=thumb_path,
            )
            send_elapsed = time.time() - send_start
            out_meta.setdefault("durations", {})["tg_send_ms"] = int(
                send_elapsed * 1000
            )
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            out_meta.setdefault("status", "done")
            try:
                out_meta.setdefault("sizes", {})["out_bytes"] = (
                    os.path.getsize(upload_path)
                )
            except Exception:  # nosec B110
                pass
            try:
                out_meta["tg_response"] = res
            except Exception:  # nosec B110
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass

            # Auto-delete the transient messages now that the output was
            # delivered: the progress bar and the "Queued your file..."
            # confirmation.  Skipped when running inside a batch job (the
            # batch cleans up its own confirmation after all items).
            _cleanup_after_success(
                chat_id,
                _rq_job_id,
                _progress_msg_id,
                skip_queued_delete=_skip_queued_delete,
            )

            try:
                if get_current_job is not None:
                    job = get_current_job()
                    if job is not None:
                        job.meta["tg_response"] = res
                        job.meta["user_id"] = user_id
                        job.save_meta()
            except Exception:  # nosec B110
                pass

            return res

        else:
            # in-memory pathway
            dl_start = time.time()
            if _userbot_dl_data is not None:
                file_bytes = _userbot_dl_data
                logger.info(
                    "Used userbot-fallback data for in-memory path (%d bytes)",
                    len(file_bytes),
                )
            else:
                file_bytes = _tg_download_to_bytes(None, tg_file_path)
            dl_elapsed = time.time() - dl_start
            out_meta.setdefault("durations", {})["download_ms"] = int(
                dl_elapsed * 1000
            )
            out_meta.setdefault("timestamps", {})["download_end"] = int(
                time.time()
            )
            try:
                out_meta.setdefault("sizes", {})["orig_bytes"] = len(
                    file_bytes
                )
            except Exception:  # nosec B110
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass

            # ── Content-hash dedup: same bytes already processed ──────────
            # See the disk-path block above — same short-circuit, keyed on the
            # bytes' sha256 so different files can never collide on the key.
            _content_hash = None
            try:
                _content_hash = content_sha256(file_bytes)
            except Exception:  # nosec B110 - dedup unavailable; run normally
                logger.debug("Failed to hash in-memory file")
            _bind_fuid_content(file_unique_id, _content_hash)
            _dedup_src = (
                _shortcircuit_if_processed(
                    _content_hash,
                    "thumb",
                    chat_id,
                    filename=filename,
                    user_id=user_id,
                    note=(
                        "\u267b\ufe0f Already processed this file before — "
                        "re-sent the cached result, no new job was started."
                    ),
                )
                if _content_hash
                else None
            )
            if _dedup_src:
                out_meta.setdefault("status", "already_processed")
                # Observability: how the re-send happened (bot file_id /
                # userbot forward / skipped) for io:out inspection.
                out_meta.setdefault("resend_source", _dedup_src)
                out_meta.setdefault("skipped", True)
                out_meta.setdefault("timestamps", {})["finished"] = int(
                    time.time()
                )
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:  # nosec B110
                    pass
                _cleanup_after_failure(
                    chat_id,
                    _rq_job_id,
                    _progress_msg_id,
                    skip_queued_delete=_skip_queued_delete,
                )
                return {"status": "already_processed", "skipped": True}

            if (
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower()
            ):
                # Validator: "already has a thumbnail" — short-circuit instead
                # of re-adding one to a PDF that already ships a cover preview.
                _embedded_thumb = extract_pdf_embedded_thumbnail_bytes(
                    file_bytes
                )
                if _embedded_thumb is not None:
                    _skip_already_thumbed(
                        _content_hash,
                        filename,
                        file_size,
                        user_id=user_id,
                        chat_id=chat_id,
                    )
                    out_meta.setdefault("status", "already_thumbed")
                    out_meta.setdefault("timestamps", {})["finished"] = int(
                        time.time()
                    )
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:  # nosec B110
                        pass
                    _cleanup_after_failure(
                        chat_id,
                        _rq_job_id,
                        _progress_msg_id,
                        skip_queued_delete=_skip_queued_delete,
                    )
                    return {"status": "already_thumbed", "skipped": True}
                thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)
            else:
                thumb_bytes = create_thumbnail_from_image_bytes(file_bytes)
            # A blank/white (or empty) preview is worse than none — drop it
            # so delivery attaches nothing instead of a white cover.
            try:
                if not thumb_bytes or thumbnail_bytes_is_blank(thumb_bytes):
                    thumb_bytes = b""
            except Exception:  # nosec B110 - best-effort guard
                pass

            # If large, attempt compression via temp file flow
            if upload_limit and len(file_bytes) > upload_limit:
                td = tempfile.mkdtemp()
                try:
                    tmp_in = os.path.join(td, filename)
                    with open(tmp_in, "wb") as fh:
                        fh.write(file_bytes)

                    compress_total = 0.0
                    try:
                        a_start = time.time()
                        c1 = tmp_in + ".compressed.pdf"
                        ok1 = compress_pdf(tmp_in, c1, gs_quality="/ebook")
                        a_elapsed = time.time() - a_start
                        compress_total += a_elapsed
                        out_meta.setdefault("durations", {})["compress_ms"] = (
                            int(compress_total * 1000)
                        )
                        out_meta.setdefault("timestamps", {})[
                            "compress_attempt_1_end"
                        ] = int(time.time())
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:  # nosec B110
                            pass
                        if ok1:
                            try:
                                csize = os.path.getsize(c1)
                            except Exception:
                                csize = None
                            if csize and csize <= upload_limit:
                                with open(c1, "rb") as cf:
                                    file_bytes = cf.read()
                                out_meta.setdefault("sizes", {})[
                                    "compressed_bytes"
                                ] = csize
                    except Exception:  # nosec B110
                        pass

                    if len(file_bytes) > upload_limit:
                        try:
                            b_start = time.time()
                            c2 = tmp_in + ".compressed.screen.pdf"
                            ok2 = compress_pdf(
                                tmp_in, c2, gs_quality="/screen"
                            )
                            b_elapsed = time.time() - b_start
                            compress_total += b_elapsed
                            out_meta.setdefault("durations", {})[
                                "compress_ms"
                            ] = int(compress_total * 1000)
                            out_meta.setdefault("timestamps", {})[
                                "compress_attempt_2_end"
                            ] = int(time.time())
                            try:
                                _set_io_keys(unique_key, output_meta=out_meta)
                            except Exception:  # nosec B110
                                pass
                            if ok2:
                                try:
                                    c2size = os.path.getsize(c2)
                                except Exception:
                                    c2size = None
                                if c2size and c2size <= upload_limit:
                                    with open(c2, "rb") as cf:
                                        file_bytes = cf.read()
                                    out_meta.setdefault("sizes", {})[
                                        "compressed_bytes"
                                    ] = c2size
                        except Exception:  # nosec B110
                            pass

                    # if still too big, deliver via userbot first, then S3
                    if len(file_bytes) > upload_limit:
                        # ── Userbot delivery first: write the bytes to temp
                        # files and deliver into the user's DM with the bot.
                        # The temp file keeps the real ``filename`` so the
                        # delivered document is not renamed. ──
                        _ub_tmp = os.path.join(td, filename)
                        with open(_ub_tmp, "wb") as _ub_fh:
                            _ub_fh.write(file_bytes)
                        _ub_thumb = None
                        if thumb_bytes:
                            _ub_thumb = os.path.join(
                                td, "userbot_thumb.jpg"
                            )
                            with open(_ub_thumb, "wb") as _ub_th:
                                _ub_th.write(thumb_bytes)
                        _ub_res = _deliver_result_via_userbot(
                            chat_id,
                            _ub_tmp,
                            filename,
                            "Here is your file with an auto-generated cover preview.",
                            _ub_thumb,
                            user_id,
                        )
                        if _ub_res:
                            _sent, _src_chat = _ub_res
                            # Oversized PDFs delivered via the userbot get a
                            # one-tap 🗜 Compress prompt pointing at the
                            # delivered copy (mirrors the book/convert
                            # fallback).
                            _maybe_attach_result_prompt(
                                chat_id,
                                _ub_tmp,
                                filename,
                                user_id,
                                _src_chat,
                                getattr(_sent, "id", None),
                            )
                            _cache_userbot_delivered_copy(
                                _content_hash,
                                filename,
                                file_size,
                                "thumb",
                                _src_chat,
                                getattr(_sent, "id", None),
                                user_id=user_id,
                                chat_id=chat_id,
                            )
                            _publish_thumb_ready(
                                file_unique_id,
                                filename.lower().endswith(".pdf")
                                or "pdf" in (mime or "").lower(),
                                thumb_bytes=thumb_bytes,
                            )
                            try:
                                out_meta.setdefault("status", "done")
                                out_meta.setdefault("delivery", "userbot")
                                _set_io_keys(
                                    unique_key, output_meta=out_meta
                                )
                            except Exception:  # nosec B110
                                pass
                            _cleanup_after_success(
                                chat_id,
                                _rq_job_id,
                                _progress_msg_id,
                                skip_queued_delete=_skip_queued_delete,
                            )
                            return {
                                "status": "done",
                                "delivery": "userbot",
                            }
                        if (
                            getattr(config, "ENABLE_S3_FALLBACK", False)
                            and getattr(config, "S3_BUCKET", None)
                            and upload_file_and_get_presigned_url
                        ):
                            try:
                                up_start = time.time()
                                # prefer candidate compressed file if present
                                candidate = None
                                if os.path.exists(c2):
                                    candidate = c2
                                elif os.path.exists(c1):
                                    candidate = c1
                                else:
                                    candidate = tmp_in
                                url = upload_file_and_get_presigned_url(
                                    candidate, filename
                                )
                                up_elapsed = time.time() - up_start
                                if url:
                                    try:
                                        _tg_send_message(
                                            None,
                                            chat_id,
                                            f"File was too large for Telegram; uploaded to external storage: {url}",
                                        )
                                    except Exception:  # nosec B110
                                        pass
                                    out_meta.setdefault("durations", {})[
                                        "s3_upload_ms"
                                    ] = int(up_elapsed * 1000)
                                    out_meta.setdefault("timestamps", {})[
                                        "s3_upload_end"
                                    ] = int(time.time())
                                    out_meta.setdefault(
                                        "status", "s3_fallback"
                                    )
                                    out_meta.setdefault("s3", {})["url"] = url
                                    try:
                                        _set_io_keys(
                                            unique_key, output_meta=out_meta
                                        )
                                    except Exception:  # nosec B110
                                        pass
                                    _cleanup_after_success(
                                        chat_id,
                                        _rq_job_id,
                                        _progress_msg_id,
                                        skip_queued_delete=_skip_queued_delete,
                                    )
                                    return {"s3_url": url}
                            except Exception:
                                logger.exception(
                                    "S3 fallback failed for in-memory file for chat_id=%s",
                                    chat_id,
                                )
                        try:
                            _tg_send_message(
                                None,
                                chat_id,
                                f"File too large to upload via bot after compression; size={len(file_bytes)} bytes",
                            )
                        except Exception:  # nosec B110
                            pass
                        out_meta.setdefault(
                            "status", "too_large_after_compress"
                        )
                        out_meta.setdefault("timestamps", {})["finished"] = (
                            int(time.time())
                        )
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:  # nosec B110
                            pass
                        _cleanup_after_failure(
                            chat_id,
                            _rq_job_id,
                            _progress_msg_id,
                            skip_queued_delete=_skip_queued_delete,
                        )
                        return {"error": "file too large after compression"}
                finally:
                    shutil.rmtree(td, ignore_errors=True)

            # send via Telegram
            send_start = time.time()
            doc_buf = io.BytesIO(file_bytes)
            thumb_buf = (
                io.BytesIO(thumb_bytes) if thumb_bytes else None
            )
            doc_buf.seek(0)
            if thumb_buf is not None:
                thumb_buf.seek(0)
            res = _tg_send_document(
                None,
                chat_id,
                doc_buf,
                filename,
                thumb_fileobj=thumb_buf,
                caption="Here is your file with an auto-generated cover preview.",
                compress_user_id=user_id,
                ocr_user_id=user_id,
            )
            _cache_delivered_copy(
                _content_hash,
                filename,
                file_size,
                "thumb",
                res,
                user_id=user_id,
                chat_id=chat_id,
            )
            _publish_thumb_ready(
                file_unique_id,
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower(),
                thumb_bytes=thumb_bytes,
            )
            send_elapsed = time.time() - send_start
            out_meta.setdefault("durations", {})["tg_send_ms"] = int(
                send_elapsed * 1000
            )
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            out_meta.setdefault("status", "done")
            try:
                out_meta["tg_response"] = res
            except Exception:  # nosec B110
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:  # nosec B110
                pass
            try:
                if get_current_job is not None:
                    job = get_current_job()
                    if job is not None:
                        job.meta["tg_response"] = res
                        job.meta["user_id"] = user_id
                        job.save_meta()
            except Exception:  # nosec B110
                pass
            # Auto-delete the transient messages now that the output was
            # delivered (progress bar + "Queued your file..." confirmation).
            _cleanup_after_success(
                chat_id,
                _rq_job_id,
                _progress_msg_id,
                skip_queued_delete=_skip_queued_delete,
            )
            return res

    except Exception as e:
        logger.exception("Error while processing document job %s", file_id)
        try:
            out_meta.setdefault("status", "error")
            out_meta.setdefault("error", str(e))
            out_meta.setdefault("timestamps", {})["finished"] = int(
                time.time()
            )
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:  # nosec B110
            pass
        # Auto-delete the transient progress + "Queued..." messages now that
        # the job failed; the standalone error notification below remains as
        # the user-facing failure message.
        _cleanup_after_failure(
            chat_id,
            _rq_job_id,
            _progress_msg_id,
            skip_queued_delete=_skip_queued_delete,
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error processing file in background: "
                f"{_short_error(e)}\n"
                "Check server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        return {"error": str(e)}
    finally:
        try:
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


def process_document_batch_job(
    chat_id: int, items: list, user_id: int | None = None
) -> None:
    """RQ job: process a batch of forwarded document items in order.

    Each item dict is expected to have: file_id, filename, mime.
    ``user_id`` is threaded to each item so per-user sessions are used.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "process_document_batch_job: start chat_id=%s user_id=%s items=%d",
        chat_id,
        user_id,
        len(items),
    )
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "items": len(items),
                    "enqueued_at": int(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to write io:in for batch job %s", _rq_job_id)
    results = []
    for item in items:
        file_id = item.get("file_id")
        filename = item.get("filename", "unknown")
        mime = item.get("mime", "")
        if not file_id:
            logger.warning(
                "process_document_batch_job: skipping item with no file_id: %s (user_id=%s)",
                item,
                user_id,
            )
            continue
        # ── Early format validation: skip unsupported items in batch ──
        if not is_supported_format(filename, mime):
            logger.info(
                "process_document_batch_job: skipping unsupported format: filename=%s mime=%s (user_id=%s)",
                filename,
                mime,
                user_id,
            )
            results.append({"skipped": "unsupported format", "filename": filename, "mime": mime})
            continue
        # ── E-books: conversion has its own clean interface, even in batch ──
        if _is_ebook(filename):
            if not _book_conversion_enabled():
                logger.info(
                    "process_document_batch_job: skipping e-book (conversion disabled): %s (user_id=%s)",
                    filename,
                    user_id,
                )
                results.append(
                    {
                        "skipped": "book conversion disabled",
                        "filename": filename,
                    }
                )
                continue
            try:
                res = deliver_book_job(
                    chat_id,
                    file_id,
                    filename,
                    mime,
                    user_id,
                    file_unique_id=item.get("file_unique_id"),
                    message_id=item.get("message_id"),
                    forward_info=item.get("forward_info"),
                    file_size=item.get("file_size"),
                    _skip_queued_delete=True,
                )
                results.append(res)
            except Exception:
                logger.exception(
                    "Failed processing batch e-book item %s (user_id=%s)",
                    filename,
                    user_id,
                )
                results.append({"error": f"failed: {filename}"})
            continue
        try:
            res = process_document_job(
                chat_id,
                file_id,
                filename,
                mime,
                file_unique_id=item.get("file_unique_id"),
                message_id=item.get("message_id"),
                forward_info=item.get("forward_info"),
                file_size=item.get("file_size"),
                user_id=user_id,
                _skip_queued_delete=True,
            )
            results.append(res)
        except Exception:
            logger.exception(
                "Failed processing batch item %s (user_id=%s)",
                filename,
                user_id,
            )
            results.append({"error": f"failed: {filename}"})
    # All items delivered — delete the batch's "Queued batch..." confirmation.
    try:
        _batch_job = get_current_job()
        _delete_queued_messages(
            getattr(_batch_job, "id", None)
        )
    except Exception:  # nosec B110
        pass
    logger.info(
        "process_document_batch_job: complete chat_id=%s user_id=%s items=%d processed=%d",
        chat_id,
        user_id,
        len(items),
        len(results),
    )
    _tg_send_message(
        None,
        chat_id,
        f"Batch processing complete: {len(results)} items processed.",
    )
    return results


def _download_progress_cb(
    chat_id: int,
    filename: str,
    recv: int,
    total: int,
    state: dict[str, float],
) -> None:
    """Throttled live 'Downloading...' progress reporter for conversion jobs.

    ``state`` is a mutable dict (``{"msg_id": ..., "last_pct": -1.0,
    "last_t": 0.0}``) reused across calls so the Telegram progress message
    isn't spammed per chunk.  The live message id is tracked inside ``state``
    because ``_tg_send_progress`` recreates the message when an edit fails.
    """
    if not total or not state.get("msg_id"):
        return
    pct = int(recv * 100 / total)
    now = time.time()
    if pct - state["last_pct"] < 2 and now - state["last_t"] < 2.0:
        return
    state["last_pct"] = float(pct)
    state["last_t"] = now
    try:
        new_id = _tg_send_progress(
            chat_id,
            filename,
            "downloading",
            detail=(
                f"\U0001f4e5 Downloading: "
                f"{_format_size(recv)} / {_format_size(total)} ({pct}%)"
            ),
            message_id=int(state["msg_id"]),
            progress_pct=pct,
        )
        if new_id:
            state["msg_id"] = float(new_id)
    except Exception:  # nosec B110 - progress is best-effort
        pass


def _download_job_file(
    file_id: str | None,
    dest_path: str,
    filename: str,
    file_size: int | None = None,
    user_id: int | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    chat_id: int | None = None,
    message_id: int | None = None,
    file_unique_id: str | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Download a job's source file, chaining the Bot API + userbot pipes.

    Mirrors ``process_document_job``'s fallback chain so conversion/compression
    jobs work for files of every size:

    1. Bot API ``getFile`` -- streamed to disk; skipped entirely when the size
       is already known to exceed ``BOT_API_DOWNLOAD_LIMIT_BYTES``.
    2. Userbot file-id download (``download_bytes_by_file_id_via_userbot``) --
       in-memory, works with only a ``file_id``.
    3. Userbot chat-based disk download (``download_forward_via_userbot``:
       Telethon, then Pyrogram) -- the reliable path for large files, used
       when ``chat_id``/``message_id`` are known.
    4. Relay-group fallback (forward via Bot API to ``RELAY_CHAT_ID``, then
       userbot download there) -- mirrors fallback (d) of the main pipeline.

    ``progress_callback(recv, total)`` (optional) is forwarded to every leg for
    LIVE progress.  ``cancel_check()`` (optional) is polled before each leg and
    raises ``_JobCancelledError`` so a /canceljob can abort mid-download.

    Returns True when ``dest_path`` exists and is non-empty.
    """
    import asyncio as _asyncio
    import os as _os

    _fails: list[str] = []

    def _raise_if_cancelled() -> None:
        if cancel_check and cancel_check():
            raise _JobCancelledError("download cancelled")

    # ── Pipe 1: Bot API getFile (capped by the getFile download limit; files
    # above it would 400 "file is too big", so route straight to the userbot).
    _bot_dl_limit = getattr(config, "BOT_API_DOWNLOAD_LIMIT_BYTES", 0)
    if file_size is None or not _bot_dl_limit or file_size <= _bot_dl_limit:
        try:
            _raise_if_cancelled()
            _path = _tg_get_file_path(None, file_id)
            if _path:
                _tg_download_to_file(
                    None,
                    _path,
                    dest_path,
                    total=file_size or 0,
                    progress_callback=progress_callback,
                )
                if _os.path.exists(dest_path) and _os.path.getsize(dest_path) > 0:
                    return True
                _fails.append("bot api: empty file")
            else:
                _fails.append("bot api: getFile returned empty")
        except _JobCancelledError:
            raise
        except Exception as exc:
            _fails.append(f"bot api: {exc}")
            logger.warning("_download_job_file: Bot API download failed: %s", exc)
    else:
        logger.info(
            "_download_job_file: size %s above Bot API getFile cap, using userbot pipes",
            _format_size(file_size),
        )

    # ── Pipe 2a: userbot file-id download (in-memory; works with file_id
    # only).  Skipped for very large files so a 100MB+ book is never fully
    # buffered in RAM before the streaming disk pipe below is tried.
    _in_mem_max = 200 * 1024 * 1024
    try:
        from utils.bigfile_pipeline import (
            IN_MEMORY_MAX_BYTES as _IMM,  # noqa: N814
        )

        _in_mem_max = int(_IMM)
    except Exception:  # nosec B110 - fall back to a sane default
        pass
    # Only skip the in-memory pipe when a streaming alternative actually
    # exists (chat context present); otherwise attempt it rather than fail.
    _skip_file_id = bool(
        chat_id
        and message_id
        and file_size
        and _in_mem_max
        and file_size > _in_mem_max
    )
    if _skip_file_id:
        logger.info(
            "_download_job_file: size %s above in-memory threshold, skipping file_id pipe",
            _format_size(file_size),
        )
    else:
        try:
            _raise_if_cancelled()
            from utils.userbot_downloader import (
                download_bytes_by_file_id_via_userbot,
            )

            _data = _asyncio.run(
                download_bytes_by_file_id_via_userbot(
                    file_id, progress_callback=progress_callback, user_id=user_id
                )
            )
            if _data:
                with open(dest_path, "wb") as _fh:
                    _fh.write(_data)
                if _os.path.getsize(dest_path) > 0:
                    return True
            _fails.append("userbot file_id: empty")
        except _JobCancelledError:
            raise
        except Exception as exc:
            _fails.append(f"userbot file_id: {exc}")
            logger.warning("_download_job_file: file_id download failed: %s", exc)

    # ── Pipe 2b: userbot chat-based disk download (Telethon -> Pyrogram).
    if chat_id and message_id:
        try:
            _raise_if_cancelled()
            from utils.userbot_downloader import download_forward_via_userbot

            _ok = _asyncio.run(
                download_forward_via_userbot(
                    chat_id,
                    message_id,
                    dest_path,
                    file_unique_id=file_unique_id,
                    progress_callback=progress_callback,
                    file_id=file_id,
                    user_id=user_id,
                )
            )
            if _ok and _os.path.exists(dest_path) and _os.path.getsize(dest_path) > 0:
                return True
            _fails.append("userbot chat: empty")
        except _JobCancelledError:
            raise
        except Exception as exc:
            _fails.append(f"userbot chat: {exc}")
            logger.warning(
                "_download_job_file: chat-based download failed (%s/%s): %s",
                chat_id,
                message_id,
                exc,
            )

    # ── Pipe 2c: relay-group fallback (forward via Bot API, download there).
    if chat_id and message_id:
        try:
            _raise_if_cancelled()
            _relay = getattr(config, "RELAY_CHAT_ID", None)
            if _relay:
                _relay_id = int(_relay)
                _fwd_id = _tg_forward_message(
                    config.BOT_TOKEN, _relay_id, chat_id, message_id
                )
                if _fwd_id:
                    from utils.userbot_downloader import (
                        download_forward_via_userbot,
                    )

                    _ok = _asyncio.run(
                        download_forward_via_userbot(
                            _relay_id,
                            _fwd_id,
                            dest_path,
                            progress_callback=progress_callback,
                            user_id=user_id,
                        )
                    )
                    if _ok and (
                        _os.path.exists(dest_path)
                        and _os.path.getsize(dest_path) > 0
                    ):
                        return True
                    _fails.append("relay: empty")
                else:
                    _fails.append("relay: forwardMessage failed")
            else:
                _fails.append("relay: RELAY_CHAT_ID not configured")
        except _JobCancelledError:
            raise
        except Exception as exc:
            _fails.append(f"relay: {exc}")
            logger.warning("_download_job_file: relay download failed: %s", exc)

    logger.warning(
        "_download_job_file: all download pipes failed for %s: %s",
        filename,
        "; ".join(_fails) or "no attempts",
    )
    return False


def _deliver_converted_file(
    chat_id: int,
    file_path: str,
    filename: str,
    thumb_path: str | None,
    caption: str,
    user_id: int | None,
    progress_msg_id: int | None = None,
    convert_user_id: int | None = None,
    ocr_user_id: int | None = None,
) -> dict | None:
    """Send a converted/compressed result with live progress + cancel respect.

    Returns the sendDocument response dict when the result was delivered via
    the Bot API, or ``{"ok": False, "delivery": "userbot", "src_chat_id": ...,
    "src_message_id": ...}`` when delivered via the userbot (big files) — so
    callers can cache the delivered copy (file_id, or the userbot copy's
    location for forward-based re-sends) for re-send dedup.

    Streams the file via ``_tg_send_document`` (with the Compress-PDF button
    for PDF outputs and the 🔁 Convert button for e-books when
    ``convert_user_id`` is set) and deletes the transient progress message
    when done.  Results too large for the Bot API upload cap fall back to
    userbot delivery with a bot-API Compress/Convert prompt pointing at the
    delivered copy, so conversion/compression results of ANY size arrive.
    Raises when delivery fails entirely so callers can notify the user.
    """
    _cb_state = {"last_pct": -1, "last_t": 0.0}
    _send_res = None

    def _live_cb(recv: int, total: int) -> None:
        if not total:
            return
        pct = int(recv * 100 / total)
        if pct - _cb_state["last_pct"] < 2 and time.time() - _cb_state["last_t"] < 2.0:
            return
        _cb_state["last_pct"] = pct
        _cb_state["last_t"] = time.time()
        try:
            _tg_send_progress(
                chat_id,
                filename,
                "sending",
                detail=(
                    f"\U0001f4e4 Sending to Telegram: "
                    f"{_format_size(recv)} / {_format_size(total)}"
                ),
                file_size=total,
                message_id=progress_msg_id,
                progress_pct=pct,
            )
        except Exception:  # nosec B110 - progress is best-effort
            pass

    # A blank/white (or missing) preview is worse than none — deliver without
    # a thumbnail rather than attaching a white cover.
    if not thumbnail_is_usable(thumb_path):
        logger.info(
            "_deliver_converted_file: skipping unusable thumbnail for %s",
            filename,
        )
        thumb_path = None
    _thumb = None
    try:
        try:
            # Pre-gate uploads by size: a result above the sendDocument cap
            # would only be rejected (400) by the Bot API.  Raise INSIDE this
            # try so the except below routes straight to the userbot pipe
            # (mirroring the download-side getFile gate) instead of burning a
            # doomed Bot API upload attempt on every oversized result.
            _ul_limit = getattr(config, "BOT_API_UPLOAD_LIMIT_BYTES", 0)
            if _ul_limit and os.path.getsize(file_path) > _ul_limit:
                logger.info(
                    "_deliver_converted_file: %s exceeds Bot API upload cap "
                    "(%s), routing to the userbot pipe",
                    filename,
                    _format_size(_ul_limit),
                )
                raise ValueError("result exceeds Bot API upload cap")
            if thumb_path and os.path.exists(thumb_path):
                _thumb = open(thumb_path, "rb")
            with open(file_path, "rb") as _doc:
                _send_res = _tg_send_document(
                    None,
                    chat_id,
                    _doc,
                    filename,
                    thumb_fileobj=_thumb,
                    caption=caption,
                    progress_callback=_live_cb,
                    compress_user_id=user_id,
                    convert_user_id=convert_user_id,
                    ocr_user_id=ocr_user_id or user_id,
                )
        except Exception:
            # Too large for a Bot API upload (sendDocument cap): deliver via
            # the userbot and post a bot-API prompt pointing at the delivered
            # copy, so the Compress/Convert interface survives big results too.
            logger.warning(
                "_deliver_converted_file: Bot API send failed for %s, "
                "trying userbot",
                filename,
            )
            _res = _deliver_result_via_userbot(
                chat_id, file_path, filename, caption, thumb_path, user_id
            )
            if not _res:
                raise
            _sent, _src_chat = _res
            _sent_id = getattr(_sent, "id", None)
            try:
                _dl_size = 0
                try:
                    _dl_size = os.path.getsize(file_path)
                except Exception:  # nosec B110
                    pass
                _name = filename or ""
                _is_pdf = _name.lower().endswith(".pdf")
                if _is_pdf:
                    _tg_send_pending_prompt(
                        *COMPRESS_PDF_ACTION,
                        chat_id=chat_id,
                        filename=filename,
                        user_id=user_id,
                        file_size=_dl_size,
                        src_chat_id=_src_chat,
                        src_message_id=_sent_id,
                        extra_action=(
                            (OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2])
                            if ocr_enabled()
                            else None
                        ),
                    )
                elif is_ocr_source(_name):
                    if ocr_enabled():
                        _tg_send_pending_prompt(
                            *OCR_ACTION,
                            chat_id=chat_id,
                            filename=filename,
                            user_id=user_id,
                            file_size=_dl_size,
                            src_chat_id=_src_chat,
                            src_message_id=_sent_id,
                        )
                elif convert_user_id:
                    _tg_send_pending_prompt(
                        *BOOK_CONVERT_ACTION,
                        chat_id=chat_id,
                        filename=filename,
                        user_id=user_id,
                        file_size=_dl_size,
                        src_chat_id=_src_chat,
                        src_message_id=_sent_id,
                    )
            except Exception:  # nosec B110 - the prompt is best-effort
                logger.exception(
                    "_deliver_converted_file: failed to attach prompt for %s",
                    filename,
                )
            # Surface the userbot copy's location so callers can cache it for
            # forward-based re-sends (no Bot API file_id exists for it).
            _send_res = {
                "ok": False,
                "delivery": "userbot",
                "src_chat_id": _src_chat,
                "src_message_id": _sent_id,
            }
        # Delivery done: remove the transient progress bar (mirrors
        # _cleanup_after_success in the other worker flows).
        try:
            _tg_delete_message(chat_id, progress_msg_id)
        except Exception:  # nosec B110
            pass
    finally:
        try:
            if _thumb:
                _thumb.close()
        except Exception:  # nosec B110
            pass
    return _send_res


def _deliver_book_echo(
    chat_id: int,
    file_path: str,
    filename: str,
    user_id: int | None,
    progress_msg_id: int | None = None,
) -> bool:
    """Echo a local e-book back with a one-tap 🔁 Convert button.

    Shared by ``deliver_book_job`` and the URL flow so every entry point uses
    the same conversion-only delivery.  The Convert button is only attached
    when Calibre is installed; oversized books fall back to userbot delivery
    (a bot-API Convert prompt still follows, so big books keep the full
    conversion interface).  Returns True on delivery.
    """
    _convert_uid = user_id if calibre_available() else None
    _caption = (
        "\U0001f4da Here's your book. Tap the Convert button to re-format it."
        if _convert_uid
        else "\U0001f4da Here's your book."
    )
    try:
        _deliver_converted_file(
            chat_id=chat_id,
            file_path=file_path,
            filename=filename,
            thumb_path=None,
            caption=_caption,
            user_id=user_id,
            progress_msg_id=progress_msg_id,
            convert_user_id=_convert_uid,
        )
        return True
    except Exception:
        logger.warning(
            "_deliver_book_echo: delivery failed for %s", filename
        )
        return False


def deliver_book_job(
    chat_id: int,
    file_id: str,
    filename: str,
    mime: str | None = "",
    user_id: int | None = None,
    file_unique_id: str | None = None,
    message_id: int | None = None,
    forward_info: dict | None = None,
    file_size: int | None = None,
    _skip_queued_delete: bool = False,
) -> dict:
    """RQ job: echo an e-book back with a one-tap 🔁 Convert button.

    Conversion deliberately has its own clean interface, separate from the
    thumbnail flow: the book is downloaded (shared Bot-API <20MB / userbot
    pipe), sent back with a Convert button, and nothing else is processed
    until the user picks a target format.  ``_skip_queued_delete`` keeps the
    caller's "Queued..." confirmation (batch items) from being deleted.
    Returns a result dict.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "deliver_book_job: start chat_id=%s user_id=%s file=%s size=%s",
        chat_id,
        user_id,
        filename,
        file_size,
    )
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "filename": filename,
                    "mime": mime,
                    "file_size": file_size,
                    "message_id": message_id,
                    "forward_info": forward_info,
                    "enqueued_at": int(time.time()),
                },
            )
            out_meta = {
                "status": "processing",
                "timestamps": {"start": int(time.time())},
                "user_id": user_id,
            }
            _set_io_keys(_rq_job_id, output_meta=out_meta)
        except Exception:
            logger.exception(
                "Failed to write io:in for deliver job %s", _rq_job_id
            )
    _cancel_check_id = _rq_job_id or file_id
    if _job_cancelled(_cancel_check_id):
        return {"status": "cancelled"}

    tmpdir = (
        tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        if getattr(config, "TMP_DIR", None)
        else tempfile.mkdtemp()
    )
    _progress_msg_id = None
    try:
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "downloading",
            detail="\U0001f4e5 Downloading book...",
            file_size=file_size or 0,
        )
        _src = os.path.join(tmpdir, _safe_local_filename(filename))
        _dl_state: dict[str, float] = {
            "msg_id": float(_progress_msg_id or 0),
            "last_pct": -1.0,
            "last_t": 0.0,
        }

        def _dl_cb(recv: int, total: int) -> None:
            _download_progress_cb(chat_id, filename, recv, total, _dl_state)

        try:
            _ok = _download_job_file(
                file_id,
                _src,
                filename,
                file_size,
                user_id,
                progress_callback=_dl_cb,
                chat_id=chat_id,
                message_id=message_id,
                file_unique_id=file_unique_id,
                cancel_check=lambda: _job_cancelled(_cancel_check_id),
            )
        except _JobCancelledError:
            _cleanup_after_failure(
                chat_id,
                _rq_job_id,
                int(_dl_state["msg_id"]) or _progress_msg_id,
                skip_queued_delete=_skip_queued_delete,
            )
            return {"status": "cancelled"}
        _progress_msg_id = int(_dl_state["msg_id"]) or _progress_msg_id
        if not _ok:
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\u274c Failed to download the book. "
                    "Try again in a moment.",
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(
                chat_id,
                _rq_job_id,
                _progress_msg_id,
                skip_queued_delete=_skip_queued_delete,
            )
            return {"error": "download_failed"}

        # Deliver to the original chat (the user's DM with the bot) — same
        # place the progress message lives, so live edits and cleanup line up
        # (mirrors convert_book_job).  The userbot fallback inside the echo
        # helper targets the bot DM internally.
        if not _deliver_book_echo(
            chat_id, _src, filename, user_id, _progress_msg_id
        ):
            raise RuntimeError("book echo delivery failed")
        _cleanup_after_success(
            chat_id,
            _rq_job_id,
            _progress_msg_id,
            skip_queued_delete=_skip_queued_delete,
        )
        try:
            if _rq_job_id:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "done",
                        "delivery": "bot_api",
                        "timestamps": {"finished": int(time.time())},
                        "user_id": user_id,
                    },
                )
        except Exception:  # nosec B110
            pass
        return {"status": "done", "delivery": "bot_api"}
    except Exception as exc:
        logger.exception(
            "deliver_book_job: failed for %s (chat=%s user_id=%s)",
            filename,
            chat_id,
            user_id,
        )
        _cleanup_after_failure(
            chat_id,
            _rq_job_id,
            _progress_msg_id,
            skip_queued_delete=_skip_queued_delete,
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error delivering the book: "
                f"{_short_error(exc)}\nCheck server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        return {"error": "deliver_failed"}
    finally:
        try:
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


def convert_book_job(
    chat_id: int,
    file_id: str,
    filename: str,
    mime: str | None = "",
    target_fmt: str = "pdf",
    user_id: int | None = None,
    file_unique_id: str | None = None,
    message_id: int | None = None,
    forward_info: dict | None = None,
    file_size: int | None = None,
    source_chat_id: int | str | None = None,
    compress: bool = False,
) -> dict:
    """RQ job: convert an e-book to ``target_fmt`` and deliver it.

    Downloads the file (Bot API, userbot fallback for large files), converts
    with Calibre's ``ebook-convert``, attaches a cover thumbnail for PDF
    outputs, and streams the result back with live progress.  The delivered
    PDF carries a one-tap Compress button.  ``source_chat_id`` overrides the
    chat used for the download when the book was delivered elsewhere (e.g. a
    large book re-downloaded from the userbot's DM/Saved Messages copy);
    delivery still targets ``chat_id``.  ``compress=True`` (the 🗜 Compress
    PDF menu button) additionally shrinks a PDF target with ``compress_pdf``
    before delivery, so a book is converted-to-PDF-then-compressed in one
    job.  Returns a result dict.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "convert_book_job: start chat_id=%s user_id=%s file=%s target=%s",
        chat_id,
        user_id,
        filename,
        target_fmt,
    )
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "filename": filename,
                    "target_fmt": target_fmt,
                    "enqueued_at": int(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to write io:in for convert job %s", _rq_job_id)
    _cancel_check_id = _rq_job_id or file_id
    if _job_cancelled(_cancel_check_id):
        return {"status": "cancelled"}

    tmpdir = (
        tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        if getattr(config, "TMP_DIR", None)
        else tempfile.mkdtemp()
    )
    _progress_msg_id = None
    try:
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "downloading",
            detail="\U0001f4e5 Downloading...",
            file_size=file_size or 0,
        )
        _src = os.path.join(tmpdir, _safe_local_filename(filename))
        _dl_state: dict[str, float] = {
            "msg_id": float(_progress_msg_id or 0),
            "last_pct": -1.0,
            "last_t": 0.0,
        }

        def _dl_cb(recv: int, total: int) -> None:
            _download_progress_cb(chat_id, filename, recv, total, _dl_state)

        try:
            _ok = _download_job_file(
                file_id,
                _src,
                filename,
                file_size,
                user_id,
                progress_callback=_dl_cb,
                # Large books are re-downloaded from the delivered copy's chat
                # (userbot DM/Saved Messages), not the original chat.
                chat_id=source_chat_id or chat_id,
                message_id=message_id,
                file_unique_id=file_unique_id,
                cancel_check=lambda: _job_cancelled(_cancel_check_id),
            )
        except _JobCancelledError:
            _cleanup_after_failure(
                chat_id,
                _rq_job_id,
                int(_dl_state["msg_id"]) or _progress_msg_id,
            )
            return {"status": "cancelled"}
        # Sync the live (possibly recreated) progress message id back so the
        # follow-up phase-switch edit and failure cleanup target the right msg.
        _progress_msg_id = int(_dl_state["msg_id"]) or _progress_msg_id
        if not _ok:
            _tg_send_progress(
                chat_id, filename, "failed",
                detail="\u274c Failed to download the file.",
                message_id=_progress_msg_id,
            )
            return {"error": "download_failed"}

        if _job_cancelled(_cancel_check_id):
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "cancelled"}

        # ── Content-hash dedup: this exact book already converted ─────────
        # Keyed on the INPUT e-book's sha256 — a re-send of the same book
        # (even with a fresh fuid) re-sends the cached converted copy instead
        # of re-running Calibre.  The deliverable depends on the target
        # (``pdf:compress`` = convert-to-PDF + shrink), so a record for a
        # DIFFERENT target is never re-sent — the job converts fresh.  NOTE:
        # convert records are PER-TARGET (``ops:convert:pdf`` etc) —
        # converting the same book to a different format keeps BOTH formats'
        # cached copies, so alternating formats never re-run Calibre.
        _content_hash = None
        try:
            _content_hash = content_sha256_file(_src)
        except Exception:  # nosec B110 - dedup unavailable; run normally
            logger.debug("Failed to hash %s", _src)
        _bind_fuid_content(file_unique_id, _content_hash)
        _convert_target_key = f"{target_fmt}:compress" if compress else target_fmt
        _dedup_src = (
            _shortcircuit_if_processed(
                _content_hash,
                "convert",
                chat_id,
                filename=filename,
                user_id=user_id,
                expected_target=_convert_target_key,
                note=(
                    "\u267b\ufe0f Already converted to this format — re-sent "
                    "the cached result. No new job was started."
                ),
            )
            if _content_hash
            else None
        )
        if _dedup_src:
            try:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "already_processed",
                        "resend_source": _dedup_src,
                        "target_fmt": target_fmt,
                        "skipped": True,
                        "timestamps": {"finished": int(time.time())},
                    },
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "already_processed", "skipped": True}

        _tg_send_progress(
            chat_id, filename, "compressing",
            detail=f"\u2699\ufe0f Converting to {target_fmt.upper()}...",
            message_id=_progress_msg_id,
        )
        _out_name = safe_target_name(filename, target_fmt)
        _out = os.path.join(tmpdir, _out_name)
        _timeout = getattr(config, "BOOK_CONVERT_TIMEOUT_SECONDS", 600)
        _thumb = None
        _thumb_path = None
        try:
            if target_fmt.lower() == "pdf":
                _conv_ok = convert_book_to_pdf_with_thumbnail(
                    _src, _out, os.path.join(tmpdir, "thumb.jpg"),
                    timeout=_timeout,
                    cancel_check=lambda: _job_cancelled(_cancel_check_id),
                )
                _thumb_path = os.path.join(tmpdir, "thumb.jpg")
            else:
                _conv_ok = convert_ebook_robust(
                    _src, _out, timeout=_timeout,
                    cancel_check=lambda: _job_cancelled(_cancel_check_id),
                )
        except ConversionCancelledError:
            logger.info(
                "convert_book_job: cancelled mid-conversion for %s", filename
            )
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "cancelled"}
        if not _conv_ok or not os.path.exists(_out):
            _tg_send_progress(
                chat_id, filename, "failed",
                detail=(
                    f"\u274c Conversion to {target_fmt.upper()} failed. "
                    "The file may be DRM-protected or corrupt — check the "
                    "server logs for the converter's error output."
                ),
                message_id=_progress_msg_id,
            )
            # Keep the failed progress message VISIBLE: it is the user's only
            # feedback when a conversion dies mid-run.  Only drop the
            # transient "Queued..." confirmation.
            _delete_queued_messages(_rq_job_id)
            return {"error": "conversion_failed"}

        if _job_cancelled(_cancel_check_id):
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "cancelled"}

        _caption = f"Here is your file converted to {target_fmt.upper()}."
        # 🗜 Compress for a book = convert-to-PDF then shrink (the menu's
        # Compress PDF button): run compress_pdf on the converted output and
        # deliver the smaller copy.  Best-effort — a failed compression still
        # delivers the converted PDF with the original caption.
        if compress and target_fmt.lower() == "pdf":
            _comp_path = os.path.join(tmpdir, "compressed_" + _out_name)
            _gs = getattr(config, "PDF_COMPRESS_QUALITY", "/ebook")
            try:
                if compress_pdf(_out, _comp_path, gs_quality=_gs) and (
                    os.path.exists(_comp_path)
                    and os.path.getsize(_comp_path)
                ):
                    _out = _comp_path
                    _caption = (
                        "Here is your book converted to PDF and compressed."
                    )
                    # Refresh the thumbnail from the compressed PDF (best
                    # effort).  Render to a temp path and only swap it in
                    # when usable — a blank render of the compressed PDF must
                    # never clobber the pre-compression cover.
                    try:
                        if _thumb_path:
                            _rethumb = _thumb_path + ".rethumb.jpg"
                            create_thumbnail_from_pdf(_out, _rethumb)
                            if thumbnail_is_usable(_rethumb):
                                os.replace(_rethumb, _thumb_path)
                            else:
                                os.remove(_rethumb)
                    except Exception:  # nosec B110 - best-effort
                        pass
            except Exception:  # nosec B110 - compression best-effort
                logger.warning(
                    "convert_book_job: compression failed for %s, "
                    "delivering converted PDF only",
                    filename,
                )
        _conv_res = _deliver_converted_file(
            chat_id, _out, _out_name, _thumb_path, _caption, user_id,
            progress_msg_id=_progress_msg_id,
        )
        # Cache the delivered converted copy for re-send dedup (both delivery
        # modes: Bot API file_id, or the userbot copy's location to forward).
        if _conv_res and _conv_res.get("ok"):
            try:
                _doc = (_conv_res.get("result") or {}).get("document") or {}
                upsert_processed_record(
                    _content_hash,
                    "convert",
                    "done",
                    filename=filename,
                    file_size=file_size,
                    target=_convert_target_key,
                    file_id=_doc.get("file_id"),
                    thumb_file_id=(_doc.get("thumbnail") or {}).get(
                        "file_id"
                    ),
                    user_id=user_id,
                    chat_id=chat_id,
                )
            except Exception:  # nosec B110 - cache is best-effort
                pass
        elif _conv_res and _conv_res.get("delivery") == "userbot":
            _cache_userbot_delivered_copy(
                _content_hash,
                filename,
                file_size,
                "convert",
                _conv_res.get("src_chat_id"),
                _conv_res.get("src_message_id"),
                user_id=user_id,
                chat_id=chat_id,
                target=_convert_target_key,
            )
        out_meta = {
            "status": "done",
            "filename": _out_name,
            "target_fmt": target_fmt,
            "timestamps": {"finished": int(time.time())},
        }
        try:
            _set_io_keys(_rq_job_id, output_meta=out_meta)
        except Exception:  # nosec B110
            pass
        _delete_queued_messages(_rq_job_id)
        return {"status": "done", "converted_to": target_fmt}
    except Exception as exc:
        logger.exception("convert_book_job: error for %s", filename)
        _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error converting the file: "
                f"{_short_error(exc)}\nCheck server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        try:
            _set_io_keys(
                _rq_job_id,
                output_meta={"status": "error", "error": str(exc)},
            )
        except Exception:  # nosec B110
            pass
        return {"error": str(exc)}
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


def compress_pdf_job(
    chat_id: int,
    file_id: str,
    filename: str,
    user_id: int | None = None,
    file_unique_id: str | None = None,
    message_id: int | None = None,
    file_size: int | None = None,
    forward_info: dict | None = None,
    source_chat_id: int | str | None = None,
) -> dict:
    """RQ job: compress a delivered PDF and send the smaller version back.

    Triggered by the 🗜 Compress PDF button on a delivered result.  Downloads
    via the shared Bot-API + userbot pipe chain (so any delivered size works),
    compresses with ``compress_pdf`` (Ghostscript with PyMuPDF fallback) and
    streams the result back with live progress.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "compress_pdf_job: start chat_id=%s user_id=%s file=%s size=%s",
        chat_id, user_id, filename, file_size,
    )
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "filename": filename,
                    "file_size": file_size,
                    "message_id": message_id,
                    "forward_info": forward_info,
                    "enqueued_at": int(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to write io:in for compress job %s", _rq_job_id)
    _cancel_check_id = _rq_job_id or file_id
    if _job_cancelled(_cancel_check_id):
        return {"status": "cancelled"}

    tmpdir = (
        tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        if getattr(config, "TMP_DIR", None)
        else tempfile.mkdtemp()
    )
    _progress_msg_id = None
    try:
        _progress_msg_id = _tg_send_progress(
            chat_id, filename, "downloading",
            detail="\U0001f4e5 Downloading PDF...",
        )
        _src = os.path.join(tmpdir, _safe_local_filename(filename))
        _dl_state: dict[str, float] = {
            "msg_id": float(_progress_msg_id or 0),
            "last_pct": -1.0,
            "last_t": 0.0,
        }

        def _dl_cb(recv: int, total: int) -> None:
            _download_progress_cb(chat_id, filename, recv, total, _dl_state)

        try:
            _ok = _download_job_file(
                file_id,
                _src,
                filename,
                file_size,
                user_id,
                progress_callback=_dl_cb,
                # Large PDFs are re-downloaded from the delivered copy's chat
                # (userbot DM/Saved Messages), not the original chat.
                chat_id=source_chat_id or chat_id,
                message_id=message_id,
                file_unique_id=file_unique_id,
                cancel_check=lambda: _job_cancelled(_cancel_check_id),
            )
        except _JobCancelledError:
            _cleanup_after_failure(
                chat_id,
                _rq_job_id,
                int(_dl_state["msg_id"]) or _progress_msg_id,
            )
            return {"status": "cancelled"}
        # Sync the live (possibly recreated) progress message id back so the
        # follow-up phase-switch edit and failure cleanup target the right msg.
        _progress_msg_id = int(_dl_state["msg_id"]) or _progress_msg_id
        if not _ok:
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\u274c Failed to download the PDF. "
                    "Try again in a moment.",
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"error": "download_failed"}

        # ── Content-hash dedup: this exact PDF already compressed ─────────
        # Keyed on the INPUT bytes' sha256 — re-sends of the same PDF (even
        # with a fresh fuid) re-send the cached compressed copy instead of
        # re-running Ghostscript.  Different files never collide on the key.
        _content_hash = None
        try:
            _content_hash = content_sha256_file(_src)
        except Exception:  # nosec B110 - dedup unavailable; run normally
            logger.debug("Failed to hash %s", _src)
        _bind_fuid_content(file_unique_id, _content_hash)
        _dedup_src = (
            _shortcircuit_if_processed(
                _content_hash,
                "compress",
                chat_id,
                filename=filename,
                user_id=user_id,
                note=(
                    "\u267b\ufe0f Already processed — re-sent the cached "
                    "result. No new job was started."
                ),
            )
            if _content_hash
            else None
        )
        if _dedup_src:
            try:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "already_processed",
                        "resend_source": _dedup_src,
                        "skipped": True,
                        "timestamps": {"finished": int(time.time())},
                    },
                )
            except Exception:  # nosec B110
                pass
            # Remove the transient download-progress + "Queued..." messages
            # (the re-sent cached copy is the only feedback the user needs).
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "already_processed", "skipped": True}

        _progress_msg_id = _tg_send_progress(
            chat_id, filename, "compressing",
            detail="\U0001f5dc\ufe0f Compressing PDF...",
            message_id=_progress_msg_id,
        )

        _out_name = safe_target_name(filename, "pdf")
        _out = os.path.join(tmpdir, _out_name)
        _gs = getattr(config, "PDF_COMPRESS_QUALITY", "/ebook")
        if not compress_pdf(_src, _out, gs_quality=_gs):
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\u274c PDF compression failed. The file may be "
                    "corrupt or password-protected.",
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"error": "compression_failed"}
        if not os.path.exists(_out) or not os.path.getsize(_out):
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\u274c PDF compression produced an empty file. "
                    "Try again in a moment.",
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"error": "compression_failed"}

        _orig = os.path.getsize(_src)
        _comp = os.path.getsize(_out)
        _saved = max(0, int((1 - _comp / _orig) * 100)) if _orig else 0

        # ── Validator: "already compressed" — keep the original ───────────
        # Re-encoding a PDF Ghostscript can barely shrink wastes CPU and
        # re-uploads a file already in the user's chat.  Skip delivery when
        # the gain is below BOTH floors (percent AND absolute bytes) — a big
        # file that still sheds 25MB at 5% is worth keeping, a tiny one isn't.
        _min_gain_pct = float(getattr(config, "COMPRESS_MIN_GAIN_PCT", 5) or 5)
        _min_gain_bytes = int(
            getattr(config, "COMPRESS_MIN_GAIN_BYTES", 100_000) or 100_000
        )
        if _orig and _saved < _min_gain_pct and max(0, _orig - _comp) < _min_gain_bytes:
            upsert_processed_record(
                _content_hash,
                "compress",
                "skipped",
                filename=filename,
                file_size=file_size,
                user_id=user_id,
                chat_id=chat_id,
            )
            _tg_send_message(
                None,
                chat_id,
                "✅ This PDF is already well-compressed: "
                f"{_format_size(_orig)} → {_format_size(_comp)} "
                f"(only {_saved}% smaller). Kept the original — "
                "no meaningful gain from re-encoding.",
            )
            try:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "already_compressed",
                        "orig_bytes": _orig,
                        "compressed_bytes": _comp,
                        "saved_pct": _saved,
                        "timestamps": {"finished": int(time.time())},
                    },
                )
            except Exception:  # nosec B110
                pass
            _delete_queued_messages(_rq_job_id)
            return {"status": "already_compressed", "saved_pct": 0}

        _thumb_path = os.path.join(tmpdir, "thumb.jpg")
        try:
            create_thumbnail_from_pdf(_out, _thumb_path)
        except Exception:  # nosec B110 - thumbnail best-effort
            _thumb_path = None

        _caption = f"Here is your compressed PDF ({_saved}% smaller)."
        _comp_res = _deliver_converted_file(
            chat_id, _out, _out_name, _thumb_path, _caption, user_id,
            progress_msg_id=_progress_msg_id,
        )
        if _comp_res and _comp_res.get("ok"):
            try:
                _doc = (_comp_res.get("result") or {}).get("document") or {}
                upsert_processed_record(
                    _content_hash,
                    "compress",
                    "done",
                    filename=filename,
                    file_size=file_size,
                    file_id=_doc.get("file_id"),
                    thumb_file_id=(_doc.get("thumbnail") or {}).get(
                        "file_id"
                    ),
                    user_id=user_id,
                    chat_id=chat_id,
                )
            except Exception:  # nosec B110 - cache is best-effort
                pass
        elif _comp_res and _comp_res.get("delivery") == "userbot":
            _cache_userbot_delivered_copy(
                _content_hash,
                filename,
                file_size,
                "compress",
                _comp_res.get("src_chat_id"),
                _comp_res.get("src_message_id"),
                user_id=user_id,
                chat_id=chat_id,
            )
        try:
            _set_io_keys(
                _rq_job_id,
                output_meta={
                    "status": "done",
                    "orig_bytes": _orig,
                    "compressed_bytes": _comp,
                    "saved_pct": _saved,
                    "timestamps": {"finished": int(time.time())},
                },
            )
        except Exception:  # nosec B110
            pass
        _delete_queued_messages(_rq_job_id)
        return {"status": "done", "saved_pct": _saved}
    except Exception as exc:
        logger.exception("compress_pdf_job: error for %s", filename)
        _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error compressing the PDF: "
                f"{_short_error(exc)}\nCheck server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        try:
            _set_io_keys(
                _rq_job_id,
                output_meta={"status": "error", "error": str(exc)},
            )
        except Exception:  # nosec B110
            pass
        return {"error": str(exc)}
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


def _build_pdf_output_preview(
    src: str,
    filename: str,
    file_unique_id: str | None,
    tmpdir: str,
) -> str | None:
    """Best-effort first-page/cover preview for a PDF OCR result.

    Reuses the cached embedded-thumbnail result when known (extract instead
    of rendering page 1 at 2x); falls back to a blank-skipping page render
    for PDFs and an image render for image sources.  Returns the thumb path
    or ``None`` (a missing preview never fails the job).
    """
    thumb_path = os.path.join(tmpdir, "thumb.jpg")
    try:
        if filename.lower().endswith(".pdf"):
            checks = _get_pdf_checks(file_unique_id)
            has_thumb = checks["has_thumb"] if checks is not None else None
            if has_thumb is False:
                create_thumbnail_from_pdf(src, thumb_path)
            elif not extract_pdf_embedded_thumbnail(src, thumb_path):
                create_thumbnail_from_pdf(src, thumb_path)
        else:
            create_thumbnail_from_image(src, thumb_path)
    except Exception:  # nosec B110 - preview is best-effort
        return None
    return thumb_path


def ocr_job(
    chat_id: int,
    file_id: str,
    filename: str,
    user_id: int | None = None,
    file_unique_id: str | None = None,
    message_id: int | None = None,
    forward_info: dict | None = None,
    file_size: int | None = None,
    source_chat_id: int | str | None = None,
    target: str = "txt",
) -> dict:
    """RQ job: OCR a delivered PDF/image and send the result back.

    Triggered by the 🔎 OCR button (then a format pick) on a delivered result.
    Downloads via the shared Bot-API + userbot pipe chain (so any delivered
    size works), then either:

    - ``target="txt"`` (default): Tesseract via PyMuPDF/pytesseract → ``.txt``;
    - ``target="pdf"``: ocrmypdf sandwiches an INVISIBLE text layer over the
      ORIGINAL page rendering → searchable PDF that looks identical to the
      input but has selectable/copyable text.

    ``source_chat_id`` overrides the download chat when the file was
    userbot-delivered elsewhere.  Returns a result dict.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "ocr_job: start chat_id=%s user_id=%s file=%s size=%s",
        chat_id, user_id, filename, file_size,
    )
    # E-books get converted to PDF mid-job (filename is reassigned below);
    # the processed-record key must stay the ORIGINAL filename so a re-send
    # of the original e-book matches the cached OCR result.
    _orig_filename = filename
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "filename": filename,
                    "file_size": file_size,
                    "message_id": message_id,
                    "forward_info": forward_info,
                    "enqueued_at": int(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to write io:in for ocr job %s", _rq_job_id)
    _cancel_check_id = _rq_job_id or file_id
    if _job_cancelled(_cancel_check_id):
        return {"status": "cancelled"}

    tmpdir = (
        tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        if getattr(config, "TMP_DIR", None)
        else tempfile.mkdtemp()
    )
    _progress_msg_id = None
    try:
        _progress_msg_id = _tg_send_progress(
            chat_id, filename, "downloading",
            detail="\U0001f4e5 Downloading file...",
            file_size=file_size or 0,
        )
        _src = os.path.join(tmpdir, _safe_local_filename(filename))
        _dl_state: dict[str, float] = {
            "msg_id": float(_progress_msg_id or 0),
            "last_pct": -1.0,
            "last_t": 0.0,
        }

        def _dl_cb(recv: int, total: int) -> None:
            _download_progress_cb(chat_id, filename, recv, total, _dl_state)

        try:
            _ok = _download_job_file(
                file_id,
                _src,
                filename,
                file_size,
                user_id,
                progress_callback=_dl_cb,
                # Large files are re-downloaded from the delivered copy's chat
                # (userbot DM/Saved Messages), not the original chat.
                chat_id=source_chat_id or chat_id,
                message_id=message_id,
                file_unique_id=file_unique_id,
                cancel_check=lambda: _job_cancelled(_cancel_check_id),
            )
        except _JobCancelledError:
            _cleanup_after_failure(
                chat_id,
                _rq_job_id,
                int(_dl_state["msg_id"]) or _progress_msg_id,
            )
            return {"status": "cancelled"}
        _progress_msg_id = int(_dl_state["msg_id"]) or _progress_msg_id
        if not _ok:
            try:
                _tg_send_message(
                    None,
                    chat_id,
                    "\u274c Failed to download the file. Try again in a moment.",
                )
            except Exception:  # nosec B110
                pass
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"error": "download_failed"}

        # ── Content-hash dedup: this exact content already OCR'd ──────────
        # Hash the ORIGINAL downloaded bytes (before any book->PDF conversion
        # reassigns _src) so a re-send of the original matches the record.
        # Two different files sharing a name+size can never collide on the
        # sha256 key.
        _content_hash = None
        try:
            _content_hash = content_sha256_file(_src)
        except Exception:  # nosec B110 - dedup unavailable; run normally
            logger.debug("Failed to hash %s", _src)
        _bind_fuid_content(file_unique_id, _content_hash)
        _dedup_src = (
            _shortcircuit_if_processed(
                _content_hash,
                "ocr",
                chat_id,
                filename=filename,
                user_id=user_id,
                note=(
                    "\u267b\ufe0f Already processed — re-sent the cached "
                    "result. No new job was started."
                ),
            )
            if _content_hash
            else None
        )
        if _dedup_src:
            try:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "already_processed",
                        "resend_source": _dedup_src,
                        "skipped": True,
                        "timestamps": {"finished": int(time.time())},
                    },
                )
            except Exception:  # nosec B110
                pass
            # Remove the transient download-progress + "Queued..." messages
            # (the re-sent cached copy is the only feedback the user needs).
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "already_processed", "skipped": True}

        # PDF outputs carry a cover preview — the user-facing thumbnail rule:
        # ebook→PDF results get the extracted cover/first page, everything else
        # (txt, other converted formats) ships as a plain document.
        _thumb_path = None
        # Whether this job converted an e-book to PDF mid-run (the user does
        # not otherwise hold the converted file — used to decide whether the
        # "already has a text layer" short-circuit should DELIVER the PDF).
        _converted_book = False
        # E-books aren't OCR-able directly — convert to PDF with Calibre first,
        # then OCR the PDF (both searchable-PDF and text extraction work on it).
        if is_book_format(filename) and not filename.lower().endswith(".pdf"):
            _pdf_name = safe_target_name(filename, "pdf")
            _pdf_src = os.path.join(tmpdir, _pdf_name)
            _conv_timeout = getattr(config, "BOOK_CONVERT_TIMEOUT_SECONDS", 600)
            _tg_send_progress(
                chat_id, filename, "compressing",
                detail="\U0001f4d5 Converting book to PDF for OCR...",
                message_id=_progress_msg_id,
            )
            try:
                _conv_ok = convert_book_to_pdf_with_thumbnail(
                    _src, _pdf_src, os.path.join(tmpdir, "thumb.jpg"),
                    timeout=_conv_timeout,
                    cancel_check=lambda: _job_cancelled(_cancel_check_id),
                )
            except ConversionCancelledError:
                logger.info(
                    "ocr_job: cancelled mid book->PDF conversion for %s",
                    filename,
                )
                _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
                return {"status": "cancelled"}
            if not _conv_ok or not os.path.exists(_pdf_src):
                _tg_send_progress(
                    chat_id, filename, "failed",
                    detail=(
                        "\u274c Couldn't convert the book to PDF for OCR. "
                        "The file may be DRM-protected or corrupt."
                    ),
                    message_id=_progress_msg_id,
                )
                _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
                return {"error": "conversion_failed"}
            _src = _pdf_src
            filename = _pdf_name
            _converted_book = True
            # The intermediate conversion already extracted the book's cover
            # (or first page) into thumb.jpg — reuse it ONLY for a PDF output;
            # a txt extraction stays a plain document (the user-facing rule:
            # PDF results carry a preview, other formats ship bare).
            if target == "pdf":
                _thumb_path = os.path.join(tmpdir, "thumb.jpg")

        if _job_cancelled(_cancel_check_id):
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "cancelled"}

        _is_pdf_out = target == "pdf"

        # ── Validator: "already OCR'd" — skip the whole OCR pass ──────────
        # A born-digital or previously-OCR'd PDF already carries a text layer;
        # re-running ocrmypdf/tesseract would burn a full download + engine
        # pass for zero gain.  Detect the layer cheaply on the already
        # downloaded file and short-circuit.
        _src_is_pdf = filename.lower().endswith(".pdf")
        _checks = _get_pdf_checks(file_unique_id) if _src_is_pdf else None
        if _checks is not None and _checks["has_text_layer"] is not None:
            # Reuse the cached result (the thumbnail job computed it on its own
            # download — same file, immutable content) instead of re-running
            # the text-layer pass here.
            _already_ocr = bool(_checks["has_text_layer"])
        else:
            _already_ocr = _src_is_pdf and pdf_has_text_layer(_src)
            if _src_is_pdf:
                _store_pdf_checks(
                    file_unique_id,
                    has_text_layer=_already_ocr,
                    content_hash=_content_hash,
                )
        if _already_ocr and _is_pdf_out:
            # The file already carries a searchable text layer — no OCR pass
            # is needed.  But the button promised the searchable PDF, so
            # deliver it: books ship the Calibre-converted PDF (which the user
            # never received); direct PDFs get the original file back.
            if _converted_book:
                _caption = (
                    "\U0001f4d6 Here is your book as a searchable PDF — the "
                    "converted file already has a text layer, so no OCR "
                    "was needed."
                )
            else:
                # Direct PDF: build a cover preview so the re-sent file
                # carries one (best-effort — a missing preview is dropped at
                # delivery rather than shipping a white box).
                _thumb_path = _build_pdf_output_preview(
                    _src, filename, file_unique_id, tmpdir
                )
                _caption = (
                    "\U0001f4c4 Here is your PDF back — it already has a "
                    "searchable text layer, so no OCR was needed."
                )
            _ocr_res = _deliver_converted_file(
                chat_id, _src, filename, _thumb_path, _caption, user_id,
                progress_msg_id=_progress_msg_id,
            )
            if _ocr_res and _ocr_res.get("ok"):
                try:
                    _doc = (_ocr_res.get("result") or {}).get(
                        "document"
                    ) or {}
                    upsert_processed_record(
                        _content_hash,
                        "ocr",
                        "done",
                        filename=_orig_filename,
                        file_size=file_size,
                        file_id=_doc.get("file_id"),
                        thumb_file_id=(_doc.get("thumbnail") or {}).get(
                            "file_id"
                        ),
                        user_id=user_id,
                        chat_id=chat_id,
                        target=target,
                    )
                except Exception:  # nosec B110 - cache is best-effort
                    pass
            elif _ocr_res and _ocr_res.get("delivery") == "userbot":
                _cache_userbot_delivered_copy(
                    _content_hash,
                    _orig_filename,
                    file_size,
                    "ocr",
                    _ocr_res.get("src_chat_id"),
                    _ocr_res.get("src_message_id"),
                    user_id=user_id,
                    chat_id=chat_id,
                    target=target,
                )
            if _ocr_res and not _converted_book:
                # Direct PDF delivered with its preview — publish
                # has_thumb=True so repeat taps short-circuit at enqueue
                # (PDFs only; the ebook fuid must never be flagged).
                _publish_thumb_ready(
                    file_unique_id,
                    filename.lower().endswith(".pdf"),
                    thumb_path=_thumb_path,
                )
            try:
                _set_io_keys(
                    _rq_job_id,
                    output_meta={
                        "status": "already_ocr_delivered",
                        "filename": filename,
                        "timestamps": {"finished": int(time.time())},
                    },
                )
            except Exception:  # nosec B110
                pass
            _delete_queued_messages(_rq_job_id)
            return {"status": "already_ocr_delivered", "delivered": True}

        if _is_pdf_out and _thumb_path is None:
            # Direct PDF or image source: build a first-page preview for the
            # OCR'd PDF (best-effort — a missing preview never fails the job).
            _thumb_path = _build_pdf_output_preview(
                _src, filename, file_unique_id, tmpdir
            )
        _progress_msg_id = _tg_send_progress(
            chat_id, filename, "ocr",
            detail=(
                "\U0001f4c4 Building searchable PDF (text layer)..."
                if _is_pdf_out
                else (
                    "\U0001f50e Pulling the existing text layer..."
                    if _already_ocr
                    else "\U0001f50e Extracting text with OCR..."
                )
            ),
            message_id=_progress_msg_id,
        )
        _lang = getattr(config, "OCR_LANG", "eng")
        _dpi = int(getattr(config, "OCR_DPI", 200) or 200)
        _ocr_to = int(getattr(config, "OCR_TIMEOUT_SECONDS", 600) or 0)
        try:
            if _is_pdf_out:
                # Searchable PDF: invisible text layer over the ORIGINAL pages.
                _out_name = safe_target_name(filename, "pdf")
                _out = os.path.join(tmpdir, _out_name)
                # Never overwrite the downloaded input when it is already a
                # .pdf with the same basename (ocrmypdf needs distinct paths).
                if os.path.abspath(_out) == os.path.abspath(_src):
                    _base = os.path.splitext(_out_name)[0]
                    _out_name = f"{_base}_ocr.pdf"
                    _out = os.path.join(tmpdir, _out_name)
                _pdf_text = run_ocr_pdf(
                    _src,
                    filename,
                    _out,
                    lang=_lang,
                    dpi=_dpi,
                    timeout=_ocr_to,
                    cancel_check=lambda: _job_cancelled(_cancel_check_id),
                )
                if _pdf_text is None:
                    _tg_send_progress(
                        chat_id, filename, "failed",
                        detail=(
                            "\u274c OCR failed to build the searchable PDF. "
                            "The file may be corrupt/DRM-protected, or the OCR "
                            "engine timed out (see server logs)."
                        ),
                        message_id=_progress_msg_id,
                    )
                    _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
                    return {"error": "ocr_failed"}
                text = _pdf_text or ""
            elif _already_ocr:
                # Already has a text layer: extract it directly — cheaper and
                # more accurate than re-OCRing rendered pages.
                _out_name = safe_target_name(filename, "txt")
                _out = os.path.join(tmpdir, _out_name)
                text = _extract_pdf_text(_src)
                if not (text or "").strip():
                    # Edge: validator said text but extraction came back empty;
                    # fall through to the real OCR engine.
                    text = run_ocr(
                        _src,
                        filename,
                        lang=_lang,
                        dpi=_dpi,
                        timeout=_ocr_to,
                        cancel_check=lambda: _job_cancelled(_cancel_check_id),
                    )
            else:
                _out_name = safe_target_name(filename, "txt")
                _out = os.path.join(tmpdir, _out_name)
                text = run_ocr(
                    _src,
                    filename,
                    lang=_lang,
                    dpi=_dpi,
                    timeout=_ocr_to,
                    cancel_check=lambda: _job_cancelled(_cancel_check_id),
                )
        except OCRCancelledError:
            logger.info("ocr_job: cancelled mid-OCR for %s", filename)
            _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
            return {"status": "cancelled"}

        if _is_pdf_out:
            # The searchable PDF EXISTS and is delivered regardless of whether
            # any text was recognized (a blank scan still yields the user's
            # file, just without a text layer).
            if text.strip():
                _caption = (
                    "\U0001f50e Here is your searchable PDF — it looks exactly "
                    "like the original, but the text is now selectable and "
                    "searchable."
                )
            else:
                _caption = (
                    "\U0001f50e Here is your PDF — no text was recognized, "
                    "so it was returned unchanged (blank pages / unreadable "
                    "scan)."
                )
        else:
            if not text or not text.strip():
                _tg_send_progress(
                    chat_id, filename, "failed",
                    detail=(
                        "\u274c No text was found. The file may contain no "
                        "text (e.g. a blank page or a DRM-protected PDF), or "
                        "the OCR engine timed out / is misconfigured (see "
                        "server logs)."
                    ),
                    message_id=_progress_msg_id,
                )
                # Keep the failed progress message visible — it's the only
                # feedback the user gets (same as convert_book_job).
                _delete_queued_messages(_rq_job_id)
                return {"error": "no_text_found"}
            with open(_out, "w", encoding="utf-8") as _fh:
                _fh.write(text)
            _caption = "\U0001f50e Here is the extracted text."
        _ocr_res = _deliver_converted_file(
            chat_id, _out, _out_name, _thumb_path, _caption, user_id,
            progress_msg_id=_progress_msg_id,
        )
        if _ocr_res and not _converted_book:
            # Only direct-PDF/image sources: a converted book's fuid is not a
            # PDF (the has_thumb gate targets PDFs), so the ebook must never
            # be flagged even though its OCR output carried a preview.
            _publish_thumb_ready(
                file_unique_id,
                filename.lower().endswith(".pdf"),
                thumb_path=_thumb_path,
            )
        if _ocr_res and _ocr_res.get("ok"):
            try:
                _doc = (_ocr_res.get("result") or {}).get("document") or {}
                upsert_processed_record(
                    _content_hash,
                    "ocr",
                    "done",
                    filename=_orig_filename,
                    file_size=file_size,
                    file_id=_doc.get("file_id"),
                    thumb_file_id=(_doc.get("thumbnail") or {}).get(
                        "file_id"
                    ),
                    user_id=user_id,
                    chat_id=chat_id,
                    target=target,
                )
            except Exception:  # nosec B110 - cache is best-effort
                pass
        elif _ocr_res and _ocr_res.get("delivery") == "userbot":
            _cache_userbot_delivered_copy(
                _content_hash,
                _orig_filename,
                file_size,
                "ocr",
                _ocr_res.get("src_chat_id"),
                _ocr_res.get("src_message_id"),
                user_id=user_id,
                chat_id=chat_id,
                target=target,
            )
        out_meta = {
            "status": "done",
            "filename": _out_name,
            "chars": len(text),
            "timestamps": {"finished": int(time.time())},
        }
        try:
            _set_io_keys(_rq_job_id, output_meta=out_meta)
        except Exception:  # nosec B110
            pass
        _delete_queued_messages(_rq_job_id)
        return {"status": "done"}
    except Exception as exc:
        logger.exception("ocr_job: error for %s", filename)
        _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error running OCR: "
                f"{_short_error(exc)}\nCheck server logs for details.",
            )
        except Exception:  # nosec B110
            pass
        try:
            _set_io_keys(
                _rq_job_id,
                output_meta={"status": "error", "error": str(exc)},
            )
        except Exception:  # nosec B110
            pass
        return {"error": str(exc)}
    finally:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:  # nosec B110
            pass


def process_url_job(
    chat_id: int, url: str, filename: str, user_id: int | None = None
) -> None:
    """RQ job: download a PDF from URL, create thumbnail, and send back.

    Validates the URL to prevent SSRF attacks before downloading.
    """
    _rq_job_id = _attach_job_user_meta(user_id)
    logger.info(
        "process_url_job: start chat_id=%s user_id=%s url=%s filename=%s",
        chat_id,
        user_id,
        url[:100],
        filename,
    )
    if _rq_job_id:
        try:
            _set_io_keys(
                _rq_job_id,
                input_meta={
                    "job_id": _rq_job_id,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "url": url[:200],
                    "filename": filename,
                    "enqueued_at": int(time.time()),
                },
            )
        except Exception:
            logger.exception("Failed to write io:in for URL job %s", _rq_job_id)

    def _write_out(status: str, **extra) -> None:
        """Persist io:out for this URL job (best-effort)."""
        if not _rq_job_id:
            return
        try:
            _set_io_keys(
                _rq_job_id,
                output_meta={
                    "status": status,
                    "filename": filename,
                    "user_id": user_id,
                    "timestamps": {"finished": int(time.time())},
                    **extra,
                },
            )
        except Exception:  # nosec B110
            pass

    # SSRF prevention: validate the URL before making any requests
    if not _validate_url_safe(url):
        logger.warning(
            "SSRF prevention: blocked invalid/dangerous URL in process_url_job: %s (user_id=%s)",
            url[:100],
            user_id,
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Invalid or blocked URL. Only http/https URLs to public servers are allowed.",
            )
        except Exception:  # nosec B110
            pass
        _write_out("blocked", reason="invalid_url")
        return

    tmpdir = None
    _progress_msg_id = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None) or None)
        file_path = os.path.join(tmpdir, _safe_local_filename(filename))

        # Live download progress (throttled edits on the same message).
        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "downloading",
            detail="\U0001f4e5 Downloading from URL...",
        )
        _dl_state: dict[str, float] = {
            "msg_id": float(_progress_msg_id or 0),
            "last_pct": -1.0,
            "last_t": 0.0,
        }
        try:
            with requests.head(
                url, allow_redirects=False, timeout=30
            ) as _hr:
                _total = int(_hr.headers.get("Content-Length") or 0)
        except Exception:  # nosec B110 - unknown size still shows bytes
            _total = 0
        _seen = 0
        # Disable redirects to prevent SSRF bypass via redirect chains
        with requests.get(
            url, stream=True, allow_redirects=False, timeout=120
        ) as r:
            r.raise_for_status()
            with open(file_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        fh.write(chunk)
                        _seen += len(chunk)
                        _download_progress_cb(
                            chat_id, filename, _seen, _total, _dl_state
                        )
        _progress_msg_id = int(_dl_state["msg_id"]) or _progress_msg_id

        # ── E-books via URL: the conversion-only interface ──
        if _is_ebook(filename):
            if not _book_conversion_enabled():
                try:
                    _tg_send_message(
                        None,
                        chat_id,
                        "\U0001f4d5 E-book conversion is currently disabled "
                        "on this instance.",
                    )
                except Exception:  # nosec B110
                    pass
                _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
                _write_out("skipped", reason="book conversion disabled")
                return
            _ebook_ok = _deliver_book_echo(
                chat_id, file_path, filename, user_id, _progress_msg_id
            )
            if _ebook_ok:
                _write_out("done")
            else:
                _write_out("error", error="book echo failed")
            if _ebook_ok:
                _cleanup_after_success(
                    chat_id, _rq_job_id, _progress_msg_id
                )
            else:
                _cleanup_after_failure(
                    chat_id, _rq_job_id, _progress_msg_id
                )
                try:
                    _tg_send_message(
                        None,
                        chat_id,
                        "\u274c Failed to deliver the e-book from the URL.",
                    )
                except Exception:  # nosec B110
                    pass
            logger.info(
                "process_url_job: e-book complete chat_id=%s user_id=%s url=%s",
                chat_id,
                user_id,
                url[:100],
            )
            return

        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "thumbnailing",
            detail="\U0001f5bc\ufe0f Creating cover preview...",
            message_id=_progress_msg_id,
        )
        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        if filename.lower().endswith(".pdf"):
            create_thumbnail_from_pdf(file_path, thumb_path)
        else:
            create_thumbnail_from_image(file_path, thumb_path)

        _progress_msg_id = _tg_send_progress(
            chat_id,
            filename,
            "sending",
            detail="\U0001f4e4 Sending result to Telegram...",
            message_id=_progress_msg_id,
        )

        # Oversized result (> Bot API upload cap): deliver via userbot into
        # the user's DM with the bot instead of failing the Bot API send.
        try:
            _dl_size = os.path.getsize(file_path)
        except Exception:
            _dl_size = 0
        if (
            _dl_size
            and config.BOT_API_UPLOAD_LIMIT_BYTES
            and _dl_size > config.BOT_API_UPLOAD_LIMIT_BYTES
        ):
            _ub_res = _deliver_result_via_userbot(
                chat_id,
                file_path,
                filename,
                "Here is your file with an auto-generated cover preview.",
                thumb_path,
                user_id,
            )
            if _ub_res:
                _sent, _src_chat = _ub_res
                # Oversized PDFs delivered via the userbot get a one-tap
                # 🗜 Compress prompt pointing at the delivered copy (mirrors
                # the book/convert fallback).
                _maybe_attach_result_prompt(
                    chat_id,
                    file_path,
                    filename,
                    user_id,
                    _src_chat,
                    getattr(_sent, "id", None),
                )
                _write_out("done")
                _cleanup_after_success(chat_id, _rq_job_id, _progress_msg_id)
                logger.info(
                    "process_url_job: complete chat_id=%s user_id=%s url=%s",
                    chat_id,
                    user_id,
                    url[:100],
                )
                return

        _thumb_fh = None
        try:
            if thumbnail_is_usable(thumb_path):
                _thumb_fh = open(thumb_path, "rb")
            with open(file_path, "rb") as f_doc:
                _tg_send_document(
                    None,
                    chat_id,
                    f_doc,
                    filename,
                    thumb_fileobj=_thumb_fh,
                    caption="Here is your file with an auto-generated cover preview.",
                    compress_user_id=user_id,
                    ocr_user_id=user_id,
                )
        finally:
            if _thumb_fh is not None:
                _thumb_fh.close()
        _write_out("done")
        _cleanup_after_success(chat_id, _rq_job_id, _progress_msg_id)
        logger.info(
            "process_url_job: complete chat_id=%s user_id=%s url=%s",
            chat_id,
            user_id,
            url[:100],
        )
    except Exception as exc:
        logger.exception(
            "Failed processing URL job: %s (user_id=%s)", url, user_id
        )
        _write_out("error", error=str(exc))
        _cleanup_after_failure(chat_id, _rq_job_id, _progress_msg_id)
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Error processing URL. Check server logs for details.",
            )
        except Exception:  # nosec B110
            pass
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
