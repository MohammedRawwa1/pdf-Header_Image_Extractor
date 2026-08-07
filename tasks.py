import io
import json
import logging
import os
import shutil
import tempfile
import time
import uuid

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
    extract_pdf_metadata,
    is_supported_format,
)
from utils.progress_tracker import _format_size  # noqa: E402
from utils.storage import _TransferProgress  # noqa: E402,F401
from utils.tg_http import (  # noqa: E402
    _tg_delete_message,
    _tg_download_to_bytes,
    _tg_download_to_file,
    _tg_forward_message,
    _tg_get_file_path,
    _tg_send_document,
    _tg_send_message,
    _tg_send_progress,
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
    key: str, dest_path: str, progress_callback=None
) -> bool:
    """Download an S3 object (by key) to local `dest_path` using boto3.

    ``progress_callback(current_bytes, total_bytes)`` (optional) receives LIVE
    transfer progress.  Returns True on success, False on failure.
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
        s3.download_file(bucket, key, dest_path, Callback=_cb)
        return True
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
                            fh.write(chunk)
                            _cb(len(chunk))
            return True
        except Exception:
            logger.exception(
                "Presigned GET fallback failed for S3 key %s", key
            )
            return False


def process_input_key_job(job: dict) -> dict:
    """Process a job dict produced by telethon_ingest._upload_and_enqueue.

    Expected keys: 'job_id', 'input_key' (S3 key), 'original_filename', 'size', 'chat_id', 'message_id', 'cleanup_input'
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
    cleanup_input = job.get("cleanup_input", True)

    unique_key = job_id

    # Honour /canceljob: abort before downloading when the flag is set.
    # (/canceljob's "\u2705 Cancelled..." reply is the user-facing
    # confirmation, so no lingering extra message is posted here.)
    if _job_cancelled(job_id) or _pipeline_cancel_flag(job_id):
        return {"status": "cancelled"}

    # write input metadata for observability
    try:
        input_meta = {
            "job_id": job_id,
            "input_key": input_key,
            "filename": filename,
            "size": job.get("size") or job.get("file_size"),
            "chat_id": chat_id,
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
    }
    try:
        _set_io_keys(unique_key, output_meta=out_meta)
    except Exception:  # nosec B110
        pass

    tmpdir = None
    _progress_msg_id = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None))
        dest_path = os.path.join(tmpdir, filename)

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
        if input_key:
            ok = _download_s3_key_to_file(
                input_key, dest_path, progress_callback=_live_download_cb
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
            return {"error": "s3_download_failed"}
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

        # If still too large, try S3 fallback (should rarely be needed since input was uploaded already)
        if (
            upload_path == dest_path
            and orig_size
            and upload_limit
            and orig_size > upload_limit
        ):
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
        with (
            open(upload_path, "rb") as f_doc,
            open(thumb_path, "rb") as f_thumb,
        ):
            res = _tg_send_document(
                None,
                chat_id,
                f_doc,
                filename,
                thumb_fileobj=f_thumb,
                caption="Here is your file with an auto-generated cover preview.",
                progress_callback=_live_send_cb,
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
            "process_document_job: rejected unsupported format: filename=%s mime=%s chat_id=%s",
            filename,
            mime,
            chat_id,
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Unsupported file format.\n\n"
                "This bot only processes **PDF documents** and **images** (JPEG, PNG, WEBP, GIF).\n"
                "Video files (MKV, AVI, MP4, MOV, etc.) and other formats are not supported.",
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
                            _tg_send_progress(
                                chat_id,
                                filename,
                                "done",
                                detail="\u2705 Large file queued via S3 pipeline. You'll receive the result when ready.",
                                file_size=file_size or 0,
                                message_id=_progress_msg_id,
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
                        "All download methods failed for file_id=%s chat=%s msg=%s. Errors: %s",
                        file_id,
                        chat_id,
                        message_id,
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
            file_path = os.path.join(tmpdir, filename)

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
                create_thumbnail_from_pdf(file_path, thumb_path)
            else:
                create_thumbnail_from_image(file_path, thumb_path)

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

            # if still too large, try S3 fallback
            if (
                upload_path == file_path
                and orig_size
                and upload_limit
                and orig_size > upload_limit
            ):
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
            with (
                open(upload_path, "rb") as f_doc,
                open(thumb_path, "rb") as f_thumb,
            ):
                res = _tg_send_document(
                    None,
                    chat_id,
                    f_doc,
                    filename,
                    thumb_fileobj=f_thumb,
                    caption="Here is your file with an auto-generated cover preview.",
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

            if (
                filename.lower().endswith(".pdf")
                or "pdf" in (mime or "").lower()
            ):
                thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)
            else:
                thumb_bytes = create_thumbnail_from_image_bytes(file_bytes)

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

                    # if still too big, try S3
                    if len(file_bytes) > upload_limit:
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
            thumb_buf = io.BytesIO(thumb_bytes)
            doc_buf.seek(0)
            thumb_buf.seek(0)
            res = _tg_send_document(
                None,
                chat_id,
                doc_buf,
                filename,
                thumb_fileobj=thumb_buf,
                caption="Here is your file with an auto-generated cover preview.",
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
    results = []
    for item in items:
        file_id = item.get("file_id")
        filename = item.get("filename", "unknown")
        mime = item.get("mime", "")
        if not file_id:
            logger.warning("Skipping batch item with no file_id: %s", item)
            continue
        # ── Early format validation: skip unsupported items in batch ──
        if not is_supported_format(filename, mime):
            logger.info(
                "process_document_batch_job: skipping unsupported format: filename=%s mime=%s",
                filename,
                mime,
            )
            results.append({"skipped": "unsupported format", "filename": filename, "mime": mime})
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
            logger.exception("Failed processing batch item %s", filename)
            results.append({"error": f"failed: {filename}"})
    # All items delivered — delete the batch's "Queued batch..." confirmation.
    try:
        _batch_job = get_current_job()
        _delete_queued_messages(
            getattr(_batch_job, "id", None)
        )
    except Exception:  # nosec B110
        pass
    _tg_send_message(
        None,
        chat_id,
        f"Batch processing complete: {len(results)} items processed.",
    )
    return results


def process_url_job(chat_id: int, url: str, filename: str) -> None:
    """RQ job: download a PDF from URL, create thumbnail, and send back.

    Validates the URL to prevent SSRF attacks before downloading.
    """
    # SSRF prevention: validate the URL before making any requests
    if not _validate_url_safe(url):
        logger.warning(
            "SSRF prevention: blocked invalid/dangerous URL in process_url_job: %s",
            url[:100],
        )
        try:
            _tg_send_message(
                None,
                chat_id,
                "\u274c Invalid or blocked URL. Only http/https URLs to public servers are allowed.",
            )
        except Exception:  # nosec B110
            pass
        return

    tmpdir = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, "TMP_DIR", None) or None)
        file_path = os.path.join(tmpdir, filename)

        # download
        # Disable redirects to prevent SSRF bypass via redirect chains
        with requests.get(
            url, stream=True, allow_redirects=False, timeout=120
        ) as r:
            r.raise_for_status()
            with open(file_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        fh.write(chunk)

        thumb_path = os.path.join(tmpdir, "thumb.jpg")
        if filename.lower().endswith(".pdf"):
            create_thumbnail_from_pdf(file_path, thumb_path)
        else:
            create_thumbnail_from_image(file_path, thumb_path)

        with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
            _tg_send_document(
                None,
                chat_id,
                f_doc,
                filename,
                thumb_fileobj=f_thumb,
                caption="Here is your file with an auto-generated cover preview.",
            )
    except Exception:
        logger.exception("Failed processing URL job: %s", url)
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
