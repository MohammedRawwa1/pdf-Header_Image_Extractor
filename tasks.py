import os
import tempfile
import shutil
from typing import Optional
import io
import requests
import time
import logging
import json
import uuid

logger = logging.getLogger(__name__)

# Use direct Telegram Bot HTTP API calls in background workers (synchronous)


def _tg_get_file_path(bot_token: str | None, file_id: str) -> str:
    # Use provided bot_token or fall back to configured token
    if not bot_token:
        try:
            import config as _config
            bot_token = _config.BOT_TOKEN
        except Exception:
            bot_token = None
    logger = logging.getLogger(__name__)
    url = f"https://api.telegram.org/bot{bot_token}/getFile"
    # Try a couple of times for transient issues (e.g., 5xx or rate limits)
    for attempt in range(3):
        try:
            r = requests.get(url, params={"file_id": file_id}, timeout=30)
        except Exception as e:
            logger.exception("Network error fetching getFile for %s (attempt %s)", file_id, attempt + 1)
            if attempt < 2:
                time.sleep(1 + attempt)
                continue
            raise

        if r.status_code != 200:
            # Try to extract Telegram error description for more context
            try:
                body = r.json()
                desc = body.get("description") or body
            except Exception:
                desc = r.text
            msg = f"Telegram getFile failed: status={r.status_code} desc={desc}"
            logger.error(msg)
            # record diagnostic info in Redis io:out key for this file_id
            try:
                _set_io_keys(file_id, output_meta={"status": "getfile_failed", "http_status": r.status_code, "desc": str(desc), "timestamp": int(time.time())})
            except Exception:
                pass
            # For server errors or rate limits, retry a couple times
            if r.status_code >= 500 or r.status_code == 429:
                if attempt < 2:
                    time.sleep(1 + attempt)
                    continue
            # Raise an HTTPError with details so callers can include it in their handling
            raise requests.HTTPError(msg)

        try:
            data = r.json()
            return data["result"]["file_path"]
        except Exception as e:
            logger.exception("Failed parsing getFile JSON for %s", file_id)
            # On 400 errors like 'file is too big' record diagnostic info in io:out key
            try:
                unique_key = file_id
                _set_io_keys(unique_key, output_meta={"status": "getfile_failed", "error": str(e), "http_status": r.status_code, "desc": r.text, "timestamp": int(time.time())})
            except Exception:
                pass
            raise


def _tg_download_to_bytes(bot_token: str | None, tg_file_path: str) -> bytes:
    if not bot_token:
        try:
            import config as _config
            bot_token = _config.BOT_TOKEN
        except Exception:
            bot_token = None
    url = f"https://api.telegram.org/file/bot{bot_token}/{tg_file_path}"
    last_exc = None
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                if r.status_code >= 500 or r.status_code == 429:
                    # Server error or rate limit — retry with backoff
                    last_exc = requests.HTTPError(
                        f"Telegram download failed: status={r.status_code}"
                    )
                    logger.warning(
                        "_tg_download_to_bytes: HTTP %s on attempt %d for %s",
                        r.status_code, attempt + 1, tg_file_path,
                    )
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    raise last_exc
                r.raise_for_status()
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        buf.write(chunk)
                return buf.getvalue()
        except (requests.ConnectionError, requests.Timeout, requests.ChunkedEncodingError) as e:
            # Transient network errors — retry with exponential backoff
            last_exc = e
            logger.warning(
                "_tg_download_to_bytes: transient error %s on attempt %d for %s",
                type(e).__name__, attempt + 1, tg_file_path,
            )
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
        except requests.HTTPError:
            # Non-retryable HTTP errors (e.g., 400, 404) — raise immediately
            raise
    raise last_exc or RuntimeError(f"Failed to download {tg_file_path} after 3 attempts")


def _tg_send_document(bot_token: str | None, chat_id: int, doc_fileobj, filename: str, thumb_fileobj=None, caption: str | None = None):
    if not bot_token:
        try:
            import config as _config
            bot_token = _config.BOT_TOKEN
        except Exception:
            bot_token = None
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    files = {"document": (filename, doc_fileobj)}
    if thumb_fileobj is not None:
        files["thumb"] = ("thumb.jpg", thumb_fileobj, "image/jpeg")
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
    r = requests.post(url, data=data, files=files, timeout=120)
    r.raise_for_status()
    return r.json()


def _tg_send_message(bot_token: str | None, chat_id: int, text: str):
    if not bot_token:
        try:
            import config as _config
            bot_token = _config.BOT_TOKEN
        except Exception:
            bot_token = None
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": str(chat_id), "text": text}
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

from tools import (
    create_thumbnail_from_pdf,
    create_thumbnail_from_image,
    create_thumbnail_from_pdf_bytes,
    create_thumbnail_from_image_bytes,
    compress_pdf,
)
import config
try:
    from rq import get_current_job
except Exception:
    get_current_job = None

try:
    from storage import upload_file_and_get_presigned_url
except Exception:
    upload_file_and_get_presigned_url = None


def _download_s3_key_to_file(key: str, dest_path: str) -> bool:
    """Download an S3 object (by key) to local `dest_path` using boto3.

    Returns True on success, False on failure.
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        logger.exception("boto3 not available for downloading S3 key %s", key)
        return False

    bucket = getattr(config, 'S3_BUCKET', None)
    if not bucket:
        logger.error("S3 bucket not configured; cannot download key %s", key)
        return False

    client_kwargs = {}
    if getattr(config, 'S3_REGION', None):
        client_kwargs['region_name'] = config.S3_REGION
    if getattr(config, 'S3_ENDPOINT', None):
        client_kwargs['endpoint_url'] = config.S3_ENDPOINT
    if getattr(config, 'AWS_ACCESS_KEY_ID', None) or getattr(config, 'AWS_SECRET_ACCESS_KEY', None):
        client_kwargs['aws_access_key_id'] = config.AWS_ACCESS_KEY_ID or None
        client_kwargs['aws_secret_access_key'] = config.AWS_SECRET_ACCESS_KEY or None

    try:
        sig = getattr(config, 'S3_SIGNATURE_VERSION', 's3v4')
        boto_cfg = BotoConfig(signature_version=sig)
        s3 = boto3.client('s3', config=boto_cfg, **client_kwargs)
    except Exception:
        logger.exception("Failed to create S3 client for download of %s", key)
        return False

    try:
        # ensure parent dir exists
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        s3.download_file(bucket, key, dest_path)
        return True
    except Exception:
        logger.exception("Failed to download S3 key %s to %s", key, dest_path)
        # fallback: try to generate a presigned URL and download via requests
        try:
            url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=int(getattr(config, 'S3_PRESIGNED_EXPIRY', 3600)))
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(dest_path, 'wb') as fh:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            fh.write(chunk)
            return True
        except Exception:
            logger.exception("Presigned GET fallback failed for S3 key %s", key)
            return False


def process_input_key_job(job: dict) -> dict:
    """Process a job dict produced by telethon_ingest._upload_and_enqueue.

    Expected keys: 'job_id', 'input_key' (S3 key), 'original_filename', 'size', 'chat_id', 'message_id', 'cleanup_input'
    This will download the object to a temp dir and run the disk-mode flow (thumbnail, compress, send).
    Returns the Telegram send response or an error dict.
    """
    job_id = job.get('job_id') or uuid.uuid4().hex
    input_key = job.get('input_key')
    filename = job.get('original_filename') or os.path.basename(input_key or '') or f"{job_id}.bin"
    chat_id = job.get('chat_id')
    cleanup_input = job.get('cleanup_input', True)

    unique_key = job_id

    # write input metadata for observability
    try:
        input_meta = {
            'job_id': job_id,
            'input_key': input_key,
            'filename': filename,
            'size': job.get('size') or job.get('file_size'),
            'chat_id': chat_id,
            'enqueued_at': int(time.time()),
        }
        _set_io_keys(unique_key, input_meta=input_meta)
    except Exception:
        logger.exception("Failed to write io:in for job %s", unique_key)

    out_meta = {'status': 'processing', 'timestamps': {'start': int(time.time())}, 'durations': {}, 'sizes': {}}
    try:
        _set_io_keys(unique_key, output_meta=out_meta)
    except Exception:
        pass

    tmpdir = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, 'TMP_DIR', None))
        dest_path = os.path.join(tmpdir, filename)

        dl_start = time.time()
        ok = False
        if input_key:
            ok = _download_s3_key_to_file(input_key, dest_path)
        if not ok:
            # nothing to do
            out_meta.setdefault('status', 'download_failed')
            out_meta.setdefault('error', 's3_download_failed')
            out_meta.setdefault('timestamps', {})['finished'] = int(time.time())
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass
            return {'error': 's3_download_failed'}
        dl_elapsed = time.time() - dl_start
        out_meta.setdefault('durations', {})['download_ms'] = int(dl_elapsed * 1000)
        out_meta.setdefault('timestamps', {})['download_end'] = int(time.time())
        try:
            out_meta.setdefault('sizes', {})['orig_bytes'] = os.path.getsize(dest_path)
        except Exception:
            pass
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
            pass

        # Now reuse disk-mode flow: thumbnail, compress, s3-fallback if needed, send
        thumb_path = os.path.join(tmpdir, 'thumb.jpg')
        if filename.lower().endswith('.pdf'):
            create_thumbnail_from_pdf(dest_path, thumb_path)
        else:
            create_thumbnail_from_image(dest_path, thumb_path)

        upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
        upload_path = dest_path
        try:
            orig_size = os.path.getsize(dest_path)
        except Exception:
            orig_size = None

        compress_total = 0.0
        if orig_size and upload_limit and orig_size > upload_limit:
            # first attempt
            try:
                a_start = time.time()
                c1 = dest_path + '.compressed.pdf'
                ok1 = compress_pdf(dest_path, c1, gs_quality='/ebook')
                a_elapsed = time.time() - a_start
                compress_total += a_elapsed
                out_meta.setdefault('durations', {})['compress_ms'] = int(compress_total * 1000)
                out_meta.setdefault('timestamps', {})['compress_attempt_1_end'] = int(time.time())
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:
                    pass
                if ok1:
                    try:
                        csize = os.path.getsize(c1)
                    except Exception:
                        csize = None
                    if csize and csize <= upload_limit:
                        upload_path = c1
                        out_meta.setdefault('sizes', {})['compressed_bytes'] = csize
            except Exception:
                pass

            if upload_path == dest_path:
                try:
                    b_start = time.time()
                    c2 = dest_path + '.compressed.screen.pdf'
                    ok2 = compress_pdf(dest_path, c2, gs_quality='/screen')
                    b_elapsed = time.time() - b_start
                    compress_total += b_elapsed
                    out_meta.setdefault('durations', {})['compress_ms'] = int(compress_total * 1000)
                    out_meta.setdefault('timestamps', {})['compress_attempt_2_end'] = int(time.time())
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:
                        pass
                    if ok2:
                        try:
                            c2size = os.path.getsize(c2)
                        except Exception:
                            c2size = None
                        if c2size and c2size <= upload_limit:
                            upload_path = c2
                            out_meta.setdefault('sizes', {})['compressed_bytes'] = c2size
                except Exception:
                    pass

        # If still too large, try S3 fallback (should rarely be needed since input was uploaded already)
        if upload_path == dest_path and orig_size and upload_limit and orig_size > upload_limit:
            if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None) and upload_file_and_get_presigned_url:
                try:
                    up_start = time.time()
                    url = upload_file_and_get_presigned_url(dest_path, filename)
                    up_elapsed = time.time() - up_start
                    if url:
                        try:
                            _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                        except Exception:
                            pass
                        out_meta.setdefault('durations', {})['s3_upload_ms'] = int(up_elapsed * 1000)
                        out_meta.setdefault('timestamps', {})['s3_upload_end'] = int(time.time())
                        out_meta.setdefault('status', 's3_fallback')
                        out_meta.setdefault('s3', {})['url'] = url
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:
                            pass
                        return {'s3_url': url}
                except Exception:
                    logger.exception("S3 fallback failed for job %s", job_id)

            # fallback notify and persist
            try:
                _tg_send_message(None, chat_id, f"File too large to upload via bot ({orig_size} bytes); compression didn't reduce it below {upload_limit} bytes.")
            except Exception:
                pass
            out_meta.setdefault('status', 'too_large_after_compress')
            out_meta.setdefault('sizes', {})['orig_bytes'] = orig_size
            out_meta.setdefault('timestamps', {})['finished'] = int(time.time())
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass
            return {'error': 'file too large after compression'}

        # send final document via Telegram
        send_start = time.time()
        with open(upload_path, 'rb') as f_doc, open(thumb_path, 'rb') as f_thumb:
            res = _tg_send_document(None, chat_id, f_doc, filename, thumb_fileobj=f_thumb, caption="Here is your file with an auto-generated cover preview.")
        send_elapsed = time.time() - send_start
        out_meta.setdefault('durations', {})['tg_send_ms'] = int(send_elapsed * 1000)
        out_meta.setdefault('timestamps', {})['finished'] = int(time.time())
        out_meta.setdefault('status', 'done')
        try:
            out_meta.setdefault('sizes', {})['out_bytes'] = os.path.getsize(upload_path)
        except Exception:
            pass
        try:
            out_meta['tg_response'] = res
        except Exception:
            pass
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
            pass

        try:
            if get_current_job is not None:
                job_obj = get_current_job()
                if job_obj is not None:
                    job_obj.meta['tg_response'] = res
                    job_obj.save_meta()
        except Exception:
            pass

        return res

    except Exception as e:
        logger.exception("Error processing input_key job %s", job_id)
        out_meta.setdefault('status', 'error')
        out_meta.setdefault('error', str(e))
        out_meta.setdefault('timestamps', {})['finished'] = int(time.time())
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
            pass
        try:
            _tg_send_message(None, chat_id, f"Error processing uploaded file: {e}")
        except Exception:
            pass
        return {'error': str(e)}
    finally:
        try:
            if tmpdir and os.path.exists(tmpdir):
                if cleanup_input:
                    shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


# IO mapping TTL (seconds) for input/output keys stored in Redis
IO_TTL = 7 * 24 * 3600


def _get_redis_client():
    if not getattr(config, 'REDIS_URL', None):
        return None
    try:
        import redis
        return redis.from_url(config.REDIS_URL)
    except Exception:
        return None


_pymongo_client = None
_pymongo_db = None


def _get_mongo_db():
    """Return a cached pymongo database (best-effort, for RQ worker sync path).

    Uses centralized URI and db_name from utils.db to stay consistent.
    """
    global _pymongo_client, _pymongo_db
    if _pymongo_db is not None:
        return _pymongo_db
    try:
        from utils.db import get_mongo_uri, get_db_name
        mongo_uri = get_mongo_uri()
        if not mongo_uri:
            return None
        import pymongo
        _pymongo_client = pymongo.MongoClient(
            mongo_uri, serverSelectionTimeoutMS=3000
        )
        _pymongo_db = _pymongo_client[get_db_name()]
        return _pymongo_db
    except Exception:
        _pymongo_client = None
        _pymongo_db = None
        return None


def _set_io_keys(unique_id: str, input_meta: dict | None = None, output_meta: dict | None = None, ttl: int | None = None) -> bool:
    """Set input and/or output JSON blobs in Redis under `io:in:{id}` and `io:out:{id}`.

    Also writes a best-effort backup to MongoDB (sync) so
    metadata survives Redis key expiry or restarts.
    """
    r = _get_redis_client()
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
            mongo_db = _get_mongo_db()
            if mongo_db is not None:
                mongo_db.job_metadata.update_one(
                    {"job_id": f"io:{unique_id}"},
                    {"$set": mongo_meta},
                    upsert=True,
                )
    except Exception:
        pass

    return redis_ok


def process_document_job(
    chat_id: int,
    file_id: str,
    filename: str,
    mime: Optional[str] = "",
    file_unique_id: Optional[str] = None,
    message_id: Optional[int] = None,
    forward_info: Optional[dict] = None,
    file_size: Optional[int] = None,
) -> Optional[dict]:
    """RQ job: download a Telegram file by file_id, create thumbnail, and send back the original with thumb.

    When the Bot API cannot handle a large file (>50MB), falls back through:
      1. File_id-based userbot download (fast, but may fail for modern file_id formats)
      2. Chat-based userbot download ``download_bytes_via_userbot(chat_id, message_id)``
      3. BigFilePipeline (S3 pipeline + separate worker) — only if S3 is configured

    NOTE: This function reads the bot token from `config.BOT_TOKEN` internally; do NOT pass the token as a job argument.
    """
    unique_key = file_unique_id or file_id

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
        logger.exception("Failed to write initial io input key for %s", unique_key)

    # init output meta / timings
    out_meta = {"status": "processing", "timestamps": {"start": int(time.time())}, "durations": {}, "sizes": {}}
    try:
        _set_io_keys(unique_key, output_meta=out_meta)
    except Exception:
        pass

    tmpdir = None
    # Flag for userbot fallback data (large files that Bot API can't handle)
    _userbot_dl_data = None
    # Calculate upload limit BEFORE getFile so the early size check can use it
    upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
    try:
        # 1) getFile (path) — with multi-level userbot fallback for files >50MB
        #
        # Fallback chain when Bot API cannot handle the file:
        #   a) File_id-based download  — fastest, but broken for v4+ file_ids
        #   b) Chat-based download     — works reliably with any file_id
        #   c) BigFilePipeline         — S3 pipeline + separate worker
        #
        gf_start = time.time()
        try:
            # If we already know the file exceeds Bot API limits, skip getFile entirely
            _skip_bot_api = file_size and upload_limit and file_size > upload_limit
            if _skip_bot_api:
                logger.info(
                    "file_size=%d > upload_limit=%d; skipping Bot API getFile, "
                    "proceeding directly to userbot download",
                    file_size, upload_limit,
                )
                raise requests.HTTPError("Bad Request: file is too big")
            tg_file_path = _tg_get_file_path(None, file_id)
        except requests.HTTPError as _gf_err:
            _gf_err_str = str(_gf_err)
            if "file is too big" in _gf_err_str.lower():
                logger.info("Bot API cannot handle large file; trying userbot fallback chain")
                import asyncio as _asyncio
                _ub_data = None
                _fallback_errors = []

                # ── Fallback (a): file_id-based download ──
                try:
                    from utils.userbot_downloader import download_bytes_by_file_id_via_userbot as _dl_file_id
                    _ub_data = _asyncio.run(_dl_file_id(file_id))
                    if _ub_data and len(_ub_data) > 0:
                        logger.info("Userbot file_id download succeeded: %d bytes", len(_ub_data))
                    else:
                        _ub_data = None
                        raise Exception("file_id download returned empty")
                except Exception as _fb_a:
                    _fallback_errors.append(f"file_id download: {_fb_a}")
                    logger.warning("Fallback (a) file_id download failed: %s", _fb_a)

                # ── Fallback (b): chat-based download (works with any file_id) ──
                if _ub_data is None and message_id:
                    try:
                        from utils.userbot_downloader import download_bytes_via_userbot as _dl_chat
                        logger.info(
                            "Trying fallback (b) chat-based download: chat=%s msg=%s",
                            chat_id, message_id,
                        )
                        _ub_data = _asyncio.run(_dl_chat(chat_id, message_id))
                        if _ub_data and len(_ub_data) > 0:
                            logger.info("Userbot chat-based download succeeded: %d bytes", len(_ub_data))
                        else:
                            _ub_data = None
                            raise Exception("chat download returned empty")
                    except Exception as _fb_b:
                        _fallback_errors.append(f"chat download: {_fb_b}")
                        logger.warning("Fallback (b) chat-based download failed: %s", _fb_b)

                # ── Fallback (c): BigFilePipeline (S3 pipeline) ──
                if _ub_data is None and message_id:
                    try:
                        from utils.bigfile_pipeline import BigFilePipeline as _BFP
                        logger.info(
                            "Trying fallback (c) BigFilePipeline: chat=%s msg=%s size=%s",
                            chat_id, message_id, file_size or "unknown",
                        )
                        _pipeline = _BFP()
                        _result = _asyncio.run(_pipeline.ingest_large_file(
                            chat_id=chat_id,
                            message_id=message_id,
                            file_size=file_size or 0,
                            file_unique_id=file_unique_id,
                            original_filename=filename,
                        ))
                        if _result and _result.ok:
                            logger.info(
                                "BigFilePipeline job enqueued: job_id=%s s3_key=%s",
                                _result.job_id, _result.s3_key,
                            )
                            try:
                                _size_display = file_size // (1024 * 1024) if file_size else "?"
                                _tg_send_message(None, chat_id,
                                    f"Large file ({_size_display} MB) queued via pipeline. "
                                    f"Job: {_result.job_id[:8] if _result.job_id else 'unknown'}... "
                                    f"You'll receive the result when ready."
                                )
                            except Exception:
                                pass
                            return {"pipeline": _result.job_id}
                        else:
                            raise Exception(f"BigFilePipeline failed: {_result.error if _result else 'unknown'}")
                    except Exception as _fb_c:
                        _fallback_errors.append(f"BigFilePipeline: {_fb_c}")
                        logger.warning("Fallback (c) BigFilePipeline failed: %s", _fb_c)

                # ── All fallbacks exhausted ──
                if _ub_data is not None:
                    _userbot_dl_data = _ub_data
                    tg_file_path = "__userbot_fallback__"
                else:
                    logger.error(
                        "All download methods failed for file_id=%s chat=%s msg=%s. Errors: %s",
                        file_id, chat_id, message_id,
                        "; ".join(_fallback_errors),
                    )
                    raise _gf_err from RuntimeError(
                        f"All {len(_fallback_errors)} fallbacks exhausted: "
                        + "; ".join(_fallback_errors)
                    )
            else:
                raise
        gf_elapsed = time.time() - gf_start
        out_meta.setdefault("durations", {})["getfile_ms"] = int(gf_elapsed * 1000)
        out_meta.setdefault("timestamps", {})["getfile_end"] = int(time.time())
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
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
                with open(file_path, 'wb') as fh:
                    fh.write(_userbot_dl_data)
                logger.info("Used userbot-fallback data for file_id=%s (%d bytes written)", file_id, len(_userbot_dl_data))
            else:
                try:
                    import config as _conf
                    bot_token = _conf.BOT_TOKEN
                except Exception:
                    bot_token = None

                with requests.get(f"https://api.telegram.org/file/bot{bot_token}/{tg_file_path}", stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(file_path, 'wb') as fh:
                        for chunk in r.iter_content(chunk_size=64 * 1024):
                            if chunk:
                                fh.write(chunk)
            dl_elapsed = time.time() - dl_start
            out_meta.setdefault("durations", {})["download_ms"] = int(dl_elapsed * 1000)
            out_meta.setdefault("timestamps", {})["download_end"] = int(time.time())
            try:
                out_meta.setdefault("sizes", {})["orig_bytes"] = os.path.getsize(file_path)
            except Exception:
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass

            # thumbnail
            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
                create_thumbnail_from_pdf(file_path, thumb_path)
            else:
                create_thumbnail_from_image(file_path, thumb_path)

            # compression flow
            upload_path = file_path
            try:
                orig_size = os.path.getsize(file_path)
            except Exception:
                orig_size = None

            compress_total = 0.0
            if orig_size and upload_limit and orig_size > upload_limit:
                # attempt first pass
                try:
                    a_start = time.time()
                    c1 = file_path + '.compressed.pdf'
                    ok1 = compress_pdf(file_path, c1, gs_quality='/ebook')
                    a_elapsed = time.time() - a_start
                    compress_total += a_elapsed
                    out_meta.setdefault("durations", {})["compress_ms"] = int(compress_total * 1000)
                    out_meta.setdefault("timestamps", {})["compress_attempt_1_end"] = int(time.time())
                    try:
                        _set_io_keys(unique_key, output_meta=out_meta)
                    except Exception:
                        pass
                    if ok1:
                        try:
                            csize = os.path.getsize(c1)
                        except Exception:
                            csize = None
                        if csize and csize <= upload_limit:
                            upload_path = c1
                            out_meta.setdefault("sizes", {})["compressed_bytes"] = csize
                except Exception:
                    pass

                if upload_path == file_path:
                    # try second, more aggressive pass
                    try:
                        b_start = time.time()
                        c2 = file_path + '.compressed.screen.pdf'
                        ok2 = compress_pdf(file_path, c2, gs_quality='/screen')
                        b_elapsed = time.time() - b_start
                        compress_total += b_elapsed
                        out_meta.setdefault("durations", {})["compress_ms"] = int(compress_total * 1000)
                        out_meta.setdefault("timestamps", {})["compress_attempt_2_end"] = int(time.time())
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:
                            pass
                        if ok2:
                            try:
                                c2size = os.path.getsize(c2)
                            except Exception:
                                c2size = None
                            if c2size and c2size <= upload_limit:
                                upload_path = c2
                                out_meta.setdefault("sizes", {})["compressed_bytes"] = c2size
                    except Exception:
                        pass

            # if still too large, try S3 fallback
            if upload_path == file_path and orig_size and upload_limit and orig_size > upload_limit:
                if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None) and upload_file_and_get_presigned_url:
                    try:
                        up_start = time.time()
                        url = upload_file_and_get_presigned_url(file_path, filename)
                        up_elapsed = time.time() - up_start
                        if url:
                            try:
                                _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                            except Exception:
                                pass
                            out_meta.setdefault("durations", {})["s3_upload_ms"] = int(up_elapsed * 1000)
                            out_meta.setdefault("timestamps", {})["s3_upload_end"] = int(time.time())
                            out_meta.setdefault("status", "s3_fallback")
                            out_meta.setdefault("s3", {})["url"] = url
                            try:
                                _set_io_keys(unique_key, output_meta=out_meta)
                            except Exception:
                                pass
                            return {"s3_url": url}
                    except Exception:
                        logger.exception("S3 fallback failed for file_id=%s", file_id)

                # otherwise notify user and persist io entry
                try:
                    _tg_send_message(None, chat_id, f"File too large to upload via bot ({orig_size} bytes); compression didn't reduce it below {upload_limit} bytes. Consider external storage or a smaller file.")
                except Exception:
                    pass
                out_meta.setdefault("status", "too_large_after_compress")
                out_meta.setdefault("sizes", {})["orig_bytes"] = orig_size
                out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
                try:
                    _set_io_keys(unique_key, output_meta=out_meta)
                except Exception:
                    pass
                return {"error": "file too large after compression"}

            # send final document via Telegram
            send_start = time.time()
            with open(upload_path, 'rb') as f_doc, open(thumb_path, 'rb') as f_thumb:
                res = _tg_send_document(None, chat_id, f_doc, filename, thumb_fileobj=f_thumb, caption="Here is your file with an auto-generated cover preview.")
            send_elapsed = time.time() - send_start
            out_meta.setdefault("durations", {})["tg_send_ms"] = int(send_elapsed * 1000)
            out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
            out_meta.setdefault("status", "done")
            try:
                out_meta.setdefault("sizes", {})["out_bytes"] = os.path.getsize(upload_path)
            except Exception:
                pass
            try:
                out_meta["tg_response"] = res
            except Exception:
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass

            try:
                if get_current_job is not None:
                    job = get_current_job()
                    if job is not None:
                        job.meta['tg_response'] = res
                        job.save_meta()
            except Exception:
                pass

            return res

        else:
            # in-memory pathway
            dl_start = time.time()
            if _userbot_dl_data is not None:
                file_bytes = _userbot_dl_data
                logger.info("Used userbot-fallback data for in-memory path (%d bytes)", len(file_bytes))
            else:
                file_bytes = _tg_download_to_bytes(None, tg_file_path)
            dl_elapsed = time.time() - dl_start
            out_meta.setdefault("durations", {})["download_ms"] = int(dl_elapsed * 1000)
            out_meta.setdefault("timestamps", {})["download_end"] = int(time.time())
            try:
                out_meta.setdefault("sizes", {})["orig_bytes"] = len(file_bytes)
            except Exception:
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass

            if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
                thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)
            else:
                thumb_bytes = create_thumbnail_from_image_bytes(file_bytes)

            # If large, attempt compression via temp file flow
            if upload_limit and len(file_bytes) > upload_limit:
                td = tempfile.mkdtemp()
                try:
                    tmp_in = os.path.join(td, filename)
                    with open(tmp_in, 'wb') as fh:
                        fh.write(file_bytes)

                    compress_total = 0.0
                    try:
                        a_start = time.time()
                        c1 = tmp_in + '.compressed.pdf'
                        ok1 = compress_pdf(tmp_in, c1, gs_quality='/ebook')
                        a_elapsed = time.time() - a_start
                        compress_total += a_elapsed
                        out_meta.setdefault("durations", {})["compress_ms"] = int(compress_total * 1000)
                        out_meta.setdefault("timestamps", {})["compress_attempt_1_end"] = int(time.time())
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:
                            pass
                        if ok1:
                            try:
                                csize = os.path.getsize(c1)
                            except Exception:
                                csize = None
                            if csize and csize <= upload_limit:
                                with open(c1, 'rb') as cf:
                                    file_bytes = cf.read()
                                out_meta.setdefault("sizes", {})["compressed_bytes"] = csize
                    except Exception:
                        pass

                    if len(file_bytes) > upload_limit:
                        try:
                            b_start = time.time()
                            c2 = tmp_in + '.compressed.screen.pdf'
                            ok2 = compress_pdf(tmp_in, c2, gs_quality='/screen')
                            b_elapsed = time.time() - b_start
                            compress_total += b_elapsed
                            out_meta.setdefault("durations", {})["compress_ms"] = int(compress_total * 1000)
                            out_meta.setdefault("timestamps", {})["compress_attempt_2_end"] = int(time.time())
                            try:
                                _set_io_keys(unique_key, output_meta=out_meta)
                            except Exception:
                                pass
                            if ok2:
                                try:
                                    c2size = os.path.getsize(c2)
                                except Exception:
                                    c2size = None
                                if c2size and c2size <= upload_limit:
                                    with open(c2, 'rb') as cf:
                                        file_bytes = cf.read()
                                    out_meta.setdefault("sizes", {})["compressed_bytes"] = c2size
                        except Exception:
                            pass

                    # if still too big, try S3
                    if len(file_bytes) > upload_limit:
                        if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None) and upload_file_and_get_presigned_url:
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
                                url = upload_file_and_get_presigned_url(candidate, filename)
                                up_elapsed = time.time() - up_start
                                if url:
                                    try:
                                        _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                                    except Exception:
                                        pass
                                    out_meta.setdefault("durations", {})["s3_upload_ms"] = int(up_elapsed * 1000)
                                    out_meta.setdefault("timestamps", {})["s3_upload_end"] = int(time.time())
                                    out_meta.setdefault("status", "s3_fallback")
                                    out_meta.setdefault("s3", {})["url"] = url
                                    try:
                                        _set_io_keys(unique_key, output_meta=out_meta)
                                    except Exception:
                                        pass
                                    return {"s3_url": url}
                            except Exception:
                                logger.exception("S3 fallback failed for in-memory file for chat_id=%s", chat_id)
                        try:
                            _tg_send_message(None, chat_id, f"File too large to upload via bot after compression; size={len(file_bytes)} bytes")
                        except Exception:
                            pass
                        out_meta.setdefault("status", "too_large_after_compress")
                        out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
                        try:
                            _set_io_keys(unique_key, output_meta=out_meta)
                        except Exception:
                            pass
                        return {"error": "file too large after compression"}
                finally:
                    shutil.rmtree(td, ignore_errors=True)

            # send via Telegram
            send_start = time.time()
            doc_buf = io.BytesIO(file_bytes)
            thumb_buf = io.BytesIO(thumb_bytes)
            doc_buf.seek(0)
            thumb_buf.seek(0)
            res = _tg_send_document(None, chat_id, doc_buf, filename, thumb_fileobj=thumb_buf, caption="Here is your file with an auto-generated cover preview.")
            send_elapsed = time.time() - send_start
            out_meta.setdefault("durations", {})["tg_send_ms"] = int(send_elapsed * 1000)
            out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
            out_meta.setdefault("status", "done")
            try:
                out_meta["tg_response"] = res
            except Exception:
                pass
            try:
                _set_io_keys(unique_key, output_meta=out_meta)
            except Exception:
                pass
            try:
                if get_current_job is not None:
                    job = get_current_job()
                    if job is not None:
                        job.meta['tg_response'] = res
                        job.save_meta()
            except Exception:
                pass
            return res

    except Exception as e:
        logger.exception("Error while processing document job %s", file_id)
        try:
            out_meta.setdefault("status", "error")
            out_meta.setdefault("error", str(e))
            out_meta.setdefault("timestamps", {})["finished"] = int(time.time())
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
            pass
        try:
            _tg_send_message(None, chat_id, f"Error processing file in background: {e}")
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        try:
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
    # In-memory processing
    try:
        if not filename:
            filename = os.path.basename(url.split('?', 1)[0]) or 'download.pdf'
        if not filename.lower().endswith('.pdf'):
            filename = filename + '.pdf'

        buf = io.BytesIO()
        with requests.get(url, stream=True, allow_redirects=True, timeout=60) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    buf.write(chunk)
        file_bytes = buf.getvalue()

        thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)

        doc_buf = io.BytesIO(file_bytes)
        thumb_buf = io.BytesIO(thumb_bytes)
        doc_buf.seek(0)
        thumb_buf.seek(0)

        res = _tg_send_document(None, chat_id, doc_buf, filename, thumb_fileobj=thumb_buf,
                                caption="Here is your file with an auto-generated cover preview.")
        return res
    except Exception as e:
        try:
            _tg_send_message(None, chat_id, f"Error processing URL in background: {e}")
        except Exception:
            pass
        return {"error": str(e)}




def process_document_batch_job(chat_id: int, items: list) -> None:
    """RQ job: process a batch of forwarded document items in order.

    Each item dict is expected to have: file_id, filename, mime.
    """
    results = []
    for item in items:
        file_id = item.get('file_id')
        filename = item.get('filename', 'unknown')
        mime = item.get('mime', '')
        if not file_id:
            logger.warning('Skipping batch item with no file_id: %s', item)
            continue
        try:
            res = process_document_job(chat_id, file_id, filename, mime)
            results.append(res)
        except Exception:
            logger.exception('Failed processing batch item %s', filename)
            results.append({'error': f'failed: {filename}'})
    _tg_send_message(None, chat_id, f'Batch processing complete: {len(results)} items processed.')
    return results


def process_url_job(chat_id: int, url: str, filename: str) -> None:
    """RQ job: download a PDF from URL, create thumbnail, and send back."""
    tmpdir = None
    try:
        tmpdir = tempfile.mkdtemp(dir=getattr(config, 'TMP_DIR', None) or None)
        file_path = os.path.join(tmpdir, filename)

        # download
        with requests.get(url, stream=True, allow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as fh:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        fh.write(chunk)

        thumb_path = os.path.join(tmpdir, 'thumb.jpg')
        if filename.lower().endswith('.pdf'):
            create_thumbnail_from_pdf(file_path, thumb_path)
        else:
            create_thumbnail_from_image(file_path, thumb_path)

        with open(file_path, 'rb') as f_doc, open(thumb_path, 'rb') as f_thumb:
            _tg_send_document(None, chat_id, f_doc, filename, thumb_fileobj=f_thumb,
                             caption='Here is your file with an auto-generated cover preview.')
    except Exception as e:
        logger.exception('Failed processing URL job: %s', url)
        try:
            _tg_send_message(None, chat_id, f'Error processing URL: {e}')
        except Exception:
            pass
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
