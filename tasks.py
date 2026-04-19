import os
import tempfile
import shutil
from typing import Optional
import io
import requests
import time
import logging
import json

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
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=64 * 1024):
            if chunk:
                buf.write(chunk)
        return buf.getvalue()


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


def _set_io_keys(unique_id: str, input_meta: dict | None = None, output_meta: dict | None = None, ttl: int | None = None) -> bool:
    """Set input and/or output JSON blobs in Redis under `io:in:{id}` and `io:out:{id}`."""
    r = _get_redis_client()
    if not r:
        return False
    try:
        if ttl is None:
            ttl = IO_TTL
        if input_meta is not None:
            r.set(f"io:in:{unique_id}", json.dumps(input_meta), ex=ttl)
        if output_meta is not None:
            r.set(f"io:out:{unique_id}", json.dumps(output_meta), ex=ttl)
        return True
    except Exception:
        logger.exception("Failed setting IO keys for %s", unique_id)
        return False


def process_document_job(chat_id: int, file_id: str, filename: str, mime: Optional[str] = "", file_unique_id: Optional[str] = None) -> None:
    """RQ job: download a Telegram file by file_id, create thumbnail, and send back the original with thumb.

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
    try:
        # 1) getFile (path)
        gf_start = time.time()
        tg_file_path = _tg_get_file_path(None, file_id)
        gf_elapsed = time.time() - gf_start
        out_meta.setdefault("durations", {})["getfile_ms"] = int(gf_elapsed * 1000)
        out_meta.setdefault("timestamps", {})["getfile_end"] = int(time.time())
        try:
            _set_io_keys(unique_key, output_meta=out_meta)
        except Exception:
            pass

        # Decide disk vs in-memory
        upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024

        if config.TMP_DIR:
            # disk-mode
            tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR)
            file_path = os.path.join(tmpdir, filename)

            # download file
            try:
                import config as _conf
                bot_token = _conf.BOT_TOKEN
            except Exception:
                bot_token = None

            dl_start = time.time()
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


# recache_thumbs_job removed (thumbnail caching disabled)
