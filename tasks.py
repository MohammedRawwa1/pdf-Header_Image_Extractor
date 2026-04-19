import os
import tempfile
import shutil
from typing import Optional
import io
import requests
import time
import logging

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
    import cache
except Exception:
    cache = None
try:
    from rq import get_current_job
except Exception:
    get_current_job = None

try:
    from storage import upload_file_and_get_presigned_url
except Exception:
    upload_file_and_get_presigned_url = None


def process_document_job(chat_id: int, file_id: str, filename: str, mime: Optional[str] = "", file_unique_id: Optional[str] = None) -> None:
    """RQ job: download a Telegram file by file_id, create thumbnail, and send back the original with thumb.

    NOTE: This function reads the bot token from `config.BOT_TOKEN` internally; do NOT pass the token as a job argument.
    """
    # If TMP_DIR is set, fallback to disk-based processing for large files.
    if config.TMP_DIR:
        tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR)
        try:
            # Fetch Telegram file path and download via HTTP
            tg_file_path = _tg_get_file_path(None, file_id)
            # resolve bot token from config for file download URL
            try:
                import config as _config
                _bot_token = _config.BOT_TOKEN
            except Exception:
                _bot_token = None
            file_path = os.path.join(tmpdir, filename)
            with requests.get(f"https://api.telegram.org/file/bot{_bot_token}/{tg_file_path}", stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            f.write(chunk)

            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
                create_thumbnail_from_pdf(file_path, thumb_path)
            else:
                create_thumbnail_from_image(file_path, thumb_path)

            # Check upload limit (use configured MAX_FILE_SIZE if >0, otherwise default 50MB)
            upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
            try:
                orig_size = os.path.getsize(file_path)
            except Exception:
                orig_size = None

            upload_path = file_path
            # Attempt compression when file is larger than upload_limit
            if orig_size and upload_limit and orig_size > upload_limit:
                # Try default compression (Ghostscript /ebook then /screen)
                compressed1 = file_path + ".compressed.pdf"
                tried = False
                try:
                    tried = compress_pdf(file_path, compressed1, gs_quality="/ebook")
                except Exception:
                    tried = False
                if tried:
                    try:
                        csize = os.path.getsize(compressed1)
                    except Exception:
                        csize = None
                    if csize and csize <= upload_limit:
                        upload_path = compressed1
                    else:
                        # try more aggressive compression and accept only if under the limit
                        compressed2 = file_path + ".compressed.screen.pdf"
                        try:
                            if compress_pdf(file_path, compressed2, gs_quality="/screen"):
                                c2 = os.path.getsize(compressed2)
                                if c2 and c2 <= upload_limit:
                                    upload_path = compressed2
                        except Exception:
                            pass
                # If compression didn't produce an acceptable file, try S3 fallback (if enabled), otherwise notify and stop
                if upload_path == file_path and orig_size and orig_size > upload_limit:
                    # S3 fallback
                    if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None):
                        try:
                            if upload_file_and_get_presigned_url:
                                url = upload_file_and_get_presigned_url(file_path, filename)
                                if url:
                                    try:
                                        _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                                    except Exception:
                                        pass
                                    return {"s3_url": url}
                        except Exception:
                            logger.exception("S3 fallback failed for file_id=%s", file_id)

                    try:
                        _tg_send_message(None, chat_id, f"File too large to upload via bot ({orig_size} bytes); compression didn't reduce it below {upload_limit} bytes. Consider external storage or a smaller file.")
                    except Exception:
                        pass
                    return {"error": "file too large after compression"}

            with open(upload_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                # cache thumbnail for future use
                try:
                    if cache is not None:
                        key = file_unique_id or file_id
                        try:
                            cache.set_thumbnail(key, file_id, f_thumb.read())
                            # reset file pointer for upload
                            f_thumb.seek(0)
                        except Exception:
                            pass
                except Exception:
                    pass
                res = _tg_send_document(None, chat_id, f_doc, filename, thumb_fileobj=f_thumb,
                                       caption="Here is your file with an auto-generated cover preview.")
                # persist response in job meta for debugging
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
            logger.exception("Error in disk-mode processing for file_id=%s", file_id)
            try:
                _tg_send_message(None, chat_id, f"Error processing file in background: {e}")
            except Exception:
                pass
            return {"error": str(e)}


        
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return

    # In-memory processing (no disk writes)
    try:
        # Get file path and download into memory via HTTP
        tg_file_path = _tg_get_file_path(None, file_id)
        file_bytes = _tg_download_to_bytes(None, tg_file_path)

        if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
            thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)
        else:
            thumb_bytes = create_thumbnail_from_image_bytes(file_bytes)

        # If in-memory buffer is large, attempt compression via a temp file
        upload_limit = config.MAX_FILE_SIZE if getattr(config, 'MAX_FILE_SIZE', 0) and config.MAX_FILE_SIZE > 0 else 50 * 1024 * 1024
        if upload_limit and len(file_bytes) > upload_limit:
            tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR) if config.TMP_DIR else tempfile.mkdtemp()
            try:
                tmp_in = os.path.join(tmpdir, filename)
                with open(tmp_in, 'wb') as f:
                    f.write(file_bytes)
                compressed_tmp = tmp_in + '.compressed.pdf'
                compressed_ok = False
                try:
                    compressed_ok = compress_pdf(tmp_in, compressed_tmp, gs_quality="/ebook")
                except Exception:
                    compressed_ok = False
                if compressed_ok:
                    try:
                        csize = os.path.getsize(compressed_tmp)
                    except Exception:
                        csize = None
                    if csize and csize <= upload_limit:
                        with open(compressed_tmp, 'rb') as cf:
                            file_bytes = cf.read()
                    else:
                        # try more aggressive
                        compressed2 = tmp_in + '.compressed.screen.pdf'
                        try:
                            if compress_pdf(tmp_in, compressed2, gs_quality="/screen"):
                                c2 = os.path.getsize(compressed2)
                                if c2 and c2 <= upload_limit:
                                    with open(compressed2, 'rb') as cf:
                                        file_bytes = cf.read()
                                else:
                                    # still too big: try S3 fallback if enabled
                                    if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None):
                                        try:
                                            # prefer the more compressed file if it exists
                                            candidate = compressed2 if os.path.exists(compressed2) else compressed_tmp
                                            if upload_file_and_get_presigned_url and candidate and os.path.exists(candidate):
                                                url = upload_file_and_get_presigned_url(candidate, filename)
                                                if url:
                                                    try:
                                                        _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                                                    except Exception:
                                                        pass
                                                    return {"s3_url": url}
                                        except Exception:
                                            logger.exception("S3 fallback failed for in-memory file for chat_id=%s", chat_id)
                                    try:
                                        _tg_send_message(None, chat_id, f"File too large to upload via bot after compression; size={len(file_bytes)} bytes")
                                    except Exception:
                                        pass
                                    return {"error": "file too large after compression"}
                        except Exception:
                            # Compression attempt failed; try S3 fallback or notify
                            if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None):
                                try:
                                    # upload original tmp_in
                                    if upload_file_and_get_presigned_url and os.path.exists(tmp_in):
                                        url = upload_file_and_get_presigned_url(tmp_in, filename)
                                        if url:
                                            try:
                                                _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                                            except Exception:
                                                pass
                                            return {"s3_url": url}
                                except Exception:
                                    logger.exception("S3 fallback failed for in-memory compression exception for chat_id=%s", chat_id)
                            try:
                                _tg_send_message(None, chat_id, f"File too large to upload via bot after compression; size={len(file_bytes)} bytes")
                            except Exception:
                                pass
                            return {"error": "file too large after compression"}
                else:
                    # compression not available; try S3 fallback
                    if getattr(config, 'ENABLE_S3_FALLBACK', False) and getattr(config, 'S3_BUCKET', None):
                        try:
                            if upload_file_and_get_presigned_url and os.path.exists(tmp_in):
                                url = upload_file_and_get_presigned_url(tmp_in, filename)
                                if url:
                                    try:
                                        _tg_send_message(None, chat_id, f"File was too large for Telegram; uploaded to external storage: {url}")
                                    except Exception:
                                        pass
                                    return {"s3_url": url}
                        except Exception:
                            logger.exception("S3 fallback failed for in-memory fallback for chat_id=%s", chat_id)
                    try:
                        _tg_send_message(None, chat_id, f"File too large to upload via bot and compression is not available on server; size={len(file_bytes)} bytes")
                    except Exception:
                        pass
                    return {"error": "file too large and compression unavailable"}
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

        doc_buf = io.BytesIO(file_bytes)
        thumb_buf = io.BytesIO(thumb_bytes)
        doc_buf.seek(0)
        thumb_buf.seek(0)

        # store cache entry if possible
        try:
            if cache is not None:
                key = file_unique_id or file_id
                try:
                    cache.set_thumbnail(key, file_id, thumb_bytes)
                except Exception:
                    pass
        except Exception:
            pass

        res = _tg_send_document(None, chat_id, doc_buf, filename, thumb_fileobj=thumb_buf,
                                caption="Here is your file with an auto-generated cover preview.")
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
        logger.exception("Error in in-memory processing for file_id=%s", file_id)
        try:
            _tg_send_message(None, chat_id, f"Error processing file in background: {e}")
        except Exception:
            pass
        return {"error": str(e)}


def process_url_job(chat_id: int, url: str, filename: Optional[str] = None) -> None:
    """RQ job: download a remote URL (PDF), generate thumbnail, and send file back.

    NOTE: Bot token is read from `config.BOT_TOKEN` internally; do NOT pass it as job arg.
    """

    # Disk-mode when TMP_DIR specified
    if config.TMP_DIR:
        tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR)
        try:
            if not filename:
                filename = os.path.basename(url.split('?', 1)[0]) or 'download.pdf'
            if not filename.lower().endswith('.pdf'):
                filename = filename + '.pdf'
            file_path = os.path.join(tmpdir, filename)

            with requests.get(url, stream=True, allow_redirects=True, timeout=60) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            f.write(chunk)

            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            create_thumbnail_from_pdf(file_path, thumb_path)

            with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                res = _tg_send_document(None, chat_id, f_doc, filename, thumb_fileobj=f_thumb,
                                       caption="Here is your file with an auto-generated cover preview.")
                return res
        except Exception as e:
            try:
                _tg_send_message(None, chat_id, f"Error processing URL in background: {e}")
            except Exception:
                pass
            return {"error": str(e)}
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

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


def recache_thumbs_job(admin_chat_id: Optional[int] = None, limit: Optional[int] = None, dry_run: bool = False) -> dict:
    """Scan Redis for thumbnail metadata keys missing the blob and regenerate cached thumb bytes.

    If `admin_chat_id` is provided, sends progress messages to that chat.
    Returns a dict with counts.
    """
    results = {"scanned": 0, "recached": 0, "skipped": 0, "errors": 0}
    if not getattr(config, 'REDIS_URL', None):
        return {"error": "no redis configured"}
    try:
        from redis import Redis
        r = Redis.from_url(config.REDIS_URL)
    except Exception as e:
        logger.exception("Failed to connect to Redis for recache")
        return {"error": str(e)}

    try:
        it = r.scan_iter(match='thumb:*')
    except Exception:
        # older redis-py may not have scan_iter on connection; fall back to keys (not recommended)
        try:
            it = iter(r.keys('thumb:*'))
        except Exception:
            return {"error": "failed enumerating keys"}

    for idx, meta_key in enumerate(it):
        # meta_key may be bytes
        try:
            if isinstance(meta_key, (bytes, bytearray)):
                meta_key = meta_key.decode('utf-8')
        except Exception:
            continue
        # skip blob keys
        if meta_key.endswith(':b'):
            continue
        results['scanned'] += 1
        if limit and results['scanned'] > limit:
            break

        blob_key = meta_key + ':b'
        try:
            exists = r.exists(blob_key)
        except Exception:
            exists = False
        if exists:
            results['skipped'] += 1
            continue

        # need to recache
        try:
            fid = r.hget(meta_key, 'file_id')
            if not fid:
                results['errors'] += 1
                continue
            if isinstance(fid, (bytes, bytearray)):
                fid = fid.decode('utf-8', errors='ignore')

            # fetch file bytes from Telegram and generate thumbnail
            try:
                tg_path = _tg_get_file_path(None, fid)
                f_bytes = _tg_download_to_bytes(None, tg_path)
            except Exception:
                logger.exception("Failed to download file for recache, file_id=%s", fid)
                results['errors'] += 1
                continue

            # try PDF thumbnail first, then image
            thumb_bytes = None
            try:
                thumb_bytes = create_thumbnail_from_pdf_bytes(f_bytes)
            except Exception:
                try:
                    thumb_bytes = create_thumbnail_from_image_bytes(f_bytes)
                except Exception:
                    thumb_bytes = None

            if not thumb_bytes:
                results['errors'] += 1
                continue

            if not dry_run:
                try:
                    # unique id is the meta_key suffix after 'thumb:'
                    unique_id = meta_key.split(':', 1)[1] if ':' in meta_key else meta_key
                    cache.set_thumbnail(unique_id, fid, thumb_bytes)
                    results['recached'] += 1
                except Exception:
                    logger.exception("Failed writing cache for %s", meta_key)
                    results['errors'] += 1
                    continue
            else:
                results['recached'] += 1

        except Exception:
            logger.exception("Unexpected error while recaching %s", meta_key)
            results['errors'] += 1

        # optionally notify admin periodically
        if admin_chat_id and results['scanned'] % 25 == 0:
            try:
                _tg_send_message(None, admin_chat_id, f"Recache progress: scanned={results['scanned']} recached={results['recached']} errors={results['errors']}")
            except Exception:
                pass

    # final admin notification
    if admin_chat_id:
        try:
            _tg_send_message(None, admin_chat_id, f"Recache finished: scanned={results['scanned']} recached={results['recached']} errors={results['errors']}")
        except Exception:
            pass

    return results
