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

            with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
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
