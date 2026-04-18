import os
import tempfile
import shutil
from typing import Optional
import io
import requests

from telegram import Bot, InputFile

from tools import (
    create_thumbnail_from_pdf,
    create_thumbnail_from_image,
    create_thumbnail_from_pdf_bytes,
    create_thumbnail_from_image_bytes,
)
import config


def process_document_job(bot_token: str, chat_id: int, file_id: str, filename: str, mime: Optional[str] = "") -> None:
    """RQ job: download a Telegram file by file_id, create thumbnail, and send back the original with thumb."""
    bot = Bot(token=bot_token)

    # If TMP_DIR is set, fallback to disk-based processing for large files.
    if config.TMP_DIR:
        tmpdir = tempfile.mkdtemp(dir=config.TMP_DIR)
        try:
            tgfile = bot.get_file(file_id)
            file_path = os.path.join(tmpdir, filename)
            tgfile.download(custom_path=file_path)

            thumb_path = os.path.join(tmpdir, "thumb.jpg")
            if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
                create_thumbnail_from_pdf(file_path, thumb_path)
            else:
                create_thumbnail_from_image(file_path, thumb_path)

            with open(file_path, "rb") as f_doc, open(thumb_path, "rb") as f_thumb:
                bot.send_document(chat_id=chat_id, document=f_doc, thumb=f_thumb,
                                  caption="Here is your file with an auto-generated cover preview.")
        except Exception as e:
            try:
                bot.send_message(chat_id=chat_id, text=f"Error processing file in background: {e}")
            except Exception:
                pass
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        return

    # In-memory processing (no disk writes)
    try:
        tgfile = bot.get_file(file_id)

        bio = io.BytesIO()
        try:
            # Attempt to download into in-memory buffer using PTB File.download(out=)
            tgfile.download(out=bio)
            bio.seek(0)
            file_bytes = bio.read()
        except Exception:
            # Fallback to direct file URL download
            file_path_attr = getattr(tgfile, "file_path", None)
            if not file_path_attr:
                raise
            url = f"https://api.telegram.org/file/bot{bot_token}/{file_path_attr}"
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        buf.write(chunk)
                file_bytes = buf.getvalue()

        if filename.lower().endswith('.pdf') or 'pdf' in (mime or '').lower():
            thumb_bytes = create_thumbnail_from_pdf_bytes(file_bytes)
        else:
            thumb_bytes = create_thumbnail_from_image_bytes(file_bytes)

        doc_buf = io.BytesIO(file_bytes)
        thumb_buf = io.BytesIO(thumb_bytes)
        doc_buf.seek(0)
        thumb_buf.seek(0)

        bot.send_document(chat_id=chat_id, document=InputFile(doc_buf, filename=filename),
                          thumb=InputFile(thumb_buf, filename="thumb.jpg"),
                          caption="Here is your file with an auto-generated cover preview.")
    except Exception as e:
        try:
            bot.send_message(chat_id=chat_id, text=f"Error processing file in background: {e}")
        except Exception:
            pass


def process_url_job(bot_token: str, chat_id: int, url: str, filename: Optional[str] = None) -> None:
    """RQ job: download a remote URL (PDF), generate thumbnail, and send file back."""
    bot = Bot(token=bot_token)

    # If TMP_DIR is set, use disk-based processing
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
                bot.send_document(chat_id=chat_id, document=f_doc, thumb=f_thumb,
                                  caption="Here is your file with an auto-generated cover preview.")
        except Exception as e:
            try:
                bot.send_message(chat_id=chat_id, text=f"Error processing URL in background: {e}")
            except Exception:
                pass
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


    def process_document_batch_job(bot_token: str, chat_id: int, items: list) -> None:
        """Process a list of document items sequentially.

        Each item should be a dict with keys: file_id, filename, mime (optional).
        """
        bot = Bot(token=bot_token)
        for idx, item in enumerate(items):
            try:
                file_id = item.get("file_id")
                filename = item.get("filename") or f"file_{file_id}"
                mime = item.get("mime", "")
                # Reuse existing single-file processor for robustness
                process_document_job(bot_token, chat_id, file_id, filename, mime)
                # small pause to avoid flooding Telegram
                time.sleep(0.8)
            except Exception as e:
                try:
                    bot.send_message(chat_id=chat_id, text=f"Error processing item #{idx+1}: {e}")
                except Exception:
                    pass
        return

    # In-memory download and processing
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

        bot.send_document(chat_id=chat_id, document=InputFile(doc_buf, filename=filename),
                          thumb=InputFile(thumb_buf, filename="thumb.jpg"),
                          caption="Here is your file with an auto-generated cover preview.")
    except Exception as e:
        try:
            bot.send_message(chat_id=chat_id, text=f"Error processing URL in background: {e}")
        except Exception:
            pass
