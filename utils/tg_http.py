"""Raw Telegram Bot API HTTP helpers shared by the web process and the workers.

PTB's ``send_document`` exposes no upload-progress hook and buffers the whole
file into memory up front (``InputFile`` -> ``load_file()`` -> ``obj.read()``),
so long-running document sends stream the multipart body over raw Bot API HTTP
instead.  This module is the single home for ALL raw Bot API HTTP calls --
``sendDocument`` (with live progress), ``sendMessage``, ``editMessageText``,
``deleteMessage``, ``forwardMessage``, ``getFile`` (file_id resolution), the
file downloads (to memory or disk) and the HTTP-based progress messages used
by the workers -- so ``bot.py`` (web process) and ``tasks.py`` (RQ + pipeline
workers) share one implementation without cross-importing each other.

The ``_tg_*`` helpers retry transient 429/5xx responses with backoff (the
worker shares the bot token with the web process, so sends must tolerate
global-rate-limit responses instead of failing the job).
"""

import atexit
import io
import json
import logging
import os
import time
from collections.abc import Callable
from typing import Any, Protocol

import requests

from utils.markdown_utils import escape_markdown, sanitize_text
from utils.ocr import is_ocr_source, ocr_enabled
from utils.progress_tracker import _build_progress_bar, _format_size

logger = logging.getLogger(__name__)


def _sanitize_outbound(text: str | None) -> str | None:
\
\
\
\
\
\
\
\
       
    if text is None:
        return None
    return sanitize_text(text)

                                                                        
                                                                               
                                                                            
                                                                         
                                                                              
                                                        
_SESSION = requests.Session()
atexit.register(_SESSION.close)


def _get_bot_token(bot_token: str | None = None) -> str | None:
\
\
\
\
\
\
       
    if bot_token:
        return bot_token
    try:
        import config as _config

        return _config.BOT_TOKEN
    except Exception:
        return None


class _BinaryFile(Protocol):
\
\
\
\
\
       

    def read(self, size: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...

    def tell(self) -> int: ...

    def fileno(self) -> int: ...


class _ProgressCallback(Protocol):
\
\
\
\
\
       

    def __call__(self, current: int, total: int) -> None: ...


class _ProgressFileReader:
\
\
\
\
\
       

    def __init__(
        self, fh: _BinaryFile, total: int, callback: _ProgressCallback, throttle: float = 0.7
    ):
        self._fh = fh
        self._total = total or 0
        self._callback = callback
        self._read = 0
        self._last_t = 0.0
        self._throttle = throttle

    def read(self, size: int = -1):
        data = self._fh.read(size)
        if data:
            self._read += len(data)
            self._maybe_report()
        return data

    def seek(self, offset: int, whence: int = 0):
        result = self._fh.seek(offset, whence)
        if offset == 0 and whence == 0:
            self._read = 0
            self._last_t = 0.0
        return result

    def tell(self):
        return self._fh.tell()

    def __getattr__(self, name):
        return getattr(self._fh, name)

    def _maybe_report(self):
        if self._callback is None:
            return
        now = time.time()
        if self._read >= self._total or (
            now - self._last_t
        ) >= self._throttle:
            self._last_t = now
            try:
                self._callback(self._read, self._total)
            except Exception:                                        
                pass


def _tg_send_document(
    bot_token: str | None,
    chat_id: int,
    doc_fileobj: _BinaryFile,
    filename: str,
    thumb_fileobj: _BinaryFile | None = None,
    caption: str | None = None,
    progress_callback: _ProgressCallback | None = None,
    compress_user_id: int | None = None,
    convert_user_id: int | None = None,
    ocr_user_id: int | None = None,
    done_ops: tuple[str, ...] = (),
) -> dict:
\
\
\
\
\
\
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    if progress_callback is not None:
                                                                           
        try:
            _doc_total = os.fstat(doc_fileobj.fileno()).st_size
        except Exception:
            _doc_total = 0
        doc_fileobj = _ProgressFileReader(
            doc_fileobj, _doc_total, progress_callback
        )
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
                                                                    
                                                                           
                                                                     
                                                                         
                                   
    filename = _sanitize_outbound(filename) or "file"
    caption = _sanitize_outbound(caption)
    files: dict[str, Any] = {"document": (filename, doc_fileobj)}
    if thumb_fileobj is not None:
        files["thumb"] = ("thumb.jpg", thumb_fileobj, "image/jpeg")
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
                                                                            
                                                                              
                                                             
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
                                                                      
                                                                         
                                             
            try:
                doc_fileobj.seek(0)
                if thumb_fileobj is not None:
                    thumb_fileobj.seek(0)
            except Exception:                                     
                pass
            r = _SESSION.post(url, data=data, files=files, timeout=120)
            if r.status_code in (429,) or r.status_code >= 500:
                last_exc = requests.exceptions.HTTPError(
                    f"Telegram sendDocument failed: status={r.status_code}"
                )
                logger.warning(
                    "_tg_send_document: HTTP %s on attempt %d for %s",
                    r.status_code,
                    attempt + 1,
                    filename,
                )
                                                                            
                                                                             
                                                                            
                                                                    
                try:
                    r.close()
                except Exception:                                    
                    pass
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise last_exc
            r.raise_for_status()
            _res = r.json()
                                                                          
                                                                          
                                                         
            if _res and _res.get("ok"):
                _attach_send_buttons(
                    chat_id,
                    _res,
                    filename,
                    compress_user_id,
                    convert_user_id,
                    ocr_user_id,
                    done_ops,
                )
            return _res
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            last_exc = e
            logger.warning(
                "_tg_send_document: transient error %s on attempt %d for %s",
                type(e).__name__,
                attempt + 1,
                filename,
            )
            if attempt < 2:
                time.sleep(2**attempt)
                continue
    raise last_exc or RuntimeError(f"Failed to send document {filename}")


def _attach_send_buttons(
    chat_id: int,
    res: dict,
    filename: str,
    compress_user_id: int | None = None,
    convert_user_id: int | None = None,
    ocr_user_id: int | None = None,
    done_ops: tuple[str, ...] = (),
) -> None:
\
\
\
\
\
\
\
\
\
\
\
\
       
    if not res or not res.get("ok"):
        return
    try:
        _result = res.get("result") or {}
        _msg_id = _result.get("message_id")
        _doc = _result.get("document") or {}
        _actions: list[tuple[str, str, str]] = []
        if (
            compress_user_id
            and filename
            and filename.lower().endswith(".pdf")
            and "compress" not in done_ops
        ):
                                                                           
                                                                            
                                     
            _actions.append(
                (
                    COMPRESS_PDF_ACTION[0],
                    COMPRESS_PDF_ACTION[1],
                    COMPRESS_PDF_ACTION[2],
                )
            )
        if (
            convert_user_id
            and filename
            and not filename.lower().endswith(".pdf")
            and "convert" not in done_ops
        ):
                                                                       
                                             
            _actions.append(
                (
                    BOOK_CONVERT_ACTION[0],
                    BOOK_CONVERT_ACTION[1],
                    BOOK_CONVERT_ACTION[2],
                )
            )
        if (
            ocr_user_id
            and is_ocr_source(filename)
            and ocr_enabled()
            and "ocr" not in done_ops
        ):
                                                                          
                                                                     
            _actions.append((OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2]))
        if _actions:
            _attach_pending_buttons(
                chat_id,
                _msg_id,
                _doc.get("file_id"),
                _doc.get("file_unique_id"),
                filename,
                compress_user_id or convert_user_id or ocr_user_id,
                _doc.get("file_size"),
                tuple(_actions),
            )
    except Exception:                                   
        pass


def _tg_send_document_by_id(
    bot_token: str | None,
    chat_id: int,
    document_file_id: str,
    filename: str,
    caption: str | None = None,
    compress_user_id: int | None = None,
    convert_user_id: int | None = None,
    ocr_user_id: int | None = None,
    done_ops: tuple[str, ...] = (),
) -> dict:
\
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
                                                                           
                                                                          
                                                          
    filename = _sanitize_outbound(filename) or "file"
    caption = _sanitize_outbound(caption)
    data: dict[str, Any] = {
        "chat_id": str(chat_id),
        "document": document_file_id,
    }
    if caption:
        data["caption"] = caption
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = _SESSION.post(url, data=data, timeout=60)
            if r.status_code in (429,) or r.status_code >= 500:
                last_exc = requests.exceptions.HTTPError(
                    f"Telegram sendDocument-by-id failed: status={r.status_code}"
                )
                logger.warning(
                    "_tg_send_document_by_id: HTTP %s on attempt %d",
                    r.status_code,
                    attempt + 1,
                )
                try:
                    r.close()
                except Exception:                                    
                    pass
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise last_exc
            r.raise_for_status()
            _res = r.json()
            if _res and _res.get("ok"):
                _attach_send_buttons(
                    chat_id,
                    _res,
                    filename,
                    compress_user_id,
                    convert_user_id,
                    ocr_user_id,
                    done_ops,
                )
            return _res
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            last_exc = e
            logger.warning(
                "_tg_send_document_by_id: transient error %s on attempt %d",
                type(e).__name__,
                attempt + 1,
            )
            if attempt < 2:
                time.sleep(2**attempt)
                continue
    raise last_exc or RuntimeError(
        f"Failed to re-send document by file_id {document_file_id}"
    )


def _tg_send_message(
    bot_token: str | None,
    chat_id: int,
    text: str,
    reply_markup: dict | None = None,
    parse_mode: str | None = None,
):
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                                                                          
                                                                          
                                                            
    data = {"chat_id": str(chat_id), "text": _sanitize_outbound(text) or ""}
    if reply_markup is not None:
                                                                           
                                                                       
                                                      
        data["reply_markup"] = json.dumps(reply_markup)
    if parse_mode:
        data["parse_mode"] = parse_mode
                                                                        
                                                                           
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = _SESSION.post(url, data=data, timeout=30)
            if r.status_code in (429,) or r.status_code >= 500:
                last_exc = requests.exceptions.HTTPError(
                    f"Telegram sendMessage failed: status={r.status_code}"
                )
                logger.warning(
                    "_tg_send_message: HTTP %s on attempt %d (chat %s)",
                    r.status_code,
                    attempt + 1,
                    chat_id,
                )
                                                                            
                                                                              
                                                                         
                                                                            
                                             
                try:
                    r.close()
                except Exception:                                    
                    pass
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise last_exc
            r.raise_for_status()
            return r.json()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            last_exc = e
            logger.warning(
                "_tg_send_message: transient error %s on attempt %d (chat %s)",
                type(e).__name__,
                attempt + 1,
                chat_id,
            )
            if attempt < 2:
                time.sleep(2**attempt)
                continue
    raise last_exc or RuntimeError(f"Failed to send message to chat {chat_id}")


def _tg_edit_message_text(
    chat_id: int,
    message_id: int,
    text: str,
    parse_mode: str = "Markdown",
    reply_markup: dict | None = None,
):
\
\
\
\
\
\
       
    bot_token = _get_bot_token()
    if not bot_token:
        return None
    try:
        url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
        data = {
            "chat_id": str(chat_id),
            "message_id": message_id,
                                                                          
            "text": _sanitize_outbound(text) or "",
            "parse_mode": parse_mode,
        }
        if reply_markup is not None:
                                                                
            data["reply_markup"] = json.dumps(reply_markup)
        r = _SESSION.post(url, data=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _tg_edit_message_reply_markup(
    chat_id: int, message_id: int | None, reply_markup: dict | None = None
) -> bool:
\
\
\
\
\
\
\
       
    if not message_id:
        return False
    bot_token = _get_bot_token()
    if not bot_token:
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup"
        r = _SESSION.post(
            url,
            data={
                "chat_id": str(chat_id),
                "message_id": message_id,
                                                                    
                "reply_markup": json.dumps(
                    reply_markup or {"inline_keyboard": []}
                ),
            },
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception:
        return False


def _attach_pending_buttons(
    chat_id: int,
    message_id: int | None,
    file_id: str | None,
    file_unique_id: str | None,
    filename: str | None,
    user_id: int | None,
    file_size: int | None,
    actions: tuple[tuple[str, str, str], ...],
) -> None:
\
\
\
\
\
\
\
       
    if not message_id or not file_id or not actions:
        return
    try:
        import secrets

        kb_rows: list[list[dict]] = []
        for prefix, text, cb in actions:
            token = secrets.token_hex(4)
            _store_pending_record(
                prefix,
                token,
                chat_id=chat_id,
                message_id=message_id,
                file_id=file_id,
                file_unique_id=file_unique_id,
                filename=filename,
                user_id=user_id,
                file_size=file_size,
            )
            kb_rows.append(
                [
                    {
                        "text": text,
                        "callback_data": f"{cb}:{user_id or 0}:{token}",
                    }
                ]
            )
        _tg_edit_message_reply_markup(
            chat_id, message_id, {"inline_keyboard": kb_rows}
        )
    except Exception:                                          
        pass


                                                                             
                                                                               
                                                                     
                                                                           
BOOK_CONVERT_ACTION: tuple[str, str, str, str] = (
    "bookconvert",
    "\U0001f501 Convert",
    "bookconvert",
    "\U0001f4da Your book was large, so it was delivered via the "
    "userbot. Tap **Convert** to re-format it to another format.",
)
COMPRESS_PDF_ACTION: tuple[str, str, str, str] = (
    "bookcompress",
    "\U0001f5dc\ufe0f Compress PDF",
    "compresspdf",
    "\U0001f5dc\ufe0f Your PDF was delivered via the userbot. "
    "Tap **Compress PDF** to shrink it.",
)
OCR_ACTION: tuple[str, str, str, str] = (
    "bookocr",
    "\U0001f50e\U0001f5bc\ufe0f OCR & Thumbnail",
    "ocr",
    "\U0001f50e Your file was delivered via the userbot. "
    "Tap **OCR & Thumbnail** to make it a searchable PDF (selectable "
    "text), extract plain text, or get a cover preview.",
)


def _store_pending_record(
    prefix: str,
    token: str,
    *,
    chat_id: int | str,
    message_id: int | None,
    file_id: str | None,
    file_unique_id: str | None,
    filename: str | None,
    user_id: int | None,
    file_size: int | None = None,
    mime: str = "",
    source_chat_id: int | str | None = None,
) -> None:
\
\
\
\
\
\
\
\
\
       
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if r:
            try:
                import config as _cfg

                _ttl = getattr(_cfg, "BOOK_ASK_TTL_SECONDS", 600)
            except Exception:              
                _ttl = 600
            r.setex(
                f"{prefix}:{token}",
                _ttl,
                json.dumps(
                    {
                        "file_id": file_id,
                        "file_unique_id": file_unique_id,
                        "filename": filename,
                        "mime": mime,
                        "chat_id": chat_id,
                        "source_chat_id": source_chat_id,
                        "message_id": message_id,
                        "file_size": file_size,
                        "user_id": user_id,
                    }
                ),
            )
    except Exception:              
        pass





def sent_doc_file_unique_id(msg: Any | None) -> str | None:
\
\
\
\
\
\
\
\
       
    if msg is None:
        return None
    _doc = getattr(msg, "document", None)
    if _doc is None:
        _doc = getattr(msg, "file", None)
    return getattr(_doc, "file_unique_id", None) or None


def _tg_send_pending_prompt(
    record_prefix: str,
    button_text: str,
    callback_prefix: str,
    prompt_text: str,
    *,
    chat_id: int,
    filename: str,
    user_id: int | None,
    file_size: int | None,
    src_chat_id: int | str,
    src_message_id: int | None,
    file_unique_id: str | None = None,
    mime: str = "",
    extra_action: tuple[str, str, str] | None = None,
) -> None:
\
\
\
\
\
\
\
\
       
    if not src_message_id:
        return
    try:
        import secrets

        def _store(src_prefix: str) -> str:
            _t = secrets.token_hex(4)
            _store_pending_record(
                src_prefix,
                _t,
                chat_id=chat_id,
                source_chat_id=src_chat_id,
                message_id=src_message_id,
                file_id=None,
                file_unique_id=file_unique_id,
                filename=filename,
                user_id=user_id,
                file_size=file_size,
                mime=mime,
            )
            return _t

        kb_rows: list[list[dict]] = [
            [
                {
                    "text": button_text,
                    "callback_data": (
                        f"{callback_prefix}:{user_id or 0}:{_store(record_prefix)}"
                    ),
                }
            ]
        ]
        if extra_action:
            _extra_prefix, _extra_text, _extra_cb = extra_action
            kb_rows.append(
                [
                    {
                        "text": _extra_text,
                        "callback_data": (
                            f"{_extra_cb}:{user_id or 0}:{_store(_extra_prefix)}"
                        ),
                    }
                ]
            )
        _tg_send_message(
            None,
            chat_id,
            prompt_text,
            reply_markup={"inline_keyboard": kb_rows},
            parse_mode="Markdown",
        )
    except Exception:                                   
        pass


def _tg_delete_message(chat_id: int, message_id: int | None) -> bool:
                                                                                    
    if not message_id:
        return False
    bot_token = _get_bot_token()
    if not bot_token:
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/deleteMessage"
        r = _SESSION.post(
            url,
            data={"chat_id": str(chat_id), "message_id": message_id},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception:
        return False


def _tg_get_file_path(
    bot_token: str | None,
    file_id: str,
    diagnostic: Callable[[dict], None] | None = None,
) -> str:
\
\
\
\
\
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/bot{bot_token}/getFile"
                                                                           
    for attempt in range(3):
        try:
            r = _SESSION.get(url, params={"file_id": file_id}, timeout=30)
        except Exception:
            logger.exception(
                "Network error fetching getFile for %s (attempt %s)",
                file_id,
                attempt + 1,
            )
            if attempt < 2:
                time.sleep(1 + attempt)
                continue
            raise

        if r.status_code != 200:
                                                                        
            try:
                body = r.json()
                desc = body.get("description") or body
            except Exception:
                desc = r.text
            msg = (
                f"Telegram getFile failed: status={r.status_code} desc={desc}"
            )
            logger.error(msg)
                                                                                
            if diagnostic is not None:
                try:
                    diagnostic(
                        {
                            "status": "getfile_failed",
                            "http_status": r.status_code,
                            "desc": str(desc),
                            "timestamp": int(time.time()),
                        }
                    )
                except Exception:              
                    pass
                                                                    
            if r.status_code >= 500 or r.status_code == 429:
                if attempt < 2:
                    time.sleep(1 + attempt)
                    continue
                                                                                         
            raise requests.exceptions.HTTPError(msg)

        try:
            data = r.json()
            return data["result"]["file_path"]
        except Exception as e:
            logger.exception("Failed parsing getFile JSON for %s", file_id)
                                                                         
            if diagnostic is not None:
                try:
                    diagnostic(
                        {
                            "status": "getfile_failed",
                            "error": str(e),
                            "http_status": r.status_code,
                            "desc": r.text,
                            "timestamp": int(time.time()),
                        }
                    )
                except Exception:              
                    pass
            raise

                                                                              
                                                         
    raise RuntimeError(f"Failed to resolve file_path for {file_id}")


def _tg_download_to_bytes(bot_token: str | None, tg_file_path: str) -> bytes:
\
\
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/file/bot{bot_token}/{tg_file_path}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with _SESSION.get(url, stream=True, timeout=60) as r:
                if r.status_code >= 500 or r.status_code == 429:
                                                                     
                    last_exc = requests.exceptions.HTTPError(
                        f"Telegram download failed: status={r.status_code}"
                    )
                    logger.warning(
                        "_tg_download_to_bytes: HTTP %s on attempt %d for %s",
                        r.status_code,
                        attempt + 1,
                        tg_file_path,
                    )
                    if attempt < 2:
                        time.sleep(2**attempt)
                        continue
                    raise last_exc
                r.raise_for_status()
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        buf.write(chunk)
                return buf.getvalue()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
                                                                       
            last_exc = e
            logger.warning(
                "_tg_download_to_bytes: transient error %s on attempt %d for %s",
                type(e).__name__,
                attempt + 1,
                tg_file_path,
            )
            if attempt < 2:
                time.sleep(2**attempt)
                continue
        except requests.exceptions.HTTPError:
                                                                            
            raise
    raise last_exc or RuntimeError(
        f"Failed to download {tg_file_path} after 3 attempts"
    )


def _tg_download_to_file(
    bot_token: str | None,
    tg_file_path: str,
    dest_path: str,
    total: int = 0,
    progress_callback: _ProgressCallback | None = None,
) -> int:
\
\
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/file/bot{bot_token}/{tg_file_path}"
    seen = 0
    with _SESSION.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as fh:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    fh.write(chunk)
                    seen += len(chunk)
                    if total and progress_callback is not None:
                        try:
                            progress_callback(seen, total)
                        except Exception:                                        
                            pass
    return seen


def _tg_forward_message(
    bot_token: str | None,
    chat_id: int,
    from_chat_id: int,
    message_id: int,
) -> int | None:
\
\
\
\
\
       
    bot_token = _get_bot_token(bot_token)
    try:
        url = f"https://api.telegram.org/bot{bot_token}/forwardMessage"
        r = _SESSION.post(
            url,
            data={
                "chat_id": str(chat_id),
                "from_chat_id": str(from_chat_id),
                "message_id": message_id,
            },
            timeout=30,
        )
        if r.status_code != 200:
            logger.warning(
                "_tg_forward_message: HTTP %s forwarding %s/%s -> %s (%s)",
                r.status_code,
                from_chat_id,
                message_id,
                chat_id,
                r.text[:200],
            )
            return None
        data = r.json()
        return data["result"]["message_id"]
    except Exception:
        logger.warning(
            "_tg_forward_message: failed forwarding %s/%s -> %s",
            from_chat_id,
            message_id,
            chat_id,
        )
        return None


                                                                   
                                                                             
                                                

_PROGRESS_STAGES = {
    "queued": 0,
    "downloading": 25,
    "downloaded": 50,
    "thumbnailing": 65,
    "ocr": 70,
    "compressing": 80,
    "sending": 90,
    "done": 100,
    "failed": 0,
}


def _tg_send_progress(
    chat_id: int,
    filename: str,
    stage: str,
    detail: str = "",
    file_size: int = 0,
    message_id: int | None = None,
    progress_pct: int | None = None,
    reply_markup: dict | None = None,
) -> int | None:
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
       
    pct = _PROGRESS_STAGES.get(stage, 0)
    if progress_pct is not None:
        pct = max(0, min(100, int(progress_pct)))
    bar = _build_progress_bar(pct)

    size_str = _format_size(file_size) if file_size else ""
    emojis = {
        "queued": "\u23f3",
        "downloading": "\U0001f4e5",
        "downloaded": "\u2705",
        "thumbnailing": "\U0001f5bc\ufe0f",
        "ocr": "\U0001f50e",
        "compressing": "\U0001f5dc\ufe0f",
        "sending": "\U0001f4e4",
        "done": "\u2705",
        "failed": "\u274c",
    }
    emoji = emojis.get(stage, "\u2753")

    lines = [
                                                                             
                                                                             
                                                                        
        f"\U0001f4c1 **{escape_markdown(filename)}**",
        f"{bar} `{pct}%`",
    ]
    if size_str:
        lines.insert(1, f"\U0001f4cf Size: `{size_str}`")
    if detail:
        lines.append(f"\n{emoji} {detail}")

    text = "\n".join(lines)

    try:
        if message_id:
            _tg_edit_message_text(
                chat_id, message_id, text, reply_markup=reply_markup
            )
            return message_id
        else:
            res = _tg_send_message(
                None,
                chat_id,
                text,
                reply_markup=reply_markup,
                                                               
                                                                             
                                                                              
                                                                           
                                                                 
                parse_mode="Markdown",
            )
            if res and "result" in res and "message_id" in res["result"]:
                return res["result"]["message_id"]
            return None
    except Exception:
        return None
