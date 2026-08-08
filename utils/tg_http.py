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

from utils.markdown_utils import escape_markdown
from utils.ocr import is_ocr_source, ocr_enabled
from utils.progress_tracker import _build_progress_bar, _format_size

logger = logging.getLogger(__name__)

# Shared HTTP session for Bot API calls.  urllib3's connection pools are
# thread-safe, so a single session is reused across the web process's streaming
# uploads (each runs in its own worker thread via asyncio.to_thread) and the
# RQ/pipeline workers — avoiding a fresh TCP + TLS handshake per request.
# No per-request session state is mutated (headers/cookies stay untouched), so
# concurrent ``post()`` calls through the pool are safe.
_SESSION = requests.Session()
atexit.register(_SESSION.close)


def _get_bot_token(bot_token: str | None = None) -> str | None:
    """Return ``bot_token`` if provided, else the configured ``config.BOT_TOKEN``.

    The Bot API helpers accept an explicit token (e.g. from a caller that
    resolved its own) and otherwise fall back to the shared
    ``config.BOT_TOKEN``.  ``config`` is imported lazily so this module stays
    importable without forcing a config load at import time.
    """
    if bot_token:
        return bot_token
    try:
        import config as _config

        return _config.BOT_TOKEN
    except Exception:
        return None


class _BinaryFile(Protocol):
    """Minimal binary file-object interface consumed by the raw HTTP helpers.

    Satisfied by ``open(...)`` handles and ``io.BytesIO``-style streams; the
    ``_ProgressFileReader`` wrapper satisfies it too via its ``__getattr__``
    delegation.
    """

    def read(self, size: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...

    def tell(self) -> int: ...

    def fileno(self) -> int: ...


class _ProgressCallback(Protocol):
    """``(current_bytes, total_bytes)`` upload/download progress callback.

    Used for both uploads (``_ProgressFileReader``, ``_tg_send_document``) and
    downloads (``_tg_download_to_file``); implementations may also accept
    extra trailing args (Telethon-style callbacks).
    """

    def __call__(self, current: int, total: int) -> None: ...


class _ProgressFileReader:
    """Wrap a binary file object so a multipart upload reports live progress.

    urllib3 reads the payload via ``read()``; we count the bytes and invoke a
    (throttled) ``callback(current, total)``.    ``seek(0)`` — used by the retry
    loop before re-sending — resets the counter so a retry reports from 0 again.
    """

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
            except Exception:  # nosec B110 - progress is best-effort
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
) -> dict:
    """Send a document via the Bot API ``sendDocument`` endpoint.

    Streams the multipart body straight from the open file objects (wrapping
    the document in ``_ProgressFileReader`` to report live progress via
    ``progress_callback(current, total)``) and retries transient 429/5xx
    responses with backoff.

    Returns:
        The Bot API response dict (``{"ok": true, "result": {...}}``).
        Raises after 3 attempts on persistent 429/5xx, connection or timeout
        errors.
    """
    bot_token = _get_bot_token(bot_token)
    if progress_callback is not None:
        # Wrap the file so the multipart upload reports LIVE send progress.
        try:
            _doc_total = os.fstat(doc_fileobj.fileno()).st_size
        except Exception:
            _doc_total = 0
        doc_fileobj = _ProgressFileReader(
            doc_fileobj, _doc_total, progress_callback
        )
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    files: dict[str, Any] = {"document": (filename, doc_fileobj)}
    if thumb_fileobj is not None:
        files["thumb"] = ("thumb.jpg", thumb_fileobj, "image/jpeg")
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
    attach_compress = bool(
        compress_user_id and filename and filename.lower().endswith(".pdf")
    )
    attach_convert = bool(
        convert_user_id and filename and not filename.lower().endswith(".pdf")
    )
    attach_ocr = bool(
        ocr_user_id and is_ocr_source(filename) and ocr_enabled()
    )
    # Retry on transient 429/5xx (Telegram flood control) with backoff — the
    # worker shares the bot token with the web process, so sends must tolerate
    # global-rate-limit responses instead of failing the job.
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            # Rewind file streams so a retry re-sends the FULL payload
            # (requests consumes the file object; without seek(0) a retry
            # would upload a truncated file).
            try:
                doc_fileobj.seek(0)
                if thumb_fileobj is not None:
                    thumb_fileobj.seek(0)
            except Exception:  # nosec B110 - non-seekable streams
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
                # The error body is never read, so close the response so the
                # connection is released promptly instead of being held until
                # GC (urllib3 discards a partially-read connection, which is
                # fine — the retry opens a fresh one from the pool).
                try:
                    r.close()
                except Exception:  # nosec B110 - best-effort cleanup
                    pass
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise last_exc
            r.raise_for_status()
            _res = r.json()
            # Attach the Compress-PDF button to delivered PDF results (one
            # tap -> compress_pdf_job).  Uses the DELIVERED file_id so the
            # user compresses exactly what they received.
            if _res and _res.get("ok"):
                try:
                    _result = _res.get("result") or {}
                    _msg_id = _result.get("message_id")
                    _doc = _result.get("document") or {}
                    _actions: list[tuple[str, str, str]] = []
                    if attach_compress:
                        # 🗜 Compress on delivered PDFs (thumb/convert/compress
                        # results — one tap -> compress_pdf_job).
                        _actions.append(
                            (
                                COMPRESS_PDF_ACTION[0],
                                COMPRESS_PDF_ACTION[1],
                                COMPRESS_PDF_ACTION[2],
                            )
                        )
                    if attach_convert:
                        # 🔁 Convert on delivered e-books (its own interface —
                        # never mixed with the thumbnail flow).
                        _actions.append(
                            (
                                BOOK_CONVERT_ACTION[0],
                                BOOK_CONVERT_ACTION[1],
                                BOOK_CONVERT_ACTION[2],
                            )
                        )
                    if attach_ocr:
                        # 🔎 OCR on delivered PDFs/images (scanned text).
                        _actions.append(
                            (OCR_ACTION[0], OCR_ACTION[1], OCR_ACTION[2])
                        )
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
                except Exception:  # nosec B110 - best-effort button
                    pass
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


def _tg_send_message(
    bot_token: str | None,
    chat_id: int,
    text: str,
    reply_markup: dict | None = None,
    parse_mode: str | None = None,
):
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": str(chat_id), "text": text}
    if reply_markup is not None:
        # The Bot API expects reply_markup as a JSON-serialized form value;
        # passing the dict raw makes requests urlencode it as a mangled
        # Python repr (and mypy flags the assignment).
        data["reply_markup"] = json.dumps(reply_markup)
    if parse_mode:
        data["parse_mode"] = parse_mode
    # Retry on transient 429/5xx (Telegram flood control) with backoff —
    # mirrors the sendDocument helper so background workers survive bursts.
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
                # The error body is never read, so close the response so the
                # pooled connection is released promptly instead of being held
                # through the backoff sleep and retry (urllib3 discards a
                # partially-read connection, which is fine — the retry opens
                # a fresh one from the pool).
                try:
                    r.close()
                except Exception:  # nosec B110 - best-effort cleanup
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
    """Edit a previously-sent message using Bot API's editMessageText.

    ``reply_markup`` is an optional inline-keyboard payload (``{"inline_keyboard":
    [...]}``). Pass ``{"inline_keyboard": []}`` to remove an existing keyboard.

    Returns the API response dict on success, or None on failure (non-fatal).
    """
    bot_token = _get_bot_token()
    if not bot_token:
        return None
    try:
        url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
        data = {
            "chat_id": str(chat_id),
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        if reply_markup is not None:
            # JSON-serialized form value — see _tg_send_message.
            data["reply_markup"] = json.dumps(reply_markup)
        r = _SESSION.post(url, data=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _tg_edit_message_reply_markup(
    chat_id: int, message_id: int | None, reply_markup: dict | None = None
) -> bool:
    """Set (or clear) the inline keyboard on a message (editMessageReplyMarkup).

    ``reply_markup=None`` sends an empty keyboard (removes the buttons) — used
    to strip a stale cancel button once a job hands off to a different
    pipeline, so a file never shows two cancel controls.  Pass a keyboard dict
    to attach a new set of buttons (e.g. the Compress-PDF button on a
    delivered result). Best-effort.
    """
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
                # JSON-serialized form value — see _tg_send_message.
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
    """Store pending records and attach ONE combined keyboard to a result.

    ``actions`` is a sequence of ``(record_prefix, button_text,
    callback_prefix)`` — e.g. (Compress, OCR) on a delivered PDF.  Each action
    gets its own token + pending record; all buttons land on a single keyboard
    via one edit (two sequential edits would overwrite each other).  Best-
    effort: any failure just leaves the message without buttons.
    """
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
    except Exception:  # nosec B110 - best-effort button attach
        pass


# ── Pending-action button configs ──────────────────────────────────────────
# ``(record_prefix, button_text, callback_prefix, prompt_text)`` — defined once
# so the attach helpers (tg_http.py) and the worker prompt call sites
# (tasks.py) can never drift apart on labels, prefixes or user-facing text.
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
    "\U0001f50e OCR",
    "ocr",
    "\U0001f50e Your file was delivered via the userbot. "
    "Tap **OCR** to make it a searchable PDF (selectable text) "
    "or extract plain text.",
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
    """Persist a pending button record under ``<prefix>:<token>`` (best-effort).

    Shared by the Convert (``bookconvert``) and Compress-PDF (``bookcompress``)
    buttons.  ``chat_id`` is where the user tapped the button (the delivery
    target for the result).  ``source_chat_id``/``message_id`` describe where
    the file currently lives so the job can re-download it — for bot-sent
    results that is the same chat as the button (source_chat_id left None),
    for userbot-delivered (large) copies it is the DM/Saved Messages where the
    copy landed.  Records expire after ``BOOK_ASK_TTL_SECONDS``.
    """
    try:
        from utils.redis_client import get_sync_redis

        r = get_sync_redis()
        if r:
            try:
                import config as _cfg

                _ttl = getattr(_cfg, "BOOK_ASK_TTL_SECONDS", 600)
            except Exception:  # nosec B110
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
    except Exception:  # nosec B110
        pass





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
    """Post a bot-API Compress/Convert prompt for a userbot-delivered file.

    The bot cannot edit the userbot's delivered message, so it sends its own
    prompt message in the user's chat.  The pending record (keyed under
    ``<record_prefix>:<token>``) points at the delivered copy (``src_chat_id``
    + ``src_message_id``) so the corresponding job can re-download it via the
    userbot chat-based pipe.  Best-effort: any failure leaves the file
    delivered without the button.
    """
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
    except Exception:  # nosec B110 - best-effort prompt
        pass


def _tg_delete_message(chat_id: int, message_id: int | None) -> bool:
    """Delete a message via the Bot API ``deleteMessage`` endpoint (best-effort)."""
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
    """Resolve a Bot API ``file_id`` to its ``file_path`` via ``getFile``.

    Retries transient issues (5xx / 429) with backoff.  ``diagnostic`` is an
    optional callback invoked with an output-meta dict when getFile fails, so
    callers can record their own diagnostics without coupling this module to
    their storage layer.

    Returns:
        The resolved ``file_path``.  Raises ``requests.exceptions.HTTPError`` on
        non-200 responses and ``ValueError``-style exceptions on parse errors.
    """
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/bot{bot_token}/getFile"
    # Try a couple of times for transient issues (e.g., 5xx or rate limits)
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
            # Try to extract Telegram error description for more context
            try:
                body = r.json()
                desc = body.get("description") or body
            except Exception:
                desc = r.text
            msg = (
                f"Telegram getFile failed: status={r.status_code} desc={desc}"
            )
            logger.error(msg)
            # Record diagnostic info for this file_id (caller-side, best-effort)
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
                except Exception:  # nosec B110
                    pass
            # For server errors or rate limits, retry a couple times
            if r.status_code >= 500 or r.status_code == 429:
                if attempt < 2:
                    time.sleep(1 + attempt)
                    continue
            # Raise an HTTPError with details so callers can include it in their handling
            raise requests.exceptions.HTTPError(msg)

        try:
            data = r.json()
            return data["result"]["file_path"]
        except Exception as e:
            logger.exception("Failed parsing getFile JSON for %s", file_id)
            # On 400 errors like 'file is too big' record diagnostic info
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
                except Exception:  # nosec B110
                    pass
            raise

    # Unreachable in practice (every loop path returns or raises) but lets the
    # type checker prove the function always returns str.
    raise RuntimeError(f"Failed to resolve file_path for {file_id}")


def _tg_download_to_bytes(bot_token: str | None, tg_file_path: str) -> bytes:
    """Download a Bot API file into memory (``api.telegram.org/file/...``).

    Retries transient 429/5xx and connection errors with backoff; non-retryable
    HTTP errors (e.g. 400/404) raise immediately.

    Returns:
        The raw file bytes.
    """
    bot_token = _get_bot_token(bot_token)
    url = f"https://api.telegram.org/file/bot{bot_token}/{tg_file_path}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with _SESSION.get(url, stream=True, timeout=60) as r:
                if r.status_code >= 500 or r.status_code == 429:
                    # Server error or rate limit — retry with backoff
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
            # Transient network errors — retry with exponential backoff
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
            # Non-retryable HTTP errors (e.g., 400, 404) — raise immediately
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
    """Download a Bot API file to ``dest_path`` (streamed, never fully buffered).

    When ``total`` > 0, ``progress_callback(recv, total)`` is invoked per chunk
    so callers can live-edit a progress message.

    Returns:
        The number of bytes written.
    """
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
                        except Exception:  # nosec B110 - progress is best-effort
                            pass
    return seen


def _tg_forward_message(
    bot_token: str | None,
    chat_id: int,
    from_chat_id: int,
    message_id: int,
) -> int | None:
    """Forward a message via the Bot API ``forwardMessage`` endpoint.

    Returns:
        The forwarded message id on success, or None on failure (the failure
        detail is logged).
    """
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


# ── HTTP-based progress messages (no PTB needed in workers) ──────
# Mirror bot.py's send_progress_update but use raw HTTP calls so they work in
# background workers without a PTB bot instance.

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
    """Send or update a progress message with a visual Unicode progress bar.

    Args:
        chat_id: Telegram chat ID to send to.
        filename: Display name of the file being processed.
        stage: Key from _PROGRESS_STAGES dict (e.g. "downloading", "done").
        detail: Optional detail line (e.g. "40.2 MB downloaded").
        file_size: Total file size for display.
        message_id: If provided, *edit* the existing message instead of sending new.
        progress_pct: Optional live byte percentage (0-100) that overrides the
            stage's fixed percentage (used while downloading via userbot).
        reply_markup: Optional inline-keyboard payload to attach (e.g. a live
            cancel button on the handoff message). Pass ``{"inline_keyboard":
            []}`` to remove an existing keyboard.

    Returns:
        message_id of the sent/edited message, or None on failure.
    """
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
        # filename is user-controlled: entities ARE parsed inside **bold** in
        # legacy Markdown, so a raw ``_``/``*``/``[`` would render mangled or
        # crash the send with "Can't parse entities" -- escape it first.
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
                # Mirror the edit branch (and the web process's
                # send_progress_update) so the FIRST progress message renders
                # Markdown exactly like every subsequent live edit — otherwise
                # the **bold**/`code` markers show literally on the initial
                # post and only render once the first edit lands.
                parse_mode="Markdown",
            )
            if res and "result" in res and "message_id" in res["result"]:
                return res["result"]["message_id"]
            return None
    except Exception:
        return None
