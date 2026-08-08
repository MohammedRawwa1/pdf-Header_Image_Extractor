"""Shared Redis caches: processed-result records + PDF validator results.

Two caches live here so the web process (bot.py) and the worker (tasks.py)
read/write the SAME keys:

1. **Processed-result cache** (``processed:<content_sha256>``) — remembers that
   a file's Thumbnail/OCR/Compress operation already finished and stores the
   delivered copy's reusable Bot API ``file_id``.  Keyed by a SHA-256 of the
   file's BYTES (computed by the worker after download), so two DIFFERENT
   files never collide even when they share a name AND an exact byte size.
   A re-send of the same file can then be re-delivered straight from the
   cached copy — NO new job — even when the user deleted the bot's earlier
   messages (Telegram keeps the file behind the file_id).

2. **PDF validator cache** (``pdfcheck:<file_unique_id>``) — moved here from
   tasks.py so bot.py can reuse the embedded-thumbnail / text-layer results
   at ENQUEUE time ("already thumbed" / "already OCR'd" short-circuits).  It
   also carries the record's ``content_hash`` field: the worker binds a
   Telegram ``file_unique_id`` to the content hash it computed, so the web
   process can resolve ``processed:<hash>`` WITHOUT the bytes (it only has
   the fuid at enqueue time).

Both are best-effort: any Redis failure degrades to the pre-cache behaviour
(the job just runs normally, and the worker re-checks the hash itself).
"""

import hashlib
import json
import logging
import time

from utils.redis_client import get_sync_redis

logger = logging.getLogger(__name__)

# ── Content hashing ──────────────────────────────────────────────
# The dedup fingerprint is the file's bytes, not its name/size.  SHA-256 of
# 20MB streams in ~0.1s, far below the download time, so every job hashes
# its own download and never trusts another process's numbers.


def content_sha256(data: bytes) -> str:
    """SHA-256 hex digest of in-memory file bytes."""
    return hashlib.sha256(data).hexdigest()


def content_sha256_file(path: str, chunk_size: int = 1 << 20) -> str:
    """Streamed SHA-256 hex digest of a file on disk (memory-safe for large
    files).  Raises on IO errors — callers should treat a hash failure as
    \"dedup unavailable, run the job normally\"."""
    _h = hashlib.sha256()
    with open(path, "rb") as _fh:
        while True:
            _chunk = _fh.read(chunk_size)
            if not _chunk:
                break
            _h.update(_chunk)
    return _h.hexdigest()


# ── PDF validator-result cache ─────────────────────────────────────
# Keyed by Telegram's ``file_unique_id`` (an immutable property of the
# file's content), NOT the RQ job id, so the two jobs enqueued by the
# OCR & Thumbnail button can share results even though each writes its own
# ``io:in:<job_id>`` key.  Safe to reuse because the embedded-thumbnail /
# text-layer status of a given file never changes.  Stored as a Redis HASH
# (``pdfcheck:<file_unique_id>`` with ``has_thumb`` / ``has_text_layer`` /
# ``content_hash`` fields) so each job's write is atomic per field — no
# lost-update races between the thumbnail and OCR jobs.
#
# ``content_hash`` is the worker's binding of this fuid to the content hash
# it computed — the ONLY way the web process can resolve a processed record
# at enqueue time without downloading the file.  The binding is ALSO written
# to the durable ``pfuid:<fuid>`` index (same 30-day TTL as the processed
# record), so the enqueue-time fast path covers the record's FULL lifetime;
# the pdfcheck copy (7-day) is kept for legacy reads.  After 30 days, a
# re-send queues a job and the worker's own content-hash dedup re-sends the
# cached copy — correct, just with a download.
PDF_CHECK_PREFIX = "pdfcheck:"
PDF_CHECK_TTL = 7 * 24 * 3600  # 7 days - bounds storage; content is immutable


def _get_pdf_checks(file_unique_id: str | None) -> dict | None:
    """Read cached validator results for a Telegram file (by file_unique_id).

    Returns ``{"has_thumb": bool|None, "has_text_layer": bool|None,
    "content_hash": str|None}`` or None when nothing is cached yet.  A field
    is None when it has never been computed (caller should run the check);
    True/False are real cached results — distinguishing "absent" from a
    cached False is what lets the OCR job backfill missing fields without
    re-running on legit False values.
    """
    if not file_unique_id:
        return None
    try:
        r = get_sync_redis()
        if not r:
            return None
        _raw = r.hgetall(f"{PDF_CHECK_PREFIX}{file_unique_id}")
        if not _raw:
            return None

        def _b(v: object) -> bool | None:
            if v is None:
                return None
            return str(v) in ("1", "true", "True")

        _ch = _raw.get("content_hash")
        return {
            "has_thumb": _b(_raw.get("has_thumb")),
            "has_text_layer": _b(_raw.get("has_text_layer")),
            "content_hash": str(_ch) if _ch else None,
        }
    except Exception:
        logger.debug("Failed to read pdf check cache for %s", file_unique_id)
        return None


def _store_pdf_checks(
    file_unique_id: str | None,
    has_thumb: bool | None = None,
    has_text_layer: bool | None = None,
    content_hash: str | None = None,
) -> None:
    """Cache validator results (best-effort) under ``pdfcheck:<file_unique_id>``.

    Writes only the fields explicitly given (hash fields are atomic per
    field), so the thumbnail job (has_thumb), the OCR job (has_text_layer)
    and any job (content_hash binding) can publish independently without
    clobbering each other.
    """
    if not file_unique_id:
        return
    try:
        r = get_sync_redis()
        if not r:
            return
        _map = {}
        if has_thumb is not None:
            _map["has_thumb"] = "1" if has_thumb else "0"
        if has_text_layer is not None:
            _map["has_text_layer"] = "1" if has_text_layer else "0"
        if content_hash:
            _map["content_hash"] = content_hash
        if _map:
            r.hset(f"{PDF_CHECK_PREFIX}{file_unique_id}", mapping=_map)
            r.expire(f"{PDF_CHECK_PREFIX}{file_unique_id}", PDF_CHECK_TTL)
    except Exception:
        logger.debug("Failed to cache pdf checks for %s", file_unique_id)


# ── Processed-result cache ─────────────────────────────────────────
# Keyed by the content hash (``processed:<sha256(bytes)>``).  A re-upload of
# the same file — even one Telegram gives a fresh ``file_unique_id`` — hashes
# to the SAME key, so same-content re-sends dedupe regardless of name, size
# or fuid.  Two DIFFERENT files can never collide (SHA-256 of different
# bytes).  The record stores the original name/size as metadata (for the
# re-send caption), and a stale cached file_id simply falls back to a fresh
# job when Telegram rejects it.
PROCESSED_PREFIX = "processed:"
PROCESSED_TTL = 30 * 24 * 3600  # 30 days - bounds storage; Telegram may drop
# stale file_ids earlier, and resend then falls back to a fresh job.

# fuid -> content_hash durable index: ``pfuid:<file_unique_id>`` carries the
# SAME 30-day TTL as the processed record, so the WEB process can resolve
# ``processed:<hash>`` from a Telegram file_unique_id for the record's FULL
# lifetime.  The binding used to live only inside the 7-day pdfcheck record;
# after day 7 a re-send would enqueue a job and the worker's hash dedup would
# re-send the cached copy only AFTER downloading the file.  This index closes
# that gap: re-sends within 30 days are caught at the surface with zero
# download.
PFUID_PREFIX = "pfuid:"


def _store_fuid_binding(file_unique_id: str | None, content_hash: str | None) -> None:
    """Best-effort: persist ``file_unique_id -> content_hash`` (30-day TTL).

    Written by the worker whenever it computes a content hash (alongside the
    pdfcheck binding), so the web process can short-circuit re-sends at the
    surface WITHOUT downloading.  The 30-day TTL matches the processed
    record's, keeping the fast path alive for the record's full lifetime.
    """
    if not file_unique_id or not content_hash:
        return
    try:
        r = get_sync_redis()
        if not r:
            return
        r.setex(f"{PFUID_PREFIX}{file_unique_id}", PROCESSED_TTL, content_hash)
    except Exception:
        logger.debug("Failed to bind fuid %s", file_unique_id)


def _get_fuid_binding(file_unique_id: str | None) -> str | None:
    """Read the durable ``file_unique_id -> content_hash`` index, or None."""
    if not file_unique_id:
        return None
    try:
        r = get_sync_redis()
        if not r:
            return None
        _v = r.get(f"{PFUID_PREFIX}{file_unique_id}")
        return str(_v) if _v else None
    except Exception:
        logger.debug("Failed to read fuid binding for %s", file_unique_id)
        return None

# Per-operation record fields:
#   ops: { "thumb": {"status": "done"|"skipped", "file_id": ..., "thumb_file_id": ...},
#          "ocr": {...}, "compress": {...},
#          "convert:pdf": {...}, "convert:txt": {...} }
# "done"    = the result was delivered (file_id = reusable Bot API copy).
# "skipped" = the op short-circuited (already thumbed/OCR'd/compressed); no
#             copy exists, re-sends get the "already ..." reply instead.
#
# Book CONVERSION is per-TARGET (``ops:convert:pdf``, ``ops:convert:txt`` ...)
# because the deliverable differs by format — converting the same book to a
# second format must NOT evict the first format's cached copy.  The legacy
# single ``ops:convert`` field is still read (target matched) for records
# written before per-target fields existed.


def processed_key(content_hash: str | None) -> str | None:
    """Redis key for a file's content hash (sha256 hex of its bytes)."""
    if not content_hash:
        return None
    return f"{PROCESSED_PREFIX}{content_hash}"


def processed_op_field(op: str, target: str | None = None) -> str:
    """Redis hash-field suffix for an op, per-target when ``target`` is set.

    Most ops (thumb/ocr/compress) store one entry per content hash
    (``ops:thumb`` etc).  Book CONVERSION is per-target: the deliverable
    differs by format, so each target keeps its OWN field
    (``ops:convert:pdf``, ``ops:convert:txt`` ...) — converting the same book
    to a second format NEVER evicts the first format's cached copy.
    """
    return f"{op}:{target}" if target else op


def get_processed_op(
    rec: dict | None, op: str, target: str | None = None
) -> dict | None:
    """Look up one op's entry in a processed record (per-target aware).

    Returns the entry dict for ``op`` (``ops:<op>``) or, for target-bearing
    ops, the per-target entry (``ops:convert:pdf`` etc).  Falls back to the
    legacy single-entry field (``ops:convert``) when the per-target field is
    absent AND the legacy entry's stored ``target`` matches — so records
    written before per-target convert fields keep working.
    """
    if not rec:
        return None
    ops = rec.get("ops") or {}
    _entry = ops.get(processed_op_field(op, target))
    if _entry is not None:
        return _entry
    if target:
        # Legacy: pre-per-target convert records live under the plain field.
        _legacy = ops.get(op)
        if _legacy is not None and (_legacy.get("target") or "") == target:
            return _legacy
    return None


def get_processed_record(content_hash: str | None) -> dict | None:
    """Read the processed-result record for a content hash, or None.

    Returns ``{"filename": ..., "size": ..., "user_id": ..., "chat_id": ...,
    "at": ..., "ops": {"thumb": {...}, "ocr": {...}, "compress": {...}}}``
    where each op entry is ``{"status": "done"|"skipped", "file_id": ...,
    "thumb_file_id": ..., "target": ...}``.
    """
    key = processed_key(content_hash)
    if not key:
        return None
    try:
        r = get_sync_redis()
        if not r:
            return None
        raw = r.hgetall(key)
        if not raw:
            return None
        rec: dict = {}
        ops: dict = {}
        for _k, _v in raw.items():
            if _k == "meta":
                try:
                    _m = json.loads(_v)
                    if isinstance(_m, dict):
                        rec.update(_m)
                except Exception:  # nosec B110 - corrupt meta = ignore
                    pass
            elif _k.startswith("ops:"):
                try:
                    _e = json.loads(_v)
                    if isinstance(_e, dict):
                        ops[_k[4:]] = _e
                except Exception:  # nosec B110 - corrupt op = ignore
                    pass
        rec["ops"] = ops
        return rec
    except Exception:
        logger.debug("Failed to read processed record for %s", key)
        return None


def get_processed_by_file_unique_id(file_unique_id: str | None) -> dict | None:
    """Resolve a file's processed record via its Telegram ``file_unique_id``.

    The worker binds ``file_unique_id -> content_hash`` — BOTH in the durable
    ``pfuid:<fuid>`` index (30-day TTL, the primary path) and inside the
    pdfcheck record (legacy/7-day) — letting the WEB process (which only has
    the fuid, never the bytes) look up ``processed:<hash>`` at enqueue time
    WITHOUT downloading.  The durable index covers the record's full 30-day
    lifetime, so re-sends are caught at the surface (zero download) even
    after the 7-day pdfcheck binding expires.  Returns None when the binding
    is unknown — callers then fall back to enqueueing, and the worker's own
    content-hash dedup catches the re-send after download.
    """
    if not file_unique_id:
        return None
    # Primary: durable 30-day index (survives the 7-day pdfcheck TTL).
    _content_hash = _get_fuid_binding(file_unique_id)
    if _content_hash:
        _rec = get_processed_record(_content_hash)
        if _rec:
            return _rec
    # Legacy fallback: content_hash stored inside the pdfcheck record.
    _checks = _get_pdf_checks(file_unique_id)
    if not _checks:
        return None
    return get_processed_record(_checks.get("content_hash"))


def upsert_processed_record(
    content_hash: str | None,
    op: str,
    status: str,
    *,
    filename: str | None = None,
    file_size: int | None = None,
    file_id: str | None = None,
    thumb_file_id: str | None = None,
    delivery: str | None = None,
    src_chat_id: int | str | None = None,
    src_message_id: int | None = None,
    user_id: int | None = None,
    chat_id: int | None = None,
    target: str | None = None,
) -> None:
    """Record that ``op`` (thumb/ocr/compress) finished for a content hash.

    Each op is stored as its OWN hash field (``ops:<op>``) so the concurrent
    Thumbnail / OCR / Compress jobs can never clobber each other's entries —
    the same per-field-atomic pattern as ``pdfcheck``.  Book CONVERSION is
    stored per-TARGET (``ops:convert:pdf`` / ``ops:convert:txt`` ...) so
    converting the same book to another format never evicts the previous
    format's cached copy (``target`` is also kept inside the entry).
    ``status`` is ``"done"`` or ``"skipped"`` (the op short-circuited —
    already thumbed/OCR'd/compressed).  For ``done`` copies:

    * Bot API delivery stores ``file_id``/``thumb_file_id`` (reusable via
      sendDocument).
    * USERBOT delivery (big files) has NO Bot API file_id — it stores
      ``delivery="userbot"`` plus ``src_chat_id``/``src_message_id`` (the
      delivered copy's location as the userbot sees it), which re-sends
      FORWARD via the userbot (server-side media copy).

    ``filename``/``file_size`` are metadata for the re-send caption (the KEY
    is the content hash).  Best-effort.
    """
    key = processed_key(content_hash)
    if not key:
        return
    try:
        r = get_sync_redis()
        if not r:
            return
        entry = {"status": status}
        if file_id:
            entry["file_id"] = file_id
        if thumb_file_id:
            entry["thumb_file_id"] = thumb_file_id
        if delivery:
            entry["delivery"] = delivery
        if src_chat_id:
            entry["src_chat_id"] = str(src_chat_id)
        if src_message_id:
            entry["src_message_id"] = int(src_message_id)
        if target:
            entry["target"] = target
        meta = {
            "filename": filename,
            "size": int(file_size or 0),
            "at": int(time.time()),
        }
        if user_id is not None:
            meta["user_id"] = user_id
        if chat_id is not None:
            meta["chat_id"] = chat_id
        r.hset(
            key,
            mapping={
                f"ops:{processed_op_field(op, target)}": json.dumps(entry),
                "meta": json.dumps(meta),
            },
        )
        r.expire(key, PROCESSED_TTL)
    except Exception:
        logger.debug("Failed to cache processed result for %s", key)
