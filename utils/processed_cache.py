import hashlib
import json
import logging
import time

from utils.redis_client import get_sync_redis

logger = logging.getLogger(__name__)

PDF_CHECK_PREFIX = "pdfcheck:"
PDF_CHECK_TTL = 7 * 24 * 3600
PROCESSED_PREFIX = "processed:"
PROCESSED_TTL = 30 * 24 * 3600
PFUID_PREFIX = "pfuid:"


def content_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_sha256_file(path: str, chunk_size: int = 1 << 20) -> str:
    _h = hashlib.sha256()
    with open(path, "rb") as _fh:
        while True:
            _chunk = _fh.read(chunk_size)
            if not _chunk:
                break
            _h.update(_chunk)
    return _h.hexdigest()


def _get_pdf_checks(file_unique_id: str | None) -> dict | None:
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
        return {"has_thumb": _b(_raw.get("has_thumb")), "has_text_layer": _b(_raw.get("has_text_layer")), "content_hash": str(_ch) if _ch else None}
    except Exception:
        logger.debug("Failed to read pdf check cache for %s", file_unique_id)
        return None


def _store_pdf_checks(file_unique_id: str | None, has_thumb: bool | None = None, has_text_layer: bool | None = None, content_hash: str | None = None) -> None:
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


def _store_fuid_binding(file_unique_id: str | None, content_hash: str | None) -> None:
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


def processed_key(content_hash: str | None) -> str | None:
    if not content_hash:
        return None
    return f"{PROCESSED_PREFIX}{content_hash}"


def processed_op_field(op: str, target: str | None = None) -> str:
    return f"{op}:{target}" if target else op


def get_processed_op(rec: dict | None, op: str, target: str | None = None) -> dict | None:
    if not rec:
        return None
    ops = rec.get("ops") or {}
    _entry = ops.get(processed_op_field(op, target))
    if _entry is not None:
        return _entry
    if target:
        _legacy = ops.get(op)
        if _legacy is not None and (_legacy.get("target") or "") == target:
            return _legacy
    return None


def get_processed_record(content_hash: str | None) -> dict | None:
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
                except Exception:
                    pass
            elif _k.startswith("ops:"):
                try:
                    _e = json.loads(_v)
                    if isinstance(_e, dict):
                        ops[_k[4:]] = _e
                except Exception:
                    pass
        rec["ops"] = ops
        return rec
    except Exception:
        logger.debug("Failed to read processed record for %s", key)
        return None


def get_processed_by_file_unique_id(file_unique_id: str | None) -> dict | None:
    if not file_unique_id:
        return None
    _content_hash = _get_fuid_binding(file_unique_id)
    if _content_hash:
        _rec = get_processed_record(_content_hash)
        if _rec:
            return _rec
    _checks = _get_pdf_checks(file_unique_id)
    if not _checks:
        return None
    return get_processed_record(_checks.get("content_hash"))


def upsert_processed_record(
    content_hash: str | None, op: str, status: str, *,
    filename: str | None = None, file_size: int | None = None, file_id: str | None = None,
    thumb_file_id: str | None = None, delivery: str | None = None,
    src_chat_id: int | str | None = None, src_message_id: int | None = None,
    user_id: int | None = None, chat_id: int | None = None, target: str | None = None,
) -> None:
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
        meta = {"filename": filename, "size": int(file_size or 0), "at": int(time.time())}
        if user_id is not None:
            meta["user_id"] = user_id
        if chat_id is not None:
            meta["chat_id"] = chat_id
        r.hset(key, mapping={f"ops:{processed_op_field(op, target)}": json.dumps(entry), "meta": json.dumps(meta)})
        r.expire(key, PROCESSED_TTL)
    except Exception:
        logger.debug("Failed to cache processed result for %s", key)
