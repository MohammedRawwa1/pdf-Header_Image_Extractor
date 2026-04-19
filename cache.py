import time
from typing import Optional, Tuple
import config


def _get_redis():
    if not config.REDIS_URL:
        return None
    try:
        import redis

        # Use binary-safe client for bytes storage
        return redis.from_url(config.REDIS_URL, decode_responses=False)
    except Exception:
        return None


def _thumb_key(unique_id: str) -> str:
    return f"thumb:{unique_id}"


def set_thumbnail(unique_id: str, file_id: str, thumb_bytes: bytes, ttl: int = 60 * 60 * 24 * 30) -> bool:
    """Store thumbnail bytes and metadata for a given file unique id.

    Stores metadata in a hash and raw bytes in a separate binary key so reads are safe.
    """
    r = _get_redis()
    if not r:
        return False
    try:
        meta_key = _thumb_key(unique_id)
        blob_key = meta_key + ":b"
        # store metadata (file_id, created_at)
        r.hset(meta_key, mapping={"file_id": file_id, "created_at": str(int(time.time()))})
        # store raw bytes
        r.set(blob_key, thumb_bytes, ex=ttl)
        return True
    except Exception:
        return False


def get_thumbnail(unique_id: str) -> Optional[Tuple[str, bytes]]:
    """Return (file_id, thumb_bytes) or None if not found."""
    r = _get_redis()
    if not r:
        return None
    try:
        meta_key = _thumb_key(unique_id)
        blob_key = meta_key + ":b"
        file_id = r.hget(meta_key, "file_id")
        thumb = r.get(blob_key)
        if not thumb:
            return None
        if isinstance(file_id, (bytes, bytearray)):
            file_id = file_id.decode('utf-8', errors='ignore')
        return (file_id, thumb)
    except Exception:
        return None
