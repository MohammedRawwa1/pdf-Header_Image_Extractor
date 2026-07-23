"""Redis-backed caching layer for metadata, file info, and bot responses.

Provides a simple async cache with TTL support for:
- Job metadata and status
- File info (size, type, hash)
- Bot response caching
- User session data
- Media analysis results

Usage:
    from utils.cache import get_cache

    cache = await get_cache()
    await cache.set("job:abc123", {"status": "processing"}, ttl=3600)
    data = await cache.get("job:abc123")
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger(__name__)

# Default TTLs (seconds)
DEFAULT_TTL = 3600          # 1 hour
SHORT_TTL = 300             # 5 minutes
MEDIUM_TTL = 1800           # 30 minutes
LONG_TTL = 86400            # 24 hours

# Key prefixes
PREFIX_JOB = "cache:job:"
PREFIX_FILE = "cache:file:"
PREFIX_USER = "cache:user:"
PREFIX_META = "cache:meta:"
PREFIX_RESPONSE = "cache:resp:"


class RedisCache:
    """Async Redis-backed cache with TTL support."""

    def __init__(self, redis_url: str | None = None):
        self._redis_url = redis_url or os.getenv("REDIS_URL") or ""
        self._client: aioredis.Redis | None = None

    async def _get_client(self) -> aioredis.Redis | None:
        """Lazy-initialize and return the Redis client."""
        if self._client is not None:
            return self._client
        if not self._redis_url or aioredis is None:
            return None
        try:
            self._client = aioredis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=20,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            # Verify connection
            await self._client.ping()
            logger.info("Redis cache connected successfully")
            return self._client
        except Exception as e:
            logger.warning("Redis cache connection failed: %s", e)
            self._client = None
            return None

    async def get(self, key: str) -> Any | None:
        """Get a cached value by key. Returns None on miss or error."""
        client = await self._get_client()
        if client is None:
            return None
        try:
            raw = await client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.debug("Cache GET failed for %s: %s", key, e)
            return None

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> bool:
        """Set a cached value with TTL. Returns True on success."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            serialized = json.dumps(value, default=str)
            await client.setex(key, ttl, serialized)
            return True
        except Exception as e:
            logger.debug("Cache SET failed for %s: %s", key, e)
            return False

    async def delete(self, key: str) -> bool:
        """Delete a cached value. Returns True on success."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            await client.delete(key)
            return True
        except Exception as e:
            logger.debug("Cache DELETE failed for %s: %s", key, e)
            return False

    async def exists(self, key: str) -> bool:
        """Check if a key exists in cache."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            return bool(await client.exists(key))
        except Exception:
            return False

    async def incr(self, key: str, amount: int = 1, ttl: int = DEFAULT_TTL) -> int | None:
        """Increment a counter. Returns new value or None on error."""
        client = await self._get_client()
        if client is None:
            return None
        try:
            val = await client.incrby(key, amount)
            # Set TTL only if key is new (avoid resetting on every incr)
            if val == amount:
                await client.expire(key, ttl)
            return val
        except Exception as e:
            logger.debug("Cache INCR failed for %s: %s", key, e)
            return None

    async def get_many(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple values at once. Returns dict of key->value for non-None results."""
        client = await self._get_client()
        if client is None:
            return {}
        try:
            values = await client.mget(keys)
            result = {}
            for key, raw in zip(keys, values):
                if raw is not None:
                    try:
                        result[key] = json.loads(raw)
                    except Exception:
                        pass
            return result
        except Exception as e:
            logger.debug("Cache MGET failed: %s", e)
            return {}

    async def set_many(self, mapping: dict[str, Any], ttl: int = DEFAULT_TTL) -> bool:
        """Set multiple values at once with TTL."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            pipe = client.pipeline()
            for key, value in mapping.items():
                serialized = json.dumps(value, default=str)
                pipe.setex(key, ttl, serialized)
            await pipe.execute()
            return True
        except Exception as e:
            logger.debug("Cache MSET failed: %s", e)
            return False

    async def get_or_set(self, key: str, compute_fn, ttl: int = DEFAULT_TTL):
        """Get from cache or compute, store, and return. Single entry point for read-through caching."""
        cached = await self.get(key)
        if cached is not None:
            return cached
        result = await compute_fn()
        if result is not None:
            await self.set(key, result, ttl=ttl)
        return result

    # \u2500\u2500 Binary-safe methods (store raw bytes, e.g. for large file caching) \u2500\u2500

    async def _get_binary_client(self) -> aioredis.Redis | None:
        """Lazy-init a dedicated Redis connection with ``decode_responses=False``.

        The main client uses ``decode_responses=True`` for JSON convenience, but
        that breaks retrieval of arbitrary binary data.  This separate connection
        is used exclusively for ``set_binary`` / ``get_binary``.
        """
        if getattr(self, "_binary_client", None) is not None:
            return self._binary_client
        if not self._redis_url or aioredis is None:
            return None
        try:
            bc = aioredis.from_url(
                self._redis_url,
                decode_responses=False,  # binary-safe
                max_connections=5,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await bc.ping()
            self._binary_client = bc
            return bc
        except Exception as e:
            logger.warning("Binary Redis client connection failed: %s", e)
            self._binary_client = None
            return None

    async def set_binary(self, key: str, data: bytes, ttl: int = DEFAULT_TTL) -> bool:
        """Store raw bytes in Redis (binary-safe).

        Uses the dedicated ``_binary_client`` with ``decode_responses=False``
        so the bytes are stored and retrieved exactly as-is.
        """
        client = await self._get_binary_client()
        if client is None:
            logger.debug("Binary client unavailable, falling back to main client for set_binary")
            main = await self._get_client()
            if main is None:
                return False
            try:
                await main.setex(key, ttl, data)
                return True
            except Exception as e:
                logger.debug("Cache SET_BINARY (fallback) failed for %s: %s", key, e)
                return False
        try:
            await client.setex(key, ttl, data)
            return True
        except Exception as e:
            logger.debug("Cache SET_BINARY failed for %s: %s", key, e)
            return False

    async def get_binary(self, key: str) -> bytes | None:
        """Retrieve raw bytes from Redis (binary-safe)."""
        client = await self._get_binary_client()
        if client is None:
            logger.debug("Binary client unavailable, falling back to main client for get_binary")
            main = await self._get_client()
            if main is None:
                return None
            try:
                raw = await main.get(key)
                if raw is None:
                    return None
                if isinstance(raw, str):
                    return raw.encode("latin-1")
                return raw
            except (UnicodeDecodeError, Exception) as e:
                logger.debug("Cache GET_BINARY (fallback) failed for %s: %s", key, e)
                return None
        try:
            raw = await client.get(key)
            if raw is None:
                return None
            return raw
        except Exception as e:
            logger.debug("Cache GET_BINARY failed for %s: %s", key, e)
            return None

    async def cache_file_bytes(self, file_key: str, data: bytes, ttl: int = LONG_TTL) -> bool:
        """Cache raw file bytes by a unique file key (e.g. file_unique_id).

        This allows re-using previously downloaded file bytes without hitting
        Telegram's API again.  Use ``BIGFILE_CACHE_TTL`` env var to override
        the default 24-hour TTL.
        """
        try:
            ttl = int(os.getenv("BIGFILE_CACHE_TTL", str(ttl)))
        except Exception:
            pass
        return await self.set_binary(f"{PREFIX_FILE}bytes:{file_key}", data, ttl=ttl)

    async def get_cached_file_bytes(self, file_key: str) -> bytes | None:
        """Retrieve previously cached file bytes by file key."""
        return await self.get_binary(f"{PREFIX_FILE}bytes:{file_key}")

    async def close(self):
        """Close the Redis connections (main + binary)."""
        # Close the main (decode_responses=True) client
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                try:
                    await self._client.close()
                except Exception:
                    pass
            self._client = None
        # Close the binary (decode_responses=False) client if present
        bc = getattr(self, "_binary_client", None)
        if bc is not None:
            try:
                await bc.aclose()
            except Exception:
                try:
                    await bc.close()
                except Exception:
                    pass
            self._binary_client = None

    # \u2500\u2500 Convenience methods for common patterns \u2500\u2500

    async def cache_job_metadata(self, job_id: str, metadata: dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        """Cache job metadata (status, progress, etc.)."""
        return await self.set(f"{PREFIX_JOB}{job_id}", metadata, ttl=ttl)

    async def get_job_metadata(self, job_id: str) -> dict[str, Any] | None:
        """Get cached job metadata."""
        return await self.get(f"{PREFIX_JOB}{job_id}")

    async def update_job_metadata(self, job_id: str, fields: dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        """Update specific fields in cached job metadata (read-modify-write)."""
        existing = await self.get_job_metadata(job_id) or {}
        existing.update(fields)
        return await self.cache_job_metadata(job_id, existing, ttl=ttl)

    async def cache_file_info(self, file_key: str, info: dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache file metadata (size, type, hash, ffprobe output)."""
        return await self.set(f"{PREFIX_FILE}{file_key}", info, ttl=ttl)

    async def get_file_info(self, file_key: str) -> dict[str, Any] | None:
        """Get cached file metadata."""
        return await self.get(f"{PREFIX_FILE}{file_key}")

    async def cache_user_session(self, user_id: str, session_data: dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache user session/preferences."""
        return await self.set(f"{PREFIX_USER}{user_id}", session_data, ttl=ttl)

    async def get_user_session(self, user_id: str) -> dict[str, Any] | None:
        """Get cached user session."""
        return await self.get(f"{PREFIX_USER}{user_id}")

    async def cache_response(self, key: str, response: Any, ttl: int = SHORT_TTL) -> bool:
        """Cache a bot response for deduplication."""
        return await self.set(f"{PREFIX_RESPONSE}{key}", response, ttl=ttl)

    async def get_cached_response(self, key: str) -> Any | None:
        """Get a cached bot response."""
        return await self.get(f"{PREFIX_RESPONSE}{key}")

    async def cache_media_analysis(self, file_hash: str, analysis: dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache ffprobe/media analysis results."""
        return await self.set(f"{PREFIX_META}analysis:{file_hash}", analysis, ttl=ttl)

    async def get_media_analysis(self, file_hash: str) -> dict[str, Any] | None:
        """Get cached media analysis."""
        return await self.get(f"{PREFIX_META}analysis:{file_hash}")


# \u2500\u2500 Singleton access \u2500\u2500

_cache_singleton: RedisCache | None = None


async def get_cache() -> RedisCache:
    """Return the shared Redis cache instance."""
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = RedisCache()
    return _cache_singleton


async def close_cache():
    """Close the shared Redis cache."""
    global _cache_singleton
    if _cache_singleton is not None:
        await _cache_singleton.close()
        _cache_singleton = None
