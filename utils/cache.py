"""Redis-backed caching layer for metadata, file info, and bot responses.

Provides a simple async cache with TTL support for:
- Job metadata and status
- File info (size, type, hash)
- User session data
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger(__name__)

DEFAULT_TTL = 3600
SHORT_TTL = 300
MEDIUM_TTL = 1800
LONG_TTL = 86400

PREFIX_JOB = "cache:job:"
PREFIX_FILE = "cache:file:"
PREFIX_USER = "cache:user:"
PREFIX_META = "cache:meta:"


class RedisCache:
    """Async Redis-backed cache with TTL support."""

    def __init__(self, redis_url: Optional[str] = None):
        self._redis_url = redis_url or os.getenv("REDIS_URL") or ""
        self._client: Optional[aioredis.Redis] = None

    async def _get_client(self) -> Optional[aioredis.Redis]:
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
            await self._client.ping()
            logger.info("Redis cache connected successfully")
            return self._client
        except Exception as e:
            logger.warning("Redis cache connection failed: %s", e)
            self._client = None
            return None

    async def get(self, key: str) -> Optional[Any]:
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
        client = await self._get_client()
        if client is None:
            return False
        try:
            return bool(await client.exists(key))
        except Exception:
            return False

    async def incr(self, key: str, amount: int = 1, ttl: int = DEFAULT_TTL) -> Optional[int]:
        client = await self._get_client()
        if client is None:
            return None
        try:
            val = await client.incrby(key, amount)
            if val == amount:
                await client.expire(key, ttl)
            return val
        except Exception as e:
            logger.debug("Cache INCR failed for %s: %s", key, e)
            return None

    async def get_many(self, keys: list[str]) -> Dict[str, Any]:
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

    async def set_many(self, mapping: Dict[str, Any], ttl: int = DEFAULT_TTL) -> bool:
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

    async def cache_job_metadata(self, job_id: str, metadata: Dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        return await self.set(f"{PREFIX_JOB}{job_id}", metadata, ttl=ttl)

    async def get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        return await self.get(f"{PREFIX_JOB}{job_id}")

    async def update_job_metadata(self, job_id: str, fields: Dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        existing = await self.get_job_metadata(job_id) or {}
        existing.update(fields)
        return await self.cache_job_metadata(job_id, existing, ttl=ttl)

    async def cache_file_info(self, file_key: str, info: Dict[str, Any], ttl: int = LONG_TTL) -> bool:
        return await self.set(f"{PREFIX_FILE}{file_key}", info, ttl=ttl)

    async def get_file_info(self, file_key: str) -> Optional[Dict[str, Any]]:
        return await self.get(f"{PREFIX_FILE}{file_key}")

    async def cache_user_session(self, user_id: str, session_data: Dict[str, Any], ttl: int = LONG_TTL) -> bool:
        return await self.set(f"{PREFIX_USER}{user_id}", session_data, ttl=ttl)

    async def get_user_session(self, user_id: str) -> Optional[Dict[str, Any]]:
        return await self.get(f"{PREFIX_USER}{user_id}")

    async def close(self):
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                try:
                    await self._client.close()
                except Exception:
                    pass
            self._client = None


_cache_singleton: Optional[RedisCache] = None


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
