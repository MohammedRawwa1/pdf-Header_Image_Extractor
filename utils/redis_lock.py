"""Redis-based distributed lock for coordinating across multiple processes.

Used to prevent the 409 getUpdates conflict when multiple uvicorn workers
are running. Only one worker should hold the long-poller lock at a time.

Usage:
    from utils.redis_lock import RedisLock

    lock = RedisLock("longpoller", ttl=30)
    if await lock.acquire():
        try:
            # ... run long-poller ...
        finally:
            await lock.release()
"""

import logging
import os
import time
from typing import Optional

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger(__name__)


class RedisLock:
    """A simple Redis-backed distributed lock with auto-renewal support."""

    def __init__(
        self,
        name: str,
        ttl: int = 30,
        redis_url: Optional[str] = None,
        owner: Optional[str] = None,
    ):
        """
        Args:
            name: Lock name (used as Redis key prefix).
            ttl: Lock expiration in seconds (prevents stale locks).
            redis_url: Redis connection URL. Falls back to REDIS_URL env var.
            owner: Unique owner identifier. Defaults to PID-based string.
        """
        self._name = f"lock:{name}"
        self._ttl = ttl
        self._redis_url = redis_url or os.getenv("REDIS_URL") or ""
        self._owner = owner or f"pid:{os.getpid()}:{id(self)}"
        self._client: Optional[aioredis.Redis] = None
        self._last_connect_attempt: float = 0
        self._acquired = False

    async def _get_client(self) -> Optional[aioredis.Redis]:
        if self._client is not None:
            return self._client
        import time
        now = time.time()
        if now - self._last_connect_attempt < 10:
            return None  # cooldown to avoid hammering Redis
        self._last_connect_attempt = now
        if not self._redis_url or aioredis is None:
            return None
        try:
            self._client = aioredis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            return self._client
        except Exception as e:
            logger.debug("RedisLock: failed to create client: %s", e)
            self._client = None
            return None

    async def acquire(self) -> bool:
        """Try to acquire the lock. Returns True if acquired."""
        client = await self._get_client()
        if client is None:
            # No Redis available \u2014 allow operation (degraded mode)
            logger.debug("RedisLock(%s): no Redis, allowing in degraded mode", self._name)
            self._acquired = True
            return True
        try:
            acquired = await client.set(
                self._name,
                self._owner,
                nx=True,
                ex=self._ttl,
            )
            if acquired:
                self._acquired = True
                logger.debug("RedisLock(%s): acquired by %s", self._name, self._owner)
            else:
                logger.debug("RedisLock(%s): held by another owner", self._name)
            return bool(acquired)
        except Exception as e:
            logger.warning("RedisLock(%s): acquire failed: %s", self._name, e)
            # Degrade to allowed on Redis errors
            self._acquired = True
            return True

    async def release(self) -> bool:
        """Release the lock (only if we own it)."""
        if not self._acquired:
            return True
        client = await self._get_client()
        if client is None:
            self._acquired = False
            return True
        try:
            # Atomic release: only delete if we own it
            script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            await client.eval(script, 1, self._name, self._owner)
            self._acquired = False
            logger.debug("RedisLock(%s): released by %s", self._name, self._owner)
            return True
        except Exception as e:
            logger.warning("RedisLock(%s): release failed: %s", self._name, e)
            self._acquired = False
            return True

    async def renew(self) -> bool:
        """Extend the lock TTL (heartbeat). Returns True on success."""
        if not self._acquired:
            return False
        client = await self._get_client()
        if client is None:
            return True
        try:
            # Atomic renew: only expire if we own it
            script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('expire', KEYS[1], ARGV[2])
            else
                return 0
            end
            """
            result = await client.eval(script, 1, self._name, self._owner, self._ttl)
            return bool(result)
        except Exception as e:
            logger.debug("RedisLock(%s): renew failed: %s", self._name, e)
            return False

    @property
    def is_acquired(self) -> bool:
        return self._acquired

    async def close(self):
        """Close the underlying Redis connection."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                try:
                    await self._client.close()
                except Exception:
                    pass
            self._client = None
