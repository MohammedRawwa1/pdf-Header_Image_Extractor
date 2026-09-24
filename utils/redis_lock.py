import logging
import os
import time

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger(__name__)


class RedisLock:
    def __init__(
        self,
        name: str,
        ttl: int = 30,
        redis_url: str | None = None,
        owner: str | None = None,
    ):
        self._name = f"lock:{name}"
        self._ttl = ttl
        self._redis_url = redis_url or os.getenv("REDIS_URL") or ""
        self._owner = owner or f"pid:{os.getpid()}:{id(self)}"
        self._client: aioredis.Redis | None = None
        self._last_connect_attempt: float = 0
        self._acquired = False

    async def _get_client(self) -> aioredis.Redis | None:
        if self._client is not None:
            return self._client
        now = time.time()
        if now - self._last_connect_attempt < 10:
            return None
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
        client = await self._get_client()
        if client is None:
            self._acquired = True
            return True
        try:
            acquired = await client.set(
                self._name, self._owner, nx=True, ex=self._ttl
            )
            if acquired:
                self._acquired = True
            return bool(acquired)
        except Exception as e:
            logger.warning("RedisLock(%s): acquire failed: %s", self._name, e)
            self._acquired = True
            return True

    async def release(self) -> bool:
        if not self._acquired:
            return True
        client = await self._get_client()
        if client is None:
            self._acquired = False
            return True
        try:
            script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
            await client.eval(script, 1, self._name, self._owner)
            self._acquired = False
            return True
        except Exception as e:
            logger.warning("RedisLock(%s): release failed: %s", self._name, e)
            self._acquired = False
            return True

    async def renew(self) -> bool:
        if not self._acquired:
            return False
        client = await self._get_client()
        if client is None:
            return True
        try:
            script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
            result = await client.eval(
                script, 1, self._name, self._owner, self._ttl
            )
            return bool(result)
        except Exception as e:
            logger.debug("RedisLock(%s): renew failed: %s", self._name, e)
            return False

    @property
    def is_acquired(self) -> bool:
        return self._acquired

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
