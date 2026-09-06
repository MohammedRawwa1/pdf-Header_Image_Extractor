from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_sync_client = None
_sync_raw_client = None
_async_client = None
_async_wrapper = None


def get_redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def get_sync_redis(decode_responses: bool = True) -> redis.Redis | None:
    global _sync_client
    if _sync_client is not None:
        return _sync_client
    url = get_redis_url()
    try:
        import redis
        _sync_client = redis.from_url(url, decode_responses=decode_responses)
        logger.info("redis_client: sync Redis connected")
        return _sync_client
    except Exception as e:
        logger.warning("redis_client: sync Redis connection failed: %s", e)
        _sync_client = None
        return None


def get_sync_redis_raw() -> redis.Redis | None:
    global _sync_raw_client
    if _sync_raw_client is not None:
        return _sync_raw_client
    url = get_redis_url()
    try:
        import redis
        _sync_raw_client = redis.from_url(url, decode_responses=False)
        logger.info("redis_client: sync raw Redis connected")
        return _sync_raw_client
    except Exception as e:
        logger.warning("redis_client: sync raw Redis connection failed: %s", e)
        _sync_raw_client = None
        return None


def close_sync_redis():
    global _sync_client, _sync_raw_client
    if _sync_client is not None:
        try:
            _sync_client.close()
        except Exception:
            pass
    _sync_client = None
    if _sync_raw_client is not None:
        try:
            _sync_raw_client.close()
        except Exception:
            pass
    _sync_raw_client = None


class _AsyncRedisWrapper:
    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def close(self):
        return


async def get_async_redis(
    decode_responses: bool = True,
    max_connections: int | None = None,
    socket_connect_timeout: int | None = None,
    socket_timeout: int | None = None,
) -> redis.asyncio.Redis | None:
    if max_connections is None:
        try:
            max_connections = int(os.environ.get("REDIS_MAX_CONNECTIONS", "50"))
        except (ValueError, TypeError):
            max_connections = 50
    global _async_client, _async_wrapper
    if _async_wrapper is not None:
        return _async_wrapper
    url = get_redis_url()
    try:
        import redis.asyncio as aioredis
        kwargs: dict = {"decode_responses": decode_responses}
        if max_connections is not None:
            kwargs["max_connections"] = max_connections
        if socket_connect_timeout is not None:
            kwargs["socket_connect_timeout"] = socket_connect_timeout
        if socket_timeout is not None:
            kwargs["socket_timeout"] = socket_timeout
        _async_client = aioredis.from_url(url, **kwargs)
        _async_wrapper = _AsyncRedisWrapper(_async_client)
        logger.info("redis_client: async Redis connected")
        return _async_wrapper
    except Exception as e:
        logger.warning("redis_client: async Redis connection failed: %s", e)
        _async_client = None
        _async_wrapper = None
        return None


async def close_async_redis():
    global _async_client, _async_wrapper
    if _async_client is not None:
        try:
            aclose = getattr(_async_client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await _async_client.close()
        except Exception:
            pass
    _async_client = None
    _async_wrapper = None
