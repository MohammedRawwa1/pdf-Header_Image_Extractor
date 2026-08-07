"""Shared Redis connection manager for sync and async clients.

Provides lazy singleton access to both sync (``redis.Redis``) and async
(``redis.asyncio.Redis``) clients, all sourced from a single ``REDIS_URL``
environment variable.

For the async client, returns a wrapper with a no-op ``.close()`` so callers
can safely ``await r.close()`` without affecting the shared persistent connection.
Use ``close_async_redis()`` for actual cleanup.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ── Globals ───────────────────────────────────────────────────

_sync_client = None
_sync_raw_client = None
_async_client = None
_async_wrapper = None


# ── URL source of truth ───────────────────────────────────────


def get_redis_url() -> str:
    """Return the Redis URL from the environment (single source of truth).

    Defaults to ``redis://localhost:6379/0`` to match :mod:`config`.
    """
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


# ── Sync client ───────────────────────────────────────────────


def get_sync_redis(decode_responses: bool = True) -> redis.Redis | None:  # noqa: F821
    """Return a cached sync Redis client (lazy singleton).

    Args:
        decode_responses: If True, decode Redis responses from bytes to str.
                          Set to False if raw bytes are needed.

    Returns:
        A ``redis.Redis`` instance, or None if the connection fails.
    """
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


def get_sync_redis_raw() -> redis.Redis | None:  # noqa: F821
    """Return a cached sync Redis client with ``decode_responses=False``.

    RQ stores job payloads pickled as raw bytes, so any RQ operation
    (``Queue.enqueue``, ``Job.fetch``, ``Job.cancel``) MUST use a non-
    decoding client.  The normal ``get_sync_redis()`` singleton caches the
    first client created and ignores the ``decode_responses`` parameter on
    later calls, so it can never back RQ code — use this helper instead.

    Returns a cached client, or None if the connection fails.
    """
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
    """Close the sync Redis connection and clear the cached singletons."""
    global _sync_client, _sync_raw_client
    if _sync_client is not None:
        try:
            _sync_client.close()
        except Exception:  # nosec B110
            pass
    _sync_client = None
    if _sync_raw_client is not None:
        try:
            _sync_raw_client.close()
        except Exception:  # nosec B110
            pass
    _sync_raw_client = None


# ── Async client (no-op close wrapper) ────────────────────────


class _AsyncRedisWrapper:
    """Delegates all attribute access to the real async Redis client.

    The ``.close()`` method is a no-op so that callers can safely call
    ``await r.close()`` without closing the shared persistent connection.
    Use ``close_async_redis()`` for actual cleanup.
    """

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def close(self):
        return  # no-op


async def get_async_redis(
    decode_responses: bool = True,
    max_connections: int | None = None,
    socket_connect_timeout: int | None = None,
    socket_timeout: int | None = None,
) -> redis.asyncio.Redis | None:  # noqa: F821
    """Return a cached async Redis client (lazy singleton).

    The returned wrapper's ``.close()`` is a no-op (see ``_AsyncRedisWrapper``
    docs).  Use ``close_async_redis()`` for real cleanup.

    Args:
        decode_responses: If True, decode responses from bytes to str.
        max_connections: Max pool connections.  Falls back to the
            ``REDIS_MAX_CONNECTIONS`` env var, then the library default.
        socket_connect_timeout: Connection timeout in seconds.
        socket_timeout: Read/write timeout in seconds.

    Returns:
        A proxy/wrapper around ``redis.asyncio.Redis``, or None on failure.
    """
    if max_connections is None:
        try:
            max_connections = int(
                os.environ.get("REDIS_MAX_CONNECTIONS", "50")
            )
        except (ValueError, TypeError):
            max_connections = 50
    global _async_client, _async_wrapper
    if _async_wrapper is not None:
        return _async_wrapper

    url = get_redis_url()

    try:
        import redis.asyncio as aioredis

        kwargs: dict = {
            "decode_responses": decode_responses,
        }
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
    """Close the async Redis connection and clear the cached singleton."""
    global _async_client, _async_wrapper
    if _async_client is not None:
        try:
            aclose = getattr(_async_client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await _async_client.close()
        except Exception:  # nosec B110
            pass
    _async_client = None
    _async_wrapper = None
