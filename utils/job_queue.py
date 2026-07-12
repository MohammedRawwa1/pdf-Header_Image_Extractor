"""Async Redis job queue for background processing.

Push/pop job dicts via Redis list with optional delayed scheduling.
"""

import json
import os
import asyncio
import logging
import time
from typing import Optional
from urllib.parse import urlparse
import uuid

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

JOB_LIST = "pdf:jobs"
DELAYED_SET = "pdf:delayed"
JOB_METADATA_TTL = int(os.getenv("JOB_METADATA_TTL", "86400"))

_redis_client = None
_redis_proxy = None


async def get_redis():
    global _redis_client, _redis_proxy
    if not aioredis:
        raise RuntimeError("redis.asyncio is required for job queue")
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")

    try:
        if _redis_proxy is not None:
            return _redis_proxy
    except NameError:
        pass

    _redis_client = aioredis.from_url(
        redis_url,
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "50")),
    )

    class _RedisProxy:
        def __init__(self, client):
            self._client = client

        def __getattr__(self, name):
            return getattr(self._client, name)

        async def close(self):
            return

    _redis_proxy = _RedisProxy(_redis_client)
    return _redis_proxy


async def close_redis():
    global _redis_client, _redis_proxy
    try:
        if _redis_client is not None:
            try:
                aclose = getattr(_redis_client, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await _redis_client.close()
            except Exception:
                pass
    finally:
        _redis_client = None
        _redis_proxy = None


async def enqueue_job(job: dict) -> None:
    """Push a job dict to the Redis job list."""
    r = await get_redis()
    try:
        import pathlib
        if job.get("input_path"):
            try:
                job["input_path"] = pathlib.PurePath(job["input_path"]).as_posix()
            except Exception:
                job["input_path"] = job["input_path"].replace("\\", "/")
        if job.get("output_path"):
            try:
                job["output_path"] = pathlib.PurePath(job["output_path"]).as_posix()
            except Exception:
                job["output_path"] = job["output_path"].replace("\\", "/")
    except Exception:
        try:
            if job.get("input_path"):
                job["input_path"] = job["input_path"].replace("\\", "/")
            if job.get("output_path"):
                job["output_path"] = job["output_path"].replace("\\", "/")
        except Exception:
            pass

    try:
        if not job.get("request_id"):
            job["request_id"] = str(uuid.uuid4())
    except Exception:
        pass

    try:
        job_id = job.get("job_id")
        if job_id:
            mapping = {
                "status": "queued",
                "progress": 0,
                "message": "queued",
                "input": job.get("input_path") or job.get("input_key") or job.get("source_url") or "",
                "input_key": job.get("input_key") or "",
                "output": job.get("output_path") or job.get("output") or "",
                "created_at": str(time.time()),
                "request_id": job.get("request_id") or "",
            }
            try:
                await r.hset(f"pdf:job:{job_id}", mapping=mapping)
                if JOB_METADATA_TTL and JOB_METADATA_TTL > 0:
                    try:
                        await r.expire(f"pdf:job:{job_id}", JOB_METADATA_TTL)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                src = mapping.get("input")
                out = mapping.get("output")
                logging.getLogger(__name__).info("Prepared job %s request_id=%s input=%s output=%s", job_id, mapping.get("request_id"), src, out)
            except Exception:
                pass
    except Exception:
        pass

    try:
        await r.lpush(JOB_LIST, json.dumps(job))
    except Exception:
        try:
            logging.getLogger(__name__).exception("Failed to push job onto Redis list for job %s", job.get("job_id"))
        except Exception:
            pass
    except Exception:
        pass


async def pop_job(timeout: int = 5) -> Optional[dict]:
    """Blocking pop a job from the Redis job list."""
    r = await get_redis()
    try:
        try:
            now = int(time.time())
            due = await r.zrangebyscore(DELAYED_SET, "-inf", now, 0, 50)
            if due:
                for item in due:
                    raw = item.decode() if isinstance(item, bytes) else item
                    try:
                        await r.zrem(DELAYED_SET, raw)
                    except Exception:
                        pass
                    try:
                        await r.lpush(JOB_LIST, raw)
                    except Exception:
                        pass
        except Exception:
            pass
        item = await r.brpop(JOB_LIST, timeout=timeout)
        if not item:
            return None
        raw = item[1].decode() if isinstance(item[1], bytes) else item[1]
        return json.loads(raw)
    finally:
        await r.close()


async def publish_update(channel: str, payload: dict) -> None:
    r = await get_redis()
    try:
        await r.publish(channel, json.dumps(payload))
    finally:
        await r.close()


async def cancel_job(job_id: str) -> None:
    """Set cancel flag for a job."""
    r = await get_redis()
    try:
        await r.hset(f"pdf:job:{job_id}", mapping={"cancel": "1"})
    finally:
        await r.close()
