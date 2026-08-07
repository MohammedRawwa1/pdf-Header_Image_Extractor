"""Async Redis job queue for background processing.

Push/pop job dicts via Redis list with optional delayed scheduling.
"""

import json
import logging
import os
import pathlib
import time
import uuid

from utils.redis_client import get_async_redis

JOB_LIST = "pdf:jobs"
DELAYED_SET = "pdf:delayed"
JOB_METADATA_TTL = int(os.getenv("JOB_METADATA_TTL", "86400"))


async def enqueue_job(job: dict) -> None:
    """Push a job dict to the Redis job list."""
    r = await get_async_redis()
    if job.get("input_path"):
        try:
            job["input_path"] = pathlib.PurePath(job["input_path"]).as_posix()
        except Exception:
            job["input_path"] = job["input_path"].replace("\\", "/")
    if job.get("output_path"):
        try:
            job["output_path"] = pathlib.PurePath(
                job["output_path"]
            ).as_posix()
        except Exception:
            job["output_path"] = job["output_path"].replace("\\", "/")

    try:
        if not job.get("request_id"):
            job["request_id"] = str(uuid.uuid4())
    except Exception:  # nosec B110
        pass

    try:
        job_id = job.get("job_id")
        if job_id:
            mapping = {
                "status": "queued",
                "progress": 0,
                "message": "queued",
                "input": job.get("input_path")
                or job.get("input_key")
                or job.get("source_url")
                or "",
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
                    except Exception:  # nosec B110
                        pass
            except Exception:  # nosec B110
                pass

            try:
                src = mapping.get("input")
                out = mapping.get("output")
                logging.getLogger(__name__).info(
                    "Prepared job %s request_id=%s input=%s output=%s",
                    job_id,
                    mapping.get("request_id"),
                    src,
                    out,
                )
            except Exception:  # nosec B110
                pass
    except Exception:  # nosec B110
        pass

    try:
        await r.lpush(JOB_LIST, json.dumps(job))
    except Exception:
        try:
            logging.getLogger(__name__).exception(
                "Failed to push job onto Redis list for job %s",
                job.get("job_id"),
            )
        except Exception:  # nosec B110
            pass
    except Exception:  # nosec B110
        pass


async def pop_job(timeout: int = 5) -> dict | None:
    """Blocking pop a job from the Redis job list."""
    r = await get_async_redis()
    try:
        try:
            now = int(time.time())
            due = await r.zrangebyscore(DELAYED_SET, "-inf", now, 0, 50)
            if due:
                for item in due:
                    raw = item.decode() if isinstance(item, bytes) else item
                    try:
                        await r.zrem(DELAYED_SET, raw)
                    except Exception:  # nosec B110
                        pass
                    try:
                        await r.lpush(JOB_LIST, raw)
                    except Exception:  # nosec B110
                        pass
        except Exception:  # nosec B110
            pass
        item = await r.brpop(JOB_LIST, timeout=timeout)
        if not item:
            return None
        raw = item[1].decode() if isinstance(item[1], bytes) else item[1]
        return json.loads(raw)
    finally:
        await r.close()


async def publish_update(channel: str, payload: dict) -> None:
    r = await get_async_redis()
    try:
        await r.publish(channel, json.dumps(payload))
    finally:
        await r.close()


async def cancel_job(job_id: str) -> None:
    """Set cancel flag for a job."""
    r = await get_async_redis()
    try:
        await r.hset(f"pdf:job:{job_id}", mapping={"cancel": "1"})
    finally:
        await r.close()
