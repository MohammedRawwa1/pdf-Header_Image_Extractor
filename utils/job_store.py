"""Async MongoDB job store for conversion jobs."""
import os
import asyncio
from typing import Optional, Dict, Any

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None

_client = None
_db = None


async def init(mongo_uri: Optional[str] = None, db_name: str = "media_bot"):
    global _client, _db
    if AsyncIOMotorClient is None:
        raise RuntimeError("motor is required for job_store")
    mongo_uri = mongo_uri or os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI not set for job_store")
    _client = AsyncIOMotorClient(mongo_uri)
    _db = _client[db_name]


async def save_job(job: Dict[str, Any]) -> None:
    """Insert a new job document. Job dict must contain `job_id`."""
    if _db is None:
        return
    doc = {**job, "status": job.get("status", "queued")}
    try:
        bot_id = job.get("bot_id") or os.environ.get("BOT_ID") or os.environ.get("BOT_USERNAME")
        if bot_id:
            doc["bot_id"] = bot_id
    except Exception:
        pass
    await _db.jobs.insert_one(doc)


async def update_job(job_id: str, fields: Dict[str, Any]) -> None:
    if _db is None:
        return
    await _db.jobs.update_one({"job_id": job_id}, {"$set": fields}, upsert=False)


async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    if _db is None:
        return None
    return await _db.jobs.find_one({"job_id": job_id})


async def close():
    global _client
    if _client is not None:
        _client.close()
        _client = None
