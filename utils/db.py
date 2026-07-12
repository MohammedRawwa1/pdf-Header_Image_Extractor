"""Unified MongoDB connection manager for persistent caching.

Database: pdf_header_bot (configurable via MONGODB_NAME env var)

Collections:
  - job_metadata       : RQ job lifecycle, io:in/io:out, progress, errors
                        TTL: 7 days on created_at
  - user_sessions      : User interaction tracking (user_id, last_action, etc.)
                        TTL: 30 days on last_active
  - forward_batches    : /startbatch forwarded file batches
                        TTL: 24 hours on created_at
  - telethon_forwards  : Telethon userbot forward metadata
                        TTL: 7 days on created_at

All operations are best-effort and never block the main flow.
"""

import os
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_client = None
_db = None

# ── Configuration ─────────────────────────────────────────────

MONGO_URI_KEYS = ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL")
DEFAULT_DB_NAME = "pdf_header_bot"

# ── Collection names (single source of truth) ─────────────────

COL_JOBS = "job_metadata"
COL_SESSIONS = "user_sessions"
COL_BATCHES = "forward_batches"
COL_TELETHON = "telethon_forwards"

# TTL in seconds
TTL_JOBS = 7 * 24 * 3600          # 7 days
TTL_SESSIONS = 30 * 24 * 3600     # 30 days
TTL_BATCHES = 24 * 3600           # 24 hours
TTL_TELETHON = 7 * 24 * 3600      # 7 days


# ── Helpers ───────────────────────────────────────────────────

def get_db_name() -> str:
    """Return the MongoDB database name."""
    return os.environ.get("MONGODB_NAME", DEFAULT_DB_NAME)


def get_mongo_uri() -> Optional[str]:
    """Return the MongoDB connection URI from environment."""
    for key in MONGO_URI_KEYS:
        val = os.environ.get(key, "").strip()
        if val:
            return val
    return None


async def get_db():
    """Return the shared motor database instance (lazy singleton)."""
    global _client, _db
    if _db is not None:
        return _db

    uri = get_mongo_uri()
    if not uri:
        logger.debug("db: no MONGO_URI configured; MongoDB disabled")
        return None

    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        _client = AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        db_name = get_db_name()
        _db = _client[db_name]
        # Verify connectivity
        await _client.admin.command("ping")
        logger.info("db: MongoDB connected (db=%s)", db_name)
        # Create TTL indexes for automatic cleanup
        await _ensure_indexes(_db)
        return _db
    except Exception as e:
        logger.warning("db: MongoDB connection failed: %s", e)
        _client = None
        _db = None
        return None


async def _ensure_indexes(db):
    """Create TTL and unique indexes on all collections."""
    try:
        await db[COL_JOBS].create_index(
            "job_id", unique=True, background=True
        )
        await db[COL_JOBS].create_index(
            "created_at", expireAfterSeconds=TTL_JOBS, background=True
        )
        await db[COL_SESSIONS].create_index(
            "user_id", unique=True, background=True
        )
        await db[COL_SESSIONS].create_index(
            "last_active", expireAfterSeconds=TTL_SESSIONS, background=True
        )
        await db[COL_BATCHES].create_index(
            "created_at", expireAfterSeconds=TTL_BATCHES, background=True
        )
        await db[COL_TELETHON].create_index(
            "created_at", expireAfterSeconds=TTL_TELETHON, background=True
        )
        logger.debug("db: indexes ensured on all collections")
    except Exception as e:
        logger.debug("db: index creation skipped: %s", e)


async def close_db():
    """Close MongoDB connection."""
    global _client, _db
    if _client is not None:
        try:
            _client.close()
        except Exception:
            pass
    _client = None
    _db = None


# ── Job Metadata ──────────────────────────────────────────────
# Stores RQ job lifecycle, io metadata, progress, and errors.
# Keyed by job_id (unique).

async def save_job_metadata(job_id: str, meta: Dict[str, Any]) -> bool:
    """Persist job metadata (io:in, io:out, progress) to MongoDB."""
    db = await get_db()
    if not db:
        return False
    try:
        meta["job_id"] = job_id
        meta["updated_at"] = time.time()
        if "created_at" not in meta:
            meta["created_at"] = time.time()
        await db[COL_JOBS].update_one(
            {"job_id": job_id},
            {"$set": meta},
            upsert=True,
        )
        return True
    except Exception as e:
        logger.debug("db: save_job_metadata failed for %s: %s", job_id, e)
        return False


async def get_job_metadata(job_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve job metadata from MongoDB."""
    db = await get_db()
    if not db:
        return None
    try:
        return await db[COL_JOBS].find_one(
            {"job_id": job_id},
            {"_id": 0},
        )
    except Exception as e:
        logger.debug("db: get_job_metadata failed for %s: %s", job_id, e)
        return None


async def update_job_metadata(job_id: str, fields: Dict[str, Any]) -> bool:
    """Update specific fields in job metadata."""
    db = await get_db()
    if not db:
        return False
    try:
        fields["updated_at"] = time.time()
        await db[COL_JOBS].update_one(
            {"job_id": job_id},
            {"$set": fields},
            upsert=True,
        )
        return True
    except Exception as e:
        logger.debug("db: update_job_metadata failed for %s: %s", job_id, e)
        return False


async def list_jobs(status: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """List recent jobs, optionally filtered by status."""
    db = await get_db()
    if not db:
        return []
    try:
        query = {"status": status} if status else {}
        cursor = db[COL_JOBS].find(query, {"_id": 0}).sort("created_at", -1).limit(limit)
        return await cursor.to_list(length=limit)
    except Exception:
        return []


async def count_jobs() -> int:
    """Count total jobs in MongoDB."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await db[COL_JOBS].count_documents({})
    except Exception:
        return 0


# ── User Sessions ─────────────────────────────────────────────
# Tracks user interactions: last action, last seen, role flags.
# Keyed by user_id (unique).

async def save_user_session(user_id: int, session_data: Dict[str, Any]) -> bool:
    """Persist user session data to MongoDB."""
    db = await get_db()
    if not db:
        return False
    try:
        session_data["user_id"] = user_id
        session_data["last_active"] = time.time()
        if "created_at" not in session_data:
            session_data["created_at"] = time.time()
        await db[COL_SESSIONS].update_one(
            {"user_id": user_id},
            {"$set": session_data},
            upsert=True,
        )
        return True
    except Exception as e:
        logger.debug("db: save_user_session failed for %s: %s", user_id, e)
        return False


async def get_user_session(user_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve user session from MongoDB."""
    db = await get_db()
    if not db:
        return None
    try:
        return await db[COL_SESSIONS].find_one(
            {"user_id": user_id},
            {"_id": 0},
        )
    except Exception as e:
        logger.debug("db: get_user_session failed for %s: %s", user_id, e)
        return None


async def list_sessions(limit: int = 50) -> List[Dict[str, Any]]:
    """List recent user sessions."""
    db = await get_db()
    if not db:
        return []
    try:
        cursor = db[COL_SESSIONS].find({}, {"_id": 0}).sort("last_active", -1).limit(limit)
        return await cursor.to_list(length=limit)
    except Exception:
        return []


async def count_sessions() -> int:
    """Count total user sessions in MongoDB."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await db[COL_SESSIONS].count_documents({})
    except Exception:
        return 0


# ── Forward Batches ───────────────────────────────────────────
# Stores /startbatch forwarded file lists.
# Keyed by (chat_id, user_id).

async def save_forward_batch(chat_id: int, user_id: int, items: list) -> bool:
    """Persist forward batch metadata to MongoDB."""
    db = await get_db()
    if not db:
        return False
    try:
        doc = {
            "chat_id": chat_id,
            "user_id": user_id,
            "items": items,
            "count": len(items),
            "created_at": time.time(),
        }
        await db[COL_BATCHES].update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": doc},
            upsert=True,
        )
        return True
    except Exception as e:
        logger.debug("db: save_forward_batch failed: %s", e)
        return False


async def get_forward_batch(chat_id: int, user_id: int) -> Optional[list]:
    """Retrieve forward batch items from MongoDB."""
    db = await get_db()
    if not db:
        return None
    try:
        doc = await db[COL_BATCHES].find_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"_id": 0, "items": 1},
        )
        return doc["items"] if doc else None
    except Exception as e:
        logger.debug("db: get_forward_batch failed: %s", e)
        return None


async def delete_forward_batch(chat_id: int, user_id: int) -> bool:
    """Delete forward batch from MongoDB."""
    db = await get_db()
    if not db:
        return False
    try:
        await db[COL_BATCHES].delete_one(
            {"chat_id": chat_id, "user_id": user_id}
        )
        return True
    except Exception as e:
        logger.debug("db: delete_forward_batch failed: %s", e)
        return False


# ── Telethon Forwards ─────────────────────────────────────────
# Stores metadata from Telethon userbot ingestion.
# Keyed by job_id, appended (not upserted).

async def save_telethon_forward(job: dict) -> bool:
    """Save Telethon forward metadata to MongoDB."""
    db = await get_db()
    if not db:
        return False
    try:
        job["created_at"] = time.time()
        await db[COL_TELETHON].insert_one(job)
        logger.info("db: saved telethon forward %s", job.get("job_id"))
        return True
    except Exception as e:
        logger.debug("db: save_telethon_forward failed: %s", e)
        return False


async def count_telethon_forwards() -> int:
    """Count total Telethon forward records."""
    db = await get_db()
    if not db:
        return 0
    try:
        return await db[COL_TELETHON].count_documents({})
    except Exception:
        return 0


# ── Diagnostics ───────────────────────────────────────────────

async def db_stats() -> Dict[str, Any]:
    """Return a summary of all collection sizes."""
    db = await get_db()
    if not db:
        return {"connected": False, "db_name": get_db_name()}
    try:
        stats = {
            "connected": True,
            "db_name": get_db_name(),
            "collections": {},
        }
        for name in [COL_JOBS, COL_SESSIONS, COL_BATCHES, COL_TELETHON]:
            try:
                count = await db[name].count_documents({})
                stats["collections"][name] = count
            except Exception:
                stats["collections"][name] = -1
        return stats
    except Exception as e:
        return {"connected": False, "error": str(e)}
