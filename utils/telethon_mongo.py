"""Minimal Telethon-MongoDB bridge stub.

In the full media_conersion_bot this saves Telethon ingestion metadata to MongoDB.
For the PDF header extractor bot, this is a lightweight no-op placeholder.
"""

import logging

logger = logging.getLogger(__name__)


async def save_telethon_forward(job: dict) -> None:
    """Best-effort save of Telethon forward metadata to MongoDB.

    This is a no-op when MONGO_URI is not configured.
    """
    import os
    mongo_uri = (
        os.environ.get("MONGO_URI")
        or os.environ.get("MONGODB_URL")
        or os.environ.get("MONGODB_URI")
        or os.environ.get("MONGO_URL")
    )
    if not mongo_uri:
        logger.debug("telethon_mongo: no MONGO_URI configured; skipping save")
        return
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        client = AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=3000)
        db_name = os.environ.get("MONGODB_NAME", "pdf_header_bot")
        col_prefix = os.environ.get("MONGODB_COLLECTION_PREFIX", "")
        col_name = f"{col_prefix}telethon_forwards" if col_prefix else "telethon_forwards"
        db = client[db_name]
        await db[col_name].insert_one(job)
        logger.info("telethon_mongo: saved forward job %s", job.get("job_id"))
    except Exception:
        logger.debug("telethon_mongo: failed to save (non-critical)")
