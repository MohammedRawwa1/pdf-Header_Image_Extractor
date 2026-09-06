import logging

logger = logging.getLogger(__name__)


async def save_telethon_forward(job: dict) -> None:
    try:
        from utils.db import save_telethon_forward as _db_save
        await _db_save(job)
    except Exception:
        logger.debug("telethon_mongo: failed to save (non-critical)")
