"""Async MongoDB job store for conversion jobs.

Refactored to use the prepared-statement MongoQueryBuilder from utils.db
for consistent NoSQL injection prevention across the codebase.
"""

from typing import Any

from utils.db import COL_JOBS, query


async def save_job(job: dict[str, Any]) -> None:
    """Insert a new job document using the prepared statement query builder.

    Only fields in the COL_JOBS whitelist are persisted (like Laravel's $fillable).
    Job dict must contain `job_id`.
    """
    doc = {**job, "status": job.get("status", "queued")}
    try:
        bot_id = job.get("bot_id")
        if bot_id:
            doc["bot_id"] = bot_id
    except Exception:  # nosec B110
        pass
    await query(COL_JOBS).insert(doc)


async def update_job(job_id: str, fields: dict[str, Any]) -> None:
    """Update job fields using parameter binding in the WHERE clause.

    Only whitelisted fields are applied to the document.
    """
    await query(COL_JOBS).where("job_id", "=", job_id).update(fields)


async def get_job(job_id: str) -> dict[str, Any] | None:
    """Retrieve a job by job_id using parameterized queries."""
    return await query(COL_JOBS).where("job_id", "=", job_id).first()
