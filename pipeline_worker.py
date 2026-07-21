"""Pipeline worker: consumes jobs from the BigFilePipeline Redis queue.

Polls the 'pdf:jobs' Redis list for jobs produced by
``BigFilePipeline.ingest_large_file()`` and processes them via
``process_input_key_job()`` (download from S3 -> thumbnail -> compress -> send).

Usage:
  python pipeline_worker.py

Requires:
  - REDIS_URL       — Redis connection string (for the job queue)
  - BOT_TOKEN       — Telegram bot token (for sending results)
  - S3_BUCKET       — S3 bucket name (for downloading stored files)
  - S3_REGION       — S3 region (optional but recommended)
  - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY — S3 credentials (optional,
    falls back to IAM role / default chain)
"""

import asyncio
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)

# How long to block on brpop when the queue is empty (seconds).
POLL_TIMEOUT = 60

# Fallback sleep between retries after unexpected errors (seconds).
ERROR_RETRY_SLEEP = 10

# Minimum required env vars for the pipeline worker to function.
_REQUIRED_ENV_VARS = {
    "REDIS_URL": "Redis connection string for job queue",
    "BOT_TOKEN": "Telegram bot token for sending results",
    "S3_BUCKET": "S3 bucket where BigFilePipeline stores ingested files",
}


def _check_env() -> list[str]:
    """Check that all required environment variables are set.

    Returns a list of missing variable names (empty if all good).
    """
    missing = []
    for var, purpose in _REQUIRED_ENV_VARS.items():
        if not os.getenv(var):
            missing.append(f"{var} ({purpose})")
    return missing


def _check_boto3() -> str | None:
    """Check that boto3 is importable (needed for S3 downloads).

    Returns an error string, or None if OK.
    """
    try:
        import boto3  # noqa: F401
        return None
    except ImportError:
        return "boto3 is not installed (run: pip install boto3)"


async def consume_loop():
    """Infinite loop: pop jobs from 'pdf:jobs' and process them."""
    from utils.job_queue import pop_job, close_redis
    import tasks

    logger.info(
        "Pipeline worker started, polling 'pdf:jobs' queue (timeout=%ss)...",
        POLL_TIMEOUT,
    )

    while True:
        try:
            job = await pop_job(timeout=POLL_TIMEOUT)
            if job is None:
                # brpop timed out with no data — queue was empty, loop back
                continue

            job_id = job.get("job_id", "unknown")
            filename = job.get("original_filename", "?")
            chat_id = job.get("chat_id")
            logger.info(
                "Pipeline worker picked up job %s (file=%s, chat=%s)",
                job_id, filename, chat_id,
            )

            start = time.time()
            try:
                # process_input_key_job is synchronous; run in a thread to avoid
                # blocking the asyncio event loop (which is needed for pop_job).
                result = await asyncio.to_thread(tasks.process_input_key_job, job)
                elapsed = time.time() - start

                if isinstance(result, dict):
                    status = result.get("status", result.get("error", "unknown"))
                    logger.info(
                        "Pipeline worker completed job %s in %.2fs: status=%s",
                        job_id, elapsed, status,
                    )
                else:
                    logger.info(
                        "Pipeline worker completed job %s in %.2fs: %s",
                        job_id, elapsed, result,
                    )
            except Exception as e:
                elapsed = time.time() - start
                logger.exception(
                    "Pipeline worker failed job %s after %.2fs: %s",
                    job_id, elapsed, e,
                )
                # (process_input_key_job already sends an error message to the user)

        except asyncio.CancelledError:
            logger.info("Pipeline worker received cancellation signal, shutting down...")
            break
        except Exception as e:
            logger.exception("Pipeline worker loop error: %s", e)
            await asyncio.sleep(ERROR_RETRY_SLEEP)

    try:
        await close_redis()
    except Exception:
        pass


def run_pipeline_worker():
    """Entry point for the pipeline worker process."""
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    # Suppress noisy library logs
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("rq").setLevel(logging.INFO)
    logging.getLogger("telegram").setLevel(logging.INFO)
    logging.getLogger("pyrogram.session.session").setLevel(logging.WARNING)
    logging.getLogger("pyrogram.connection.transport.tcp.tcp").setLevel(logging.WARNING)
    logging.getLogger("pyrogram.connection.connection").setLevel(logging.WARNING)

    # ── Startup checks ──────────────────────────────────────────
    missing = _check_env()
    if missing:
        for item in missing:
            logger.error("Missing required env var: %s", item)
        sys.exit(1)

    boto3_err = _check_boto3()
    if boto3_err:
        logger.error("Startup check failed: %s", boto3_err)
        sys.exit(1)

    logger.info("Startup checks passed. Starting pipeline worker (pid=%d)...", os.getpid())
    try:
        asyncio.run(consume_loop())
    except KeyboardInterrupt:
        logger.info("Pipeline worker stopped by user.")


if __name__ == "__main__":
    run_pipeline_worker()
