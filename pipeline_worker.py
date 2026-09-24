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

# How long to wait between startup-check retries when the worker is blocked
# on missing configuration (seconds).
STARTUP_RETRY_SLEEP = 30

# Log a heartbeat line every N startup-check retries so a misconfigured
# deploy stays visible in logs instead of silently dying.
HEARTBEAT_EVERY_RETRIES = 10

# Minimum required env vars for the pipeline worker to function.
_REQUIRED_ENV_VARS = {
    "REDIS_URL": "Redis connection string for job queue",
    "BOT_TOKEN": "Required for the bot to deliver results",  # nosec B105 - env-var name, not a literal secret
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


def _startup_gate() -> None:
    """Block until startup checks pass, retrying instead of exiting.

    A worker that ``sys.exit(1)``s on missing config crash-loops under a
    Railway ``ON_FAILURE`` restart policy and, once the retry budget is
    exhausted, dies *silently* — the queue keeps growing and nothing is
    visible in the logs.  This gate keeps the process alive, logs exactly
    what is missing, and emits a periodic heartbeat so a misconfigured
    deploy is impossible to miss.

    It is also self-healing: if the env is fixed (or a late dependency
    finishes installing) the worker proceeds to ``consume_loop`` on its
    own without a redeploy.

    Note: a permanently-missing dependency (e.g. boto3 not in the image)
    keeps the worker blocked rather than crash-looping — deliberate, so the
    failure stays visible; it normally resolves on the next deploy.
    """
    attempt = 0
    while True:
        missing = _check_env()
        boto3_err = _check_boto3()
        if not missing and boto3_err is None:
            if attempt:
                logger.info("Startup checks passed after %d retries.", attempt)
            return

        attempt += 1
        if attempt == 1:
            # First failure: spell out exactly what is wrong so the deploy
            # is diagnosed from the first glance at the logs.
            for item in missing:
                logger.error("Missing required env var: %s", item)
            if boto3_err:
                logger.error("Startup check failed: %s", boto3_err)
            logger.error(
                "Pipeline worker is BLOCKED on configuration - it will stay "
                "alive and retry every %ss instead of exiting, so this "
                "misconfigured deploy stays visible in the logs.",
                STARTUP_RETRY_SLEEP,
            )
        elif attempt % HEARTBEAT_EVERY_RETRIES == 0:
            # Periodic heartbeat: proves the process is alive, just blocked.
            issues = [", ".join(missing)] if missing else []
            if boto3_err:
                issues.append(boto3_err)
            logger.warning(
                "Pipeline worker heartbeat (attempt %d): still blocked on "
                "startup checks - %s. Next retry in %ss.",
                attempt,
                "; ".join(issues),
                STARTUP_RETRY_SLEEP,
            )
        time.sleep(STARTUP_RETRY_SLEEP)


async def consume_loop():
    """Infinite loop: pop jobs from 'pdf:jobs' and process them."""
    import tasks
    from utils.job_queue import close_redis, pop_job

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
                job_id,
                filename,
                chat_id,
            )

            start = time.time()
            try:
                # process_input_key_job is synchronous; run in a thread to avoid
                # blocking the asyncio event loop (which is needed for pop_job).
                result = await asyncio.to_thread(
                    tasks.process_input_key_job, job
                )
                elapsed = time.time() - start

                if isinstance(result, dict):
                    status = result.get(
                        "status", result.get("error", "unknown")
                    )
                    logger.info(
                        "Pipeline worker completed job %s in %.2fs: status=%s",
                        job_id,
                        elapsed,
                        status,
                    )
                else:
                    logger.info(
                        "Pipeline worker completed job %s in %.2fs: %s",
                        job_id,
                        elapsed,
                        result,
                    )
            except Exception as e:
                elapsed = time.time() - start
                logger.exception(
                    "Pipeline worker failed job %s after %.2fs: %s",
                    job_id,
                    elapsed,
                    e,
                )
                # (process_input_key_job already sends an error message to the user)

        except asyncio.CancelledError:
            logger.info(
                "Pipeline worker received cancellation signal, shutting down..."
            )
            break
        except Exception as e:
            logger.exception("Pipeline worker loop error: %s", e)
            await asyncio.sleep(ERROR_RETRY_SLEEP)

    try:
        await close_redis()
    except Exception:  # nosec B110
        pass


def run_pipeline_worker():
    """Entry point for the pipeline worker process."""
    logging.basicConfig(
        level=getattr(
            logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO
        ),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    # Suppress noisy library logs
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("rq").setLevel(logging.INFO)
    logging.getLogger("telegram").setLevel(logging.INFO)
    logging.getLogger("pyrogram.session.session").setLevel(logging.WARNING)
    logging.getLogger("pyrogram.connection.transport.tcp.tcp").setLevel(
        logging.WARNING
    )
    logging.getLogger("pyrogram.connection.connection").setLevel(
        logging.WARNING
    )

    # ── Startup checks (retry gate instead of exit) ────────────
    # Missing env vars / missing boto3 no longer crash the process (which
    # goes silent once Railway's ON_FAILURE retry budget is exhausted).
    # The gate below keeps the worker alive, logs what is wrong, retries,
    # and emits a heartbeat so a misconfigured deploy is visible.
    try:
        _startup_gate()
    except KeyboardInterrupt:
        logger.info("Pipeline worker stopped during startup wait.")
        return

    logger.info(
        "Startup checks passed. Starting pipeline worker (pid=%d)...",
        os.getpid(),
    )
    try:
        asyncio.run(consume_loop())
    except KeyboardInterrupt:
        logger.info("Pipeline worker stopped by user.")


if __name__ == "__main__":
    run_pipeline_worker()
