"""Run an RQ worker that processes jobs from `tasks.py`.

Usage:
  python worker.py

Ensure `REDIS_URL` is set in the environment.
"""

import argparse
import logging
import os
import sys
import threading
import traceback

from redis import Redis
from rq import Queue, SimpleWorker, Worker

listen = ["default"]
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def run_worker():
    print(
        f"WORKER: run_worker starting pid={os.getpid()} redis_url={redis_url}",
        flush=True,
    )
    try:
        redis_conn = Redis.from_url(redis_url)
        print("WORKER: connected to Redis", flush=True)
        qs = [Queue(name, connection=redis_conn) for name in listen]
        print(f"WORKER: listening queues={listen}", flush=True)
        # If running in a non-main thread (started in-process), use SimpleWorker
        # to avoid installing signal handlers (which requires the main thread).
        if threading.current_thread() is not threading.main_thread():
            print(
                "WORKER: running in non-main thread; using SimpleWorker to avoid signal handlers",
                flush=True,
            )
            worker_cls = SimpleWorker
        else:
            # Use SimpleWorker on Windows (no fork) otherwise use the regular Worker
            worker_cls = SimpleWorker if os.name == "nt" else Worker
        worker = worker_cls(qs, connection=redis_conn)
        print(
            f"WORKER: instantiated worker {getattr(worker, 'name', repr(worker))}, starting work()",
            flush=True,
        )
        worker.work()
    except Exception as e:
        print("WORKER: exception in run_worker:", e, flush=True)
        traceback.print_exc()
        # re-raise so the process exits (logs will show the traceback)
        raise


def enqueue_test_job(
    bot_token: str,
    chat_id: int,
    file_id: str,
    filename: str,
    mime: str = "",
    user_id: int | None = None,
) -> None:
    """Enqueue `process_document_job` into Redis for testing from CLI."""
    redis_conn = Redis.from_url(redis_url)
    q = Queue("default", connection=redis_conn)
    try:
        import tasks

        func = getattr(tasks, "process_document_job")
        # Do NOT pass bot_token into the job; tasks will use config.BOT_TOKEN internally.
        # user_id is optional (defaults to global/admin session when None).
        q.enqueue(
            func,
            chat_id=chat_id,
            file_id=file_id,
            filename=filename,
            mime=mime,
            user_id=user_id,
        )
        logging.info(
            "Enqueued process_document_job: chat_id=%s file_id=%s filename=%s user_id=%s",
            chat_id,
            file_id,
            filename,
            user_id,
        )
    except Exception:
        logging.exception("Failed to enqueue test job")


def main():
    parser = argparse.ArgumentParser(
        description="RQ worker runner / test enqueuer"
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run", help="Run RQ worker (default)")
    enq = sub.add_parser("enqueue", help="Enqueue a process_document_job test")
    enq.add_argument(
        "--bot-token", default=os.getenv("BOT_TOKEN", ""), help="Bot token"
    )
    enq.add_argument(
        "--chat-id", required=True, type=int, help="Chat id to send result to"
    )
    enq.add_argument(
        "--file-id", required=True, help="Telegram file_id to process"
    )
    enq.add_argument(
        "--filename",
        required=True,
        help="Filename to attach to uploaded document",
    )
    enq.add_argument("--mime", default="", help="Optional mime type")
    enq.add_argument(
        "--user-id",
        type=int,
        default=None,
        help="Telegram user id to resolve the per-user session (default: global/admin session)",
    )

    args = parser.parse_args()
    # Respect LOG_LEVEL env var so we can increase verbosity without changing code
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(level=numeric_level)
    logging.getLogger("rq").setLevel(numeric_level)
    # Keep httpx at least INFO to avoid logging full request URLs (which may contain tokens)
    import logging as _logging

    logging.getLogger("httpx").setLevel(max(numeric_level, _logging.INFO))
    logging.getLogger("telegram").setLevel(numeric_level)
    # Suppress Pyrogram's noisy transport retries (BrokenPipe, timeout, etc.)
    logging.getLogger("pyrogram.session.session").setLevel(_logging.WARNING)
    logging.getLogger("pyrogram.connection.transport.tcp.tcp").setLevel(
        _logging.WARNING
    )
    logging.getLogger("pyrogram.connection.connection").setLevel(
        _logging.WARNING
    )

    if args.command == "enqueue":
        if not args.bot_token:
            logging.error(
                "Bot token required via --bot-token or BOT_TOKEN env var"
            )
            sys.exit(2)
        enqueue_test_job(
            args.bot_token,
            args.chat_id,
            args.file_id,
            args.filename,
            args.mime,
            args.user_id,
        )
    else:
        run_worker()


if __name__ == "__main__":
    main()
