"""Run an RQ worker that processes jobs from `tasks.py`.

Usage:
  python worker.py

Ensure `REDIS_URL` is set in the environment.
"""
import os
import logging
import argparse
import sys
from redis import Redis
from rq import Worker, Queue, Connection

listen = ["default"]
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def run_worker():
    redis_conn = Redis.from_url(redis_url)
    with Connection(redis_conn):
        qs = list(map(Queue, listen))
        worker = Worker(qs)
        worker.work()


def enqueue_test_job(bot_token: str, chat_id: int, file_id: str, filename: str, mime: str = "") -> None:
  """Enqueue `process_document_job` into Redis for testing from CLI."""
  redis_conn = Redis.from_url(redis_url)
  q = Queue("default", connection=redis_conn)
  try:
    import tasks
    func = getattr(tasks, "process_document_job")
    q.enqueue(func, bot_token, chat_id, file_id, filename, mime)
    logging.info("Enqueued process_document_job: chat_id=%s file_id=%s filename=%s", chat_id, file_id, filename)
  except Exception:
    logging.exception("Failed to enqueue test job")


def main():
  parser = argparse.ArgumentParser(description="RQ worker runner / test enqueuer")
  sub = parser.add_subparsers(dest="command")
  sub.add_parser("run", help="Run RQ worker (default)")
  enq = sub.add_parser("enqueue", help="Enqueue a process_document_job test")
  enq.add_argument("--bot-token", default=os.getenv("BOT_TOKEN", ""), help="Bot token")
  enq.add_argument("--chat-id", required=True, type=int, help="Chat id to send result to")
  enq.add_argument("--file-id", required=True, help="Telegram file_id to process")
  enq.add_argument("--filename", required=True, help="Filename to attach to uploaded document")
  enq.add_argument("--mime", default="", help="Optional mime type")

  args = parser.parse_args()
  logging.basicConfig(level=logging.INFO)

  if args.command == "enqueue":
    if not args.bot_token:
      logging.error("Bot token required via --bot-token or BOT_TOKEN env var")
      sys.exit(2)
    enqueue_test_job(args.bot_token, args.chat_id, args.file_id, args.filename, args.mime)
  else:
    run_worker()


if __name__ == "__main__":
  main()
