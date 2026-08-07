# utils/progress_tracker.py
"""Progress tracker for PDF download/upload operations with visual progress bar.

Now includes Redis persistence so progress survives process restarts,
and MongoDB backup for durable job history.
"""

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

PREFIX_PROGRESS = "progress:"


from utils.rate_limiter import telegram_api_limiter  # noqa: E402
from utils.redis_client import get_sync_redis  # noqa: E402


@dataclass
class TaskProgress:
    """Track progress of a PDF processing task."""

    task_id: str
    user_id: int
    file_name: str
    total_size: int
    processed_size: int = 0
    status: str = "pending"  # pending, downloading, processing, uploading, completed, failed
    start_time: float | None = None
    end_time: float | None = None
    error_message: str | None = None
    _last_update: float | None = None

    @property
    def progress_percentage(self) -> float:
        if self.total_size == 0:
            return 0
        return (self.processed_size / self.total_size) * 100

    @property
    def elapsed_time(self) -> float:
        if self.start_time is None:
            return 0
        return (self.end_time or time.time()) - self.start_time

    @property
    def estimated_time_remaining(self) -> float:
        if self.progress_percentage == 0:
            return 0
        elapsed = self.elapsed_time
        if elapsed == 0:
            return 0
        return (elapsed / self.progress_percentage) * (
            100 - self.progress_percentage
        )

    def update_progress(self, processed_size: int):
        self.processed_size = processed_size

    def start(self):
        self.start_time = time.time()
        self.status = "processing"

    def complete(self):
        self.end_time = time.time()
        self.status = "completed"
        self.processed_size = self.total_size

    def fail(self, error_message: str):
        self.end_time = time.time()
        self.status = "failed"
        self.error_message = error_message

    def cancel(self):
        """Mark the task as cancelled (used by /canceljob)."""
        self.end_time = time.time()
        self.status = "cancelled"

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "file_name": self.file_name,
            "total_size": self.total_size,
            "processed_size": self.processed_size,
            "progress_percentage": self.progress_percentage,
            "status": self.status,
            "elapsed_time": self.elapsed_time,
            "estimated_time_remaining": self.estimated_time_remaining,
            "error_message": self.error_message,
        }


class ProgressTracker:
    """Manage multiple task progress trackers with Redis persistence."""

    def __init__(self):
        self.tasks: dict[str, TaskProgress] = {}
        self.callbacks: dict[str, Callable] = {}

    def _persist_to_redis(self, task: TaskProgress):
        """Best-effort write progress to Redis for survival across restarts."""
        try:
            r = get_sync_redis()
            if r is None:
                return
            key = f"{PREFIX_PROGRESS}{task.task_id}"
            data = json.dumps(task.to_dict(), default=str)
            # Active tasks get 1h TTL, completed/failed get 5min
            ttl = 3600 if task.status not in ("completed", "failed") else 300
            r.setex(key, ttl, data)
        except Exception:  # nosec B110
            pass

    def create_task(
        self, task_id: str, user_id: int, file_name: str, total_size: int
    ) -> TaskProgress:
        task = TaskProgress(
            task_id=task_id,
            user_id=user_id,
            file_name=file_name,
            total_size=total_size,
        )
        self.tasks[task_id] = task
        self._persist_to_redis(task)
        logger.info("Created task tracker: %s", task_id)
        return task

    def get_task(self, task_id: str) -> TaskProgress | None:
        # Try in-memory first
        task = self.tasks.get(task_id)
        if task:
            return task
        # Fall back to Redis
        try:
            r = get_sync_redis()
            if r:
                raw = r.get(f"{PREFIX_PROGRESS}{task_id}")
                if raw:
                    data = json.loads(raw)
                    task = TaskProgress(
                        task_id=data["task_id"],
                        user_id=data["user_id"],
                        file_name=data["file_name"],
                        total_size=data["total_size"],
                        processed_size=data.get("processed_size", 0),
                        status=data.get("status", "pending"),
                        start_time=data.get("start_time"),
                        end_time=data.get("end_time"),
                        error_message=data.get("error_message"),
                    )
                    self.tasks[task_id] = task
                    return task
        except Exception:  # nosec B110
            pass
        return None

    async def update_task_progress(self, task_id: str, processed_size: int):
        task = self.tasks.get(task_id)
        if task:
            task.update_progress(processed_size)
            self._persist_to_redis(task)
            await self._notify_callbacks(task_id, task)

    def start_task(self, task_id: str):
        task = self.tasks.get(task_id)
        if task:
            task.start()
            self._persist_to_redis(task)
            logger.info("Started task: %s", task_id)

    async def complete_task(self, task_id: str):
        task = self.tasks.get(task_id)
        if task:
            task.complete()
            self._persist_to_redis(task)
            # Save to MongoDB for durable history
            await self._save_to_mongodb(task)
            logger.info("Completed task: %s", task_id)
            await self._notify_callbacks(task_id, task)

    async def fail_task(self, task_id: str, error_message: str):
        task = self.tasks.get(task_id)
        if task:
            task.fail(error_message)
            self._persist_to_redis(task)
            # Save to MongoDB for durable history
            await self._save_to_mongodb(task)
            logger.error("Task failed: %s - %s", task_id, error_message)
            await self._notify_callbacks(task_id, task)

    async def _save_to_mongodb(self, task: TaskProgress):
        """Best-effort save completed/failed task to MongoDB for history."""
        try:
            from utils.db import save_job_metadata

            await save_job_metadata(
                task.task_id,
                {
                    "type": "progress",
                    "user_id": task.user_id,
                    "file_name": task.file_name,
                    "total_size": task.total_size,
                    "processed_size": task.processed_size,
                    "status": task.status,
                    "elapsed_time": task.elapsed_time,
                    "error_message": task.error_message,
                },
            )
        except Exception:  # nosec B110
            pass

    def remove_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
        # Always clean up the Redis key, even when the task only exists in
        # Redis (e.g. after a process restart) so no stale keys accumulate.
        try:
            r = get_sync_redis()
            if r:
                r.delete(f"{PREFIX_PROGRESS}{task_id}")
        except Exception:  # nosec B110
            pass
        logger.info("Removed task: %s", task_id)

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a task completely: mark cancelled, persist, then wipe from
        Redis + memory so no stale keys remain.

        Returns True if a task was found and cancelled.
        """
        task = self.tasks.get(task_id)
        if task is None:
            task = self.get_task(task_id)
        if task is None:
            return False
        task.cancel()
        # No need to persist the cancelled state to Redis: remove_task below
        # wipes the key entirely (cancelled status only matters for the Mongo
        # backup + callbacks).
        await self._save_to_mongodb(task)
        await self._notify_callbacks(task_id, task)
        # Wipe from Redis and memory now that the task is cancelled
        self.remove_task(task_id)
        logger.info("Cancelled task: %s", task_id)
        return True

    def find_task_id_by_prefix(self, prefix: str) -> str | None:
        """Find an active task id by prefix (users see truncated ids like `abc12345`)."""
        # in-memory first
        for tid in self.tasks:
            if tid.startswith(prefix):
                return tid
        # Redis fallback (active tasks are persisted under `progress:<id>`)
        try:
            r = get_sync_redis()
            if r:
                for key in r.scan_iter(f"{PREFIX_PROGRESS}*", count=100):
                    k = key.decode() if isinstance(key, bytes) else key
                    tid = k[len(PREFIX_PROGRESS):]
                    if tid.startswith(prefix):
                        return tid
        except Exception:  # nosec B110
            pass
        return None

    def register_callback(self, task_id: str, callback: Callable):
        self.callbacks[task_id] = callback

    def unregister_callback(self, task_id: str):
        """Remove a registered progress callback (called on final state)."""
        self.callbacks.pop(task_id, None)

    async def _notify_callbacks(self, task_id: str, task: TaskProgress):
        callback = self.callbacks.get(task_id)
        if not callback:
            return
        try:
            if inspect.iscoroutinefunction(callback):
                await callback(task)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, callback, task)
        except Exception as e:
            logger.error(
                "Error executing callback for task %s: %s", task_id, e
            )

    def get_all_tasks(self) -> dict[str, TaskProgress]:
        return self.tasks

    def cleanup_old_tasks(self, max_age_hours: int = 24):
        current_time = time.time()
        tasks_to_remove = []
        for task_id, task in self.tasks.items():
            if task.end_time and (current_time - task.end_time) > (
                max_age_hours * 3600
            ):
                tasks_to_remove.append(task_id)
        for task_id in tasks_to_remove:
            self.remove_task(task_id)
        return len(tasks_to_remove)


# Global progress tracker instance
progress_tracker = ProgressTracker()


def _format_size(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _build_progress_bar(percentage: float, segments: int = 20) -> str:
    """Build a Unicode progress bar string.

    █ = filled, ░ = empty
    20 segments = 5% each for smooth granularity.
    """
    full_block = "\u2588"
    empty = "\u2591"
    filled = int(percentage / (100 / segments))
    filled = max(0, min(segments, filled))

    if filled >= segments:
        bar = full_block * segments
    elif filled <= 0:
        bar = empty * segments
    else:
        bar = full_block * filled + empty * (segments - filled)
    return "[" + bar + "]"


def _format_time(seconds: float) -> str:
    """Format seconds to human readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


async def send_progress_update(
    chat_id: int, bot, task: TaskProgress, message_id: int | None = None
):
    """Send or update progress message with visual progress bar.

    Uses Unicode block characters for maximum cross-client compatibility.
    Bar uses 3 shades: █ (filled), ▓ (partial), ░ (remaining)
    20 segments = 5% each for smooth granularity.

    Throttled by the shared ``telegram_api_limiter`` (global 30/s +
    per-user 1/s) — this is the hottest outbound path (2-3 sends per
    file under multi-user load), so it must respect Telegram flood limits.
    """
    # ── Respect Telegram API rate limits (global 30/s + per-user 1/s) ──
    try:
        await telegram_api_limiter.wait_if_needed(
            str(getattr(task, "user_id", 0) or 0)
        )
    except Exception:  # nosec B110 - throttling is best-effort
        pass
    try:
        total_progress = task.progress_percentage
        bar = _build_progress_bar(total_progress)

        processed = _format_size(task.processed_size)
        total = _format_size(task.total_size)

        status_emojis = {
            "pending": "\u23f3",
            "downloading": "\U0001f4e5",
            "processing": "\u2699\ufe0f",
            "uploading": "\U0001f4e4",
            "completed": "\u2705",
            "failed": "\u274c",
        }
        status_emoji = status_emojis.get(task.status, "\u2753")

        # Speed calculation (best-effort)
        speed_str = ""
        if task.start_time and task.processed_size > 0:
            elapsed = task.elapsed_time
            if elapsed > 0:
                bytes_per_sec = task.processed_size / elapsed
                speed_str = (
                    f"\U0001f680 Speed: {_format_size(int(bytes_per_sec))}/s\n"
                )

        message_text = (
            f"\U0001f4ca **File Processing Progress**\n\n"
            f"\U0001f4c1 File: `{task.file_name}`\n"
            f"\U0001f4cf Size: {processed} / {total}\n"
            f"\U0001f4c8 Progress: `{total_progress:.1f}%`\n"
            f"`{bar}`\n\n"
            f"{speed_str}"
            f"\u23f1 Elapsed: `{_format_time(task.elapsed_time)}`\n"
            f"\u23f3 Remaining: `{_format_time(task.estimated_time_remaining)}`\n"
            f"{status_emoji} Status: **{task.status.title()}**\n\n"
            f"\U0001f194 ID: `{task.task_id[:8]}`"
        )

        if message_id:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message_text,
                parse_mode="Markdown",
            )
        else:
            msg = await bot.send_message(
                chat_id=chat_id, text=message_text, parse_mode="Markdown"
            )
            return msg.message_id

    except Exception as e:
        err_msg = str(e).lower()
        # "Message to edit not found" / "message not found" are benign — the progress
        # message was already deleted or the task finished before the final edit.
        # Log at DEBUG instead of ERROR to avoid alarming in an otherwise healthy pipeline.
        if (
            "message to edit not found" in err_msg
            or "message not found" in err_msg
            or "message can't be edited" in err_msg
        ):
            logger.debug("Progress update edit skipped (message gone): %s", e)
        else:
            logger.error("Error sending progress update: %s", e)
        return None
