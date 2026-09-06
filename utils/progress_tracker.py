import asyncio
import inspect
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from utils.markdown_utils import safe_code_span

logger = logging.getLogger(__name__)

PREFIX_PROGRESS = "progress:"

from utils.rate_limiter import telegram_api_limiter
from utils.redis_client import get_sync_redis


@dataclass
class TaskProgress:
    task_id: str
    user_id: int
    file_name: str
    total_size: int
    processed_size: int = 0
    status: str = "pending"
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
        return (elapsed / self.progress_percentage) * (100 - self.progress_percentage)

    def update_progress(self, processed_size: int):
        self.processed_size = processed_size
        self._last_update = time.time()

    def start(self):
        self.start_time = time.time()
        self.status = "processing"
        self.end_time = None
        self.error_message = None
        self._last_update = time.time()

    def complete(self):
        self.end_time = time.time()
        self.status = "completed"
        self.processed_size = self.total_size

    def fail(self, error_message: str):
        self.end_time = time.time()
        self.status = "failed"
        self.error_message = error_message

    def cancel(self):
        self.end_time = time.time()
        self.status = "cancelled"

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id, "user_id": self.user_id, "file_name": self.file_name,
            "total_size": self.total_size, "processed_size": self.processed_size,
            "progress_percentage": self.progress_percentage, "status": self.status,
            "elapsed_time": self.elapsed_time, "estimated_time_remaining": self.estimated_time_remaining,
            "error_message": self.error_message, "_last_update": self._last_update,
        }


class ProgressTracker:
    def __init__(self):
        self.tasks: dict[str, TaskProgress] = {}
        self.callbacks: dict[str, Callable] = {}

    def _persist_to_redis(self, task: TaskProgress):
        try:
            r = get_sync_redis()
            if r is None:
                return
            key = f"{PREFIX_PROGRESS}{task.task_id}"
            data = json.dumps(task.to_dict(), default=str)
            ttl = 3600 if task.status not in ("completed", "failed") else 300
            r.setex(key, ttl, data)
        except Exception:
            pass

    def create_task(self, task_id: str, user_id: int, file_name: str, total_size: int) -> TaskProgress:
        task = TaskProgress(task_id=task_id, user_id=user_id, file_name=file_name, total_size=total_size)
        task._last_update = time.time()
        self.tasks[task_id] = task
        self._persist_to_redis(task)
        logger.info("Created task tracker: %s", task_id)
        return task

    def get_task(self, task_id: str) -> TaskProgress | None:
        task = self.tasks.get(task_id)
        if task:
            return task
        try:
            r = get_sync_redis()
            if r:
                raw = r.get(f"{PREFIX_PROGRESS}{task_id}")
                if raw:
                    data = json.loads(raw)
                    task = TaskProgress(
                        task_id=data["task_id"], user_id=data["user_id"], file_name=data["file_name"],
                        total_size=data["total_size"], processed_size=data.get("processed_size", 0),
                        status=data.get("status", "pending"), start_time=data.get("start_time"),
                        end_time=data.get("end_time"), error_message=data.get("error_message"),
                        _last_update=data.get("_last_update"),
                    )
                    self.tasks[task_id] = task
                    return task
        except Exception:
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
            await self._save_to_mongodb(task)
            logger.info("Completed task: %s", task_id)
            await self._notify_callbacks(task_id, task)

    async def fail_task(self, task_id: str, error_message: str):
        task = self.tasks.get(task_id)
        if task:
            task.fail(error_message)
            self._persist_to_redis(task)
            await self._save_to_mongodb(task)
            logger.error("Task failed: %s - %s", task_id, error_message)
            await self._notify_callbacks(task_id, task)

    async def _save_to_mongodb(self, task: TaskProgress):
        try:
            from utils.db import save_job_metadata
            await save_job_metadata(task.task_id, {
                "type": "progress", "user_id": task.user_id, "file_name": task.file_name,
                "total_size": task.total_size, "processed_size": task.processed_size,
                "status": task.status, "elapsed_time": task.elapsed_time, "error_message": task.error_message,
            })
        except Exception:
            pass

    def remove_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
        try:
            r = get_sync_redis()
            if r:
                r.delete(f"{PREFIX_PROGRESS}{task_id}")
        except Exception:
            pass
        logger.info("Removed task: %s", task_id)

    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task is None:
            task = self.get_task(task_id)
        if task is None:
            return False
        task.cancel()
        await self._save_to_mongodb(task)
        await self._notify_callbacks(task_id, task)
        self.remove_task(task_id)
        logger.info("Cancelled task: %s", task_id)
        return True

    def find_task_id_by_prefix(self, prefix: str) -> str | None:
        if len(prefix) < 4:
            return None
        for tid in self.tasks:
            if tid.startswith(prefix):
                return tid
        try:
            r = get_sync_redis()
            if r:
                for key in r.scan_iter(f"{PREFIX_PROGRESS}*", count=100):
                    k = key.decode() if isinstance(key, bytes) else key
                    tid = k[len(PREFIX_PROGRESS):]
                    if tid.startswith(prefix):
                        return tid
        except Exception:
            pass
        return None

    def register_callback(self, task_id: str, callback: Callable):
        self.callbacks[task_id] = callback

    def unregister_callback(self, task_id: str):
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
            logger.error("Error executing callback for task %s: %s", task_id, e)

    def get_all_tasks(self) -> dict[str, TaskProgress]:
        return self.tasks

    def cleanup_old_tasks(self, max_age_hours: int = 24):
        current_time = time.time()
        tasks_to_remove = []
        for task_id, task in self.tasks.items():
            if task.end_time and (current_time - task.end_time) > (max_age_hours * 3600):
                tasks_to_remove.append(task_id)
        for task_id in tasks_to_remove:
            self.remove_task(task_id)
        return len(tasks_to_remove)

    async def watchdog_stale_tasks(self, max_stale_seconds: int = 1800) -> list[str]:
        now = time.time()
        failed: list[str] = []
        for task_id in list(self.tasks):
            task = self.tasks[task_id]
            if task.status in ("completed", "failed", "cancelled"):
                continue
            last = task._last_update or task.start_time
            if last is None:
                continue
            if now - last <= max_stale_seconds:
                continue
            await self.fail_task(task_id, f"auto-failed by watchdog: no progress for {max_stale_seconds // 60} min")
            failed.append(task_id)
        return failed


progress_tracker = ProgressTracker()


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _build_progress_bar(percentage: float, segments: int = 20) -> str:
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


async def send_progress_update(chat_id: int, bot, task: TaskProgress, message_id: int | None = None):
    try:
        await telegram_api_limiter.wait_if_needed(str(getattr(task, "user_id", 0) or 0))
    except Exception:
        pass
    try:
        total_progress = task.progress_percentage
        bar = _build_progress_bar(total_progress)
        processed = _format_size(task.processed_size)
        total = _format_size(task.total_size)
        status_emojis = {"pending": "\u23f3", "downloading": "\U0001f4e5", "processing": "\u2699\ufe0f", "uploading": "\U0001f4e4", "completed": "\u2705", "failed": "\u274c"}
        status_emoji = status_emojis.get(task.status, "\u2753")
        speed_str = ""
        if task.start_time and task.processed_size > 0:
            elapsed = task.elapsed_time
            if elapsed > 0:
                bytes_per_sec = task.processed_size / elapsed
                speed_str = f"\U0001f680 Speed: {_format_size(int(bytes_per_sec))}/s\n"
        message_text = (
            f"\U0001f4ca **File Processing Progress**\n\n"
            f"\U0001f4c1 File: `{safe_code_span(task.file_name)}`\n"
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
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, parse_mode="Markdown")
        else:
            msg = await bot.send_message(chat_id=chat_id, text=message_text, parse_mode="Markdown")
            return msg.message_id
    except Exception as e:
        err_msg = str(e).lower()
        if "message to edit not found" in err_msg or "message not found" in err_msg or "message can't be edited" in err_msg:
            logger.debug("Progress update edit skipped (message gone): %s", e)
        else:
            logger.error("Error sending progress update: %s", e)
        return None
