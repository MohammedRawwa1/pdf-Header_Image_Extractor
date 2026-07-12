# utils/progress_tracker.py
"""Progress tracker for PDF download/upload operations with visual progress bar."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class TaskProgress:
    """Track progress of a PDF processing task."""

    task_id: str
    user_id: int
    file_name: str
    total_size: int
    processed_size: int = 0
    status: str = "pending"  # pending, downloading, processing, uploading, completed, failed
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    _last_update: Optional[float] = None

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

    def to_dict(self) -> Dict:
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
    """Manage multiple task progress trackers."""

    def __init__(self):
        self.tasks: Dict[str, TaskProgress] = {}
        self.callbacks: Dict[str, Callable] = {}

    def create_task(self, task_id: str, user_id: int, file_name: str, total_size: int) -> TaskProgress:
        task = TaskProgress(task_id=task_id, user_id=user_id, file_name=file_name, total_size=total_size)
        self.tasks[task_id] = task
        logger.info("Created task tracker: %s", task_id)
        return task

    def get_task(self, task_id: str) -> Optional[TaskProgress]:
        return self.tasks.get(task_id)

    async def update_task_progress(self, task_id: str, processed_size: int):
        task = self.tasks.get(task_id)
        if task:
            task.update_progress(processed_size)
            await self._notify_callbacks(task_id, task)

    def start_task(self, task_id: str):
        task = self.tasks.get(task_id)
        if task:
            task.start()
            logger.info("Started task: %s", task_id)

    async def complete_task(self, task_id: str):
        task = self.tasks.get(task_id)
        if task:
            task.complete()
            logger.info("Completed task: %s", task_id)
            await self._notify_callbacks(task_id, task)

    async def fail_task(self, task_id: str, error_message: str):
        task = self.tasks.get(task_id)
        if task:
            task.fail(error_message)
            logger.error("Task failed: %s - %s", task_id, error_message)
            await self._notify_callbacks(task_id, task)

    def remove_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
            logger.info("Removed task: %s", task_id)

    def register_callback(self, task_id: str, callback: Callable):
        self.callbacks[task_id] = callback

    async def _notify_callbacks(self, task_id: str, task: TaskProgress):
        callback = self.callbacks.get(task_id)
        if not callback:
            return
        try:
            import inspect
            if inspect.iscoroutinefunction(callback):
                await callback(task)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, callback, task)
        except Exception as e:
            logger.error("Error executing callback for task %s: %s", task_id, e)

    def get_all_tasks(self) -> Dict[str, TaskProgress]:
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


async def send_progress_update(chat_id: int, bot, task: TaskProgress, message_id: Optional[int] = None):
    """Send or update progress message with visual progress bar."""
    try:
        filled = int(task.progress_percentage / 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)

        processed = _format_size(task.processed_size)
        total = _format_size(task.total_size)

        status_emoji = {
            "pending": "⏳",
            "downloading": "📥",
            "processing": "⚙️",
            "uploading": "📤",
            "completed": "✅",
            "failed": "❌",
        }.get(task.status, "❓")

        message_text = (
            f"📊 **PDF Processing Progress**\n\n"
            f"📁 File: `{task.file_name}`\n"
            f"📏 Size: {processed} / {total}\n"
            f"📈 Progress: {task.progress_percentage:.1f}%\n"
            f"{bar}\n\n"
            f"⏱️ Elapsed: {_format_time(task.elapsed_time)}\n"
            f"⏳ Remaining: {_format_time(task.estimated_time_remaining)}\n"
            f"{status_emoji} Status: {task.status.title()}\n\n"
            f"🆔 Task: `{task.task_id[:8]}`"
        )

        if message_id:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, parse_mode="Markdown")
        else:
            msg = await bot.send_message(chat_id=chat_id, text=message_text, parse_mode="Markdown")
            return msg.message_id

    except Exception as e:
        logger.error("Error sending progress update: %s", e)
        return None
