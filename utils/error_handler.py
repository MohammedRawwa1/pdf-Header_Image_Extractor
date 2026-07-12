# utils/error_handler.py
"""
Comprehensive error handling and logging system for the PDF header extractor bot.

Provides:
- Error categorization (timeout, file_too_large, network_error, etc.)
- User-friendly error messages for Telegram responses
- In-memory error log with size limit
- Async error handler decorator for automatic logging
"""

import asyncio
import logging
import traceback
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class BotErrorHandler:
    """Centralized error handling for the bot with categorized user messages."""

    # User-friendly messages mapped by error category
    ERROR_MESSAGES = {
        "timeout": (
            "\u23f1\ufe0f Operation took too long. "
            "Please try a smaller file or check your connection."
        ),
        "file_not_found": (
            "\U0001f4c1 File not found. "
            "The file may have been deleted or the path is incorrect."
        ),
        "file_too_large": (
            "\U0001f4e6 File is too large. "
            "Maximum file size is 50 MB via the Bot API. "
            "Use a userbot account for larger files."
        ),
        "invalid_format": (
            "\u274c Invalid file format. "
            "Supported formats: PDF, JPEG, PNG, WEBP."
        ),
        "pdf_error": (
            "\U0001f4d5 PDF processing failed. "
            "The file may be corrupted or password-protected."
        ),
        "thumbnail_failed": (
            "\U0001f5bc\ufe0f Thumbnail generation failed. "
            "Could not extract a cover image from this file."
        ),
        "disk_full": (
            "\U0001f4be Not enough disk space. "
            "Please free up space and try again."
        ),
        "permission_denied": (
            "\U0001f510 Permission denied. "
            "Cannot access or create file in that location."
        ),
        "network_error": (
            "\U0001f310 Network error. "
            "Please check your connection and try again."
        ),
        "download_failed": (
            "\U0001f4e5 Download failed. "
            "Could not download the file from Telegram. "
            "It may be too large or the server is busy."
        ),
        "upload_failed": (
            "\U0001f4e4 Upload failed. "
            "Could not send the result back to Telegram."
        ),
        "userbot_error": (
            "\U0001f916 Userbot error. "
            "The user account client encountered an issue. "
            "Check API_ID/API_HASH and session validity."
        ),
        "rate_limited": (
            "\u23f3 Too many requests. "
            "Please wait a moment before sending more files."
        ),
        "compression_failed": (
            "\U0001f9be PDF compression failed. "
            "Ghostscript may not be installed on the server."
        ),
        "cancelled": "\u274c Operation was cancelled.",
        "internal_error": (
            "\U0001f622 An unexpected error occurred. "
            "Please try again later. If the problem persists, contact the bot owner."
        ),
    }

    def __init__(self):
        self.error_log: list = []
        self.max_log_size = 1000

    @staticmethod
    def categorize_error(exception: Exception, context: Optional[str] = None) -> str:
        """Categorize an exception into a known error type."""
        exc_str = str(exception).lower()
        ctx = (context or "").lower()

        if "timeout" in exc_str or "timeout" in ctx:
            return "timeout"
        if "file not found" in exc_str or "no such file" in exc_str:
            return "file_not_found"
        if "too large" in exc_str or "file size" in exc_str:
            return "file_too_large"
        if "network" in exc_str or "connection" in exc_str or "econnrefused" in exc_str:
            return "network_error"
        if "disk" in exc_str or "space" in exc_str or "disk full" in exc_str:
            return "disk_full"
        if "permission" in exc_str:
            return "permission_denied"
        if "pdf" in exc_str or "pymupdf" in exc_str or "fitz" in exc_str:
            return "pdf_error"
        if "thumbnail" in exc_str or "thumb" in exc_str:
            return "thumbnail_failed"
        if "download" in exc_str:
            return "download_failed"
        if "upload" in exc_str:
            return "upload_failed"
        if "userbot" in ctx or "telethon" in exc_str or "pyrogram" in exc_str:
            return "userbot_error"
        if "ratelimit" in exc_str or "429" in exc_str or "flood" in exc_str:
            return "rate_limited"
        if "compress" in exc_str or "ghostscript" in exc_str:
            return "compression_failed"
        if "format" in exc_str or "mime" in exc_str:
            return "invalid_format"
        if "cancel" in exc_str:
            return "cancelled"
        return "internal_error"

    @staticmethod
    def get_user_friendly_message(error_category: str) -> str:
        """Get a user-friendly message for a given error category."""
        return BotErrorHandler.ERROR_MESSAGES.get(
            error_category,
            BotErrorHandler.ERROR_MESSAGES["internal_error"],
        )

    def log_error(
        self,
        exception: Exception,
        context: str,
        severity: str = "error",
        user_id: Optional[int] = None,
        additional_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Log an error with full context and return a structured error entry.

        Args:
            exception: The exception that occurred.
            context: A description of what was happening.
            severity: One of 'critical', 'error', 'warning', 'info', 'debug'.
            user_id: Optional Telegram user ID who triggered the error.
            additional_info: Optional dict with extra context.

        Returns:
            Dict with error details including a 'user_message' key.
        """
        timestamp = datetime.now().isoformat()
        error_category = self.categorize_error(exception, context)
        user_message = self.get_user_friendly_message(error_category)

        error_entry = {
            "timestamp": timestamp,
            "severity": severity,
            "category": error_category,
            "context": context,
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "traceback": traceback.format_exc(),
            "user_id": user_id,
            "additional_info": additional_info or {},
            "user_message": user_message,
        }

        # Log with appropriate severity
        log_msg = f"[{context}] {error_entry['exception_type']}: {error_entry['exception_message']}"
        if severity == "critical":
            logger.critical(log_msg)
        elif severity == "error":
            logger.error(log_msg)
        elif severity == "warning":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # Keep in-memory log with size limit
        self.error_log.append(error_entry)
        if len(self.error_log) > self.max_log_size:
            self.error_log = self.error_log[-self.max_log_size:]

        return error_entry

    def get_error_report(self, limit: int = 50) -> str:
        """Get a formatted report of recent errors for debugging."""
        if not self.error_log:
            return "No errors recorded."

        recent = self.error_log[-limit:]
        report = f"Recent Errors ({len(recent)} total):\n"
        report += "=" * 60 + "\n"
        for i, entry in enumerate(recent, 1):
            report += f"\n{i}. [{entry['timestamp']}] {entry['context']}\n"
            report += f"   Type: {entry['exception_type']}\n"
            report += f"   Category: {entry['category']}\n"
            report += f"   Message: {entry['exception_message']}\n"
            if entry["user_id"]:
                report += f"   User: {entry['user_id']}\n"
        return report


# Global error handler singleton
_error_handler = BotErrorHandler()


def get_error_handler() -> BotErrorHandler:
    """Return the global BotErrorHandler instance."""
    return _error_handler


async def handle_bot_error(
    exception: Exception,
    context: str,
    update=None,
    user_id: Optional[int] = None,
    send_user_message=None,
) -> Dict[str, Any]:
    """Handle an error: log it and optionally notify the user.

    Args:
        exception: The exception that occurred.
        context: Description of what was happening.
        update: Telegram Update object (optional, used to extract user_id).
        user_id: Telegram user ID (optional, overrides extraction from update).
        send_user_message: Async callable that takes a string message.

    Returns:
        Error information dict including a 'user_message' key.
    """
    handler = get_error_handler()

    # Try to get user_id from update if not provided
    if not user_id and update and hasattr(update, "effective_user") and update.effective_user:
        user_id = update.effective_user.id

    # Log the error
    error_info = handler.log_error(
        exception,
        context,
        severity="error",
        user_id=user_id,
        additional_info={"update_type": type(update).__name__ if update else None},
    )

    # Notify user if callback provided
    if send_user_message:
        try:
            await send_user_message(error_info["user_message"])
        except Exception as e:
            logger.error("Failed to send error message to user %s: %s", user_id, e)

    return error_info


def async_error_handler(
    context: str,
    send_user_message_callback: Optional[Callable] = None,
    re_raise: bool = False,
):
    """Decorator for async functions to handle errors gracefully.

    Args:
        context: Description of the operation for logging.
        send_user_message_callback: Optional async callable(user_message).
        re_raise: If True, re-raises the exception after logging.

    Usage:
        @async_error_handler(context="PDF Processing")
        async def process_pdf(...):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                logger.warning("%s was cancelled", context)
                if re_raise:
                    raise
                return False, "Operation was cancelled"
            except Exception as e:
                update = None
                user_id = None
                for arg in args:
                    if hasattr(arg, "effective_user"):
                        update = arg
                        user_id = arg.effective_user.id
                        break

                error_info = await handle_bot_error(
                    e, context, update=update, user_id=user_id,
                    send_user_message=send_user_message_callback,
                )

                if re_raise:
                    raise
                return False, error_info["user_message"]

        return wrapper

    return decorator
