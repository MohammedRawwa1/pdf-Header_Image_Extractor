"""Big PDF files pipeline: userbot download -> S3 upload -> Redis queue -> Worker -> userbot delivery.

Handles PDFs that exceed the Telegram Bot API DOWNLOAD limit (20MB via
``getFile``) by routing them through a userbot-based download, S3 storage,
and worker processing pipeline.

Adapted from media_conersion_bot for PDF-only use (no video/FFmpeg).
"""

import asyncio
import logging
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default thresholds
# Memory-streaming threshold: files larger than this are downloaded to temp
# disk (disk-based path, Telethon-first) instead of being held in RAM.  This
# is a MEMORY strategy knob, not the Bot API limit — the routing limit is
# config.BOT_API_DOWNLOAD_LIMIT_BYTES (20MB via getFile).
DEFAULT_BOT_API_MAX_MB = int(os.getenv("BOT_API_MAX_MB", "50"))
DEFAULT_BOT_API_MAX_BYTES = DEFAULT_BOT_API_MAX_MB * 1024 * 1024

# Files up to this size get streamed through memory instead of temp disk.
# Default is 200MB — safe for Railway free tier (512MB RAM).
# Increase via BIGFILE_IN_MEMORY_MAX_MB (e.g., 1024 for 1GB) if your server has more RAM.
# Files above this threshold fall back to disk-based download → S3 upload.
#
# Memory usage during in-memory streaming: downloaded_bytes * 2 (buffer + S3 upload),
# so a 200MB file uses ~400MB RAM.  Stay under ~40% of total RAM to avoid OOM.
IN_MEMORY_MAX_BYTES = (
    int(os.getenv("BIGFILE_IN_MEMORY_MAX_MB", "200")) * 1024 * 1024
)

try:
    from storage import get_storage_backend
except Exception:
    get_storage_backend = None

try:
    from utils.cache import get_cache
except Exception:
    get_cache = None


@dataclass
class IngestResult:
    """Result of a big file ingestion attempt."""

    ok: bool
    job_id: str | None = None
    s3_key: str | None = None
    error: str | None = None


class BigFilePipeline:
    """Orchestrates the large PDF file ingestion pipeline."""

    def __init__(self):
        self._storage = None
        self._cache = None
        self._init_lock = asyncio.Lock()

    async def _ensure_initialized(self):
        """Lazy-init storage and cache backends."""
        if self._storage is not None:
            return
        async with self._init_lock:
            if self._storage is not None:
                return
            try:
                if get_storage_backend is not None:
                    self._storage = await get_storage_backend()
            except Exception as e:
                logger.warning("BigFilePipeline: storage init failed: %s", e)
            try:
                if get_cache is not None:
                    self._cache = await get_cache()
            except Exception:  # nosec B110
                pass

    async def ingest_large_file(
        self,
        chat_id: int,
        message_id: int,
        file_size: int,
        file_unique_id: str | None = None,
        user_id: int | None = None,
        original_filename: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> IngestResult:
        """Download a large PDF file via userbot, upload to S3, enqueue a processing job.

        Args:
            chat_id: Telegram chat ID where the file was sent.
            message_id: Telegram message ID of the file.
            file_size: Size of the file in bytes.
            file_unique_id: Telegram file_unique_id for caching/dedup.
            user_id: User who sent the file.
            original_filename: Original filename if known.
            progress_callback: Optional callable(current_bytes, total_bytes, phase)
                where phase is "download" or "s3_upload".  Used for LIVE progress
                on both the userbot download and the S3 upload legs of the pipeline.

        Returns:
            IngestResult with job_id and s3_key on success.
        """
        await self._ensure_initialized()

        # Phase-aware wrappers so callers can label the live progress message
        # correctly for the download vs the S3-upload leg.
        _dl_cb = (
            (lambda recv, total: progress_callback(recv, total, "download"))
            if progress_callback is not None
            else None
        )
        _upload_cb = (
            (lambda recv, total: progress_callback(recv, total, "s3_upload"))
            if progress_callback is not None
            else None
        )

        # The pipeline requires S3 storage: the worker (separate process) only
        # knows how to fetch the input via `input_key`. Without storage, the
        # previously-enqueued job could never be downloaded by the worker, so
        # fail fast with a clear error instead of enqueuing a doomed job.
        if self._storage is None:
            return IngestResult(
                ok=False,
                error="BigFilePipeline requires S3 storage (S3_BUCKET/credentials) to be configured.",
            )

        job_id = uuid.uuid4().hex
        input_s3_key = f"inputs/{job_id}/source"

        ext = ""
        if original_filename:
            _, ext = os.path.splitext(original_filename)
        # Whitelist the extension so a hostile filename can't smuggle path
        # separators into the temp path below (e.g. `..\evil`).
        if not ext or len(ext) > 12 or not re.fullmatch(r"\.[A-Za-z0-9]+", ext):
            ext = ".pdf"

        actual_size = 0
        s3_key = input_s3_key
        _in_memory_success = False

        # Try in-memory streaming for files larger than the disk/memory cutoff
        # (default 50MB) when S3 is available; smaller pipeline files use the
        # proven disk-based download path instead.
        _use_in_memory = (
            self._storage is not None
            and file_size > DEFAULT_BOT_API_MAX_BYTES
            and file_size <= IN_MEMORY_MAX_BYTES
        )

        if _use_in_memory:
            try:
                from utils.userbot_downloader import download_bytes_via_userbot

                logger.info(
                    "BigFilePipeline: in-memory download chat=%s msg=%s size=%dMB",
                    chat_id,
                    message_id,
                    file_size // (1024 * 1024),
                )
                data = await download_bytes_via_userbot(
                    chat_id,
                    message_id,
                    progress_callback=_dl_cb,
                    user_id=user_id,
                )
                if data is not None and len(data) > 0:
                    actual_size = len(data)
                    await self._storage.upload_bytes(
                        data, s3_key, progress_callback=_upload_cb
                    )
                    logger.info(
                        "BigFilePipeline: S3 upload via bytes complete"
                    )
                    _in_memory_success = True

                    if self._cache and file_unique_id:
                        try:
                            await self._cache.cache_file_info(
                                file_unique_id,
                                {
                                    "job_id": job_id,
                                    "size": actual_size,
                                    "path": s3_key,
                                    "chat_id": chat_id,
                                    "message_id": message_id,
                                },
                                ttl=86400,
                            )
                        except Exception:  # nosec B110
                            pass
            except Exception as e:
                logger.warning(
                    "BigFilePipeline: in-memory path failed (%s); falling back to disk-based download",
                    e,
                )

        if not _in_memory_success:
            try:
                temp_dir = os.path.join(
                    os.getenv("STORAGE_PATH", "storage"), "temp"
                )
                os.makedirs(temp_dir, exist_ok=True)

                temp_path = os.path.join(temp_dir, f"{job_id}_src{ext}")
                logger.info(
                    "BigFilePipeline: downloading via userbot chat=%s msg=%s size=%dMB -> %s",
                    chat_id,
                    message_id,
                    file_size // (1024 * 1024),
                    temp_path,
                )

                from utils.userbot_downloader import (
                    download_forward_via_userbot,
                )

                download_ok = await download_forward_via_userbot(
                    chat_id,
                    message_id,
                    temp_path,
                    progress_callback=_dl_cb,
                    user_id=user_id,
                )
                if (
                    not download_ok
                    or not os.path.exists(temp_path)
                    or os.path.getsize(temp_path) == 0
                ):
                    return IngestResult(
                        ok=False, error="Userbot download failed"
                    )

                actual_size = os.path.getsize(temp_path)

                if self._cache and file_unique_id:
                    try:
                        await self._cache.cache_file_info(
                            file_unique_id,
                            {
                                "job_id": job_id,
                                "size": actual_size,
                                "path": input_s3_key
                                if self._storage is not None
                                else temp_path,
                                "chat_id": chat_id,
                                "message_id": message_id,
                            },
                            ttl=86400,
                        )
                    except Exception:  # nosec B110
                        pass

            except Exception as e:
                logger.exception(
                    "BigFilePipeline: userbot download error: %s", e
                )
                return IngestResult(
                    ok=False,
                    error="Userbot download failed. Check server logs for details.",
                )

            # Upload to S3
            try:
                if self._storage is not None:
                    await self._storage.upload_file(
                        temp_path, s3_key, progress_callback=_upload_cb
                    )
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as cleanup_err:
                        logger.warning(
                            "BigFilePipeline: failed to clean up temp file %s: %s",
                            temp_path,
                            cleanup_err,
                        )
                else:
                    s3_key = temp_path
            except Exception as e:
                logger.exception("BigFilePipeline: S3 upload failed: %s", e)
                s3_key = temp_path

        # Enqueue processing job
        try:
            from utils.job_queue import enqueue_job

            job = {
                "job_id": job_id,
                "input_key": s3_key if self._storage is not None else None,
                "input_path": s3_key if self._storage is None else None,
                "chat_id": chat_id,
                "user_id": user_id,
                "message_id": message_id,
                "original_filename": original_filename
                or f"file_{job_id}{ext}",
                "file_unique_id": file_unique_id,
                "file_size": actual_size,
                "progress_channel": f"pdf:progress:{job_id}",
                "cleanup_input": True,
                "type": "pdf_extract",
                "created_at": time.time(),
            }

            await enqueue_job(job)
            logger.info(
                "BigFilePipeline: job %s enqueued (input_key=%s)",
                job_id,
                s3_key,
            )

            return IngestResult(ok=True, job_id=job_id, s3_key=s3_key)

        except Exception as e:
            logger.exception("BigFilePipeline: enqueue failed: %s", e)
            return IngestResult(
                ok=False,
                error="Enqueue failed. Check server logs for details.",
            )
