"""Periodic cleanup manager for temp files, S3 objects, and stale Redis keys.

Adapted from the media_conersion_bot reference implementation.
Runs as a background asyncio task that wakes every hour and cleans:
  1. Old temp / input / output files from storage directories
  2. Old S3 objects via storage.purge_objects_older_than
  3. Stale io:in / io:out keys from Redis
  4. Stale ``processed:*`` records (cached re-send results whose Telegram
     file_id has likely expired — see utils/processed_cache.py)
"""

import asyncio
import json
import logging
import os
import time

logger = logging.getLogger(__name__)

try:
    import config
except Exception:
    config = None

try:
    from utils.processed_cache import PROCESSED_TTL as _DEFAULT_PROCESSED_TTL
except Exception:  # nosec B110 - fall back to 30 days
    _DEFAULT_PROCESSED_TTL = 30 * 24 * 3600


class CleanupManager:
    """Manages periodic cleanup of temporary files and stale data."""

    def __init__(self):
        self.cleanup_interval = int(
            os.getenv("CLEANUP_INTERVAL", "3600")
        )  # 1 hour
        self.max_file_age = int(
            os.getenv("CLEANUP_MAX_FILE_AGE", "86400")
        )  # 24 hours
        self.max_temp_age = int(
            os.getenv("CLEANUP_MAX_TEMP_AGE", "3600")
        )  # 1 hour
        self.s3_ttl = int(os.getenv("CLEANUP_S3_TTL", "86400"))  # 24 hours
        self.io_ttl = int(os.getenv("CLEANUP_IO_TTL", "604800"))  # 7 days
        # Cached re-send records (utils/processed_cache.py) are actively
        # pruned once their last write is older than this, instead of only
        # relying on Redis TTL expiry — a record that old is keeping a
        # Telegram file_id that is likely expired, so it would only produce
        # failed re-sends.  Defaults to the cache's own TTL so both stay in
        # sync from one place.
        self.processed_ttl = int(
            os.getenv("CLEANUP_PROCESSED_TTL", str(_DEFAULT_PROCESSED_TTL))
        )
        self.is_running = False

    async def start(self):
        """Start periodic cleanup loop."""
        self.is_running = True
        logger.info(
            "CleanupManager started (interval=%ds, file_age=%ds, temp_age=%ds, "
            "s3_ttl=%ds, processed_ttl=%ds)",
            self.cleanup_interval,
            self.max_file_age,
            self.max_temp_age,
            self.s3_ttl,
            self.processed_ttl,
        )

        while self.is_running:
            try:
                await self.cleanup_all()
            except Exception as e:
                logger.error("Cleanup error: %s", e)
            await asyncio.sleep(self.cleanup_interval)

    def stop(self):
        """Stop cleanup loop."""
        self.is_running = False
        logger.info("CleanupManager stopped")

    async def cleanup_all(self) -> dict:
        """Run all cleanup operations and return counts."""
        results = {
            "temp_files": await self._cleanup_directory(
                getattr(config, "TEMP_PATH", "storage/temp")
                if config
                else "storage/temp",
                self.max_temp_age,
            ),
            "input_files": await self._cleanup_directory(
                getattr(config, "INPUT_PATH", "storage/input")
                if config
                else "storage/input",
                self.max_file_age,
            ),
            "output_files": await self._cleanup_directory(
                getattr(config, "OUTPUT_PATH", "storage/output")
                if config
                else "storage/output",
                self.max_file_age,
            ),
            "thumbnails": await self._cleanup_directory(
                getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
                if config
                else "storage/thumbnails",
                self.max_file_age,
            ),
            "s3_objects": await self._cleanup_s3(),
            "redis_io_keys": await self._cleanup_redis_io_keys(),
            "redis_processed_keys": await self._cleanup_redis_processed_keys(),
        }

        total = sum(results.values())
        if total > 0:
            logger.info("Cleanup completed: %s (total=%d)", results, total)

        return results

    async def _cleanup_directory(self, directory: str, max_age: int) -> int:
        """Remove files older than ``max_age`` seconds from ``directory``."""
        try:
            if not directory or not os.path.exists(directory):
                return 0

            now = time.time()
            removed = 0

            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)

                if os.path.isfile(item_path):
                    age = now - os.path.getmtime(item_path)
                    if age > max_age:
                        try:
                            os.remove(item_path)
                            removed += 1
                        except Exception as e:
                            logger.debug(
                                "Cleanup: failed to remove %s: %s",
                                item_path,
                                e,
                            )

                elif os.path.isdir(item_path):
                    sub = await self._cleanup_directory(item_path, max_age)
                    removed += sub
                    # Remove empty subdirectory
                    try:
                        if not os.listdir(item_path):
                            os.rmdir(item_path)
                            removed += 1
                    except Exception:  # nosec B110
                        pass

            return removed

        except Exception as e:
            logger.error("Cleanup: error in directory %s: %s", directory, e)
            return 0

    async def _cleanup_s3(self) -> int:
        """Purge old S3 objects via ``storage.purge_objects_older_than``."""
        try:
            from storage import purge_objects_older_than

            loop = asyncio.get_running_loop()
            deleted = await loop.run_in_executor(
                None, purge_objects_older_than, self.s3_ttl, "pdf-bot/"
            )
            if deleted and deleted > 0:
                logger.info("Cleanup: purged %d old S3 objects", deleted)
            return deleted or 0
        except Exception:
            return 0

    async def _cleanup_redis_io_keys(self) -> int:
        """Remove stale ``io:in:*`` and ``io:out:*`` keys from Redis."""
        try:
            if not getattr(config, "REDIS_URL", None):
                return 0

            import redis as _redis

            r = _redis.from_url(config.REDIS_URL)
            removed = 0

            for pattern in ("io:in:*", "io:out:*"):
                keys = r.keys(pattern)
                for key in keys:
                    try:
                        ttl = r.ttl(key)
                        if ttl == -1:  # No expiry — set one
                            r.expire(key, self.io_ttl)
                            removed += 1
                        elif ttl == -2:  # Already gone
                            pass
                    except Exception:  # nosec B110
                        pass

            return removed
        except Exception:
            return 0

    async def _cleanup_redis_processed_keys(self) -> int:
        """Prune stale ``processed:*`` cached re-send records from Redis.

        Each record (see utils/processed_cache.py) stores a delivered copy's
        Telegram file_id.  Records whose last write (``meta.at``) is older
        than ``self.processed_ttl`` are deleted outright — the stored file_id
        is long past its useful life, so a cached re-send would only fail.
        Younger records that somehow lack a Redis TTL get one as a safety
        net.  Returns the number of keys removed or TTL-fixed.
        """
        try:
            from utils.redis_client import get_sync_redis

            r = get_sync_redis()
            if not r:
                return 0

            now = time.time()
            handled = 0
            keys = r.keys("processed:*")
            for key in keys:
                try:
                    ttl = r.ttl(key)
                    if ttl == -2:  # Already gone
                        continue
                    # Last-write timestamp recorded by upsert_processed_record.
                    _at = None
                    _raw = r.hget(key, "meta")
                    if _raw:
                        try:
                            _meta = json.loads(_raw)
                            _at = (
                                _meta.get("at") if isinstance(_meta, dict) else None
                            )
                        except Exception:  # nosec B110 - unreadable meta
                            _at = None
                    if _at is not None and now - float(_at) > self.processed_ttl:
                        # Stale: prune even if it still has a TTL left.
                        r.delete(key)
                        handled += 1
                        logger.debug(
                            "Cleanup: pruned stale processed record %s", key
                        )
                    elif ttl == -1:  # Young but no expiry — set one
                        r.expire(key, self.processed_ttl)
                        handled += 1
                except Exception:  # nosec B110 - best-effort per key
                    pass

            return handled
        except Exception:
            return 0

    async def force_cleanup(self, directory: str = None) -> int:
        """Force-clean a specific directory or all locations."""
        if directory and os.path.exists(directory):
            return await self._cleanup_directory(
                directory, 0
            )  # Remove everything
        results = await self.cleanup_all()
        return sum(results.values())

    async def get_storage_stats(self) -> dict:
        """Return storage usage stats (size_mb, file_count) per directory."""
        stats = {}
        for name, path_key in (
            ("temp", "TEMP_PATH"),
            ("input", "INPUT_PATH"),
            ("output", "OUTPUT_PATH"),
            ("thumbnails", "THUMBNAIL_PATH"),
        ):
            directory = (
                getattr(config, path_key, f"storage/{name}")
                if config
                else f"storage/{name}"
            )
            size_bytes = 0
            file_count = 0
            try:
                if os.path.exists(directory):
                    for root, _dirs, files in os.walk(directory):
                        for f in files:
                            fp = os.path.join(root, f)
                            if os.path.isfile(fp):
                                size_bytes += os.path.getsize(fp)
                                file_count += 1
            except Exception:  # nosec B110
                pass
            stats[name] = {
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "files": file_count,
            }

        stats["total"] = {
            "size_mb": round(sum(d["size_mb"] for d in stats.values()), 2),
            "files": sum(d["files"] for d in stats.values()),
        }
        return stats


# Singleton instance
cleanup_manager = CleanupManager()
