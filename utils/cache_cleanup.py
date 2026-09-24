"""Admin ``/clear_cache`` support: wipe Redis caches and cached files.

Everything is split into three buckets so the admin command can never
disturb live work:

1. **Pure caches** (deleted unconditionally) — the re-send/dedup records
   (``processed:*``, ``pfuid:*``, ``pdfcheck:*``), the generic cache layer
   (``cache:job:*``, ``cache:file:*``, ``cache:user:*``, ``cache:meta:*``,
   ``cache:resp:*``), pending action-menu records (``ctxfile:*``) and
   forward-batch state (``forward_batch:*``).  Dropping these only costs a
   re-download/re-process on the next identical file — never a failure.

2. **Per-job bookkeeping** (deleted only when the job is NOT live) —
   ``progress:*``, ``io:in:*``, ``io:out:*``, ``cancel:*``, ``pdf:job:*``,
   ``pdf:progress:*``, ``queued_msg:*``.  Keys whose job id is currently
   queued or running (read from the RQ queues/registries and the BigFile
   pipeline queues) are preserved so an active upload/conversion is never
   disturbed.

3. **On-disk files** (temp/input/output/thumbnails) — deleted only when
   older than a short grace window, so files an in-flight job is actively
   writing are left alone.

Queued & in-flight jobs themselves (``rq:*``, ``pdf:jobs``,
``pdf:delayed``, S3 ``inputs/``) are intentionally NOT touched — use
``scripts/clear_jobs.py --all`` for a full reset.
"""

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

# Bucket 1: pure caches — safe to delete unconditionally.
CACHE_PREFIXES = (
    "processed:",  # re-send/dedup records (30-day TTL)
    "pfuid:",  # file_unique_id -> content_hash index (30-day)
    "pdfcheck:",  # PDF validator results (7-day)
    "cache:job:",  # generic job-metadata cache
    "cache:file:",  # generic file-info cache (+ cached file bytes)
    "cache:user:",  # user-session cache
    "cache:meta:",  # media-analysis cache
    "cache:resp:",  # bot-response cache
    "ctxfile:",  # pending action-menu records
    "forward_batch:",  # batch-collection state
)

# Bucket 2: per-job bookkeeping — deleted ONLY when the job id is not live.
JOB_KEY_PREFIXES = (
    "progress:",
    "io:in:",
    "io:out:",
    "cancel:",
    "pdf:job:",
    "pdf:progress:",
    "queued_msg:",
)

# On-disk storage directories cleared by /clear_cache: name -> config attr.
STORAGE_DIR_ATTRS = (
    ("thumbnails", "THUMBNAIL_PATH"),
    ("temp", "TEMP_PATH"),
    ("input", "INPUT_PATH"),
    ("output", "OUTPUT_PATH"),
)

# Terminal statuses written to a ``pdf:job:<id>`` hash when a BigFile
# pipeline job finishes (see tasks._mark_pipeline_hash_status).  A hash in
# any OTHER state (or with no status yet) means the job is queued or still
# being processed — its bookkeeping keys must be preserved.
PIPELINE_TERMINAL_STATUSES = frozenset(
    {
        "done",
        "s3_fallback",
        "too_large",
        "cancelled",
        "failed",
        "error",
        "already_processed",
    }
)

# Default grace window for on-disk files: anything modified within this is
# assumed to be in use by a live job and is skipped.
DEFAULT_FILE_GRACE_SECONDS = 600.0


def _decode(value) -> str:
    """Decode a possibly-bytes Redis value to str."""
    return (
        value.decode("utf-8", "replace")
        if isinstance(value, bytes)
        else str(value)
    )


def _pipe_queue_items(r, key: str) -> list[dict]:
    """Read JSON job dicts from a BigFile pipeline queue key (list or zset)."""
    try:
        _type = _decode(r.type(key))
    except Exception:
        return []
    try:
        if _type == "list":
            items = r.lrange(key, 0, -1)
        elif _type == "zset":
            items = r.zrange(key, 0, -1)
        else:
            return []
    except Exception:
        return []
    out: list[dict] = []
    for raw in items:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                out.append(data)
        except Exception:
            continue
    return out


def collect_live_job_ids(r) -> set[str]:
    """Return job ids that are currently queued or running.

    Reads both background pipes: RQ (queue lists + started/wip/deferred/
    scheduled registries) and the BigFile pipeline (``pdf:jobs`` /
    ``pdf:delayed``).  Best-effort: any read failure degrades to an empty
    set, which only means bookkeeping keys get deleted (the jobs themselves
    are never touched).
    """
    live: set[str] = set()
    try:
        # RQ queue lists (raw job-id members).
        for key in r.keys("rq:queue:*"):
            for jid in r.lrange(key, 0, -1):
                live.add(_decode(jid))
        # RQ registries (job ids are zset members).
        for pattern in (
            "rq:wip:*",
            "rq:started:*",
            "rq:deferred:*",
            "rq:scheduled:*",
        ):
            for key in r.keys(pattern):
                for jid in r.zrange(key, 0, -1):
                    live.add(_decode(jid))
    except Exception:
        logger.debug("collect_live_job_ids: RQ pass failed", exc_info=True)
    try:
        # BigFile pipeline queues store JSON job dicts with a job_id field.
        for key in ("pdf:jobs", "pdf:delayed"):
            for item in _pipe_queue_items(r, key):
                _jid = item.get("job_id")
                if _jid:
                    live.add(str(_jid))
    except Exception:
        logger.debug(
            "collect_live_job_ids: pipeline pass failed", exc_info=True
        )
    # In-flight BigFile pipeline jobs: the pipeline worker ``brpop``s jobs
    # off ``pdf:jobs`` the moment it starts, so a processing job exists ONLY
    # as its ``pdf:job:<id>`` hash (24h TTL, status "queued" until it
    # finishes).  Protect ids whose hash status is not terminal — a terminal
    # status means the job is done and its leftovers are fair game.
    try:
        for key in r.scan_iter("pdf:job:*", count=200):
            _jid = _decode(key)[len("pdf:job:") :]
            if not _jid:
                continue
            try:
                if r.ttl(key) == -2:  # already gone (scan race)
                    continue
                _status = r.hget(key, "status")
                _status = _decode(_status) if _status else ""
                if _status not in PIPELINE_TERMINAL_STATUSES:
                    live.add(_jid)
            except Exception:
                # Unreadable hash -> assume active (safe direction).
                live.add(_jid)
    except Exception:
        logger.debug(
            "collect_live_job_ids: pdf:job pass failed", exc_info=True
        )
    return live


def count_cache_keys(r) -> dict[str, int]:
    """Count existing keys per cache/bookkeeping prefix (for summaries)."""
    counts: dict[str, int] = {}
    total = 0
    for prefix in CACHE_PREFIXES + JOB_KEY_PREFIXES:
        try:
            counts[prefix] = sum(
                1 for _ in r.scan_iter(f"{prefix}*", count=200)
            )
        except Exception:
            counts[prefix] = 0
        total += counts[prefix]
    counts["TOTAL"] = total
    return counts


def clear_redis_caches(r, live_ids: set[str]) -> dict:
    """Delete cache keys; per-job keys only when the job is not live.

    Returns ``{"deleted": int, "skipped_live": int, "by_prefix": {...}}``.
    """
    deleted = 0
    skipped_live = 0
    by_prefix: dict[str, int] = {}

    def _delete(prefix: str, key) -> None:
        nonlocal deleted
        try:
            r.delete(key)
            deleted += 1
            by_prefix[prefix] = by_prefix.get(prefix, 0) + 1
        except Exception:
            logger.debug("clear_redis_caches: delete failed for %s", key)

    for prefix in CACHE_PREFIXES:
        for key in r.scan_iter(f"{prefix}*", count=200):
            _delete(prefix, key)

    for prefix in JOB_KEY_PREFIXES:
        for key in r.scan_iter(f"{prefix}*", count=200):
            _job_id = _decode(key)[len(prefix) :]
            if _job_id in live_ids:
                skipped_live += 1
                continue
            _delete(prefix, key)

    return {
        "deleted": deleted,
        "skipped_live": skipped_live,
        "by_prefix": by_prefix,
    }


def _prune_dir_files(path: str, min_age: float) -> tuple[int, int]:
    """Delete files under ``path`` older than ``min_age`` seconds.

    Returns ``(files_deleted, bytes_freed)``.  Empty directories left
    behind are removed.  Never raises — best-effort like the periodic
    cleaner.
    """
    if not path or not os.path.isdir(path):
        return (0, 0)
    now = time.time()
    files = 0
    freed = 0
    for root, _dirs, names in os.walk(path, topdown=False):
        for name in names:
            fp = os.path.join(root, name)
            try:
                if now - os.path.getmtime(fp) > min_age:
                    freed += os.path.getsize(fp)
                    os.remove(fp)
                    files += 1
            except Exception:
                logger.debug("_prune_dir_files: failed to remove %s", fp)
        for name in _dirs:
            dp = os.path.join(root, name)
            try:
                if not os.listdir(dp):
                    os.rmdir(dp)
            except Exception:
                pass
    return (files, freed)


def _storage_path(name: str, attr: str) -> str:
    """Resolve a storage dir from config, falling back to ``storage/<name>``."""
    try:
        import config
    except Exception:
        config = None
    path = getattr(config, attr, None) if config else None
    return path or os.path.join("storage", name)


def _dir_stats() -> dict:
    """Return per-directory file counts and byte totals for the summary."""
    stats: dict = {}
    total_files = 0
    total_bytes = 0
    for name, attr in STORAGE_DIR_ATTRS:
        path = _storage_path(name, attr)
        size = 0
        count = 0
        try:
            if os.path.isdir(path):
                for root, _dirs, names in os.walk(path):
                    for f in names:
                        fp = os.path.join(root, f)
                        try:
                            size += os.path.getsize(fp)
                            count += 1
                        except Exception:
                            pass
        except Exception:
            pass
        stats[name] = {"files": count, "size": size}
        total_files += count
        total_bytes += size
    stats["TOTAL"] = {"files": total_files, "size": total_bytes}
    return stats


def run_cache_clear(min_age: float = DEFAULT_FILE_GRACE_SECONDS) -> dict:
    """Execute the /clear_cache operation; returns a full summary dict.

    ``min_age`` is the on-disk grace window in seconds — files modified
    within it are assumed to be in use by a live job and are skipped.
    Never raises: each pass degrades to empty results on failure.
    """
    from utils.redis_client import get_sync_redis

    result: dict = {
        "redis": {
            "keys_before": 0,
            "keys_after": 0,
            "deleted": 0,
            "skipped_live": 0,
            "by_prefix": {},
        },
        "files": {
            "before": {},
            "after": {},
            "deleted_files": 0,
            "freed_bytes": 0,
            "by_dir": {},
        },
    }
    try:
        r = get_sync_redis()
        if r is not None:
            result["redis"]["keys_before"] = count_cache_keys(r)["TOTAL"]
            live = collect_live_job_ids(r)
            cleared = clear_redis_caches(r, live)
            result["redis"]["deleted"] = cleared["deleted"]
            result["redis"]["skipped_live"] = cleared["skipped_live"]
            result["redis"]["by_prefix"] = cleared["by_prefix"]
            result["redis"]["keys_after"] = count_cache_keys(r)["TOTAL"]
    except Exception:
        logger.exception("run_cache_clear: Redis pass failed")
    try:
        result["files"]["before"] = _dir_stats()
        for name, attr in STORAGE_DIR_ATTRS:
            path = _storage_path(name, attr)
            files, freed = _prune_dir_files(path, min_age)
            result["files"]["by_dir"][name] = {
                "files": files,
                "bytes": freed,
            }
            result["files"]["deleted_files"] += files
            result["files"]["freed_bytes"] += freed
        result["files"]["after"] = _dir_stats()
    except Exception:
        logger.exception("run_cache_clear: file pass failed")
    return result
