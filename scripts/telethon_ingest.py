"""Telethon ingestion service.

Listens for incoming Telegram messages (via a userbot session), streams media
to a temp file, uploads the file to S3/R2 (via `utils.storage`), and enqueues a
Redis job containing lightweight metadata and the remote `input_key`.

Environment variables:
  API_ID, API_HASH - required for Telethon
  TELETHON_SESSION - optional string session (StringSession)
  TELETHON_SESSION_NAME - session filename when not using string session
  ENABLE_TELETHON_INGEST - if set to 1/true enables service (otherwise run manually)
  KEEP_LOCAL_UPLOADS - if set, keep local temp copies after upload
  STORAGE_BACKEND - must be 's3'/'r2' to upload to remote storage

Run: `python tools/telethon_ingest.py` (ensure TELETHON env vars present).
"""

import asyncio
import os
import logging
import uuid
import time
from typing import Optional
import sys
from pathlib import Path
import json

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - Telethon may be optional in some envs
    TelegramClient = None
    events = None
    StringSession = None

try:
    # Ensure project root is on sys.path so imports like `from utils...` work
    # even when this script is executed from the `tools/` directory.
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # Prefer async factory but also import sync helper for robustness
    from utils.storage import get_storage_backend, get_storage_backend_sync
except Exception as e:
    # Surface import failures for diagnostics and fall back to None
    print("Failed to import storage factories from utils.storage:", e)
    get_storage_backend = None
    try:
        from utils.storage import get_storage_backend_sync
    except Exception:
        get_storage_backend_sync = None

try:
    from utils.job_queue import enqueue_job
except Exception:
    enqueue_job = None

# Optional Redis async client for triggered fetch handling
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# Optional helpers used when processing remote-forward fetch requests
try:
    from utils.forward_store import load_forward_metadata
except Exception:
    load_forward_metadata = None

try:
    from utils.userbot_downloader import download_forward_via_userbot
except Exception:
    download_forward_via_userbot = None

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telethon_ingest")
# Per-run file logger for Telethon debug info
try:
    from logging.handlers import RotatingFileHandler
    LOG_PATH = Path(os.environ.get("TELETHON_LOG_PATH", "/tmp/telethon_ingest.log"))
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(str(LOG_PATH), maxBytes=5_000_000, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    fh.setFormatter(fmt)
    # Attach both module logger and telethon's logger for verbose output
    logger.addHandler(fh)
    try:
        import logging as _logging
        _tel = _logging.getLogger("telethon")
        _tel.setLevel(logging.DEBUG)
        _tel.addHandler(fh)
    except Exception:
        pass
except Exception:
    LOG_PATH = None

# Track last processed fetch timestamp for health checks
LAST_FETCH_TS = None

# Optional aiohttp debug server to expose local telethon log without storage
try:
    from aiohttp import web as _web
except Exception:
    _web = None


async def _start_aiohttp_debug_server():
    if _web is None:
        logger.debug("aiohttp not installed; debug HTTP server disabled")
        return None
    port = int(os.environ.get("TELETHON_DEBUG_PORT", "8081"))

    async def _handle_log(request):
        try:
            # optional token protection
            token_env = os.environ.get("TELETHON_DEBUG_TOKEN")
            if token_env:
                provided = request.headers.get("X-Debug-Token") or request.rel_url.query.get("debug_token")
                if not provided or provided != token_env:
                    return _web.json_response({"error": "unauthorized"}, status=401)
            if not LOG_PATH or not LOG_PATH.exists():
                return _web.json_response({"error": "log not available"}, status=404)
            return _web.FileResponse(path=str(LOG_PATH), headers={"Content-Type": "text/plain"})
        except Exception:
            logger.exception("telethon_ingest: debug HTTP handler error")
            return _web.json_response({"error": "internal"}, status=500)

    async def _handle_health(request):
        try:
            token_env = os.environ.get("TELETHON_DEBUG_TOKEN")
            if token_env:
                provided = request.headers.get("X-Debug-Token") or request.rel_url.query.get("debug_token")
                if not provided or provided != token_env:
                    return _web.json_response({"error": "unauthorized"}, status=401)
            status = {"service": "telethon_ingest", "ok": True}
            # Redis connectivity check
            try:
                if aioredis is None:
                    status["redis"] = "disabled"
                else:
                    redis_url = os.environ.get("REDIS_URL")
                    if not redis_url:
                        status["redis"] = "not_configured"
                    else:
                        try:
                            r = aioredis.from_url(redis_url, decode_responses=True)
                            pong = await r.ping()
                            status["redis"] = "ok" if pong else "pong_failed"
                            try:
                                await r.close()
                            except Exception:
                                pass
                        except Exception:
                            status["redis"] = "error"
            except Exception:
                status["redis"] = "unknown"

            status["last_fetch_ts"] = globals().get("LAST_FETCH_TS")
            return _web.json_response(status)
        except Exception:
            logger.exception("telethon_ingest: health handler error")
            return _web.json_response({"error": "internal"}, status=500)

    app = _web.Application()
    app.router.add_get("/debug/telethon-log", _handle_log)
    app.router.add_get("/debug/health", _handle_health)
    runner = _web.AppRunner(app)
    try:
        await runner.setup()
        site = _web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info("telethon_ingest: debug HTTP server started on 0.0.0.0:%s", port)
        return runner
    except Exception:
        logger.exception("telethon_ingest: failed to start debug HTTP server")
        try:
            await runner.cleanup()
        except Exception:
            pass
        return None


async def _get_backend_instance():
    """Return an async-capable storage backend instance or None.

    Tries the async factory first, then falls back to the synchronous helper.
    """
    global get_storage_backend, get_storage_backend_sync
    if get_storage_backend:
        try:
            return await get_storage_backend()
        except Exception:
            pass
    if get_storage_backend_sync:
        try:
            return get_storage_backend_sync()
        except Exception:
            pass
    # Last-ditch: attempt on-the-fly import
    try:
        from utils.storage import get_storage_backend as _g, get_storage_backend_sync as _gs

        get_storage_backend = _g
        get_storage_backend_sync = _gs
        if get_storage_backend:
            try:
                return await get_storage_backend()
            except Exception:
                pass
        if get_storage_backend_sync:
            return get_storage_backend_sync()
    except Exception:
        pass
    return None


async def upload_telethon_log(suffix: str = "telethon_ingest.log"):
    """Upload the local telethon log file to storage if available.

    Returns the storage key on success or None on failure.
    """
    try:
        if not LOG_PATH:
            return None
        if not LOG_PATH.exists():
            return None
        backend = await _get_backend_instance()
        if backend is None:
            logger.debug("telethon_ingest: no storage backend for log upload")
            return None
        ts_key = f"telethon/{int(time.time())}_{suffix}"
        latest_key = f"telethon/telethon_ingest.latest.log"
        try:
            await backend.upload_file(str(LOG_PATH), ts_key)
            logger.info("telethon_ingest: uploaded log to %s", ts_key)
            # Also attempt to write a stable "latest" key for quick retrieval
            try:
                # Prefer backend-provided copy/put semantics if available
                if hasattr(backend, "copy_key"):
                    try:
                        await backend.copy_key(ts_key, latest_key)
                    except Exception:
                        # fall back to re-upload
                        await backend.upload_file(str(LOG_PATH), latest_key)
                else:
                    await backend.upload_file(str(LOG_PATH), latest_key)
                logger.info("telethon_ingest: updated latest log key %s", latest_key)
            except Exception:
                logger.exception("telethon_ingest: failed to write latest log key")
            return ts_key
        except Exception:
            logger.exception("telethon_ingest: failed to upload log to storage")
            return None
    except Exception:
        logger.exception("telethon_ingest: upload_telethon_log unexpected error")
        return None


async def _upload_and_enqueue(local_path: str, original_name: str, chat_id: Optional[int], message_id: Optional[int]):
    job_id = uuid.uuid4().hex
    size = None
    try:
        size = os.path.getsize(local_path)
    except Exception:
        size = None

    input_key = None
    try:
        backend = await _get_backend_instance()
        if backend is None:
            logger.error("Storage backend unavailable; cannot upload %s", local_path)
            return

        ts = time.gmtime()
        key = f"uploads/{ts.tm_year}/{ts.tm_mon:02d}/{job_id}_{os.path.basename(local_path)}"
        # Debug: log absolute path and existence before attempting upload
        try:
            abs_path = os.path.abspath(local_path)
            exists = os.path.exists(abs_path)
            size = os.path.getsize(abs_path) if exists else None
            logger.info("telethon_ingest: upload debug - local_path=%s abs_path=%s exists=%s size=%s", local_path, abs_path, exists, size)
        except Exception:
            logger.exception("telethon_ingest: failed to stat local_path %s", local_path)

        # Ensure we pass an absolute, resolved path to the storage backend
        try:
            from pathlib import Path as _Path
            abs_path = str(_Path(local_path).resolve())
        except Exception:
            abs_path = os.path.abspath(local_path)

        if not os.path.exists(abs_path):
            logger.error("telethon_ingest: upload path missing, aborting: %s (resolved from %s)", abs_path, local_path)
            raise ValueError(f"Invalid src_path: {abs_path}")

        # Some storage backends may reject Windows-style paths; copy to a
        # temporary POSIX-like path before uploading to ensure compatibility.
        try:
            import shutil, tempfile
            tmp_dir = os.environ.get("TEMP_UPLOAD_DIR") or _Path(getattr(config, "TEMP_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "temp"))).as_posix()
            os.makedirs(tmp_dir, exist_ok=True)
            tmp_dst = os.path.join(tmp_dir, os.path.basename(abs_path))
            # If source and tmp_dst are the same, skip copy
            if os.path.abspath(abs_path) != os.path.abspath(tmp_dst):
                shutil.copy2(abs_path, tmp_dst)
            upload_src = tmp_dst
        except Exception:
            logger.exception("telethon_ingest: failed to copy to temp upload path, will attempt direct upload")
            upload_src = abs_path

        try:
            await backend.upload_file(upload_src, key)
            input_key = key
            logger.info("Uploaded %s -> %s", upload_src, input_key)
        finally:
            # cleanup temporary copy if we created one and KEEP_LOCAL_UPLOADS not set
            try:
                keep_local = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
            except Exception:
                keep_local = False
            try:
                if upload_src != abs_path and not keep_local:
                    try:
                        os.remove(upload_src)
                    except Exception:
                        pass
            except Exception:
                pass
        # Only remove local temp copy if operator did NOT request to keep uploads.
        # Prefer leaving cleanup responsibility to the worker which also respects
        # KEEP_LOCAL_UPLOADS (we patched worker to honor this global flag).
        try:
            keep_local = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
        except Exception:
            keep_local = False
        if not keep_local:
            try:
                os.remove(local_path)
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to upload to storage for %s", local_path)

    # If upload failed (no input_key), do not enqueue an empty job.
    if not input_key:
        logger.error("Upload did not produce an input_key for %s; not enqueuing job %s", local_path, job_id)
        return

    # Build job metadata and enqueue to Redis (metadata-only)
    try:
        keep_local = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
    except Exception:
        keep_local = False

    job = {
        "job_id": job_id,
        "input_key": input_key,
        "original_filename": original_name or os.path.basename(local_path),
        "size": size or 0,
        "chat_id": chat_id,
        "message_id": message_id,
        "progress_channel": f"ffmpeg:progress:{job_id}",
        # Let the worker decide whether to delete local input; here we
        # indicate whether the job should cleanup the input after processing.
        "cleanup_input": not keep_local,
    }

    # Optionally save metadata to MongoDB for Telethon ingestion (best-effort, non-blocking)
    try:
        if os.environ.get("TELETHON_MONGO_BRIDGE", "").lower() in ("1", "true", "yes"):
            try:
                from utils.telethon_mongo import save_telethon_forward

                try:
                    # Schedule in background so ingestion isn't delayed by DB latency
                    asyncio.create_task(save_telethon_forward(job))
                except Exception:
                    try:
                        loop = asyncio.get_event_loop()
                        loop.create_task(save_telethon_forward(job))
                    except Exception:
                        # best-effort only
                        pass
            except Exception:
                logger.exception("Telethon->Mongo bridge unavailable")
    except Exception:
        pass

    if enqueue_job is None:
        logger.error("enqueue_job not available; cannot enqueue %s", job_id)
        return

    try:
        await enqueue_job(job)
        logger.info("Enqueued job %s (input_key=%s)", job_id, input_key)
    except Exception:
        logger.exception("Failed to enqueue job %s", job_id)


async def _process_forward_hash(forward_hash: str):
    """Handle a published forward_hash: download via userbot and upload/enqueue."""
    if not load_forward_metadata:
        logger.error("telethon_ingest: forward_store not available; cannot process %s", forward_hash)
        return False

    meta = load_forward_metadata(forward_hash)
    if not meta:
        logger.error("telethon_ingest: no metadata for forward_hash %s", forward_hash)
        return False

    logger.info("telethon_ingest: processing forward %s meta_chat=%s meta_msg=%s", forward_hash, meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"))
    try:
        global LAST_FETCH_TS
        LAST_FETCH_TS = time.time()
    except Exception:
        pass

    tmp = _make_temp_path(forward_hash, os.path.splitext(meta.get("name") or "")[1] or "")

    if not download_forward_via_userbot:
        logger.error("telethon_ingest: userbot_downloader not available; cannot fetch %s", forward_hash)
        return False

    try:
        ok = await download_forward_via_userbot(
            meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"), tmp, msg_date=meta.get("registered_at") or meta.get("created_at"), file_unique_id=meta.get("file_unique_id")
        )
        logger.info("telethon_ingest: userbot download for %s returned ok=%s exists=%s", forward_hash, bool(ok), os.path.exists(tmp))
        if not ok or not os.path.exists(tmp):
            logger.error("telethon_ingest: download failed for %s", forward_hash)
            return False
    except Exception:
        logger.exception("telethon_ingest: exception during download for %s", forward_hash)
        return False

    # Upload & enqueue using same helper
    try:
        await _upload_and_enqueue(tmp, meta.get("name"), meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"))
    except Exception:
        logger.exception("telethon_ingest: upload/enqueue failed for %s", forward_hash)
        return False

    # Optionally remove forward metadata (forward_store may handle this elsewhere)
    try:
        from utils.forward_store import delete_forward_metadata

        try:
            delete_forward_metadata(forward_hash)
        except Exception:
            pass
    except Exception:
        pass

    return True


async def redis_listener():
    """Subscribe to the fetch channel and process forward_hash messages."""
    if aioredis is None:
        logger.info("telethon_ingest: redis.asyncio not installed; fetch listener disabled")
        return

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.info("telethon_ingest: REDIS_URL not set; fetch listener disabled")
        return

    r = None
    pub = None
    fetch_channel = os.environ.get("FETCH_CHANNEL", "ffmpeg:fetch")
    forward_channel = os.environ.get("FORWARD_PUBLISH_CHANNEL", "ffmpeg:forwards")
    try:
        r = aioredis.from_url(redis_url, decode_responses=True)
        pub = r.pubsub()
        # Subscribe to both fetch requests and forward-publish notifications
        try:
            await pub.subscribe(fetch_channel, forward_channel)
            logger.info("telethon_ingest: subscribed to %s and %s", fetch_channel, forward_channel)
        except Exception:
            # fallback to subscribing to fetch_channel only
            try:
                await pub.subscribe(fetch_channel)
                logger.info("telethon_ingest: subscribed to %s", fetch_channel)
            except Exception:
                logger.exception("telethon_ingest: failed to subscribe to redis channels")

        # Use get_message loop which is friendlier to cancellation and
        # allows explicit timeout checks instead of relying on async generators
        while True:
            try:
                # `get_message` is async in redis.asyncio; timeout in seconds
                msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not msg:
                    await asyncio.sleep(0.1)
                    continue
                # Extract raw data from the pubsub message
                try:
                    # redis.asyncio pubsub returns a dict-like message
                    data = None
                    if isinstance(msg, dict):
                        data = msg.get("data")
                    else:
                        # fallback: assume message itself is payload
                        data = msg
                    # decode bytes if necessary
                    if isinstance(data, (bytes, bytearray)):
                        try:
                            data = data.decode("utf-8")
                        except Exception:
                            data = data.decode(errors="ignore")
                    # Attempt JSON parse; fall back to raw string as forward_hash
                    try:
                        payload = json.loads(data) if data else {}
                    except Exception:
                        payload = {"forward_hash": data}
                except Exception:
                    logger.exception("telethon_ingest: failed to extract data from redis message")
                    payload = {}

                # Debug: log received payload (non-sensitive)
                try:
                    logger.info("telethon_ingest: redis payload received: %s", payload)
                except Exception:
                    pass

                # Accept either `forward_hash` (preferred) or legacy `fid`/`forward_id`.
                fh = payload.get("forward_hash") or payload.get("fid") or payload.get("forward_id")
                if not fh:
                    try:
                        logger.debug("telethon_ingest: fetch payload missing forward id; payload=%s", payload)
                    except Exception:
                        pass
                else:
                    try:
                        asyncio.create_task(_process_forward_hash(fh))
                    except Exception:
                        logger.exception("telethon_ingest: failed to schedule _process_forward_hash for %s", fh)
            except asyncio.CancelledError:
                # Graceful cancellation requested
                break
            except RuntimeError as e:
                # Sometimes the underlying async generator may raise when
                # the connection is being closed; log and break so we can
                # cleanup and optionally restart.
                logger.exception("telethon_ingest: redis listener runtime error: %s", e)
                break
            except Exception:
                logger.exception("telethon_ingest: error while processing redis message")
                await asyncio.sleep(1)
    except Exception:
        logger.exception("telethon_ingest: redis listener failed to start")
    finally:
        try:
            if pub is not None:
                try:
                    await pub.unsubscribe(fetch_channel)
                except Exception:
                    pass
                try:
                    await pub.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if r is not None:
                try:
                    await r.close()
                except Exception:
                    pass
        except Exception:
            pass


def _make_temp_path(msg_id: str, ext: str = "") -> str:
    base_dir = getattr(config, "TEMP_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "temp"))
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base_dir, f"{msg_id}{ext}")


async def main():
    if TelegramClient is None:
        logger.error("Telethon not installed. Add telethon to requirements to enable ingestion.")
        return
    # Log a concise summary of environment presence so remote deploy logs reveal
    # why Telethon ingest may not start (missing creds, session, or feature flag).
    try:
        env_summary = {
            "ENABLE_TELETHON_INGEST": os.getenv("ENABLE_TELETHON_INGEST") or "",
            "API_ID_present": bool(os.getenv("API_ID") or os.getenv("USERBOT_API_ID")),
            "API_HASH_present": bool(os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")),
            "TELETHON_SESSION_present": bool(os.getenv("TELETHON_SESSION") or os.getenv("API_SESSION") or os.getenv("USERBOT_SESSION")),
            "TELETHON_SESSION_NAME": os.getenv("TELETHON_SESSION_NAME") or os.getenv("API_SESSION_NAME") or "telethon_ingest",
            "REDIS_URL_present": bool(os.getenv("REDIS_URL")),
            "STORAGE_BACKEND": os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND,
        }
        logger.info("telethon_ingest: env summary: %s", json.dumps(env_summary))
    except Exception:
        logger.exception("telethon_ingest: failed to compute env summary")

    api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
    api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
    if not api_id or not api_hash:
        logger.error("API_ID and API_HASH environment variables are required for Telethon ingestion")
        return
    try:
        api_id = int(api_id)
    except Exception:
        logger.error("API_ID must be an integer")
        return

    # Accept multiple environment variable names for the string session
    session_env_used = None
    session_str = None
    for k in ("TELETHON_SESSION", "API_SESSION", "USERBOT_SESSION", "api_session", "API_SESSION_STR"):
        v = os.getenv(k)
        if v:
            # Trim whitespace/newlines which often appear when env vars are injected
            session_str = v.strip()
            session_env_used = k
            break

    session_name = (os.getenv("TELETHON_SESSION_NAME") or os.getenv("API_SESSION_NAME") or "telethon_ingest").strip()

    # Track whether we successfully loaded a StringSession from env
    string_session_loaded = False
    if session_str and StringSession is not None:
        try:
            session = StringSession(session_str)
            string_session_loaded = True
            logger.info("Using Telethon string session from env %s", session_env_used)
        except Exception:
            logger.exception("Failed to load StringSession from env %s; falling back to file-based session name %s", session_env_used, session_name)
            session = session_name
    else:
        session = session_name
        if session_env_used:
            logger.warning("Found session env %s but StringSession class not available; using session name %s", session_env_used, session_name)
        else:
            logger.info("No string session env present; using session name %s", session_name)

    # Ensure file-based sessions are placed in a writable temp directory on remote platforms
    try:
        if not string_session_loaded:
            # Prefer TELETHON_SESSION_DIR, then TEMP_PATH from config, then /tmp
            session_dir = os.environ.get("TELETHON_SESSION_DIR") or os.environ.get("TEMP_PATH") or getattr(config, "TEMP_PATH", None) or "/tmp"
            try:
                os.makedirs(session_dir, exist_ok=True)
            except Exception:
                pass
            # Use a path under the session_dir to avoid write-permissions issues on remote
            session_path = os.path.join(session_dir, session_name)
            session = session_path
            logger.info("telethon_ingest: using session path %s", session)
    except Exception:
        logger.exception("telethon_ingest: failed to resolve session path")

    # Startup environment summary (safe): show which critical env vars/flags are present.
    try:
        env_summary = {
            "API_ID_SET": bool(os.getenv("API_ID") or os.getenv("USERBOT_API_ID")),
            "API_HASH_SET": bool(os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")),
            "SESSION_ENV_USED": session_env_used or "",
            "SESSION_NAME": session_name,
            "TELETHON_SESSION_PROVIDED": bool(session_str),
            "REDIS_URL_SET": bool(os.getenv("REDIS_URL")),
            "STORAGE_BACKEND": (os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local"),
            "S3_BUCKET_SET": bool(os.getenv("S3_BUCKET") or getattr(config, "S3_BUCKET", None)),
            "ENABLE_TELETHON_INGEST": os.getenv("ENABLE_TELETHON_INGEST", ""),
            "TELETHON_MONGO_BRIDGE": os.getenv("TELETHON_MONGO_BRIDGE", ""),
        }
        logger.info("telethon_ingest: startup env summary: %s", env_summary)
    except Exception:
        logger.exception("telethon_ingest: failed to emit startup env summary")

    # One-shot cleanup of local Telethon session files when requested.
    try:
        if os.environ.get("TELETHON_CLEAN_SESSION", "").strip().lower() in ("1", "true", "yes"):
            try:
                # If a valid StringSession was loaded, do not touch file-based sessions.
                if string_session_loaded:
                    logger.info("telethon_ingest: TELETHON_CLEAN_SESSION set, but a valid StringSession was loaded; skipping file deletion")
                else:
                    deleted = []
                    # Candidate directories to search for session files: cwd and project root
                    cand_dirs = [os.getcwd(), str(Path(__file__).resolve().parents[1])]
                    try:
                        cand_dirs.append(str(Path.home()))
                    except Exception:
                        pass
                    import glob, shutil
                    for d in cand_dirs:
                        try:
                            # remove common explicit names and any files starting with session_name
                            patterns = [session_name, f"{session_name}.session", f"{session_name}.session-journal", f"{session_name}.session.lock", f"{session_name}*"]
                            for pat in patterns:
                                for p in glob.glob(os.path.join(d, pat)):
                                    pth = Path(p)
                                    try:
                                        if pth.is_file():
                                            pth.unlink()
                                            deleted.append(str(pth))
                                        elif pth.is_dir():
                                            shutil.rmtree(pth)
                                            deleted.append(str(pth))
                                    except Exception:
                                        logger.exception("telethon_ingest: failed deleting session candidate %s", p)
                        except Exception:
                            logger.exception("telethon_ingest: error scanning for session files in %s", d)
                    logger.info("telethon_ingest: TELETHON_CLEAN_SESSION deleted %s", deleted)
            except Exception:
                logger.exception("telethon_ingest: error during TELETHON_CLEAN_SESSION cleanup")
    except Exception:
        pass
    # Determine whether we should listen for incoming messages (legacy behavior)
    LISTEN_INCOMING = os.environ.get("TELETHON_LISTEN_INCOMING", "").lower() in ("1", "true", "yes")

    client = None
    if LISTEN_INCOMING:
        # Legacy mode: register an event handler and keep a running Telethon client
        client = TelegramClient(session, api_id, api_hash)

        @client.on(events.NewMessage(incoming=True))
        async def handler(event):
            try:
                msg = event.message
                if not getattr(msg, "media", None):
                    return

                # process message in background to avoid blocking Telethon event loop
                asyncio.create_task(process_incoming(msg, client))
            except Exception:
                logger.exception("Error in Telethon handler")


        async def process_incoming(msg, client_instance):
            # determine filename/extension safely
            fname = None
            try:
                # Telethon may expose a .file or .document with a name
                file_attr = getattr(msg, "file", None)
                if file_attr is not None:
                    fname = getattr(file_attr, "name", None)
            except Exception:
                fname = None

            msg_id = getattr(msg, "id", str(uuid.uuid4()))
            # derive extension from file name or default to .mp4
            ext = os.path.splitext(fname or "")[1] or ""
            tmp = _make_temp_path(msg_id, ext)

            try:
                await client_instance.download_media(msg, file=tmp)
                logger.info("Downloaded incoming media to %s", tmp)
            except Exception:
                logger.exception("Failed to download media from message %s", msg_id)
                return

            # Upload & enqueue
            try:
                await _upload_and_enqueue(tmp, fname, getattr(msg.chat, "id", None) or getattr(msg, "chat_id", None), getattr(msg, "id", None))
            except Exception:
                logger.exception("Failed to upload/enqueue for %s", tmp)

        # start client
        await client.start()
        logger.info("Telethon ingestion client started, listening for incoming media...")
    else:
        logger.info("telethon_ingest: starting in fetch-only mode (no passive Telegram listeners)")
    # Write a small marker file so external deploy logs or healthchecks can
    # confirm the Telethon ingest process started successfully.
    try:
        marker_dir = getattr(config, "TEMP_PATH", None) or os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "temp")
        os.makedirs(marker_dir, exist_ok=True)
        marker = os.path.join(marker_dir, "telethon_ingest.started")
        with open(marker, "w") as fh:
            fh.write(f"started_at={time.time()}\nsession_env={session_env_used or ''}\n")
    except Exception:
        logger.exception("Failed to write telethon_ingest.started marker")
    # Also attempt to upload the marker to the configured storage backend (S3/R2/local)
    try:
        try:
            backend = await _get_backend_instance()
        except Exception:
            backend = None
        if backend is not None:
            try:
                dest_key = f"telethon/telethon_ingest.started"
                await backend.upload_file(marker, dest_key)
                logger.info("Uploaded telethon_ingest.started to storage: %s", dest_key)
            except Exception:
                logger.exception("Failed to upload telethon_ingest.started to storage")
        # Attempt to upload the telethon debug log as well (best-effort)
        try:
            try:
                await upload_telethon_log()
            except Exception:
                logger.exception("telethon_ingest: upload_telethon_log failed during startup")
        except Exception:
            pass
    except Exception:
        logger.exception("Error while attempting to publish telethon_ingest.started marker to storage")

    # Start Redis fetch listener (if available) so this single service can
    # both accept incoming messages and process published forward fetches
    redis_task = None
    try:
        redis_task = asyncio.create_task(redis_listener())
    except Exception:
        logger.exception("telethon_ingest: failed to start redis listener")

    # Start aiohttp debug server (serves local /tmp/telethon_ingest.log) if available
    http_runner = None
    try:
        http_runner = await _start_aiohttp_debug_server()
    except Exception:
        logger.exception("telethon_ingest: debug HTTP server failed to start")

    try:
        if LISTEN_INCOMING and client is not None:
            try:
                await client.run_until_disconnected()
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        else:
            # Fetch-only mode: wait on redis listener task, or sleep forever
            if redis_task is not None:
                try:
                    await redis_task
                except asyncio.CancelledError:
                    pass
            else:
                await asyncio.Future()
    finally:
        # Cancel background redis listener if started
        try:
            if redis_task is not None:
                redis_task.cancel()
                try:
                    await redis_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        except Exception:
            pass
        # Stop aiohttp debug server if running
        try:
            if http_runner is not None:
                try:
                    await http_runner.cleanup()
                except Exception:
                    pass
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        try:
            # Best-effort: attempt to upload log before exiting
            try:
                asyncio.run(upload_telethon_log("telethon_ingest_crash.log"))
            except Exception:
                pass
        except Exception:
            pass
