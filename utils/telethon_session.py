import asyncio
import json
import os
import time
import threading
import logging
from typing import Optional, Union

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:  # pragma: no cover - optional dependency
    PyrogramClient = None

logger = logging.getLogger(__name__)


def _get_env_value(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return None


def get_telethon_session_name() -> str:
    return _get_env_value(
        "API_SESSION_NAME",
        "SESSION_NAME",
        "USERBOT_SESSION_NAME",
        "TELETHON_SESSION_NAME",
    ) or "userbot_session"


def get_telethon_session_dir() -> str:
    return _get_env_value("TELETHON_SESSION_DIR") or os.getenv("TEMP_PATH") or os.getcwd()


def get_telethon_session_path() -> str:
    session_dir = get_telethon_session_dir()
    try:
        os.makedirs(session_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(session_dir, get_telethon_session_name())


# \u2500\u2500 JSON file persistence bridge \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#
# When a session is used via ``StringSession`` / Pyrogram in-memory,
# Telegram's client library does **not** create a file on disk that can
# be reused on restart.  If the env var is lost, there is no fallback.
#
# To bridge this gap, the session healthchecker periodically writes the
# live session strings to a shared JSON file at the same path as the
# Telethon session name.  Both ``telethon_session`` and ``pyrogram_session``
# are stored in the same file under separate keys.
# ------------------------------------------------------------------

# Keys used inside the JSON dict
_KEY_TELETHON = "telethon_session"
_KEY_PYROGRAM = "pyrogram_session"

# \u2500\u2500 In-memory cache for session file reads \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#
# Both Telethon and Pyrogram checks in the healthchecker read the same
# JSON file.  To avoid redundant disk I/O within a single cycle, the
# file contents are cached in-memory with a short TTL.  The cache is
# invalidated whenever a write occurs.
#
# A ``threading.Lock`` protects access to the module-level globals
# because the cache functions are called from thread pool workers
# (via ``asyncio.to_thread``) when the async readers are used.
# ------------------------------------------------------------------
_SESSION_CACHE_DATA = None
_SESSION_CACHE_EXPIRES = 0.0
_SESSION_CACHE_TTL = 60  # seconds
_SESSION_CACHE_LOCK = threading.Lock()


def _get_cached_sessions() -> Optional[dict]:
    """Return cached session dict if still fresh, else None."""
    with _SESSION_CACHE_LOCK:
        if _SESSION_CACHE_DATA is not None and time.time() < _SESSION_CACHE_EXPIRES:
            return _SESSION_CACHE_DATA
        return None


def _set_cached_sessions(data: dict):
    """Cache session data with the module-level TTL."""
    with _SESSION_CACHE_LOCK:
        global _SESSION_CACHE_DATA, _SESSION_CACHE_EXPIRES
        _SESSION_CACHE_DATA = data
        _SESSION_CACHE_EXPIRES = time.time() + _SESSION_CACHE_TTL


def _invalidate_session_cache():
    """Clear the in-memory cache after a write."""
    with _SESSION_CACHE_LOCK:
        global _SESSION_CACHE_DATA, _SESSION_CACHE_EXPIRES
        _SESSION_CACHE_DATA = None
        _SESSION_CACHE_EXPIRES = 0.0


def _get_persisted_session_path() -> str:
    """Return the path to the JSON file used for session string persistence.

    The file is stored alongside the Telethon session directory with a
    ``.session.json`` extension.
    """
    return get_telethon_session_path() + ".session.json"


def _load_all_sessions_from_file() -> dict:
    """Read the full persisted JSON dict from disk (synchronous).

    Returns a dict (possibly empty) on success, or an empty dict on failure.

    Results are cached in-memory for ``_SESSION_CACHE_TTL`` seconds to
    avoid redundant reads within a single healthcheck cycle.

    For async contexts, prefer ``_load_all_sessions_from_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.
    """
    # Check in-memory cache first to avoid redundant disk I/O
    cached = _get_cached_sessions()
    if cached is not None:
        return cached

    path = _get_persisted_session_path()
    if not os.path.exists(path):
        _set_cached_sessions({})
        return {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        result = data if isinstance(data, dict) else {}
        _set_cached_sessions(result)
        return result
    except Exception as exc:
        logger.debug("session: failed to read persisted session file %s: %s", path, exc)
        # DO NOT cache the empty result here!  A transient read error (e.g. temporary
        # file lock, incomplete write from another process, or a JSON decode glitch)
        # would otherwise poison the in-memory cache with {} for the next 60 seconds.
        # Any call to ``save_session_string_to_file`` during that window would then
        # read the cached {}, update only one key, and silently drop the other \u2014 which
        # is exactly how ``telethon_session`` kept disappearing from the JSON file.
        return {}


async def _load_all_sessions_from_file_async() -> dict:
    """Async version of ``_load_all_sessions_from_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk reads.  Intended for callers
    in async contexts (healthchecker).
    """
    return await asyncio.to_thread(_load_all_sessions_from_file)


def save_session_string_to_file(session_str: str, client_type: str = "telethon") -> bool:
    """Persist a session string to a shared JSON file (synchronous).

    Both Telethon and Pyrogram session strings are stored in the same file
    under different keys (``telethon_session`` / ``pyrogram_session``).
    The ``client_type`` parameter determines which key is updated.

    Best-effort: returns True on success, False on failure (logged).

    For async contexts, prefer ``save_session_string_to_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.

    Important
    ---------
    This function reads existing session data **directly from disk**,
    bypassing the in-memory ``_SESSION_CACHE``.  This avoids a subtle
    clobbering bug: if ``_load_all_sessions_from_file`` cached ``{}``
    after a transient read error, a subsequent write here would
    silently drop the other session key written by the other client.
    """
    path = _get_persisted_session_path()
    try:
        # Read existing data directly from disk, bypassing the in-memory cache.
        existing = {}
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    _raw = json.load(f)
                if isinstance(_raw, dict):
                    existing = _raw
            except Exception as exc:
                logger.debug(
                    "session: failed to read existing data from %s before write: %s",
                    path, exc,
                )
        key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
        existing[key] = session_str

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(existing, f)
        logger.info(
            "session: persisted %s session string to %s (%d chars)",
            client_type, path, len(session_str),
        )
        # Invalidate in-memory cache so subsequent reads see the new data
        _invalidate_session_cache()
        return True
    except Exception as exc:
        logger.debug(
            "session: failed to persist %s session string to %s: %s",
            client_type, path, exc,
        )
        return False


async def save_session_string_to_file_async(session_str: str, client_type: str = "telethon") -> bool:
    """Async version of ``save_session_string_to_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk writes.  Intended for callers
    in async contexts (healthchecker, login flow).
    """
    return await asyncio.to_thread(
        save_session_string_to_file,
        session_str,
        client_type=client_type,
    )


def _load_session_string_from_file(client_type: str = "telethon") -> Optional[str]:
    """Load a session string previously persisted by the healthchecker (synchronous).

    Parameters
    ----------
    client_type:
        ``"telethon"`` (default) or ``"pyrogram"``.

    Returns the string or None if the file is missing or the key not found.

    For async contexts, prefer ``_load_session_string_from_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.
    """
    data = _load_all_sessions_from_file()
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type, _get_persisted_session_path(), len(session_str),
        )
        return session_str
    return None


async def _load_session_string_from_file_async(client_type: str = "telethon") -> Optional[str]:
    """Async version of ``_load_session_string_from_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk reads.  Intended for callers
    in async contexts (healthchecker).
    """
    data = await _load_all_sessions_from_file_async()
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type, _get_persisted_session_path(), len(session_str),
        )
        return session_str
    return None


def _get_configured_session_string() -> Optional[str]:
    """Return a Telethon session string from any available source.

    Resolution order:
    1. Environment variable (``TELETHON_SESSION`` / ``API_SESSION`` etc.)
    2. Persisted JSON file (``telethon_session`` key, written by healthchecker)
    """
    # 1. Check env vars first (highest priority)
    env_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )
    if env_str:
        return env_str

    # 2. Fall back to the JSON file persisted by the healthchecker
    file_str = _load_session_string_from_file(client_type="telethon")
    if file_str:
        return file_str

    return None


async def get_telethon_session_string_for_user(user_id: Optional[int] = None, db_model: Optional[object] = None) -> Optional[str]:
    """Return a usable Telethon session string for the given user, if available.

    Checks env vars first, then a MongoDB-persisted session when db_model is supplied.
    """
    # 1. Check env vars + persisted JSON file
    session_str = _get_configured_session_string()
    if session_str:
        return session_str

    # 2. Check MongoDB-persisted session for the given user
    if user_id is not None and db_model is not None:
        try:
            saved_session = await db_model.load_session(user_id)
        except Exception as exc:
            logger.warning("Failed to inspect MongoDB Telethon session for user %s: %s", user_id, exc)
            saved_session = None

        if isinstance(saved_session, dict):
            session_value = saved_session.get("string_session") or saved_session.get("session_string")
            if session_value:
                logger.info(
                    "session: loaded Telethon session string from MongoDB for user %s",
                    user_id,
                )
                return str(session_value)

    logger.debug("session: no Telethon session string found for user %s", user_id)
    return None


async def get_telethon_session_status(user_id: Optional[int] = None, db_model: Optional[object] = None) -> dict:
    """Return a diagnostic summary for Telethon session availability.

    Checks the same sources the bot can actually use for login fallback:
    - explicit session string in env vars
    - a local .session file on disk
    - a MongoDB-persisted session for a specific user when db_model is provided
    """
    session_path = get_telethon_session_path()
    session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=db_model)

    if session_str:
        env_session = _get_configured_session_string()
        return {
            "ready": True,
            "source": "env" if env_session else "mongodb",
            "session_path": session_path,
            "details": "Telethon session string configured in environment"
            if env_session
            else "Telethon session string persisted in MongoDB",
        }

    if os.path.exists(session_path) or os.path.exists(session_path + ".session"):
        return {
            "ready": True,
            "source": "file",
            "session_path": session_path,
            "details": "Telethon session file exists on disk",
        }

    if user_id is not None and db_model is not None:
        try:
            saved_session = await db_model.load_session(user_id)
        except Exception as exc:
            logger.warning("Failed to inspect MongoDB Telethon session for user %s: %s", user_id, exc)
            saved_session = None

        if isinstance(saved_session, dict) and saved_session.get("string_session"):
            return {
                "ready": True,
                "source": "mongodb",
                "session_path": session_path,
                "details": "Telethon session persisted in MongoDB",
            }

    return {
        "ready": False,
        "source": "missing",
        "session_path": session_path,
        "details": "No Telethon session configured or persisted",
    }


def build_telethon_client(api_id: int, api_hash: str, session_str: Optional[str] = None):
    """Build a Telethon client with session persistence.

    Session resolution order:
    1. ``session_str`` parameter (explicit call-site override, e.g. from MongoDB)
    2. ``TELETHON_SESSION`` / ``API_SESSION`` env var (StringSession)
    3. File-based ``.session`` file on disk (persistent, auto-saved by Telethon)

    When a StringSession is explicitly configured but fails to load, the
    function falls back to a file-based session.  File-based sessions are
    automatically saved by Telethon on every state change, keeping them alive
    across restarts until the device is manually revoked from Telegram.

    Timeout/retry parameters are read from environment variables:
      - ``TELETHON_TIMEOUT`` (default 120): per-request timeout in seconds.
      - ``TELETHON_REQUEST_RETRIES`` (default 10): retries on request failure.
      - ``TELETHON_CONNECTION_RETRIES`` (default 5): retries on connection failure.
      - ``TELETHON_RETRY_DELAY`` (default 3): seconds between retries.
    """
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Install telethon to use userbot fallback.")

    # Read timeout/retry configuration from env vars (tuned for large-file downloads)
    try:
        _timeout = int(os.getenv("TELETHON_TIMEOUT", "120"))
    except (TypeError, ValueError):
        _timeout = 120
    try:
        _req_retries = int(os.getenv("TELETHON_REQUEST_RETRIES", "10"))
    except (TypeError, ValueError):
        _req_retries = 10
    try:
        _conn_retries = int(os.getenv("TELETHON_CONNECTION_RETRIES", "5"))
    except (TypeError, ValueError):
        _conn_retries = 5
    try:
        _retry_delay = int(os.getenv("TELETHON_RETRY_DELAY", "3"))
    except (TypeError, ValueError):
        _retry_delay = 3

    # Resolve session: explicit parameter > env var > file-based fallback
    resolved_session = session_str or _get_configured_session_string()

    if resolved_session:
        if StringSession is None:
            raise RuntimeError(
                "Telethon StringSession is not available but a session string "
                "is provided. Ensure telethon is installed."
            )
        try:
            logger.info(
                "session: building Telethon client with StringSession (%d chars)",
                len(resolved_session),
            )
            return TelegramClient(
                StringSession(resolved_session),
                api_id,
                api_hash,
                timeout=_timeout,
                request_retries=_req_retries,
                connection_retries=_conn_retries,
                retry_delay=_retry_delay,
            )
        except Exception:
            logger.exception(
                "session: StringSession failed to load; falling back to file-based "
                "session at %s.session",
                get_telethon_session_path(),
            )
            # Fall through to file-based session below

    # No session string or StringSession failed \u2014 use file-based session.
    session_path = get_telethon_session_path()
    logger.info(
        "session: building Telethon client with file-based session at %s.session",
        session_path,
    )
    return TelegramClient(
        session_path,
        api_id,
        api_hash,
        timeout=_timeout,
        request_retries=_req_retries,
        connection_retries=_conn_retries,
        retry_delay=_retry_delay,
    )


def get_pyrogram_session_string() -> Optional[str]:
    """Return a Pyrogram session string from any available source.

    Resolution order:
    1. Environment variable (``PYROGRAM_SESSION`` etc.)
    2. Persisted JSON file (``pyrogram_session`` key, written by healthchecker)
    """
    # 1. Check env vars first (highest priority)
    env_str = _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )
    if env_str:
        return env_str

    # 2. Fall back to the JSON file persisted by the healthchecker
    file_str = _load_session_string_from_file(client_type="pyrogram")
    if file_str:
        return file_str

    return None


def build_pyrogram_client(api_id: int, api_hash: str, session_str: Optional[str] = None) -> Optional[object]:
    """Build a Pyrogram client from a session string.

    Parameters
    ----------
    api_id:
        Telegram API ID.
    api_hash:
        Telegram API hash.
    session_str:
        Optional explicit session string.  If not provided, the function
        resolves the session from env vars -> persisted JSON file
        (same resolution as ``get_pyrogram_session_string()``).

    When ``session_str`` is provided explicitly, the internal resolution
    is skipped entirely, avoiding redundant file I/O \u2014 useful when the
    caller has already loaded the session string asynchronously.

    Reads the following env vars for retry/timeout configuration:
      - PYROGRAM_SLEEP_THRESHOLD (default 30): seconds to sleep before retrying
        on flood-wait or transient server errors.
      - PYROGRAM_MAX_RETRIES (default 10): max RPC retries per request.

    Returns a Pyrogram Client ready for ``client.start()``, or None if no
    session string is available (or Pyrogram is not installed).
    """
    if PyrogramClient is None:
        logger.debug("Pyrogram is not installed; cannot use Pyrogram session string.")
        return None

    if session_str is None:
        session_str = get_pyrogram_session_string()
    if not session_str:
        return None

    # Read retry/timeout configuration from env vars
    try:
        sleep_threshold = int(os.getenv("PYROGRAM_SLEEP_THRESHOLD", "30"))
    except (TypeError, ValueError):
        sleep_threshold = 30
    try:
        max_retries = int(os.getenv("PYROGRAM_MAX_RETRIES", "10"))
    except (TypeError, ValueError):
        max_retries = 10

    try:
        client = PyrogramClient(
            "pyrogram_userbot_session",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
            in_memory=True,
            sleep_threshold=sleep_threshold,
        )
        # Increase the session-level RPC retry limit
        client.MAX_RETRIES = max_retries
        logger.info(
            "userbot: Pyrogram client configured with sleep_threshold=%s max_retries=%s",
            sleep_threshold, max_retries,
        )
        return client
    except Exception:
        logger.exception("Failed to create Pyrogram client from session string")
        return None


def is_pyrogram_available() -> bool:
    """Return True if Pyrogram is installed and a session string is configured.

    Checks env vars first, then the persisted JSON file written by the
    healthchecker.
    """
    if PyrogramClient is None:
        return False
    return bool(get_pyrogram_session_string())


def has_usable_telethon_session() -> bool:
    """Return True when Telethon can use a pre-existing session without prompting for login."""
    if TelegramClient is None:
        return False

    # 1. Check env vars
    session_str = _get_env_value(
        "API_SESSION", "SESSION", "api_session", "USERBOT_SESSION",
        "userbot_session", "TELETHON_SESSION", "telethon_session",
    )
    if session_str:
        return True

    # 2. Check persisted JSON file (written by healthchecker)
    file_str = _load_session_string_from_file(client_type="telethon")
    if file_str:
        return True

    # 3. Check file-based .session files on disk
    session_path = get_telethon_session_path()
    return os.path.exists(session_path) or os.path.exists(session_path + ".session")


def is_telethon_available() -> bool:
    """Return True if Telethon is installed and configured."""
    return has_usable_telethon_session()


def get_preferred_client_type() -> str:
    """Return 'pyrogram' if Pyrogram session is available, else 'telethon'."""
    if is_pyrogram_available():
        return "pyrogram"
    return "telethon"


def get_userbot_credentials():
    """Return (api_id, api_hash) from env vars.

    Raises RuntimeError if either is missing or api_id is not an integer.
    """
    api_id = os.getenv("API_ID") or os.getenv("api_id") or os.getenv("USERBOT_API_ID") or os.getenv("userbot_api_id")
    api_hash = os.getenv("API_HASH") or os.getenv("api_hash") or os.getenv("USERBOT_API_HASH") or os.getenv("userbot_api_hash")
    if not api_id or not api_hash:
        raise RuntimeError("API_ID and API_HASH must be set to use userbot fallback")
    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        raise RuntimeError("API_ID must be an integer")
    return api_id, api_hash


def normalize_target(chat_id: Union[int, str]) -> Union[int, str]:
    """Normalize a chat_id to a form usable by both Telethon and Pyrogram."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id
