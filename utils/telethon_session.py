import asyncio
import json
import logging
import os
import threading
import time

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:
    TelegramClient = None
    StringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:
    PyrogramClient = None

logger = logging.getLogger(__name__)


def _get_env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return None


def get_telethon_session_name() -> str:
    return (
        _get_env_value(
            "API_SESSION_NAME",
            "SESSION_NAME",
            "USERBOT_SESSION_NAME",
            "TELETHON_SESSION_NAME",
        )
        or "userbot_session"
    )


def get_telethon_session_dir() -> str:
    return (
        _get_env_value("TELETHON_SESSION_DIR")
        or os.getenv("TEMP_PATH")
        or os.getcwd()
    )


def get_telethon_session_path() -> str:
    session_dir = get_telethon_session_dir()
    try:
        os.makedirs(session_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(session_dir, get_telethon_session_name())


_KEY_TELETHON = "telethon_session"
_KEY_PYROGRAM = "pyrogram_session"


_SESSION_CACHE_DATA = {}
_SESSION_CACHE_EXPIRES = {}
_SESSION_CACHE_TTL = 60
_SESSION_CACHE_LOCK = threading.Lock()


def _cache_key(user_id: int | None = None) -> str:

    return "global" if user_id is None else f"user:{user_id}"


def _get_cached_sessions(user_id: int | None = None) -> dict | None:

    k = _cache_key(user_id)
    with _SESSION_CACHE_LOCK:
        entry = _SESSION_CACHE_DATA.get(k)
        expires = _SESSION_CACHE_EXPIRES.get(k, 0.0)
        if entry is not None and time.time() < expires:
            return entry
        return None


def _prune_session_cache_locked(now: float) -> None:
    """Drop expired cache entries (must be called with the lock held).

    Expiry is otherwise only checked on read, so entries for users that never
    come back would pin their data (and expiry record) in the module dicts for
    the lifetime of the process.
    """
    stale = [k for k, exp in _SESSION_CACHE_EXPIRES.items() if exp <= now]
    for k in stale:
        _SESSION_CACHE_DATA.pop(k, None)
        _SESSION_CACHE_EXPIRES.pop(k, None)


def _set_cached_sessions(data: dict, user_id: int | None = None):
    k = _cache_key(user_id)
    now = time.time()
    with _SESSION_CACHE_LOCK:
        _prune_session_cache_locked(now)
        _SESSION_CACHE_DATA[k] = data
        _SESSION_CACHE_EXPIRES[k] = now + _SESSION_CACHE_TTL


def _invalidate_session_cache(user_id: int | None = None):

    with _SESSION_CACHE_LOCK:
        if user_id is not None:
            k = _cache_key(user_id)
            _SESSION_CACHE_DATA.pop(k, None)
            _SESSION_CACHE_EXPIRES.pop(k, None)
        else:
            _SESSION_CACHE_DATA.clear()
            _SESSION_CACHE_EXPIRES.clear()


def _get_persisted_session_path(user_id: int | None = None) -> str:

    base = get_telethon_session_path() + ".session"
    if user_id is not None:
        return f"{base}.{user_id}.json"
    return base + ".json"


def _load_all_sessions_from_file(user_id: int | None = None) -> dict:

    cached = _get_cached_sessions(user_id=user_id)
    if cached is not None:
        return cached

    path = _get_persisted_session_path(user_id=user_id)
    if not os.path.exists(path):
        _set_cached_sessions({}, user_id=user_id)
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        result = data if isinstance(data, dict) else {}
        _set_cached_sessions(result, user_id=user_id)
        return result
    except Exception as exc:
        logger.debug(
            "session: failed to read persisted session file %s: %s", path, exc
        )

        return {}


async def _load_all_sessions_from_file_async(
    user_id: int | None = None,
) -> dict:

    return await asyncio.to_thread(_load_all_sessions_from_file, user_id)


def save_session_string_to_file(
    session_str: str,
    client_type: str = "telethon",
    user_id: int | None = None,
) -> bool:

    path = _get_persisted_session_path(user_id=user_id)
    try:
        existing = {}
        if os.path.exists(path):
            try:
                with open(path) as f:
                    _raw = json.load(f)
                if isinstance(_raw, dict):
                    existing = _raw
            except Exception as exc:
                logger.debug(
                    "session: failed to read existing data from %s before write: %s",
                    path,
                    exc,
                )
        key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
        existing[key] = session_str

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(existing, f)
        logger.info(
            "session: persisted %s session string to %s (%d chars)",
            client_type,
            path,
            len(session_str),
        )

        _invalidate_session_cache(user_id=user_id)
        return True
    except Exception as exc:
        logger.debug(
            "session: failed to persist %s session string to %s: %s",
            client_type,
            path,
            exc,
        )
        return False


async def save_session_string_to_file_async(
    session_str: str,
    client_type: str = "telethon",
    user_id: int | None = None,
) -> bool:

    return await asyncio.to_thread(
        save_session_string_to_file,
        session_str,
        client_type=client_type,
        user_id=user_id,
    )


def _load_session_string_from_file(
    client_type: str = "telethon",
    user_id: int | None = None,
) -> str | None:

    data = _load_all_sessions_from_file(user_id=user_id)
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type,
            _get_persisted_session_path(user_id=user_id),
            len(session_str),
        )
        return session_str
    return None


async def _load_session_string_from_file_async(
    client_type: str = "telethon",
    user_id: int | None = None,
) -> str | None:

    data = await _load_all_sessions_from_file_async(user_id=user_id)
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type,
            _get_persisted_session_path(user_id=user_id),
            len(session_str),
        )
        return session_str
    return None


def _get_configured_session_string(user_id: int | None = None) -> str | None:

    if user_id is not None:
        file_str = _load_session_string_from_file(
            client_type="telethon", user_id=user_id
        )
        if file_str:
            return file_str

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

    file_str = _load_session_string_from_file(client_type="telethon")
    if file_str:
        return file_str

    return None


async def _resolve_telethon_session_with_source(
    user_id: int | None = None, db_model: object | None = None
) -> tuple[str | None, str]:

    if user_id is not None:
        file_str = _load_session_string_from_file(
            client_type="telethon", user_id=user_id
        )
        if file_str:
            return file_str, "json"

    if user_id is not None:
        try:
            if db_model is not None and hasattr(db_model, "load_session"):
                saved_session = await db_model.load_session(user_id)
            else:
                from utils.db import get_user_session

                saved_session = await get_user_session(user_id)
        except Exception as exc:
            logger.warning(
                "Failed to inspect MongoDB Telethon session for user %s: %s",
                user_id,
                exc,
            )
            saved_session = None

        if isinstance(saved_session, dict):
            session_value = (
                saved_session.get("string_session")
                or saved_session.get("session_string")
                or saved_session.get("telethon_session")
            )
            if session_value:
                logger.info(
                    "session: loaded Telethon session string from MongoDB for user %s",
                    user_id,
                )
                return str(session_value), "mongodb"

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
        return env_str, "env"

    file_str = _load_session_string_from_file(client_type="telethon")
    if file_str:
        return file_str, "global-json"

    logger.debug(
        "session: no Telethon session string found for user %s", user_id
    )
    return None, "missing"


async def get_telethon_session_string_for_user(
    user_id: int | None = None, db_model: object | None = None
) -> str | None:

    value, _source = await _resolve_telethon_session_with_source(
        user_id, db_model
    )
    return value


async def get_telethon_session_status(
    user_id: int | None = None, db_model: object | None = None
) -> dict:

    session_path = get_telethon_session_path()
    session_str = await get_telethon_session_string_for_user(
        user_id=user_id, db_model=db_model
    )

    if session_str:
        env_session = _get_configured_session_string(user_id=user_id)
        return {
            "ready": True,
            "source": "env" if env_session else "mongodb",
            "session_path": session_path,
            "details": "Telethon session string configured in environment"
            if env_session
            else "Telethon session string persisted in MongoDB",
        }

    if os.path.exists(session_path) or os.path.exists(
        session_path + ".session"
    ):
        return {
            "ready": True,
            "source": "file",
            "session_path": session_path,
            "details": "Telethon session file exists on disk",
        }

    if user_id is not None:
        try:
            if db_model is not None and hasattr(db_model, "load_session"):
                saved_session = await db_model.load_session(user_id)
            else:
                from utils.db import get_user_session

                saved_session = await get_user_session(user_id)
        except Exception as exc:
            logger.warning(
                "Failed to inspect MongoDB Telethon session for user %s: %s",
                user_id,
                exc,
            )
            saved_session = None

        if isinstance(saved_session, dict) and (
            saved_session.get("string_session")
            or saved_session.get("telethon_session")
        ):
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


def build_telethon_client(
    api_id: int, api_hash: str, session_str: str | None = None
):

    if TelegramClient is None:
        raise RuntimeError(
            "Telethon is not installed. Install telethon to use userbot fallback."
        )

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


def get_pyrogram_session_string(user_id: int | None = None) -> str | None:

    if user_id is not None:
        file_str = _load_session_string_from_file(
            client_type="pyrogram", user_id=user_id
        )
        if file_str:
            return file_str

    env_str = _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )
    if env_str:
        return env_str

    file_str = _load_session_string_from_file(client_type="pyrogram")
    if file_str:
        return file_str

    return None


async def _resolve_pyrogram_session_with_source(
    user_id: int | None = None, db_model: object | None = None
) -> tuple[str | None, str]:

    if user_id is not None:
        file_str = _load_session_string_from_file(
            client_type="pyrogram", user_id=user_id
        )
        if file_str:
            return file_str, "json"

    if user_id is not None:
        try:
            if db_model is not None and hasattr(db_model, "load_session"):
                saved_session = await db_model.load_session(user_id)
            else:
                from utils.db import get_user_session

                saved_session = await get_user_session(user_id)
        except Exception as exc:
            logger.warning(
                "Failed to inspect MongoDB Pyrogram session for user %s: %s",
                user_id,
                exc,
            )
            saved_session = None

        if isinstance(saved_session, dict):
            session_value = saved_session.get("pyrogram_session")
            if session_value:
                logger.info(
                    "session: loaded Pyrogram session string from MongoDB for user %s",
                    user_id,
                )
                return str(session_value), "mongodb"

    env_str = _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )
    if env_str:
        return env_str, "env"

    file_str = _load_session_string_from_file(client_type="pyrogram")
    if file_str:
        return file_str, "global-json"

    logger.debug(
        "session: no Pyrogram session string found for user %s", user_id
    )
    return None, "missing"


async def get_pyrogram_session_string_for_user(
    user_id: int | None = None, db_model: object | None = None
) -> str | None:

    value, _source = await _resolve_pyrogram_session_with_source(
        user_id, db_model
    )
    return value


async def resolve_session_string(
    client_type: str,
    session_str: str | None = None,
    user_id: int | None = None,
    db_model: object | None = None,
) -> str | None:

    if session_str is not None:
        return session_str
    if client_type == "telethon":
        return await get_telethon_session_string_for_user(
            user_id=user_id, db_model=db_model
        )
    if client_type == "pyrogram":
        return await get_pyrogram_session_string_for_user(
            user_id=user_id, db_model=db_model
        )
    raise ValueError(f"Unknown client_type: {client_type!r}")


async def restore_per_user_session_files() -> int:

    restored = 0
    try:
        from utils.db import COL_SESSIONS, get_db, query

        db = await get_db()
        if db is None:
            return 0

        docs = (
            await query(COL_SESSIONS, db)
            .where("telethon_session", "!=", "")
            .get()
            or []
        )
        pyro_docs = (
            await query(COL_SESSIONS, db)
            .where("pyrogram_session", "!=", "")
            .get()
            or []
        )

        seen: set = set()
        for doc in list(docs) + list(pyro_docs):
            uid = doc.get("user_id")
            if uid is None:
                continue
            try:
                uid = int(uid)
            except (TypeError, ValueError):
                continue
            if uid in seen:
                continue
            seen.add(uid)

            written = False

            tele = doc.get("telethon_session")
            pyro = doc.get("pyrogram_session")
            if not tele and not pyro:
                tele = doc.get("string_session")
            if tele:
                written = (
                    await save_session_string_to_file_async(
                        str(tele), client_type="telethon", user_id=uid
                    )
                    or written
                )
            if pyro:
                written = (
                    await save_session_string_to_file_async(
                        str(pyro), client_type="pyrogram", user_id=uid
                    )
                    or written
                )
            if written:
                restored += 1

        if restored:
            logger.info(
                "session: restored per-user JSON session files for %d user(s) from MongoDB",
                restored,
            )
        return restored
    except Exception as exc:
        logger.debug(
            "session: restore per-user session files from MongoDB failed: %s",
            exc,
        )
        return 0


def build_pyrogram_client(
    api_id: int, api_hash: str, session_str: str | None = None
) -> object | None:

    if PyrogramClient is None:
        logger.debug(
            "Pyrogram is not installed; cannot use Pyrogram session string."
        )
        return None

    if session_str is None:
        session_str = get_pyrogram_session_string()
    if not session_str:
        return None

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

        client.MAX_RETRIES = max_retries
        logger.info(
            "userbot: Pyrogram client configured with sleep_threshold=%s max_retries=%s",
            sleep_threshold,
            max_retries,
        )
        return client
    except Exception:
        logger.exception(
            "Failed to create Pyrogram client from session string"
        )
        return None


def is_pyrogram_available(user_id: int | None = None) -> bool:

    if PyrogramClient is None:
        return False
    return bool(get_pyrogram_session_string(user_id=user_id))


def has_usable_telethon_session(user_id: int | None = None) -> bool:

    if TelegramClient is None:
        return False

    if user_id is not None:
        file_str = _load_session_string_from_file(
            client_type="telethon", user_id=user_id
        )
        if file_str:
            return True

    session_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )
    if session_str:
        return True

    file_str = _load_session_string_from_file(client_type="telethon")
    if file_str:
        return True

    session_path = get_telethon_session_path()
    return os.path.exists(session_path) or os.path.exists(
        session_path + ".session"
    )


def is_telethon_available(user_id: int | None = None) -> bool:

    return has_usable_telethon_session(user_id=user_id)


def get_preferred_client_type(user_id: int | None = None) -> str:

    if is_pyrogram_available(user_id=user_id):
        return "pyrogram"
    return "telethon"


def get_userbot_credentials():

    api_id = (
        os.getenv("API_ID")
        or os.getenv("api_id")
        or os.getenv("USERBOT_API_ID")
        or os.getenv("userbot_api_id")
    )
    api_hash = (
        os.getenv("API_HASH")
        or os.getenv("api_hash")
        or os.getenv("USERBOT_API_HASH")
        or os.getenv("userbot_api_hash")
    )
    if not api_id or not api_hash:
        raise RuntimeError(
            "API_ID and API_HASH must be set to use userbot fallback"
        )
    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        raise RuntimeError("API_ID must be an integer")
    return api_id, api_hash


def normalize_target(chat_id: int | str) -> int | str:

    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id
