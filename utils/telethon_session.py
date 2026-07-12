import os
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


def build_telethon_client(api_id: int, api_hash: str):
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Install telethon to use userbot fallback.")

    session_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )

    if session_str and StringSession is not None:
        try:
            return TelegramClient(StringSession(session_str), api_id, api_hash)
        except Exception:
            logger.exception("Failed to load StringSession from env; falling back to file-based session")

    session_path = get_telethon_session_path()
    return TelegramClient(session_path, api_id, api_hash)


def get_pyrogram_session_string() -> Optional[str]:
    """Return a Pyrogram session string from env vars, or None."""
    return _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )


def build_pyrogram_client(api_id: int, api_hash: str) -> Optional[object]:
    """Build a Pyrogram client from a session string env var.

    Returns a started Pyrogram Client if PYROGRAM_SESSION is set, otherwise None.
    The caller must call client.start() before using it.
    """
    if PyrogramClient is None:
        logger.debug("Pyrogram is not installed; cannot use Pyrogram session string.")
        return None

    session_str = get_pyrogram_session_string()
    if not session_str:
        return None

    try:
        client = PyrogramClient(
            "pyrogram_userbot_session",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
            in_memory=True,
        )
        return client
    except Exception:
        logger.exception("Failed to create Pyrogram client from session string")
        return None


def is_pyrogram_available() -> bool:
    """Return True if Pyrogram is installed and a session string is configured."""
    return PyrogramClient is not None and bool(get_pyrogram_session_string())


def has_usable_telethon_session() -> bool:
    """Return True when Telethon can use a pre-existing session without prompting for login."""
    if TelegramClient is None:
        return False

    session_str = _get_env_value(
        "API_SESSION", "SESSION", "api_session", "USERBOT_SESSION",
        "userbot_session", "TELETHON_SESSION", "telethon_session",
    )
    if session_str:
        return True

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
