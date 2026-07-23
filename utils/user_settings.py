import json
import os
from typing import Dict, Any

import config


DEFAULTS = {
    "upload_mode": "video",  # options: video, file, zip
    "prefix": "",
    "suffix": "",
    "words_remove": [],
    "save_thumbnail": False,
    "default_thumbnail": None,  # path or URL
    "bulk_mode": False,  # when True, treat pasted URL lists as bulk uploads
    "use_custom_thumbnail": False,  # when True, use per-user custom thumbnail if set
}


def _settings_path() -> str:
    path = getattr(config, "STORAGE_PATH", "storage")
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass
    return os.path.join(path, "user_settings.json")


def _load_all() -> Dict[str, Any]:
    p = _settings_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_all(data: Dict[str, Any]) -> None:
    p = _settings_path()
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass


def get_user_settings(user_id: int) -> Dict[str, Any]:
    all_s = _load_all()
    s = all_s.get(str(user_id), {})
    result = DEFAULTS.copy()
    result.update(s or {})
    return result


def set_user_setting(user_id: int, key: str, value) -> None:
    all_s = _load_all()
    uid = str(user_id)
    user_s = all_s.get(uid, {})
    user_s[key] = value
    all_s[uid] = user_s
    _save_all(all_s)


def get_user_setting(user_id: int, key: str, default=None):
    s = get_user_settings(user_id)
    return s.get(key, default)


def toggle_user_setting(user_id: int, key: str) -> bool:
    """Toggle a boolean user setting and return the new value."""
    s = get_user_settings(user_id)
    current = bool(s.get(key))
    new = not current
    set_user_setting(user_id, key, new)
    return new


def clear_user_settings(user_id: int) -> None:
    all_s = _load_all()
    uid = str(user_id)
    if uid in all_s:
        del all_s[uid]
        _save_all(all_s)
