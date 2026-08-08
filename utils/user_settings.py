import json
import os
from typing import Any

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
    # OCR output default when tapping 🔎 OCR: "" = always show the picker,
    # "pdf" = skip the picker and go straight to Searchable PDF, "txt" =
    # skip the picker and go straight to plain text.
    "ocr_target": "",
}


def _settings_path() -> str:
    path = getattr(config, "STORAGE_PATH", "storage")
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:  # nosec B110
        pass
    return os.path.join(path, "user_settings.json")


def _load_all() -> dict[str, Any]:
    p = _settings_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_all(data: dict[str, Any]) -> None:
    p = _settings_path()
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    except Exception:  # nosec B110
        pass


def get_user_settings(user_id: int) -> dict[str, Any]:
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
