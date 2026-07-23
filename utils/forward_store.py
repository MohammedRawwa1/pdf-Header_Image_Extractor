"""Minimal forward store stub for scripts/telethon_ingest.py compatibility.

In the full media_conersion_bot this module persists forward metadata to disk/Redis.
For the PDF header extractor bot, this is a lightweight placeholder that keeps
the import alive without pulling in heavy dependencies.
"""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_STORAGE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "storage", "forwards"
)


def load_forward_metadata(forward_hash: str) -> dict[str, Any] | None:
    """Load saved forward metadata by hash. Returns dict or None."""
    try:
        path = os.path.join(_STORAGE_DIR, f"{forward_hash}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
    except Exception:
        logger.debug(
            "forward_store: failed to load metadata for %s", forward_hash
        )
    return None


def delete_forward_metadata(forward_hash: str) -> bool:
    """Delete saved forward metadata. Returns True if deleted."""
    try:
        path = os.path.join(_STORAGE_DIR, f"{forward_hash}.json")
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception:
        logger.debug(
            "forward_store: failed to delete metadata for %s", forward_hash
        )
    return False
