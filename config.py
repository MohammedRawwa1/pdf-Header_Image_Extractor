import json
import logging
import os
import re

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


def _load_environment_file() -> None:
    if load_dotenv is None:
        return
    _root = os.path.dirname(os.path.abspath(__file__))
    candidates = [os.path.join(_root, ".env"), os.path.join(os.getcwd(), ".env"), os.path.join(_root, ".env.local")]
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.exists(candidate):
            try:
                load_dotenv(candidate, override=False)
            except Exception:
                pass


_load_environment_file()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "")
if not WEBHOOK_URL:
    _railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if _railway_domain:
        WEBHOOK_URL = f"https://{_railway_domain}"
USE_POLLING: bool = os.getenv("USE_POLLING", "false").lower() in ("1", "true", "yes")
HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "8000"))

OWNER_ID: int = 0
_owner_raw = os.getenv("OWNER_ID", "")
if _owner_raw:
    try:
        OWNER_ID = int(_owner_raw)
    except ValueError:
        pass

ADMIN_USERS_RAW = os.getenv("ADMIN_USERS", "")
ADMIN_USERS: set[int] = set()
if ADMIN_USERS_RAW:
    for part in ADMIN_USERS_RAW.split(","):
        part = part.strip()
        if part:
            try:
                ADMIN_USERS.add(int(part))
            except ValueError:
                pass

ADMIN_SECRET: str = os.getenv("ADMIN_SECRET", "")
LOG_CHANNEL: str = os.getenv("LOG_CHANNEL", "")
SENTRY_DSN: str = os.getenv("SENTRY_DSN", "")
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", str(0)))

BOT_API_MAX_MB: int = int(os.getenv("BOT_API_MAX_MB", "50"))
BOT_API_DOWNLOAD_MAX_MB: int = int(os.getenv("BOT_API_DOWNLOAD_MAX_MB", "20"))
_BOT_API_MB_EFFECTIVE = max(1, BOT_API_MAX_MB)
_BOT_API_DL_MB_EFFECTIVE = max(1, BOT_API_DOWNLOAD_MAX_MB)


def _cap_by_max_file_size(limit_bytes: int) -> int:
    if MAX_FILE_SIZE and MAX_FILE_SIZE > 0:
        return min(limit_bytes, MAX_FILE_SIZE)
    return limit_bytes


BOT_API_UPLOAD_LIMIT_BYTES: int = _cap_by_max_file_size(_BOT_API_MB_EFFECTIVE * 1024 * 1024)
BOT_API_DOWNLOAD_LIMIT_BYTES: int = _cap_by_max_file_size(_BOT_API_DL_MB_EFFECTIVE * 1024 * 1024)

TMP_DIR: str = os.getenv("TMP_DIR", "")
PDF_COMPRESS_QUALITY: str = os.getenv("PDF_COMPRESS_QUALITY", "/ebook")
COMPRESS_MIN_GAIN_PCT: float = float(os.getenv("COMPRESS_MIN_GAIN_PCT", "5"))
COMPRESS_MIN_GAIN_BYTES: int = int(os.getenv("COMPRESS_MIN_GAIN_BYTES", "100000"))
# Post-render PDF shrink pass (utils/weasyprint_converter.py): when a rendered
# PDF still approaches Telegram's upload limit, embedded JPEG/PNG images are
# re-encoded to JPEG at a lower quality. The threshold defaults to 60% of the
# upload limit (30MB at the default 50MB); set PDF_RECOMPRESS_MIN_MB to override.
PDF_RECOMPRESS_MIN_MB: float = float(os.getenv("PDF_RECOMPRESS_MIN_MB", "0"))
PDF_RECOMPRESS_MIN_BYTES: int = max(
    1,
    int(PDF_RECOMPRESS_MIN_MB * 1024 * 1024)
    if PDF_RECOMPRESS_MIN_MB > 0
    else int(BOT_API_UPLOAD_LIMIT_BYTES * 0.6),
)
PDF_RECOMPRESS_JPEG_QUALITY: int = int(os.getenv("PDF_RECOMPRESS_JPEG_QUALITY", "70"))
PDF_RECOMPRESS_MIN_GAIN_PCT: float = float(os.getenv("PDF_RECOMPRESS_MIN_GAIN_PCT", "5"))
PDF_RECOMPRESS_MIN_GAIN_BYTES: int = int(os.getenv("PDF_RECOMPRESS_MIN_GAIN_BYTES", "100000"))
# Post-render sanity scan (utils/weasyprint_converter.py): wall-clock cap on
# inspecting the rendered PDF (seconds), and the overflow threshold (pt) beyond
# which an image sticking out of the page box still routes the render to the
# Calibre fallback. Smaller overflows are logged and accepted so a borderline
# render ships instead of dying in the fallback.
PDF_SANITY_CHECK_BUDGET_S: float = float(os.getenv("PDF_SANITY_CHECK_BUDGET_S", "15"))
PDF_OVERFLOW_FATAL_PT: float = float(os.getenv("PDF_OVERFLOW_FATAL_PT", "30"))
# Calibre fallback memory guard (utils/ebook_converter.py): ebook-convert is
# memory-hungry, and spawning it while the worker still holds a WeasyPrint
# render previously OOM-killed the worker (SIGKILL, no output delivered). The
# guard refuses the spawn when current RSS + reserve would exceed the cgroup
# memory limit. Set CALIBRE_FALLBACK_MAX_RSS_MB for an absolute cap; set both
# to 0 to disable the guard entirely.
CALIBRE_FALLBACK_MEM_RESERVE_MB: float = float(os.getenv("CALIBRE_FALLBACK_MEM_RESERVE_MB", "200"))
CALIBRE_FALLBACK_MAX_RSS_MB: float = float(os.getenv("CALIBRE_FALLBACK_MAX_RSS_MB", "0"))

ALLOWED_FORMATS_RAW: str = os.getenv("ALLOWED_FORMATS", "")
ALLOWED_FORMATS: set[str] = {p.strip().lower().lstrip(".") for p in ALLOWED_FORMATS_RAW.split(",") if p.strip()}
if not ALLOWED_FORMATS:
    try:
        from utils.ebook_converter import DEFAULT_ALLOWED_FORMATS
        ALLOWED_FORMATS = set(DEFAULT_ALLOWED_FORMATS)
    except Exception:
        ALLOWED_FORMATS = set()

ENABLE_BOOK_CONVERSION: bool = os.getenv("ENABLE_BOOK_CONVERSION", "true").lower() in ("1", "true", "yes")
BOOK_CONVERT_TIMEOUT_SECONDS: int = int(os.getenv("BOOK_CONVERT_TIMEOUT_SECONDS", "600"))
BOOK_ASK_TTL_SECONDS: int = int(os.getenv("BOOK_ASK_TTL_SECONDS", "600"))
EPUB_FAST_CONVERT_ENABLED: bool = os.getenv("EPUB_FAST_CONVERT_ENABLED", "true").lower() in ("1", "true", "yes")
EPUB_EMPTY_MERGE_FALLBACK_SECONDS: int = int(os.getenv("EPUB_EMPTY_MERGE_FALLBACK_SECONDS", str(240)))
# WeasyPrint fast-path page geometry (utils/weasyprint_converter.py). Any CSS
# size works: "A4", "Letter", "6in 9in", "210mm 297mm", "A4 landscape".
EPUB_PAGE_SIZE: str = os.getenv("EPUB_PAGE_SIZE", "A4")
EPUB_PAGE_MARGIN: str = os.getenv("EPUB_PAGE_MARGIN", "15mm")
# Raster images wider than this (px) are re-encoded at ~2x the printable page
# width before rendering so giant scans don't bloat the PDF past Telegram's
# upload limit; smaller images keep their original pixels.
EPUB_IMAGE_DOWNSAMPLE_MIN_WIDTH_PX: int = int(os.getenv("EPUB_IMAGE_DOWNSAMPLE_MIN_WIDTH_PX", "3000"))
EPUB_IMAGE_JPEG_QUALITY: int = int(os.getenv("EPUB_IMAGE_JPEG_QUALITY", "85"))

ENABLE_OCR: bool = os.getenv("ENABLE_OCR", "true").lower() in ("1", "true", "yes")
OCR_LANG: str = os.getenv("OCR_LANG", "eng")
OCR_TIMEOUT_SECONDS: int = int(os.getenv("OCR_TIMEOUT_SECONDS", "600"))
OCR_DPI: int = int(os.getenv("OCR_DPI", "200"))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "local")
STORAGE_PATH: str = os.getenv("STORAGE_PATH", os.path.join(ROOT_DIR, "storage"))
INPUT_PATH: str = os.getenv("INPUT_PATH", os.path.join(STORAGE_PATH, "input"))
OUTPUT_PATH: str = os.getenv("OUTPUT_PATH", os.path.join(STORAGE_PATH, "output"))
TEMP_PATH: str = os.getenv("TEMP_PATH", os.path.join(STORAGE_PATH, "temp"))
THUMBNAIL_PATH: str = os.getenv("THUMBNAIL_PATH", os.path.join(STORAGE_PATH, "thumbnails"))

ENABLE_S3_FALLBACK: bool = os.getenv("ENABLE_S3_FALLBACK", "false").lower() in ("1", "true", "yes")
S3_BUCKET: str = os.getenv("S3_BUCKET", "")
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_REGION: str = os.getenv("S3_REGION", "")
S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "")
S3_PRESIGNED_EXPIRY: int = int(os.getenv("S3_PRESIGNED_EXPIRY", "3600"))
S3_SIGNATURE_VERSION: str = os.getenv("S3_SIGNATURE_VERSION", "s3v4")
S3_USE_SSL: bool = os.getenv("S3_USE_SSL", "1") not in ("0", "false", "False", "no")
PRESIGN_EXPIRES: int = int(os.getenv("PRESIGN_EXPIRES", "3600"))

RELAY_CHAT_ID: str = os.getenv("RELAY_CHAT_ID", "")

_ENABLE_USERBOT_EXPLICIT = os.getenv("ENABLE_USERBOT", "").lower()
ENABLE_USERBOT: bool = (
    _ENABLE_USERBOT_EXPLICIT in ("1", "true", "yes")
    or bool(os.getenv("API_ID") or os.getenv("api_id") or os.getenv("USERBOT_API_ID") or os.getenv("userbot_api_id"))
)

_allowed_file = os.path.join(STORAGE_PATH, "allowed_users.json")


def _load_allowed_users() -> set[int]:
    s: set[int] = set()
    env_val = os.getenv("ALLOWED_USER_IDS", "")
    if env_val:
        for part in env_val.split(","):
            try:
                s.add(int(part.strip()))
            except ValueError:
                continue
    try:
        if os.path.exists(_allowed_file):
            with open(_allowed_file, encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    for v in data:
                        try:
                            s.add(int(v))
                        except ValueError:
                            continue
    except Exception:
        logger.debug("Failed to load allowed users from file")
    return s


ALLOWED_USER_IDS: set[int] = _load_allowed_users()


def persist_allowed_users() -> None:
    try:
        os.makedirs(STORAGE_PATH, exist_ok=True)
        with open(_allowed_file, "w", encoding="utf-8") as fh:
            json.dump(sorted(list(ALLOWED_USER_IDS)), fh)
    except Exception:
        logger.debug("Failed to persist allowed users to file")


def _parse_optional_int(val: str | None) -> int | None:
    try:
        if val is None or val == "":
            return None
        return int(val)
    except ValueError:
        return None


ADMIN_USER_ID: int | None = _parse_optional_int(os.getenv("ADMIN_USER_ID", ""))


def is_user_allowed(user_id: int) -> bool:
    try:
        if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
            return True
        if not ALLOWED_USER_IDS:
            return True
        return user_id in ALLOWED_USER_IDS
    except Exception:
        logger.warning("ACL check failed for user %s; defaulting to allowed", user_id)
        return True


def _resolve_env_reference(val: str | None) -> str | None:
    if not val:
        return val
    v = val.strip()
    m = re.match(r"^\$(\w+)$", v) or re.match(r"^\$\{(\w+)\}$", v)
    if m:
        return os.getenv(m.group(1)) or None

    def _repl(m):
        return os.getenv(m.group(1), "")

    try:
        substituted = re.sub(r"\$\{?(\w+)\}?", _repl, v)
    except Exception:
        substituted = v
    return substituted or None


_canonical_mongo = None
for _key in ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL"):
    _raw = os.getenv(_key)
    if not _raw:
        continue
    _resolved = _resolve_env_reference(_raw)
    if _resolved:
        _canonical_mongo = _resolved
        break

if _canonical_mongo:
    os.environ.setdefault("MONGO_URI", _canonical_mongo)
    os.environ.setdefault("MONGODB_URL", _canonical_mongo)
    os.environ.setdefault("MONGO_URL", _canonical_mongo)
    os.environ.setdefault("MONGODB_URI", _canonical_mongo)


def is_admin_user(user_id: int) -> bool:
    if user_id is None:
        return False
    uid = int(user_id)
    if OWNER_ID and uid == OWNER_ID:
        return True
    if ADMIN_USER_ID and uid == ADMIN_USER_ID:
        return True
    if ADMIN_USERS:
        return uid in ADMIN_USERS
    return False


def is_owner(user_id: int) -> bool:
    if user_id is None:
        return False
    if OWNER_ID:
        return int(user_id) == OWNER_ID
    return is_admin_user(user_id)
