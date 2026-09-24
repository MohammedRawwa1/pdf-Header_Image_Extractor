import logging
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable

from defusedxml import ElementTree as _DefusedET

logger = logging.getLogger(__name__)


class ConversionCancelledError(Exception):
    pass


class DRMProtectedError(Exception):
    pass


def _rss_mb() -> float:
    """Resident set size of this process in MB (0.0 when unknown)."""
    try:
        with open(
            "/proc/self/status", encoding="utf-8", errors="replace"
        ) as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return 0.0


def _container_mem_limit_mb() -> float:
    """cgroup memory limit in MB (0.0 when unlimited/unknown)."""
    try:
        with open(
            "/sys/fs/cgroup/memory.max", encoding="utf-8", errors="replace"
        ) as f:  # cgroup v2
            val = f.read().strip()
        if val and val != "max":
            return float(val) / (1024.0 * 1024.0)
    except Exception:
        pass
    try:
        with open(
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            encoding="utf-8",
            errors="replace",
        ) as f:  # cgroup v1
            val = f.read().strip()
        if val:
            return float(val) / (1024.0 * 1024.0)
    except Exception:
        pass
    return 0.0


def _calibre_memory_guard_failed(input_path: str) -> bool:
    """True when spawning ebook-convert would likely OOM-kill the worker.

    ebook-convert is memory-hungry (loads the whole book); on a small
    container, a work horse already holding a WeasyPrint render can push the
    cgroup over its limit, and the kernel SIGKILLs the worker ("waitpid
    returned 9") with no output ever delivered. This refuses the spawn -- the
    job then fails with a clear error instead of dying -- when current RSS +
    CALIBRE_FALLBACK_MEM_RESERVE_MB would exceed the container limit. Set
    CALIBRE_FALLBACK_MAX_RSS_MB for an absolute cap, or both to 0 to disable.
    """
    try:
        import config as _cfg

        reserve_mb = float(
            getattr(_cfg, "CALIBRE_FALLBACK_MEM_RESERVE_MB", 0) or 0
        )
        max_rss_mb = float(
            getattr(_cfg, "CALIBRE_FALLBACK_MAX_RSS_MB", 0) or 0
        )
    except Exception:
        return False
    if max_rss_mb <= 0:
        limit_mb = _container_mem_limit_mb()
        if limit_mb <= 128.0 or reserve_mb <= 0.0:
            return False  # unknown/unlimited limit, or guard disabled
        max_rss_mb = limit_mb - reserve_mb
    if max_rss_mb <= 0:
        return False
    rss_mb = _rss_mb()
    if rss_mb <= 0.0:
        return False  # cannot measure; do not block
    if rss_mb >= max_rss_mb:
        logger.warning(
            "convert_ebook: skipping Calibre fallback for %s (RSS %.0fMB >= %.0fMB cap); "
            "spawning ebook-convert would risk OOM-killing the worker",
            os.path.basename(input_path),
            rss_mb,
            max_rss_mb,
        )
        return True
    return False


_DRM_DOCTYPE_RE = re.compile(
    rb"<!DOCTYPE(?:\s+[^>\[\]]*)?(?:\[[^\]]*\])?[^>]*>",
    re.IGNORECASE | re.DOTALL,
)
_FONT_EXTS: tuple[str, ...] = (
    ".ttf",
    ".otf",
    ".ttc",
    ".woff",
    ".woff2",
    ".eot",
    ".pfb",
    ".pfm",
    ".dfont",
)


def epub_is_drm_protected(epub_path: str) -> bool:
    has_rights = False
    try:
        with zipfile.ZipFile(epub_path) as zf:
            names = {n.lower() for n in zf.namelist()}
            has_rights = "meta-inf/rights.xml" in names
            if "meta-inf/encryption.xml" not in names:
                return False
            try:
                raw = zf.read("META-INF/encryption.xml")
            except KeyError:
                return False
    except Exception:
        return False
    try:
        root = _DefusedET.fromstring(_DRM_DOCTYPE_RE.sub(b"", raw))
        content_exts = (".xhtml", ".html", ".htm")
        found_any_ref = False
        for ref in root.findall(".//{*}CipherReference"):
            uri = (ref.get("URI") or "").lower()
            uri = uri.split("#", 1)[0].split("?", 1)[0].rstrip("/")
            if not uri:
                continue
            if uri.endswith(content_exts):
                return True
            if uri.endswith(_FONT_EXTS):
                continue
            found_any_ref = True
        if has_rights and found_any_ref:
            return True
    except Exception:
        logger.warning(
            "epub_is_drm_protected: unparseable encryption.xml in %s",
            os.path.basename(epub_path),
        )
    return False


_KINDLE_FORMATS = {"mobi", "azw", "azw3", "prc"}


def _kindle_is_drm_protected(book_path: str) -> bool:
    try:
        with open(book_path, "rb") as fh:
            head = fh.read(256)
    except Exception:
        return False
    if len(head) < 0x4E + 0xA0:
        return False
    if (
        head[0x3C:0x40] != b"BOOK"
        or head[0x40:0x44] != b"MOBI"
        or head[0x4E:0x52] != b"MOBI"
    ):
        return False
    header_len = int.from_bytes(head[0x52:0x56], "big")
    if header_len < 0xA0:
        return False
    drm_offset = int.from_bytes(head[0x4E + 0x98 : 0x4E + 0x9C], "big")
    drm_count = int.from_bytes(head[0x4E + 0x9C : 0x4E + 0xA0], "big")
    return drm_count > 0 and drm_offset != 0xFFFFFFFF


def book_is_drm_protected(book_path: str, ext: str | None = None) -> bool:
    fmt = _normalize_ext(ext or os.path.splitext(book_path)[1])
    if fmt == "epub":
        return epub_is_drm_protected(book_path)
    if fmt in _KINDLE_FORMATS:
        return _kindle_is_drm_protected(book_path)
    return False


_MAGIC_PDF = b"%PDF"
_MAGIC_ZIP = b"PK\x03\x04"
_MAGIC_OLE2 = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def _magic_matches(book_path: str, ext: str) -> bool:
    try:
        with open(book_path, "rb") as fh:
            head = fh.read(1024)
    except Exception:
        return False
    if not head:
        return False
    if ext == "pdf":
        return _MAGIC_PDF in head
    if ext in ("epub", "docx", "odt"):
        return head.startswith(_MAGIC_ZIP)
    if ext == "lit":
        return head.startswith(_MAGIC_OLE2)
    if ext == "rtf":
        return head.lstrip(b"\xef\xbb\xbf ").startswith(b"{\\rtf")
    if ext == "fb2":
        stripped = head.lstrip(b"\xef\xbb\xbf \t\r\n")
        return stripped.startswith((b"<?xml", b"<FictionBook"))
    if ext in _KINDLE_FORMATS:
        return head[0x3C:0x44] == b"BOOKMOBI"
    if ext == "pdb":
        return head[0x3C:0x44] in (b"BOOKMOBI", b"TEXtREAd")
    return True


CALIBRE_INPUT_FORMATS: set[str] = {
    "azw",
    "azw3",
    "azw4",
    "cbz",
    "cbr",
    "cb7",
    "cbc",
    "chm",
    "djvu",
    "docx",
    "epub",
    "fb2",
    "fbz",
    "html",
    "htmlz",
    "kepub",
    "lit",
    "lrf",
    "mobi",
    "odt",
    "pdf",
    "prc",
    "pdb",
    "pml",
    "rb",
    "rtf",
    "snb",
    "tcr",
    "txt",
    "txtz",
}
CALIBRE_OUTPUT_FORMATS: set[str] = {
    "azw3",
    "epub",
    "docx",
    "fb2",
    "htmlz",
    "kepub",
    "lit",
    "lrf",
    "mobi",
    "pdb",
    "pdf",
    "rtf",
    "snb",
    "tcr",
    "txt",
    "txtz",
    "zip",
    "oeb",
}
DEFAULT_ALLOWED_FORMATS: set[str] = {
    "pdf",
    "epub",
    "mobi",
    "azw3",
    "azw",
    "fb2",
    "lit",
    "prc",
    "pdb",
    "docx",
    "rtf",
    "txt",
    "html",
    "odt",
    "snb",
    "tcr",
}


def _normalize_ext(ext: str) -> str:
    return ext.strip().lower().lstrip(".")


def load_allowed_formats(raw: str | None = None) -> set[str]:
    if not raw or not raw.strip():
        return set(DEFAULT_ALLOWED_FORMATS)
    return {_normalize_ext(p) for p in raw.split(",") if _normalize_ext(p)}


def is_book_format(filename: str, allowed: set[str] | None = None) -> bool:
    if not filename:
        return False
    ext = _normalize_ext(os.path.splitext(filename)[1])
    if not ext:
        return False
    return ext in (allowed if allowed is not None else load_allowed_formats())


def calibre_available() -> bool:
    return shutil.which("ebook-convert") is not None


def conversion_targets_for(
    source_ext: str, allowed: set[str] | None = None
) -> list[str]:
    src = _normalize_ext(source_ext)
    allowed = allowed if allowed is not None else load_allowed_formats()
    targets = (allowed & CALIBRE_OUTPUT_FORMATS) - {src}
    order = [
        "pdf",
        "epub",
        "mobi",
        "azw3",
        "fb2",
        "txt",
        "docx",
        "rtf",
        "lit",
        "pdb",
        "snb",
        "tcr",
    ]
    return sorted(
        targets, key=lambda f: (order.index(f) if f in order else 99, f)
    )


def _calibre_env() -> dict:
    env = os.environ.copy()
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    env.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    env.setdefault(
        "QTWEBENGINE_CHROMIUM_FLAGS",
        "--no-sandbox --disable-gpu --disable-dev-shm-usage",
    )
    env.setdefault("QT_QUICK_BACKEND", "software")
    env.setdefault("LIBGL_ALWAYS_SOFTWARE", "1")
    _home = env.get("HOME") or ""
    if not _home or not os.path.isdir(_home) or not os.access(_home, os.W_OK):
        env["HOME"] = tempfile.gettempdir()
    return env


def _validate_path_safe(path: str) -> bool:
    if not path:
        return False
    normalized_sep = path.replace("\\", "/").split("/")
    if ".." in normalized_sep:
        return False
    return os.path.isabs(os.path.normpath(path))


def _terminate_process_group(proc: subprocess.Popen) -> None:
    """Kill a child and its whole process group, then reap it.

    ebook-convert spawns helper processes; killing only the direct child on
    timeout/cancel would leave orphans (and an unreaped zombie).  Sends
    SIGTERM to the group, escalates to SIGKILL if it does not exit, and always
    waits so no zombie is left behind.
    """
    if proc.poll() is not None:
        return

    def _signal(sig: int) -> None:
        try:
            if os.name == "posix":
                try:
                    os.killpg(os.getpgid(proc.pid), sig)
                    return
                except Exception:
                    pass
            proc.kill()
        except Exception:  # nosec B110 - process already gone
            pass

    _signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5.0)
        return
    except Exception:
        pass
    _signal(signal.SIGKILL)
    try:
        proc.wait(timeout=5.0)
    except Exception:  # nosec B110 - best effort reaping
        pass


def convert_ebook(
    input_path: str,
    output_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    if not _validate_path_safe(input_path) or not _validate_path_safe(
        output_path
    ):
        logger.warning(
            "convert_ebook: path validation failed for input=%s output=%s",
            input_path,
            output_path,
        )
        return False
    ext = _normalize_ext(os.path.splitext(input_path)[1])
    if not _magic_matches(input_path, ext):
        logger.warning(
            "convert_ebook: %s does not match its .%s signature; refusing conversion",
            os.path.basename(input_path),
            ext,
        )
        return False
    if book_is_drm_protected(input_path, ext):
        raise DRMProtectedError(f"DRM-protected book: {input_path}")
    exe = shutil.which("ebook-convert")
    if not exe:
        logger.warning("convert_ebook: ebook-convert not found on PATH")
        return False
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
    except Exception:
        pass
    cmd = [exe, input_path, output_path]
    if _calibre_memory_guard_failed(input_path):
        return False
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=_calibre_env(),
            # Own session so a kill takes down ebook-convert's own helper
            # processes too; otherwise a timeout/cancel can orphan them.
            start_new_session=os.name == "posix",
        )
        # Keep only the tail of stderr: the drainer's job is to stop the child
        # blocking on a full pipe, so it must never accumulate the whole log in
        # memory (Calibre can be very chatty on some books).
        _err_chunks: list[bytes] = []
        _err_kept = 0
        _err_max_bytes = 64 * 1024

        def _drain_stderr() -> None:
            nonlocal _err_kept
            if proc is None or proc.stderr is None:
                return
            try:
                for _line in proc.stderr:
                    _err_chunks.append(_line)
                    _err_kept += len(_line)
                    while _err_kept > _err_max_bytes and len(_err_chunks) > 1:
                        _err_kept -= len(_err_chunks.pop(0))
            except Exception:  # nosec B110 - pipe teardown races are harmless
                pass

        _drainer = threading.Thread(target=_drain_stderr, daemon=True)
        _drainer.start()
        _deadline = time.monotonic() + timeout
        while True:
            if cancel_check and cancel_check():
                _terminate_process_group(proc)
                raise ConversionCancelledError(
                    f"conversion cancelled: {input_path}"
                )
            try:
                proc.wait(timeout=1.0)
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() >= _deadline:
                    _terminate_process_group(proc)
                    raise subprocess.TimeoutExpired(cmd, timeout)
        # Let the drainer finish reading the now-closed pipe so the captured
        # tail is complete and the thread does not outlive this call.
        _drainer.join(timeout=2.0)
        if proc.returncode != 0:
            _err = b"".join(_err_chunks)
            logger.warning(
                "convert_ebook: ebook-convert exited %s converting %s: %.500s",
                proc.returncode,
                input_path,
                _err.decode(errors="replace"),
            )
            return False
        return os.path.exists(output_path)
    except subprocess.TimeoutExpired:
        logger.warning(
            "convert_ebook: timed out after %ss converting %s",
            timeout,
            input_path,
        )
        return False
    except ConversionCancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "convert_ebook: failed converting %s -> %s: %s",
            input_path,
            output_path,
            exc,
        )
        return False
    finally:
        if proc is not None and proc.poll() is None:
            _terminate_process_group(proc)


def convert_ebook_robust(
    input_path: str,
    output_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    if convert_ebook(
        input_path, output_path, timeout=timeout, cancel_check=cancel_check
    ):
        return True
    src_ext = _normalize_ext(os.path.splitext(input_path)[1])
    dst_ext = _normalize_ext(os.path.splitext(output_path)[1])
    if src_ext == "epub" or dst_ext == "epub":
        return False
    _dir = os.path.dirname(output_path)
    _own_dir = False
    if not _dir or not os.path.isdir(_dir) or not os.access(_dir, os.W_OK):
        _dir = tempfile.mkdtemp()
        _own_dir = True
    inter = os.path.join(
        _dir, f"_intermediate_{os.getpid()}_{int(time.time() * 1000)}.epub"
    )
    _pivot_deadline = time.monotonic() + timeout

    def _leg_timeout() -> int:
        _left = int(_pivot_deadline - time.monotonic())
        return max(1, min(timeout, _left))

    try:
        logger.info(
            "convert_ebook_robust: direct %s->%s failed for %s; retrying via EPUB intermediate",
            src_ext or "?",
            dst_ext or "?",
            os.path.basename(input_path),
        )
        if not convert_ebook(
            input_path,
            inter,
            timeout=_leg_timeout(),
            cancel_check=cancel_check,
        ):
            return False
        return convert_ebook(
            inter,
            output_path,
            timeout=_leg_timeout(),
            cancel_check=cancel_check,
        )
    finally:
        try:
            if os.path.exists(inter):
                os.remove(inter)
        except Exception:
            pass
        if _own_dir:
            shutil.rmtree(_dir, ignore_errors=True)


def extract_cover_thumbnail(input_path: str, thumb_path: str) -> bool:
    if not _validate_path_safe(input_path) or not _validate_path_safe(
        thumb_path
    ):
        return False
    exe = shutil.which("ebook-meta")
    if not exe:
        return False
    tmp = thumb_path + ".cover"
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
        cmd = [exe, input_path, f"--get-cover={tmp}"]
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
            env=_calibre_env(),
        )
        if not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
            return False
        try:
            from PIL import Image

            im = Image.open(tmp).convert("RGB")
            im.thumbnail((320, 320))
            im.save(thumb_path, "JPEG", quality=85)
        except Exception:
            os.replace(tmp, thumb_path)
        try:
            from tools import thumbnail_is_blank as _thumb_is_blank

            if _thumb_is_blank(thumb_path):
                logger.info(
                    "extract_cover_thumbnail: cover for %s is blank or unreadable; ignoring it",
                    os.path.basename(input_path),
                )
                return False
        except Exception:
            pass
        return os.path.exists(thumb_path)
    except Exception:
        return False
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def convert_book_to_pdf_with_thumbnail(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    if not convert_ebook_robust(
        input_path, pdf_path, timeout=timeout, cancel_check=cancel_check
    ):
        return False
    finalize_cover_thumbnail(input_path, pdf_path, thumb_path)
    return True


def finalize_cover_thumbnail(
    input_path: str, pdf_path: str, thumb_path: str
) -> None:
    if not extract_cover_thumbnail(input_path, thumb_path):
        try:
            from tools import create_thumbnail_from_pdf

            create_thumbnail_from_pdf(pdf_path, thumb_path)
        except Exception:
            pass
    try:
        from tools import thumbnail_is_usable as _thumb_usable

        if not _thumb_usable(thumb_path):
            try:
                os.remove(thumb_path)
            except OSError:
                pass
    except Exception:
        pass


def safe_target_name(filename: str, target_ext: str) -> str:
    base, _ = os.path.splitext(os.path.basename(filename) or "book")
    return f"{base}.{_normalize_ext(target_ext)}"
