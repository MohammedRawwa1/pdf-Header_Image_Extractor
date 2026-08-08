"""E-book conversion via Calibre's ``ebook-convert`` / ``ebook-meta``.

Wraps the Calibre CLI (installed in the Docker image) with a format matrix
derived from Calibre's documented input/output format lists, an env-driven
``ALLOWED_FORMATS`` allow-list, and path-validated subprocess helpers (list
form, no shell, matching ``tools.compress_pdf`` conventions).

Key entry points:
- :func:`is_book_format` — is a filename a convertible book/PDF format?
- :func:`conversion_targets_for` — valid target formats for a source format.
- :func:`convert_ebook` — run ``ebook-convert`` with a timeout.
- :func:`convert_ebook_robust` — direct conversion with an EPUB-intermediate
  fallback for sources that can't convert directly (self-cleaning, invisible
  to the user — a pure blackbox).
- :func:`extract_cover_thumbnail` — best-effort cover image via ``ebook-meta``.
"""

import logging
import os
import shutil
import subprocess  # nosec B404 - intentional, needed for Calibre conversions
import tempfile
import threading
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class ConversionCancelledError(Exception):
    """Raised when a Calibre conversion is aborted via ``cancel_check``.

    Distinct from failure so callers can report "cancelled" (and clean up)
    rather than a generic conversion error after /canceljob fires mid-run.
    """

# ── Calibre format support (from Calibre's conversion docs) ──────────────
# Input formats Calibre can READ.
CALIBRE_INPUT_FORMATS: set[str] = {
    "azw", "azw3", "azw4", "cbz", "cbr", "cb7", "cbc", "chm", "djvu",
    "docx", "epub", "fb2", "fbz", "html", "htmlz", "kepub", "lit", "lrf",
    "mobi", "odt", "pdf", "prc", "pdb", "pml", "rb", "rtf", "snb", "tcr",
    "txt", "txtz",
}
# Output formats Calibre can WRITE.
CALIBRE_OUTPUT_FORMATS: set[str] = {
    "azw3", "epub", "docx", "fb2", "htmlz", "kepub", "lit", "lrf", "mobi",
    "pdb", "pdf", "rtf", "snb", "tcr", "txt", "txtz", "zip", "oeb",
}

# Default allow-list when ALLOWED_FORMATS env is unset: e-reader + document
# formats (the bot's book-conversion scope — comics/scans are excluded).
DEFAULT_ALLOWED_FORMATS: set[str] = {
    "pdf", "epub", "mobi", "azw3", "azw", "fb2", "lit", "prc", "pdb",
    "docx", "rtf", "txt", "html", "odt", "snb", "tcr",
}


def _normalize_ext(ext: str) -> str:
    return ext.strip().lower().lstrip(".")


def load_allowed_formats(raw: str | None = None) -> set[str]:
    """Parse the ALLOWED_FORMATS env value into a set of dotted-less extensions.

    Falls back to :data:`DEFAULT_ALLOWED_FORMATS` when unset/empty.  Unknown
    entries are kept (the env is the operator's source of truth); Calibre
    capability is enforced separately at conversion time.
    """
    if not raw or not raw.strip():
        return set(DEFAULT_ALLOWED_FORMATS)
    return {_normalize_ext(p) for p in raw.split(",") if _normalize_ext(p)}


def is_book_format(filename: str, allowed: set[str] | None = None) -> bool:
    """True when ``filename`` has a book/PDF extension from the allow-list."""
    if not filename:
        return False
    ext = _normalize_ext(os.path.splitext(filename)[1])
    if not ext:
        return False
    return ext in (allowed if allowed is not None else load_allowed_formats())


def calibre_available() -> bool:
    """True when ``ebook-convert`` is on PATH (Calibre installed)."""
    return shutil.which("ebook-convert") is not None


def conversion_targets_for(
    source_ext: str, allowed: set[str] | None = None
) -> list[str]:
    """Valid target formats to convert a ``source_ext`` file to.

    Targets = (allowed formats) ∩ (Calibre output formats), minus the source
    itself, sorted for stable button order.  Formats Calibre can only READ
    (azw/prc/html/odt) never appear as targets.
    """
    src = _normalize_ext(source_ext)
    allowed = allowed if allowed is not None else load_allowed_formats()
    targets = (allowed & CALIBRE_OUTPUT_FORMATS) - {src}
    # Prefer the most common targets first for a friendly button layout.
    order = ["pdf", "epub", "mobi", "azw3", "fb2", "txt", "docx", "rtf",
             "lit", "pdb", "snb", "tcr"]
    return sorted(targets, key=lambda f: (order.index(f) if f in order else 99, f))


def _calibre_env() -> dict:
    """Environment for Calibre subprocesses.

    ``QT_QPA_PLATFORM=offscreen`` keeps headless ``ebook-convert``/
    ``ebook-meta`` from trying to initialize the xcb Qt platform plugin
    (which is not reliably present in slim containers) — conversions run
    without any display or windowing stack.

    EPUB→PDF rendering goes through **Qt WebEngine** (Chromium).  In a
    container that means: the Chromium sandbox must be off
    (``QTWEBENGINE_DISABLE_SANDBOX``), GPU/EGL/GLES2/Vulkan must be disabled
    so the renderer falls back to software drawing, and ``HOME`` must point
    at a *writable* directory — Chromium's credential store dies with
    ``credentials.cc ... Permission denied`` when HOME isn't writable (the
    container runs as ``botuser`` whose HOME defaults to ``/root``).
    """
    env = os.environ.copy()
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    env.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    env.setdefault(
        "QTWEBENGINE_CHROMIUM_FLAGS",
        "--no-sandbox --disable-gpu --disable-dev-shm-usage",
    )
    env.setdefault("QT_QUICK_BACKEND", "software")
    env.setdefault("LIBGL_ALWAYS_SOFTWARE", "1")
    # Chromium needs a writable HOME for its credential store; fall back to a
    # temp dir when the inherited HOME is missing or not writable.
    _home = env.get("HOME") or ""
    if not _home or not os.path.isdir(_home) or not os.access(_home, os.W_OK):
        env["HOME"] = tempfile.gettempdir()
    return env


def _validate_path_safe(path: str) -> bool:
    """Reject path traversal / non-absolute paths (mirrors tools.py)."""
    if not path:
        return False
    normalized_sep = path.replace("\\", "/").split("/")
    if ".." in normalized_sep:
        return False
    return os.path.isabs(os.path.normpath(path))


def convert_ebook(
    input_path: str,
    output_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Convert ``input_path`` to ``output_path`` via ``ebook-convert``.

    Uses the list form (no shell) with path validation, mirroring
    ``tools.compress_pdf``.  ``cancel_check()`` (optional) is polled every
    second while Calibre runs; when it turns True the subprocess is killed
    and :class:`ConversionCancelledError` is raised so /canceljob can abort a
    conversion mid-run instead of waiting out the full timeout.  Returns True
    when the output file was created.
    """
    if not _validate_path_safe(input_path) or not _validate_path_safe(output_path):
        logger.warning(
            "convert_ebook: path validation failed for input=%s output=%s",
            input_path,
            output_path,
        )
        return False
    exe = shutil.which("ebook-convert")
    if not exe:
        logger.warning("convert_ebook: ebook-convert not found on PATH")
        return False
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
    except Exception:  # nosec B110
        pass
    cmd = [exe, input_path, output_path]
    proc = None
    try:
        proc = subprocess.Popen(  # nosec B603 - whitelisted exe + list form
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=_calibre_env(),
        )
        # Drain stderr in a daemon thread: the pipe is never otherwise read
        # during the run, and a full ~64KB pipe buffer would block the child
        # (falsely timing out a legitimate conversion).  Captured lines feed
        # the error log on a non-zero exit.
        _err_chunks: list[bytes] = []

        def _drain_stderr() -> None:
            # ``assert`` would be stripped under ``python -O`` — keep the
            # invariant explicit so the drain thread can never crash-silently
            # on a None pipe.
            if proc is None or proc.stderr is None:
                raise RuntimeError("ebook-convert proc/stderr unavailable")
            for _line in proc.stderr:
                _err_chunks.append(_line)

        _drainer = threading.Thread(target=_drain_stderr, daemon=True)
        _drainer.start()
        _deadline = time.monotonic() + timeout
        while True:
            if cancel_check and cancel_check():
                proc.kill()
                proc.wait()
                raise ConversionCancelledError(
                    f"conversion cancelled: {input_path}"
                )
            try:
                proc.wait(timeout=1.0)
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() >= _deadline:
                    proc.kill()
                    proc.wait()
                    raise subprocess.TimeoutExpired(cmd, timeout)
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
            proc.kill()
            proc.wait()


def convert_ebook_robust(
    input_path: str,
    output_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Convert ``input_path`` → ``output_path`` with a two-step EPUB fallback.

    Most sources convert directly to the requested target, but some formats
    (or mildly nonstandard files — a MOBI with odd metadata, an HTML file
    with relative resource links, a DOCX with exotic styles) fail on the
    direct leg while succeeding when pivoted through **EPUB**, Calibre's
    canonical interchange format.

    When the direct conversion fails and neither end is already EPUB, this
    retries as ``source → intermediate.epub → target``.  The intermediate
    lives beside the output (inside the job's private temp dir) and is
    deleted in ``finally`` — the two-step is fully virtual: no temp file
    leaks, no user-visible stage change, exactly one progress bar.  Returns
    True when ``output_path`` exists.
    """
    if convert_ebook(
        input_path, output_path, timeout=timeout, cancel_check=cancel_check
    ):
        return True
    src_ext = _normalize_ext(os.path.splitext(input_path)[1])
    dst_ext = _normalize_ext(os.path.splitext(output_path)[1])
    if src_ext == "epub" or dst_ext == "epub":
        # Already a direct-to-EPUB (or EPUB-source) attempt — pivoting through
        # EPUB would be the same conversion; nothing gained.
        return False
    _dir = os.path.dirname(output_path)
    _own_dir = False
    if not _dir or not os.path.isdir(_dir) or not os.access(_dir, os.W_OK):
        _dir = tempfile.mkdtemp()
        _own_dir = True
    inter = os.path.join(
        _dir, f"_intermediate_{os.getpid()}_{int(time.time() * 1000)}.epub"
    )
    try:
        logger.info(
            "convert_ebook_robust: direct %s->%s failed for %s; "
            "retrying via EPUB intermediate",
            src_ext or "?",
            dst_ext or "?",
            os.path.basename(input_path),
        )
        if not convert_ebook(
            input_path, inter, timeout=timeout, cancel_check=cancel_check
        ):
            return False
        return convert_ebook(
            inter, output_path, timeout=timeout, cancel_check=cancel_check
        )
    finally:
        try:
            if os.path.exists(inter):
                os.remove(inter)
        except Exception:  # nosec B110 - best-effort intermediate cleanup
            pass
        if _own_dir:
            shutil.rmtree(_dir, ignore_errors=True)


def extract_cover_thumbnail(input_path: str, thumb_path: str) -> bool:
    """Extract a book's embedded cover to ``thumb_path`` via ``ebook-meta``.

    Fast (reads metadata/cover only — no full conversion).  Falls back to
    False when the book has no embedded cover or Calibre is missing, so
    callers can generate a placeholder instead.  Returns True on success.
    """
    if not _validate_path_safe(input_path) or not _validate_path_safe(thumb_path):
        return False
    exe = shutil.which("ebook-meta")
    if not exe:
        return False
    tmp = thumb_path + ".cover"
    try:
        if os.path.exists(tmp):
            os.remove(tmp)
        # ebook-meta's option is --get-cover[=FILE] — the cover path must be
        # part of the SAME token, otherwise the positional is parsed as an
        # extra input file.
        cmd = [exe, input_path, f"--get-cover={tmp}"]
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
            env=_calibre_env(),
        )  # nosec B603 - whitelisted exe + list form
        if not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
            return False
        # Re-encode through PIL to JPEG under Telegram thumbnail limits.
        try:
            from PIL import Image

            im = Image.open(tmp).convert("RGB")
            im.thumbnail((320, 320))
            im.save(thumb_path, "JPEG", quality=85)
        except Exception:  # nosec B110 - best-effort cover thumbnail
            # Cover exists but couldn't re-encode; keep raw JPEG.
            os.replace(tmp, thumb_path)
        # Reject blank/white covers (books without a real cover often yield
        # one) — the caller falls back to a PDF-page preview instead of
        # attaching a white thumbnail.
        try:
            from tools import thumbnail_is_blank as _thumb_is_blank

            if _thumb_is_blank(thumb_path):
                logger.info(
                    "extract_cover_thumbnail: cover for %s is blank or "
                    "unreadable; ignoring it",
                    os.path.basename(input_path),
                )
                return False
        except Exception:  # nosec B110 - blank check is best-effort
            pass
        return os.path.exists(thumb_path)
    except Exception:  # nosec B110 - cover extraction is best-effort
        return False
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:  # nosec B110
            pass


def convert_book_to_pdf_with_thumbnail(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Convert any book to PDF and produce a cover thumbnail.

    Convenience for the "📸 Extract thumbnail" path on non-PDF books: converts
    to PDF (deliverable) and thumbnails from the embedded cover when present
    (falling back to the first rendered PDF page via PyMuPDF).
    ``cancel_check`` is forwarded to ``convert_ebook`` so /canceljob can abort
    mid-conversion.  Returns True when the PDF was produced; thumbnail may be
    absent.
    """
    if not convert_ebook_robust(
        input_path, pdf_path, timeout=timeout, cancel_check=cancel_check
    ):
        return False
    if not extract_cover_thumbnail(input_path, thumb_path):
        # No usable embedded cover: preview the converted PDF itself.  The
        # shared PDF thumbnaller skips leading blank pages, so a Calibre PDF
        # that opens with an empty cover page still gets a real preview
        # instead of a blank white thumbnail.
        try:
            from tools import create_thumbnail_from_pdf

            create_thumbnail_from_pdf(pdf_path, thumb_path)
        except Exception:  # nosec B110 - preview is best-effort
            pass
    # Final guard: a blank/white preview is worse than none.  Drop the thumb
    # so delivery attaches nothing (Telegram shows a plain document icon)
    # instead of a blank white cover after the conversion finishes.
    try:
        from tools import thumbnail_is_usable as _thumb_usable

        if not _thumb_usable(thumb_path):
            try:
                os.remove(thumb_path)
            except OSError:  # nosec B110 - already gone is fine
                pass
    except Exception:  # nosec B110 - best-effort guard
        pass
    return True


def safe_target_name(filename: str, target_ext: str) -> str:
    """Return ``filename`` with its extension replaced by ``target_ext``."""
    base, _ = os.path.splitext(os.path.basename(filename) or "book")
    return f"{base}.{_normalize_ext(target_ext)}"
