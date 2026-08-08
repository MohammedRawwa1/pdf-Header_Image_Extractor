import io
import logging
import os
import shutil
import subprocess  # nosec B404 - intentional, needed for Ghostscript PDF compression

from PIL import Image

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise ImportError(
        "PyMuPDF (fitz) is required. Install with 'pip install PyMuPDF'"
    ) from e


# ── Supported file format definitions ─────────────────────────────────
# Only these MIME types and file extensions are accepted for processing.
# Video formats (MKV, AVI, MP4, MOV, etc.) sent as documents are rejected
# early to avoid unnecessary relay forwarding and thumbnail processing.
SUPPORTED_MIME_TYPES: set[str] = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/tiff",
    "image/x-tiff",
}

SUPPORTED_EXTENSIONS: set[str] = {
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".bmp",
    ".tiff",
    ".tif",
}

# Video MIME types that are commonly sent as documents on Telegram
# These are explicitly blocked and logged for visibility
VIDEO_MIME_PREFIXES: tuple[str, ...] = (
    "video/",
)

# Stripped image extensions (no leading dot) — hoisted for infer_extension.
_IMAGE_EXT_STRIPPED: frozenset[str] = frozenset(
    e.lstrip(".") for e in SUPPORTED_EXTENSIONS
)

# Book/ebook MIME types → the canonical extension Calibre expects.  Used to
# (a) accept documents that arrive WITHOUT a filename (Telegram allows it —
# the bot then sees only ``file_<id>`` with no extension) and (b) give such
# files the extension they need before media detection / conversion runs.
# MIME is matched case-insensitively against this map.  Every mapped
# extension is one Calibre can actually READ — ``application/msword``
# (legacy ``.doc``) is deliberately absent because Calibre cannot read ``doc``
# and renaming it ``.docx`` would just manufacture a confusing failure.
BOOK_MIME_TO_EXT: dict[str, str] = {
    "application/epub+zip": "epub",
    "application/x-mobipocket-ebook": "mobi",
    "application/x-mobipocket": "mobi",
    "application/vnd.amazon.ebook": "azw3",
    "application/x-azw3": "azw3",
    "application/vnd.amazon.mobi8-ebook": "azw3",
    "application/x-fictionbook+xml": "fb2",
    "application/x-fictionbook": "fb2",
    "application/rtf": "rtf",
    "text/rtf": "rtf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.oasis.opendocument.text": "odt",
    "application/x-cbz": "cbz",
    "application/vnd.comicbook+zip": "cbz",
    "application/x-cbr": "cbr",
    "application/vnd.comicbook-rar": "cbr",
    "application/x-pdb": "pdb",
    "application/x-snb": "snb",
    "application/x-tcr": "tcr",
    "application/x-mobipocket-prc": "prc",
    "text/plain": "txt",
    "text/html": "html",
    "application/xhtml+xml": "html",
}


def _book_ext_for_mime(mime: str) -> str | None:
    """Canonical book extension for ``mime``, or None when not a book MIME."""
    return BOOK_MIME_TO_EXT.get((mime or "").lower().strip())


def infer_extension(filename: str, mime: str = "") -> str:
    """Return ``filename`` with a MIME-derived extension when it lacks one.

    Telegram documents can arrive without a filename (the bot falls back to
    ``file_<id>``, extensionless) or with a container-ish extension (e.g. a
    ZIP-wrapped EPUB).  Book formats are detected by extension, so such files
    would be rejected or misrouted.  This derives the canonical extension
    from the MIME type:

    - no extension + book MIME   -> append ``.epub`` / ``.mobi`` / ...
    - unknown extension + book MIME (``book.zip`` + epub MIME) -> replace
    - already-known extension     -> untouched (content sniffing is Calibre's job)
    """
    if not filename:
        return filename
    mapped = _book_ext_for_mime(mime)
    if not mapped:
        return filename
    base, ext = os.path.splitext(filename.strip())
    _ext = ext.lower().lstrip(".")
    if not ext:
        return f"{filename}.{mapped}"
    if _ext in _IMAGE_EXT_STRIPPED:
        return filename
    # Extension is unknown/container-like for this MIME → swap in the real one.
    try:
        import config as _cfg

        if _ext in _cfg.ALLOWED_FORMATS:
            return filename
    except Exception:  # nosec B110 - config is always present
        pass
    return f"{base}.{mapped}"


def is_supported_format(filename: str, mime: str = "") -> bool:
    """Check whether the given filename/MIME pair is a supported format.

    Uses the file extension as the primary signal (most reliable when users
    send documents with meaningful names) and the MIME type as secondary.

    Returns True for supported formats (PDF, images), False for everything
    else (video formats, audio, archives, etc.).
    """
    # 1) Check MIME type first (most authoritative when present)
    if mime:
        mime_lower = mime.lower().strip()
        # Explicitly reject video formats early
        if mime_lower.startswith(VIDEO_MIME_PREFIXES):
            return False
        if mime_lower in SUPPORTED_MIME_TYPES:
            return True
        # Some image subtypes like image/x-* can slip through — allow them
        if mime_lower.startswith("image/"):
            return True
        # E-book MIME types (EPUB/MOBI/FB2/DOCX/...) are accepted even when
        # the filename carries no extension — Telegram allows nameless
        # documents, and the bot derives a real extension before processing.
        if _book_ext_for_mime(mime_lower):
            return True

    # 2) Fall back to file extension check
    if filename:
        _, ext = os.path.splitext(filename.lower().strip())
        if ext in SUPPORTED_EXTENSIONS:
            return True
        # 2b) Book-conversion formats (from ALLOWED_FORMATS env) are also
        # accepted — they route through the convert flow instead of the
        # PDF/image thumbnail pipeline.
        try:
            import config as _cfg

            if (
                _cfg.ENABLE_BOOK_CONVERSION
                and ext.lstrip(".") in _cfg.ALLOWED_FORMATS
            ):
                return True
        except Exception:  # nosec B110 - config is always present
            pass

    # 3) Unknown format — reject
    return False


def is_valid_pdf(file_path: str) -> bool:
    """Check if a file is a valid PDF by attempting to open it with PyMuPDF.

    Returns True if the file opens successfully as a PDF, False otherwise.
    Does NOT raise exceptions.
    """
    try:
        doc = fitz.open(file_path)
        doc.close()
        return True
    except Exception:
        return False


def extract_pdf_metadata(pdf_path: str) -> dict:
    """Extract full PDF metadata via PyMuPDF (best-effort, never raises).

    Returns a dict with ``extracted`` flag, page count, encryption status,
    document info fields (title/author/subject/keywords/creator/producer/
    creation date/modification date) and file size.
    """
    meta = {"extracted": False, "pages": 0}
    try:
        # Magic-byte check first: PyMuPDF happily opens text files as "Tex"
        # pseudo-documents, so a .txt/.log file would otherwise be reported
        # as an extracted "PDF" with a bogus page count.
        with open(pdf_path, "rb") as fh:
            if not fh.read(5).startswith(b"%PDF-"):
                return meta
        doc = fitz.open(pdf_path)
        try:
            md = doc.metadata or {}
            meta = {
                "extracted": True,
                "pages": doc.page_count,
                "encrypted": bool(doc.is_encrypted or doc.needs_pass),
                "title": (md.get("title") or "").strip() or None,
                "author": (md.get("author") or "").strip() or None,
                "subject": (md.get("subject") or "").strip() or None,
                "keywords": (md.get("keywords") or "").strip() or None,
                "creator": (md.get("creator") or "").strip() or None,
                "producer": (md.get("producer") or "").strip() or None,
                "creation_date": (md.get("creationDate") or "").strip() or None,
                "modification_date": (md.get("modDate") or "").strip() or None,
            }
            try:
                meta["file_size"] = os.path.getsize(pdf_path)
            except Exception:  # nosec B110
                pass
        finally:
            doc.close()
    except Exception:  # nosec B110 - metadata is best-effort
        pass
    return meta


def create_thumbnail_from_pdf(pdf_path: str, thumb_path: str) -> None:
    doc = fitz.open(pdf_path)
    page = doc.load_page(0)
    zoom = 2  # render at 2x for better quality
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    # save initial rendering and then optimize to meet Telegram thumbnail constraints
    pix.save(thumb_path)
    _optimize_thumbnail(thumb_path)


def create_thumbnail_from_pdf_bytes(pdf_bytes: bytes) -> bytes:
    """Render first page of a PDF (bytes) and return optimized JPEG bytes."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc.load_page(0)
    zoom = 2
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    # convert pixmap to PIL Image
    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, (pix.width, pix.height), pix.samples)
    if img.mode == "RGBA":
        img = img.convert("RGB")
    img.thumbnail((320, 320), Image.LANCZOS)

    return _optimize_thumbnail_bytes(img)


def create_thumbnail_from_image(image_path: str, thumb_path: str) -> None:
    im = Image.open(image_path)
    im.thumbnail((320, 320))
    im.save(thumb_path, "JPEG", quality=85)
    _optimize_thumbnail(thumb_path)


def create_thumbnail_from_image_bytes(image_bytes: bytes) -> bytes:
    """Create a JPEG thumbnail (bytes) from image bytes."""
    im = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    im.thumbnail((320, 320), Image.LANCZOS)
    return _optimize_thumbnail_bytes(im)


def _optimize_thumbnail(
    path: str, max_bytes: int = 200 * 1024, max_size=(320, 320)
) -> None:
    """Ensure thumbnail is JPEG, within max dimensions and under max_bytes.

    Modifies file at `path` in-place.
    """
    try:
        im = Image.open(path).convert("RGB")
    except Exception:
        return

    im.thumbnail(max_size, Image.LANCZOS)

    # try progressive quality reduction
    q = 90
    tmp_path = f"{path}.tmp"
    while q >= 20:
        try:
            im.save(tmp_path, "JPEG", quality=q, optimize=True)
            size = os.path.getsize(tmp_path)
            if size <= max_bytes or q <= 30:
                # replace original
                os.replace(tmp_path, path)
                return
        except Exception:  # nosec B110
            pass
        q -= 10

    # fallback: save with low quality
    try:
        im.save(path, "JPEG", quality=30, optimize=True)
    except Exception:  # nosec B110
        pass


def _optimize_thumbnail_bytes(
    im: Image.Image, max_bytes: int = 200 * 1024
) -> bytes:
    """Return JPEG bytes for PIL Image `im`, optimized to be under `max_bytes` when possible."""
    buf = io.BytesIO()
    q = 90
    while q >= 20:
        try:
            buf.seek(0)
            buf.truncate()
            im.save(buf, "JPEG", quality=q, optimize=True)
            size = buf.tell()
            if size <= max_bytes or q <= 30:
                buf.seek(0)
                return buf.read()
        except Exception:  # nosec B110
            pass
        q -= 10

    # fallback low quality
    try:
        buf.seek(0)
        buf.truncate()
        im.save(buf, "JPEG", quality=30, optimize=True)
        buf.seek(0)
        return buf.read()
    except Exception:
        return b""


def _validate_path_safe(path: str) -> bool:
    """Validate that a file path doesn't contain path traversal sequences."""
    # Check for path traversal BEFORE normalization, because normpath resolves `..`
    normalized_sep = path.replace("\\", "/").split("/")
    if ".." in normalized_sep:
        return False
    # Ensure the normalized path is absolute (paths from tempfile.mkdtemp are absolute)
    normalized = os.path.normpath(path)
    return os.path.isabs(normalized)


def compress_pdf(
    input_path: str, output_path: str, gs_quality: str = "/ebook"
) -> bool:
    """Try to compress a PDF file.

    Strategy:
    1. Try Ghostscript (`gs`) with `-dPDFSETTINGS` (fast, effective when available).
    2. Fallback to PyMuPDF `Document.save(..., deflate=True, garbage=4)` which attempts
       to compress streams.

    Returns True if `output_path` was created (and may be smaller), False on failure.
    """
    # Validate paths to prevent command injection / path traversal
    if not _validate_path_safe(input_path) or not _validate_path_safe(
        output_path
    ):
        logger = logging.getLogger(__name__)
        logger.warning(
            "compress_pdf: path validation failed for input=%s output=%s",
            input_path,
            output_path,
        )
        return False

    # Validate gs_quality is one of the expected Ghostscript presets
    _VALID_GS_QUALITIES = {
        "/screen",
        "/ebook",
        "/printer",
        "/prepress",
        "/default",
    }
    if gs_quality not in _VALID_GS_QUALITIES:
        logger = logging.getLogger(__name__)
        logger.warning(
            "compress_pdf: invalid gs_quality=%s, using /ebook", gs_quality
        )
        gs_quality = "/ebook"

    # Remove any existing output
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
    except Exception:  # nosec B110
        pass

    # 1) Ghostscript: try common executable names (Linux/macOS: 'gs', Windows: 'gswin64c'/'gswin32c')

    gs_candidates = ["gs", "gswin64c", "gswin32c"]
    for gs_exe in gs_candidates:
        gs_path = shutil.which(gs_exe)
        if not gs_path:
            continue
        # Use list form (not string) to avoid shell injection
        gs_cmd = [
            gs_path,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={gs_quality}",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={output_path}",
            input_path,
        ]
        try:
            subprocess.run(
                gs_cmd,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=180,
            )  # nosec B603 - uses whitelisted exe names + list form (no shell injection)
            return os.path.exists(output_path)
        except subprocess.CalledProcessError:
            # Ghostscript ran but failed for this candidate; try next candidate
            continue
        except Exception:  # nosec B112
            # Could be permission/timeout/etc. Try next candidate
            continue

    # 2) PyMuPDF fallback (best-effort)
    try:
        import fitz

        doc = fitz.open(input_path)
        # Save with stream deflation and garbage collection to reduce size
        doc.save(output_path, deflate=True, garbage=4, clean=True)
        doc.close()
        return os.path.exists(output_path)
    except Exception:
        return False
