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


# How many leading pages to scan past a blank first page before giving up on
# a non-blank thumbnail (converted/ebook PDFs sometimes open with an empty
# cover or title page, and a white preview is worse than none).
PDF_THUMB_SCAN_LIMIT = 5


def _luma_is_blank(luma: Image.Image, blank_ratio: float = 0.998) -> bool:
    """True when a grayscale image is a (near-)blank white page.

    A page counts as blank when it has almost no ink (a real render has dark
    content pixels; a blank page has ~0% ink), or when it is a nearly
    uniform LIGHT page — a failed/blank render.  Dark or colorful solid
    fills (e.g. a solid-color book cover) are NOT flagged: they are
    legitimate content.  ``blank_ratio`` is the ink-free pixel threshold.
    """
    w, h = luma.size
    if w < 8 or h < 8:
        return True
    hist = luma.histogram()
    total = w * h
    if not total:
        return True
    # Almost no ink: fewer than (1 - blank_ratio) pixels are non-white.  A
    # truly blank page renders with ~0% non-white pixels, while even a
    # sparse text page carries well above 0.2%.
    nonwhite = sum(hist[:241])
    if nonwhite / total <= 1.0 - blank_ratio:
        return True
    # Nearly uniform AND near-white (a blank render of a white page).  The
    # deliberate (240,240,240) generic placeholder stays below the 248 bar so
    # it is never mistaken for a blank render; failed renders come out >=250.
    lo = 255
    hi = 0
    for v, count in enumerate(hist):
        if count:
            lo = min(lo, v)
            hi = max(hi, v)
    return (hi - lo) <= 8 and hi >= 248


def thumbnail_is_blank(path: str, blank_ratio: float = 0.998) -> bool:
    """True when the image file at ``path`` is an unusable preview.

    True for blank/white renders AND for files that cannot be decoded — both
    are useless as a Telegram thumbnail, so delivery paths treat them the
    same.  Used to reject blank-white previews (e.g. a PDF whose first page
    renders empty after book conversion) so a white placeholder is never
    sent to Telegram.  Best-effort: any failure returns True (unreadable =
    unusable).
    """
    try:
        im = Image.open(path)
        try:
            return _luma_is_blank(im.convert("L"), blank_ratio)
        finally:
            im.close()
    except Exception:  # nosec B110 - unreadable = unusable
        return True


def thumbnail_is_usable(thumb_path: str | None) -> bool:
    """True when ``thumb_path`` exists and is a usable preview.

    A blank/white, corrupt or missing preview is worse than none, so
    delivery paths skip the thumbnail when this returns False.
    """
    return bool(
        thumb_path
        and os.path.exists(thumb_path)
        and not thumbnail_is_blank(thumb_path)
    )


def thumbnail_bytes_is_blank(
    thumb_bytes: bytes, blank_ratio: float = 0.998
) -> bool:
    """True when JPEG/PNG bytes are an unusable preview.

    Bytes-mode twin of :func:`thumbnail_is_blank` for the in-memory worker
    path (no temp file needed).  Blank/white renders and undecodable bytes
    (including empty input) both count as unusable.  Best-effort: any
    failure returns True (unreadable = unusable).
    """
    try:
        im = Image.open(io.BytesIO(thumb_bytes))
        try:
            return _luma_is_blank(im.convert("L"), blank_ratio)
        finally:
            im.close()
    except Exception:  # nosec B110 - unreadable = unusable
        return True


def _pixmap_is_blank(pix, blank_ratio: float = 0.998) -> bool:
    """True when a PyMuPDF pixmap is (near-)blank.

    Avoids saving a white first-page render as the thumbnail; the caller then
    moves on to the next page instead.
    """
    try:
        mode = "RGB" if pix.n < 4 else "RGBA"
        im = Image.frombytes(mode, (pix.width, pix.height), pix.samples)
        try:
            return _luma_is_blank(im.convert("L"), blank_ratio)
        finally:
            im.close()
    except Exception:  # nosec B110 - best-effort blank check
        return False


def _page_has_content(page) -> bool:
    """Cheap pre-filter: does the page carry text, drawings or images?

    A page with none of these is certainly blank, so the caller can skip it
    without spending a pixmap render on the blank check.
    """
    try:
        if (page.get_text() or "").strip():
            return True
        if page.get_drawings():
            return True
        if page.get_images(full=True):
            return True
    except Exception:  # nosec B110 - treat unreadable pages as content
        return True
    return False


def _pixmap_to_thumb_bytes(pix) -> bytes:
    """Convert a PyMuPDF pixmap to optimized JPEG bytes (320px max)."""
    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, (pix.width, pix.height), pix.samples)
    if img.mode == "RGBA":
        img = img.convert("RGB")
    img.thumbnail((320, 320), Image.LANCZOS)
    return _optimize_thumbnail_bytes(img)


def _first_renderable_pixmap(doc, zoom: float = 2.0):
    """Render the first non-blank page of ``doc`` at ``zoom``, or None.

    Skips leading blank pages (blank cover/title pages are common in
    converted books) so the preview shows real content.  The first
    content-bearing page is rendered at full ``zoom`` and blank-checked
    directly — the common case costs exactly one render; pages scanned past
    a blank first page use a cheap text/drawings/images pre-filter and a 1x
    probe first.  Returns ``None`` when every scanned page is blank.
    """
    total = len(doc)
    if total == 0:
        return None
    for i in range(min(total, PDF_THUMB_SCAN_LIMIT)):
        page = doc.load_page(i)
        if not _page_has_content(page):
            continue
        if i == 0:
            # Common path: a single full-quality render, checked directly.
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
            if not _pixmap_is_blank(pix):
                return pix
            continue
        # Page past a blank first page: cheap 1x probe before the full render.
        probe = page.get_pixmap(matrix=fitz.Matrix(1, 1), alpha=False)
        if _pixmap_is_blank(probe):
            continue
        return page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    return None


def create_thumbnail_from_pdf(pdf_path: str, thumb_path: str) -> None:
    doc = fitz.open(pdf_path)
    try:
        if len(doc) == 0:
            return
        pix = _first_renderable_pixmap(doc)
        if pix is None:
            # Every scanned page is blank — render page 0 anyway so the file
            # keeps SOME preview (callers can drop it via thumbnail_is_blank).
            pix = doc.load_page(0).get_pixmap(
                matrix=fitz.Matrix(2, 2), alpha=False
            )
        # save initial rendering and then optimize to meet Telegram thumbnail
        # constraints
        pix.save(thumb_path)
        _optimize_thumbnail(thumb_path)
    finally:
        doc.close()


def create_thumbnail_from_pdf_bytes(pdf_bytes: bytes) -> bytes:
    """Render first non-blank page of a PDF (bytes) to optimized JPEG bytes."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        if len(doc) == 0:
            return b""
        pix = _first_renderable_pixmap(doc)
        if pix is None:
            pix = doc.load_page(0).get_pixmap(
                matrix=fitz.Matrix(2, 2), alpha=False
            )
        return _pixmap_to_thumb_bytes(pix)
    finally:
        doc.close()


def extract_pdf_embedded_thumbnail(pdf_path: str, thumb_path: str) -> bool:
    """Extract a PDF's embedded page-1 thumbnail to ``thumb_path``.

    The "already has a thumbnail" validator: PDF viewers store a small cover
    preview in the page dictionary's ``/Thumb`` entry.  When present, reusing
    it avoids rendering page 1 at 2x — no pixmap render, no memory spike on
    large PDFs.  Returns True when the embedded thumbnail was found, saved and
    is not a blank/white render; False when the PDF has none (or its embedded
    preview is blank — caller falls back to ``create_thumbnail_from_pdf``,
    which skips blank pages).  Best-effort: any failure returns False.

    Implemented via the raw xref API (``Document.xref_get_key`` +
    ``Document.extract_image``) because PyMuPDF 1.24.x's rebased build dropped
    the classic ``Document.has_thumbnails`` / ``Page.get_thumbnail`` helpers.
    """
    try:
        doc = fitz.open(pdf_path)
        try:
            if len(doc) == 0:
                return False
            _kt, _kv = doc.xref_get_key(doc.page_xref(0), "Thumb")
            if _kt != "xref" or not _kv:
                return False
            _thumb_xref = int(_kv.split()[0])
            _info = doc.extract_image(_thumb_xref)
            if not _info or not _info.get("image"):
                return False
            with open(thumb_path, "wb") as _fh:
                _fh.write(_info["image"])
        finally:
            doc.close()
        _optimize_thumbnail(thumb_path)
        # A PDF that ships a blank/white or unreadable embedded preview is
        # effectively thumb-less — fall back to a rendered page instead of
        # reporting "already has a thumbnail".
        if thumbnail_is_blank(thumb_path):
            try:
                os.remove(thumb_path)
            except OSError:  # nosec B110 - best-effort cleanup
                pass
            return False
        return os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0
    except Exception:
        logger = logging.getLogger(__name__)
        logger.warning(
            "tools: failed to extract embedded thumbnail from %s", pdf_path
        )
        return False


def extract_pdf_embedded_thumbnail_bytes(pdf_bytes: bytes) -> bytes | None:
    """Bytes-mode twin of :func:`extract_pdf_embedded_thumbnail`.

    Returns optimized JPEG bytes of the PDF's embedded page-1 thumbnail, or
    None when the PDF has none (or its embedded preview is blank/white —
    caller falls back to ``create_thumbnail_from_pdf_bytes``, which skips
    blank pages).  Best-effort: any failure returns None.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            if len(doc) == 0:
                return None
            _kt, _kv = doc.xref_get_key(doc.page_xref(0), "Thumb")
            if _kt != "xref" or not _kv:
                return None
            _thumb_xref = int(_kv.split()[0])
            _info = doc.extract_image(_thumb_xref)
            if not _info or not _info.get("image"):
                return None
            img = Image.open(io.BytesIO(_info["image"])).convert("RGB")
        finally:
            doc.close()
        # A blank/white embedded preview is effectively thumb-less — return
        # None so the caller falls back to rendering a page.
        if _luma_is_blank(img.convert("L")):
            return None
        return _optimize_thumbnail_bytes(img)
    except Exception:
        logger = logging.getLogger(__name__)
        logger.warning("tools: failed to extract embedded thumbnail (bytes)")
        return None


def pdf_has_text_layer(
    pdf_path: str, min_ratio: float = 0.9, min_chars: int = 60
) -> bool:
    """True when most pages of ``pdf_path`` already carry MEANINGFUL text.

    The "already OCR'd" validator: born-digital or previously-OCR'd PDFs have
    a real text layer, so re-running ocrmypdf/tesseract would waste CPU for
    zero gain.  ``min_ratio`` is the fraction of pages that must qualify (a
    mixed scan only partially OCR'd still gets the full pass).

    ``min_chars`` is the minimum non-whitespace text a page must carry before
    it counts as "already OCR'd".  This is the fix for pirated scans stamped
    with a website footer (e.g. ``www.example.com`` on every page): such pages
    DO contain extractable text, so the old any-text check reported them as
    100% OCR'd and skipped the whole pass even though the textbook body is
    nothing but images.  A page whose only text is a short link/watermark/
    page number falls below the threshold and still gets the full OCR pass.

    Corrupt or empty PDFs return False so the OCR job proceeds and surfaces
    the real error as today.
    """
    try:
        doc = fitz.open(pdf_path)
        try:
            total = len(doc)
            if total == 0:
                return False
            with_text = sum(
                1
                for i in range(total)
                if len((doc.load_page(i).get_text() or "").strip()) >= min_chars
            )
            return with_text / total >= min_ratio
        finally:
            doc.close()
    except Exception:
        logger = logging.getLogger(__name__)
        logger.warning("tools: failed to inspect text layer of %s", pdf_path)
        return False


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
