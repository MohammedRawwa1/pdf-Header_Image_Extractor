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
        except Exception:
            pass
        q -= 10

    # fallback: save with low quality
    try:
        im.save(path, "JPEG", quality=30, optimize=True)
    except Exception:
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
        except Exception:
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
    except Exception:
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
        except Exception:
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
