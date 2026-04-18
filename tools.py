import shutil
import os
from PIL import Image
import io

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise ImportError("PyMuPDF (fitz) is required. Install with 'pip install PyMuPDF'") from e


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


def _optimize_thumbnail(path: str, max_bytes: int = 200 * 1024, max_size=(320, 320)) -> None:
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


def _optimize_thumbnail_bytes(im: Image.Image, max_bytes: int = 200 * 1024) -> bytes:
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
