import logging
import os
import shutil
import signal
import subprocess
import time

logger = logging.getLogger(__name__)

OCR_SOURCE_EXTS: set[str] = {"pdf", "jpg", "jpeg", "png", "webp"}


class OCRCancelledError(Exception):
    pass


def is_ocr_source(filename: str | None) -> bool:
    if not filename:
        return False
    return os.path.splitext(filename)[1].lower().lstrip(".") in OCR_SOURCE_EXTS


def ocr_available() -> bool:
    return shutil.which("tesseract") is not None


def ocr_enabled() -> bool:
    try:
        import config as _cfg

        if not getattr(_cfg, "ENABLE_OCR", True):
            return False
    except Exception:
        pass
    return ocr_available()


def ocr_pdf_available() -> bool:
    return shutil.which("ocrmypdf") is not None


def _resolve_lang(lang: str | None) -> str:
    return (lang or "eng").strip() or "eng"


def _raise_if_cancelled(cancel_check) -> None:
    if cancel_check and cancel_check():
        raise OCRCancelledError("OCR cancelled")


def _import_ocr_deps():
    import pytesseract
    from PIL import Image

    return pytesseract, Image


def _ocr_image_text(
    pytesseract,
    image_path: str,
    lang: str,
    cancel_check=None,
    timeout: int = 0,
) -> str:
    try:
        from PIL import Image

        with Image.open(image_path) as im:
            _raise_if_cancelled(cancel_check)
            rgb = im.convert("RGB")
            return (
                pytesseract.image_to_string(
                    rgb, lang=lang, timeout=timeout or None
                )
                or ""
            ).strip()
    except Exception:
        logger.warning(
            "ocr: failed to OCR image %s", image_path, exc_info=True
        )
        return ""


def _ocr_pdf_text(
    pytesseract,
    pdf_path: str,
    lang: str,
    dpi: int,
    cancel_check=None,
    timeout: int = 0,
) -> str:
    try:
        import fitz
        from PIL import Image
    except Exception:
        logger.warning("ocr: PyMuPDF/PIL not available for PDF OCR")
        return ""
    zoom = max(1.0, (dpi or 200) / 72.0)
    matrix = fitz.Matrix(zoom, zoom)
    chunks: list[str] = []
    doc = fitz.open(pdf_path)
    try:
        total = len(doc)
        for page_num in range(total):
            _raise_if_cancelled(cancel_check)
            try:
                page = doc.load_page(page_num)
                pix = page.get_pixmap(matrix=matrix, alpha=False)
                img = Image.frombytes(
                    "RGB", (pix.width, pix.height), pix.samples
                )
                text = (
                    pytesseract.image_to_string(
                        img, lang=lang, timeout=timeout or None
                    )
                    or ""
                ).strip()
            except Exception:
                logger.warning(
                    "ocr: page %d failed for %s", page_num + 1, pdf_path
                )
                text = ""
            if text:
                chunks.append(f"--- Page {page_num + 1} ---\n{text}")
    finally:
        doc.close()
    return "\n\n".join(chunks)


def run_ocr(
    input_path: str,
    filename: str,
    lang: str | None = None,
    dpi: int = 200,
    timeout: int = 0,
    cancel_check=None,
) -> str:
    lang = _resolve_lang(lang)
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        return ""
    try:
        pytesseract, _ = _import_ocr_deps()
    except Exception:
        logger.warning("ocr: pytesseract not installed")
        return ""
    ext = os.path.splitext(filename or "")[1].lower().lstrip(".")
    if ext == "pdf":
        return _ocr_pdf_text(
            pytesseract,
            input_path,
            lang,
            dpi=dpi,
            timeout=timeout,
            cancel_check=cancel_check,
        )
    return _ocr_image_text(
        pytesseract,
        input_path,
        lang,
        timeout=timeout,
        cancel_check=cancel_check,
    )


def _terminate_process_group(proc: subprocess.Popen) -> None:
    """Kill a child and its whole process group, then reap it.

    ocrmypdf drives tesseract as a child; killing only the direct process
    would leave the OCR child orphaned.  Escalates SIGTERM \u2192 SIGKILL and
    always waits so the child cannot linger as a zombie.
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
        except Exception:
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
    except Exception:
        pass


def _extract_pdf_text(pdf_path: str) -> str:
    try:
        import fitz
    except Exception:
        logger.warning("ocr: PyMuPDF not available to read %s", pdf_path)
        return ""
    chunks: list[str] = []
    try:
        doc = fitz.open(pdf_path)
        try:
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text = (page.get_text() or "").strip()
                if text:
                    chunks.append(f"--- Page {page_num + 1} ---\n{text}")
        finally:
            doc.close()
    except Exception:
        logger.warning("ocr: failed to read text layer of %s", pdf_path)
        return ""
    return "\n\n".join(chunks)


def run_ocr_pdf(
    input_path: str,
    filename: str,
    output_path: str,
    lang: str | None = None,
    dpi: int = 200,
    timeout: int = 0,
    cancel_check=None,
) -> str | None:
    lang = _resolve_lang(lang)
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        return None
    ext = os.path.splitext(filename or "")[1].lower().lstrip(".")
    cmd = [
        "ocrmypdf",
        "--quiet",
        "--redo-ocr",
        "--output-type",
        "pdf",
        "--optimize",
        "1",
        "--jobs",
        "1",
        "--language",
        lang,
    ]
    if timeout and timeout > 0:
        cmd += ["--tesseract-timeout", str(int(timeout))]
    if ext in ("jpg", "jpeg", "png", "webp"):
        cmd += ["--image-dpi", str(int(dpi or 200))]
    _real_input = input_path
    _cleanup_png = None
    if ext == "webp":
        try:
            from PIL import Image

            _png = output_path + ".webp_input.png"
            with Image.open(input_path) as _im:
                _im.convert("RGB").save(_png, "PNG")
            _real_input = _png
            _cleanup_png = _png
        except Exception:
            logger.warning(
                "ocr: failed to pre-convert webp %s for ocrmypdf", filename
            )
    cmd += [_real_input, output_path]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    except Exception:
        logger.warning(
            "ocr: failed to start ocrmypdf for %s", filename, exc_info=True
        )
        return None
    # Enforce a wall-clock deadline: the caller's ``timeout`` used to only be
    # forwarded to tesseract (``--tesseract-timeout``), so a wedged ocrmypdf
    # could spin this loop forever.
    _deadline = time.monotonic() + timeout if timeout and timeout > 0 else None
    try:
        while True:
            try:
                # communicate() keeps draining stdout/stderr, so a chatty
                # ocrmypdf can never block on a full pipe while we poll.
                _out, _err = proc.communicate(timeout=0.5)
                break
            except subprocess.TimeoutExpired:
                if cancel_check and cancel_check():
                    _terminate_process_group(proc)
                    raise OCRCancelledError("OCR cancelled")
                if _deadline is not None and time.monotonic() >= _deadline:
                    _terminate_process_group(proc)
                    logger.warning(
                        "ocr: ocrmypdf timed out after %ss for %s",
                        timeout,
                        filename,
                    )
                    return None
        _rc = proc.returncode
        if _rc != 0:
            logger.warning(
                "ocr: ocrmypdf failed (rc=%s) for %s: %s",
                _rc,
                filename,
                (_err or b"").decode(errors="replace")[-500:],
            )
            return None
        if (
            not os.path.exists(output_path)
            or os.path.getsize(output_path) == 0
        ):
            logger.warning("ocr: ocrmypdf produced no output for %s", filename)
            return None
        return _extract_pdf_text(output_path)
    finally:
        if proc.poll() is None:
            _terminate_process_group(proc)
        if _cleanup_png:
            try:
                os.remove(_cleanup_png)
            except Exception:
                pass
