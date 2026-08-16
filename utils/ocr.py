"""OCR via Tesseract (pytesseract / ocrmypdf) for PDFs and images.

Wraps the ``tesseract`` binary through ``pytesseract`` with the same
conventions as the other tool modules in this repo: lazily-imported heavy
dependencies, and a polled ``cancel_check`` so /canceljob can abort a long OCR
run mid-page instead of waiting out the timeout.

Searchable-PDF output is produced by ``ocrmypdf`` (pip package): it OCRs the
pages and sandwiches an INVISIBLE text layer over the ORIGINAL page rendering,
so the delivered file looks identical to the input but its text is
selectable/copyable/searchable (never a locked image).

Key entry points:
- :func:`is_ocr_source` — can a filename be OCR'd (PDF or raster image)?
- :func:`ocr_available` — is the ``tesseract`` binary on PATH?
- :func:`ocr_pdf_available` — is the ``ocrmypdf`` binary on PATH?
- :func:`run_ocr` — extract plain text from an image or PDF file.
- :func:`run_ocr_pdf` — build a searchable PDF (original look + text layer).
"""

import logging
import os
import shutil
import signal
import subprocess
import time

logger = logging.getLogger(__name__)

# Formats the 🔎🖼 OCR & Thumbnail button is offered for: scanned PDFs +
# raster images.
OCR_SOURCE_EXTS: set[str] = {"pdf", "jpg", "jpeg", "png", "webp"}


class OCRCancelledError(Exception):
    """Raised when a running OCR job is aborted via ``cancel_check``.

    Distinct from failure so callers can report "cancelled" (and clean up)
    rather than a generic OCR error after /canceljob fires mid-run.
    """


def is_ocr_source(filename: str | None) -> bool:
    """True when ``filename`` has an OCR-able extension."""
    if not filename:
        return False
    return os.path.splitext(filename)[1].lower().lstrip(".") in OCR_SOURCE_EXTS


def ocr_available() -> bool:
    """True when the ``tesseract`` binary is on PATH (Tesseract installed)."""
    return shutil.which("tesseract") is not None


def ocr_enabled() -> bool:
    """True when the 🔎🖼 OCR & Thumbnail feature is both installed AND enabled.

    Combines the ``ENABLE_OCR`` config master switch with the binary check, so
    the button disappears entirely when the operator flips the switch off —
    mirroring ``ENABLE_BOOK_CONVERSION`` for Calibre.
    """
    try:
        import config as _cfg  # noqa: PLC0415

        if not getattr(_cfg, "ENABLE_OCR", True):
            return False
    except Exception:  # nosec B110 - default to enabled
        pass
    return ocr_available()


def ocr_pdf_available() -> bool:
    """True when the ``ocrmypdf`` binary is on PATH (searchable-PDF support)."""
    return shutil.which("ocrmypdf") is not None


def _resolve_lang(lang: str | None) -> str:
    """Normalise the OCR language string (fall back to English)."""
    return (lang or "eng").strip() or "eng"


def _raise_if_cancelled(cancel_check) -> None:
    if cancel_check and cancel_check():
        raise OCRCancelledError("OCR cancelled")


def _import_ocr_deps():
    """Import pytesseract + PIL lazily (they are optional at runtime)."""
    import pytesseract  # noqa: PLC0415
    from PIL import Image  # noqa: PLC0415

    return pytesseract, Image


def _ocr_image_text(
    pytesseract, image_path: str, lang: str, cancel_check=None, timeout: int = 0
) -> str:
    """OCR a raster image file (JPEG/PNG/WebP/BMP/...) with Tesseract."""
    try:
        from PIL import Image  # noqa: PLC0415

        with Image.open(image_path) as im:
            _raise_if_cancelled(cancel_check)
            # RGB conversion: Tesseract expects a standard 3-channel image
            # (alpha/CMYK inputs error out or degrade accuracy).
            rgb = im.convert("RGB")
            return (
                pytesseract.image_to_string(
                    rgb, lang=lang, timeout=timeout or None
                )
                or ""
            ).strip()
    except Exception:  # nosec B110 - a failed OCR attempt yields no text
        logger.warning("ocr: failed to OCR image %s", image_path, exc_info=True)
        return ""


def _ocr_pdf_text(
    pytesseract, pdf_path: str, lang: str, dpi: int, cancel_check=None, timeout: int = 0
) -> str:
    """OCR every page of a PDF (rasterized via PyMuPDF at ``dpi``)."""
    try:
        import fitz  # PyMuPDF  # noqa: PLC0415
        from PIL import Image  # noqa: PLC0415
    except Exception:  # nosec B110
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
            except Exception:  # nosec B110 - skip a broken page
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
    """Extract text from ``input_path`` (image or PDF) via Tesseract.

    ``cancel_check()`` (optional) is polled per page / per image; when it turns
    True, :class:`OCRCancelledError` is raised so /canceljob can abort mid-run.
    ``timeout`` (seconds, 0 = none) bounds each Tesseract invocation, mirroring
    ``OCR_TIMEOUT_SECONDS``.  Returns the extracted text (may be empty when
    nothing was found).
    """
    lang = _resolve_lang(lang)
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        return ""
    try:
        pytesseract, _ = _import_ocr_deps()
    except Exception:  # nosec B110 - pytesseract missing = no OCR
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
    """Best-effort kill of an ocrmypdf process (and its worker children)."""
    try:
        if os.name == "posix" and proc.poll() is None:
            # ocrmypdf spawns tesseract/gs workers; signal the whole group.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:  # nosec B110 - fall through to kill()
                proc.kill()
        else:
            proc.kill()
    except Exception:  # nosec B110 - best-effort
        pass


def _extract_pdf_text(pdf_path: str) -> str:
    """Pull the (invisible + visible) text layer out of a searchable PDF."""
    try:
        import fitz  # PyMuPDF  # noqa: PLC0415
    except Exception:  # nosec B110
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
    except Exception:  # nosec B110
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
    """Build a searchable PDF from ``input_path`` via ocrmypdf.

    ocrmypdf OCRs each page and overlays an INVISIBLE text layer on the ORIGINAL
    page rendering — the output looks exactly like the input but its text is
    selectable/copyable/searchable.  ``--redo-ocr`` re-OCRs pages that already
    carry a text layer while RETAINING the original layer: this is deliberate.
    Pirated scans are often stamped with a website footer (e.g. ``www.site.com``
    on every page) — ocrmypdf's default ``--skip-text`` sees that thin text and
    skips the whole page, so the textbook body never gets OCR'd.  ``--redo-ocr``
    guarantees every page gets a real searchable layer regardless of such
    fragments, and the ``pdf_has_text_layer`` validator (which requires
    meaningful text per page) gates the run up front so genuinely born-digital
    PDFs never reach the engine.  ``--output-type pdf`` skips PDF/A conversion
    for speed.

    ``cancel_check()`` is polled while the subprocess runs; when it turns True
    the process group is killed and :class:`OCRCancelledError` is raised so
    /canceljob can abort mid-run.  ``timeout`` maps to ocrmypdf's per-page
    ``--tesseract-timeout``.

    Returns:
        ``None`` when ocrmypdf FAILED (binary missing, non-zero exit, or no
        output produced).  Otherwise the text extracted from the OUTPUT PDF —
        which may be ``""`` for a genuinely blank/unreadable scan.  Callers
        must deliver the produced file regardless of the returned text, using
        it only for diagnostics/messaging.
    """
    lang = _resolve_lang(lang)
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        # Failure to produce anything — matches the documented None-on-failure
        # contract ('' is reserved for 'PDF produced but blank').
        return None
    ext = os.path.splitext(filename or "")[1].lower().lstrip(".")
    cmd = [
        "ocrmypdf",
        "--quiet",
        "--redo-ocr",
        "--output-type", "pdf",
        "--optimize", "1",
        "--jobs", "1",
        "--language", lang,
    ]
    if timeout and timeout > 0:
        cmd += ["--tesseract-timeout", str(int(timeout))]
    if ext in ("jpg", "jpeg", "png", "webp"):
        # Images usually lack DPI metadata; tell ocrmypdf the input resolution
        # so Tesseract computes correct character scales.
        cmd += ["--image-dpi", str(int(dpi or 200))]
    _real_input = input_path
    _cleanup_png = None
    if ext == "webp":
        # img2pdf/ocrmypdf webp support is not guaranteed across versions —
        # rasterize to PNG via PIL first so the searchable-PDF path works
        # for every source the button is offered on.
        try:
            from PIL import Image  # noqa: PLC0415

            _png = output_path + ".webp_input.png"
            with Image.open(input_path) as _im:
                _im.convert("RGB").save(_png, "PNG")
            _real_input = _png
            _cleanup_png = _png
        except Exception:  # nosec B110 - fall back to the raw file
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
        logger.warning("ocr: failed to start ocrmypdf for %s", filename, exc_info=True)
        return None
    try:
        while True:
            _rc = proc.poll()
            if _rc is not None:
                break
            if cancel_check and cancel_check():
                _terminate_process_group(proc)
                raise OCRCancelledError("OCR cancelled")
            time.sleep(0.5)
        _out, _err = proc.communicate()
        if _rc != 0:
            logger.warning(
                "ocr: ocrmypdf failed (rc=%s) for %s: %s",
                _rc,
                filename,
                (_err or b"").decode(errors="replace")[-500:],
            )
            return None
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            logger.warning(
                "ocr: ocrmypdf produced no output for %s", filename
            )
            return None
        # The PDF was created — success.  The extracted text is diagnostic
        # (may be empty for a genuinely blank scan) and must NOT be conflated
        # with failure: the caller delivers the file either way.
        return _extract_pdf_text(output_path)
    finally:
        if proc.poll() is None:
            _terminate_process_group(proc)
        if _cleanup_png:
            try:
                os.remove(_cleanup_png)
            except Exception:  # nosec B110
                pass
