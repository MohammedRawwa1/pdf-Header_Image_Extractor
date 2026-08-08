"""Fast EPUB→PDF conversion via WeasyPrint for text-heavy books.

Calibre's EPUB→PDF renders every page through Qt WebEngine (Chromium) and is
strictly CPU-bound — a multi-minute slog for big books.  WeasyPrint lays out
the same content with Pango and produces a vector PDF in a fraction of the
time for text-dominated books (novels, essays).  Image-heavy books (photo
albums, most fixed-layout EPUBs) keep the Calibre path, whose per-page
rendering handles dense images far better.

Design rules:
- Every entry point is a *fallback chain*: WeasyPrint is used only when it is
  installed, enabled (``EPUB_FAST_CONVERT_ENABLED``), and the EPUB is
  heuristically text-heavy.  ANY failure (parse error, render exception,
  timeout, cancel) drops back to the Calibre path so the fast path can never
  regress a conversion Calibre would have completed.
- A /canceljob that fires while WeasyPrint renders raises
  :class:`ConversionCancelledError` (mirroring Calibre's abort), so a
  cancelled fast path never triggers a fresh Calibre conversion.
- The pure-EPUB parts (image-weight heuristic, spine parsing, HTML merging)
  are importable and testable without WeasyPrint installed.
"""

import logging
import os
import posixpath
import re
import shutil
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable

from defusedxml import ElementTree

from utils.ebook_converter import (
    ConversionCancelledError,
    finalize_cover_thumbnail,
)

logger = logging.getLogger(__name__)

# EPUB XHTML routinely carries a DOCTYPE declaration.  defusedxml rejects ANY
# doctype (even benign ones) to block entity-expansion attacks; the declaration
# carries no layout value for the body/style merge, so it is stripped before
# parsing.  Without a doctype there is nothing an entity attack can hang on,
# and defusedxml remains the parser of record for untrusted EPUB content.
_DOCTYPE_RE = re.compile(
    rb"<!DOCTYPE(?:\s+[^>\[\]]*)?(?:\[[^\]]*\])?[^>]*>",
    re.IGNORECASE | re.DOTALL,
)


def _safe_xml_parse(raw: bytes) -> ET.Element:
    """Parse untrusted EPUB XML with defusedxml (DOCTYPE already stripped)."""
    return ElementTree.fromstring(_DOCTYPE_RE.sub(b"", raw))

# Content media types accepted in the spine.  EPUB3 XHTML carries
# ``application/xhtml+xml``; older books may use ``text/html``.
_CONTENT_PREFIXES = ("application/xhtml", "text/html")
# Files whose bytes count as "image content" for the text-heavy heuristic.
_IMAGE_EXTS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".svg", ".avif",
)


def weasyprint_available() -> bool:
    """True when WeasyPrint is importable (installed in the image)."""
    try:
        import weasyprint  # noqa: F401

        return True
    except Exception:
        return False


def epub_image_weight(epub_path: str) -> tuple[int, int]:
    """Return ``(uncompressed_total_bytes, image_bytes)`` for an EPUB.

    ``(0, 0)`` on unreadable files — callers treat that as "not text-heavy".
    """
    total = images = 0
    try:
        with zipfile.ZipFile(epub_path) as zf:
            for info in zf.infolist():
                total += info.file_size
                if info.filename.lower().endswith(_IMAGE_EXTS):
                    images += info.file_size
    except Exception:
        return 0, 0
    return total, images


def epub_is_text_heavy(
    epub_path: str,
    image_bytes_limit: int = 8 * 1024 * 1024,
    image_ratio_limit: float = 0.35,
) -> bool:
    """True when an EPUB is text-dominated (fast + faithful via WeasyPrint).

    Image-heavy books (photo albums, most fixed-layout EPUBs) are rejected so
    they keep Calibre's page rendering.  Unreadable files are rejected.
    """
    total, images = epub_image_weight(epub_path)
    if total <= 0:
        return False
    if images >= image_bytes_limit:
        return False
    return (images / total) <= image_ratio_limit


def _parse_epub_spine(epub_path: str) -> tuple[str, list[str], str]:
    """Return ``(opf_dir, spine_content_paths, merged_<style> css)``.

    ``content_paths`` are paths INSIDE the zip (relative to the EPUB root,
    already joined with the OPF directory) in spine order.  ``css`` collects
    every ``<style>`` element found in the content heads so per-chapter
    styling survives the merge.  Raises on structurally broken EPUBs —
    callers fall back to Calibre.
    """
    with zipfile.ZipFile(epub_path) as zf:
        try:
            container = zf.read("META-INF/container.xml")
        except KeyError as exc:
            raise ValueError("EPUB missing META-INF/container.xml") from exc
        croot = _safe_xml_parse(container)
        rootfile = croot.find(".//{*}rootfile")
        if rootfile is None or not rootfile.get("full-path"):
            raise ValueError("EPUB container.xml has no rootfile")
        opf_path = rootfile.get("full-path")
        opf_dir = posixpath.dirname(opf_path) or "."
        try:
            opf = zf.read(opf_path)
        except KeyError as exc:
            raise ValueError(f"EPUB OPF not found: {opf_path}") from exc
        oroot = _safe_xml_parse(opf)

        def _is_content(item: ET.Element) -> bool:
            media = (item.get("media-type") or "").lower()
            if media.startswith(_CONTENT_PREFIXES):
                return True
            if not media:  # nonstandard books may omit media-type
                href = (item.get("href") or "").lower()
                return href.endswith((".xhtml", ".html", ".htm"))
            return False

        manifest: dict[str, str] = {}  # item id -> href
        for item in oroot.findall(".//{*}item"):
            item_id = item.get("id")
            href = item.get("href")
            if item_id and href and _is_content(item):
                manifest[item_id] = href
        spine: list[str] = []
        for ref in oroot.findall(".//{*}itemref"):
            idref = ref.get("idref")
            if idref and idref in manifest:
                spine.append(posixpath.join(opf_dir, manifest[idref]))
        if not spine:
            raise ValueError("EPUB spine has no readable content")

        styles: list[str] = []
        seen: set[str] = set()
        for content_path in spine:
            if content_path in seen:
                continue
            seen.add(content_path)
            try:
                raw = zf.read(content_path)
            except KeyError:
                continue
            try:
                ctree = _safe_xml_parse(raw)
            except Exception:  # nosec B110 - skip unparseable chapter
                continue
            for style in ctree.findall(".//{*}style"):
                if style.text and style.text.strip():
                    styles.append(style.text)
    return opf_dir, spine, "\n".join(styles)


def _strip_ns(elem: ET.Element) -> ET.Element:
    """Namespace-free copy of an element subtree (XHTML → plain HTML).

    ElementTree serializes default-namespace XHTML with ``ns0:`` prefixes,
    which bloats the merged document and breaks selectors; stripping
    namespaces yields clean markup WeasyPrint (html5lib) parses correctly.
    Namespaced attributes (e.g. ``xlink:href``) collapse to their local name.
    """
    tag = elem.tag
    if isinstance(tag, str) and "}" in tag:
        tag = tag.split("}", 1)[-1]
    new = ET.Element(tag)
    for key, value in elem.attrib.items():
        if key.startswith("{"):
            key = key.split("}", 1)[-1]
        new.set(key, value)
    new.text = elem.text
    new.tail = elem.tail
    for child in elem:
        new.append(_strip_ns(child))
    return new


def _resolve_ref(href: str, chapter_dir: str, opf_dir: str) -> str:
    """Rebase a chapter-relative resource ref to an OPF-relative path.

    Merged chapters are served from the OPF directory, so a chapter in a
    subdirectory referencing ``../images/x.jpg`` must be rebased to the OPF
    root.  Fragments, data:/scheme URLs, and absolute paths pass through.
    """
    if not href:
        return href
    href = href.strip()
    if (
        href.startswith("#")
        or href.startswith("data:")
        or href.startswith("/")
        or "://" in href
    ):
        return href
    plain = href.split("#", 1)[0].split("?", 1)[0]
    if not plain:
        return href
    full = posixpath.normpath(posixpath.join(chapter_dir, plain))
    try:
        return posixpath.relpath(full, opf_dir)
    except ValueError:  # nosec B110 - path unreachable; keep as-is
        return href


def _build_merged_html(
    epub_path: str,
    content_paths: list[str],
    styles: str,
    opf_dir: str,
) -> str:
    """Merge spine documents into ONE XHTML document for a single render.

    Only each chapter's ``<body>`` is kept (head ``<style>`` blocks are merged
    into the output head) so pagination flows continuously through the book
    instead of restarting per chapter.  Image refs are rebased to the OPF
    directory (``opf_dir``).  Chapters whose body can't be parsed are skipped;
    a completely empty result is an empty document (caller falls back).
    """
    bodies: list[str] = []
    with zipfile.ZipFile(epub_path) as zf:
        for content_path in content_paths:
            try:
                raw = zf.read(content_path)
            except KeyError:
                continue
            try:
                ctree = _safe_xml_parse(raw)
            except Exception:  # nosec B110 - skip unparseable chapter
                continue
            body = ctree.find(".//{*}body")
            if body is None:
                continue
            chapter_dir = posixpath.dirname(content_path)
            clean = _strip_ns(body)
            for elem in clean.iter():
                tag = elem.tag
                if tag in ("img", "source"):
                    src = elem.get("src")
                    if src:
                        elem.set(
                            "src", _resolve_ref(src, chapter_dir, opf_dir)
                        )
                elif tag == "image":
                    src = elem.get("href") or elem.get("src")
                    if src:
                        elem.set(
                            "href", _resolve_ref(src, chapter_dir, opf_dir)
                        )
            bodies.append(ET.tostring(clean, encoding="unicode"))
    style_block = f"<style>{styles}</style>" if styles else ""
    return (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        f"{style_block}</head><body>{''.join(bodies)}</body></html>"
    )


def _extract_epub(epub_path: str, dest_dir: str) -> None:
    """Extract an EPUB zip safely (no zip-slip) into ``dest_dir``."""
    with zipfile.ZipFile(epub_path) as zf:
        for info in zf.infolist():
            name = info.filename.replace("\\", "/")
            parts = name.split("/")
            if not parts or name.startswith("/") or ".." in parts:
                continue
            target = os.path.join(dest_dir, *parts)
            if info.is_dir():
                os.makedirs(target, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)


def _render_weasyprint(
    html: str,
    base_url: str | None,
    pdf_path: str,
    timeout: int,
    cancel_check: Callable[[], bool] | None,
) -> bool:
    """Render merged HTML to PDF with WeasyPrint in a watchdog thread.

    WeasyPrint is in-process (not a killable subprocess), so on cancel or
    timeout we abandon the daemon thread and return False — the caller falls
    back to Calibre.  Output is written to a temp path then atomically
    replaced, so an abandoned render can never leave a partial PDF behind.
    """
    from weasyprint import HTML

    temp_pdf = pdf_path + ".wp.tmp"
    state: dict = {"ok": False}
    done = threading.Event()

    def _run() -> None:
        try:
            HTML(string=html, base_url=base_url).write_pdf(temp_pdf)
            if os.path.exists(temp_pdf) and os.path.getsize(temp_pdf) > 0:
                state["ok"] = True
        except Exception as exc:  # nosec B110 - fallback handles it
            logger.warning("weasyprint: render failed: %s", exc)
        finally:
            done.set()

    threading.Thread(target=_run, daemon=True).start()
    deadline = time.monotonic() + timeout
    while not done.wait(1.0):
        if cancel_check is not None and cancel_check():
            break
        if time.monotonic() >= deadline:
            logger.warning("weasyprint: timed out after %ss", timeout)
            break
    if not state.get("ok"):
        try:
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)
        except Exception:  # nosec B110
            pass
        return False
    try:
        os.replace(temp_pdf, pdf_path)
    except Exception:
        return False
    return True


def _calibre_fallback(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int,
    cancel_check: Callable[[], bool] | None,
) -> bool:
    """Convert via the standard Calibre path (``convert_book_to_pdf_with_thumbnail``)."""
    from utils.ebook_converter import convert_book_to_pdf_with_thumbnail

    return convert_book_to_pdf_with_thumbnail(
        input_path, pdf_path, thumb_path,
        timeout=timeout, cancel_check=cancel_check,
    )


def convert_epub_to_pdf_fast(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    """Convert an EPUB to PDF, preferring the fast WeasyPrint path.

    WeasyPrint is used only when: installed, enabled (``EPUB_FAST_CONVERT_ENABLED``)
    and the EPUB is heuristically text-heavy.  Any failure anywhere falls back
    to the Calibre path, so this function can only ever be as good as the
    status quo — never worse.  Thumbnail behavior mirrors
    ``convert_book_to_pdf_with_thumbnail`` (cover via ``ebook-meta``, else a
    preview of the produced PDF's first page).
    """
    import config as _cfg

    if not weasyprint_available():
        return _calibre_fallback(
            input_path, pdf_path, thumb_path, timeout, cancel_check
        )
    if not getattr(_cfg, "EPUB_FAST_CONVERT_ENABLED", True):
        return _calibre_fallback(
            input_path, pdf_path, thumb_path, timeout, cancel_check
        )
    try:
        if not epub_is_text_heavy(
            input_path,
            image_bytes_limit=getattr(
                _cfg, "EPUB_FAST_IMAGE_BYTES_LIMIT", 8 * 1024 * 1024
            ),
        ):
            logger.info(
                "weasyprint: %s is image-heavy; using Calibre",
                os.path.basename(input_path),
            )
            return _calibre_fallback(
                input_path, pdf_path, thumb_path, timeout, cancel_check
            )
    except Exception as exc:  # nosec B110 - heuristic failure falls back
        logger.warning("weasyprint: heuristic failed (%s); using Calibre", exc)
        return _calibre_fallback(
            input_path, pdf_path, thumb_path, timeout, cancel_check
        )

    tmp = tempfile.mkdtemp(prefix="wp_epub_")
    try:
        if cancel_check is not None and cancel_check():
            # /canceljob fired between the heuristic and the render — abort
            # before doing any work (and never fall back to Calibre).
            raise ConversionCancelledError(
                f"conversion cancelled: {input_path}"
            )
        _render_start = time.monotonic()
        opf_dir, spine_paths, styles = _parse_epub_spine(input_path)
        _extract_epub(input_path, tmp)
        html = _build_merged_html(input_path, spine_paths, styles, opf_dir)
        base_url = os.path.join(tmp, opf_dir)
        if _render_weasyprint(html, base_url, pdf_path, timeout, cancel_check):
            # finalize_cover_thumbnail swallows its own errors (best-effort).
            finalize_cover_thumbnail(input_path, pdf_path, thumb_path)
            return True
        if cancel_check is not None and cancel_check():
            # User cancelled mid-render — never start a Calibre conversion.
            raise ConversionCancelledError(
                f"conversion cancelled: {input_path}"
            )
        logger.warning(
            "weasyprint: render failed/timed out after %ss for %s; "
            "using Calibre",
            int(time.monotonic() - _render_start),
            os.path.basename(input_path),
        )
    except ConversionCancelledError:
        raise
    except Exception as exc:  # nosec B110 - any failure falls back
        logger.warning(
            "weasyprint: fast path failed (%s) for %s; using Calibre",
            exc,
            os.path.basename(input_path),
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    # Bound the whole chain: WeasyPrint may have burned part of the caller's
    # budget, so Calibre gets the REMAINDER (never a fresh full timeout on top
    # of a timed-out render — that would blow the RQ job timeout).
    _remain = timeout - int(time.monotonic() - _render_start)
    return _calibre_fallback(
        input_path, pdf_path, thumb_path, max(60, _remain), cancel_check
    )
