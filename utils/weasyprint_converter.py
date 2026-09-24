"""Fast EPUB→PDF conversion via WeasyPrint with Calibre fallback."""

import gc
import html
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
from functools import lru_cache
from io import BytesIO

from defusedxml import ElementTree

from utils.ebook_converter import (
    ConversionCancelledError,
    DRMProtectedError,
    epub_is_drm_protected,
    finalize_cover_thumbnail,
)

logger = logging.getLogger(__name__)

_DOCTYPE_RE = re.compile(
    rb"<!DOCTYPE(?:\s+[^>\[\]]*)?(?:\[[^\]]*\])?[^>]*>",
    re.IGNORECASE | re.DOTALL,
)
_CONTENT_PREFIXES = ("application/xhtml", "text/html")
_C0_CONTROL_RE = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_MAX_CONTROL_RATIO = 0.02
_MIN_MERGE_TEXT_CHARS = 20
_MIN_PDF_BYTES = 8 * 1024
_MIN_PDF_TEXT_CHARS = 40
_MIN_PAGE_DIM_PT = 150.0
_MAX_PAGE_DIM_PT = 1400.0
_PAGE_OVERFLOW_TOLERANCE_PT = 5.0
# Wall-clock cap on the post-render PDF scan so inspecting a big book can
# never eat minutes (PDF_SANITY_CHECK_BUDGET_S overrides).
_SANITY_CHECK_BUDGET_S = 15.0
# Overflow beyond the tolerance is logged and accepted; only overflow larger
# than this (pt) still routes the render to the Calibre fallback
# (PDF_OVERFLOW_FATAL_PT overrides). A slightly clipped page ships rather
# than dying in the fallback.
_PDF_OVERFLOW_FATAL_PT = 30.0

_PX_PER_MM = 96.0 / 25.4
_CSS_UNIT_PX = {
    "px": 1.0,
    "pt": 96.0 / 72.0,
    "pc": 16.0,
    "in": 96.0,
    "cm": 96.0 / 2.54,
    "mm": _PX_PER_MM,
    "q": 96.0 / 101.6,
}
_NAMED_PAGE_SIZES_PX: dict[str, tuple[float, float]] = {
    "a3": (297.0 * _PX_PER_MM, 420.0 * _PX_PER_MM),
    "a4": (210.0 * _PX_PER_MM, 297.0 * _PX_PER_MM),
    "a5": (148.0 * _PX_PER_MM, 210.0 * _PX_PER_MM),
    "b4": (250.0 * _PX_PER_MM, 353.0 * _PX_PER_MM),
    "b5": (176.0 * _PX_PER_MM, 250.0 * _PX_PER_MM),
    "letter": (8.5 * 96.0, 11.0 * 96.0),
    "legal": (8.5 * 96.0, 14.0 * 96.0),
}
_XML_ENCODING_RE = re.compile(
    rb"<\?xml[^>]*encoding=[\"']([A-Za-z0-9._-]+)[\"']", re.IGNORECASE
)
_RAW_BODY_RE = re.compile(
    rb"<body[^>]*>(.*?)</body>", re.IGNORECASE | re.DOTALL
)
_TAG_RE = re.compile(rb"<[^>]+>")
_TAG_TEXT_RE = re.compile(r"<[^>]+>")
_MIN_SALVAGE_MEANINGFUL_RATIO = 0.5
_SKIP_REGION_RE = re.compile(
    rb"<(head|style|script|title)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL
)
_UNCLOSED_REGION_RE = re.compile(
    rb"<(head|style|script|title)[^>]*>(.*)$", re.IGNORECASE | re.DOTALL
)
_SALVAGED_REF_RE = re.compile(
    rb"(?P<attr>\b(?:src|href)\s*=\s*)(?P<quote>['\"])(?P<url>[^'\"]+)",
    re.IGNORECASE,
)
_CSS_URL_RE = re.compile(
    r"url\s*\(\s*(?P<quote>['\"]?)(?P<url>[^'\")]+)(?P=quote)\s*\)",
    re.IGNORECASE,
)

_HTML_PARSER: Callable[[bytes], ET.Element] | bool | None = None


def _safe_xml_parse(raw: bytes) -> ET.Element:
    return ElementTree.fromstring(_DOCTYPE_RE.sub(b"", raw))


def _get_lenient_parser() -> Callable[[bytes], ET.Element] | None:
    global _HTML_PARSER
    if _HTML_PARSER is not None:
        return _HTML_PARSER if _HTML_PARSER is not False else None
    try:
        import tinyhtml5

        def _parse_with_tinyhtml5(raw: bytes) -> ET.Element:
            return tinyhtml5.parse(
                raw, treebuilder="etree", namespaceHTMLElements=True
            )

        _HTML_PARSER = _parse_with_tinyhtml5
    except Exception:
        try:
            import html5lib

            def _parse_with_html5lib(raw: bytes) -> ET.Element:
                return html5lib.parse(
                    raw, treebuilder="etree", namespaceHTMLElements=True
                )

            _HTML_PARSER = _parse_with_html5lib
        except Exception:
            _HTML_PARSER = False
    return _HTML_PARSER if _HTML_PARSER is not False else None


def _decode_chapter(raw: bytes) -> bytes | str:
    match = _XML_ENCODING_RE.search(raw)
    if not match:
        return raw
    try:
        return raw.decode(match.group(1).decode("ascii"))
    except (LookupError, UnicodeDecodeError):
        return raw


def _is_binary_garbage(raw: bytes) -> bool:
    if not raw:
        return True
    return len(_C0_CONTROL_RE.findall(raw)) / len(raw) > _MAX_CONTROL_RATIO


def _parse_content_html(raw: bytes) -> ET.Element | None:
    if _is_binary_garbage(raw):
        return None
    parser = _get_lenient_parser()
    if parser is None:
        try:
            return _safe_xml_parse(raw)
        except Exception:
            return None
    try:
        return parser(_decode_chapter(raw))
    except Exception:
        return None


def weasyprint_available() -> bool:
    try:
        import weasyprint  # noqa: F401  (import probe: validates native deps too)

        return True
    except Exception:
        return False


def _parse_epub_spine(epub_path: str) -> tuple[str, list[str], str]:
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
            if not media:
                href = (item.get("href") or "").lower()
                return href.endswith((".xhtml", ".html", ".htm"))
            return False

        manifest: dict[str, str] = {}
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
            ctree = _parse_content_html(raw)
            if ctree is None:
                continue
            for style in ctree.findall(".//{*}style"):
                if style.text and style.text.strip():
                    styles.append(
                        _rebase_css_urls(
                            style.text,
                            posixpath.dirname(content_path),
                            opf_dir,
                        )
                    )
    return opf_dir, spine, "\n".join(styles)


def _strip_ns(elem: ET.Element) -> ET.Element | None:
    if not isinstance(elem.tag, str):
        return None
    tag = elem.tag.split("}", 1)[-1] if "}" in elem.tag else elem.tag
    new = ET.Element(tag)
    for key, value in elem.attrib.items():
        new.set(key.split("}", 1)[-1] if "{" in key else key, value)
    new.text = elem.text
    new.tail = elem.tail
    for child in elem:
        stripped = _strip_ns(child)
        if stripped is not None:
            new.append(stripped)
    return new


def _resolve_ref(href: str, chapter_dir: str, opf_dir: str) -> str:
    if not href:
        return href
    href = href.strip()
    if href.startswith(("#", "data:", "/")) or "://" in href:
        return href
    plain = href.split("#", 1)[0].split("?", 1)[0]
    if not plain:
        return href
    full = posixpath.normpath(posixpath.join(chapter_dir, plain))
    try:
        return posixpath.relpath(full, opf_dir)
    except ValueError:
        return href


def _rebase_css_urls(css: str, chapter_dir: str, opf_dir: str) -> str:
    """Rewrite url(...) refs in a chapter's <style> block to be opf_dir-relative.

    CSS url() inside an XHTML file resolves against that chapter's directory,
    so it must be rebased before all chapter styles are concatenated into one
    blob rendered with base_url = opf_dir.
    """

    def _sub(m: re.Match[str]) -> str:
        url = m.group("url").strip()
        if not url or url.startswith(("#", "data:", "/")) or "://" in url:
            return m.group(0)
        rebased = _resolve_ref(url, chapter_dir, opf_dir)
        return f"url({m.group('quote')}{rebased}{m.group('quote')})"

    return _CSS_URL_RE.sub(_sub, css)


def _rebase_salvaged_refs(markup: str, chapter_dir: str, opf_dir: str) -> str:
    def _sub(m: re.Match[bytes]) -> bytes:
        url = m.group("url")
        if url.startswith((b"#", b"data:", b"/")) or b"://" in url or not url:
            return m.group(0)
        rebased = _resolve_ref(
            url.decode("utf-8", errors="replace"), chapter_dir, opf_dir
        ).encode("utf-8", errors="replace")
        return m.group("attr") + m.group("quote") + rebased + m.group("quote")

    return _SALVAGED_REF_RE.sub(_sub, markup.encode("utf-8")).decode(
        "utf-8", errors="replace"
    )


def _is_garbage_text(decoded: str) -> bool:
    plain = _TAG_TEXT_RE.sub("", decoded)
    if not plain:
        return True
    meaningful = sum(1 for ch in plain if ch.isprintable() and ch != "\ufffd")
    return meaningful / len(plain) < _MIN_SALVAGE_MEANINGFUL_RATIO


def _salvage_raw_body(raw: bytes) -> str | None:
    if _is_binary_garbage(raw):
        return None
    m = _RAW_BODY_RE.search(raw)
    if m:
        inner = m.group(1)
        if inner.strip():
            decoded = _decode_chapter(inner)
            if isinstance(decoded, bytes):
                decoded = decoded.decode("utf-8", errors="replace")
            if not _is_garbage_text(decoded):
                return decoded
    stripped = _UNCLOSED_REGION_RE.sub(b"", _SKIP_REGION_RE.sub(b"", raw))
    stripped = _TAG_RE.sub(b"", stripped)
    if not stripped.strip():
        return None
    decoded = _decode_chapter(stripped)
    if isinstance(decoded, bytes):
        decoded = decoded.decode("utf-8", errors="replace")
    text = html.unescape(decoded).strip()
    if not text or _is_garbage_text(text):
        return None
    return f"<p>{html.escape(text)}</p>"


def _css_length_to_px(value: str) -> float | None:
    m = re.match(
        r"^\s*([0-9]*\.?[0-9]+)\s*(px|pt|pc|in|cm|mm|q)?\s*$",
        value,
        re.IGNORECASE,
    )
    if not m:
        return None
    return float(m.group(1)) * _CSS_UNIT_PX[(m.group(2) or "px").lower()]


def _css_horizontal_margin_px(margin_css: str) -> float | None:
    parts = margin_css.strip().split()
    if not parts:
        return None
    if len(parts) >= 2:
        return _css_length_to_px(parts[1])
    return _css_length_to_px(parts[0])


def _page_size_px(size_css: str) -> tuple[float, float] | None:
    parts = size_css.strip().lower().split()
    landscape = False
    if len(parts) >= 2 and parts[-1] in ("landscape", "portrait"):
        landscape = parts[-1] == "landscape"
        parts = parts[:-1]
    if not parts:
        return None
    if len(parts) == 1 and parts[0] in _NAMED_PAGE_SIZES_PX:
        size = _NAMED_PAGE_SIZES_PX[parts[0]]
    elif len(parts) == 2:
        w = _css_length_to_px(parts[0])
        h = _css_length_to_px(parts[1])
        if w is None or h is None:
            return None
        size = (w, h)
    else:
        return None
    return (size[1], size[0]) if landscape else size


@lru_cache(maxsize=1)
def _render_page_config() -> tuple[str, float]:
    """Return the @page CSS rule and printable content width (px) for images.

    The @page rule is appended after the EPUB's own <style> blocks so it wins
    the cascade: an ebook image is never allowed to determine a page dimension
    larger than the printable page.
    """
    import config as _cfg

    size_css = str(getattr(_cfg, "EPUB_PAGE_SIZE", "A4") or "A4").strip()
    margin_css = str(
        getattr(_cfg, "EPUB_PAGE_MARGIN", "15mm") or "15mm"
    ).strip()
    page_css = f"@page {{ size: {size_css}; margin: {margin_css}; }} "
    margin_px = _css_horizontal_margin_px(margin_css)
    if margin_px is None:
        margin_px = 15.0 * _PX_PER_MM
    size_px = _page_size_px(size_css)
    if size_px is None:
        content_w = 680.0
    else:
        content_w = size_px[0] - 2.0 * margin_px
        if content_w <= 0.0:
            content_w = 680.0
    return page_css, content_w


def _normalize_raster_image(
    elem: ET.Element,
    zf: zipfile.ZipFile,
    zip_path: str,
    image_dir: str,
    resolved_ref: str,
) -> None:
    """Bound every raster image to the printable page width; downsample the
    pathological ones so no image can force right-edge clipping or bloat the
    PDF past Telegram's upload limit. Non-raster / unreadable images are left
    to the CSS backstop in _build_merged_html."""
    if (
        not resolved_ref
        or resolved_ref.startswith(("#", "/", "data:"))
        or "://" in resolved_ref
    ):
        return
    safe = posixpath.normpath(zip_path)
    if not safe or safe.startswith("..") or safe.startswith("/"):
        return
    try:
        raw = zf.read(safe)
    except (KeyError, OSError):
        return
    try:
        from PIL import Image

        im = Image.open(BytesIO(raw))
        width, height = im.size
    except Exception:
        return
    if width <= 0 or height <= 0:
        return
    _page_css, content_w = _render_page_config()
    max_w = int(round(content_w))
    if max_w <= 0:
        return
    existing = (elem.get("style") or "").strip()
    # !important so a book stylesheet rule like `img { width: 2000px
    # !important }` cannot override the inline cap.
    inline = f"max-width:{max_w}px !important;min-width:0;height:auto;"
    elem.set("style", f"{existing};{inline}" if existing else inline)
    new_ref = _downsample_image(zf, zip_path, image_dir, resolved_ref)
    if new_ref:
        if elem.tag == "image":
            elem.set("href", new_ref)
        else:
            elem.set("src", new_ref)


def _downsample_image(
    zf: zipfile.ZipFile,
    zip_path: str,
    image_dir: str,
    resolved_ref: str,
) -> str | None:
    """Re-encode a raster wider than the downsample threshold to ~2x the
    printable page width; return the new opf_dir-relative ref, or None when
    the image is small enough, unreadable, or already at a sane size."""
    safe = posixpath.normpath(zip_path)
    if not safe or safe.startswith("..") or safe.startswith("/"):
        return None
    try:
        raw = zf.read(safe)
    except (KeyError, OSError):
        return None
    try:
        from PIL import Image

        im = Image.open(BytesIO(raw))
        width, height = im.size
    except Exception:
        return None
    if width <= 0 or height <= 0:
        return None
    _page_css, content_w = _render_page_config()
    try:
        import config as _cfg

        min_w = int(
            getattr(_cfg, "EPUB_IMAGE_DOWNSAMPLE_MIN_WIDTH_PX", 3000) or 3000
        )
        quality = int(getattr(_cfg, "EPUB_IMAGE_JPEG_QUALITY", 85) or 85)
    except Exception:
        min_w, quality = 3000, 85
    if width <= min_w:
        return None
    target_w = int(round(min(width, content_w * 2.0)))
    if target_w >= width:
        return None
    target_h = max(1, int(round(height * target_w / width)))
    try:
        if "A" in im.getbands():
            rgba = im.convert("RGBA")
            bg = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
            im = Image.alpha_composite(bg, rgba).convert("RGB")
        else:
            im = im.convert("RGB")
        im = im.resize((target_w, target_h), Image.LANCZOS)
        stem = os.path.splitext(os.path.basename(resolved_ref))[0]
        new_ref = posixpath.join(
            posixpath.dirname(resolved_ref), f"{stem}.wp.jpg"
        )
        out_path = os.path.join(image_dir, *new_ref.split("/"))
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        im.save(out_path, "JPEG", quality=quality, optimize=True)
        return new_ref
    except Exception as exc:
        logger.debug(
            "weasyprint: image downsampling skipped for %s: %s",
            resolved_ref,
            exc,
        )
        return None


def _normalize_css_backgrounds(
    styles: str,
    zf: zipfile.ZipFile,
    opf_dir: str,
    image_dir: str,
) -> str:
    """Downsample oversized raster files referenced via CSS url() so giant
    background images don't bloat the PDF. The urls were already rebased to
    be opf_dir-relative when the chapter styles were collected."""

    def _sub(m: re.Match[str]) -> str:
        url = m.group("url").strip()
        if not url or url.startswith(("#", "data:", "/")) or "://" in url:
            return m.group(0)
        new_ref = _downsample_image(
            zf, posixpath.join(opf_dir, url), image_dir, url
        )
        if new_ref:
            return f"url({m.group('quote')}{new_ref}{m.group('quote')})"
        return m.group(0)

    return _CSS_URL_RE.sub(_sub, styles)


def _build_merged_html(
    epub_path: str,
    content_paths: list[str],
    styles: str,
    opf_dir: str,
    image_dir: str | None = None,
) -> str:
    bodies: list[str] = []
    parsed = salvaged = failed = 0
    with zipfile.ZipFile(epub_path) as zf:
        for content_path in content_paths:
            try:
                raw = zf.read(content_path)
            except KeyError:
                failed += 1
                continue
            ctree = _parse_content_html(raw)
            body = ctree.find(".//{*}body") if ctree is not None else None
            if body is None:
                raw_body = _salvage_raw_body(raw)
                if raw_body is not None:
                    bodies.append(
                        _rebase_salvaged_refs(
                            raw_body, posixpath.dirname(content_path), opf_dir
                        )
                    )
                    salvaged += 1
                else:
                    failed += 1
                continue
            parsed += 1
            chapter_dir = posixpath.dirname(content_path)
            clean = _strip_ns(body)
            if clean is None:
                failed += 1
                continue
            for elem in clean.iter():
                tag = elem.tag
                if tag in ("img", "source"):
                    src = elem.get("src")
                    if src:
                        resolved = _resolve_ref(src, chapter_dir, opf_dir)
                        elem.set("src", resolved)
                        if tag == "img" and image_dir:
                            _normalize_raster_image(
                                elem,
                                zf,
                                posixpath.join(opf_dir, resolved),
                                image_dir,
                                resolved,
                            )
                elif tag == "image":
                    src = elem.get("href") or elem.get("src")
                    if src:
                        resolved = _resolve_ref(src, chapter_dir, opf_dir)
                        elem.set("href", resolved)
                        if image_dir:
                            _normalize_raster_image(
                                elem,
                                zf,
                                posixpath.join(opf_dir, resolved),
                                image_dir,
                                resolved,
                            )
            bodies.append(ET.tostring(clean, encoding="unicode"))
        if image_dir and styles:
            styles = _normalize_css_backgrounds(styles, zf, opf_dir, image_dir)
    if failed:
        logger.warning(
            "weasyprint: merge for %s: %d parsed, %d salvaged, %d failed (of %d spine chapters)",
            os.path.basename(epub_path),
            parsed,
            salvaged,
            failed,
            len(content_paths),
        )
    style_block = f"<style>{styles}</style>" if styles else ""
    page_css, _content_w = _render_page_config()
    # !important on max-width beats book CSS like `img { width: 2000px
    # !important }` that would otherwise defeat our inline caps and blow the
    # page box (right-edge clipping). Ours is appended last, so for same-
    # origin !important declarations the later rule wins.
    _wp_fix = (
        "<style>"
        f"{page_css}"
        "img, image, svg { max-width: 100% !important; height: auto; } "
        "img, image { margin-left: 0 !important; margin-right: 0 !important; } "
        "body { margin: 0; padding: 0; } "
        "</style>"
    )
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        f"{style_block}{_wp_fix}</head><body>{''.join(bodies)}</body></html>"
    )


def _merged_html_is_blank(html: str) -> bool:
    text = re.sub(r"<[^>]+>", "", html)
    text = "".join(ch for ch in text if ch != "\ufffd").strip()
    if len(text) >= _MIN_MERGE_TEXT_CHARS:
        return False
    lowered = html.lower()
    return not (
        "<img" in lowered
        or "<image" in lowered
        or "<svg" in lowered
        or "<picture" in lowered
        or "url(" in lowered
    )


def _extract_epub(epub_path: str, dest_dir: str) -> None:
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
    from weasyprint import HTML

    temp_pdf = pdf_path + ".wp.tmp"
    state: dict = {"ok": False, "abandoned": False}
    done = threading.Event()

    def _run() -> None:
        try:
            HTML(string=html, base_url=base_url).write_pdf(temp_pdf)
            if state.get("abandoned"):
                # The caller already gave up (timeout/cancel); the render must
                # not leave its temp PDF behind, since nothing will reap it.
                try:
                    if os.path.exists(temp_pdf):
                        os.remove(temp_pdf)
                except Exception:
                    pass
                return
            if os.path.exists(temp_pdf) and os.path.getsize(temp_pdf) > 0:
                state["ok"] = True
        except Exception as exc:
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
        # Flag the still-running render as abandoned so it cleans up its own
        # temp file when it eventually finishes (a thread cannot be killed).
        state["abandoned"] = True
        try:
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)
        except Exception:
            pass
        return False
    try:
        os.replace(temp_pdf, pdf_path)
    except Exception:
        return False
    return True


def _pdf_fails_sanity_check(pdf_path: str) -> str | None:
    """Return a reason string when the rendered PDF looks broken, else None.

    Routes to the Calibre fallback only for genuinely broken output: blank
    renders, zero pages, absurd page dimensions, or images spilling well past
    the page box. Small overflows (beyond the base tolerance but within
    PDF_OVERFLOW_FATAL_PT) are logged and accepted -- a slightly clipped page
    ships, because routing it to Calibre previously OOM-killed the worker. The
    scan is wall-clock budgeted (PDF_SANITY_CHECK_BUDGET_S) so inspecting a big
    book can never eat minutes, and uses a single get_image_info() pass per
    page instead of per-image get_image_rects() re-walks.
    """
    import config as _cfg

    try:
        budget_s = float(getattr(_cfg, "PDF_SANITY_CHECK_BUDGET_S", 0) or 0)
        fatal_pt = float(getattr(_cfg, "PDF_OVERFLOW_FATAL_PT", 0) or 0)
    except Exception:
        budget_s, fatal_pt = 0.0, 0.0
    if budget_s <= 0:
        budget_s = _SANITY_CHECK_BUDGET_S
    if fatal_pt <= 0:
        fatal_pt = _PDF_OVERFLOW_FATAL_PT
    try:
        size = os.path.getsize(pdf_path)
        if size == 0:
            return "empty PDF file"
        import fitz

        doc = fitz.open(pdf_path)
        try:
            if doc.page_count == 0:
                return "PDF has zero pages"
            total_text = 0
            total_images = 0
            start = time.monotonic()
            for page in doc:
                if time.monotonic() - start > budget_s:
                    logger.warning(
                        "weasyprint: sanity scan of %s exceeded %ss budget; accepting render",
                        os.path.basename(pdf_path),
                        budget_s,
                    )
                    return None
                pr = page.rect
                if (
                    pr.width < _MIN_PAGE_DIM_PT
                    or pr.height < _MIN_PAGE_DIM_PT
                    or pr.width > _MAX_PAGE_DIM_PT
                    or pr.height > _MAX_PAGE_DIM_PT
                ):
                    return (
                        f"page size {pr.width:.0f}x{pr.height:.0f}pt outside "
                        f"{_MIN_PAGE_DIM_PT:.0f}-{_MAX_PAGE_DIM_PT:.0f}pt"
                    )
                total_text += sum(
                    1 for ch in page.get_text() if ch != "\ufffd"
                )
                for info in page.get_image_info(xrefs=True):
                    total_images += 1
                    bbox = fitz.Rect(info.get("bbox", (0, 0, 0, 0)))
                    overflow = max(
                        0.0,
                        bbox.x1 - pr.x1,
                        -bbox.x0,
                        bbox.y1 - pr.y1,
                        -bbox.y0,
                    )
                    if overflow > _PAGE_OVERFLOW_TOLERANCE_PT + fatal_pt:
                        return (
                            f"image overflows the page box by {overflow:.0f}pt "
                            f"on page {page.number + 1}"
                        )
                    if overflow > _PAGE_OVERFLOW_TOLERANCE_PT:
                        logger.warning(
                            "weasyprint: image on page %d of %s sticks out %.1fpt "
                            "past the page box (accepted; fatal threshold %.0fpt)",
                            page.number + 1,
                            os.path.basename(pdf_path),
                            overflow,
                            fatal_pt,
                        )
            if (
                size <= _MIN_PDF_BYTES
                and total_text < _MIN_PDF_TEXT_CHARS
                and total_images == 0
            ):
                return f"blank render ({size} bytes, no text or images)"
            return None
        finally:
            doc.close()
    except Exception as exc:
        return f"PDF inspection failed: {exc}"


def _reencode_pdf_image(info: dict, jpg_quality: int) -> bytes | None:
    """Re-encode one extracted PDF image to a smaller JPEG (None = keep it).

    Images with an alpha mask are skipped (replacing them would break
    transparency), as are tiny icons not worth the quality loss and exotic
    color spaces (CMYK/Indexed/ICC) where a re-encode would shift colors.
    """
    try:
        from PIL import Image

        raw = info.get("image")
        if not raw:
            return None
        im = Image.open(BytesIO(raw))
        width, height = im.size
        if width <= 0 or height <= 0:
            return None
        if "A" in im.getbands():
            return None
        if width < 256 and height < 256:
            return None
        cs_name = str(info.get("cs-name") or "").lower()
        gray = cs_name in ("devicegray", "calgray")
        rgb = cs_name == "devicergb"
        if not gray and not rgb:
            return None
        buf = BytesIO()
        im.convert("L" if gray else "RGB").save(
            buf, "JPEG", quality=jpg_quality, optimize=True
        )
        return buf.getvalue()
    except Exception:
        return None


def _replace_pdf_image(doc, xref: int, new_bytes: bytes) -> bool:
    """Swap an image's stream data for re-encoded JPEG bytes (PyMuPDF 1.24
    has no replace_image(); update_stream must stay uncompressed or MuPDF
    will try to JPEG-decode the zlib wrapper)."""
    try:
        doc.update_stream(xref, new_bytes, new=False, compress=False)
        doc.xref_set_key(xref, "Filter", "/DCTDecode")
        doc.xref_set_key(xref, "BitsPerComponent", "8")
        if doc.xref_get_key(xref, "DecodeParms")[1] != "null":
            doc.xref_set_key(xref, "DecodeParms", "null")
        return True
    except Exception:
        return False


def _shrink_oversized_pdf(pdf_path: str) -> bool:
    """Post-render pass: re-encode embedded JPEG/PNG images so PDFs that still
    approach Telegram's upload limit get smaller. Replaces the file in place
    only when the gain is meaningful."""
    import config as _cfg

    try:
        threshold = int(getattr(_cfg, "PDF_RECOMPRESS_MIN_BYTES", 0) or 0)
    except Exception:
        threshold = 0
    if threshold <= 0:
        return False
    try:
        if os.path.getsize(pdf_path) <= threshold:
            return False
    except OSError:
        return False
    try:
        jpg_quality = int(
            getattr(_cfg, "PDF_RECOMPRESS_JPEG_QUALITY", 70) or 70
        )
        min_gain_pct = float(
            getattr(_cfg, "PDF_RECOMPRESS_MIN_GAIN_PCT", 5) or 5
        )
        min_gain_bytes = int(
            getattr(_cfg, "PDF_RECOMPRESS_MIN_GAIN_BYTES", 100000) or 100000
        )
    except Exception:
        jpg_quality, min_gain_pct, min_gain_bytes = 70, 5.0, 100000
    try:
        original_size = os.path.getsize(pdf_path)
        import fitz

        doc = fitz.open(pdf_path)
    except Exception:
        return False
    replaced = 0
    seen: set[int] = set()
    try:
        for page in doc:
            for img in page.get_images(full=True):
                xref = img[0]
                if xref in seen:
                    continue
                seen.add(xref)
                if img[1]:  # SMask (alpha): replacing would break transparency
                    continue
                try:
                    info = doc.extract_image(xref)
                except Exception:
                    continue
                if not info:
                    continue
                new_bytes = _reencode_pdf_image(info, jpg_quality)
                if not new_bytes or len(new_bytes) >= len(info["image"]):
                    continue
                if _replace_pdf_image(doc, xref, new_bytes):
                    replaced += 1
        if replaced == 0:
            return False
        tmp = pdf_path + ".recompress.tmp"
        try:
            doc.save(tmp, garbage=4, deflate=True)
        except Exception:
            return False
    finally:
        doc.close()
    try:
        new_size = os.path.getsize(tmp)
    except OSError:
        return False
    gained = original_size - new_size
    if (
        new_size < original_size
        and gained >= min_gain_bytes
        and (100.0 * gained / original_size) >= min_gain_pct
    ):
        try:
            os.replace(tmp, pdf_path)
        except OSError:
            return False
        logger.info(
            "weasyprint: recompressed %d image(s) in %s: %d -> %d bytes",
            replaced,
            os.path.basename(pdf_path),
            original_size,
            new_size,
        )
        return True
    try:
        os.remove(tmp)
    except OSError:
        pass
    return False


def _calibre_fallback(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int,
    cancel_check: Callable[[], bool] | None,
) -> bool:
    from utils.ebook_converter import convert_book_to_pdf_with_thumbnail

    return convert_book_to_pdf_with_thumbnail(
        input_path,
        pdf_path,
        thumb_path,
        timeout=timeout,
        cancel_check=cancel_check,
    )


def _calibre_fallback_shrunk(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int,
    cancel_check: Callable[[], bool] | None,
) -> bool:
    # Free the WeasyPrint render (a 200+-page layout tree can be hundreds of
    # MB) before ebook-convert starts; running both in the same container is
    # what previously OOM-killed the worker.
    gc.collect()
    if not _calibre_fallback(
        input_path, pdf_path, thumb_path, timeout, cancel_check
    ):
        return False
    _shrink_oversized_pdf(pdf_path)
    return True


def convert_epub_to_pdf_fast(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    import config as _cfg

    if epub_is_drm_protected(input_path):
        raise DRMProtectedError(
            f"DRM-protected EPUB: {os.path.basename(input_path)}"
        )

    if not weasyprint_available():
        return _calibre_fallback_shrunk(
            input_path, pdf_path, thumb_path, timeout, cancel_check
        )
    if not getattr(_cfg, "EPUB_FAST_CONVERT_ENABLED", True):
        return _calibre_fallback_shrunk(
            input_path, pdf_path, thumb_path, timeout, cancel_check
        )

    tmp = tempfile.mkdtemp(prefix="wp_epub_")
    _unreadable = False
    try:
        if cancel_check is not None and cancel_check():
            raise ConversionCancelledError(
                f"conversion cancelled: {input_path}"
            )
        _render_start = time.monotonic()
        opf_dir, spine_paths, styles = _parse_epub_spine(input_path)
        _extract_epub(input_path, tmp)
        image_dir = os.path.join(tmp, opf_dir)
        html = _build_merged_html(
            input_path, spine_paths, styles, opf_dir, image_dir=image_dir
        )
        base_url = image_dir
        if _merged_html_is_blank(html):
            if epub_is_drm_protected(input_path):
                raise DRMProtectedError(
                    f"DRM-protected EPUB: {os.path.basename(input_path)}"
                )
            _unreadable = True
            logger.warning(
                "weasyprint: merged content for %s is empty; using Calibre",
                os.path.basename(input_path),
            )
        elif _render_weasyprint(
            html, base_url, pdf_path, timeout, cancel_check
        ):
            sanity = _pdf_fails_sanity_check(pdf_path)
            if sanity:
                _unreadable = sanity.startswith("blank")
                logger.warning(
                    "weasyprint: rendered PDF failed sanity check (%s) for %s; using Calibre",
                    sanity,
                    os.path.basename(input_path),
                )
                try:
                    os.remove(pdf_path)
                except OSError:
                    pass
            else:
                _shrink_oversized_pdf(pdf_path)
                finalize_cover_thumbnail(input_path, pdf_path, thumb_path)
                return True
        else:
            if cancel_check is not None and cancel_check():
                raise ConversionCancelledError(
                    f"conversion cancelled: {input_path}"
                )
            logger.warning(
                "weasyprint: render failed/timed out after %ss for %s; using Calibre",
                int(time.monotonic() - _render_start),
                os.path.basename(input_path),
            )
    except ConversionCancelledError:
        raise
    except DRMProtectedError:
        raise
    except Exception as exc:
        logger.warning(
            "weasyprint: fast path failed (%s) for %s; using Calibre",
            exc,
            os.path.basename(input_path),
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    _remain = timeout - int(time.monotonic() - _render_start)
    if _unreadable:
        _cap = getattr(_cfg, "EPUB_EMPTY_MERGE_FALLBACK_SECONDS", 240)
        logger.warning(
            "weasyprint: capping Calibre fallback for %s at %ss (content was unreadable)",
            os.path.basename(input_path),
            min(_cap, max(60, _remain)),
        )
        _remain = min(_cap, max(60, _remain))
    return _calibre_fallback_shrunk(
        input_path, pdf_path, thumb_path, max(60, _remain), cancel_check
    )
