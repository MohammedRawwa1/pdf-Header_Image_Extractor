"""Fast EPUB→PDF conversion via WeasyPrint with Calibre fallback."""

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
_XML_ENCODING_RE = re.compile(
    rb"<\?xml[^>]*encoding=[\"']([A-Za-z0-9._-]+)[\"']", re.IGNORECASE
)
_RAW_BODY_RE = re.compile(rb"<body[^>]*>(.*?)</body>", re.IGNORECASE | re.DOTALL)
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

_HTML_PARSER: Callable[[bytes], ET.Element] | bool | None = None


def _safe_xml_parse(raw: bytes) -> ET.Element:
    return ElementTree.fromstring(_DOCTYPE_RE.sub(b"", raw))


def _get_lenient_parser() -> Callable[[bytes], ET.Element] | None:
    global _HTML_PARSER
    if _HTML_PARSER is not None:
        return _HTML_PARSER if _HTML_PARSER is not False else None
    try:
        import tinyhtml5
        _HTML_PARSER = lambda raw: tinyhtml5.parse(raw, treebuilder="etree", namespaceHTMLElements=True)
    except Exception:
        try:
            import html5lib
            _HTML_PARSER = lambda raw: html5lib.parse(raw, treebuilder="etree", namespaceHTMLElements=True)
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
        import weasyprint
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
                    styles.append(style.text)
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


def _rebase_salvaged_refs(markup: str, chapter_dir: str, opf_dir: str) -> str:
    def _sub(m: re.Match[bytes]) -> bytes:
        url = m.group("url")
        if url.startswith((b"#", b"data:", b"/")) or b"://" in url or not url:
            return m.group(0)
        rebased = _resolve_ref(url.decode("utf-8", errors="replace"), chapter_dir, opf_dir).encode("utf-8", errors="replace")
        return m.group("attr") + m.group("quote") + rebased + m.group("quote")
    return _SALVAGED_REF_RE.sub(_sub, markup.encode("utf-8")).decode("utf-8", errors="replace")


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


def _build_merged_html(
    epub_path: str,
    content_paths: list[str],
    styles: str,
    opf_dir: str,
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
                    bodies.append(_rebase_salvaged_refs(raw_body, posixpath.dirname(content_path), opf_dir))
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
                        elem.set("src", _resolve_ref(src, chapter_dir, opf_dir))
                elif tag == "image":
                    src = elem.get("href") or elem.get("src")
                    if src:
                        elem.set("href", _resolve_ref(src, chapter_dir, opf_dir))
            bodies.append(ET.tostring(clean, encoding="unicode"))
    if failed:
        logger.warning(
            "weasyprint: merge for %s: %d parsed, %d salvaged, %d failed (of %d spine chapters)",
            os.path.basename(epub_path), parsed, salvaged, failed, len(content_paths),
        )
    style_block = f"<style>{styles}</style>" if styles else ""
    _wp_fix = (
        "<style>"
        "img, image, svg { max-width: 100%%; height: auto; } "
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
    return not ("<img" in lowered or "<image" in lowered or "<svg" in lowered or "<picture" in lowered or "url(" in lowered)


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
    state: dict = {"ok": False}
    done = threading.Event()

    def _run() -> None:
        try:
            HTML(string=html, base_url=base_url).write_pdf(temp_pdf)
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


def _pdf_is_degenerate(pdf_path: str) -> bool:
    try:
        if os.path.getsize(pdf_path) > _MIN_PDF_BYTES:
            return False
        import fitz
        doc = fitz.open(pdf_path)
        try:
            text_len = sum(
                len("".join(ch for ch in page.get_text() if ch != "\ufffd").strip())
                for page in doc
            )
            image_count = sum(len(page.get_images(full=True)) for page in doc)
            return text_len < _MIN_PDF_TEXT_CHARS and image_count == 0
        finally:
            doc.close()
    except Exception:
        return False


def _calibre_fallback(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int,
    cancel_check: Callable[[], bool] | None,
) -> bool:
    from utils.ebook_converter import convert_book_to_pdf_with_thumbnail
    return convert_book_to_pdf_with_thumbnail(input_path, pdf_path, thumb_path, timeout=timeout, cancel_check=cancel_check)


def convert_epub_to_pdf_fast(
    input_path: str,
    pdf_path: str,
    thumb_path: str,
    timeout: int = 600,
    cancel_check: Callable[[], bool] | None = None,
) -> bool:
    import config as _cfg

    if epub_is_drm_protected(input_path):
        raise DRMProtectedError(f"DRM-protected EPUB: {os.path.basename(input_path)}")

    if not weasyprint_available():
        return _calibre_fallback(input_path, pdf_path, thumb_path, timeout, cancel_check)
    if not getattr(_cfg, "EPUB_FAST_CONVERT_ENABLED", True):
        return _calibre_fallback(input_path, pdf_path, thumb_path, timeout, cancel_check)

    tmp = tempfile.mkdtemp(prefix="wp_epub_")
    _unreadable = False
    try:
        if cancel_check is not None and cancel_check():
            raise ConversionCancelledError(f"conversion cancelled: {input_path}")
        _render_start = time.monotonic()
        opf_dir, spine_paths, styles = _parse_epub_spine(input_path)
        _extract_epub(input_path, tmp)
        html = _build_merged_html(input_path, spine_paths, styles, opf_dir)
        base_url = os.path.join(tmp, opf_dir)
        if _merged_html_is_blank(html):
            if epub_is_drm_protected(input_path):
                raise DRMProtectedError(f"DRM-protected EPUB: {os.path.basename(input_path)}")
            _unreadable = True
            logger.warning("weasyprint: merged content for %s is empty; using Calibre", os.path.basename(input_path))
        elif _render_weasyprint(html, base_url, pdf_path, timeout, cancel_check):
            if _pdf_is_degenerate(pdf_path):
                _unreadable = True
                logger.warning("weasyprint: rendered a blank %s-byte PDF for %s; using Calibre", os.path.getsize(pdf_path), os.path.basename(input_path))
                try:
                    os.remove(pdf_path)
                except OSError:
                    pass
            else:
                finalize_cover_thumbnail(input_path, pdf_path, thumb_path)
                return True
        else:
            if cancel_check is not None and cancel_check():
                raise ConversionCancelledError(f"conversion cancelled: {input_path}")
            logger.warning("weasyprint: render failed/timed out after %ss for %s; using Calibre", int(time.monotonic() - _render_start), os.path.basename(input_path))
    except ConversionCancelledError:
        raise
    except DRMProtectedError:
        raise
    except Exception as exc:
        logger.warning("weasyprint: fast path failed (%s) for %s; using Calibre", exc, os.path.basename(input_path))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    _remain = timeout - int(time.monotonic() - _render_start)
    if _unreadable:
        _cap = getattr(_cfg, "EPUB_EMPTY_MERGE_FALLBACK_SECONDS", 240)
        logger.warning("weasyprint: capping Calibre fallback for %s at %ss (content was unreadable)", os.path.basename(input_path), min(_cap, max(60, _remain)))
        _remain = min(_cap, max(60, _remain))
    return _calibre_fallback(input_path, pdf_path, thumb_path, max(60, _remain), cancel_check)
