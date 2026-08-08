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
  heuristically text-heavy.  ANY failure — parse error, empty merge, blank
  render, render exception, timeout, cancel — drops back to the Calibre path
  so the fast path can never regress a conversion Calibre would have
  completed.
- A /canceljob that fires while WeasyPrint renders raises
  :class:`ConversionCancelledError` (mirroring Calibre's abort), so a
  cancelled fast path never triggers a fresh Calibre conversion.
- Chapters are parsed with the SAME HTML5 engine WeasyPrint renders with
  (tinyhtml5 → html5lib).  Real-world EPUB chapters are frequently NOT
  well-formed XML (unclosed tags, ``&nbsp;``-style named entities, unquoted
  attributes); a strict XML parser silently drops them, which would merge
  into an empty document and produce a blank PDF.
- The rendered output is VALIDATED before delivery: a tiny single-page PDF
  with no text and no images is treated as a blank render and re-run through
  Calibre.  A silent empty PDF must never reach the user.
- The pure-EPUB parts (image-weight heuristic, spine parsing, HTML merging,
  blank checks) are importable and testable without WeasyPrint installed.
"""

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

# EPUB XHTML routinely carries a DOCTYPE declaration.  defusedxml rejects ANY
# doctype (even benign ones) to block entity-expansion attacks; the declaration
# carries no layout value for the body/style merge, so it is stripped before
# parsing.  Without a doctype there is nothing an entity attack can hang on,
# and defusedxml remains the parser of record for untrusted EPUB metadata.
_DOCTYPE_RE = re.compile(
    rb"<!DOCTYPE(?:\s+[^>\[\]]*)?(?:\[[^\]]*\])?[^>]*>",
    re.IGNORECASE | re.DOTALL,
)

# Content media types accepted in the spine.  EPUB3 XHTML carries
# ``application/xhtml+xml``; older books may use ``text/html``.
_CONTENT_PREFIXES = ("application/xhtml", "text/html")
# Files whose bytes count as "image content" for the text-heavy heuristic.
_IMAGE_EXTS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".svg", ".avif",
)
# A merged document with less than this much text AND no images is blank.
_MIN_MERGE_TEXT_CHARS = 20
# A rendered PDF under this size is immediately suspect (a real book embeds
# fonts and produces a larger file than a single empty page).
_MIN_PDF_BYTES = 8 * 1024
# A page needs at least this much extractable text to count as real content.
_MIN_PDF_TEXT_CHARS = 40
# XML prolog encoding declaration (``<?xml ... encoding="..."?>``) — honored
# for decoding before the HTML5 parser runs.
_XML_ENCODING_RE = re.compile(
    rb"<\?xml[^>]*encoding=[\"']([A-Za-z0-9._-]+)[\"']", re.IGNORECASE
)


def _safe_xml_parse(raw: bytes) -> ET.Element:
    """Parse untrusted EPUB XML with defusedxml (DOCTYPE already stripped)."""
    return ElementTree.fromstring(_DOCTYPE_RE.sub(b"", raw))


# Cached HTML5 parser: a callable(raw) -> root Element, ``False`` once we know
# no lenient parser is installed, ``None`` before first use.
_HTML_PARSER: Callable[[bytes], ET.Element] | bool | None = None


def _get_lenient_parser() -> Callable[[bytes], ET.Element] | None:
    """Return an HTML5 parser (tinyhtml5 → html5lib) or None.

    WeasyPrint renders HTML with an HTML5 parser, so chapters are parsed the
    same way here.  Returns None only when neither library is installed; the
    callers then fall back to strict defusedxml parsing.
    """
    global _HTML_PARSER
    if _HTML_PARSER is not None:
        return _HTML_PARSER if _HTML_PARSER is not False else None
    try:
        import tinyhtml5

        def _parse_tiny(raw: bytes) -> ET.Element:
            return tinyhtml5.parse(
                raw, treebuilder="etree", namespaceHTMLElements=True
            )

        _HTML_PARSER = _parse_tiny
    except Exception:
        try:
            import html5lib

            def _parse_h5lib(raw: bytes) -> ET.Element:
                return html5lib.parse(
                    raw, treebuilder="etree", namespaceHTMLElements=True
                )

            _HTML_PARSER = _parse_h5lib
        except Exception:
            _HTML_PARSER = False
    return _HTML_PARSER if _HTML_PARSER is not False else None


def _decode_chapter(raw: bytes) -> bytes | str:
    """Decode chapter bytes honoring the XML prolog's declared encoding.

    The HTML5 tokenizer treats ``<?xml encoding="..."?>`` as a bogus comment
    and defaults to UTF-8 for byte input, so non-UTF-8 chapters (common in
    older EPUBs: windows-1252 / latin-1) would otherwise render as mojibake
    — a silent wrong-success.  Returns ``str`` when the declared encoding
    decodes cleanly, else the raw bytes (parser sniffs BOM/UTF-8 as usual).
    """
    match = _XML_ENCODING_RE.search(raw)
    if not match:
        return raw
    try:
        return raw.decode(match.group(1).decode("ascii"))
    except (LookupError, UnicodeDecodeError):
        return raw


def _parse_content_html(raw: bytes) -> ET.Element | None:
    """Parse a chapter's content, preferring the lenient HTML5 parser.

    Returns the parsed root element (namespaced), or None when unparseable.
    """
    parser = _get_lenient_parser()
    if parser is None:
        try:
            return _safe_xml_parse(raw)
        except Exception:  # nosec B110 - unparseable chapter
            return None
    try:
        return parser(_decode_chapter(raw))
    except Exception:  # nosec B110 - unparseable chapter
        return None


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
            ctree = _parse_content_html(raw)
            if ctree is None:
                continue
            for style in ctree.findall(".//{*}style"):
                if style.text and style.text.strip():
                    styles.append(style.text)
    return opf_dir, spine, "\n".join(styles)


def _strip_ns(elem: ET.Element) -> ET.Element | None:
    """Namespace-free copy of an element subtree (XHTML → plain HTML).

    ElementTree serializes default-namespace XHTML with ``ns0:`` prefixes,
    which bloats the merged document and breaks selectors; stripping
    namespaces yields clean markup WeasyPrint (an HTML5 parser) handles
    correctly.  Namespaced attributes (e.g. ``xlink:href``) collapse to their
    local name.  Comment/processing nodes (non-string tags, produced by HTML5
    parsers) are dropped.
    """
    if not isinstance(elem.tag, str):
        return None
    tag = elem.tag
    if "}" in tag:
        tag = tag.split("}", 1)[-1]
    new = ET.Element(tag)
    for key, value in elem.attrib.items():
        if key.startswith("{"):
            key = key.split("}", 1)[-1]
        new.set(key, value)
    new.text = elem.text
    new.tail = elem.tail
    for child in elem:
        stripped = _strip_ns(child)
        if stripped is not None:
            new.append(stripped)
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


# Fallback salvage for chapters that defeat BOTH the HTML5 parser and the
# strict XML parser (e.g. severely mangled markup).  Regex-grab the raw
# ``<body>...</body>`` region and hand it to WeasyPrint verbatim — its own
# HTML5 parser tolerates far more than the tree builders above.
_RAW_BODY_RE = re.compile(
    rb"<body[^>]*>(.*?)</body>", re.IGNORECASE | re.DOTALL
)
_TAG_RE = re.compile(rb"<[^>]+>")
# head/style/script/title blocks must never count as book content during
# the no-body salvage (titles, CSS, and JS are not the book's text).
_SKIP_REGION_RE = re.compile(
    rb"<(head|style|script|title)[^>]*>.*?</\1>",
    re.IGNORECASE | re.DOTALL,
)
# Mangled chapters sometimes NEVER close a region — treat any unclosed
# head/style/script/title as running to end-of-content so its CSS/JS/title
# text still can't fake a salvage.
_UNCLOSED_REGION_RE = re.compile(
    rb"<(head|style|script|title)[^>]*>(.*)$",
    re.IGNORECASE | re.DOTALL,
)
# src/href attributes inside SALVAGED raw-body markup (the parsed path
# rebases refs via _resolve_ref; salvaged markup needs the same pass).
_SALVAGED_REF_RE = re.compile(
    rb"(?P<attr>\b(?:src|href)\s*=\s*)(?P<quote>['\"])(?P<url>[^'\"]+)",
    re.IGNORECASE,
)


def _rebase_salvaged_refs(markup: str, chapter_dir: str, opf_dir: str) -> str:
    """Rebase src/href refs inside salvaged body markup (chapter→OPF).

    The parsed path rewrites image refs so they resolve when the merged
    document is served from the OPF directory; the raw-body salvage returns
    markup verbatim, so broken image refs would silently render in the PDF.
    Same rules as :func:`_resolve_ref`: fragments, data:/scheme, absolute
    paths and URLs pass through untouched.
    """

    def _sub(m: re.Match[bytes]) -> bytes:
        url = m.group("url")
        if (
            url.startswith((b"#", b"data:", b"/"))
            or b"://" in url
            or not url
        ):
            return m.group(0)
        rebased = _resolve_ref(
            url.decode("utf-8", errors="replace"), chapter_dir, opf_dir
        ).encode("utf-8", errors="replace")
        return m.group("attr") + m.group("quote") + rebased + m.group("quote")

    return _SALVAGED_REF_RE.sub(_sub, markup.encode("utf-8")).decode(
        "utf-8", errors="replace"
    )


def _salvage_raw_body(raw: bytes) -> str | None:
    """Extract a chapter's raw content when both parsers fail.

    First tries the raw ``<body>`` region (kept as markup, so WeasyPrint's
    own tolerant HTML5 parser handles it).  Fragmented XHTML — common in
    iBookZZ-era EPUBs — often has NO usable ``<body>`` region (missing,
    empty, or content spilled past ``</body>``), so when the region is
    empty we fall back to stripping head/style/script blocks and ALL tags
    from the whole chapter and returning the plain text (entity-unescaped,
    re-escaped, wrapped in ``<p>``) — a plain-text page beats a silently
    dropped chapter, but CSS/JS/titles can't fake a salvage.  Returns None
    only when nothing meaningful can be salvaged.
    """
    m = _RAW_BODY_RE.search(raw)
    if m:
        inner = m.group(1)
        if inner.strip():
            decoded = _decode_chapter(inner)
            if isinstance(decoded, bytes):
                return decoded.decode("utf-8", errors="replace")
            return decoded
        # Empty <body> region — fall through to the whole-document salvage
        # (with head/style/script removed) so content spilled AFTER
        # ``</body>`` is still rescued, while ``<title>`` never is.
    stripped = _UNCLOSED_REGION_RE.sub(
        b"", _SKIP_REGION_RE.sub(b"", raw)
    )
    stripped = _TAG_RE.sub(b"", stripped)
    if not stripped.strip():
        return None
    decoded = _decode_chapter(stripped)
    if isinstance(decoded, bytes):
        decoded = decoded.decode("utf-8", errors="replace")
    text = html.unescape(decoded).strip()
    if not text:
        return None
    # Re-escape: stray < > left behind by the tag-strip must not be parsed
    # as markup when the text is injected into the merged document.
    return f"<p>{html.escape(text)}</p>"


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
    directory (``opf_dir``).  Chapters whose body can't be parsed are skipped
    — but only after a raw-``<body>`` salvage attempt, so a book with a few
    mangled chapters still merges the rest instead of silently dropping them.
    A completely empty result is an empty document (callers detect this with
    :func:`_merged_html_is_blank` and fall back to Calibre).
    """
    bodies: list[str] = []
    parsed = 0
    salvaged = 0
    failed = 0
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
                # Both parsers failed on this chapter — try the raw body
                # region before giving up on it.
                raw_body = _salvage_raw_body(raw)
                if raw_body is not None:
                    bodies.append(
                        _rebase_salvaged_refs(
                            raw_body,
                            posixpath.dirname(content_path),
                            opf_dir,
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
    if failed:
        logger.warning(
            "weasyprint: merge for %s: %d parsed, %d salvaged, %d failed "
            "(of %d spine chapters)",
            os.path.basename(epub_path),
            parsed,
            salvaged,
            failed,
            len(content_paths),
        )
    style_block = f"<style>{styles}</style>" if styles else ""
    return (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        f"{style_block}</head><body>{''.join(bodies)}</body></html>"
    )


def _merged_html_is_blank(html: str) -> bool:
    """True when the merged document has no meaningful content.

    Chapters that fail to parse are skipped during the merge; a book whose
    chapters are ALL unparseable yields an empty body — WeasyPrint would
    render a single blank page (a silent wrong-success).  Detected BEFORE
    rendering so the caller falls back to Calibre instead of delivering an
    empty PDF.
    """
    text = re.sub(r"<[^>]+>", "", html).strip()
    if len(text) >= _MIN_MERGE_TEXT_CHARS:
        return False
    lowered = html.lower()
    # Media counts as content: an image-only book (or one whose chapters are
    # <picture>-based or use CSS background-image) is NOT "empty" and must
    # keep the full Calibre budget, not the unreadable-content cap.
    return not (
        "<img" in lowered
        or "<image" in lowered
        or "<svg" in lowered
        or "<picture" in lowered
        or "url(" in lowered
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


def _pdf_is_degenerate(pdf_path: str) -> bool:
    """True when a rendered PDF is an empty/blank deliverable.

    A real book PDF embeds fonts and exceeds the size gate; a degenerate
    fast-path output is a tiny PDF (all pages combined) with no extractable
    text and no images — exactly what WeasyPrint emits when the merged
    document was empty (the silent wrong-success seen in production).  All
    pages are inspected (a blank merge whose CSS forces page breaks can span
    several empty pages).  Unreadable/unavailable files are never judged
    degenerate (we keep the PDF rather than re-convert).
    """
    try:
        if os.path.getsize(pdf_path) > _MIN_PDF_BYTES:
            return False
        import fitz

        doc = fitz.open(pdf_path)
        try:
            text_len = sum(
                len(page.get_text().strip()) for page in doc
            )
            image_count = sum(
                len(page.get_images(full=True)) for page in doc
            )
            return text_len < _MIN_PDF_TEXT_CHARS and image_count == 0
        finally:
            doc.close()
    except Exception:  # nosec B110 - can't judge; keep the PDF
        return False


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
    and the EPUB is heuristically text-heavy.  Any failure anywhere — an empty
    merge, a blank rendered PDF, a parse error, a timeout — falls back to the
    Calibre path, so this function can only ever be as good as the status quo,
    never worse.  Thumbnail behavior mirrors
    ``convert_book_to_pdf_with_thumbnail`` (cover via ``ebook-meta``, else a
    preview of the produced PDF's first page).
    """
    import config as _cfg

    # DRM-protected EPUBs can't be read by EITHER converter here (WeasyPrint
    # sees empty chapters, Calibre has no decryption plugin and would burn
    # the full timeout).  Fail fast with a clear error so the user isn't left
    # staring at a frozen progress bar for 10 minutes.
    if epub_is_drm_protected(input_path):
        raise DRMProtectedError(
            f"DRM-protected EPUB: {os.path.basename(input_path)}"
        )

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
    # Set when the fast path gave up because the CONTENT was unreadable
    # (empty merge or blank render) rather than because rendering itself
    # failed/timed out.  Such books get a BOUNDED Calibre fallback (see the
    # tail) so an unreadable file can't burn the full caller timeout.
    _unreadable = False
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
        if _merged_html_is_blank(html):
            # All chapters were unparseable → WeasyPrint would emit a blank
            # PDF.  When the cause is DRM encryption, Calibre can't read it
            # either — fail fast instead of falling into the timeout trap.
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
            if _pdf_is_degenerate(pdf_path):
                _unreadable = True
                logger.warning(
                    "weasyprint: rendered a blank %s-byte PDF for %s; "
                    "using Calibre",
                    os.path.getsize(pdf_path),
                    os.path.basename(input_path),
                )
                try:
                    os.remove(pdf_path)
                except OSError:  # nosec B110 - already gone is fine
                    pass
            else:
                # finalize_cover_thumbnail swallows its own errors.
                finalize_cover_thumbnail(input_path, pdf_path, thumb_path)
                return True
        else:
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
    except DRMProtectedError:
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
    if _unreadable:
        # The content itself was unreadable (empty merge / blank render): a
        # fresh conversion is a long shot, so cap the fallback budget instead
        # of inheriting the caller's full timeout.  The merge-side salvage
        # above already rescued whatever was parseable, so what remains is
        # genuinely broken — a few minutes of Calibre is a fair shake.
        _cap = getattr(_cfg, "EPUB_EMPTY_MERGE_FALLBACK_SECONDS", 240)
        logger.warning(
            "weasyprint: capping Calibre fallback for %s at %ss "
            "(content was unreadable)",
            os.path.basename(input_path),
            min(_cap, max(60, _remain)),
        )
        _remain = min(_cap, max(60, _remain))
    return _calibre_fallback(
        input_path, pdf_path, thumb_path, max(60, _remain), cancel_check
    )
