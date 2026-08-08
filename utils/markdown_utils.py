"""Shared helpers for Telegram legacy-Markdown message safety.

User-controlled strings (names, phones, error text, etc.) must never be
interpolated raw into ``parse_mode="Markdown"`` messages: a lone ``_`` or
``*`` crashes the send with "Can't parse entities". Keep the escape logic
in one place so callers can't drift apart.

:func:`sanitize_text` additionally guarantees outbound text always encodes
cleanly to UTF-8 (a corrupted surrogate escape left in a string crashes the
send layer with ``UnicodeEncodeError: surrogates not allowed`` — httpx and
requests both URL-encode outbound form fields), so no third-party content
(e.g. a filename, phone or error string) can ever take down a send.
"""


def sanitize_text(text: str) -> str:
    """Repair text so it can never crash a Telegram send (UTF-8 safe).

    Handles the three failure classes that can come from third-party
    content:

    * **Lone surrogates** — a corrupted backslash-``\\u`` escape leaves an
      unpaired surrogate codepoint in the string, which the send layer
      cannot URL-encode (``surrogates not allowed`` — this crashed
      ``/sessionstatus`` in production).  Valid surrogate PAIRS are
      reassembled into their real codepoint; lone surrogates are dropped.
    * **C0 control characters** (except ``\\t``/``\\n``/``\\r``) — Telegram
      rejects messages containing them (400 ``Bad Request``), so they are
      stripped rather than left to fail the whole send.
    * **Markdown-breaking characters** are NOT touched here — escape with
      :func:`escape_markdown`/:func:`safe_code_span` where user text is
      interpolated into a Markdown message.

    Defensive: message text should always use proper backslash-``U``
    (``\\U0001XXXX``) escapes, but this guarantees a send never fails on
    the text itself.
    """
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        o = ord(c)
        if 0xD800 <= o <= 0xDBFF:  # high surrogate
            if i + 1 < n:
                o2 = ord(text[i + 1])
                if 0xDC00 <= o2 <= 0xDFFF:  # valid pair -> real codepoint
                    out.append(
                        chr(0x10000 + ((o - 0xD800) << 10) + (o2 - 0xDC00))
                    )
                    i += 2
                    continue
            i += 1  # lone high surrogate — drop
            continue
        if 0xDC00 <= o <= 0xDFFF:  # lone low surrogate — drop
            i += 1
            continue
        if o < 0x20 and o not in (0x09, 0x0A, 0x0D):
            # C0 control char other than \t \n \r — Telegram rejects them.
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def safe_code_span(text: str) -> str:
    """Sanitize text for embedding inside a Markdown backtick code span.

    Telegram does not parse entities inside a code span, so ``_``/``*`` are
    safe there -- only a literal backtick would terminate the span early.
    Strip them so a user-supplied id can't break out of the span (unlike
    :func:`escape_markdown`, which must never be used inside backticks: its
    ``\\``-escapes render literally there).
    """
    return str(text).replace("`", "'")


def escape_markdown(text: str) -> str:
    """Escape Telegram legacy-Markdown special chars in user-supplied text.

    Applies to values embedded as *raw text* (not inside backticks).  Inside
    a backtick code span entities are not parsed, so ``_``/``*`` are safe
    there — only literal backticks must be stripped/replaced separately.
    """
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("`", "\\`")
        .replace("[", "\\[")
    )
