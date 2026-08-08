"""Shared helpers for Telegram legacy-Markdown message safety.

User-controlled strings (names, phones, error text, etc.) must never be
interpolated raw into ``parse_mode="Markdown"`` messages: a lone ``_`` or
``*`` crashes the send with "Can't parse entities". Keep the escape logic
in one place so callers can't drift apart.
"""


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
