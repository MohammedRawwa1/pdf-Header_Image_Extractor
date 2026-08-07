"""Shared helpers for Telegram legacy-Markdown message safety.

User-controlled strings (names, phones, error text, etc.) must never be
interpolated raw into ``parse_mode="Markdown"`` messages: a lone ``_`` or
``*`` crashes the send with "Can't parse entities". Keep the escape logic
in one place so callers can't drift apart.
"""


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
