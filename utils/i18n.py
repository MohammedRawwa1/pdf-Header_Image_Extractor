"""Bilingual (English / Arabic) catalog for the PDF bot's user-facing text.

The bot was English-only and its command list existed in three different
places with drifting contents (``bot.py`` twice and ``manage_commands.py``).
This module is now the single source for the command list and holds the Arabic
side of ``/start``, ``/help`` and the action-menu buttons.

Language selection: explicit, then the script of the text being handled, then
``BOT_LANG`` (``en`` by default, ``ar`` to switch the UI). Default behaviour is
unchanged, so existing English output is preserved byte for byte.

Only text lives here; callers do their own escaping.
"""

import os
import re

SUPPORTED = ("en", "ar")

_AR_LETTER_RE = re.compile("[\u0600-\u06ff\u0750-\u077f\u08a0-\u08ff\ufb50-\ufdff\ufe70-\ufeff]")
_LATIN_LETTER_RE = re.compile(r"[A-Za-z]")

#: ``(command, {lang: description})`` - the one command list.
COMMANDS = [
    ("start", {"en": "Start interaction with the bot", "ar": "\u0628\u062f\u0621 \u0627\u0633\u062a\u062e\u062f\u0627\u0645 \u0627\u0644\u0628\u0648\u062a"}),
    ("help", {"en": "Show help and available commands", "ar": "\u0639\u0631\u0636 \u0627\u0644\u0645\u0633\u0627\u0639\u062f\u0629 \u0648\u0627\u0644\u0623\u0648\u0627\u0645\u0631 \u0627\u0644\u0645\u062a\u0627\u062d\u0629"}),
    ("status", {"en": "Bot status: queue & your jobs", "ar": "\u062d\u0627\u0644\u0629 \u0627\u0644\u0628\u0648\u062a: \u0627\u0644\u0637\u0627\u0628\u0648\u0631 \u0648\u0645\u0647\u0627\u0645\u0643"}),
    ("login", {"en": "Login your Telethon userbot", "ar": "\u062a\u0633\u062c\u064a\u0644 \u062d\u0633\u0627\u0628\u0643 \u0641\u064a Telethon"}),
    ("loginpyro", {"en": "Login your Pyrogram userbot", "ar": "\u062a\u0633\u062c\u064a\u0644 \u062d\u0633\u0627\u0628\u0643 \u0641\u064a Pyrogram"}),
    ("loginstatus", {"en": "Check your live session health", "ar": "\u0641\u062d\u0635 \u062d\u0627\u0644\u0629 \u0627\u0644\u062c\u0644\u0633\u0629"}),
    ("logout", {"en": "Logout your Telethon session", "ar": "\u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062e\u0631\u0648\u062c \u0645\u0646 Telethon"}),
    ("logoutpyro", {"en": "Logout your Pyrogram session", "ar": "\u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062e\u0631\u0648\u062c \u0645\u0646 Pyrogram"}),
    ("clearflood", {"en": "Clear an active login flow", "ar": "\u0625\u0646\u0647\u0627\u0621 \u0639\u0645\u0644\u064a\u0629 \u062a\u0633\u062c\u064a\u0644 \u062f\u062e\u0648\u0644 \u0645\u0639\u0644\u0651\u0642\u0629"}),
    ("admin", {"en": "Manage allowed users", "ar": "\u0625\u062f\u0627\u0631\u0629 \u0627\u0644\u0645\u0633\u062a\u062e\u062f\u0645\u064a\u0646 \u0627\u0644\u0645\u0633\u0645\u0648\u062d \u0644\u0647\u0645"}),
    ("clear_cache", {"en": "(admin) Clear cached results & thumbnails", "ar": "(\u0645\u0634\u0631\u0641) \u0645\u0633\u062d \u0627\u0644\u0646\u062a\u0627\u0626\u062c \u0648\u0627\u0644\u0645\u0635\u063a\u0631\u0627\u062a \u0627\u0644\u0645\u062e\u0632\u0646\u0629"}),
    ("startbatch", {"en": "Start collecting forwarded files", "ar": "\u0628\u062f\u0621 \u062a\u062c\u0645\u064a\u0639 \u0627\u0644\u0645\u0644\u0641\u0627\u062a \u0627\u0644\u0645\u062d\u0648\u0651\u0644\u0629"}),
    ("endbatch", {"en": "Process collected batch", "ar": "\u0645\u0639\u0627\u0644\u062c\u0629 \u0627\u0644\u062f\u0641\u0639\u0629 \u0627\u0644\u0645\u062c\u0645\u0651\u0639\u0629"}),
    ("cancelbatch", {"en": "Cancel batch collection", "ar": "\u0625\u0644\u063a\u0627\u0621 \u062a\u062c\u0645\u064a\u0639 \u0627\u0644\u062f\u0641\u0639\u0629"}),
    ("ocr", {"en": "Set OCR output default (pdf/txt/picker)", "ar": "\u062a\u062d\u062f\u064a\u062f \u0645\u062e\u0631\u062c\u0627\u062a OCR (pdf/txt/\u0627\u062e\u062a\u064a\u0627\u0631)"}),
    ("canceljob", {"en": "Cancel a job (asks to confirm)", "ar": "\u0625\u0644\u063a\u0627\u0621 \u0645\u0647\u0645\u0629 (\u0645\u0639 \u062a\u0623\u0643\u064a\u062f)"}),
    ("cancelall", {"en": "(admin) Cancel all queued jobs", "ar": "(\u0645\u0634\u0631\u0641) \u0625\u0644\u063a\u0627\u0621 \u0643\u0644 \u0627\u0644\u0645\u0647\u0627\u0645 \u0627\u0644\u0645\u0646\u062a\u0638\u0631\u0629"}),
    ("cancel", {"en": "Cancel an active login flow", "ar": "\u0625\u0644\u063a\u0627\u0621 \u062a\u0633\u062c\u064a\u0644 \u062f\u062e\u0648\u0644 \u062c\u0627\u0631\u064d"}),
    ("setcommands", {"en": "(owner) Update the command list", "ar": "(\u0627\u0644\u0645\u0627\u0644\u0643) \u062a\u062d\u062f\u064a\u062b \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0623\u0648\u0627\u0645\u0631"}),
    ("sessionstatus", {"en": "(owner) Check userbot session health", "ar": "(\u0627\u0644\u0645\u0627\u0644\u0643) \u0641\u062d\u0635 \u062c\u0644\u0633\u0629 \u0627\u0644\u064a\u0648\u0632\u0631\u0628\u0648\u062a"}),
    ("setwebhook", {"en": "(owner) Set webhook URL", "ar": "(\u0627\u0644\u0645\u0627\u0644\u0643) \u062a\u0639\u064a\u064a\u0646 \u0631\u0627\u0628\u0637 \u0627\u0644\u0648\u064a\u0628\u0647\u0648\u0643"}),
    ("delwebhook", {"en": "(owner) Delete webhook", "ar": "(\u0627\u0644\u0645\u0627\u0644\u0643) \u062d\u0630\u0641 \u0627\u0644\u0648\u064a\u0628\u0647\u0648\u0643"}),
]

MESSAGES: dict[str, dict[str, str]] = {
    "en": {
        "access_denied": "Access denied. This bot is private.",
        "only_owner": "\u26d4 Only the bot owner can run this command.",
        "commands_updated": "Commands updated",
        "failed_commands": "\u274c Failed to update commands.",
        "menu_thumb": "\U0001f5bc\ufe0f Thumbnail",
        "menu_ocr_thumb": "\U0001f50e\U0001f5bc\ufe0f OCR & Thumbnail",
        "menu_compress_thumb": "\U0001f5dc\ufe0f\U0001f5bc\ufe0f Compress & Thumbnail",
        "menu_convert": "\U0001f501 Convert",
        "menu_ocr_thumb_pdf": "\U0001f50e\U0001f5bc\ufe0f OCR & Thumbnail PDF",
        "menu_cancel_all": "\U0001f5d1 Cancel all jobs",
        # Arabic side of /start and /help (English stays in bot.py)
        "start_ar": (
            "\U0001f389 \u0645\u0631\u062d\u0628\u064b\u0627 {name}!\n\n"
            "\U0001f4c4 \u0623\u0631\u0633\u0644 \u0645\u0644\u0641 **PDF** \u0623\u0648 **\u0635\u0648\u0631\u0629** \u0623\u0648 **\u0643\u062a\u0627\u0628\u064b\u0627 \u0625\u0644\u0643\u062a\u0631\u0648\u0646\u064a\u064b\u0627** \u0648\u0633\u0623\u0639\u0631\u0636 **\u0642\u0627\u0626\u0645\u0629 \u0625\u062c\u0631\u0627\u0621\u0627\u062a**\n"
            "\u2014 \u0627\u062e\u062a\u0631 \u0645\u0627 \u062a\u0631\u064a\u062f \u0648\u0644\u0646 \u064a\u064f\u0646\u0641\u0651\u0630 \u0633\u0648\u0627\u0647:\n"
            "\u2022 **PDF**: \U0001f5bc **\u0635\u0648\u0631\u0629 \u0645\u0635\u063a\u0651\u0631\u0629** \u00b7 \U0001f50e\U0001f5bc **OCR \u0648\u0635\u0648\u0631\u0629** \u00b7 \U0001f5dc\U0001f5bc **\u0636\u063a\u0637 \u0648\u0635\u0648\u0631\u0629**\n"
            "\u2022 **\u0627\u0644\u0635\u0648\u0631**: \U0001f5bc **\u0635\u0648\u0631\u0629** \u00b7 \U0001f50e\U0001f5bc **OCR \u0648\u0635\u0648\u0631\u0629**\n"
            "\u2022 **\u0627\u0644\u0643\u062a\u0628**: \U0001f501 **\u062a\u062d\u0648\u064a\u0644** \u00b7 \U0001f5dc **\u0636\u063a\u0637 PDF** \u00b7 \U0001f50e\U0001f5bc **OCR \u0648\u0635\u0648\u0631\u0629 PDF**\n\n"
            "\u26a1 **\u0623\u0648\u0627\u0645\u0631 \u0633\u0631\u064a\u0639\u0629:**\n"
            "\u2022 /help \u2014 \u0643\u0644 \u0627\u0644\u0623\u0648\u0627\u0645\u0631\n"
            "\u2022 /login \u2014 \u0631\u0628\u0637 \u062d\u0633\u0627\u0628\u0643 \u0641\u064a **Telethon** (\u0627\u0644\u0645\u0644\u0641\u0627\u062a \u0627\u0644\u0643\u0628\u064a\u0631\u0629)\n"
            "\u2022 /loginpyro \u2014 \u0631\u0628\u0637 \u062d\u0633\u0627\u0628\u0643 \u0641\u064a **Pyrogram** (\u0627\u0644\u0645\u0644\u0641\u0627\u062a \u0627\u0644\u0643\u0628\u064a\u0631\u0629)\n"
            "\u2022 /loginstatus \u2014 \u0641\u062d\u0635 **\u062d\u0627\u0644\u0629 \u062c\u0644\u0633\u062a\u0643**\n"
            "\u2022 /ocr [pdf|txt|picker] \u2014 \u062a\u062d\u062f\u064a\u062f **\u0645\u062e\u0631\u062c\u0627\u062a OCR**\n"
            "\u2022 /canceljob <id> \u2014 \u0625\u0644\u063a\u0627\u0621 \u0645\u0647\u0645\u0629 \u0645\u0646\u062a\u0638\u0631\u0629/\u062c\u0627\u0631\u064a\u0629\n"
            "\u2022 /startbatch + /endbatch \u2014 \u0645\u0639\u0627\u0644\u062c\u0629 \u0639\u062f\u0629 \u0645\u0644\u0641\u0627\u062a \u0645\u0639\u064b\u0627\n\n"
            "_\u0627\u0644\u062c\u0644\u0633\u0627\u062a \u062e\u0627\u0635\u0629 \u0628\u0643\u0644 \u0645\u0633\u062a\u062e\u062f\u0645: \u0644\u0627 \u064a\u0645\u0643\u0646 \u0644\u0623\u062d\u062f \u0627\u0633\u062a\u062e\u062f\u0627\u0645 \u062d\u0633\u0627\u0628\u0643._"
        ),
        "help_ar": (
            "\U0001f4da \u0627\u0644\u0645\u0633\u0627\u0639\u062f\u0629 \u2014 \u0643\u0644 \u0627\u0644\u0623\u0648\u0627\u0645\u0631\n\n"
            "\U0001f4c4 \u0627\u0644\u0623\u0633\u0627\u0633\u064a\u0629\n"
            "\u2022 /start \u2014 \u0627\u0644\u062a\u0631\u062d\u064a\u0628 \u0648\u0627\u0644\u0628\u062f\u0621 \u0627\u0644\u0633\u0631\u064a\u0639\n"
            "\u2022 /help \u2014 \u0647\u0630\u0647 \u0627\u0644\u0645\u0633\u0627\u0639\u062f\u0629\n"
            "\u2022 /status \u2014 \u062d\u0627\u0644\u0629 \u0627\u0644\u0628\u0648\u062a \u0648\u0627\u0644\u0637\u0627\u0628\u0648\u0631 \u0648\u0645\u0647\u0627\u0645\u0643\n\n"
            "\U0001f510 \u062c\u0644\u0633\u0627\u062a\u0643 (\u0644\u0643\u0644 \u0645\u0633\u062a\u062e\u062f\u0645)\n"
            "\u2022 /login [\u0647\u0627\u062a\u0641] \u2014 \u0631\u0628\u0637 \u062d\u0633\u0627\u0628 Telethon\n"
            "\u2022 /loginpyro [\u0647\u0627\u062a\u0641] \u2014 \u0631\u0628\u0637 \u062d\u0633\u0627\u0628 Pyrogram\n"
            "\u2022 /loginstatus \u2014 \u0641\u062d\u0635 \u0627\u0644\u062c\u0644\u0633\u0629\n"
            "\u2022 /logout \u2014 \u0642\u0637\u0639 \u062c\u0644\u0633\u0629 Telethon\n"
            "\u2022 /logoutpyro \u2014 \u0642\u0637\u0639 \u062c\u0644\u0633\u0629 Pyrogram\n"
            "\u2022 /clearflood \u2014 \u0625\u0646\u0647\u0627\u0621 \u062a\u0633\u062c\u064a\u0644 \u062f\u062e\u0648\u0644 \u0645\u0639\u0644\u0651\u0642\n"
            "\u2022 /cancel \u2014 \u0625\u0644\u063a\u0627\u0621 \u062a\u0633\u062c\u064a\u0644 \u062f\u062e\u0648\u0644 \u062c\u0627\u0631\u064d\n\n"
            "\U0001f4e6 \u0627\u0644\u0645\u0647\u0627\u0645\n"
            "\u2022 /canceljob <id> \u2014 \u0625\u0644\u063a\u0627\u0621 \u0645\u0647\u0645\u0629 \u0645\u0646\u062a\u0638\u0631\u0629/\u062c\u0627\u0631\u064a\u0629\n\n"
            "\U0001f5c2 \u0627\u0644\u062f\u0641\u0639\u0627\u062a\n"
            "\u2022 /startbatch \u2014 \u0628\u062f\u0621 \u062a\u062c\u0645\u064a\u0639 \u0627\u0644\u0645\u0644\u0641\u0627\u062a\n"
            "\u2022 /endbatch \u2014 \u0645\u0639\u0627\u0644\u062c\u0629 \u0627\u0644\u062f\u0641\u0639\u0629\n"
            "\u2022 /cancelbatch \u2014 \u062a\u062c\u0627\u0647\u0644 \u0627\u0644\u062f\u0641\u0639\u0629\n\n"
            "\u2699\ufe0f \u0623\u0648\u0627\u0645\u0631 \u0627\u0644\u0645\u0627\u0644\u0643/\u0627\u0644\u0645\u0634\u0631\u0641\n"
            "\u2022 /admin add|remove|list <user_id>\n"
            "\u2022 /sessionstatus\n"
            "\u2022 /clear_cache\n"
            "\u2022 /cancelall\n"
            "\u2022 /setwebhook <url>\n"
            "\u2022 /delwebhook\n"
            "\u2022 /setcommands\n\n"
            "\U0001f4d6 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 \u2192 \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0625\u062c\u0631\u0627\u0621\u0627\u062a\n"
            "\u2022 \u0623\u0631\u0633\u0644 PDF \u0623\u0648 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0643\u062a\u0627\u0628\u064b\u0627 \u0625\u0644\u0643\u062a\u0631\u0648\u0646\u064a\u064b\u0627 \u2192 \u062a\u0638\u0647\u0631 \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0625\u062c\u0631\u0627\u0621\u0627\u062a\n"
            "\u2022 /ocr [pdf|txt|picker] \u2014 \u062a\u062b\u0628\u064a\u062a \u0645\u062e\u0631\u062c\u0627\u062a OCR"
        ),
    },
    "ar": {
        "access_denied": "\u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0631\u0641\u0648\u0636. \u0647\u0630\u0627 \u0627\u0644\u0628\u0648\u062a \u062e\u0627\u0635.",
        "only_owner": "\u26d4 \u0647\u0630\u0627 \u0627\u0644\u0623\u0645\u0631 \u0644\u0644\u0645\u0627\u0644\u0643 \u0641\u0642\u0637.",
        "commands_updated": "\u062a\u0645 \u062a\u062d\u062f\u064a\u062b \u0627\u0644\u0623\u0648\u0627\u0645\u0631",
        "failed_commands": "\u274c \u0641\u0634\u0644 \u062a\u062d\u062f\u064a\u062b \u0627\u0644\u0623\u0648\u0627\u0645\u0631.",
        "menu_thumb": "\U0001f5bc\ufe0f \u0635\u0648\u0631\u0629 \u0645\u0635\u063a\u0651\u0631\u0629",
        "menu_ocr_thumb": "\U0001f50e\U0001f5bc\ufe0f OCR \u0648\u0635\u0648\u0631\u0629",
        "menu_compress_thumb": "\U0001f5dc\ufe0f\U0001f5bc\ufe0f \u0636\u063a\u0637 \u0648\u0635\u0648\u0631\u0629",
        "menu_convert": "\U0001f501 \u062a\u062d\u0648\u064a\u0644",
        "menu_ocr_thumb_pdf": "\U0001f50e\U0001f5bc\ufe0f OCR \u0648\u0635\u0648\u0631\u0629 PDF",
        "menu_cancel_all": "\U0001f5d1 \u0625\u0644\u063a\u0627\u0621 \u0643\u0644 \u0627\u0644\u0645\u0647\u0627\u0645",
    },
}


def detect_lang(text: str | None, *, threshold: float = 0.30) -> str:
    """``"ar"`` when *text* is Arabic (or a mix with enough Arabic), else ``"en"``."""
    if not text:
        return "en"
    arabic = len(_AR_LETTER_RE.findall(str(text)))
    latin = len(_LATIN_LETTER_RE.findall(str(text)))
    if arabic == 0:
        return "en"
    return "ar" if (arabic / max(1, arabic + latin)) >= threshold else "en"


def env_lang() -> str:
    lang = (os.getenv("BOT_LANG") or "en").strip().lower()
    return lang if lang in SUPPORTED else "en"


def resolve_lang(explicit: str | None = None, text: str | None = None) -> str:
    if explicit in SUPPORTED:
        return explicit
    if text and detect_lang(text) == "ar":
        return "ar"
    return env_lang()


def t(key: str, lang: str | None = None, text: str | None = None, **kwargs) -> str:
    """Translated label for *key* (English when the translation is missing)."""
    chosen = resolve_lang(lang, text)
    catalog = MESSAGES.get(chosen) or MESSAGES["en"]
    template = catalog.get(key) or MESSAGES["en"].get(key) or key
    try:
        return template.format(**kwargs)
    except (KeyError, IndexError, ValueError):
        return template


def command_menu(lang: str | None = None) -> list:
    """``[(command, description)]`` in the resolved UI language."""
    chosen = resolve_lang(lang)
    return [(cmd, desc.get(chosen) or desc["en"]) for cmd, desc in COMMANDS]
