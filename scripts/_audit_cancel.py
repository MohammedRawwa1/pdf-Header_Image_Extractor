"""Functional check: every menu/picker/queued-reply carries a cancel/close row."""

import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("BOT_TOKEN", "123456789:DUMMY_AUDIT")
os.environ.setdefault("REDIS_URL", "")

import bot  # noqa: E402
from bot import (  # noqa: E402
    _book_conv_kb,
    _ctx_menu_kb,
    _ocr_pick_kb,
    _ocr_settings_kb,
    _queued_cancel_kb,
)

FAILURES: list[str] = []


def labels(kb):
    return [b.callback_data for row in kb.inline_keyboard for b in row] if kb else []


def check(name: str, cond: bool, extra: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  ({extra})" if extra else ""))
    if not cond:
        FAILURES.append(name)


# Input context menus (calibre stubbed present for the book menu).
bot.calibre_available = lambda: True  # type: ignore[attr-defined]
for kind, fn in (("pdf", "x.pdf"), ("image", "x.png"), ("book", "b.epub")):
    kb = _ctx_menu_kb(111, "tok", fn, kind)
    ls = labels(kb)
    check(f"{kind} input menu -> Close row", any(c.startswith("ctxclose:") for c in ls),
          ",".join(c.split(":")[0] for c in ls))

# OCR output picker.
kb = _ocr_pick_kb(111, "tok")
ls = labels(kb)
check("OCR picker -> Cancel row", any(c.startswith("ocrcancel:") for c in ls),
      ",".join(c.split(":")[0] for c in ls))

# Convert format picker.
kb = _book_conv_kb(111, "tok", "b.epub")
ls = labels(kb)
check("Convert picker -> Cancel row", bool(kb) and any(
    c.startswith("bookcancel:") for c in ls), ",".join(c.split(":")[0] for c in ls))

# /ocr settings menu.
kb = _ocr_settings_kb(111)
ls = labels(kb)
check("/ocr settings -> Close row", any(c.endswith(":close") for c in ls),
      ",".join(c.split(":")[0] + ":" + c.split(":")[2] for c in ls))

# Queued-reply cancel button.
kb = _queued_cancel_kb(111, "abc12345xyz")
ls = labels(kb)
check("Queued reply -> Cancel job button", bool(kb) and any(
    c.startswith("canceljob:111:abc12345") for c in ls), ",".join(ls))

# Job-timeout safety for cancel-able long jobs is separate; here ensure the
# cancel handler + registrations exist in source.
src = open(os.path.join(ROOT, "bot.py"), encoding="utf-8").read()
for pat in ("handle_menu_cancel_callback", "^ctxclose:", "^ocrcancel:",
            "^bookcancel:", "^ocrset:\\d+:(pdf|txt|picker|close)$"):
    check(f"source has {pat!r}", pat in src.replace("\r\n", "\n"), pat)

print("\n" + ("FAILURES: " + str(FAILURES) if FAILURES else "ALL CANCEL-SURFACE CHECKS PASSED"))
sys.exit(1 if FAILURES else 0)
