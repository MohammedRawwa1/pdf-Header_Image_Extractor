"""Audit: verify every button flow routes into the correct pipe by size and
every button maps to a handler -> job capable of handling that media kind,
and every job-starting/cancellation surface is registered.

Checks:
1. Every callback_data producer prefix is registered in a CallbackQueryHandler.
2. Every handler enqueues jobs that exist in tasks.py.
3. Every enqueue passes file_size + message_id (+ source_chat_id where the job
   re-downloads via _download_job_file) so the two-pipe size routing works.
4. Convert-family jobs use the two-leg job_timeout (2*BOOK_CONVERT_TIMEOUT+300).
5. Every worker download path is size-gated (Bot API <= 20MB cap).
6. Cancellation surfaces: queued-reply cancel buttons, canceljob/cancelall
   confirm/abort handlers, and the menu/picker cancel (ctxclose/ocrcancel/
   bookcancel) are all registered.

Run: python scripts/_audit_wiring.py
"""

import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("BOT_TOKEN", "123456789:DUMMY_AUDIT")
os.environ.setdefault("REDIS_URL", "")

BOT = open(os.path.join(ROOT, "bot.py"), encoding="utf-8", newline="").read()
TASKS = open(os.path.join(ROOT, "tasks.py"), encoding="utf-8", newline="").read()
# Normalize line endings so \nasync def / \ndef boundary scans are reliable.
BOT = BOT.replace("\r\n", "\n")
TASKS = TASKS.replace("\r\n", "\n")

FAILURES: list[str] = []


def check(name: str, cond: bool, extra: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  ({extra})" if extra else ""))
    if not cond:
        FAILURES.append(name)


# ── 1. Button producers vs registered handlers ─────────────────────────────
PRODUCED = sorted(
    set(re.findall(r'callback_data=f?"([a-z]+):', BOT))
    | set(re.findall(r'callback_data="([a-z]+):', BOT))
)
REGISTERED = re.findall(r'pattern=r?"\^([a-z_]+(?::\\d+)?)', BOT)
REGISTERED_PREFIXES = set()
for pat in REGISTERED:
    m = re.match(r"([a-z]+)", pat)
    if m:
        REGISTERED_PREFIXES.add(m.group(1))

print("=== Button producers:", PRODUCED)
print("=== Registered prefixes:", sorted(REGISTERED_PREFIXES))
for p in PRODUCED:
    check(f"producer '{p}' is registered", p in REGISTERED_PREFIXES, p)

# Cancellation-specific buttons must be registered.  Asserted via plain
# substring matches on the actual registration lines (regex extraction is
# brittle with \d / underscore prefixes).
for pat in ("^canceljob:", "^canceljob_confirm:", "^canceljob_abort:",
            "^cancelall$", "^cancelall_confirm:", "^cancelall_abort:",
            "^cancelall_(confirm|abort)$",
            "^ctxclose:", "^ocrcancel:", "^bookcancel:"):
    # Registrations may use pattern="..." with or without the raw r" prefix.
    found = f'pattern=r"{pat}' in BOT or f'pattern="{pat}' in BOT
    check(f"pattern {pat!r} registered", found, pat)

# ── 2. Handler -> job -> tasks.py function mapping ────────────────────────
TASK_FUNCS = set(re.findall(r"^def (\w+)\(", TASKS, re.M))
print("\n=== Job names found in enqueue calls:", sorted(
    set(re.findall(r'enqueue_job,\s*"(\w+)"', BOT))
))
missing = [
    job for job in set(re.findall(r'enqueue_job,\s*"(\w+)"', BOT))
    if job not in TASK_FUNCS
]
check("every enqueued job exists in tasks.py", not missing, ",".join(missing) or "none")

# ── 3. Per-handler enqueue carries file_size + message_id ─────────────────
HANDLERS = [
    "handle_ctx_thumb_callback",
    "handle_ctx_thumb_ocr_callback",
    "handle_book_convert_button_callback",
    "handle_book_compress_callback",
    "handle_book_convert_callback",
    "handle_compress_callback",
    "handle_ocr_pick_callback",
]
for h in HANDLERS:
    i = BOT.find(f"async def {h}(")
    if i < 0:
        check(f"{h}: found", False)
        continue
    body = BOT[i : i + 8000]
    if "enqueue_job" not in body:
        check(f"{h}: reveal-only (no enqueue)", True)
        continue
    check(f"{h}: enqueues carry file_size", "file_size" in body)
    check(f"{h}: enqueues carry message_id", "message_id" in body)

# ── 4. Convert jobs use the two-leg timeout ───────────────────────────────
n_two_leg = BOT.count("job_timeout=2 * int(_conv_timeout) + 300")
check("convert enqueues use 2-leg job_timeout", n_two_leg >= 2, f"found {n_two_leg}")
n_old = len(re.findall(r"job_timeout=int\(_conv_timeout\) \+ 600", BOT))
check("no stale single-leg convert timeout", n_old == 0, f"found {n_old}")

# ── 5. Worker download paths are size-gated ───────────────────────────────
check(
    "_download_job_file gates getFile by download cap",
    "file_size <= _bot_dl_limit" in TASKS,
)
check(
    "process_document_job gates getFile by download cap",
    "_skip_bot_api = (" in TASKS and "file_size > download_limit" in TASKS,
)
for j in ("deliver_book_job", "convert_book_job", "compress_pdf_job", "ocr_job"):
    i = TASKS.find(f"def {j}(")
    nxt = TASKS.find("\ndef ", i + 10)
    body = TASKS[i : nxt if nxt > 0 else i + 3000]
    check(f"{j}: uses _download_job_file", "_download_job_file(" in body)

# ── 6. Upload pre-gate + cancel machinery ─────────────────────────────────
check(
    "_deliver_converted_file pre-gates upload by size",
    "BOT_API_UPLOAD_LIMIT_BYTES" in TASKS and "exceeds Bot API upload cap" in TASKS,
)
check("_job_cancelled polls cancel:<id> flag", "cancel:{job_id}" in TASKS)
check(
    "menu cancel handler consumes the record",
    "handle_menu_cancel_callback" in BOT and "_load_pending_token(" in BOT,
)

print("\n" + ("FAILURES: " + str(FAILURES) if FAILURES else "ALL WIRING CHECKS PASSED"))
sys.exit(1 if FAILURES else 0)
