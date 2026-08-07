"""
Background-task-based Telethon + Pyrogram login flows (per-user).

Instead of a ConversationHandler (which has a polling gap between
``send_code_request`` and ``sign_in`` that causes codes to expire),
this module uses a **background task** + **asyncio.Future** pattern:

1. ``/login`` → stores ``asyncio.Future`` s in ``bot_data["login_futures"]``,
   starts a background task that owns the Telethon ``TelegramClient``.
2. The background task calls ``send_code_request``, then **awaits**
   an ``asyncio.Future`` for the code — the future is resolved by
   ``_handle_login_text`` when the user's message arrives.
3. ``sign_in`` is called **in the same coroutine** immediately after
   the future resolves → **no polling gap**.
4. If 2FA is needed the task awaits a second future for the password.

Each user's login flow is tracked separately (keyed by ``user_id``), so
multiple users can log in with their own sessions at the same time.

Usage in bot.py::

    from utils.login_handler import (
        cleanup_login_flow,
        register_login_handlers,
    )

    register_login_handlers(application)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, MessageHandler, filters

from utils.markdown_utils import escape_markdown as _escape_markdown

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────


def _normalize_code(text: str) -> str:
    """Normalize Unicode digits to ASCII and strip non-digit chars."""
    trans = str.maketrans(
        {
            "\u0660": "0",
            "\u0661": "1",
            "\u0662": "2",
            "\u0663": "3",
            "\u0664": "4",
            "\u0665": "5",
            "\u0666": "6",
            "\u0667": "7",
            "\u0668": "8",
            "\u0669": "9",
            "\u06f0": "0",
            "\u06f1": "1",
            "\u06f2": "2",
            "\u06f3": "3",
            "\u06f4": "4",
            "\u06f5": "5",
            "\u06f6": "6",
            "\u06f7": "7",
            "\u06f8": "8",
            "\u06f9": "9",
        }
    )
    return "".join(c for c in text.translate(trans) if c.isdigit())


def _pyrogram_warning_text() -> str:
    """Return a warning about Pyrogram session if one is configured."""
    if os.getenv("PYROGRAM_SESSION"):
        return (
            "\n\n⚠️ A Pyrogram session is configured. If it uses the same"
            " phone number, codes may be consumed by that session."
        )
    return ""


def _parse_proxy_config():
    """Parse ``TELETHON_PROXY`` env var into a Telethon-compatible proxy tuple.

    Format: ``socks5://host:port`` or ``socks5://user:pass@host:port``
    Returns ``None`` if not set or invalid (unauthenticated fallback).
    """
    raw = os.getenv("TELETHON_PROXY", "").strip()
    if not raw:
        return None
    try:
        from urllib.parse import urlparse

        parsed = urlparse(raw)
        if parsed.scheme not in ("socks5", "socks4"):
            logger.warning(
                "proxy: only socks5/socks4 supported by Telethon, got '%s'",
                parsed.scheme,
            )
            return None
        host = parsed.hostname
        port = parsed.port or 1080
        if parsed.username and parsed.password:
            return (
                parsed.scheme,
                host,
                port,
                True,
                parsed.username,
                parsed.password,
            )
        return (parsed.scheme, host, port)
    except Exception as exc:
        logger.warning("proxy: failed to parse TELETHON_PROXY='%s': %s", raw, exc)
        return None


def _get_api_credentials() -> tuple:
    """Return ``(api_id, api_hash, "")`` or ``("error message",)``."""
    api_id_str = (
        os.getenv("API_ID")
        or os.getenv("USERBOT_API_ID")
        or os.getenv("api_id")  # noqa: SIM112
        or os.getenv("userbot_api_id")  # noqa: SIM112
    )
    api_hash = (
        os.getenv("API_HASH")
        or os.getenv("USERBOT_API_HASH")
        or os.getenv("api_hash")  # noqa: SIM112
        or os.getenv("userbot_api_hash")  # noqa: SIM112
    )
    if not api_id_str or not api_hash:
        return ("Missing Telethon credentials. Set API_ID and API_HASH.",)
    try:
        api_id = int(api_id_str)
    except (TypeError, ValueError):
        return ("Configured API_ID is invalid. It must be an integer.",)
    return (api_id, api_hash, "")


async def _persist_session_to_mongo(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, data: dict, phone: str | None = None
) -> bool:
    """Persist a session dict to MongoDB (best-effort, multi-backend).

    Tries ``bot_data["db_model"]`` (MediaConversionModel-style object with
    ``save_session``) first, then falls back to ``utils.db.save_user_session``
    used by the root bot.
    """
    try:
        db_model = context.application.bot_data.get("db_model")
        if db_model is not None and hasattr(db_model, "save_session"):
            await db_model.save_session(user_id, data, phone=phone)
            return True
        from utils.db import save_user_session

        data["user_id"] = user_id
        return await save_user_session(user_id, data)
    except Exception as exc:
        logger.warning("login: MongoDB save failed: %s", exc)
        return False


# ── Public helpers ──────────────────────────────────────────────────────


async def cleanup_login_flow(
    context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None
):
    """Cancel any active login flow for *user_id* (or the calling user).

    Handles both Telethon (``client``) and Pyrogram (``pyro_client``) clients.
    """
    if user_id is None:
        return
    futures_map: dict = context.application.bot_data.get("login_futures", {})
    entry = futures_map.pop(user_id, None)
    if entry is None:
        return
    # Signal cancellation to the background task
    entry["cancel"].set()
    if entry.get("task") is not None:
        entry["task"].cancel()
    # Disconnect client (Telethon or Pyrogram)
    client = entry.get("client")
    if client is not None:
        with contextlib.suppress(Exception):
            await client.disconnect()
    pyro_client = entry.get("pyro_client")
    if pyro_client is not None:
        with contextlib.suppress(Exception):
            await pyro_client.stop()


def register_login_handlers(application):
    """Register the ``/login``, ``/loginpyro`` commands and the login-text message handler.

    Call this from ``setup_handlers()`` to wire up the login flows.
    """
    application.add_handler(CommandHandler("login", _login_command))
    application.add_handler(CommandHandler("loginpyro", _pyro_login_command))
    # This handler catches ALL text messages that might be login input.
    # It runs *before* other text handlers and only consumes the message
    # if there is a pending login Future for that user.
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_login_text),
        group=0,
    )


# ── Login state helpers ─────────────────────────────────────────────────


def _get_futures(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, create: bool = False
) -> dict | None:
    """Get the login futures dict for *user_id*, optionally creating it."""
    if "login_futures" not in context.application.bot_data:
        if not create:
            return None
        context.application.bot_data["login_futures"] = {}
    fm = context.application.bot_data["login_futures"]
    if user_id not in fm:
        if not create:
            return None
        fm[user_id] = {
            "phone": None,  # asyncio.Future or None
            "code": None,  # asyncio.Future or None
            "password": None,  # nosec - asyncio.Future or placeholder
            "cancel": asyncio.Event(),
            "task": None,  # asyncio.Task or None
            "client": None,  # TelegramClient or None
            "chat_id": None,  # set by caller after creation
            "phone_number": None,  # str or None
            "phone_code_hash": None,
            "api_id": None,
            "api_hash": None,
        }
    return fm[user_id]


# ══════════════════════════════════════════════════════════════════════════
# /login command
# ══════════════════════════════════════════════════════════════════════════


async def _login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """``/login [phone]`` — start the Telethon login flow (per-user)."""
    # ACL: only allowed users may start a login flow (prevents abuse of a
    # private bot by strangers consuming API codes / rate limits).
    from config import is_user_allowed  # noqa: PLC0415

    if not is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.message.reply_text("Access denied. This bot is private.")
        return
    creds = _get_api_credentials()
    if isinstance(creds, tuple) and len(creds) == 1:
        await update.message.reply_text(str(creds[0]))
        return

    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    api_id, api_hash, _ = creds

    # Check for existing active flow
    existing = _get_futures(context, uid)
    if existing and existing.get("task") is not None and not existing["task"].done():
        await update.message.reply_text(
            "⏳ You already have an active login flow. Use /cancel to abort it first."
        )
        return

    # Check for already-authorised session
    try:
        from telethon import TelegramClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        saved_session = await _load_any_session(context, uid)
        if saved_session:
            test_client = TelegramClient(
                StringSession(saved_session),
                api_id,
                api_hash,
                proxy=_parse_proxy_config(),
            )
            try:
                await test_client.connect()
                if await test_client.is_user_authorized():
                    me = await test_client.get_me()
                    await update.message.reply_text(
                        f"✅ Telethon session is already authorised as {me.first_name or me.phone}.\n"
                        "No need to log in again."
                    )
                    await test_client.disconnect()
                    return
                await test_client.disconnect()
            except Exception:
                await test_client.disconnect()
    except Exception:  # nosec B110
        pass

    # Parse optional phone argument
    args = context.args if hasattr(context, "args") else []
    phone = " ".join(args).strip() if args else ""

    if phone:
        # Phone provided as argument — start immediately
        entry = _get_futures(context, uid, create=True)
        entry["client_type"] = "telethon"
        entry["api_id"] = api_id
        entry["api_hash"] = api_hash
        entry["phone_number"] = phone
        entry["chat_id"] = chat_id

        warn = _pyrogram_warning_text()

        msg = await update.message.reply_text(
            f"📱 Phone: `{phone}`\n⏳ Connecting to Telegram and requesting verification code..."
            + warn,
            parse_mode="Markdown",
        )

        entry["task"] = asyncio.create_task(
            _run_login_task(uid, phone, api_id, api_hash, entry, context, msg)
        )
    else:
        # No phone — ask for it, set up phone Future
        entry = _get_futures(context, uid, create=True)
        entry["client_type"] = "telethon"
        entry["api_id"] = api_id
        entry["api_hash"] = api_hash
        entry["chat_id"] = chat_id
        entry["phone"] = asyncio.get_running_loop().create_future()

        warn = _pyrogram_warning_text()

        await update.message.reply_text(
            "📱 Please send the phone number in international format,\n"
            "e.g. `+1234567890`\n\n"
            "⏳ Your login flow will expire after **5 minutes** of inactivity.\n"
            "Type /cancel to abort." + warn,
            parse_mode="Markdown",
        )


async def _load_any_session(
    context: ContextTypes.DEFAULT_TYPE, user_id: int
) -> str | None:
    """Try to load a saved Telethon session from JSON or MongoDB."""
    try:
        from utils.telethon_session import _load_session_string_from_file_async

        # Try per-user JSON first, then legacy global JSON
        saved = await _load_session_string_from_file_async(
            client_type="telethon", user_id=user_id
        )
        if saved:
            return saved
        saved = await _load_session_string_from_file_async(client_type="telethon")
        if saved:
            return saved
    except Exception:  # nosec B110
        pass
    try:
        db_model = context.application.bot_data.get("db_model")
        if db_model is not None and hasattr(db_model, "load_session"):
            sess = await db_model.load_session(user_id)
            if isinstance(sess, dict):
                return sess.get("telethon_session") or sess.get("string_session")
        else:
            from utils.db import get_user_session

            sess = await get_user_session(user_id)
            if isinstance(sess, dict):
                return (
                    sess.get("telethon_session") or sess.get("string_session") or None
                )
    except Exception:  # nosec B110
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════
# Text message handler (resolves Futures)
# ══════════════════════════════════════════════════════════════════════════


async def _handle_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Intercept text messages that may be login input (phone / code / password).

    This handler is registered with ``group=0`` so it fires before other
    text handlers.  It only *consumes* the message if a matching Future
    is pending; otherwise it returns without doing anything, allowing
    downstream handlers to process the message normally.
    """
    uid = update.effective_user.id
    entry = _get_futures(context, uid)
    if entry is None:
        return  # no active login flow; let other handlers process this message

    text = update.message.text.strip()

    # ── Phone ────────────────────────────────────────────────────
    phone_fut = entry.get("phone")
    if phone_fut is not None and not phone_fut.done():
        # Validate phone
        if not text.startswith("+"):
            await update.message.reply_text(
                "❌ Phone number must start with `+` and country code.\n"
                "e.g. `+1234567890`\n\nPlease try again or type /cancel.",
                parse_mode="Markdown",
            )
            return False  # consumed — stop group-0 processing so other text handlers don't also fire
        entry["phone_number"] = text
        phone_fut.set_result(text)
        # Start the background task NOW — it was waiting for the phone
        warn = _pyrogram_warning_text()
        client_type = entry.get("client_type", "telethon")
        if client_type == "pyrogram":
            msg = await update.message.reply_text(
                f"📱 Phone: `{text}`\n"
                "⏳ Connecting to Telegram via Pyrogram and requesting verification code..."
                + warn,
                parse_mode="Markdown",
            )
            entry["task"] = asyncio.create_task(
                _run_pyro_login_task(
                    uid,
                    text,
                    entry["api_id"],
                    entry["api_hash"],
                    entry,
                    context,
                    msg,
                )
            )
        else:
            msg = await update.message.reply_text(
                f"📱 Phone: `{text}`\n⏳ Connecting to Telegram and requesting verification code..."
                + warn,
                parse_mode="Markdown",
            )
            entry["task"] = asyncio.create_task(
                _run_login_task(
                    uid,
                    text,
                    entry["api_id"],
                    entry["api_hash"],
                    entry,
                    context,
                    msg,
                )
            )
        return False  # consumed

    # ── Code ─────────────────────────────────────────────────────
    code_fut = entry.get("code")
    if code_fut is not None and not code_fut.done():
        code = _normalize_code(text)
        if not code:
            await update.message.reply_text(
                "❌ No digits found. Please enter the code you received (digits only):"
            )
            return False  # consumed
        code_fut.set_result(code)
        return False  # consumed

    # ── Password ─────────────────────────────────────────────────
    pwd_fut = entry.get("password")
    if pwd_fut is not None and not pwd_fut.done():
        if not text:
            await update.message.reply_text("❌ Password cannot be empty. Please try again:")
            return False  # consumed
        pwd_fut.set_result(text)
        return False  # consumed

    # No matching future — let other handlers process this message
    return


# ══════════════════════════════════════════════════════════════════════════
# Background login task
# ══════════════════════════════════════════════════════════════════════════

TIMEOUT_SECONDS = 300  # 5 minutes per step


async def _run_login_task(
    user_id: int,
    phone: str,
    api_id: int,
    api_hash: str,
    entry: dict,
    context: ContextTypes.DEFAULT_TYPE,
    status_msg=None,
):
    """Background coroutine that owns the Telethon client and runs the login.

    ``send_code_request`` and ``sign_in`` run in this single coroutine,
    so there is **no polling gap** between them.  The user's code arrives
    via an ``asyncio.Future`` set by ``_handle_login_text``.
    """
    t0 = time.monotonic()
    bot = context.bot
    chat_id = entry["chat_id"]

    def elapsed() -> str:
        return f"+{time.monotonic() - t0:.1f}s"

    try:
        from telethon import TelegramClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        # Build proxy-aware client
        proxy_config = _parse_proxy_config()
        logger.debug(
            "login [%s]: building Telethon client, proxy=%s",
            elapsed(),
            proxy_config is not None,
        )
        client = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy_config)
        entry["client"] = client
        await client.connect()
        dc_id = client.session.dc_id
        logger.info("login [%s]: connected to DC%s", elapsed(), dc_id)

        # ── Send code request ────────────────────────────────────
        logger.debug("login [%s]: calling send_code_request...", elapsed())
        sent = await client.send_code_request(phone)
        phone_code_hash = str(sent.phone_code_hash)
        entry["phone_code_hash"] = phone_code_hash
        code_type = type(sent).__name__
        sent_timeout = getattr(sent, "timeout", None)
        logger.info(
            "login [%s]: code sent to %s (hash=%s..., type=%s, timeout=%s)",
            elapsed(),
            phone,
            phone_code_hash[:8],
            code_type,
            sent_timeout,
        )

        # NOTE: We deliberately do NOT call GetPasswordRequest here.
        # Even though the docs say it's read-only, calling it before
        # sign_in with a fresh anonymous StringSession has been observed
        # to cause side-effects that invalidate the pending auth code.
        # Instead we let the SessionPasswordNeededError catch below
        # handle 2FA naturally — which is Telethon's intended flow.

        # Notify user (Telethon-specific instructions)
        msg_text = (
            "✅ Verification code sent to your Telegram app!\n\n"
            "**Telethon login** — enter the code below.\n"
            '• Type the digits **with spaces between them** — e.g. `"1 2 3 4 5"`\n'
            "• This spacing is required for the code-hash verification to pass\n"
            "• If 2FA is enabled, you'll be prompted for your password next"
        )
        if status_msg:
            await status_msg.edit_text(msg_text, parse_mode="Markdown")
        else:
            status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

        # ── Wait for code ────────────────────────────────────────
        entry["code"] = asyncio.get_running_loop().create_future()

        try:
            code = await asyncio.wait_for(entry["code"], timeout=TIMEOUT_SECONDS)
        except TimeoutError:
            await _login_fail(
                bot, chat_id, status_msg, "⏰ Login timed out (no code received within 5 minutes)."
            )
            return

        code_received_at = time.monotonic()
        code_delay = code_received_at - t0
        logger.info(
            "login [%s]: code received from user (total elapsed=%.1fs)",
            elapsed(),
            code_delay,
        )

        if entry["cancel"].is_set():
            logger.debug("login [%s]: cancelled after code received", elapsed())
            return

        # ── Sign in with code (NO polling gap!) ──────────────────
        retry_count = 0
        has_2fa = False
        while retry_count < 3:
            try:
                sign_in_t0 = time.monotonic()
                logger.debug(
                    "login [%s]: calling sign_in(phone=%s, hash=%s...)",
                    elapsed(),
                    phone,
                    entry["phone_code_hash"][:8],
                )
                # Pass phone_code_hash explicitly to bypass any internal cache issues
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=entry["phone_code_hash"],
                )
                sign_in_duration = time.monotonic() - sign_in_t0
                logger.info(
                    "login [%s]: sign_in SUCCEEDED in %.2fs", elapsed(), sign_in_duration
                )
                break  # success
            except Exception as exc:
                exc_name = type(exc).__name__
                exc_x = getattr(exc, "x", None)
                exc_code = getattr(exc, "code", None)
                exc_type_attr = (
                    getattr(exc, "type", None)
                    or getattr(exc, "type_name", None)
                    or getattr(exc, "error_code", None)
                )
                logger.warning(
                    "login [%s]: sign_in FAILED — name=%s, code=%s, type=%s, raw=%s, exc=%s",
                    elapsed(),
                    exc_name,
                    exc_code,
                    exc_type_attr,
                    exc_x,
                    exc,
                )

                if "SessionPasswordNeededError" in exc_name:
                    logger.info("login [%s]: 2FA password required", elapsed())
                    has_2fa = True
                    break

                if "PhoneCodeExpiredError" in exc_name:
                    retry_count += 1
                    logger.warning(
                        "login [%s]: code expired for %s (attempt %d/3)",
                        elapsed(),
                        phone,
                        retry_count,
                    )
                    if retry_count >= 3:
                        await _login_fail(
                            bot,
                            chat_id,
                            status_msg,
                            "❌ The verification codes keep expiring before they can be used.\n"
                            "This usually happens when two bots share `API_ID/API_HASH` with 2FA enabled.\n"
                            "Try using /logout first, then /login again.",
                        )
                        return
                    # Wait before resending — gives Telegram's backend time to
                    # cool down and gives the user time to see the new code.
                    logger.debug("login [%s]: sleeping 15s before resend...", elapsed())
                    await asyncio.sleep(15)
                    logger.debug("login [%s]: sending new code request...", elapsed())
                    sent = await client.send_code_request(phone)
                    phone_code_hash = str(sent.phone_code_hash)
                    entry["phone_code_hash"] = phone_code_hash
                    logger.info(
                        "login [%s]: resent code (attempt %d/3, new hash=%s...)",
                        elapsed(),
                        retry_count,
                        phone_code_hash[:8],
                    )
                    # Set up new code future
                    entry["code"] = asyncio.get_running_loop().create_future()
                    try:
                        if status_msg:
                            await status_msg.edit_text(
                                f"⏰ The previous code expired. A new one has been sent!\n\n"
                                f"*Resend #{retry_count}*\n"
                                "Please enter the new code:",
                                parse_mode="Markdown",
                            )
                    except Exception:  # nosec B110
                        pass
                    try:
                        code = await asyncio.wait_for(
                            entry["code"], timeout=TIMEOUT_SECONDS
                        )
                    except TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    continue  # retry sign_in with new code

                if "PhoneCodeInvalidError" in exc_name:
                    # Set up new code future
                    entry["code"] = asyncio.get_running_loop().create_future()
                    try:
                        if status_msg:
                            await status_msg.edit_text(
                                "❌ Invalid code. Please try again:", parse_mode="Markdown"
                            )
                    except Exception:
                        await bot.send_message(chat_id, "❌ Invalid code. Please try again:")
                    try:
                        code = await asyncio.wait_for(
                            entry["code"], timeout=TIMEOUT_SECONDS
                        )
                    except TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    continue

                if "FloodWaitError" in exc_name:
                    wait = getattr(exc, "seconds", None) or getattr(exc, "timeout", None) or 60
                    await _login_fail(
                        bot,
                        chat_id,
                        status_msg,
                        f"⏳ Too many attempts. Please wait {int(wait)} seconds before trying /login again.",
                    )
                    return

                # Unhandled error
                await _login_fail(
                    bot, chat_id, status_msg, f"❌ Login failed: {_escape_markdown(exc)}"
                )
                logger.exception("login: sign_in failed: %s", exc)
                return

        if has_2fa:
            # Fetch 2FA password info NOW (after sign_in with code has confirmed 2FA is needed)
            try:
                from telethon import functions  # noqa: PLC0415

                pwd_info = await client(functions.account.GetPasswordRequest())
                pwd_hint = str(getattr(pwd_info, "hint", "") or "")
            except Exception:
                pwd_hint = ""  # nosec - fallback when hint is unavailable

            msg_text = "🔐 Two-step verification is enabled. Please enter your account password:"
            if pwd_hint:
                msg_text += f"\nPassword hint: `{pwd_hint.replace('`', '')}`"
            try:
                await status_msg.edit_text(msg_text, parse_mode="Markdown")
            except Exception:
                status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

            entry["password"] = asyncio.get_running_loop().create_future()
            try:
                password = await asyncio.wait_for(
                    entry["password"], timeout=TIMEOUT_SECONDS
                )
            except TimeoutError:
                await _login_fail(
                    bot,
                    chat_id,
                    status_msg,
                    "⏰ Login timed out (no password received within 5 minutes).",
                )
                return

            if entry["cancel"].is_set():
                return

            try:
                await client.sign_in(password=password)
            except Exception as exc:
                if "PasswordHashInvalidError" in type(exc).__name__:
                    # Allow retry
                    entry["password"] = asyncio.get_running_loop().create_future()
                    try:
                        await status_msg.edit_text(
                            "❌ Incorrect password. Please try again:", parse_mode="Markdown"
                        )
                    except Exception:
                        await bot.send_message(chat_id, "❌ Incorrect password. Please try again:")
                    try:
                        password = await asyncio.wait_for(
                            entry["password"], timeout=TIMEOUT_SECONDS
                        )
                    except TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    # Retry with new password
                    await client.sign_in(password=password)
                else:
                    raise

        # ✅ Login succeeded — save session
        session_str = client.session.save()
        saved_to = []
        if await _persist_session_to_mongo(
            context,
            user_id,
            {
                "telethon_session": session_str,
                "string_session": session_str,
            },
            phone=phone,
        ):
            saved_to.append("MongoDB")
        try:
            from utils.telethon_session import (
                save_session_string_to_file_async,  # noqa: PLC0415
            )

            if await save_session_string_to_file_async(
                session_str, client_type="telethon", user_id=user_id
            ):
                saved_to.append("JSON file")
        except Exception as exc:
            logger.warning("login: JSON save failed: %s", exc)

        me = await client.get_me()
        uname = getattr(me, "first_name", "") or getattr(me, "username", "user")
        summary = ", ".join(saved_to) if saved_to else "memory"
        try:
            await status_msg.edit_text(
                f"✅ **Telethon login successful!**\n"
                f"User: {_escape_markdown(uname)}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {_escape_markdown(summary)}",
                parse_mode="Markdown",
            )
        except Exception:
            await bot.send_message(
                chat_id,
                f"✅ **Telethon login successful!**\n"
                f"User: {_escape_markdown(uname)}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {_escape_markdown(summary)}",
                parse_mode="Markdown",
            )
        logger.info("Login successful for %s (DC=%s)", phone, client.session.dc_id)

    except asyncio.CancelledError:
        logger.info("login: task cancelled for %s", phone)
    except Exception as exc:
        logger.exception("login: unexpected error: %s", exc)
        with contextlib.suppress(Exception):
            await bot.send_message(chat_id, f"❌ Login failed unexpectedly: {exc}")
    finally:
        # Clean up
        client = entry.get("client")
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()
        # Remove from futures map
        futures_map = context.application.bot_data.get("login_futures", {})
        if user_id in futures_map:
            del futures_map[user_id]


async def _login_fail(bot, chat_id, status_msg, text: str):
    """Send failure message and clean up status_msg if possible."""
    try:
        if status_msg:
            await status_msg.edit_text(text, parse_mode="Markdown")
        else:
            await bot.send_message(chat_id, text, parse_mode="Markdown")
    except Exception:
        with contextlib.suppress(Exception):
            await bot.send_message(chat_id, text, parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════════════
# /loginpyro command (Pyrogram-based login)
# ══════════════════════════════════════════════════════════════════════════


async def _pyro_login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """``/loginpyro [phone]`` — start the Pyrogram login flow (per-user).

    Uses Pyrogram's ``send_code()`` / ``sign_in()`` / ``check_password()``
    which handles 2FA reliably from cloud servers (unlike Telethon which
    has PhoneCodeExpiredError issues on Railway for 2FA accounts).
    """
    # ACL: only allowed users may start a login flow (prevents abuse of a
    # private bot by strangers consuming API codes / rate limits).
    from config import is_user_allowed  # noqa: PLC0415

    if not is_user_allowed(getattr(update.effective_user, "id", None)):
        await update.message.reply_text("Access denied. This bot is private.")
        return
    creds = _get_api_credentials()
    if isinstance(creds, tuple) and len(creds) == 1:
        await update.message.reply_text(str(creds[0]))
        return

    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    api_id, api_hash, _ = creds

    # Check for existing active flow
    existing = _get_futures(context, uid)
    if existing and existing.get("task") is not None and not existing["task"].done():
        client_type = existing.get("client_type", "unknown")
        await update.message.reply_text(
            f"⏳ You already have an active {client_type} login flow. Use /cancel to abort it first."
        )
        return

    # Parse optional phone argument
    args = context.args if hasattr(context, "args") else []
    phone = " ".join(args).strip() if args else ""

    entry = _get_futures(context, uid, create=True)
    entry["client_type"] = "pyrogram"
    entry["api_id"] = api_id
    entry["api_hash"] = api_hash
    entry["chat_id"] = chat_id

    if phone:
        entry["phone_number"] = phone
        warn = _pyrogram_warning_text()
        msg = await update.message.reply_text(
            f"📱 Phone: `{phone}`\n⏳ Connecting to Telegram via Pyrogram and requesting verification code..."
            + warn,
            parse_mode="Markdown",
        )
        entry["task"] = asyncio.create_task(
            _run_pyro_login_task(uid, phone, api_id, api_hash, entry, context, msg)
        )
    else:
        entry["phone"] = asyncio.get_running_loop().create_future()
        warn = _pyrogram_warning_text()
        await update.message.reply_text(
            "📱 Please send the phone number in international format,\n"
            "e.g. `+1234567890`\n\n"
            "This will use **Pyrogram** login (handles 2FA reliably).\n"
            "⏳ Your login flow will expire after **5 minutes** of inactivity.\n"
            "Type /cancel to abort." + warn,
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════════════════════
# Pyrogram background login task
# ══════════════════════════════════════════════════════════════════════════


async def _run_pyro_login_task(
    user_id: int,
    phone: str,
    api_id: int,
    api_hash: str,
    entry: dict,
    context: ContextTypes.DEFAULT_TYPE,
    status_msg=None,
):
    """Background coroutine that owns the Pyrogram client and runs the login.

    Uses Pyrogram's ``send_code()`` / ``sign_in()`` / ``check_password()``
    which properly handles 2FA from cloud servers (the same flow used by
    restricted content bots).

    Key differences from the Telethon flow:
    - Pyrogram's ``send_code()`` handles phone migration in a retry loop
      with a fresh auth key for the new DC.
    - ``check_password()`` is called instead of ``sign_in(password=...)``.
    - ``export_session_string()`` gives us the serialized session.
    """
    t0 = time.monotonic()
    bot = context.bot
    chat_id = entry["chat_id"]

    def elapsed() -> str:
        return f"+{time.monotonic() - t0:.1f}s"

    try:
        from pyrogram import Client as PyrogramClient  # noqa: PLC0415
        from pyrogram import errors as pyro_errors

        logger.debug("login [%s]: building Pyrogram client (in-memory)...", elapsed())

        # Create an in-memory Pyrogram client (no file storage needed)
        client = PyrogramClient(
            "pyro_login",
            api_id=api_id,
            api_hash=api_hash,
            in_memory=True,
        )
        entry["pyro_client"] = client
        await client.connect()
        logger.info("login [%s]: Pyrogram connected to DC%s", elapsed(), client.session.dc_id)

        # ── Send code request ────────────────────────────────────
        logger.debug("login [%s]: calling send_code...", elapsed())
        sent_code = await client.send_code(phone)
        phone_code_hash = sent_code.phone_code_hash
        entry["phone_code_hash"] = phone_code_hash
        logger.info(
            "login [%s]: code sent to %s (hash=%s..., type=%s, timeout=%s)",
            elapsed(),
            phone,
            phone_code_hash[:8],
            getattr(sent_code, "type", None),
            getattr(sent_code, "timeout", None),
        )

        # Notify user (Pyrogram-specific instructions)
        msg_text = (
            "✅ Verification code sent to your Telegram app!\n\n"
            "**Pyrogram login** — enter the code below.\n"
            '• Type the digits together — e.g. `"12345"` (spaces are also fine)\n'
            "• If 2FA is enabled, you'll be prompted for your password next"
        )
        if status_msg:
            await status_msg.edit_text(msg_text, parse_mode="Markdown")
        else:
            status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

        # ── Wait for code ────────────────────────────────────────
        entry["code"] = asyncio.get_running_loop().create_future()
        try:
            code = await asyncio.wait_for(entry["code"], timeout=TIMEOUT_SECONDS)
        except TimeoutError:
            await _login_fail(
                bot, chat_id, status_msg, "⏰ Login timed out (no code received within 5 minutes)."
            )
            return

        code_received_at = time.monotonic()
        code_delay = code_received_at - t0
        logger.info(
            "login [%s]: code received from user (total elapsed=%.1fs)",
            elapsed(),
            code_delay,
        )

        if entry["cancel"].is_set():
            logger.debug("login [%s]: cancelled after code received", elapsed())
            return

        # ── Sign in with code ────────────────────────────────────
        retry_count = 0
        has_2fa = False
        while retry_count < 3:
            try:
                sign_in_t0 = time.monotonic()
                logger.debug(
                    "login [%s]: calling sign_in(phone=%s, hash=%s...)",
                    elapsed(),
                    phone,
                    phone_code_hash[:8],
                )
                await client.sign_in(phone, phone_code_hash, code)
                sign_in_duration = time.monotonic() - sign_in_t0
                logger.info(
                    "login [%s]: sign_in SUCCEEDED in %.2fs", elapsed(), sign_in_duration
                )
                break  # success
            except pyro_errors.SessionPasswordNeeded:
                logger.info("login [%s]: 2FA password required", elapsed())
                has_2fa = True
                break
            except pyro_errors.PhoneCodeExpired:
                retry_count += 1
                logger.warning(
                    "login [%s]: code expired for %s (attempt %d/3)",
                    elapsed(),
                    phone,
                    retry_count,
                )
                if retry_count >= 3:
                    await _login_fail(
                        bot,
                        chat_id,
                        status_msg,
                        "❌ The verification codes keep expiring. Try /login instead or check your network.",
                    )
                    return
                # Wait and resend
                await asyncio.sleep(15)
                sent_code = await client.send_code(phone)
                phone_code_hash = sent_code.phone_code_hash
                entry["phone_code_hash"] = phone_code_hash
                logger.info(
                    "login [%s]: resent code via Pyrogram (attempt %d/3, new hash=%s...)",
                    elapsed(),
                    retry_count,
                    phone_code_hash[:8],
                )
                entry["code"] = asyncio.get_running_loop().create_future()
                try:
                    if status_msg:
                        await status_msg.edit_text(
                            f"⏰ The previous code expired. A new one has been sent!\n\n"
                            f"*Resend #{retry_count}*\n"
                            "Please enter the new code:",
                            parse_mode="Markdown",
                        )
                except Exception:  # nosec B110
                    pass
                try:
                    code = await asyncio.wait_for(
                        entry["code"], timeout=TIMEOUT_SECONDS
                    )
                except TimeoutError:
                    await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                    return
                continue
            except pyro_errors.PhoneCodeInvalid:
                entry["code"] = asyncio.get_running_loop().create_future()
                try:
                    if status_msg:
                        await status_msg.edit_text(
                            "❌ Invalid code. Please try again:", parse_mode="Markdown"
                        )
                except Exception:
                    await bot.send_message(chat_id, "❌ Invalid code. Please try again:")
                try:
                    code = await asyncio.wait_for(
                        entry["code"], timeout=TIMEOUT_SECONDS
                    )
                except TimeoutError:
                    await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                    return
                continue
            except pyro_errors.FloodWait as e:
                wait = getattr(e, "x", None) or getattr(e, "value", None) or 60
                await _login_fail(
                    bot,
                    chat_id,
                    status_msg,
                    f"⏳ Too many attempts. Please wait {int(wait)} seconds before trying /loginpyro again.",
                )
                return
            except Exception as exc:
                exc_name = type(exc).__name__
                exc_x = getattr(exc, "x", None)
                logger.warning(
                    "login [%s]: sign_in FAILED — name=%s, raw=%s, exc=%s",
                    elapsed(),
                    exc_name,
                    exc_x,
                    exc,
                )
                await _login_fail(
                    bot, chat_id, status_msg, f"❌ Login failed: {_escape_markdown(exc)}"
                )
                return

        if has_2fa:
            # Fetch password hint
            try:
                pwd_hint = await client.get_password_hint()
            except Exception:
                pwd_hint = ""  # nosec - fallback when hint is unavailable

            msg_text = "🔐 Two-step verification is enabled. Please enter your account password:"
            if pwd_hint:
                msg_text += f"\nPassword hint: `{pwd_hint.replace('`', '')}`"
            try:
                await status_msg.edit_text(msg_text, parse_mode="Markdown")
            except Exception:
                status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

            entry["password"] = asyncio.get_running_loop().create_future()
            try:
                password = await asyncio.wait_for(
                    entry["password"], timeout=TIMEOUT_SECONDS
                )
            except TimeoutError:
                await _login_fail(
                    bot,
                    chat_id,
                    status_msg,
                    "⏰ Login timed out (no password received within 5 minutes).",
                )
                return

            if entry["cancel"].is_set():
                return

            try:
                await client.check_password(password)
            except pyro_errors.PasswordHashInvalid:
                # Allow one retry
                entry["password"] = asyncio.get_running_loop().create_future()
                try:
                    await status_msg.edit_text(
                        "❌ Incorrect password. Please try again:", parse_mode="Markdown"
                    )
                except Exception:
                    await bot.send_message(chat_id, "❌ Incorrect password. Please try again:")
                try:
                    password = await asyncio.wait_for(
                        entry["password"], timeout=TIMEOUT_SECONDS
                    )
                except TimeoutError:
                    await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                    return
                await client.check_password(password)
            except Exception as exc:
                logger.warning("login [%s]: 2FA password failed: %s", elapsed(), exc)
                await _login_fail(
                    bot, chat_id, status_msg, f"❌ 2FA failed: {_escape_markdown(exc)}"
                )
                return

        # ✅ Login succeeded — export session string
        session_str = await client.export_session_string()
        saved_to = []
        if await _persist_session_to_mongo(
            context,
            user_id,
            {"pyrogram_session": session_str},
            phone=phone,
        ):
            saved_to.append("MongoDB")
        try:
            from utils.telethon_session import (
                save_session_string_to_file_async,  # noqa: PLC0415
            )

            if await save_session_string_to_file_async(
                session_str, client_type="pyrogram", user_id=user_id
            ):
                saved_to.append("JSON file")
        except Exception as exc:
            logger.warning("login: JSON Pyrogram save failed: %s", exc)

        me = await client.get_me()
        uname = getattr(me, "first_name", "") or getattr(me, "username", "user")
        summary = ", ".join(saved_to) if saved_to else "memory"
        try:
            await status_msg.edit_text(
                f"✅ **Pyrogram login successful!**\n"
                f"User: {_escape_markdown(uname)}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {_escape_markdown(summary)}",
                parse_mode="Markdown",
            )
        except Exception:
            await bot.send_message(
                chat_id,
                f"✅ **Pyrogram login successful!**\n"
                f"User: {_escape_markdown(uname)}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {_escape_markdown(summary)}",
                parse_mode="Markdown",
            )
        logger.info("Pyrogram login successful for %s (DC=%s)", phone, client.session.dc_id)

    except asyncio.CancelledError:
        logger.info("login: Pyrogram task cancelled for %s", phone)
    except Exception as exc:
        logger.exception("login: Pyrogram unexpected error: %s", exc)
        with contextlib.suppress(Exception):
            await bot.send_message(chat_id, f"❌ Pyrogram login failed unexpectedly: {exc}")
    finally:
        # Clean up Pyrogram client
        pyro_client = entry.get("pyro_client")
        if pyro_client is not None:
            with contextlib.suppress(Exception):
                await pyro_client.stop()
        # Remove from futures map
        futures_map = context.application.bot_data.get("login_futures", {})
        if user_id in futures_map:
            del futures_map[user_id]
