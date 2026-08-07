"""
Session health-checker: periodically verifies that the configured Pyrogram /
Telethon userbot session is still alive and reports issues to the admin.

This module does **not** attempt fully automatic renewal of session strings
(which requires interactive login via phone + code). Instead it:

1. Periodically connects and verifies the session is authorized.
2. Logs warnings if the session is broken.
3. Sends an alert message to the configured admin via the bot
   when a session transitions from healthy → unhealthy.
4. Cleans up stale file-based Telethon session files that might cause
   confusion on restart.
5. Exposes a ``/sessionstatus`` command handler for on-demand diagnostics.

Usage in bot.py::

    from utils.session_healthcheck import session_healthchecker

    # Start the healthcheck loop
    asyncio.create_task(session_healthchecker.start())

    # On shutdown
    session_healthchecker.stop()
"""

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Optional dependencies (best-effort, matching the pattern in telethon_session.py)
# ──────────────────────────────────────────────────────────────────────
try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession as TelethonStringSession
except Exception:
    TelegramClient = None
    TelethonStringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:
    PyrogramClient = None


# ──────────────────────────────────────────────────────────────────────
# Health check result
# ──────────────────────────────────────────────────────────────────────
class SessionHealth:
    """Holds the health status of a single session type."""

    def __init__(self, name: str):
        self.name = name
        self.alive = False
        self.latency_ms: float | None = None
        self.error: str | None = None
        self.phone: str | None = None
        self.dc_id: int | None = None

    @property
    def ok(self) -> bool:
        return self.alive is True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "alive": self.alive,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "phone": self.phone,
            "dc_id": self.dc_id,
        }


# ──────────────────────────────────────────────────────────────────────
# The checker
# ──────────────────────────────────────────────────────────────────────
class SessionHealthChecker:
    """Periodically checks userbot session health.

    When a session is confirmed healthy during a check, the current session
    string is extracted and persisted to MongoDB (via ``db_model``). This
    ensures long-lived sessions are preserved across restarts even when the
    original env var or ``.session`` file is lost.

    Important
    ---------
    This checker runs as a background asyncio task.  It connects to Telegram
    briefly, checks authorisation, and disconnects.  The overhead is minimal
    (one MTProto round-trip every ``check_interval`` seconds).

    If a previously-healthy session becomes unhealthy the admin is notified
    **once** (rate-limited by ``_last_advisory_time``).
    """

    def __init__(
        self,
        check_interval: int = 3600,  # every hour
        admin_user_id: int | None = None,
        bot_app=None,  # PTB Application
        db_model=None,  # MongoDB model with save_session/load_session
        max_consecutive_failures: int = 3,
    ):
        self.check_interval = check_interval
        self.admin_user_id = admin_user_id
        self.bot_app = bot_app
        self.db_model = db_model
        self.max_consecutive_failures = max_consecutive_failures

        self.is_running = False
        self._task: asyncio.Task | None = None

        # Track transitions so we only alert once per failure streak
        self._prev_pyrogram_ok: bool | None = None
        self._prev_telethon_ok: bool | None = None
        self._pyrogram_failures = 0
        self._telethon_failures = 0
        self._last_advisory_time: float = 0
        self._min_advisory_interval: float = 3600  # don't spam admin

        # Cache the last health result for the /sessionstatus command
        self.last_health: dict = {}

    # ── Public API ──────────────────────────────────────────────────

    def start(self) -> asyncio.Task | None:
        """Start the periodic healthcheck loop as a background task.

        Returns ``None`` when there is no running event loop (e.g. during
        module import) instead of leaking an un-awaited coroutine; the
        startup path that has a live loop should call this again.
        """
        if self._task is not None and not self._task.done():
            logger.debug("SessionHealthChecker is already running")
            return self._task
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            logger.debug(
                "SessionHealthChecker.start: no running event loop; deferring"
            )
            self.is_running = False
            return None
        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "SessionHealthChecker started (interval=%ss, max_failures=%s)",
            self.check_interval,
            self.max_consecutive_failures,
        )
        return self._task

    def stop(self):
        """Signal the healthcheck loop to stop."""
        self.is_running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
        logger.info("SessionHealthChecker stop requested")

    async def run_once(self, user_id: int | None = None) -> dict:
        """Run a single health check and return the result dict.

        When ``user_id`` is provided, checks that user's own sessions
        (per-user login) instead of the global/admin session.

        Useful for on-demand diagnostics (e.g. the ``/loginstatus`` command).
        """
        results = await self._check_all(user_id=user_id)
        # Only the global/admin check updates the shared last_health used by
        # the background loop and admin alerts; per-user checks must not
        # clobber it.
        if user_id is None:
            self.last_health = {r["name"]: r for r in results}
        return {r["name"]: r for r in results}

    # ── Internal loop ───────────────────────────────────────────────

    async def _run_loop(self):
        """Background loop: check, sleep, repeat."""
        # Run the first check immediately so the admin gets alerted early
        # if the session is already broken.
        await asyncio.sleep(5)  # brief delay so the bot finishes starting
        try:
            await self._check_and_notify()
        except Exception:
            logger.exception("SessionHealthChecker: first check failed")

        while self.is_running:
            try:
                await asyncio.sleep(self.check_interval)
                await self._check_and_notify()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "SessionHealthChecker: check iteration failed"
                )

        logger.info("SessionHealthChecker loop stopped")

    async def _check_and_notify(self):
        """Run health checks and alert admin on transition to unhealthy."""
        results = await self._check_all()
        self.last_health = {r["name"]: r for r in results}

        # Log summary
        for r in results:
            if r["alive"]:
                logger.debug(
                    "SessionHealthChecker: %s OK (dc=%s, latency=%.0fms)",
                    r["name"],
                    r["dc_id"],
                    r["latency_ms"] or 0,
                )
            else:
                logger.warning(
                    "SessionHealthChecker: %s UNHEALTHY \u2014 %s",
                    r["name"],
                    r["error"] or "unknown error",
                )

        # Detect transitions for Pyrogram
        pyro_result = results[0] if len(results) > 0 else None
        if pyro_result is not None:
            now_ok = pyro_result["alive"]
            if now_ok:
                self._pyrogram_failures = 0
            else:
                self._pyrogram_failures += 1

            if self._prev_pyrogram_ok is True and not now_ok:
                # Transitioned healthy \u2192 unhealthy \u2014 try recovery first
                logger.info(
                    "SessionHealthChecker: Pyrogram session unhealthy, attempting recovery..."
                )
                recovered = await self._attempt_session_recovery()
                if recovered:
                    logger.info(
                        "SessionHealthChecker: Pyrogram session recovered via recycling"
                    )
                    self._pyrogram_failures = 0
                    self._prev_pyrogram_ok = True
                    # Don't send alert since we recovered
                else:
                    await self._alert_admin(
                        "\u26a0\ufe0f *Pyrogram session went UNHEALTHY*\nRecovery attempt failed \u2014 you may need to regenerate the session string.",
                        pyro_result,
                    )
                    self._prev_pyrogram_ok = now_ok
            elif self._prev_pyrogram_ok is False and now_ok:
                logger.info("SessionHealthChecker: Pyrogram session recovered")
                self._prev_pyrogram_ok = now_ok
            elif self._prev_pyrogram_ok is None:
                # First check \u2014 just record the state, no alert
                self._prev_pyrogram_ok = now_ok
            else:
                self._prev_pyrogram_ok = now_ok

        # Detect transitions for Telethon
        tl_result = results[1] if len(results) > 1 else None
        if tl_result is not None:
            now_ok = tl_result["alive"]
            if now_ok:
                self._telethon_failures = 0
            else:
                self._telethon_failures += 1

            if self._prev_telethon_ok is True and not now_ok:
                await self._alert_admin(
                    "\u26a0\ufe0f *Telethon session went UNHEALTHY*",
                    tl_result,
                )
                self._prev_telethon_ok = now_ok
            elif self._prev_telethon_ok is False and now_ok:
                logger.info("SessionHealthChecker: Telethon session recovered")
                self._prev_telethon_ok = now_ok
            elif self._prev_telethon_ok is None:
                # First check \u2014 just record the state, no alert
                self._prev_telethon_ok = now_ok
            else:
                self._prev_telethon_ok = now_ok

        # If consecutive failures exceed threshold, re-alert
        pyro_bad = (
            self._pyrogram_failures >= self.max_consecutive_failures
            and self._pyrogram_failures > 0
        )
        tl_bad = (
            self._telethon_failures >= self.max_consecutive_failures
            and self._telethon_failures > 0
        )
        if pyro_bad or tl_bad:
            now = time.time()
            if now - self._last_advisory_time > self._min_advisory_interval:
                self._last_advisory_time = now
                lines = [
                    "\ud83d\udea8 *Persistent session failures detected*",
                    f"Pyrogram failures: {self._pyrogram_failures}",
                    f"Telethon failures: {self._telethon_failures}",
                    "",
                    "You may need to regenerate the session:\n"
                    "`python scripts/create_pyrogram_session.py`",
                ]
                await self._send_admin_message("\n".join(lines))

    # ── Health checks ──────────────────────────────────────────────

    async def _check_all(self, user_id: int | None = None) -> list:
        """Run both Pyrogram and Telethon checks in parallel.

        When ``user_id`` is provided, each check resolves that user's own
        sessions (per-user login).
        """
        results = []
        tasks = []

        if PyrogramClient is not None:
            tasks.append(self._check_pyrogram(user_id=user_id))

        if TelegramClient is not None:
            tasks.append(self._check_telethon(user_id=user_id))

        if not tasks:
            logger.debug("SessionHealthChecker: no client libraries available")
            return results

        done = await asyncio.gather(*tasks, return_exceptions=True)
        for t in done:
            if isinstance(t, Exception):
                h = SessionHealth("unknown")
                h.alive = False
                h.error = str(t)
                results.append(h.to_dict())
            elif t is not None:
                results.append(
                    t.to_dict() if isinstance(t, SessionHealth) else t
                )
        return results

    async def _attempt_session_recovery(self) -> bool:
        """Try to recycle the Pyrogram session (disconnect/reconnect).

        Uses the same pattern as :func:`utils.userbot_downloader._recycle_client_session`
        to potentially revive a stale connection without requiring a new login.

        Returns ``True`` if recovery succeeded.
        """
        try:
            from utils.telethon_session import (
                build_pyrogram_client,
                get_pyrogram_session_string,
                get_userbot_credentials,
            )

            if not get_pyrogram_session_string():
                return False
            api_id, api_hash = get_userbot_credentials()
            client = build_pyrogram_client(api_id, api_hash)
            if client is None:
                return False

            logger.info(
                "SessionHealthChecker: attempting session recycling (stop\u2192start)"
            )
            try:
                await client.start()
                await client.stop()
                await asyncio.sleep(2)
                await client.start()
                me = await client.get_me()
                ok = me is not None
                logger.info(
                    "SessionHealthChecker: session recycling %s",
                    "OK" if ok else "failed",
                )
                return ok
            finally:
                try:
                    await asyncio.sleep(0.5)
                except Exception:  # nosec B110
                    pass
                try:
                    await client.stop()
                except Exception:  # nosec B110
                    pass
        except Exception as exc:
            logger.warning(
                "SessionHealthChecker: session recycling error: %s", exc
            )
            return False

    async def _check_pyrogram(self, user_id: int | None = None) -> SessionHealth:
        """Check if the Pyrogram session string is still valid.

        When ``user_id`` is provided, resolves that user's own session
        (per-user login).  On success, persists the session string to
        MongoDB + JSON so long-lived sessions survive restarts
        (see ``_save_pyrogram_session``).
        """
        h = SessionHealth("pyrogram")

        try:
            from utils.telethon_session import (
                build_pyrogram_client,
                get_pyrogram_session_string,
                get_userbot_credentials,
            )
        except ImportError as exc:
            h.error = f"import failed: {exc}"
            return h

        # Resolve which user_id to use: caller-specified, then admin, then None
        check_user_id = user_id or self.admin_user_id

        # Resolve the session string: per-user session first when user_id is
        # provided, env/global shared session as fallback.
        try:
            session_str = get_pyrogram_session_string(user_id=check_user_id)
        except Exception as exc:
            h.error = f"config check failed: {exc}"
            return h

        if not session_str:
            # Fall back to the MongoDB-persisted session (mirrors the
            # reference's build_pyrogram_client_async resolution, so
            # /loginstatus matches what downloads actually resolve).
            try:
                from utils.telethon_session import (
                    get_pyrogram_session_string_for_user,
                )

                session_str = await get_pyrogram_session_string_for_user(
                    user_id=check_user_id, db_model=self.db_model
                )
            except Exception:
                session_str = None

        if not session_str and user_id is None:
            # Fall back to the most recent session stored for ANY user — but
            # ONLY for the global periodic check, which doesn't know which user
            # owns a /loginpyro session. Per-user /loginstatus checks must stay
            # truthful (never report another user's session as this user's).
            session_str = await self._load_any_session("pyrogram_session")
            if session_str:
                logger.info(
                    "SessionHealthChecker: Pyrogram session found via latest-Mongo fallback"
                )

        if not session_str:
            h.alive = False
            h.error = "PYROGRAM_SESSION not configured"
            return h

        try:
            api_id, api_hash = get_userbot_credentials()
        except RuntimeError as exc:
            h.error = str(exc)
            return h

        client = build_pyrogram_client(
            api_id, api_hash, session_str=session_str
        )
        if client is None:
            h.error = "build_pyrogram_client returned None"
            return h

        t0 = time.time()
        try:
            await client.start()
            elapsed = (time.time() - t0) * 1000  # ms
            h.latency_ms = round(elapsed, 1)

            me = await client.get_me()
            if me is not None:
                h.alive = True
                h.phone = getattr(me, "phone_number", None)
                # Extract DC from raw session data
                try:
                    h.dc_id = (
                        client.storage.dc_id()
                        if hasattr(client.storage, "dc_id")
                        else None
                    )
                except Exception:  # nosec B110
                    pass
                # Persist session string to MongoDB + JSON for long-term survival
                await self._save_pyrogram_session(client, user_id=user_id)
            else:
                h.error = "get_me() returned None (not authorized)"
        except Exception as exc:
            elapsed = (time.time() - t0) * 1000
            h.latency_ms = round(elapsed, 1)
            h.error = str(exc)[:200]
        finally:
            try:
                await asyncio.sleep(0.5)
            except Exception:  # nosec B110
                pass
            try:
                await client.stop()
            except Exception:  # nosec B110
                pass

        return h

    async def _check_telethon(self, user_id: int | None = None) -> SessionHealth:
        """Check if the Telethon session is still valid.

        When ``user_id`` is provided, resolves that user's own session
        (per-user login).  On success, persists the session string to
        MongoDB + JSON so long-lived sessions survive restarts
        (see ``_save_telethon_session``).
        """
        h = SessionHealth("telethon")

        from utils.telethon_session import (
            _get_configured_session_string,
            build_telethon_client,
            get_telethon_session_path,
            get_userbot_credentials,
        )

        # Resolve which user_id to use: caller-specified, then admin, then None
        check_user_id = user_id or self.admin_user_id

        # Resolve the session string: per-user session first when user_id is
        # provided, env/global shared session as fallback.
        try:
            session_str = _get_configured_session_string(user_id=check_user_id)
        except Exception as exc:
            h.error = f"config check failed: {exc}"
            return h

        if not session_str:
            # Fall back to the MongoDB-persisted session (mirrors the
            # reference's resolution, so /loginstatus matches downloads).
            try:
                from utils.telethon_session import (
                    get_telethon_session_string_for_user,
                )

                session_str = await get_telethon_session_string_for_user(
                    user_id=check_user_id, db_model=self.db_model
                )
            except Exception:
                session_str = None

        if not session_str and user_id is None:
            # Fall back to the most recent session stored for ANY user — but
            # ONLY for the global periodic check. Per-user /loginstatus checks
            # must stay truthful (never report another user's session).
            session_str = await self._load_any_session("telethon_session")
            if session_str:
                logger.info(
                    "SessionHealthChecker: Telethon session found via latest-Mongo fallback"
                )

        if not session_str:
            # Fall back to checking for a file-based .session on disk
            session_path = get_telethon_session_path()
            if os.path.exists(session_path) or os.path.exists(
                session_path + ".session"
            ):
                # File-based session exists \u2014 let build_telethon_client find it
                pass  # proceed with build below (session_str stays None)
            else:
                h.error = "Telethon session not configured"
                return h

        try:
            api_id, api_hash = get_userbot_credentials()
        except RuntimeError as exc:
            h.error = str(exc)
            return h

        try:
            client = build_telethon_client(
                api_id, api_hash, session_str=session_str
            )
        except Exception as exc:
            h.error = f"build_telethon_client failed: {exc}"
            return h

        t0 = time.time()
        try:
            await client.connect()
            elapsed = (time.time() - t0) * 1000
            h.latency_ms = round(elapsed, 1)

            if await client.is_user_authorized():
                h.alive = True
                try:
                    me = await client.get_me()
                    if me is not None:
                        h.phone = getattr(me, "phone", None)
                except Exception:  # nosec B110
                    pass
                try:
                    h.dc_id = (
                        client.session.dc_id
                        if hasattr(client.session, "dc_id")
                        else None
                    )
                except Exception:  # nosec B110
                    pass
                # Persist session string to MongoDB + JSON for long-term survival
                await self._save_telethon_session(client, user_id=user_id)
            else:
                h.error = "Session exists but user is not authorized"
        except Exception as exc:
            elapsed = (time.time() - t0) * 1000
            h.latency_ms = round(elapsed, 1)
            h.error = str(exc)[:200]
        finally:
            try:
                await client.disconnect()
            except Exception:  # nosec B110
                pass

        return h

    # ── Session persistence helpers ────────────────────────────────

    async def _load_any_session(self, key: str) -> str | None:
        """Return the most recent session string of a given type for ANY user.

        The periodic healthcheck does not know which user owns a session
        created via /login or /loginpyro, so when per-user lookups miss we
        scan MongoDB for the latest document containing the requested key
        (e.g. "pyrogram_session" or "telethon_session").

        Legacy documents that predate the typed-key split and only carry
        ``string_session`` are used as a last resort.
        """
        try:
            from utils.db import COL_SESSIONS, get_db, query

            db = await get_db()
            if not db:
                return None
            # Most recent doc that actually contains this typed session key
            # (non-empty), ordered by last_active so the newest login wins.
            doc = await (
                query(COL_SESSIONS, db)
                .where(key, "!=", "")
                .order_by("last_active", "desc")
                .first()
            )
            if doc and doc.get(key):
                return str(doc[key])

            # Legacy fallback: a doc carrying the old string_session key.
            # Only use it when it does NOT also carry the OTHER typed key, so a
            # legacy Pyrogram string is never served for a Telethon lookup (and
            # vice versa) — mirrors the reference's typed-key guard.
            other_key = "pyrogram_session" if key == "telethon_session" else "telethon_session"
            legacy = await (
                query(COL_SESSIONS, db)
                .where("string_session", "!=", "")
                .order_by("last_active", "desc")
                .first()
            )
            if legacy and legacy.get("string_session"):
                # Skip if the doc carries the other typed session key: the
                # first query already covered typed-key docs, so this one is
                # either a genuine pre-split legacy doc or ambiguous.
                if legacy.get(other_key):
                    return None
                return str(legacy["string_session"])
            return None
        except Exception as exc:
            logger.debug(
                "SessionHealthChecker: latest-session Mongo fallback failed: %s",
                exc,
            )
            return None

    async def _save_telethon_session(self, client, user_id: int | None = None):
        """Extract and persist the current Telethon session string.

        When ``user_id`` is provided the session is scoped to that user
        (per-user JSON file + MongoDB keyed by user).  Otherwise the
        admin user (or the legacy global JSON file) is used.

        Saves to both MongoDB (for the login flow) and a local JSON file
        (for the downloader/uploader fallback chain). Best-effort.
        """
        try:
            session_str = TelethonStringSession.save(client.session)
            if not session_str:
                return
            session_str = str(session_str)

            target_user_id = user_id if user_id is not None else self.admin_user_id

            # Save to local JSON file (bridges StringSession -> file fallback)
            saved_file = False
            try:
                from utils.telethon_session import (
                    save_session_string_to_file_async,
                )

                saved_file = await save_session_string_to_file_async(
                    session_str, client_type="telethon", user_id=user_id
                )
            except Exception:  # nosec B110
                pass

            # Save to MongoDB (for login flow and diagnostics).
            saved_mongo = False
            if target_user_id is not None:
                try:
                    if self.db_model is not None:
                        await self.db_model.save_session(
                            target_user_id,
                            {
                                "telethon_session": session_str,
                                "string_session": session_str,  # backward compat
                            },
                        )
                        saved_mongo = True
                    else:
                        from utils.db import save_user_session

                        await save_user_session(
                            target_user_id,
                            {
                                "telethon_session": session_str,
                                "string_session": session_str,
                            },
                        )
                        saved_mongo = True
                except Exception as exc:
                    logger.debug(
                        "SessionHealthChecker: failed to persist Telethon session to MongoDB: %s",
                        exc,
                    )

            if saved_file or saved_mongo:
                logger.info(
                    "SessionHealthChecker: persisted Telethon session (file=%s, mongo=%s)",
                    saved_file,
                    saved_mongo,
                )
            else:
                logger.debug(
                    "SessionHealthChecker: skipped Telethon session persistence (no targets configured)",
                )
        except Exception as exc:
            logger.debug(
                "SessionHealthChecker: failed to extract Telethon session string: %s",
                exc,
            )

    async def _save_pyrogram_session(self, client, user_id: int | None = None):
        """Export and persist the current Pyrogram session string.

        When ``user_id`` is provided the session is scoped to that user
        (per-user JSON file + MongoDB keyed by user).  Otherwise the
        admin user (or the legacy global JSON file) is used.

        Saves to both MongoDB (for the login flow) and a local JSON file
        (for the downloader/uploader fallback chain). Best-effort.
        """
        try:
            session_str = await client.export_session_string()
            if not session_str:
                return
            session_str = str(session_str)

            target_user_id = user_id if user_id is not None else self.admin_user_id

            # Save to local JSON file (bridges in-memory session -> file fallback)
            saved_file = False
            try:
                from utils.telethon_session import (
                    save_session_string_to_file_async,
                )

                saved_file = await save_session_string_to_file_async(
                    session_str, client_type="pyrogram", user_id=user_id
                )
            except Exception:  # nosec B110
                pass

            # Save to MongoDB (for login flow and diagnostics).
            saved_mongo = False
            if target_user_id is not None:
                try:
                    if self.db_model is not None:
                        await self.db_model.save_session(
                            target_user_id,
                            {
                                "pyrogram_session": session_str,
                                "string_session": session_str,  # backward compat
                            },
                        )
                        saved_mongo = True
                    else:
                        from utils.db import save_user_session

                        await save_user_session(
                            target_user_id,
                            {
                                "pyrogram_session": session_str,
                                "string_session": session_str,
                            },
                        )
                        saved_mongo = True
                except Exception as exc:
                    logger.debug(
                        "SessionHealthChecker: failed to persist Pyrogram session to MongoDB: %s",
                        exc,
                    )

            if saved_file or saved_mongo:
                logger.info(
                    "SessionHealthChecker: persisted Pyrogram session (file=%s, mongo=%s)",
                    saved_file,
                    saved_mongo,
                )
            else:
                logger.debug(
                    "SessionHealthChecker: skipped Pyrogram session persistence (no targets configured)",
                )
        except Exception as exc:
            logger.debug(
                "SessionHealthChecker: failed to export Pyrogram session string: %s",
                exc,
            )

    # ── Privacy helper ─────────────────────────────────────────────

    @staticmethod
    def _mask_phone(phone: str | None) -> str | None:
        """Mask a phone number for privacy, showing only first 3 and last 2 digits.

        Examples:
            +1234567890  ->  +12******90
            1234567890   ->  123*****90
            None         ->  None
        """
        if not phone:
            return None
        phone = phone.strip()
        if len(phone) <= 5:
            # Short number: show only first 2 chars + ***
            return phone[:2] + "***"
        # Show first 3 chars, mask middle, show last 2
        return phone[:3] + "*" * (len(phone) - 5) + phone[-2:]

    # ── Admin alerts ────────────────────────────────────────────────

    async def _alert_admin(self, title: str, result: dict):
        """Send a one-time alert to the admin about a session issue."""
        now = time.time()
        if now - self._last_advisory_time < self._min_advisory_interval:
            logger.debug(
                "SessionHealthChecker: skipping admin alert (rate-limited)"
            )
            return
        self._last_advisory_time = now

        status_emoji = "\u274c" if not result.get("alive") else "\u2705"
        lines = [
            title,
            "",
            f"{status_emoji} Status: `{'Alive' if result.get('alive') else 'Unhealthy'}`",
            f"\u23f1 Latency: `{result.get('latency_ms', 'N/A')} ms`",
            f"\u26a0 Error: `{result.get('error', 'None')}`",
        ]
        phone = self._mask_phone(result.get("phone"))
        if phone:
            lines.append(f"\ud83d\udcf1 Phone: `{phone}`")
        if result.get("dc_id"):
            lines.append(f"\ud83d\udda5 DC: `{result['dc_id']}`")
        lines.extend(
            [
                "",
                "Regenerate with:\n"
                "`python scripts/create_pyrogram_session.py`",
            ]
        )
        await self._send_admin_message("\n".join(lines))

    async def _send_admin_message(self, text: str):
        """Send a Markdown-formatted message to the admin via the bot."""
        if not self.admin_user_id or not self.bot_app:
            logger.debug(
                "SessionHealthChecker: no admin_user_id or bot_app; "
                "skipping admin notification"
            )
            return
        try:
            await self.bot_app.bot.send_message(
                chat_id=self.admin_user_id,
                text=text,
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning(
                "SessionHealthChecker: failed to send admin message: %s",
                exc,
            )

    # ── Status report text (for /sessionstatus) ─────────────────────

    def format_status_text(self) -> str:
        """Return a human-readable Markdown string of the last health results."""
        if not self.last_health:
            return (
                "\ud83e\ude7a *Session Health* \u2014 No checks have run yet."
            )

        lines = ["\ud83e\ude7a *Session Health Report*\n"]
        for name in ("pyrogram", "telethon"):
            r = self.last_health.get(name)
            if r is None:
                lines.append(f"\u2022 *{name.capitalize()}*: Not configured")
                continue

            status_emoji = "\u2705" if r.get("alive") else "\u274c"
            lines.append(f"{status_emoji} *{name.capitalize()}*")
            lines.append(f"   Alive: `{r.get('alive')}`")
            lines.append(f"   Latency: `{r.get('latency_ms', 'N/A')} ms`")
            phone = self._mask_phone(r.get("phone"))
            if phone:
                lines.append(f"   Phone: `{phone}`")
            if r.get("dc_id"):
                lines.append(f"   DC: `{r['dc_id']}`")
            if r.get("error"):
                lines.append(f"   Error: `{r['error']}`")
            lines.append("")

        lines.append(
            "\ud83d\udd04 Check interval: `{}s`\n"
            "\ud83d\udd14 Admin alerts: `{}`".format(
                self.check_interval,
                "Enabled" if self.admin_user_id else "Disabled",
            )
        )
        return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# Global singleton (following the cleanup_manager pattern)
# ──────────────────────────────────────────────────────────────────────
_session_healthchecker: SessionHealthChecker | None = None


def get_session_healthchecker() -> SessionHealthChecker:
    """Return the global ``SessionHealthChecker`` singleton.

    Create it on first call.
    """
    global _session_healthchecker
    if _session_healthchecker is None:
        _session_healthchecker = SessionHealthChecker()
    return _session_healthchecker


def start_session_healthcheck(
    admin_user_id: int | None = None,
    bot_app=None,
    db_model=None,
    check_interval: int = 3600,
) -> asyncio.Task | None:
    """Start the session healthcheck background loop.

    When a session is confirmed healthy, its session string is automatically
    persisted to MongoDB (via ``db_model``) so it survives restarts.

    Parameters
    ----------
    admin_user_id:
        Telegram user ID to receive alerts when a session becomes unhealthy.
    bot_app:
        The PTB ``Application`` instance (needed to send admin messages).
    db_model:
        MongoDB model (e.g. ``MediaConversionModel``) with ``save_session`` method.
        When provided, session strings are persisted after successful health checks.
        Falls back to ``utils.db.save_user_session`` when not provided.
    check_interval:
        Seconds between health checks (default 3600 = 1 hour).

    Returns the background ``asyncio.Task``, or ``None`` if there was no
    running event loop (e.g. called during module import).
    """
    checker = get_session_healthchecker()
    if admin_user_id is not None:
        checker.admin_user_id = admin_user_id
    if bot_app is not None:
        checker.bot_app = bot_app
    if db_model is not None:
        checker.db_model = db_model
    checker.check_interval = check_interval
    return checker.start()


def stop_session_healthcheck():
    """Stop the session healthcheck loop."""
    checker = get_session_healthchecker()
    checker.stop()
