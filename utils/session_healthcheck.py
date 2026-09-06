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

from utils.markdown_utils import safe_code_span as _safe_code_span
from utils.markdown_utils import sanitize_text              

logger = logging.getLogger(__name__)

                                                                        
                                                                                  
                                                                        
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


                                                                        
                                                  
                                                                        
SOURCE_LABELS: dict[str, str] = {
    "json": "your session (JSON file)",
    "mongodb": "your session (MongoDB)",
    "env": "⚠️ shared env session (fallback)",
    "global-json": "⚠️ shared global session (fallback)",
    "any-user-mongodb": "⚠️ shared session (latest in MongoDB)",
    "file": "file-based .session",
    "missing": "none stored",
    "unknown": "unknown",
}


def source_label(source: str) -> str:
                                                                       
    return SOURCE_LABELS.get(source, source or "unknown")


                                                                        
                     
                                                                        
class SessionHealth:
                                                           

    def __init__(self, name: str):
        self.name = name
        self.alive = False
        self.latency_ms: float | None = None
        self.error: str | None = None
        self.phone: str | None = None
        self.dc_id: int | None = None
                                                                             
                                                                     
                                                                           
                                                       
        self.source: str = "unknown"

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
            "source": self.source,
        }


                                                                        
             
                                                                        
class SessionHealthChecker:
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
       

    def __init__(
        self,
        check_interval: int = 3600,              
        admin_user_id: int | None = None,
        bot_app=None,                   
        db_model=None,                                                
        max_consecutive_failures: int = 3,
    ):
        self.check_interval = check_interval
        self.admin_user_id = admin_user_id
        self.bot_app = bot_app
        self.db_model = db_model
        self.max_consecutive_failures = max_consecutive_failures

        self.is_running = False
        self._task: asyncio.Task | None = None

                                                                    
        self._prev_pyrogram_ok: bool | None = None
        self._prev_telethon_ok: bool | None = None
        self._pyrogram_failures = 0
        self._telethon_failures = 0
        self._last_advisory_time: float = 0
        self._min_advisory_interval: float = 3600                    

                                                                     
        self.last_health: dict = {}

                                                                      

    def start(self) -> asyncio.Task | None:
\
\
\
\
\
           
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
                                                  
        self.is_running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
        logger.info("SessionHealthChecker stop requested")

    async def run_once(self, user_id: int | None = None) -> dict:
\
\
\
\
\
\
           
        results = await self._check_all(user_id=user_id)
                                                                            
                                                                        
                     
        if user_id is None:
            self.last_health = {r["name"]: r for r in results}
        return {r["name"]: r for r in results}

                                                                      

    async def _run_loop(self):
                                                    
                                                                         
                                           
        await asyncio.sleep(5)                                            
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
                                                                           
        results = await self._check_all()
        self.last_health = {r["name"]: r for r in results}

                     
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

                                         
        pyro_result = results[0] if len(results) > 0 else None
        if pyro_result is not None:
            now_ok = pyro_result["alive"]
            if now_ok:
                self._pyrogram_failures = 0
            else:
                self._pyrogram_failures += 1

            if self._prev_pyrogram_ok is True and not now_ok:
                                                                                 
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
                                                                    
                self._prev_pyrogram_ok = now_ok
            else:
                self._prev_pyrogram_ok = now_ok

                                         
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
                                                                    
                self._prev_telethon_ok = now_ok
            else:
                self._prev_telethon_ok = now_ok

                                                            
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
                    "\U0001f6a8 *Persistent session failures detected*",
                    f"Pyrogram failures: {self._pyrogram_failures}",
                    f"Telethon failures: {self._telethon_failures}",
                    "",
                    "You may need to regenerate the session:\n"
                    "`python scripts/create_pyrogram_session.py`",
                ]
                await self._send_admin_message("\n".join(lines))

                                                                     

    async def _check_all(self, user_id: int | None = None) -> list:
\
\
\
\
           
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
\
\
\
\
\
\
           
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
                except Exception:              
                    pass
                try:
                    await client.stop()
                except Exception:              
                    pass
        except Exception as exc:
            logger.warning(
                "SessionHealthChecker: session recycling error: %s", exc
            )
            return False

    async def _check_pyrogram(self, user_id: int | None = None) -> SessionHealth:
\
\
\
\
\
\
           
        h = SessionHealth("pyrogram")

        try:
            from utils.telethon_session import (
                build_pyrogram_client,
                get_userbot_credentials,
            )
        except ImportError as exc:
            h.error = f"import failed: {exc}"
            return h

                                                                               
        check_user_id = user_id or self.admin_user_id

                                                                            
                                                                               
                                                                               
                                                                           
                                                                             
                                                                             
                                         
        try:
            from utils.telethon_session import (                 
                _resolve_pyrogram_session_with_source,
            )

            session_str, source = await _resolve_pyrogram_session_with_source(
                user_id=check_user_id, db_model=self.db_model
            )
        except Exception as exc:
            h.source = "unknown"
            h.error = f"config check failed: {exc}"
            return h
        h.source = source

        if not session_str and user_id is None:
                                                                            
                                                                               
                                                                               
                                                                            
            session_str = await self._load_any_session("pyrogram_session")
            if session_str:
                h.source = "any-user-mongodb"
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
            elapsed = (time.time() - t0) * 1000      
            h.latency_ms = round(elapsed, 1)

            me = await client.get_me()
            if me is not None:
                h.alive = True
                h.phone = getattr(me, "phone_number", None)
                                                  
                try:
                    h.dc_id = (
                        await client.storage.dc_id()
                        if hasattr(client.storage, "dc_id")
                        else None
                    )
                except Exception:              
                    pass
                                                                             
                                                                          
                                                                             
                                                                               
                                                                            
                                                               
                stored = await self._stored_session_for_user(
                    user_id, "pyrogram"
                )
                if (stored and session_str == stored) or user_id is None:
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
            except Exception:              
                pass
            try:
                await client.stop()
            except Exception:              
                pass

        return h

    async def _check_telethon(self, user_id: int | None = None) -> SessionHealth:
\
\
\
\
\
\
           
        h = SessionHealth("telethon")

        from utils.telethon_session import (
            build_telethon_client,
            get_telethon_session_path,
            get_userbot_credentials,
        )

                                                                               
        check_user_id = user_id or self.admin_user_id

                                                                            
                                                                               
                                                                               
                                                                           
                                                                             
                                                                             
                                         
        try:
            from utils.telethon_session import (                 
                _resolve_telethon_session_with_source,
            )

            session_str, source = await _resolve_telethon_session_with_source(
                user_id=check_user_id, db_model=self.db_model
            )
        except Exception as exc:
            h.source = "unknown"
            h.error = f"config check failed: {exc}"
            return h
        h.source = source

        if not session_str and user_id is None:
                                                                            
                                                                              
                                                                       
            session_str = await self._load_any_session("telethon_session")
            if session_str:
                h.source = "any-user-mongodb"
                logger.info(
                    "SessionHealthChecker: Telethon session found via latest-Mongo fallback"
                )

        if not session_str:
                                                                     
            session_path = get_telethon_session_path()
            if os.path.exists(session_path) or os.path.exists(
                session_path + ".session"
            ):
                                                                                    
                h.source = "file"
                pass                                                     
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
                except Exception:              
                    pass
                try:
                    h.dc_id = (
                        client.session.dc_id
                        if hasattr(client.session, "dc_id")
                        else None
                    )
                except Exception:              
                    pass
                                                                             
                                                                          
                                                                             
                                                                               
                                                                            
                                                               
                stored = await self._stored_session_for_user(
                    user_id, "telethon"
                )
                if (stored and session_str == stored) or user_id is None:
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
            except Exception:              
                pass

        return h

                                                                     

    async def _stored_session_for_user(
        self, user_id: int | None, client_type: str
    ) -> str | None:
\
\
\
\
\
\
\
           
        if user_id is None:
            return None
        key = "telethon_session" if client_type == "telethon" else "pyrogram_session"
        try:
            from utils.telethon_session import (                 
                _load_all_sessions_from_file_async,
            )

            data = await _load_all_sessions_from_file_async(user_id=user_id)
            if data and data.get(key):
                return str(data[key])
        except Exception:              
            pass
        try:
            if self.db_model is not None and hasattr(self.db_model, "load_session"):
                doc = await self.db_model.load_session(user_id)
            else:
                from utils.db import get_user_session

                doc = await get_user_session(user_id)
            if isinstance(doc, dict) and doc.get(key):
                return str(doc[key])
        except Exception:              
            pass
        return None

    async def _load_any_session(self, key: str) -> str | None:
\
\
\
\
\
\
\
\
\
           
        try:
            from utils.db import COL_SESSIONS, get_db, query

            db = await get_db()
            if db is None:
                return None
                                                                           
                                                                           
            doc = await (
                query(COL_SESSIONS, db)
                .where(key, "!=", "")
                .order_by("last_active", "desc")
                .first()
            )
            if doc and doc.get(key):
                return str(doc[key])

                                                                         
                                                                               
                                                                               
                                                                    
            other_key = "pyrogram_session" if key == "telethon_session" else "telethon_session"
            legacy = await (
                query(COL_SESSIONS, db)
                .where("string_session", "!=", "")
                .order_by("last_active", "desc")
                .first()
            )
            if legacy and legacy.get("string_session"):
                                                                          
                                                                            
                                                                     
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
\
\
\
\
\
\
\
\
           
        try:
            session_str = TelethonStringSession.save(client.session)
            if not session_str:
                return
            session_str = str(session_str)

            target_user_id = user_id if user_id is not None else self.admin_user_id

                                                                              
            saved_file = False
            try:
                from utils.telethon_session import (
                    save_session_string_to_file_async,
                )

                saved_file = await save_session_string_to_file_async(
                    session_str, client_type="telethon", user_id=user_id
                )
            except Exception:              
                pass

                                                               
            saved_mongo = False
            if target_user_id is not None:
                try:
                    if self.db_model is not None:
                        await self.db_model.save_session(
                            target_user_id,
                            {
                                "telethon_session": session_str,
                                "string_session": session_str,                   
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
\
\
\
\
\
\
\
\
           
        try:
            session_str = await client.export_session_string()
            if not session_str:
                return
            session_str = str(session_str)

            target_user_id = user_id if user_id is not None else self.admin_user_id

                                                                                  
            saved_file = False
            try:
                from utils.telethon_session import (
                    save_session_string_to_file_async,
                )

                saved_file = await save_session_string_to_file_async(
                    session_str, client_type="pyrogram", user_id=user_id
                )
            except Exception:              
                pass

                                                               
            saved_mongo = False
            if target_user_id is not None:
                try:
                    if self.db_model is not None:
                        await self.db_model.save_session(
                            target_user_id,
                            {
                                "pyrogram_session": session_str,
                                "string_session": session_str,                   
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

                                                                     

    @staticmethod
    def _mask_phone(phone: str | None) -> str | None:
\
\
\
\
\
\
           
        if not phone:
            return None
        phone = phone.strip()
        if len(phone) <= 5:
                                                         
            return phone[:2] + "***"
                                                      
        return phone[:3] + "*" * (len(phone) - 5) + phone[-2:]

                                                                      

    async def _alert_admin(self, title: str, result: dict):
                                                                       
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
                                                                            
                                                                 
                                                              
            f"\u26a0 Error: `{_safe_code_span(result.get('error', 'None'))}`",
        ]
        phone = self._mask_phone(result.get("phone"))
        if phone:
            lines.append(f"\U0001f4f1 Phone: `{phone}`")
        if result.get("dc_id"):
            lines.append(f"\U0001f5a5 DC: `{result['dc_id']}`")
        lines.extend(
            [
                "",
                "Regenerate with:\n"
                "`python scripts/create_pyrogram_session.py`",
            ]
        )
        await self._send_admin_message("\n".join(lines))

    async def _send_admin_message(self, text: str):
                                                                         
        if not self.admin_user_id or not self.bot_app:
            logger.debug(
                "SessionHealthChecker: no admin_user_id or bot_app; "
                "skipping admin notification"
            )
            return
        try:
            await self.bot_app.bot.send_message(
                chat_id=self.admin_user_id,
                text=sanitize_text(text),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning(
                "SessionHealthChecker: failed to send admin message: %s",
                exc,
            )

                                                                      

    def format_status_text(self) -> str:
                                                                                 
        if not self.last_health:
            return sanitize_text(
                "\U0001fa7a *Session Health* \u2014 No checks have run yet."
            )

        lines = ["\U0001fa7a *Session Health Report*\n"]
        for name in ("pyrogram", "telethon"):
            r = self.last_health.get(name)
            if r is None:
                lines.append(f"\u2022 *{name.capitalize()}*: Not configured")
                continue

            status_emoji = "\u2705" if r.get("alive") else "\u274c"
            lines.append(f"{status_emoji} *{name.capitalize()}*")
            lines.append(f"   Alive: `{r.get('alive')}`")
            lines.append(f"   Latency: `{r.get('latency_ms', 'N/A')} ms`")
            lines.append(
                f"   Source: {source_label(r.get('source', 'unknown'))}"
            )
            phone = self._mask_phone(r.get("phone"))
            if phone:
                lines.append(f"   Phone: `{phone}`")
            if r.get("dc_id"):
                lines.append(f"   DC: `{r['dc_id']}`")
            if r.get("error"):
                lines.append(f"   Error: `{_safe_code_span(r['error'])}`")
            lines.append("")

        lines.append(
            "\U0001f504 Check interval: `{}s`\n"
            "\U0001f514 Admin alerts: `{}`".format(
                self.check_interval,
                "Enabled" if self.admin_user_id else "Disabled",
            )
        )
        return sanitize_text("\n".join(lines))


                                                                        
                                                          
                                                                        
_session_healthchecker: SessionHealthChecker | None = None


def get_session_healthchecker() -> SessionHealthChecker:
\
\
\
       
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
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
       
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
                                            
    checker = get_session_healthchecker()
    checker.stop()
