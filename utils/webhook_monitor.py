# utils/webhook_monitor.py
"""Webhook heartbeat monitoring for bot reliability and free-tier spin-down prevention."""

import asyncio
import logging
import os
import random
from datetime import datetime
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)


class WebhookMonitor:
    """Monitor webhook health and connectivity."""

    def __init__(self, webhook_url: str, check_interval: int = 300):
        self.webhook_url = webhook_url
        self.check_interval = check_interval
        self.is_healthy = True
        self.last_check = None
        self.check_count = 0
        self.failed_checks = 0
        self.consecutive_failures = 0
        self.monitor_task = None
        self._current_interval = check_interval
        self._max_backoff = max(check_interval * 8, 3600)
        self.local_timeout = int(os.environ.get("WEBHOOK_LOCAL_TIMEOUT", "3"))
        self.external_timeout = int(
            os.environ.get("WEBHOOK_EXTERNAL_TIMEOUT", "10")
        )
        self.last_status_code = None
        self.last_error = None

    async def health_check(self) -> bool:
        try:
            try:
                parsed = urlparse(self.webhook_url)
                local_port = int(os.environ.get("PORT", "8000"))
                local_path = parsed.path or "/"
                local_url = f"http://127.0.0.1:{local_port}{local_path}"
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.head(
                            local_url,
                            timeout=aiohttp.ClientTimeout(
                                total=self.local_timeout
                            ),
                        ) as resp:
                            status = resp.status
                    except Exception:
                        async with session.get(
                            local_url,
                            timeout=aiohttp.ClientTimeout(
                                total=self.local_timeout + 2
                            ),
                        ) as resp:
                            status = resp.status

                    self.check_count += 1
                    self.last_check = datetime.now()
                    self.last_status_code = status
                    if status in (200, 404, 405):
                        self.is_healthy = True
                        self.consecutive_failures = 0
                        self._current_interval = self.check_interval
                        return True
            except Exception:
                pass

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.head(
                        self.webhook_url,
                        timeout=aiohttp.ClientTimeout(
                            total=self.external_timeout
                        ),
                    ) as response:
                        status = response.status
                except Exception:
                    async with session.get(
                        self.webhook_url,
                        timeout=aiohttp.ClientTimeout(
                            total=self.external_timeout + 5
                        ),
                    ) as response:
                        status = response.status

                self.check_count += 1
                self.last_check = datetime.now()
                self.last_status_code = status
                if status in (200, 404, 405):
                    self.is_healthy = True
                    self.consecutive_failures = 0
                    self._current_interval = self.check_interval
                    return True

                if status == 429:
                    self.is_healthy = False
                    self.failed_checks += 1
                    self.consecutive_failures += 1
                    old = self._current_interval
                    self._current_interval = min(
                        old * 2, self._max_backoff
                    ) + random.uniform(0, 5)
                    logger.warning(
                        "Webhook rate-limited (429). Backing off to %.1fs",
                        self._current_interval,
                    )
                    return False

                self.is_healthy = False
                self.failed_checks += 1
                self.consecutive_failures += 1
                return False

        except Exception as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            self.last_error = str(e)
            old = self._current_interval
            self._current_interval = min(
                old * 2, self._max_backoff
            ) + random.uniform(0, 5)
            return False

    async def start_monitoring(self):
        logger.info(
            "Starting webhook monitoring (interval: %ds)", self.check_interval
        )
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self):
        while True:
            try:
                await self.health_check()
                if self.consecutive_failures >= 3:
                    logger.critical(
                        "CRITICAL: Webhook failed %d consecutive checks! Total: %d/%d",
                        self.consecutive_failures,
                        self.failed_checks,
                        self.check_count,
                    )
                await asyncio.sleep(self._current_interval)
            except asyncio.CancelledError:
                logger.info("Webhook monitoring stopped")
                break
            except Exception as e:
                logger.error("Error in monitoring loop: %s", e)
                await asyncio.sleep(self.check_interval)

    async def stop_monitoring(self):
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Webhook monitoring stopped")

    def get_status(self) -> dict:
        return {
            "healthy": self.is_healthy,
            "url": self.webhook_url,
            "last_check": self.last_check.isoformat()
            if self.last_check
            else None,
            "total_checks": self.check_count,
            "failed_checks": self.failed_checks,
            "consecutive_failures": self.consecutive_failures,
            "success_rate": (
                (self.check_count - self.failed_checks)
                / self.check_count
                * 100
                if self.check_count > 0
                else 0
            ),
        }


class WebhookRecoveryManager:
    """Manage webhook recovery and automatic restart."""

    def __init__(
        self,
        bot_application,
        webhook_url: str,
        secret_token: str | None = None,
    ):
        self.application = bot_application
        self.webhook_url = webhook_url
        self.secret_token = secret_token
        self.monitor = WebhookMonitor(webhook_url)
        self.recovery_attempts = 0
        self.max_recovery_attempts = 3

    async def start(self):
        await self.monitor.start_monitoring()
        logger.info("Webhook recovery manager started")

    async def check_and_recover(self) -> bool:
        if self.monitor.is_healthy:
            self.recovery_attempts = 0
            return True

        logger.warning(
            "Attempting webhook recovery (attempt %d/%d)",
            self.recovery_attempts + 1,
            self.max_recovery_attempts,
        )
        try:
            kwargs = {
                "url": self.webhook_url,
                "allowed_updates": [
                    "message",
                    "callback_query",
                    "edited_message",
                ],
            }
            if self.secret_token:
                kwargs["secret_token"] = self.secret_token
            await self.application.bot.set_webhook(**kwargs)
            self.recovery_attempts += 1
            if await self.monitor.wait_until_healthy(timeout=30):
                logger.info("Webhook recovered successfully")
                self.recovery_attempts = 0
                return True
            else:
                logger.warning(
                    "Webhook recovery failed - still not responding"
                )
                return False
        except Exception as e:
            logger.error("Webhook recovery error: %s", e)
            return False

    async def stop(self):
        await self.monitor.stop_monitoring()
        logger.info("Webhook recovery manager stopped")

    def get_stats(self) -> dict:
        status = self.monitor.get_status()
        status["recovery_attempts"] = self.recovery_attempts
        status["max_recovery_attempts"] = self.max_recovery_attempts
        return status
