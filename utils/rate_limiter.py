# utils/rate_limiter.py
"""
Rate limiting utilities for Telegram API and bot operations.

Provides:
- RateLimiter: Token bucket algorithm with per-user or global limits
- TelegramAPIRateLimiter: Combined global (30/s) + per-user (1/s) limiter
- ConversionRateLimiter: Per-user hourly limit for media conversions
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, Optional, Tuple

try:
    from utils.job_queue import get_redis
except Exception:
    get_redis = None

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter with configurable capacity and refill rate."""

    def __init__(self, calls_per_second: float = 30, per_user: bool = False):
        """
        Args:
            calls_per_second: Maximum sustained calls per second.
            per_user: If True, rate limit is per-user; otherwise global.
        """
        self.calls_per_second = calls_per_second
        self.per_user = per_user
        self.capacity = max(1.0, calls_per_second)
        initial_tokens = float(self.capacity)
        self.buckets: Dict[str, Tuple[float, float]] = defaultdict(
            lambda: (initial_tokens, time.time())
        )
        self._lock = asyncio.Lock()

    async def acquire(self, user_id: str = "global", tokens: float = 1.0) -> bool:
        """Try to acquire tokens from the bucket.

        Returns True if tokens were available, False if rate limited.
        """
        async with self._lock:
            key = user_id if self.per_user else "global"
            current_tokens, last_time = self.buckets[key]
            now = time.time()
            elapsed = now - last_time

            # Refill tokens based on elapsed time
            new_tokens = min(self.capacity, current_tokens + (elapsed * self.calls_per_second))

            if new_tokens >= tokens:
                self.buckets[key] = (new_tokens - tokens, now)
                return True
            else:
                self.buckets[key] = (new_tokens, now)
                return False

    async def wait_if_needed(self, user_id: str = "global", tokens: float = 1.0) -> float:
        """Wait until tokens are available and acquire them.

        Returns the wait time in seconds (0 if no wait needed).
        """
        start = time.time()
        while not await self.acquire(user_id, tokens):
            await asyncio.sleep(0.01)

        waited = time.time() - start
        if waited > 2.0:
            logger.warning(
                "RateLimiter waited %.2fs for key=%s (tokens=%s)",
                waited, user_id, tokens,
            )
        return waited

    def get_stats(self, user_id: Optional[str] = None) -> Dict:
        """Return rate limiter statistics."""
        stats = {}
        if user_id:
            tokens, last_time = self.buckets.get(user_id, (self.capacity, time.time()))
            tokens_needed = max(0.0, 1.0 - tokens)
            secs = tokens_needed / self.calls_per_second if self.calls_per_second > 0 else float("inf")
            stats[user_id] = {
                "available_tokens": tokens,
                "last_refill": last_time,
                "seconds_until_refill": max(0.0, secs),
            }
        else:
            for key, (tokens, last_time) in self.buckets.items():
                tokens_needed = max(0.0, 1.0 - tokens)
                secs = tokens_needed / self.calls_per_second if self.calls_per_second > 0 else float("inf")
                stats[key] = {
                    "available_tokens": tokens,
                    "last_refill": last_time,
                    "seconds_until_refill": max(0.0, secs),
                }
        return stats


class TelegramAPIRateLimiter:
    """Specialized rate limiter for Telegram Bot API calls.

    Applies both global (30 calls/sec) and per-user (1 call/sec) limits.
    """

    GENERAL_LIMIT = 30
    PER_USER_LIMIT = 1

    def __init__(self):
        self.global_limiter = RateLimiter(self.GENERAL_LIMIT, per_user=False)
        self.per_user_limiter = RateLimiter(self.PER_USER_LIMIT, per_user=True)

    async def acquire(self, user_id: str = "global") -> bool:
        """Check if a call is allowed under both global and per-user limits."""
        global_ok = await self.global_limiter.acquire(tokens=1)
        user_ok = await self.per_user_limiter.acquire(user_id=user_id, tokens=1)
        return global_ok and user_ok

    async def wait_if_needed(self, user_id: str = "global") -> Tuple[float, float]:
        """Wait until both limiters allow a call.

        Returns (global_wait, per_user_wait) in seconds.
        """
        gw = await self.global_limiter.wait_if_needed(tokens=1)
        uw = await self.per_user_limiter.wait_if_needed(user_id=user_id, tokens=1)

        if gw > 2.0 or uw > 2.0:
            logger.warning(
                "TelegramAPIRateLimiter: user=%s global_wait=%.2fs user_wait=%.2fs",
                user_id, gw, uw,
            )
        return gw, uw

    def get_stats(self, user_id: Optional[str] = None) -> Dict:
        return {
            "global": self.global_limiter.get_stats(),
            "per_user": self.per_user_limiter.get_stats(user_id) if user_id else {},
        }


class ConversionRateLimiter:
    """Per-user hourly rate limiter for processing operations."""

    def __init__(self, conversions_per_hour: int = 100):
        self.conversions_per_hour = conversions_per_hour
        self.per_second = conversions_per_hour / 3600
        self.limiter = RateLimiter(self.per_second, per_user=True)
        self.history: Dict[str, list] = defaultdict(list)

    async def can_convert(self, user_id: str) -> Tuple[bool, str]:
        """Check if user can start a conversion (non-consuming)."""
        now = time.time()
        cutoff = now - 3600
        recent = [t for t in self.history.get(user_id, []) if t > cutoff]
        if len(recent) < self.conversions_per_hour:
            return True, "Allowed"
        earliest = min(recent) if recent else now
        wait = max(0.0, (earliest + 3600) - now)
        return False, (
            f"\u274c Rate limit reached ({len(recent)}/{self.conversions_per_hour} per hour)\n"
            f"Please wait {wait:.1f} seconds."
        )

    async def mark_conversion_started(self, user_id: str) -> bool:
        """Consume quota and record the conversion start.

        Returns True if allowed, False if rate limited.
        """
        allowed = await self.limiter.acquire(user_id=user_id, tokens=1)
        if allowed:
            self.history[user_id].append(time.time())
            cutoff = time.time() - 3600
            self.history[user_id] = [t for t in self.history[user_id] if t > cutoff]
            return True
        return False

    def get_user_conversion_count(self, user_id: str) -> int:
        now = time.time()
        cutoff = now - 3600
        return sum(1 for t in self.history.get(user_id, []) if t > cutoff)
