from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from time import time


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    reset_at: float


class FixedWindowRateLimiter:
    def __init__(self) -> None:
        self._windows: dict[tuple[str, str], tuple[float, int]] = defaultdict(lambda: (0.0, 0))
        self._lock = Lock()

    def check(self, namespace: str, key: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time()
        bucket = (namespace, key)
        with self._lock:
            window_start, used = self._windows[bucket]
            if now >= window_start + window_seconds:
                window_start = now
                used = 0
            if used >= limit:
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=window_start + window_seconds,
                )
            used += 1
            self._windows[bucket] = (window_start, used)
            return RateLimitResult(
                allowed=True,
                remaining=max(limit - used, 0),
                reset_at=window_start + window_seconds,
            )
