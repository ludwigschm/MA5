"""Utilities for accessing a monotonic host clock."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone


class Clock:
    """Centralised monotonic clock helper.

    The clock is anchored to the host's monotonic clock to prevent jumps when the
    system time changes. A reference UTC timestamp captured at import time is
    used to derive human-readable representations.
    """

    _lock = threading.Lock()
    _anchor_mono_ns = time.monotonic_ns()
    _anchor_utc = datetime.now(timezone.utc)

    @classmethod
    def now_ns(cls) -> int:
        """Return the current time in nanoseconds based on a monotonic clock."""

        return time.monotonic_ns()

    @classmethod
    def _ns_to_datetime(cls, monotonic_ns: int) -> datetime:
        """Translate a monotonic timestamp to an absolute UTC datetime."""

        with cls._lock:
            delta_ns = monotonic_ns - cls._anchor_mono_ns
            delta = timedelta(microseconds=delta_ns // 1000)
            return cls._anchor_utc + delta

    @classmethod
    def ns_to_utc_iso(cls, monotonic_ns: int) -> str:
        """Convert a monotonic timestamp to an ISO formatted UTC string."""

        return cls._ns_to_datetime(monotonic_ns).isoformat()

    @classmethod
    def ns_to_local_str(cls, monotonic_ns: int, fmt: str) -> str:
        """Format a monotonic timestamp using the local timezone."""

        return cls._ns_to_datetime(monotonic_ns).astimezone().strftime(fmt)


__all__ = ["Clock"]

