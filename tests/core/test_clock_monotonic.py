"""Tests for the central monotonic clock abstraction."""

from __future__ import annotations

import pytest

from core.clock import Clock


def test_clock_now_ns_is_monotonic() -> None:
    first = Clock.now_ns()
    second = Clock.now_ns()

    assert second >= first


def test_clock_now_ns_advances() -> None:
    first = Clock.now_ns()
    second = first

    for _ in range(1_000):
        second = Clock.now_ns()
        if second > first:
            break
    else:  # pragma: no cover - defensive in case clock resolution is poor
        pytest.fail("Clock did not advance within the expected iterations")

    assert second - first > 0
