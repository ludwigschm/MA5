"""Tests for the low-latency dispatch policy."""

from __future__ import annotations

import pytest

from tabletop.logging.policy import (
    CRITICAL_ACTIONS,
    event_priority_for_action,
    is_critical_event,
    should_batch_action,
)


@pytest.fixture()
def clear_policy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env in ("LOW_LATENCY_DISABLED", "LOW_LATENCY_OFF"):
        monkeypatch.delenv(env, raising=False)


def test_is_critical_event_flags_expected_actions(clear_policy_env: None) -> None:
    for action in CRITICAL_ACTIONS:
        assert is_critical_event(action)
        assert is_critical_event(action.upper())


@pytest.mark.parametrize("action", ["", "start_click", "signal_choice", "next_round_click"])
def test_is_critical_event_rejects_non_critical(
    action: str, clear_policy_env: None
) -> None:
    assert not is_critical_event(action)


def test_event_priority_and_batching_follow_policy(clear_policy_env: None) -> None:
    for action in CRITICAL_ACTIONS:
        assert event_priority_for_action(action) == "high"
        assert not should_batch_action(action)
    for action in ("start_click", "signal_choice"):
        assert event_priority_for_action(action) == "normal"
        assert should_batch_action(action)


def test_low_latency_env_override_disables_critical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOW_LATENCY_DISABLED", "1")
    assert not is_critical_event("bet")
    assert event_priority_for_action("bet") == "normal"
    assert should_batch_action("bet")
