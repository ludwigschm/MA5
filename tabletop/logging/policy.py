"""Low-latency policy helpers for UI event dispatch."""

from __future__ import annotations

from typing import Final

from core.events import Priority
from tabletop.utils.runtime import is_low_latency_disabled

__all__ = [
    "CRITICAL_ACTIONS",
    "is_critical_event",
    "event_priority_for_action",
    "should_batch_action",
]

CRITICAL_ACTIONS: Final[frozenset[str]] = frozenset(
    {
        "card_flip",
        "bet",
        "call",
        "fold",
        "phase_transition",
    }
)


def _normalise_action(action: str | None) -> str:
    if not action:
        return ""
    return action.strip().lower()


def is_critical_event(action: str) -> bool:
    """Return ``True`` for actions that require low-latency handling."""

    if is_low_latency_disabled():
        return False
    return _normalise_action(action) in CRITICAL_ACTIONS


def event_priority_for_action(action: str) -> Priority:
    """Return the dispatch priority for *action* according to the policy."""

    return "high" if is_critical_event(action) else "normal"


def should_batch_action(action: str) -> bool:
    """Return whether *action* should be batched when sent to the cloud."""

    return not is_critical_event(action)
