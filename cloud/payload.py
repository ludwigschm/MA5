"""Utilities for constructing cloud event payloads."""

from __future__ import annotations

from typing import Dict, Optional

from core.config import CLOUD_SESSION_ID_REQUIRED

ALLOWED_ACTIONS = {
    "card_flip",
    "bet",
    "call",
    "fold",
    "phase_transition",
    "timeout",
}

_ALLOWED_ACTORS = {"VP1", "VP2"}


def build_cloud_payload(
    action: str,
    actor: str,
    player1_id: str,
    session_id: Optional[str],
) -> Dict[str, str]:
    """Create a minimal payload suitable for the cloud events API."""

    if not isinstance(action, str):
        raise TypeError("action must be a string")
    if action not in ALLOWED_ACTIONS:
        raise ValueError(f"Unsupported action: {action}")
    if not isinstance(actor, str):
        raise TypeError("actor must be a string")
    if actor not in _ALLOWED_ACTORS:
        raise ValueError(f"Unsupported actor: {actor}")
    if not isinstance(player1_id, str):
        raise TypeError("player1_id must be a string")
    if player1_id not in _ALLOWED_ACTORS:
        raise ValueError(f"Unsupported player1 identifier: {player1_id}")

    payload: Dict[str, str] = {}
    if CLOUD_SESSION_ID_REQUIRED:
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("session_id is required when CLOUD_SESSION_ID_REQUIRED is true")
        payload["session_id"] = session_id

    payload["action"] = action
    payload["actor"] = actor
    payload["player1_id"] = player1_id

    return payload


__all__ = ["ALLOWED_ACTIONS", "build_cloud_payload"]
