"""Static configuration for cloud event integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import os


def _read_session_id() -> Optional[str]:
    """Return the configured cloud session identifier, if any."""

    value = os.getenv("CLOUD_SESSION_ID")
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@dataclass(frozen=True)
class _CloudConfig:
    """Namespace for cloud-related configuration values."""

    SESSION_ID: Optional[str]


CFG = _CloudConfig(SESSION_ID=_read_session_id())


__all__ = ["CFG"]

