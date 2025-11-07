"""Central configuration constants with environment overrides."""

from __future__ import annotations

import os
from typing import Final


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default


LOW_LATENCY_DISABLED: Final[bool] = _get_bool("LOW_LATENCY_DISABLED", False)
EVENT_BATCH_WINDOW_MS: Final[int] = _get_int("EVENT_BATCH_WINDOW_MS", 0)
EVENT_BATCH_SIZE: Final[int] = _get_int("EVENT_BATCH_SIZE", 20)
QC_RMS_NS_THRESHOLD: Final[int] = _get_int("QC_RMS_NS_THRESHOLD", 5_000)
QC_CONFIDENCE_MIN: Final[float] = _get_float("QC_CONFIDENCE_MIN", 0.9)
CLOUD_SESSION_ID_REQUIRED: Final[bool] = _get_bool("CLOUD_SESSION_ID_REQUIRED", False)

__all__ = [
    "LOW_LATENCY_DISABLED",
    "EVENT_BATCH_WINDOW_MS",
    "EVENT_BATCH_SIZE",
    "QC_RMS_NS_THRESHOLD",
    "QC_CONFIDENCE_MIN",
    "CLOUD_SESSION_ID_REQUIRED",
]
