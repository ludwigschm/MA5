"""Utilities for recording event validation errors."""

from __future__ import annotations

import csv
import logging
import threading
from pathlib import Path
from typing import Any, Mapping

_ERROR_LOG_PATH = Path("logs/event_errors.csv")
_ERROR_FIELDS = (
    "reason",
    "session_id",
    "block_idx",
    "trial_idx",
    "actor",
    "player1_id",
    "action",
    "t_ui_mono_ns",
    "t_utc_iso",
)

_lock = threading.Lock()
_log = logging.getLogger(__name__)


def _sanitize_value(value: Any) -> Any:
    if value is None:
        return ""
    return value


def _ensure_header(handle: Any, *, write_header: bool) -> csv.DictWriter:
    writer = csv.DictWriter(handle, fieldnames=_ERROR_FIELDS)
    if write_header:
        writer.writeheader()
    return writer


def log_event_error(reason: str, payload: Mapping[str, Any]) -> None:
    """Append a validation error entry for *payload* to the CSV log."""

    sanitized_reason = reason.strip() if isinstance(reason, str) else ""
    if not sanitized_reason:
        sanitized_reason = "unknown"
    row = {field: "" for field in _ERROR_FIELDS}
    row["reason"] = sanitized_reason
    for field in _ERROR_FIELDS[1:]:
        if field in payload:
            row[field] = _sanitize_value(payload[field])

    try:
        with _lock:
            _ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            file_exists = _ERROR_LOG_PATH.exists()
            with _ERROR_LOG_PATH.open("a", encoding="utf-8", newline="") as handle:
                writer = _ensure_header(handle, write_header=not file_exists)
                writer.writerow(row)
    except Exception:  # pragma: no cover - defensive logging
        _log.exception("Failed to persist event validation error")


def reason_from_exception(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


__all__ = ["log_event_error", "reason_from_exception"]

