"""Offset synchronization utilities for VP devices."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

_VALID_DEVICES = {"vp1", "vp2"}

# Location where offsets are persisted. This is relative to the project root.
_OFFSETS_PATH = Path(__file__).resolve().parent.parent / "sync" / "offsets.json"


def _load_offsets() -> Dict[str, int]:
    """Load offsets from disk, if available."""
    try:
        with _OFFSETS_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid JSON should be rare
        raise ValueError(f"Invalid offsets file {_OFFSETS_PATH}: {exc}") from exc

    return {device: int(offset) for device, offset in data.items()}


_offsets: Dict[str, int] = _load_offsets()


def _save_offsets() -> None:
    """Persist current offsets to disk."""
    _OFFSETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _OFFSETS_PATH.open("w", encoding="utf-8") as f:
        json.dump(_offsets, f, indent=2, sort_keys=True)


def _validate_device(device: str) -> None:
    if device not in _VALID_DEVICES:
        raise ValueError(f"Unknown device '{device}'. Expected one of {_VALID_DEVICES}.")


def estimate_offset(sync_point: Dict[str, int]) -> None:
    """Estimate and persist the offset for a given device.

    Args:
        sync_point: Dictionary containing host and device timestamps (in ns) and the
            device identifier ("vp1" or "vp2").
    """
    try:
        device = sync_point["device"]
        t_host_ns = int(sync_point["t_host_ns"])
        t_dev_ns = int(sync_point["t_dev_ns"])
    except KeyError as exc:
        missing_key = exc.args[0]
        raise KeyError(f"Missing '{missing_key}' in sync_point") from exc

    _validate_device(device)

    offset = t_host_ns - t_dev_ns
    _offsets[device] = offset
    _save_offsets()


def host_to_dev(t_host_ns: int, device: str) -> int:
    """Convert a host timestamp to device time."""
    _validate_device(device)
    if device not in _offsets:
        raise KeyError(f"Offset for device '{device}' is not set.")

    return int(t_host_ns) - _offsets[device]


def dev_to_host(t_dev_ns: int, device: str) -> int:
    """Convert a device timestamp to host time."""
    _validate_device(device)
    if device not in _offsets:
        raise KeyError(f"Offset for device '{device}' is not set.")

    return int(t_dev_ns) + _offsets[device]
