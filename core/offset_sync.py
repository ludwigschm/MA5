"""Offset synchronization utilities for VP devices."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Mapping

_VALID_DEVICES = {"vp1", "vp2"}

# Location where offsets are persisted. This is relative to the project root.
_OFFSETS_PATH = Path(__file__).resolve().parent.parent / "sync" / "offsets.json"
_SYNC_POINTS_PATH = Path(__file__).resolve().parent.parent / "sync" / "sync_points.jsonl"


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


def _extract_int(mapping: Mapping[str, Any], *, keys: tuple[str, ...]) -> int:
    """Extract an integer value from ``mapping`` using the provided ``keys``."""

    for key in keys:
        if key not in mapping:
            continue
        value = mapping[key]
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise ValueError(f"Expected integer-compatible value for '{key}'") from exc
    raise KeyError(f"Missing one of {keys!r}")


def _extract_devices(event: Mapping[str, Any]) -> Mapping[str, Mapping[str, Any]]:
    """Return a mapping of device identifiers to payload dictionaries."""

    candidates: list[Mapping[str, Any]] = []

    devices = event.get("devices")
    if isinstance(devices, Mapping):
        return devices  # type: ignore[return-value]

    payload = event.get("payload")
    if isinstance(payload, Mapping):
        inner_devices = payload.get("devices")
        if isinstance(inner_devices, Mapping):
            return inner_devices  # type: ignore[return-value]
        candidates.append(payload)
    elif isinstance(payload, list):
        candidates.extend(item for item in payload if isinstance(item, Mapping))

    candidates.append(event)

    aggregated: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        device = candidate.get("device")
        if isinstance(device, str):
            aggregated[device] = candidate

    if aggregated:
        return aggregated

    raise KeyError("No device information found in neon event")


def capture_sync_point(
    host_entry: Mapping[str, Any], neon_event: Mapping[str, Any]
) -> Dict[str, Dict[str, int]]:
    """Capture a synchronisation point from host and device events.

    Args:
        host_entry: CSV entry describing the host-side action. Must contain a
            timestamp in nanoseconds under ``t_host_ns`` or ``timestamp_ns``.
        neon_event: Corresponding Neon event containing device timestamps and
            recording identifiers per device.

    Returns:
        Dictionary containing the synchronisation data per device.
    """

    t_host_ns = _extract_int(host_entry, keys=("t_host_ns", "timestamp_ns"))

    device_payloads = _extract_devices(neon_event)

    results: Dict[str, Dict[str, int]] = {}
    sync_points_for_log: dict[str, Dict[str, Any]] = {}

    for device, payload in device_payloads.items():
        _validate_device(device)
        t_dev_ns = _extract_int(
            payload, keys=("t_dev_ns", "t_device_ns", "timestamp_ns")
        )
        recording_id = payload.get("recording_id")

        sync_point = {"device": device, "t_host_ns": t_host_ns, "t_dev_ns": t_dev_ns}
        if recording_id is not None:
            sync_point["recording_id"] = recording_id

        estimate_offset(sync_point)

        entry: Dict[str, int] = {"t_host_ns": t_host_ns, "t_dev_ns": t_dev_ns}
        results[device] = entry
        sync_points_for_log[device] = sync_point

    log_entry = {
        "host_event": dict(host_entry),
        "neon_event": dict(neon_event),
        "sync_points": sync_points_for_log,
    }

    _SYNC_POINTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _SYNC_POINTS_PATH.open("a", encoding="utf-8") as fp:
        json.dump(log_entry, fp, ensure_ascii=False)
        fp.write("\n")

    return results


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
