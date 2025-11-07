"""Helpers for recording compact QC summaries after a session run."""

from __future__ import annotations

import csv
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, MutableSequence, Sequence

from tabletop.logging.policy import is_critical_event

__all__ = ["emit_mapping_summary", "emit_latency_summary"]


@dataclass(frozen=True)
class _SummaryConfig:
    output_dir: Path
    filename: str
    fieldnames: Sequence[str]

    @property
    def path(self) -> Path:
        return self.output_dir / self.filename


_MAPPING_FIELDS: Sequence[str] = (
    "session_id",
    "mapping_version",
    "avg_mapping_rms_ns",
    "events_total",
    "events_with_t_device",
    "events_without_t_device",
    "share_with_t_device",
    "share_without_t_device",
)

_LATENCY_FIELDS: Sequence[str] = (
    "session_id",
    "critical_event_count",
    "latency_samples",
    "median_latency_ns",
    "p95_latency_ns",
)

_HOST_SEND_KEYS: tuple[str, ...] = (
    "t_host_sent_ns",
    "t_host_send_ns",
    "t_send_ns",
    "t_ui_mono_ns",
    "t_host_ns",
)

_ACK_KEYS: tuple[str, ...] = (
    "t_api_ack_ns",
    "t_ack_ns",
    "api_ack_ns",
    "ack_ns",
)


def emit_mapping_summary(
    session_id: str,
    events: Iterable[Mapping[str, object]],
    *,
    output_dir: Path | str = Path("qc"),
) -> Path:
    """Append a mapping summary row for *session_id* based on *events*.

    The report captures the highest mapping version encountered, the mean
    ``mapping_rms_ns`` and the share of events with or without a mapped
    ``t_device_ns`` timestamp. The data is written to ``qc/mapping_summary.csv``
    by default and is safe to call repeatedly across sessions.
    """

    config = _SummaryConfig(
        output_dir=Path(output_dir),
        filename="mapping_summary.csv",
        fieldnames=_MAPPING_FIELDS,
    )

    events_list = list(events)
    total_events = len(events_list)
    mapping_versions: list[int] = []
    rms_values: list[float] = []
    with_device = 0

    for entry in events_list:
        version = _coerce_int(entry.get("mapping_version"))
        if version is not None:
            mapping_versions.append(version)

        rms = _coerce_number(entry.get("mapping_rms_ns"))
        if rms is not None:
            rms_values.append(float(rms))

        if _coerce_int(entry.get("t_device_ns")) is not None:
            with_device += 1

    without_device = total_events - with_device
    avg_rms_ns = (
        int(round(statistics.mean(rms_values))) if rms_values else ""
    )
    mapping_version = max(mapping_versions) if mapping_versions else ""

    share_with = _format_ratio(with_device, total_events)
    share_without = _format_ratio(without_device, total_events)

    row = {
        "session_id": session_id,
        "mapping_version": mapping_version,
        "avg_mapping_rms_ns": avg_rms_ns,
        "events_total": total_events,
        "events_with_t_device": with_device,
        "events_without_t_device": without_device,
        "share_with_t_device": share_with,
        "share_without_t_device": share_without,
    }

    _append_row(config, row)
    return config.path


def emit_latency_summary(
    session_id: str,
    events: Iterable[Mapping[str, object]],
    *,
    output_dir: Path | str = Path("qc"),
) -> Path:
    """Record latency percentiles for critical events of *session_id*.

    Events without a detectable send/ack timestamp pair are ignored for latency
    statistics while still contributing to the total critical event count. The
    resulting CSV (``qc/latency_summary.csv`` by default) contains median and
    95th percentile latencies in nanoseconds alongside the number of samples
    considered.
    """

    config = _SummaryConfig(
        output_dir=Path(output_dir),
        filename="latency_summary.csv",
        fieldnames=_LATENCY_FIELDS,
    )

    latencies: list[float] = []
    critical_events = 0

    for entry in events:
        action = str(entry.get("action", ""))
        if not is_critical_event(action):
            continue
        critical_events += 1

        latency_value = _coerce_number(entry.get("latency_ns"))
        if latency_value is None:
            send_ns = _extract_timestamp(entry, _HOST_SEND_KEYS)
            ack_ns = _extract_timestamp(entry, _ACK_KEYS)
            if send_ns is not None and ack_ns is not None:
                latency_candidate = ack_ns - send_ns
                if latency_candidate >= 0:
                    latency_value = float(latency_candidate)
        if latency_value is None:
            continue
        latencies.append(float(latency_value))

    median_latency = (
        int(round(statistics.median(latencies))) if latencies else ""
    )
    p95_latency = (
        int(round(_percentile(latencies, 0.95))) if latencies else ""
    )

    row = {
        "session_id": session_id,
        "critical_event_count": critical_events,
        "latency_samples": len(latencies),
        "median_latency_ns": median_latency,
        "p95_latency_ns": p95_latency,
    }

    _append_row(config, row)
    return config.path


def _append_row(config: _SummaryConfig, row: Mapping[str, object]) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    path = config.path
    needs_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=config.fieldnames)
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def _coerce_int(value: object) -> int | None:
    number = _coerce_number(value)
    if number is None:
        return None
    if math.isnan(number):  # pragma: no cover - defensive guard
        return None
    return int(round(number))


def _coerce_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _format_ratio(numerator: int, denominator: int) -> str:
    if denominator <= 0 or numerator < 0:
        return ""
    return f"{numerator / denominator:.3f}"


def _extract_timestamp(
    entry: Mapping[str, object], keys: Sequence[str]
) -> int | None:
    for key in keys:
        candidate = entry.get(key)
        coerced = _coerce_int(candidate)
        if coerced is not None:
            return coerced
    return None


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ValueError("Cannot compute percentile of empty data")
    if not 0.0 <= fraction <= 1.0:
        raise ValueError("Percentile fraction must be between 0 and 1")
    ordered: MutableSequence[float] = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * fraction
    lower = math.floor(rank)
    upper = math.ceil(rank)
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    if lower == upper:
        return lower_value
    weight = rank - lower
    return lower_value + (upper_value - lower_value) * weight
