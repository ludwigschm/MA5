"""Quick stability check for device offsets based on captured sync points.

The script inspects the ``sync/sync_points.jsonl`` log that is produced by
``core.offset_sync.capture_sync_point``.  It compares the offsets of additional
sync points (e.g. middle and end of a recording session) against the offset of
the first sync point to reveal potential clock drift.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, MutableMapping, Optional


_DEVICES = ("vp1", "vp2")
_THRESHOLD_MS = 15.0


@dataclass
class _SyncPoint:
    index: int
    raw: Mapping[str, object]
    host_event: Optional[Mapping[str, object]]
    offsets_ns: Dict[str, Optional[int]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log",
        dest="log_path",
        type=Path,
        default=Path("sync/sync_points.jsonl"),
        help="Pfad zur sync_points.jsonl (default: %(default)s)",
    )
    parser.add_argument(
        "--start-index",
        dest="start_index",
        type=int,
        default=1,
        help="1-basierter Index für den Start-Sync-Punkt (default: %(default)s)",
    )
    parser.add_argument(
        "--middle-index",
        dest="middle_index",
        type=int,
        default=None,
        help="Optionaler 1-basierter Index für den Mittel-Sync-Punkt",
    )
    parser.add_argument(
        "--end-index",
        dest="end_index",
        type=int,
        default=None,
        help="Optionaler 1-basierter Index für den End-Sync-Punkt",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        type=Path,
        default=Path("qc_report.json"),
        help="Pfad zur Ausgabe-Datei (default: %(default)s)",
    )
    return parser.parse_args()


def _load_json_lines(path: Path) -> Iterable[Mapping[str, object]]:
    with path.open("r", encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Ungültiges JSON in {path} Zeile {line_no}: {exc}") from exc


def _ensure_index(name: str, value: int, total: int) -> int:
    if value < 1:
        raise ValueError(f"{name} muss >= 1 sein")
    if value > total:
        raise ValueError(f"{name} ({value}) liegt außerhalb des Bereichs (max {total})")
    return value


def _extract_offsets(sync_point: Mapping[str, object]) -> Dict[str, Optional[int]]:
    offsets: Dict[str, Optional[int]] = {}
    devices = sync_point.get("sync_points")
    if not isinstance(devices, Mapping):
        raise KeyError("Eintrag enthält keine 'sync_points'")

    for device in _DEVICES:
        payload = devices.get(device)
        if not isinstance(payload, Mapping):
            offsets[device] = None
            continue
        try:
            t_host_ns = int(payload["t_host_ns"])
            t_dev_ns = int(payload["t_dev_ns"])
        except KeyError:
            offsets[device] = None
            continue
        except (TypeError, ValueError):
            offsets[device] = None
            continue
        offsets[device] = t_host_ns - t_dev_ns
    return offsets


def _load_sync_points(path: Path) -> list[_SyncPoint]:
    entries: list[_SyncPoint] = []
    for idx, entry in enumerate(_load_json_lines(path), start=1):
        host_event = entry.get("host_event")
        host_mapping = host_event if isinstance(host_event, Mapping) else None
        offsets = _extract_offsets(entry)
        entries.append(_SyncPoint(index=idx, raw=entry, host_event=host_mapping, offsets_ns=offsets))
    if not entries:
        raise ValueError(f"Keine Sync-Punkte in {path} gefunden")
    return entries


def _delta_ms(reference: Dict[str, Optional[int]], sample: Dict[str, Optional[int]]) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {}
    for device in _DEVICES:
        ref = reference.get(device)
        cur = sample.get(device)
        if ref is None or cur is None:
            result[device] = None
        else:
            result[device] = (cur - ref) / 1_000_000.0
    return result


def _event_name(entry: _SyncPoint) -> str:
    if not entry.host_event:
        return ""
    name = entry.host_event.get("event")
    return str(name) if name is not None else ""


def main() -> None:
    args = _parse_args()

    log_path = args.log_path.resolve()
    if not log_path.exists():
        raise FileNotFoundError(f"sync_points.jsonl nicht gefunden: {log_path}")

    sync_points = _load_sync_points(log_path)
    total = len(sync_points)

    start_idx = _ensure_index("start-index", args.start_index, total)
    start_point = sync_points[start_idx - 1]

    comparisons: list[Dict[str, object]] = []

    indices: MutableMapping[str, Optional[int]] = {
        "middle": args.middle_index,
        "end": args.end_index,
    }

    # Default end index to the last entry if not provided explicitly.
    if indices["end"] is None and total > 1:
        indices["end"] = total

    for label, idx in indices.items():
        if idx is None:
            continue
        resolved = _ensure_index(f"{label}-index", idx, total)
        entry = sync_points[resolved - 1]
        delta = _delta_ms(start_point.offsets_ns, entry.offsets_ns)
        comparisons.append(
            {
                "label": label,
                "index": resolved,
                "event": _event_name(entry),
                "delta_offset_ms": delta,
            }
        )

    delta_end = next((c for c in comparisons if c["label"] == "end"), None)
    delta_offset_ms_vp1 = delta_end["delta_offset_ms"]["vp1"] if delta_end else None
    delta_offset_ms_vp2 = delta_end["delta_offset_ms"]["vp2"] if delta_end else None

    warnings: list[str] = []
    anomalies: list[Dict[str, object]] = []

    for comp in comparisons:
        deltas = comp["delta_offset_ms"]
        for device, value in deltas.items():
            if value is None:
                continue
            if abs(value) > _THRESHOLD_MS:
                message = (
                    f"Offset-Drift über Schwellwert für {device} "
                    f"({comp['label']} @ {value:.3f} ms)"
                )
                warnings.append(message)
                anomalies.append(
                    {
                        "label": comp["label"],
                        "device": device,
                        "delta_offset_ms": round(value, 3),
                        "index": comp["index"],
                        "event": comp["event"],
                    }
                )

    report = {
        "source": str(log_path),
        "start_index": start_idx,
        "start_event": _event_name(start_point),
        "threshold_ms": _THRESHOLD_MS,
        "delta_offset_ms_vp1": None if delta_offset_ms_vp1 is None else round(delta_offset_ms_vp1, 3),
        "delta_offset_ms_vp2": None if delta_offset_ms_vp2 is None else round(delta_offset_ms_vp2, 3),
        "comparisons": [
            {
                "label": comp["label"],
                "index": comp["index"],
                "event": comp["event"],
                "delta_offset_ms": {
                    device: (None if value is None else round(value, 3))
                    for device, value in comp["delta_offset_ms"].items()
                },
            }
            for comp in comparisons
        ],
        "warnings": warnings,
        "anomalies": anomalies,
    }

    output_path = args.output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
