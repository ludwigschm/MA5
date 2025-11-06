"""Generate synchronization diagnostics between host CSV and Neon events."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Mapping, Optional

import matplotlib

from core.offset_sync import dev_to_host


matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402  (matplotlib backend must be set first)


_TARGET_EVENT = "fixation_flash"
_SYNC_EVENT = "sync.flash_beep"
_DEVICES = ("vp1", "vp2")


@dataclass
class HostEvent:
    index: int
    timestamp_ns: int
    raw: Mapping[str, object]


@dataclass
class SyncEvent:
    index: int
    host_times_ns: Mapping[str, int]
    raw: Mapping[str, object]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        dest="csv_path",
        type=Path,
        default=Path("csv_master.csv"),
        help="Pfad zur CSV-Datei mit host-seitigen Events (default: %(default)s)",
    )
    parser.add_argument(
        "--neon",
        dest="neon_path",
        type=Path,
        default=Path("neon_events.jsonl"),
        help="Pfad zur Neon-Event-Datei (JSON oder JSONL)",
    )
    parser.add_argument(
        "--report",
        dest="report_path",
        type=Path,
        default=Path("alignment_report.json"),
        help="Zielpfad für den Alignment-Report (default: %(default)s)",
    )
    parser.add_argument(
        "--artifacts-dir",
        dest="artifacts_dir",
        type=Path,
        default=Path("artifacts"),
        help="Verzeichnis für erzeugte Artefakte (default: %(default)s)",
    )
    return parser.parse_args()


def _load_csv_events(path: Path) -> List[HostEvent]:
    if not path.exists():
        raise FileNotFoundError(f"CSV-Datei nicht gefunden: {path}")

    events: List[HostEvent] = []
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for idx, row in enumerate(reader, start=1):
            if not row:
                continue
            event = (row.get("event") or "").strip().lower()
            if event != _TARGET_EVENT:
                continue

            raw_ts = row.get("t_host_ns") or row.get("timestamp_ns")
            if raw_ts is None:
                raise KeyError(
                    "CSV-Eintrag ohne 't_host_ns' oder 'timestamp_ns' entdeckt"
                )

            try:
                t_host_ns = int(raw_ts)
            except ValueError as exc:  # pragma: no cover - defensive
                raise ValueError(
                    f"Ungültiger Zeitstempel '{raw_ts}' für Event in Zeile {idx}"
                ) from exc

            events.append(HostEvent(index=len(events) + 1, timestamp_ns=t_host_ns, raw=row))

    if not events:
        raise ValueError(
            f"Keine Events '{_TARGET_EVENT}' in CSV {path} gefunden"
        )

    events.sort(key=lambda entry: entry.timestamp_ns)
    for new_index, event in enumerate(events, start=1):
        event.index = new_index
    return events


def _load_json_lines(path: Path) -> Iterator[Mapping[str, object]]:
    with path.open("r", encoding="utf-8") as fp:
        content = fp.read()

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        for line_no, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Ungültiges JSON in {path} Zeile {line_no}: {exc}"
                ) from exc
    else:
        if isinstance(parsed, list):
            for entry in parsed:
                if isinstance(entry, Mapping):
                    yield entry
        elif isinstance(parsed, Mapping):
            yield parsed
        else:  # pragma: no cover - defensive
            raise ValueError(f"Unerwartetes JSON-Format in {path}")


def _extract_device_payloads(event: Mapping[str, object]) -> Mapping[str, Mapping[str, object]]:
    devices = event.get("devices")
    if isinstance(devices, Mapping):
        return {k: v for k, v in devices.items() if isinstance(v, Mapping)}

    payload = event.get("payload")
    if isinstance(payload, Mapping):
        inner_devices = payload.get("devices")
        if isinstance(inner_devices, Mapping):
            return {k: v for k, v in inner_devices.items() if isinstance(v, Mapping)}

    aggregated: Dict[str, Mapping[str, object]] = {}
    candidates: List[Mapping[str, object]] = []

    if isinstance(payload, list):
        candidates.extend(item for item in payload if isinstance(item, Mapping))

    candidates.append(event)

    for candidate in candidates:
        device = candidate.get("device")
        if isinstance(device, str) and device.lower() in _DEVICES:
            aggregated[device.lower()] = candidate

    return aggregated


def _extract_timestamp_ns(payload: Mapping[str, object]) -> Optional[int]:
    for key in ("t_dev_ns", "t_device_ns", "timestamp_ns"):
        if key not in payload:
            continue
        value = payload[key]
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            raise ValueError(f"Zeitstempel '{key}' konnte nicht als int gelesen werden")
    return None


def _load_sync_events(path: Path) -> List[SyncEvent]:
    if not path.exists():
        raise FileNotFoundError(f"Neon-Event-Datei nicht gefunden: {path}")

    events: List[SyncEvent] = []
    for entry in _load_json_lines(path):
        event_name = str(entry.get("event") or "").strip().lower()
        if event_name != _SYNC_EVENT:
            continue

        payloads = _extract_device_payloads(entry)
        if not payloads:
            continue

        host_times: Dict[str, int] = {}
        for device, payload in payloads.items():
            timestamp = _extract_timestamp_ns(payload)
            if timestamp is None:
                continue
            try:
                host_times[device] = dev_to_host(timestamp, device)
            except KeyError:
                # Offset unknown for this device; skip it.
                continue

        if host_times:
            events.append(
                SyncEvent(index=len(events) + 1, host_times_ns=host_times, raw=entry)
            )

    if not events:
        raise ValueError(
            f"Keine Sync-Events '{_SYNC_EVENT}' in {path} gefunden"
        )

    events.sort(key=lambda entry: min(entry.host_times_ns.values()))
    for new_index, event in enumerate(events, start=1):
        event.index = new_index
    return events


def _pair_events(host_events: List[HostEvent], sync_events: List[SyncEvent]) -> List[tuple[HostEvent, SyncEvent]]:
    count = min(len(host_events), len(sync_events))
    if count == 0:
        raise ValueError("Keine passenden Events für den Vergleich gefunden")
    return list(zip(host_events[:count], sync_events[:count]))


def _difference_ms(host_ts_ns: int, sync_ts_ns: int) -> float:
    return (sync_ts_ns - host_ts_ns) / 1_000_000.0


def _event_label(idx: int, total: int) -> str:
    if idx == 0:
        return "start"
    if idx == total - 1:
        return "end"
    if total == 3 and idx == 1:
        return "mid"
    if total > 3 and idx == total // 2:
        return "mid"
    return f"p{idx + 1}"


def _build_report(pairs: List[tuple[HostEvent, SyncEvent]]) -> Dict[str, object]:
    per_event: List[Dict[str, object]] = []
    differences: List[float] = []

    for pair_index, (host_event, sync_event) in enumerate(pairs):
        label = _event_label(pair_index, len(pairs))
        entry: Dict[str, object] = {
            "index": host_event.index,
            "label": label,
            "t_host_ns": host_event.timestamp_ns,
            "neon_host_times_ns": sync_event.host_times_ns,
            "differences_ms": {},
        }
        for device in _DEVICES:
            sync_ts = sync_event.host_times_ns.get(device)
            diff = None
            if sync_ts is not None:
                diff = _difference_ms(host_event.timestamp_ns, sync_ts)
                differences.append(diff)
            entry["differences_ms"][device] = diff
        per_event.append(entry)

    if not differences:
        raise ValueError("Keine Differenzen berechnet – fehlen Offsets?")

    return {
        "n_events": len(per_event),
        "min_diff_ms": min(differences),
        "mean_diff_ms": statistics.fmean(differences),
        "max_diff_ms": max(differences),
        "per_event": per_event,
    }


def _plot_differences(pairs: List[tuple[HostEvent, SyncEvent]], *, artifacts_dir: Path) -> Path:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    plot_path = artifacts_dir / "plots_offset.png"

    x_positions: List[int] = []
    averages: List[float] = []
    labels: List[str] = []

    for idx, (host_event, sync_event) in enumerate(pairs):
        diffs = [
            _difference_ms(host_event.timestamp_ns, sync_event.host_times_ns[device])
            for device in _DEVICES
            if device in sync_event.host_times_ns
        ]
        if not diffs:
            continue
        x_positions.append(idx)
        averages.append(statistics.fmean(diffs))
        labels.append(_event_label(idx, len(pairs)))

    if not averages:
        raise ValueError("Keine Daten zum Plotten verfügbar")

    mean_value = statistics.fmean(averages)

    plt.figure(figsize=(8, 4.5))
    plt.axhline(mean_value, color="tab:gray", linestyle="--", label=f"Mittelwert ({mean_value:.2f} ms)")
    plt.scatter(x_positions, averages, color="tab:blue", zorder=3)
    for x, y, label in zip(x_positions, averages, labels):
        plt.text(x, y, label, ha="center", va="bottom", fontsize=9)

    plt.title("Sync-Abweichung: Host vs. Neon (ms)")
    plt.xlabel("Sync-Punkte")
    plt.ylabel("Differenz (ms)")
    plt.grid(True, axis="y", linestyle=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(plot_path, dpi=150)
    plt.close()

    return plot_path


def main() -> None:
    args = _parse_args()

    host_events = _load_csv_events(args.csv_path)
    sync_events = _load_sync_events(args.neon_path)
    paired = _pair_events(host_events, sync_events)

    report = _build_report(paired)
    report_path = args.report_path.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, ensure_ascii=False)

    plot_path = _plot_differences(paired, artifacts_dir=args.artifacts_dir.resolve())

    print(f"Report geschrieben nach: {report_path}")
    print(f"Plot gespeichert nach: {plot_path}")


if __name__ == "__main__":  # pragma: no cover - script entry point
    main()
