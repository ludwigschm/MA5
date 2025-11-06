from __future__ import annotations

"""Align host-side CSV events with Neon device timelines.

The script consumes a CSV export containing host timestamps and mirrors each
entry into the Neon device timelines using the persisted offsets.  The output
is a ``.jsonl`` file where every line contains the host timestamp, both device
timestamps and the most recent recording identifier observed for each device in
the runtime state.
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, Mapping, MutableMapping, Optional

from core.offset_sync import host_to_dev

_DEVICES = ("vp1", "vp2")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="csv_master.csv",
        type=Path,
        help="Pfad zur Eingabe-CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--runtime-state",
        dest="runtime_state",
        type=Path,
        default=None,
        help="Pfad zum Runtime-State (JSON oder JSONL)",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        type=Path,
        default=Path("aligned_events.jsonl"),
        help="Zielpfad für die ausgerichteten Events (default: %(default)s)",
    )
    return parser.parse_args()


def _candidate_runtime_state_paths(base_dir: Path) -> Iterable[Path]:
    candidates = (
        base_dir / "runtime_state.json",
        base_dir / "runtime_state.jsonl",
        base_dir / "sync" / "runtime_state.json",
        base_dir / "sync" / "runtime_state.jsonl",
        base_dir / "tabletop" / "sync" / "runtime_state.json",
        base_dir / "tabletop" / "sync" / "runtime_state.jsonl",
    )
    for path in candidates:
        yield path


def _resolve_runtime_state_path(explicit: Optional[Path]) -> Path:
    if explicit is not None:
        if not explicit.exists():
            raise FileNotFoundError(f"Runtime-State nicht gefunden: {explicit}")
        return explicit

    base_dir = Path.cwd()
    for path in _candidate_runtime_state_paths(base_dir):
        if path.exists():
            return path

    raise FileNotFoundError(
        "Runtime-State konnte nicht gefunden werden. Bitte Pfad mit --runtime-state angeben."
    )


def _load_json_lines(path: Path) -> Iterable[Mapping[str, object]]:
    with path.open("r", encoding="utf-8") as fp:
        content = fp.read()
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
    else:
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, Mapping):
                    yield item
        elif isinstance(parsed, Mapping):
            yield parsed
        else:
            raise ValueError(f"Unerwartetes JSON-Format in {path}")


def _extract_recording_ids(data: Iterable[Mapping[str, object]]) -> Dict[str, str]:
    latest: Dict[str, str] = {}

    def update_from_mapping(mapping: Mapping[str, object]) -> None:
        device = mapping.get("device")
        if isinstance(device, str) and device.lower() in _DEVICES:
            rec = mapping.get("recording_id")
            if rec is not None:
                latest[device.lower()] = str(rec)
        for key, value in mapping.items():
            lowered = key.lower()
            if lowered in _DEVICES:
                if isinstance(value, Mapping):
                    rec = value.get("recording_id")
                    if rec is not None:
                        latest[lowered] = str(rec)
                    update_from_mapping(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, Mapping):
                            update_from_mapping(item)
            if isinstance(value, Mapping):
                update_from_mapping(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, Mapping):
                        update_from_mapping(item)

    for entry in data:
        update_from_mapping(entry)

    missing = [device for device in _DEVICES if device not in latest]
    if missing:
        raise KeyError(
            "Keine recording_id im Runtime-State für: " + ", ".join(sorted(missing))
        )

    return latest


def _parse_payload(raw: str | None) -> object | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _ensure_monotonic(
    previous: MutableMapping[str, Optional[int]], device: str, value: int
) -> None:
    last = previous.get(device)
    if last is not None and value < last:
        raise ValueError(
            f"Zeitordnung verletzt für {device}: {value} < {last}"
        )
    previous[device] = value


def main() -> None:
    args = _parse_args()

    csv_path = args.csv_path.resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV-Datei nicht gefunden: {csv_path}")

    runtime_state_path = _resolve_runtime_state_path(args.runtime_state)
    recording_ids = _extract_recording_ids(_load_json_lines(runtime_state_path))

    output_path = args.output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    previous_world_ns: Dict[str, Optional[int]] = {device: None for device in _DEVICES}

    with csv_path.open("r", encoding="utf-8", newline="") as fp_in, output_path.open(
        "w", encoding="utf-8"
    ) as fp_out:
        reader = csv.DictReader(fp_in)
        for idx, row in enumerate(reader, start=1):
            if not row:
                continue
            try:
                t_host_ns = int(row.get("t_host_ns") or row["timestamp_ns"])
            except KeyError as exc:
                raise KeyError(
                    "Spalte 't_host_ns' fehlt in der CSV"
                ) from exc
            except ValueError as exc:
                raise ValueError(
                    f"Ungültiger t_host_ns in Zeile {idx}: {row.get('t_host_ns')}"
                ) from exc

            event_name = row.get("event") or ""
            payload = _parse_payload(row.get("payload"))

            vp_entries = {}
            for device in _DEVICES:
                t_world_ns = host_to_dev(t_host_ns, device)
                _ensure_monotonic(previous_world_ns, device, t_world_ns)
                vp_entries[device] = {
                    "t_world_ns": t_world_ns,
                    "rec_id": recording_ids[device],
                }

            record: Dict[str, object] = {
                "event": event_name,
                "t_host_ns": t_host_ns,
                "vp1": vp_entries["vp1"],
                "vp2": vp_entries["vp2"],
            }
            if payload is not None:
                record["payload"] = payload

            fp_out.write(json.dumps(record, ensure_ascii=False))
            fp_out.write("\n")


if __name__ == "__main__":
    main()
