"""Collect diagnostics about the local cloud event environment."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping

from cloud.client import append_only_mode
from core.config import (
    CLOUD_SESSION_ID_REQUIRED,
    EVENT_BATCH_SIZE,
    EVENT_BATCH_WINDOW_MS,
    LOW_LATENCY_DISABLED,
)

log = logging.getLogger(__name__)

_FLAG_ENV_MAP: Mapping[str, str] = {
    "SENDE_UPSERT": "upsert",
    "SENDE_ALLOW_UPSERT": "upsert",
    "SENDE_MERGE": "merge",
    "SENDE_ALLOW_MERGE": "merge",
    "SENDE_BATCHING": "batching",
    "SENDE_ENABLE_BATCHING": "batching",
}

_DIAGNOSTIC_DIR = Path("diagnostics")


def _read_bool(name: str) -> tuple[bool | None, str | None]:
    value = os.getenv(name)
    if value is None:
        return None, None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True, value
    if normalized in {"0", "false", "no", "off"}:
        return False, value
    return None, value


def _collect_sdk_flags() -> Dict[str, MutableMapping[str, Any]]:
    flags: Dict[str, MutableMapping[str, Any]] = {}
    for env_name, alias in _FLAG_ENV_MAP.items():
        effective, raw = _read_bool(env_name)
        if effective is None and raw is None:
            continue
        entry = flags.setdefault(alias, {"sources": {}, "effective": None})
        entry["sources"][env_name] = raw if raw is not None else ""
        if effective is not None:
            current = entry.get("effective")
            if current is None:
                entry["effective"] = effective
            else:
                entry["effective"] = bool(current or effective)
    return flags


def _collect_project_flags() -> Dict[str, Any]:
    return {
        "CLOUD_SESSION_ID_REQUIRED": CLOUD_SESSION_ID_REQUIRED,
        "EVENT_BATCH_SIZE": EVENT_BATCH_SIZE,
        "EVENT_BATCH_WINDOW_MS": EVENT_BATCH_WINDOW_MS,
        "LOW_LATENCY_DISABLED": LOW_LATENCY_DISABLED,
    }


def _collect_firmware_versions() -> Dict[str, str]:
    firmware: Dict[str, str] = {}
    for key, value in os.environ.items():
        if key.endswith("_FIRMWARE_VERSION") and value:
            firmware[key] = value
    legacy = os.getenv("FIRMWARE_VERSION")
    if legacy:
        firmware.setdefault("FIRMWARE_VERSION", legacy)
    return firmware


def gather_diagnostics() -> Dict[str, Any]:
    sdk_version = (
        os.getenv("SENDE_SDK_VERSION")
        or os.getenv("SDK_VERSION")
        or os.getenv("SENDE_CLIENT_VERSION")
    )
    flags = _collect_sdk_flags()
    if flags.get("upsert", {}).get("effective"):
        log.warning("Suspicious flag detected: upsert enabled")
    if flags.get("merge", {}).get("effective"):
        log.warning("Suspicious flag detected: merge enabled")
    data: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "sdk_version": sdk_version or "unknown",
        "append_only_mode": append_only_mode,
        "sdk_flags": flags,
        "project_flags": _collect_project_flags(),
        "firmware_versions": _collect_firmware_versions(),
    }
    batching = flags.get("batching", {}).get("effective")
    if batching:
        log.warning("Suspicious flag detected: batching enabled")
    return data


def write_reports(data: Mapping[str, Any], directory: Path = _DIAGNOSTIC_DIR) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "cloud_env.json"
    txt_path = directory / "cloud_env.txt"

    with json_path.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2, sort_keys=True)

    lines = [
        f"Timestamp: {data.get('timestamp', 'unknown')}",
        f"SDK-Version: {data.get('sdk_version', 'unknown')}",
        f"Append-only-Modus: {'aktiv' if data.get('append_only_mode') else 'deaktiviert'}",
        "",
        "SDK-Flags:",
    ]

    sdk_flags = data.get("sdk_flags", {})
    if isinstance(sdk_flags, Mapping) and sdk_flags:
        for alias, details in sorted(sdk_flags.items()):
            if not isinstance(details, Mapping):
                continue
            effective = details.get("effective")
            sources = details.get("sources", {})
            source_str = ", ".join(f"{k}={v}" for k, v in sorted(sources.items()))
            lines.append(f"  - {alias}: {effective!r} ({source_str})")
    else:
        lines.append("  - keine Flags erkannt")

    lines.append("")
    lines.append("Projekt- und Pipeline-Flags:")
    for name, value in sorted(_collect_project_flags().items()):
        lines.append(f"  - {name}={value}")

    firmware = data.get("firmware_versions", {})
    lines.append("")
    lines.append("Firmware-Versionen:")
    if isinstance(firmware, Mapping) and firmware:
        for name, value in sorted(firmware.items()):
            lines.append(f"  - {name}: {value}")
    else:
        lines.append("  - keine Angaben")

    with txt_path.open("w", encoding="utf-8") as fp:
        fp.write("\n".join(lines) + "\n")

    return json_path, txt_path


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    data = gather_diagnostics()
    write_reports(data)


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()
