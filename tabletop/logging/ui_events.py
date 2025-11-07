"""Utilities for dispatching structured UI events to local/remote sinks."""

from __future__ import annotations

import logging
import sqlite3
import threading
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Optional

from core.events import BaseEvent, CloudClient, Priority, validate_base_event
from core.single_writer_logger import SingleWriterLogger

__all__ = ["UIEventLocalLogger", "UIEventSender", "log_mapping_warning"]

log = logging.getLogger(__name__)


_CSV_FIELDS = (
    "session_id",
    "block_idx",
    "trial_idx",
    "actor",
    "player1_id",
    "action",
    "t_ui_mono_ns",
    "t_device_ns",
    "t_device_vp1_ns",
    "t_device_vp2_ns",
    "mapping_version",
    "mapping_confidence",
    "mapping_rms_ns",
    "t_utc_iso",
)

_COLUMN_TYPES = {
    "session_id": "TEXT",
    "block_idx": "INTEGER",
    "trial_idx": "INTEGER",
    "actor": "TEXT",
    "player1_id": "TEXT",
    "action": "TEXT",
    "t_ui_mono_ns": "INTEGER",
    "t_device_ns": "INTEGER",
    "t_device_vp1_ns": "INTEGER",
    "t_device_vp2_ns": "INTEGER",
    "mapping_version": "INTEGER",
    "mapping_confidence": "REAL",
    "mapping_rms_ns": "INTEGER",
    "t_utc_iso": "TEXT",
}

_SQL_COLUMNS = ", ".join(_CSV_FIELDS)
_SQL_PLACEHOLDERS = ", ".join("?" for _ in _CSV_FIELDS)
_MAPPING_WARNINGS_PATH = Path("logs/mapping_warnings.log")


def log_mapping_warning(message: str) -> None:
    """Append *message* to the mapping warnings log with a UTC timestamp."""

    try:
        _MAPPING_WARNINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
        with _MAPPING_WARNINGS_PATH.open("a", encoding="utf-8") as handle:
            handle.write(f"{timestamp} {message}\n")
    except Exception:  # pragma: no cover - best effort logging
        log.debug("Failed to record mapping warning", exc_info=True)


class UIEventLocalLogger:
    """Persist UI base events to CSV and SQLite backends."""

    def __init__(self, log_dir: Path, session_label: str) -> None:
        safe_label = "".join(
            ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in session_label
        )
        if not safe_label:
            safe_label = "session"

        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        base_path = log_dir / f"ui_events_{safe_label}"
        self._csv_logger = SingleWriterLogger(
            base_path.with_suffix(".csv"),
            queue_size=2000,
            batch_size=32,
            flush_interval=0.05,
            logger=log,
        )

        self._db_path = base_path.with_suffix(".sqlite3")
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ui_events(
                  session_id TEXT,
                  block_idx INTEGER,
                  trial_idx INTEGER,
                  actor TEXT,
                  player1_id TEXT,
                  action TEXT,
                  t_ui_mono_ns INTEGER,
                  t_device_ns INTEGER,
                  t_device_vp1_ns INTEGER,
                  t_device_vp2_ns INTEGER,
                  mapping_version INTEGER,
                  mapping_confidence REAL,
                  mapping_rms_ns INTEGER,
                  t_utc_iso TEXT
                )
                """
            )
            self._ensure_schema_locked()
            self._conn.commit()
        self._closed = False

    def log(self, payload: BaseEvent) -> None:
        if self._closed:
            raise RuntimeError("Cannot log UI events after logger has been closed")

        csv_row = {
            key: ("" if payload.get(key) is None else payload.get(key))
            for key in _CSV_FIELDS
        }
        sequence_no = payload.get("sequence_no")
        if sequence_no is not None:
            csv_row["sequence_no"] = sequence_no
        self._csv_logger.log_event(csv_row)

        values = tuple(payload.get(key) for key in _CSV_FIELDS)
        with self._lock:
            self._conn.execute(
                f"INSERT INTO ui_events ({_SQL_COLUMNS}) VALUES ({_SQL_PLACEHOLDERS})",
                values,
            )
            self._conn.commit()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(Exception):
            self._csv_logger.close()
        with self._lock:
            with suppress(Exception):
                self._conn.commit()
                self._conn.close()

    def _ensure_schema_locked(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA table_info(ui_events)")
        existing = {row[1] for row in cur.fetchall()}
        for column, column_type in _COLUMN_TYPES.items():
            if column in existing:
                continue
            self._conn.execute(
                f"ALTER TABLE ui_events ADD COLUMN {column} {column_type}"
            )


class UIEventSender:
    """Validate UI events and forward them to configured sinks."""

    def __init__(
        self,
        *,
        local_logger: Optional[UIEventLocalLogger] = None,
        cloud_client: Optional[CloudClient] = None,
    ) -> None:
        self._local_logger = local_logger
        self._cloud_client = cloud_client
        self._sequence_lock = threading.Lock()
        self._sequence_counters: dict[tuple[str, str], int] = {}

    def send_event(self, payload: BaseEvent, priority: Priority = "normal") -> None:
        try:
            validated = validate_base_event(payload)
        except ValueError:
            log.exception("UI event payload failed validation")
            return

        with self._sequence_lock:
            key = (validated["session_id"], validated["actor"])
            next_sequence = self._sequence_counters.get(key, 0) + 1
            self._sequence_counters[key] = next_sequence

        validated["sequence_no"] = next_sequence  # type: ignore[assignment]

        if self._local_logger is not None:
            try:
                self._local_logger.log(validated)
            except Exception:
                log.exception("Failed to persist UI event locally")

        if self._cloud_client is not None:
            try:
                self._cloud_client.send_event(validated, priority=priority)
            except Exception:
                log.exception("Failed to forward UI event to cloud client")

    def close(self) -> None:
        if self._cloud_client is not None:
            with suppress(Exception):
                self._cloud_client.close()
        if self._local_logger is not None:
            with suppress(Exception):
                self._local_logger.close()
