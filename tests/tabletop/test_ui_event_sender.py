import csv
from pathlib import Path

from tabletop.logging.ui_events import UIEventLocalLogger, UIEventSender


def _base_event(trial_idx: int) -> dict[str, object]:
    return {
        "session_id": "sess-42",
        "block_idx": 1,
        "trial_idx": trial_idx,
        "actor": "player",
        "player1_id": "p1",
        "action": "bet",
        "t_ui_mono_ns": 1000 + trial_idx,
    }


def test_sequence_number_persisted_in_csv(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    local_logger = UIEventLocalLogger(log_dir, "session-x")
    sender = UIEventSender(local_logger=local_logger)
    try:
        for idx in range(3):
            sender.send_event(_base_event(idx), priority="high")
    finally:
        sender.close()

    csv_path = log_dir / "ui_events_session-x.csv"
    assert csv_path.exists()

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert [row.get("sequence_no") for row in rows] == ["1", "2", "3"]


def test_invalid_action_logged(tmp_path: Path) -> None:
    error_log = Path("logs/event_errors.csv")
    if error_log.exists():
        error_log.unlink()

    sender = UIEventSender()
    try:
        payload = _base_event(1)
        payload["action"] = "not-valid"
        sender.send_event(payload)
    finally:
        sender.close()

    assert error_log.exists()
    with error_log.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) == 1
    row = rows[0]
    assert row["reason"] == "Unsupported action: not-valid"
    assert row["session_id"] == "sess-42"
    assert row["action"] == "not-valid"
    assert row["trial_idx"] == "1"
