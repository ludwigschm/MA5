import csv
import json
import threading
import time
from pathlib import Path
from typing import List

from core.events import CloudClient


def _base_event(**overrides):
    event = {
        "session_id": "sess-1",
        "block_idx": 0,
        "trial_idx": 1,
        "actor": "player",
        "player1_id": "p1",
        "action": "bet",
        "t_ui_mono_ns": 1234567890,
    }
    event.update(overrides)
    return event


class _TransportSpy:
    def __init__(self) -> None:
        self.calls: List[str] = []
        self.threads: List[str] = []
        self._lock = threading.Lock()

    def __call__(self, payload: str) -> None:
        with self._lock:
            self.calls.append(payload)
            self.threads.append(threading.current_thread().name)


def _wait_for_calls(spy: _TransportSpy, expected: int, timeout: float = 0.5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with spy._lock:
            if len(spy.calls) >= expected:
                return
        time.sleep(0.005)
    raise AssertionError(f"Timed out waiting for {expected} transport calls")


def test_high_priority_triggers_immediate_send():
    spy = _TransportSpy()
    client = CloudClient(spy, batch_window_s=0.5, batch_size=5)
    try:
        client.send_event(_base_event(trial_idx=5), priority="high")
        _wait_for_calls(spy, 1)
        sent = json.loads(spy.calls[0])
        assert len(sent) == 1
        assert sent[0]["trial_idx"] == 5
    finally:
        client.close()


def test_normal_priority_batches_after_window():
    spy = _TransportSpy()
    client = CloudClient(spy, batch_window_s=0.05, batch_size=10)
    try:
        client.send_event(_base_event(trial_idx=2))
        client.send_event(_base_event(trial_idx=3))
        assert spy.calls == []
        time.sleep(0.08)
        assert len(spy.calls) == 1
        payload = json.loads(spy.calls[0])
        assert [item["trial_idx"] for item in payload] == [2, 3]
    finally:
        client.close()


def test_payload_limited_to_whitelist_fields():
    spy = _TransportSpy()
    client = CloudClient(spy, batch_window_s=0.5, batch_size=5)
    try:
        client.send_event(
            _base_event(
                t_device_ns=99,
                t_device_vp1_ns=101,
                t_device_vp2_ns=202,
                mapping_version=7,
                mapping_confidence=0.9,
                mapping_rms_ns=42,
                t_utc_iso="2024-01-01T00:00:00Z",
            ),
            priority="high",
        )
        _wait_for_calls(spy, 1)
        sent = json.loads(spy.calls[0])[0]
        assert set(sent.keys()) == {
            "session_id",
            "block_idx",
            "trial_idx",
            "actor",
            "player1_id",
            "action",
            "t_ui_mono_ns",
            "t_device_ns",
            "mapping_version",
            "mapping_confidence",
        }
        assert "mapping_rms_ns" not in sent
        assert "t_utc_iso" not in sent
    finally:
        client.close()


def test_invalid_event_logged_and_not_sent():
    error_log = Path("logs/event_errors.csv")
    if error_log.exists():
        error_log.unlink()
    spy = _TransportSpy()
    client = CloudClient(spy, batch_window_s=0.05, batch_size=5)
    try:
        client.send_event(
            {
                "session_id": "sess-2",
                "block_idx": 1,
                "trial_idx": 2,
                "actor": "player",
                "player1_id": "p1",
                "t_ui_mono_ns": 42,
            }
        )
        time.sleep(0.02)
        assert spy.calls == []
    finally:
        client.close()

    assert error_log.exists()
    with error_log.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) == 1
    row = rows[0]
    assert row["reason"] == "Missing required field: action"
    assert row["session_id"] == "sess-2"
    assert row["block_idx"] == "1"
    assert row["trial_idx"] == "2"
    assert row["actor"] == "player"
    assert row["player1_id"] == "p1"
    assert row["action"] == ""
    assert row["t_ui_mono_ns"] == "42"


def test_high_priority_events_preserve_order_and_thread():
    spy = _TransportSpy()
    client = CloudClient(spy, batch_window_s=0.5, batch_size=5)
    try:
        total = 8
        for idx in range(total):
            client.send_event(_base_event(trial_idx=idx), priority="high")

        _wait_for_calls(spy, total)

        payloads = [json.loads(call)[0]["trial_idx"] for call in spy.calls]
        assert payloads == list(range(total))
        assert len(set(spy.threads)) == 1
    finally:
        client.close()
