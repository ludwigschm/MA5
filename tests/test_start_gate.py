import logging
import threading
import time
from typing import Dict

import pytest

import core.offset_sync as offset_sync
from tabletop.start_gate import StartGate


class _DummyBridge:
    def __init__(self) -> None:
        self.connected: Dict[str, bool] = {"VP1": True, "VP2": True}
        self.sensors: Dict[str, Dict[str, bool]] = {
            "VP1": {},
            "VP2": {},
        }
        self.recording_ids: Dict[str, str] = {}

    def is_connected(self, player: str) -> bool:
        return self.connected.get(player, False)

    def get_sensor_snapshot(self, player: str) -> Dict[str, bool]:
        return dict(self.sensors.get(player, {}))

    def get_recording_id(self, player: str):
        return self.recording_ids.get(player)


@pytest.fixture
def isolated_offsets(tmp_path, monkeypatch):
    offsets_path = tmp_path / "sync" / "offsets.json"
    sync_points_path = tmp_path / "sync" / "sync_points.jsonl"
    monkeypatch.setattr(offset_sync, "_OFFSETS_PATH", offsets_path)
    monkeypatch.setattr(offset_sync, "_SYNC_POINTS_PATH", sync_points_path)
    offset_sync._offsets = {}
    yield
    offset_sync._offsets = {}


def test_start_gate_waits_for_all_conditions(isolated_offsets):
    bridge = _DummyBridge()
    logger = logging.getLogger("test.start_gate")
    gate = StartGate(
        bridge,
        players=("VP1", "VP2"),
        poll_interval=0.01,
        logger=logger,
    )
    ready = threading.Event()
    gate.start(ready.set)

    time.sleep(0.05)
    assert not ready.is_set()

    for player in ("VP1", "VP2"):
        bridge.sensors[player] = {sensor: True for sensor in gate.required_sensors}
    time.sleep(0.05)
    assert not ready.is_set()

    bridge.recording_ids = {"VP1": "r1", "VP2": "r2"}
    time.sleep(0.05)
    assert not ready.is_set()

    offset_sync._offsets = {"vp1": 0, "vp2": 0}
    assert ready.wait(0.2)
    gate.stop()


def test_start_gate_logs_blockers(caplog, isolated_offsets):
    bridge = _DummyBridge()
    logger = logging.getLogger("tabletop.start_gate")
    caplog.set_level(logging.INFO, logger="tabletop.start_gate")
    gate = StartGate(
        bridge,
        players=("VP1", "VP2"),
        poll_interval=0.01,
        logger=logger,
    )
    ready = threading.Event()
    gate.start(ready.set)

    for _ in range(100):
        if any("START-GATE waiting" in record.message for record in caplog.records):
            break
        time.sleep(0.01)

    assert any(
        "START-GATE waiting" in record.message and "sensors" in record.message
        for record in caplog.records
    )

    for player in ("VP1", "VP2"):
        bridge.sensors[player] = {sensor: True for sensor in gate.required_sensors}
    bridge.recording_ids = {"VP1": "r1", "VP2": "r2"}
    offset_sync._offsets = {"vp1": 0, "vp2": 0}
    assert ready.wait(0.2)
    gate.stop()
