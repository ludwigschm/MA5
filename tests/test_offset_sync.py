import json
from random import Random

import pytest

import core.offset_sync as offset_sync


@pytest.fixture()
def isolated_offsets(tmp_path, monkeypatch):
    offsets_path = tmp_path / "sync" / "offsets.json"
    sync_points_path = tmp_path / "sync" / "sync_points.jsonl"
    monkeypatch.setattr(offset_sync, "_OFFSETS_PATH", offsets_path)
    monkeypatch.setattr(offset_sync, "_SYNC_POINTS_PATH", sync_points_path)
    offset_sync._offsets = offset_sync._load_offsets()
    yield offsets_path
    offset_sync._offsets = {}


def test_estimate_offset_persists(isolated_offsets):
    offsets_path = isolated_offsets
    offset_sync.estimate_offset({"t_host_ns": 1_000_000, "t_dev_ns": 250_000, "device": "vp1"})
    offset_sync.estimate_offset({"t_host_ns": 2_500_000, "t_dev_ns": 2_000_000, "device": "vp2"})

    with offsets_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert data == {"vp1": 750_000, "vp2": 500_000}


def test_roundtrip_conversion(isolated_offsets):
    offset_sync.estimate_offset({"t_host_ns": 10_000_000, "t_dev_ns": 2_000_000, "device": "vp1"})

    rng = Random(42)
    samples = [rng.randint(0, 1_000_000_000) for _ in range(10)]
    for host_time in samples:
        dev_time = offset_sync.host_to_dev(host_time, "vp1")
        assert offset_sync.dev_to_host(dev_time, "vp1") == host_time

    for dev_time in samples:
        host_time = offset_sync.dev_to_host(dev_time, "vp1")
        assert offset_sync.host_to_dev(host_time, "vp1") == dev_time


def test_unknown_device_raises(isolated_offsets):
    with pytest.raises(ValueError):
        offset_sync.host_to_dev(0, "vp3")

    with pytest.raises(ValueError):
        offset_sync.dev_to_host(0, "vp3")

    with pytest.raises(ValueError):
        offset_sync.estimate_offset({"t_host_ns": 0, "t_dev_ns": 0, "device": "vp3"})


def test_capture_sync_point_persists_offsets_and_logs(isolated_offsets):
    offsets_path = isolated_offsets
    sync_points_path = offsets_path.parent / "sync_points.jsonl"

    host_entry = {"event": "fixation_flash", "t_host_ns": 1_500_000}
    neon_event = {
        "event": "sync.flash_beep",
        "devices": {
            "vp1": {"t_dev_ns": 1_000_000, "recording_id": "rec-1"},
            "vp2": {"t_dev_ns": 1_200_000, "recording_id": "rec-2"},
        },
    }

    result = offset_sync.capture_sync_point(host_entry, neon_event)

    assert result == {
        "vp1": {"t_host_ns": 1_500_000, "t_dev_ns": 1_000_000},
        "vp2": {"t_host_ns": 1_500_000, "t_dev_ns": 1_200_000},
    }

    with offsets_path.open("r", encoding="utf-8") as f:
        offsets_data = json.load(f)

    assert offsets_data == {"vp1": 500_000, "vp2": 300_000}

    assert sync_points_path.exists()
    with sync_points_path.open("r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    assert len(lines) == 1
    stored = lines[0]["sync_points"]
    assert stored["vp1"]["t_host_ns"] == 1_500_000
    assert stored["vp1"]["t_dev_ns"] == 1_000_000
    assert stored["vp1"]["recording_id"] == "rec-1"


def test_capture_sync_point_appends_entries(isolated_offsets):
    offsets_path = isolated_offsets
    sync_points_path = offsets_path.parent / "sync_points.jsonl"

    host_entry_1 = {"event": "fixation_flash", "t_host_ns": 1_000_000}
    neon_event_1 = {
        "payload": {
            "devices": {
                "vp1": {"t_dev_ns": 800_000, "recording_id": "rec-a"},
            }
        }
    }

    offset_sync.capture_sync_point(host_entry_1, neon_event_1)

    host_entry_2 = {"event": "fixation_flash", "t_host_ns": 2_000_000}
    neon_event_2 = {
        "device": "vp1",
        "t_dev_ns": 1_600_000,
        "recording_id": "rec-b",
    }

    offset_sync.capture_sync_point(host_entry_2, neon_event_2)

    with sync_points_path.open("r", encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]

    assert len(lines) == 2
