import json
from random import Random

import pytest

import core.offset_sync as offset_sync


@pytest.fixture()
def isolated_offsets(tmp_path, monkeypatch):
    offsets_path = tmp_path / "sync" / "offsets.json"
    monkeypatch.setattr(offset_sync, "_OFFSETS_PATH", offsets_path)
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
