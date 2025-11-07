import csv
from pathlib import Path

import pytest

from qc.report import emit_latency_summary, emit_mapping_summary


@pytest.fixture()
def sample_events() -> list[dict[str, object]]:
    return [
        {
            "session_id": "S-1",
            "mapping_version": 1,
            "mapping_rms_ns": 1_500,
            "t_device_ns": 10,
            "action": "card_flip",
            "t_host_sent_ns": 1_000,
            "t_api_ack_ns": 1_080,
        },
        {
            "session_id": "S-1",
            "mapping_version": 2,
            "mapping_rms_ns": 2_500,
            "t_device_ns": None,
            "action": "bet",
            "t_host_send_ns": 2_000,
            "t_ack_ns": 2_100,
        },
        {
            "session_id": "S-1",
            "mapping_version": 3,
            "mapping_rms_ns": 2_000,
            "t_device_ns": 20,
            "action": "fold",
            "t_send_ns": 3_000,
            "t_ack_ns": 3_060,
        },
        {
            "session_id": "S-1",
            "mapping_version": None,
            "mapping_rms_ns": None,
            "t_device_ns": 30,
            "action": "timeout",
            "t_ui_mono_ns": 4_000,
        },
        {
            "session_id": "S-1",
            "mapping_version": 3,
            "mapping_rms_ns": 3_000,
            "t_device_ns": None,
            "action": "call",
            "t_host_sent_ns": 5_000,
            "t_api_ack_ns": 5_116,
        },
        {
            "session_id": "S-1",
            "mapping_version": 3,
            "mapping_rms_ns": 1_875,
            "t_device_ns": 40,
            "action": "bet",
            "t_host_sent_ns": 6_000,
            "t_api_ack_ns": 5_950,
        },
    ]


def test_emit_mapping_summary_writes_expected_row(tmp_path: Path, sample_events: list[dict[str, object]]) -> None:
    output_dir = tmp_path / "qc"
    path = emit_mapping_summary("session-42", sample_events, output_dir=output_dir)
    assert path.exists()

    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    row = rows[0]
    assert row["session_id"] == "session-42"
    assert row["mapping_version"] == "3"
    assert row["avg_mapping_rms_ns"] == "2175"
    assert row["events_total"] == "6"
    assert row["events_with_t_device"] == "4"
    assert row["events_without_t_device"] == "2"
    assert row["share_with_t_device"] == "0.667"
    assert row["share_without_t_device"] == "0.333"


def test_emit_latency_summary_filters_and_computes_percentiles(
    tmp_path: Path, sample_events: list[dict[str, object]]
) -> None:
    output_dir = tmp_path / "qc"
    path = emit_latency_summary("session-42", sample_events, output_dir=output_dir)
    assert path.exists()

    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    row = rows[0]
    assert row["session_id"] == "session-42"
    # Five critical actions (card_flip, bet x2, fold, call) but one latency invalid
    assert row["critical_event_count"] == "5"
    assert row["latency_samples"] == "4"
    assert row["median_latency_ns"] == "90"
    assert row["p95_latency_ns"] == "114"
