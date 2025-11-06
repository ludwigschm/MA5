import json
import logging
import time
from pathlib import Path

from core.single_writer_logger import SingleWriterLogger


def _load_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def test_single_writer_logger_handles_high_throughput(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    target = tmp_path / "events.jsonl"

    # A larger queue absorbs the 50k events burst without forcing producers to block.
    logger = SingleWriterLogger(target, queue_size=50_000)
    start = time.perf_counter()
    for i in range(50_000):
        assert logger.log_event({"index": i, "value": f"payload-{i}"})
    elapsed = time.perf_counter() - start
    logger.close()

    assert elapsed < 1.0
    assert logger.dropped_events == 0

    records = [record.message for record in caplog.records if record.levelno <= logging.INFO]
    assert any("DroppedEvents=0" in message for message in records)

    stored = _load_jsonl(target)
    assert len(stored) == 50_000
    assert stored[0] == {"index": 0, "value": "payload-0"}
    assert stored[-1] == {"index": 49_999, "value": "payload-49999"}
