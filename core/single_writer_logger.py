"""Single-writer logger implementation backed by a bounded queue.

The :class:`SingleWriterLogger` decouples log producers from disk I/O by
buffering events in a bounded queue that is drained by a dedicated consumer
thread.  Producers never block – once the queue is full, events are dropped and
counted so that callers can assert on the number of lost entries at shutdown.

The consumer batches writes and periodically flushes to disk, which minimises
syscall overhead while still providing predictable latency.
"""

from __future__ import annotations

import csv
import json
import logging
import threading
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable, Optional

__all__ = ["SingleWriterLogger"]


class SingleWriterLogger:
    """Write structured log events to disk using a single consumer thread.

    Parameters
    ----------
    path:
        Destination file.  The suffix determines the format (``.jsonl`` or
        ``.csv``).
    queue_size:
        Maximum number of events buffered at any point in time.
    batch_size:
        Number of events to accumulate before forcing a write.
    flush_interval:
        Maximum duration (in seconds) events are buffered before being flushed.
    logger:
        Optional :class:`logging.Logger` used for operational messages.
    """

    _SENTINEL = object()

    def __init__(
        self,
        path: Path,
        *,
        queue_size: int = 5000,
        batch_size: int = 100,
        flush_interval: float = 0.05,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval <= 0:
            raise ValueError("flush_interval must be positive")

        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._capacity = queue_size
        self._queue: deque[Any] = deque()
        self._queue_size = 0
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._logger = logger or logging.getLogger(__name__)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="SingleWriterLogger", daemon=True)
        self._dropped_events = 0
        self._written_events = 0
        self._high_watermark = max(1, int(queue_size * 0.8))
        self._low_watermark = max(1, int(queue_size * 0.6))
        self._warned_backpressure = False
        self._csv_writer: Optional[csv.DictWriter[str]] = None
        self._csv_fieldnames: Optional[list[str]] = None

        suffix = self._path.suffix.lower()
        if suffix not in {".jsonl", ".csv"}:
            raise ValueError("SingleWriterLogger only supports .jsonl or .csv outputs")
        self._format = suffix.lstrip(".")

        if self._format == "jsonl":
            self._file = self._path.open("ab")
        else:
            self._file = self._path.open("a", encoding="utf-8", newline="")

        if self._format == "jsonl":
            self._prepare_event = self._prepare_jsonl_event
        else:
            self._prepare_event = self._prepare_csv_event

        self._thread.start()

    @property
    def dropped_events(self) -> int:
        """Return the number of events that were dropped because the queue was full."""

        return self._dropped_events

    @property
    def written_events(self) -> int:
        """Return the number of events successfully written to disk."""

        return self._written_events

    def log_event(self, event: Mapping[str, Any]) -> bool:
        """Enqueue an event for asynchronous persistence.

        Parameters
        ----------
        event:
            Mapping containing the structured log payload.

        Returns
        -------
        bool
            :data:`True` when the event was enqueued, :data:`False` if it was
            dropped due to a full queue.
        """

        if self._stop_event.is_set():
            raise RuntimeError("Cannot log events after logger has been closed")

        if not isinstance(event, Mapping):
            raise TypeError("event must be a mapping")

        prepared = self._prepare_event(event)
        with self._not_empty:
            if self._queue_size >= self._capacity:
                self._dropped_events += 1
                return False
            self._queue.append(prepared)
            self._queue_size += 1
            queued = self._queue_size
            self._not_empty.notify()

        if not self._warned_backpressure and queued >= self._high_watermark:
            self._logger.warning(
                "SingleWriterLogger queue utilisation high size=%d capacity=%d", queued, self._capacity
            )
            self._warned_backpressure = True
        
        return True

    def close(self) -> None:
        """Flush pending events and stop the consumer thread."""

        if self._stop_event.is_set():
            return

        self._stop_event.set()
        with self._not_empty:
            self._queue.append(self._SENTINEL)
            self._queue_size += 1
            self._not_empty.notify()
        self._thread.join()
        self._file.flush()
        self._file.close()
        self._logger.info("SingleWriterLogger stopped DroppedEvents=%d", self._dropped_events)

    def __enter__(self) -> "SingleWriterLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _run(self) -> None:
        batch: list[Any] = []
        last_flush = time.perf_counter()

        while True:
            timeout = max(0.0, self._flush_interval - (time.perf_counter() - last_flush))
            target = max(1, self._batch_size - len(batch))
            items, sentinel_received, timed_out = self._dequeue_many(target, timeout)

            if items:
                batch.extend(items)

            now = time.perf_counter()
            should_flush = False
            if batch and len(batch) >= self._batch_size:
                should_flush = True
            elif batch and timed_out:
                should_flush = True

            if should_flush:
                self._flush_batch(batch)
                batch.clear()
                last_flush = now

            if sentinel_received:
                if batch:
                    self._flush_batch(batch)
                    batch.clear()
                break

        # Flush any late arrivals drained after the sentinel (should not happen but safe).
        if batch:
            self._flush_batch(batch)

    def _dequeue_many(self, max_items: int, timeout: float) -> tuple[list[Any], bool, bool]:
        deadline = time.perf_counter() + timeout
        items: list[Any] = []
        sentinel_received = False
        timed_out = False

        with self._not_empty:
            while not self._queue:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    timed_out = True
                    return items, sentinel_received, timed_out
                self._not_empty.wait(remaining)

            while self._queue and len(items) < max_items:
                item = self._queue.popleft()
                self._queue_size -= 1
                if self._warned_backpressure and self._queue_size <= self._low_watermark:
                    self._warned_backpressure = False
                if item is self._SENTINEL:
                    sentinel_received = True
                    break
                items.append(item)

            if not items and not sentinel_received:
                # If we exited because max_items == 0 we still honour timeout semantics.
                timed_out = False

        return items, sentinel_received, timed_out

    def _flush_batch(self, batch: list[Any]) -> None:
        if self._format == "jsonl":
            self._write_jsonl(batch)
        else:
            self._write_csv(batch)

    def _prepare_jsonl_event(self, event: Mapping[str, Any]) -> bytes:
        if not isinstance(event, Mapping):
            raise TypeError("event must be a mapping")
        return (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")

    def _write_jsonl(self, batch: list[bytes]) -> None:
        if not batch:
            return
        self._file.write(b"".join(batch))
        self._written_events += len(batch)

    def _prepare_csv_event(self, event: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(event, Mapping):
            raise TypeError("event must be a mapping")
        return dict(event)

    def _write_csv(self, batch: Iterable[Mapping[str, Any]]) -> None:
        if self._csv_writer is None:
            first = next(iter(batch))
            if not isinstance(first, Mapping):
                raise TypeError("CSV logging requires mapping events")
            self._csv_fieldnames = list(first.keys())
            self._csv_writer = csv.DictWriter(self._file, fieldnames=self._csv_fieldnames, extrasaction="ignore")
            self._csv_writer.writeheader()

        assert self._csv_writer is not None

        for item in batch:
            self._csv_writer.writerow({key: item.get(key, "") for key in self._csv_fieldnames or ()})
            self._written_events += 1
