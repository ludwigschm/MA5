"""Client for batching and sending validated events to the cloud service."""

from __future__ import annotations

import json
import logging
import queue
import threading
from collections import deque
from typing import Callable, Deque, Dict, Iterable, Literal, Mapping, MutableSequence, Tuple

from core.config import EVENT_BATCH_SIZE, EVENT_BATCH_WINDOW_MS

from .error_logger import log_event_error, reason_from_exception
from .schema import BaseEvent, validate_base_event

Priority = Literal["high", "normal"]

log = logging.getLogger(__name__)

_ALLOWED_FIELDS: tuple[str, ...] = (
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
)


_HIGH_SENTINEL = object()
_HighQueueItem = tuple[Dict[str, object], Tuple[str, str], int | None]


class CloudClient:
    """Batch cloud events while respecting priority semantics."""

    def __init__(
        self,
        transport: Callable[[str], None],
        *,
        batch_window_s: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        self._transport = transport
        window_ms = EVENT_BATCH_WINDOW_MS if EVENT_BATCH_WINDOW_MS > 0 else 5
        default_window = max(0.0, window_ms / 1000.0)
        self._batch_window = (
            default_window if batch_window_s is None else max(0.0, batch_window_s)
        )
        default_batch_size = EVENT_BATCH_SIZE if EVENT_BATCH_SIZE > 0 else 20
        self._batch_size = (
            default_batch_size if batch_size is None else max(1, int(batch_size))
        )
        self._lock = threading.Lock()
        self._queue: Deque[Dict[str, object]] = deque()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._high_queue: "queue.Queue[_HighQueueItem]" = queue.Queue()
        self._high_thread = threading.Thread(
            target=self._drain_high_priority_queue,
            name="CloudClientHighPriority",
            daemon=True,
        )
        self._high_thread.start()
        self._high_last_sequence: Dict[Tuple[str, str], int] = {}

    # ------------------------------------------------------------------
    def send_event(self, payload: BaseEvent, priority: Priority = "normal") -> None:
        """Validate and dispatch *payload* to the cloud backend."""

        if priority not in ("high", "normal"):
            raise ValueError(f"Unsupported priority: {priority}")

        try:
            validated = validate_base_event(payload)
        except ValueError as exc:
            log_event_error(reason_from_exception(exc), payload)
            return

        filtered: Dict[str, object] = {}
        for key in _ALLOWED_FIELDS:
            if key in validated:
                value = validated[key]
                if value is not None:
                    filtered[key] = value

        if not filtered:
            return

        if priority == "high":
            key = (validated["session_id"], validated["actor"])
            sequence_obj = validated.get("sequence_no")  # type: ignore[assignment]
            sequence_no = sequence_obj if isinstance(sequence_obj, int) else None
            with self._lock:
                if self._closed:
                    return
            self._high_queue.put((filtered, key, sequence_no))
            return

        with self._lock:
            if self._closed:
                return
            self._queue.append(filtered)
            if len(self._queue) >= self._batch_size:
                batch = self._dequeue_locked(self._batch_size)
            else:
                self._schedule_timer_locked()
                return

        if batch:
            self._send_batch(batch)

    # ------------------------------------------------------------------
    def flush(self) -> None:
        """Send all queued normal-priority events immediately."""

        with self._lock:
            batch = self._dequeue_locked()
        if batch:
            self._send_batch(batch)

    def close(self) -> None:
        """Flush remaining events and prevent further sends."""

        send_sentinel = False
        with self._lock:
            if self._closed:
                batch: list[Dict[str, object]] = []
            else:
                self._closed = True
                if self._timer is not None:
                    self._timer.cancel()
                    self._timer = None
                batch = self._dequeue_locked(cancel_timer=False)
                send_sentinel = True
        if batch:
            self._send_batch(batch)
        if send_sentinel:
            self._high_queue.put(_HIGH_SENTINEL)
            self._high_thread.join()

    # ------------------------------------------------------------------
    def _dequeue_locked(
        self, max_items: int | None = None, *, cancel_timer: bool = True
    ) -> list[Dict[str, object]]:
        items: list[Dict[str, object]] = []
        while self._queue and (max_items is None or len(items) < max_items):
            items.append(self._queue.popleft())
        if cancel_timer and not self._queue and self._timer is not None:
            self._timer.cancel()
            self._timer = None
        return items

    def _schedule_timer_locked(self) -> None:
        if self._timer is not None:
            return
        delay = max(0.0, self._batch_window)
        timer = threading.Timer(delay, self._on_timer)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def _on_timer(self) -> None:
        batch: list[Dict[str, object]]
        with self._lock:
            if self._closed:
                return
            self._timer = None
            batch = self._dequeue_locked(cancel_timer=False)
        if batch:
            self._send_batch(batch)

    def _send_batch(self, batch: Iterable[Dict[str, object]]) -> None:
        events = list(batch)
        if not events:
            return
        payload = json.dumps(events, separators=(",", ":"), ensure_ascii=False)
        try:
            self._transport(payload)
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Cloud event transport failed for %d events", len(events))
            self._requeue_front(events)

    def _requeue_front(self, events: MutableSequence[Dict[str, object]]) -> None:
        if not events:
            return
        with self._lock:
            if self._closed:
                return
            for event in reversed(events):
                self._queue.appendleft(event)
            if self._timer is None:
                self._schedule_timer_locked()

    def _drain_high_priority_queue(self) -> None:
        while True:
            item = self._high_queue.get()
            if item is _HIGH_SENTINEL:
                break
            payload, key, sequence_no = item
            if sequence_no is not None:
                last = self._high_last_sequence.get(key)
                assert (
                    last is None or sequence_no > last
                ), f"High-priority sequence regression for {key}: {sequence_no} <= {last}"
                self._high_last_sequence[key] = sequence_no
            self._send_batch((payload,))


__all__ = ["CloudClient", "Priority"]
