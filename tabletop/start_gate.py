"""Readiness gate for starting an experimental session."""

from __future__ import annotations

import logging
import threading
from typing import Callable, Optional, Sequence, Set, Tuple

from core import offset_sync

__all__ = ["StartGate"]


class StartGate:
    """Poll device readiness until all start conditions are met."""

    DEFAULT_SENSORS: Tuple[str, ...] = (
        "world",
        "eyes",
        "gaze",
        "imu",
        "eye_events",
    )

    def __init__(
        self,
        bridge: object,
        *,
        players: Sequence[str],
        sensors: Optional[Sequence[str]] = None,
        poll_interval: float = 0.5,
        logger: Optional[logging.Logger] = None,
        offset_devices: Optional[Sequence[str]] = None,
    ) -> None:
        self._bridge = bridge
        self._players = tuple(players)
        self._sensors = tuple(sensors or self.DEFAULT_SENSORS)
        self._poll_interval = max(0.1, float(poll_interval))
        self._log = logger or logging.getLogger(__name__)
        self._offset_devices = tuple(offset_devices or ("vp1", "vp2"))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._callback: Optional[Callable[[], None]] = None
        self._last_blockers: Set[str] = set()

    @property
    def required_sensors(self) -> Tuple[str, ...]:
        return self._sensors

    def start(self, on_ready: Callable[[], None]) -> None:
        """Start polling and invoke *on_ready* once all checks pass."""

        if on_ready is None:
            raise ValueError("on_ready callback is required")
        self._callback = on_ready
        self._stop.clear()
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="StartGate", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop polling and wait for the worker thread to finish."""

        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive() and threading.current_thread() is not thread:
            thread.join(timeout=0.5)
        self._thread = None

    # ------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            blockers = self._evaluate_blockers()
            if not blockers:
                self._dispatch_ready()
                return
            if blockers != self._last_blockers:
                reasons = ", ".join(sorted(blockers)) or "-"
                self._log.info("START-GATE waiting: %s", reasons)
                self._last_blockers = blockers
            self._stop.wait(self._poll_interval)

    def _dispatch_ready(self) -> None:
        callback = self._callback
        if callback is None:
            return
        try:
            callback()
        except Exception:  # pragma: no cover - defensive
            self._log.exception("START-GATE callback failed")
        finally:
            self._callback = None
            self.stop()

    def _evaluate_blockers(self) -> Set[str]:
        blockers: Set[str] = set()
        if not self._players:
            return blockers
        if not self._sensors_ready():
            blockers.add("sensors")
        if not self._recordings_ready():
            blockers.add("recording")
        if not self._sync_ready():
            blockers.add("sync_point")
        return blockers

    def _sensors_ready(self) -> bool:
        bridge = self._bridge
        getter = getattr(bridge, "get_sensor_snapshot", None)
        is_connected = getattr(bridge, "is_connected", None)
        if not callable(getter):
            return False
        for player in self._players:
            if callable(is_connected) and not is_connected(player):
                return False
            snapshot = getter(player) or {}
            for sensor in self._sensors:
                if not snapshot.get(sensor, False):
                    return False
        return True

    def _recordings_ready(self) -> bool:
        bridge = self._bridge
        getter = getattr(bridge, "get_recording_id", None)
        if not callable(getter):
            return False
        for player in self._players:
            recording_id = getter(player)
            if not recording_id:
                return False
        return True

    def _sync_ready(self) -> bool:
        required = self._offset_devices or ()
        if not required:
            return True
        return offset_sync.have_offsets(required)

