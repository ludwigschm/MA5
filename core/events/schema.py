"""Canonical event schema and validation utilities."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, TypedDict, cast

ALLOWED_ACTIONS = {
    "card_flip",
    "bet",
    "call",
    "fold",
    "phase_transition",
    "timeout",
}


class BaseEventRequired(TypedDict):
    session_id: str
    block_idx: int
    trial_idx: int
    actor: str
    player1_id: str
    action: str
    t_ui_mono_ns: int


class BaseEvent(BaseEventRequired, total=False):
    t_device_ns: Optional[int]
    mapping_version: Optional[int]
    mapping_confidence: Optional[float]
    mapping_rms_ns: Optional[int]
    t_utc_iso: Optional[str]


@dataclass(frozen=True)
class _FieldSpec:
    name: str
    expected_type: tuple[type[Any], ...]
    optional: bool = False
    allow_none: bool = False


_REQUIRED_FIELD_SPECS = (
    _FieldSpec("session_id", (str,)),
    _FieldSpec("block_idx", (int,)),
    _FieldSpec("trial_idx", (int,)),
    _FieldSpec("actor", (str,)),
    _FieldSpec("player1_id", (str,)),
    _FieldSpec("action", (str,)),
    _FieldSpec("t_ui_mono_ns", (int,)),
)

_OPTIONAL_FIELD_SPECS = (
    _FieldSpec("t_device_ns", (int,), optional=True, allow_none=True),
    _FieldSpec("mapping_version", (int,), optional=True, allow_none=True),
    _FieldSpec("mapping_confidence", (float,), optional=True, allow_none=True),
    _FieldSpec("mapping_rms_ns", (int,), optional=True, allow_none=True),
    _FieldSpec("t_utc_iso", (str,), optional=True, allow_none=True),
)

_KNOWN_FIELDS = {spec.name for spec in (*_REQUIRED_FIELD_SPECS, *_OPTIONAL_FIELD_SPECS)}


def _is_valid_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_number(value: Any, expected_types: tuple[type[Any], ...]) -> bool:
    if float in expected_types:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return not (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))
    return isinstance(value, expected_types) and not isinstance(value, bool)


def validate_base_event(data: Mapping[str, Any]) -> BaseEvent:
    """Validate *data* and return it as a :class:`BaseEvent`.

    Raises:
        ValueError: If a required field is missing or if any field fails validation.
    """

    validated: Dict[str, Any] = {}

    for spec in _REQUIRED_FIELD_SPECS:
        if spec.name not in data:
            raise ValueError(f"Missing required field: {spec.name}")
        value = data[spec.name]
        if spec.expected_type == (int,):
            if not _is_valid_int(value):
                raise ValueError(f"Field '{spec.name}' must be an int")
        else:
            if not isinstance(value, spec.expected_type):
                raise ValueError(
                    f"Field '{spec.name}' must be of type {spec.expected_type}, got {type(value).__name__}"
                )
        if isinstance(value, str) and not value:
            raise ValueError(f"Field '{spec.name}' cannot be empty")
        validated[spec.name] = value

    action = validated["action"]
    if action not in ALLOWED_ACTIONS:
        raise ValueError(f"Unsupported action: {action}")

    for spec in _OPTIONAL_FIELD_SPECS:
        if spec.name not in data:
            continue
        value = data[spec.name]
        if value is None:
            validated[spec.name] = None
            continue
        if spec.expected_type == (int,):
            if not _is_valid_int(value):
                raise ValueError(f"Field '{spec.name}' must be an int or None")
        elif spec.expected_type == (float,):
            if not _validate_number(value, spec.expected_type):
                raise ValueError(f"Field '{spec.name}' must be a finite float or None")
            value = float(value)
        else:
            if not isinstance(value, spec.expected_type):
                raise ValueError(
                    f"Field '{spec.name}' must be of type {spec.expected_type} or None, got {type(value).__name__}"
                )
            if isinstance(value, str) and not value:
                raise ValueError(f"Field '{spec.name}' cannot be empty when provided")
        validated[spec.name] = value

    extra_fields = set(data.keys()) - _KNOWN_FIELDS
    if extra_fields:
        raise ValueError(f"Unexpected fields: {sorted(extra_fields)}")

    return cast(BaseEvent, validated)


__all__ = ["BaseEvent", "ALLOWED_ACTIONS", "validate_base_event"]
