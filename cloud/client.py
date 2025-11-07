"""HTTP client utilities for talking to the Sende append-only endpoint."""

from __future__ import annotations

import logging
import os
import time
from http import HTTPStatus
from typing import Any, Dict, Mapping, MutableMapping, Optional

try:  # pragma: no cover - optional dependency, exercised in tests via monkeypatching
    import requests
    from requests import Response
except Exception:  # pragma: no cover - import side effects shouldn't break runtime
    requests = None  # type: ignore[assignment]
    Response = Any  # type: ignore[misc, assignment]


log = logging.getLogger(__name__)

_APPEND_EVENT_URL_ENV = "SENDE_EVENTS_URL"
_API_KEY_ENV = "SENDE_API_KEY"
_TIMEOUT_ENV = "SENDE_TIMEOUT_SECONDS"

_DEFAULT_TIMEOUT = 10.0
_MAX_ATTEMPTS = 5
_INITIAL_BACKOFF = 0.5

# ``upsert`` is disabled explicitly to guarantee append-only semantics even if the
# backend SDK or API happens to default to an upsert behaviour.
_UPSERT_QUERY = {"upsert": "false"}

if requests:  # pragma: no branch - hint for type checkers only
    _Session = requests.Session
    _RequestException = requests.RequestException
else:  # pragma: no cover - only evaluated when requests is unavailable
    _Session = Any  # type: ignore[misc, assignment]
    _RequestException = Exception  # type: ignore[assignment]

_session: Optional[_Session] = None


class AppendEventError(RuntimeError):
    """Raised when an event could not be appended after retries."""


def _get_session() -> _Session:
    if requests is None:
        raise RuntimeError("The 'requests' package is required to send events")
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def _get_timeout() -> float:
    value = os.environ.get(_TIMEOUT_ENV)
    if not value:
        return _DEFAULT_TIMEOUT
    try:
        timeout = float(value)
    except ValueError:
        log.warning("Invalid %s value '%s', falling back to default", _TIMEOUT_ENV, value)
        return _DEFAULT_TIMEOUT
    return max(0.0, timeout)


def _get_append_url() -> str:
    url = os.environ.get(_APPEND_EVENT_URL_ENV)
    if not url:
        raise RuntimeError(
            "Missing append endpoint. Set the SENDE_EVENTS_URL environment variable"
        )
    return url


def _build_headers(idempotency_key: str) -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    api_key = os.environ.get(_API_KEY_ENV)
    if api_key:
        headers.setdefault("apikey", api_key)
        headers.setdefault("Authorization", f"Bearer {api_key}")
    return headers


def _send_request(url: str, payload: Mapping[str, Any], headers: Mapping[str, str]) -> Response:
    session = _get_session()
    return session.post(
        url,
        json=dict(payload),
        headers=dict(headers),
        params=dict(_UPSERT_QUERY),
        timeout=_get_timeout(),
    )


def append_event(payload: Dict[str, Any], *, idempotency_key: str) -> None:
    """Send *payload* to the append-only Sende endpoint with retries.

    The same *idempotency_key* is reused across retries so the backend can recognise
    duplicates. HTTP 409 responses are considered a success (duplicate) and abort
    further retries.
    """

    if not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")

    append_url = _get_append_url()
    headers = _build_headers(idempotency_key)

    # Copy the payload so callers do not observe mutations and ensure the
    # idempotency key is recorded locally even if the backend ignores the header.
    body: MutableMapping[str, Any] = dict(payload)
    body.setdefault("client_idempotency_key", idempotency_key)

    last_error: Optional[Exception] = None
    delay = _INITIAL_BACKOFF

    for attempt in range(_MAX_ATTEMPTS):
        try:
            response = _send_request(append_url, body, headers)
        except _RequestException as exc:  # pragma: no cover - exercised via tests
            last_error = exc
            log.warning(
                "Append attempt %d for %s failed with network error: %s",
                attempt + 1,
                idempotency_key,
                exc,
            )
        else:
            status = response.status_code
            if status == HTTPStatus.CONFLICT:
                log.debug(
                    "Duplicate event detected for %s (HTTP 409), treating as success",
                    idempotency_key,
                )
                return
            if 200 <= status < 300:
                return
            content = response.text
            message = (
                f"Append attempt {attempt + 1} for {idempotency_key} failed "
                f"with status {status}: {content!r}"
            )
            if 400 <= status < 500:
                raise AppendEventError(message)
            last_error = AppendEventError(message)
            log.warning(message)

        if attempt >= _MAX_ATTEMPTS - 1:
            break
        time.sleep(delay)
        delay *= 2

    if last_error is None:
        last_error = AppendEventError(
            f"Failed to append event for {idempotency_key} after {_MAX_ATTEMPTS} attempts"
        )
    raise AppendEventError("Append-only request failed") from last_error


__all__ = ["append_event", "AppendEventError"]

