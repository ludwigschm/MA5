import json
from types import SimpleNamespace
from unittest import mock

import pytest

from cloud import client as cloud_client
from cloud.payload import build_cloud_payload


@pytest.fixture(autouse=True)
def reset_append_only_mode():
    original = cloud_client.append_only_mode
    try:
        cloud_client.append_only_mode = True
        yield
    finally:
        cloud_client.append_only_mode = original


def test_minimal_payload_keys():
    payload = build_cloud_payload(
        action="card_flip", actor="VP1", player1_id="VP1", session_id=None
    )
    assert set(payload.keys()) == {"action", "actor", "player1_id"}


def test_payload_with_extra_key_logs_and_raises(monkeypatch, tmp_path):
    log_path = tmp_path / "violation.log"
    monkeypatch.setattr(cloud_client, "_PAYLOAD_VIOLATION_LOG", log_path)
    monkeypatch.setenv("SENDE_EVENTS_URL", "https://example.invalid")

    called = False

    def _fail(*_args, **_kwargs):
        nonlocal called
        called = True
        pytest.fail("request should not be issued for invalid payload")

    monkeypatch.setattr(cloud_client, "_send_request", _fail)

    payload = {"action": "card_flip", "actor": "VP1", "player1_id": "VP1", "debug": 1}

    with pytest.raises(ValueError, match="Cloud payload must contain"):
        cloud_client.append_event(payload, idempotency_key="abc")

    assert not called
    assert log_path.exists()
    content = log_path.read_text(encoding="utf-8")
    assert "debug" in content
    assert "payload" in content
    assert "Traceback" in content


def test_append_only_idempotency(monkeypatch):
    monkeypatch.setenv("SENDE_EVENTS_URL", "https://example.invalid")

    events = []
    seen_keys: dict[str, int] = {}

    class DummyResponse:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

    def _fake_send(_url, payload, headers):
        key = headers["Idempotency-Key"]
        count = seen_keys.get(key, 0)
        seen_keys[key] = count + 1
        if count:
            return DummyResponse(409, "duplicate")
        events.append(json.dumps(payload, sort_keys=True))
        return DummyResponse(201, "created")

    monkeypatch.setattr(cloud_client, "_send_request", _fake_send)

    base_payload = build_cloud_payload(
        action="card_flip", actor="VP1", player1_id="VP1", session_id=None
    )

    cloud_client.append_event(dict(base_payload), idempotency_key="k1")
    cloud_client.append_event(dict(base_payload), idempotency_key="k2")
    cloud_client.append_event(dict(base_payload), idempotency_key="k1")

    assert len(events) == 2
    assert seen_keys["k1"] == 2
    assert seen_keys["k2"] == 1


def test_update_event_noop_in_append_only_mode(caplog):
    caplog.set_level("WARNING")
    cloud_client.update_event({"action": "card_flip"})
    assert "append_only_mode" in caplog.text


def test_ui_order_send_before_mutation():
    import ast
    from pathlib import Path

    source = Path("tabletop/tabletop_view.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    def _find_start_pressed() -> ast.FunctionDef:
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "TabletopRoot":
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "start_pressed":
                        return item
        raise AssertionError("start_pressed not found")

    func = _find_start_pressed()

    class _Collector(ast.NodeVisitor):
        def __init__(self) -> None:
            self.calls: list[str] = []

        def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast API
            func = node.func
            if isinstance(func, ast.Attribute):
                if func.attr in {"_append_minimal_cloud_event", "record_action"}:
                    self.calls.append(func.attr)
            self.generic_visit(node)

    collector = _Collector()
    collector.visit(func)

    assert "_append_minimal_cloud_event" in collector.calls
    assert "record_action" in collector.calls
    assert collector.calls.index("_append_minimal_cloud_event") < collector.calls.index("record_action")
