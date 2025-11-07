import importlib.util
import json
import logging
from pathlib import Path

import pytest


def _load_cloud_env_check():
    module_path = Path(__file__).resolve().parents[1] / "diagnostics" / "cloud_env_check.py"
    spec = importlib.util.spec_from_file_location("cloud_env_check", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


cloud_env_check = _load_cloud_env_check()


def test_gather_diagnostics_warns_on_upsert(monkeypatch, caplog, tmp_path):
    caplog.set_level(logging.WARNING, logger=cloud_env_check.log.name)
    monkeypatch.setenv("SENDE_UPSERT", "true")
    monkeypatch.setenv("SENDE_SDK_VERSION", "1.2.3")
    monkeypatch.setenv("VP1_FIRMWARE_VERSION", "0.9.1")

    data = cloud_env_check.gather_diagnostics()
    assert data["sdk_version"] == "1.2.3"
    assert data["firmware_versions"]["VP1_FIRMWARE_VERSION"] == "0.9.1"
    assert "Suspicious flag detected" in caplog.text

    json_path, txt_path = cloud_env_check.write_reports(data, directory=tmp_path)

    assert json_path.exists()
    assert txt_path.exists()

    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["sdk_version"] == "1.2.3"

    text = txt_path.read_text(encoding="utf-8")
    assert "SDK-Version: 1.2.3" in text
    assert "VP1_FIRMWARE_VERSION" in text


@pytest.mark.parametrize(
    "env_name, value",
    [
        ("SENDE_BATCHING", "true"),
        ("SENDE_MERGE", "true"),
    ],
)
def test_gather_diagnostics_flags(monkeypatch, env_name, value, caplog):
    caplog.set_level(logging.WARNING, logger=cloud_env_check.log.name)
    monkeypatch.setenv(env_name, value)
    data = cloud_env_check.gather_diagnostics()
    alias = cloud_env_check._FLAG_ENV_MAP[env_name]
    assert data["sdk_flags"][alias]["effective"] is True
    assert f"{alias} enabled" in caplog.text
