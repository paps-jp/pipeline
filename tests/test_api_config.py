"""/api/v1/config (宣言的構成管理) の統合テスト。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app

_YAML = """
version: 1
workloads:
  demo:
    name: Demo
    executor_type: shell
    executor_config: {command: "true"}
    host_affinity: [ai-gpu1]
hosts:
  ai-gpu1: {tier: gpu-16g-pro}
"""


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app(Settings(db_url="sqlite:///:memory:", mode="dev")))


def test_plan_then_apply_then_export_roundtrip(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/config/plan", json={"yaml": _YAML})
        assert r.status_code == 200
        assert r.json()["summary"]["create"] == 2
        assert client.get("/api/v1/workloads/demo").status_code == 404   # plan は書かない

        r = client.post("/api/v1/config/apply", json={"yaml": _YAML})
        assert r.status_code == 200 and r.json()["ok"] is True
        w = client.get("/api/v1/workloads/demo").json()
        assert w["host_affinity"] == ["ai-gpu1"] and w["enabled"] is False

        # 反映後は差分なし
        r = client.post("/api/v1/config/plan", json={"yaml": _YAML})
        assert r.json()["has_changes"] is False

        # export した YAML をそのまま plan に戻しても差分なし
        text = client.get("/api/v1/config/export").text
        assert "demo:" in text and "pipeline.config.yaml" in text
        r = client.post("/api/v1/config/plan", json={"yaml": text})
        assert r.json()["has_changes"] is False


def test_invalid_yaml_returns_422_with_messages(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/config/plan",
                        json={"yaml": "workloads:\n  x:\n    name: X\n    executor_type: shell\n    enabled: true\n"})
        assert r.status_code == 422
        assert any("enabled" in m for m in r.json()["detail"]["errors"])


def test_prune_flag_is_respected(client: TestClient) -> None:
    with client:
        client.post("/api/v1/config/apply", json={"yaml": _YAML})
        empty = "workloads: {}\n"
        r = client.post("/api/v1/config/plan", json={"yaml": empty})
        assert r.json()["summary"]["extra"] == 1 and r.json()["summary"]["delete"] == 0
        r = client.post("/api/v1/config/plan", json={"yaml": empty, "prune": True})
        assert r.json()["summary"]["delete"] == 1
