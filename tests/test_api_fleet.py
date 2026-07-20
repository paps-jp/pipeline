"""/api/v1/fleet — 全停止 / 再開の統合テスト。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app


@pytest.fixture
def client() -> TestClient:
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    app = create_app(settings)
    return TestClient(app)


def _mk(client: TestClient, slug: str, enabled: bool = True) -> None:
    r = client.post("/api/v1/workloads", json={
        "slug": slug,
        "name": slug,
        "executor_type": "shell",
        "executor_config": {"command": "echo hi"},
    })
    assert r.status_code == 201, r.text
    # WorkloadCreate の既定は enabled=False。 稼働状態にするには明示的に有効化する。
    if enabled:
        r = client.patch(f"/api/v1/workloads/{slug}/enabled", json={"enabled": True})
        assert r.status_code == 200, r.text


def _enabled(client: TestClient) -> set[str]:
    return {w["slug"] for w in client.get("/api/v1/workloads").json()["workloads"]
            if w["enabled"]}


def test_stop_records_only_running_then_resume_restores_them(client: TestClient) -> None:
    """止まっていたものは再開で動かさない、が本質。"""
    with client:
        _mk(client, "alpha")                      # 稼働中
        _mk(client, "beta")                       # 稼働中
        _mk(client, "gamma", enabled=False)       # もともと停止
        assert _enabled(client) == {"alpha", "beta"}

        r = client.post("/api/v1/fleet/stop")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ok"] is True
        assert set(body["recorded_slugs"]) == {"alpha", "beta"}
        assert _enabled(client) == set()          # 全部止まった

        st = client.get("/api/v1/fleet/state").json()
        assert st["stopped"] is True
        assert set(st["recorded_slugs"]) == {"alpha", "beta"}

        r = client.post("/api/v1/fleet/resume")
        assert r.status_code == 200, r.text
        # gamma は復活しない
        assert _enabled(client) == {"alpha", "beta"}
        assert client.get("/api/v1/fleet/state").json()["stopped"] is False


def test_double_stop_is_rejected(client: TestClient) -> None:
    """2度押しで記録を空に上書きすると再開不能になるので弾く。"""
    with client:
        _mk(client, "alpha")
        assert client.post("/api/v1/fleet/stop").status_code == 200
        r = client.post("/api/v1/fleet/stop")
        assert r.status_code == 409
        # 記録は最初のまま
        assert client.get("/api/v1/fleet/state").json()["recorded_slugs"] == ["alpha"]


def test_resume_without_stop_is_rejected(client: TestClient) -> None:
    with client:
        _mk(client, "alpha")
        assert client.post("/api/v1/fleet/resume").status_code == 409


def test_supervisor_is_stopped_first_and_resumed_last(client: TestClient) -> None:
    """supervisor が他 workload を復活させないよう順序が決まっている。"""
    with client:
        _mk(client, "pipeline-supervisor")
        _mk(client, "alpha")

        body = client.post("/api/v1/fleet/stop").json()
        assert body["changed"][0] == "pipeline-supervisor"

        body = client.post("/api/v1/fleet/resume").json()
        assert body["changed"][-1] == "pipeline-supervisor"


def test_stop_with_nothing_running(client: TestClient) -> None:
    """稼働ゼロでも停止状態にはなり、再開は no-op で閉じる。"""
    with client:
        _mk(client, "alpha", enabled=False)
        r = client.post("/api/v1/fleet/stop")
        assert r.status_code == 200
        assert r.json()["recorded_slugs"] == []
        assert client.get("/api/v1/fleet/state").json()["stopped"] is True

        assert client.post("/api/v1/fleet/resume").status_code == 200
        assert _enabled(client) == set()
        assert client.get("/api/v1/fleet/state").json()["stopped"] is False
