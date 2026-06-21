"""Worker registry + HTTP-based queue access の統合テスト."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app


@pytest.fixture()
def client():
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    with TestClient(create_app(settings)) as c:
        yield c


def _make_workload(client, slug: str = "w1") -> str:
    r = client.post(
        "/api/v1/workloads",
        json={
            "slug": slug, "name": "w", "enabled": True,
            "executor_type": "shell", "executor_config": {"command": ["echo", "hi"]},
            "batch_size": 5, "lease_secs": 30, "max_attempts": 2,
        },
    )
    assert r.status_code == 201, r.text
    return slug


# ---------------- registry ----------------


def test_register_and_get(client: TestClient):
    r = client.post("/api/v1/workers", json={"host": "h1"})
    assert r.status_code == 201
    info = r.json()
    assert info["id"].startswith("w_h1_")
    assert info["state"] == "active"
    # list
    lst = client.get("/api/v1/workers").json()
    assert lst["total"] == 1


def test_register_with_explicit_id_updates_in_place(client: TestClient):
    r1 = client.post("/api/v1/workers", json={"host": "h1", "worker_id": "w_fixed"})
    assert r1.json()["id"] == "w_fixed"
    # 再 register → 既存 update
    r2 = client.post("/api/v1/workers", json={"host": "h2", "worker_id": "w_fixed"})
    assert r2.json()["id"] == "w_fixed"
    assert r2.json()["host"] == "h2"
    assert client.get("/api/v1/workers").json()["total"] == 1


def test_heartbeat_404_for_unknown(client: TestClient):
    r = client.put("/api/v1/workers/unknown_id/heartbeat", json={})
    assert r.status_code == 404


def test_heartbeat_increments_counters(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    r = client.put(
        f"/api/v1/workers/{wid}/heartbeat",
        json={"rows_processed_delta": 5, "errors_total_delta": 1, "current_workload": "w1"},
    )
    assert r.status_code == 200
    info = r.json()
    assert info["rows_processed"] == 5
    assert info["errors_total"] == 1
    assert info["current_workload"] == "w1"


def test_deregister(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    r = client.delete(f"/api/v1/workers/{wid}")
    assert r.status_code == 204
    assert client.get("/api/v1/workers").json()["total"] == 0


# ---------------- workloads-for-worker ----------------


def test_workloads_for_worker_returns_enabled(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    _make_workload(client, "enabled-one")
    # disabled な workload
    client.post(
        "/api/v1/workloads",
        json={
            "slug": "disabled-one", "name": "d", "enabled": False,
            "executor_type": "shell", "executor_config": {"command": ["echo"]},
            "batch_size": 1, "lease_secs": 30, "max_attempts": 1,
        },
    )
    r = client.get(f"/api/v1/workers/{wid}/workloads")
    assert r.status_code == 200
    slugs = [w["slug"] for w in r.json()["workloads"]]
    assert "enabled-one" in slugs
    assert "disabled-one" not in slugs


# ---------------- claim / complete / fail ----------------


def test_claim_returns_tasks(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    _make_workload(client, "w1")
    for pk in ["a", "b", "c"]:
        client.post("/api/v1/workloads/w1/tasks", json={"pk": pk})
    r = client.post(
        f"/api/v1/workers/{wid}/claim",
        json={"workload_slug": "w1", "limit": 10},
    )
    assert r.status_code == 200
    d = r.json()
    assert d["workload_slug"] == "w1"
    assert {t["pk"] for t in d["tasks"]} == {"a", "b", "c"}


def test_complete_removes_from_queue(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    _make_workload(client, "w1")
    client.post("/api/v1/workloads/w1/tasks", json={"pk": "x"})
    client.post(f"/api/v1/workers/{wid}/claim", json={"workload_slug": "w1", "limit": 1})
    r = client.post(
        f"/api/v1/workers/{wid}/complete",
        json={"workload_slug": "w1", "pks": ["x"]},
    )
    assert r.status_code == 204
    assert client.get("/api/v1/workloads/w1/queue").json()["total"] == 0


def test_fail_increments_attempt(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    _make_workload(client, "w1")
    client.post("/api/v1/workloads/w1/tasks", json={"pk": "x"})
    client.post(f"/api/v1/workers/{wid}/claim", json={"workload_slug": "w1", "limit": 1})
    r = client.post(
        f"/api/v1/workers/{wid}/fail",
        json={"workload_slug": "w1", "pk": "x", "error": "oops"},
    )
    assert r.status_code == 204
    # max_attempts=2 で 1 回失敗 → pending に戻る
    stats = client.get("/api/v1/workloads/w1/queue").json()
    assert stats["by_state"] == {"pending": 1}


def test_record_run(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    _make_workload(client, "w1")
    r = client.post(
        f"/api/v1/workers/{wid}/runs",
        json={
            "workload_slug": "w1", "pk": "a", "attempt": 0,
            "started_at": "2026-06-18T00:00:00Z",
            "success": True, "duration_ms": 10, "exit_code": 0,
        },
    )
    assert r.status_code == 201
    assert r.json()["id"].startswith("r_")
    # runs に積まれてる
    out = client.get("/api/v1/workloads/w1/runs").json()
    assert out["total"] == 1


def test_unknown_worker_id_404_on_queue_ops(client: TestClient):
    _make_workload(client, "w1")
    r = client.post(
        "/api/v1/workers/nope/claim",
        json={"workload_slug": "w1", "limit": 1},
    )
    assert r.status_code == 404


def test_unknown_workload_404_on_claim(client: TestClient):
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    r = client.post(
        f"/api/v1/workers/{wid}/claim",
        json={"workload_slug": "nope", "limit": 1},
    )
    assert r.status_code == 404
