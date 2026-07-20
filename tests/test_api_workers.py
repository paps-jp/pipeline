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


# ---------------- Track B: 単一 workload スカラ ----------------


def test_workload_scalar_derived_from_filter(client: TestClient):
    """WorkerInfo.workload は Track B の派生スカラ: filter が要素1のときのみ slug、
    None/複数 (= 移行残) は None。"""
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]

    # filter 未設定 → workload=None
    info = client.get("/api/v1/workers").json()["workers"][0]
    assert "workload" in info, "WorkerInfo に workload スカラが露出していること"
    assert info["workload"] is None

    # 単一 slug filter → その slug
    r = client.post(f"/api/v1/workers/{wid}/filter", json={"workloads": ["w1"]})
    assert r.status_code == 200
    assert r.json()["workload"] == "w1"
    assert r.json()["workload_filter"] == ["w1"]

    # 複数 slug (移行残) → workload は None、filter 配列は保持
    r = client.post(f"/api/v1/workers/{wid}/filter", json={"workloads": ["w1", "w2"]})
    assert r.status_code == 200
    assert r.json()["workload"] is None
    assert set(r.json()["workload_filter"]) == {"w1", "w2"}

    # 解除 → None に戻る
    r = client.post(f"/api/v1/workers/{wid}/filter", json={"workloads": None})
    assert r.status_code == 200
    assert r.json()["workload"] is None
    assert r.json()["workload_filter"] is None


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


def test_b4_none_is_idle_flag(client: TestClient, monkeypatch):
    """Track B B4: PIPELINE_CLAIM_NONE_IS_IDLE=1 なら filter 無し worker は idle
    (workloads_for_worker が空)。 既定 off なら旧挙動 (優先度で全 workload 受け)。"""
    _make_workload(client, "w1")
    wid = client.post("/api/v1/workers", json={"host": "h"}).json()["id"]
    # flag off (既定): filter 無し worker は w1 を claim 候補にできる
    off = client.get(f"/api/v1/workers/{wid}/workloads").json()
    assert "w1" in [x["slug"] for x in off["workloads"]]
    # flag on: filter 無し worker は idle (空)
    monkeypatch.setenv("PIPELINE_CLAIM_NONE_IS_IDLE", "1")
    on = client.get(f"/api/v1/workers/{wid}/workloads").json()
    assert on["workloads"] == []
    # claim も拒否される (defense-in-depth)
    r = client.post(f"/api/v1/workers/{wid}/claim", json={"workload_slug": "w1", "limit": 1})
    assert r.json()["tasks"] == []


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


# ---------------- per-host concurrency cap (2026-07-21 image-pull 二重稼働対策) ----


def _make_capped_workload(client, slug: str, per_host: int) -> None:
    r = client.post(
        "/api/v1/workloads",
        json={
            "slug": slug, "name": slug, "enabled": True,
            "executor_type": "shell", "executor_config": {"command": ["echo", "hi"]},
            "batch_size": 2, "lease_secs": 30, "max_attempts": 2,
            "max_concurrent_per_host": per_host,
        },
    )
    assert r.status_code == 201, r.text


def test_host_family_strips_cpu_and_digit_suffix():
    from pipeline.api.workers import _host_family
    # GPU / CPU instance はどちらも物理ホスト単位にまとまる
    assert _host_family("ai-gpu1-3") == "ai-gpu1"
    assert _host_family("ai-gpu1-cpu4") == "ai-gpu1"
    assert _host_family("ai-gpu1-cpu2") == "ai-gpu1"
    # 数字でも cpu<N> でもない suffix は温存
    assert _host_family("nas-c2-cpu") == "nas-c2-cpu"
    assert _host_family("delian-prod") == "delian-prod"


def test_claim_cap_blocks_second_cpu_worker_same_host(client: TestClient):
    """同一物理ホストの 2 つの cpu slot が max_concurrent_per_host=1 を破って
    同時に claim できてしまう回帰を防ぐ (image-pull 二重稼働 → 接続 leak の再発防止)。"""
    _make_capped_workload(client, "pull", per_host=1)
    for pk in ["a", "b", "c", "d"]:
        client.post("/api/v1/workloads/pull/tasks", json={"pk": pk})
    a = client.post("/api/v1/workers", json={"host": "ai-gpu1-cpu2"}).json()["id"]
    b = client.post("/api/v1/workers", json={"host": "ai-gpu1-cpu4"}).json()["id"]

    # A が holder になる
    ra = client.post(f"/api/v1/workers/{a}/claim", json={"workload_slug": "pull", "limit": 2})
    assert ra.status_code == 200
    assert len(ra.json()["tasks"]) == 2

    # B (同一物理ホスト ai-gpu1 の別 cpu slot) は cap で弾かれ空を返す
    rb = client.post(f"/api/v1/workers/{b}/claim", json={"workload_slug": "pull", "limit": 2})
    assert rb.status_code == 200
    assert rb.json()["tasks"] == []

    # 別物理ホストの worker は制限されない
    c = client.post("/api/v1/workers", json={"host": "ai-gpu4-cpu1"}).json()["id"]
    rc = client.post(f"/api/v1/workers/{c}/claim", json={"workload_slug": "pull", "limit": 2})
    assert rc.status_code == 200
    assert len(rc.json()["tasks"]) == 2


def test_claim_cap_allows_same_worker_to_keep_claiming(client: TestClient):
    """holder 自身は exclude されるので連続 claim できる (自分で自分を締め出さない)。"""
    _make_capped_workload(client, "pull", per_host=1)
    for pk in ["a", "b", "c", "d"]:
        client.post("/api/v1/workloads/pull/tasks", json={"pk": pk})
    a = client.post("/api/v1/workers", json={"host": "ai-gpu1-cpu2"}).json()["id"]
    r1 = client.post(f"/api/v1/workers/{a}/claim", json={"workload_slug": "pull", "limit": 2})
    assert len(r1.json()["tasks"]) == 2
    r2 = client.post(f"/api/v1/workers/{a}/claim", json={"workload_slug": "pull", "limit": 2})
    assert len(r2.json()["tasks"]) == 2
