"""worker API の batch 系 endpoint と claimable ヒントのテスト。

2026-08-07 のボトルネック対処 (perf/control-plane-bottleneck.md) で足した
`runs/start-batch` / `batch-result` / `WorkloadsForWorkerResponse.claimable` と、
`complete` の bulk 化の挙動を固定する。
"""

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


def _make_workload(client, slug: str = "w1", **over) -> str:
    body = {
        "slug": slug, "name": slug, "enabled": True,
        "executor_type": "shell", "executor_config": {"command": ["true"]},
        "batch_size": 10, "lease_secs": 300, "max_attempts": 2,
    }
    body.update(over)
    r = client.post("/api/v1/workloads", json=body)
    assert r.status_code == 201, r.text
    return slug


def _register(client, host: str = "h1") -> str:
    return client.post("/api/v1/workers", json={"host": host}).json()["id"]


def _enqueue(client, slug: str, n: int) -> None:
    r = client.post(f"/api/v1/workloads/{slug}/tasks/batch",
                    json={"items": [{"pk": f"p{i}", "extra": {}} for i in range(n)]})
    assert r.status_code == 201, r.text


def _claim(client, wid: str, slug: str, limit: int = 10) -> list[dict]:
    r = client.post(f"/api/v1/workers/{wid}/claim",
                    json={"workload_slug": slug, "limit": limit})
    assert r.status_code == 200, r.text
    return r.json()["tasks"]


# ---------------- complete の bulk 化 ----------------


def test_complete_removes_all_pks(client: TestClient):
    slug = _make_workload(client)
    wid = _register(client)
    _enqueue(client, slug, 5)
    tasks = _claim(client, wid, slug)

    r = client.post(f"/api/v1/workers/{wid}/complete",
                    json={"workload_slug": slug, "pks": [t["pk"] for t in tasks]})

    assert r.status_code == 204
    assert client.get(f"/api/v1/workloads/{slug}/queue").json()["total"] == 0


# ---------------- claimable ヒント ----------------


def test_workloads_offer_includes_claimable_hint(client: TestClient):
    """queue が空の workload は offer には残るが claimable には載らない。"""
    _make_workload(client, "has-work")
    _make_workload(client, "no-work")
    wid = _register(client)
    _enqueue(client, "has-work", 1)

    body = client.get(f"/api/v1/workers/{wid}/workloads").json()

    offered = {w["slug"] for w in body["workloads"]}
    assert offered == {"has-work", "no-work"}       # offer からは外さない (= executor 保護)
    assert body["claimable"] == ["has-work"]


def test_claimable_empty_when_no_queue_has_work(client: TestClient):
    _make_workload(client, "a")
    _make_workload(client, "b")
    wid = _register(client)

    body = client.get(f"/api/v1/workers/{wid}/workloads").json()

    assert len(body["workloads"]) == 2
    assert body["claimable"] == []


def test_claimable_drops_slug_once_drained(client: TestClient):
    slug = _make_workload(client)
    wid = _register(client)
    _enqueue(client, slug, 1)
    assert client.get(f"/api/v1/workers/{wid}/workloads").json()["claimable"] == [slug]

    tasks = _claim(client, wid, slug)
    client.post(f"/api/v1/workers/{wid}/complete",
                json={"workload_slug": slug, "pks": [t["pk"] for t in tasks]})

    body = client.get(f"/api/v1/workers/{wid}/workloads").json()
    assert [w["slug"] for w in body["workloads"]] == [slug]   # offer は維持
    assert body["claimable"] == []


# ---------------- runs/start-batch ----------------


def test_start_batch_creates_running_runs(client: TestClient):
    slug = _make_workload(client)
    wid = _register(client)

    r = client.post(f"/api/v1/workers/{wid}/runs/start-batch", json={
        "workload_slug": slug,
        "items": [{"pk": f"p{i}", "attempt": 0,
                   "started_at": "2026-08-07T00:00:00+00:00"} for i in range(3)],
    })

    assert r.status_code == 201, r.text
    ids = r.json()["ids"]
    assert len(ids) == 3 and len(set(ids)) == 3
    # Dashboard の「実行中」は runs.finished_at IS NULL を見るので、ここに出ること
    running = client.get("/api/v1/dashboard/overview").json()["running"]
    assert {x["pk"] for x in running} == {"p0", "p1", "p2"}


def test_start_batch_404_for_unknown_worker(client: TestClient):
    slug = _make_workload(client)
    r = client.post("/api/v1/workers/nope/runs/start-batch", json={
        "workload_slug": slug,
        "items": [{"pk": "p0", "attempt": 0, "started_at": "2026-08-07T00:00:00+00:00"}],
    })
    assert r.status_code == 404


# ---------------- batch-result ----------------


def test_batch_result_completes_successes_and_finishes_runs(client: TestClient):
    slug = _make_workload(client)
    wid = _register(client)
    _enqueue(client, slug, 3)
    tasks = _claim(client, wid, slug)
    ids = client.post(f"/api/v1/workers/{wid}/runs/start-batch", json={
        "workload_slug": slug,
        "items": [{"pk": t["pk"], "attempt": t["attempt"],
                   "started_at": "2026-08-07T00:00:00+00:00"} for t in tasks],
    }).json()["ids"]

    r = client.post(f"/api/v1/workers/{wid}/batch-result", json={
        "workload_slug": slug,
        "results": [
            {"pk": t["pk"], "attempt": t["attempt"], "run_id": rid,
             "success": True, "exit_code": 0, "duration_ms": 7,
             "output_json": {"n": 1}}
            for t, rid in zip(tasks, ids, strict=True)
        ],
    })

    assert r.status_code == 200, r.text
    assert r.json() == {"completed": 3, "failed": 0, "retry_pks": [], "dead_pks": []}
    assert client.get(f"/api/v1/workloads/{slug}/queue").json()["total"] == 0
    assert client.get("/api/v1/dashboard/overview").json()["running"] == []
    runs = client.get(f"/api/v1/workloads/{slug}/runs").json()["runs"]
    assert len(runs) == 3 and all(x["success"] for x in runs)


def test_batch_result_retries_failures_until_max_attempts(client: TestClient):
    slug = _make_workload(client, max_attempts=2)
    wid = _register(client)
    _enqueue(client, slug, 1)

    def _round():
        tasks = _claim(client, wid, slug)
        assert tasks, "task should be claimable"
        return client.post(f"/api/v1/workers/{wid}/batch-result", json={
            "workload_slug": slug,
            "results": [{"pk": tasks[0]["pk"], "attempt": tasks[0]["attempt"],
                         "started_at": "2026-08-07T00:00:00+00:00",
                         "success": False, "exit_code": 1, "duration_ms": 1,
                         "error": "boom"}],
        }).json()

    first = _round()
    assert first["completed"] == 0 and first["failed"] == 1
    assert first["retry_pks"] == ["p0"] and first["dead_pks"] == []

    second = _round()
    assert second["retry_pks"] == [] and second["dead_pks"] == ["p0"]
    assert client.get(f"/api/v1/workloads/{slug}/queue").json()["by_state"] == {"failed": 1}


def test_batch_result_without_run_id_records_history(client: TestClient):
    """start-batch を打てなかった場合でも run 履歴が残ること。"""
    slug = _make_workload(client)
    wid = _register(client)
    _enqueue(client, slug, 2)
    tasks = _claim(client, wid, slug)

    r = client.post(f"/api/v1/workers/{wid}/batch-result", json={
        "workload_slug": slug,
        "results": [{"pk": t["pk"], "attempt": t["attempt"],
                     "started_at": "2026-08-07T00:00:00+00:00",
                     "success": True, "exit_code": 0, "duration_ms": 2}
                    for t in tasks],
    })

    assert r.status_code == 200
    assert r.json()["completed"] == 2
    runs = client.get(f"/api/v1/workloads/{slug}/runs").json()["runs"]
    assert len(runs) == 2
    assert client.get("/api/v1/dashboard/overview").json()["running"] == []


def test_batch_result_mixed_success_and_failure(client: TestClient):
    slug = _make_workload(client, max_attempts=5)
    wid = _register(client)
    _enqueue(client, slug, 4)
    tasks = _claim(client, wid, slug)

    r = client.post(f"/api/v1/workers/{wid}/batch-result", json={
        "workload_slug": slug,
        "results": [
            {"pk": t["pk"], "attempt": t["attempt"],
             "started_at": "2026-08-07T00:00:00+00:00",
             "success": i % 2 == 0, "exit_code": 0 if i % 2 == 0 else 1,
             "duration_ms": 1, "error": None if i % 2 == 0 else "boom"}
            for i, t in enumerate(tasks)
        ],
    })

    body = r.json()
    assert body["completed"] == 2
    assert body["failed"] == 2
    assert sorted(body["retry_pks"]) == sorted([t["pk"] for i, t in enumerate(tasks) if i % 2])
    assert client.get(f"/api/v1/workloads/{slug}/queue").json()["by_state"] == {"pending": 2}


def test_batch_result_404_for_unknown_workload(client: TestClient):
    wid = _register(client)
    r = client.post(f"/api/v1/workers/{wid}/batch-result", json={
        "workload_slug": "ghost",
        "results": [{"pk": "p0", "attempt": 0, "success": True,
                     "started_at": "2026-08-07T00:00:00+00:00"}],
    })
    assert r.status_code == 404


def test_batch_result_is_idempotent_for_repeated_success(client: TestClient):
    """再送 (= worker の retry) しても状態が壊れないこと。"""
    slug = _make_workload(client)
    wid = _register(client)
    _enqueue(client, slug, 1)
    tasks = _claim(client, wid, slug)
    payload = {
        "workload_slug": slug,
        "results": [{"pk": tasks[0]["pk"], "attempt": 0,
                     "started_at": "2026-08-07T00:00:00+00:00",
                     "success": True, "exit_code": 0, "duration_ms": 1}],
    }

    first = client.post(f"/api/v1/workers/{wid}/batch-result", json=payload).json()
    second = client.post(f"/api/v1/workers/{wid}/batch-result", json=payload).json()

    assert first["completed"] == 1
    assert second["completed"] == 0          # 既に消えている
    assert client.get(f"/api/v1/workloads/{slug}/queue").json()["total"] == 0
