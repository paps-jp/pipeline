"""Queue + Runs Repository のテスト."""

from __future__ import annotations

import time

import pytest

from pipeline.db.sqlite import SqliteDatabase
from pipeline.models.workload import WorkloadCreate, queue_table_for
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import WorkloadRepository


@pytest.fixture()
def db():
    d = SqliteDatabase("sqlite:///:memory:")
    d.ensure_schema()
    yield d
    d.close()


@pytest.fixture()
def queue_table(db):
    """echo workload を 1 個作って queue_table 名を返す。"""
    repo = WorkloadRepository(db)
    repo.create(
        WorkloadCreate(
            slug="echo",
            name="Echo",
            executor_type="shell",
            executor_config={"command": ["echo", "hi"]},
            max_attempts=2,
        )
    )
    return queue_table_for("echo")


def test_enqueue_inserts_and_dedupes(db, queue_table):
    q = QueueRepository(db)
    assert q.enqueue(queue_table, "a") is True
    assert q.enqueue(queue_table, "a") is False  # duplicate
    assert q.count_by_state(queue_table) == {"pending": 1}


def test_enqueue_many(db, queue_table):
    q = QueueRepository(db)
    n = q.enqueue_many(queue_table, [("x", {"u": 1}), ("y", {}), ("x", {})])
    assert n == 2  # 重複 x はスキップ
    assert q.count_by_state(queue_table)["pending"] == 2


def test_claim_returns_tasks_and_marks_claimed(db, queue_table):
    q = QueueRepository(db)
    q.enqueue(queue_table, "a", {"src": "u1"})
    q.enqueue(queue_table, "b")
    tasks = q.claim(queue_table, worker_id="w1", limit=10, lease_secs=60)
    assert {t.pk for t in tasks} == {"a", "b"}
    assert all(t.attempt == 0 for t in tasks)
    a = next(t for t in tasks if t.pk == "a")
    assert a.extra == {"src": "u1"}
    # 同じ呼出をもう一回 → 既に claimed なので 0 件
    assert q.claim(queue_table, worker_id="w2", limit=10, lease_secs=60) == []


def test_claim_respects_limit(db, queue_table):
    q = QueueRepository(db)
    for i in range(5):
        q.enqueue(queue_table, f"k{i}")
    tasks = q.claim(queue_table, worker_id="w1", limit=2, lease_secs=60)
    assert len(tasks) == 2
    remaining = q.claim(queue_table, worker_id="w2", limit=10, lease_secs=60)
    assert len(remaining) == 3


def test_claim_picks_up_lease_expired(db, queue_table):
    q = QueueRepository(db)
    q.enqueue(queue_table, "a")
    first = q.claim(queue_table, worker_id="w1", limit=10, lease_secs=1)
    assert len(first) == 1
    # まだ lease 内なので別 worker は取れない
    assert q.claim(queue_table, worker_id="w2", limit=10, lease_secs=1) == []
    time.sleep(1.2)
    # lease 切れで再 claim 可
    again = q.claim(queue_table, worker_id="w2", limit=10, lease_secs=1)
    assert [t.pk for t in again] == ["a"]


def test_complete_deletes(db, queue_table):
    q = QueueRepository(db)
    q.enqueue(queue_table, "a")
    q.claim(queue_table, worker_id="w1", limit=10, lease_secs=60)
    q.complete(queue_table, "a")
    assert q.count_by_state(queue_table) == {}


def test_fail_retries_then_marks_failed(db, queue_table):
    # echo workload は max_attempts=2 で作成済
    q = QueueRepository(db)
    q.enqueue(queue_table, "a")
    q.claim(queue_table, worker_id="w1", limit=10, lease_secs=60)
    res1 = q.fail(queue_table, "a", max_attempts=2, error="boom")
    assert res1 == "pending"
    # 再 claim できる
    again = q.claim(queue_table, worker_id="w1", limit=10, lease_secs=60)
    assert [t.attempt for t in again] == [1]
    res2 = q.fail(queue_table, "a", max_attempts=2, error="boom2")
    assert res2 == "failed"
    counts = q.count_by_state(queue_table)
    assert counts == {"failed": 1}
    # failed は再 claim されない
    assert q.claim(queue_table, worker_id="w1", limit=10, lease_secs=60) == []


def test_runs_record_and_list(db):
    runs = RunsRepository(db)
    rid = runs.record(
        workload_slug="echo",
        pk="a",
        worker_id="w1",
        attempt=0,
        started_at="2026-06-18T00:00:00.000Z",
        success=True,
        exit_code=0,
        duration_ms=12,
        stdout="hi\n",
        stderr="",
        output_json=None,
        error=None,
    )
    assert rid.startswith("r_")
    out = runs.list_for_workload("echo", limit=10)
    assert len(out) == 1
    assert out[0]["pk"] == "a"
    assert out[0]["success"] is True
    assert out[0]["stdout"] == "hi\n"


def test_queue_table_name_validation(db):
    q = QueueRepository(db)
    with pytest.raises(ValueError):
        q.enqueue("queue_x;DROP", "a")
    with pytest.raises(ValueError):
        q.enqueue("", "a")
