"""queue の complete/fail が deadlock(1213) を retry すること。

claim は自前で retry を持っていたが complete/fail は素通しで、 deadlock がそのまま
500 になり worker 側に httpx エラーとして出ていた。 タスクは claimed のまま残り
lease 失効まで再処理されない (= 二重処理と遅延) ので、 claim と同じ扱いにする。
"""

from __future__ import annotations

import pytest

from pipeline.db.sqlite import SqliteDatabase
from pipeline.models.workload import WorkloadCreate, queue_table_for
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.workloads import WorkloadRepository


class _Deadlock(Exception):
    def __init__(self) -> None:
        super().__init__(1213, "Deadlock found when trying to get lock; try restarting transaction")


@pytest.fixture()
def repo_and_table():
    db = SqliteDatabase("sqlite:///:memory:")
    db.ensure_schema()
    WorkloadRepository(db).create(
        WorkloadCreate(
            slug="dl", name="DL", enabled=True,
            executor_type="shell", executor_config={"command": ["true"]},
        )
    )
    q = QueueRepository(db)
    table = queue_table_for("dl")
    yield q, table
    db.close()


def _flaky(n_failures: int, inner):
    """最初の n 回だけ deadlock を投げ、 以降は inner を実行する。"""
    calls = {"n": 0}

    def _wrapped(*a, **k):
        calls["n"] += 1
        if calls["n"] <= n_failures:
            raise _Deadlock()
        return inner(*a, **k)

    return _wrapped, calls


def test_complete_はdeadlockをretryして完了する(repo_and_table, monkeypatch):
    q, table = repo_and_table
    q.enqueue(table, "pk1")
    orig = q._get_db
    wrapped, calls = _flaky(2, orig)
    monkeypatch.setattr(q, "_get_db", wrapped)

    q.complete(table, "pk1")

    assert calls["n"] == 3, f"retry していない (呼び出し {calls['n']} 回)"
    monkeypatch.undo()
    assert q.count_by_state(table).get("pending", 0) == 0, "row が消えていない"


def test_fail_はdeadlockをretryして完了する(repo_and_table, monkeypatch):
    q, table = repo_and_table
    q.enqueue(table, "pk1")
    q.claim(table, "w1", 1, lease_secs=60)
    orig = q._get_db
    wrapped, calls = _flaky(1, orig)
    monkeypatch.setattr(q, "_get_db", wrapped)

    state = q.fail(table, "pk1", max_attempts=5, error="boom")

    assert calls["n"] == 2, f"retry していない (呼び出し {calls['n']} 回)"
    assert state == "pending"
    monkeypatch.undo()
    # retry しても attempt が二重加算されない (失敗した transaction は rollback 済み)
    assert q.peek(table, limit=1)[0]["attempt"] == 1


def test_retryを使い切ったら例外を上げる(repo_and_table, monkeypatch):
    q, table = repo_and_table
    q.enqueue(table, "pk1")
    wrapped, calls = _flaky(99, q._get_db)
    monkeypatch.setattr(q, "_get_db", wrapped)

    with pytest.raises(Exception) as ei:
        q.complete(table, "pk1")
    assert "deadlock" in str(ei.value).lower()
    assert calls["n"] == 5, f"試行回数が想定と違う ({calls['n']})"


def test_deadlock以外のエラーはretryせず即座に上げる(repo_and_table, monkeypatch):
    q, table = repo_and_table
    q.enqueue(table, "pk1")
    calls = {"n": 0}

    def _boom(*a, **k):
        calls["n"] += 1
        raise ValueError("disk on fire")

    monkeypatch.setattr(q, "_get_db", _boom)
    with pytest.raises(ValueError):
        q.complete(table, "pk1")
    assert calls["n"] == 1, "deadlock でないのに retry している"
