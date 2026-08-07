"""QueueRepository / RunsRepository の bulk API のテスト。

2026-08-07 のボトルネック対処で足した complete_many / fail_many / claimable_tables /
start_many / finish_many と、 空振り claim の短絡の挙動を固定する。
"""

from __future__ import annotations

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


def _make_workload(db, slug: str, max_attempts: int = 2) -> str:
    WorkloadRepository(db).create(
        WorkloadCreate(
            slug=slug,
            name=slug,
            executor_type="shell",
            executor_config={"command": ["true"]},
            max_attempts=max_attempts,
        )
    )
    return queue_table_for(slug)


@pytest.fixture()
def queue_table(db):
    return _make_workload(db, "echo")


# ---------------- complete_many ----------------


def test_complete_many_deletes_all_pks(db, queue_table):
    repo = QueueRepository(db)
    repo.enqueue_many(queue_table, [(f"p{i}", {}) for i in range(5)])
    assert repo.count_by_state(queue_table)["pending"] == 5

    deleted = repo.complete_many(queue_table, ["p0", "p1", "p2"])

    assert deleted == 3
    assert repo.count_by_state(queue_table) == {"pending": 2}


def test_complete_many_is_noop_for_empty_and_unknown_pks(db, queue_table):
    repo = QueueRepository(db)
    repo.enqueue(queue_table, "p0")

    assert repo.complete_many(queue_table, []) == 0
    assert repo.complete_many(queue_table, ["nope"]) == 0
    assert repo.count_by_state(queue_table)["pending"] == 1


def test_complete_many_handles_more_pks_than_bind_limit(db, queue_table):
    """IN (...) の chunk 分割が効いていること (= bind 変数上限を越える件数)。"""
    repo = QueueRepository(db)
    pks = [f"p{i}" for i in range(1200)]        # _IN_CHUNK=400 の 3 倍
    repo.enqueue_many(queue_table, [(pk, {}) for pk in pks])

    assert repo.complete_many(queue_table, pks) == 1200
    assert repo.count_by_state(queue_table) == {}


# ---------------- fail_many ----------------


def test_fail_many_retries_until_max_attempts(db, queue_table):
    """max_attempts=2 なので 1 回目は pending 復帰、 2 回目で failed 打切り。"""
    repo = QueueRepository(db)
    repo.enqueue_many(queue_table, [("a", {}), ("b", {})])

    first = repo.fail_many(queue_table, [("a", "boom"), ("b", "boom")], max_attempts=2)
    assert first == {"a": "pending", "b": "pending"}
    assert repo.count_by_state(queue_table)["pending"] == 2

    second = repo.fail_many(queue_table, [("a", "boom again")], max_attempts=2)
    assert second == {"a": "failed"}
    counts = repo.count_by_state(queue_table)
    assert counts == {"pending": 1, "failed": 1}


def test_fail_many_matches_single_fail_semantics(db):
    """fail() を 2 回呼んだ場合と fail_many() で同じ状態になること。"""
    qt_single = _make_workload(db, "single", max_attempts=3)
    qt_bulk = _make_workload(db, "bulk", max_attempts=3)
    repo = QueueRepository(db)
    repo.enqueue(qt_single, "x")
    repo.enqueue(qt_bulk, "x")

    repo.fail(qt_single, "x", max_attempts=3, error="e1")
    repo.fail_many(qt_bulk, [("x", "e1")], max_attempts=3)

    a = repo.peek(qt_single)[0]
    b = repo.peek(qt_bulk)[0]
    assert (a["state"], a["attempt"], a["last_error"]) == \
           (b["state"], b["attempt"], b["last_error"])


def test_fail_many_treats_missing_pk_as_failed(db, queue_table):
    repo = QueueRepository(db)
    assert repo.fail_many(queue_table, [("ghost", "gone")], max_attempts=5) == \
           {"ghost": "failed"}


# ---------------- claimable_tables ----------------


def test_claimable_tables_reports_only_tables_with_work(db):
    full = _make_workload(db, "full")
    empty = _make_workload(db, "empty")
    repo = QueueRepository(db)
    repo.enqueue(full, "p0")

    got = repo.claimable_tables([(full, 300), (empty, 300)])

    assert got == {full}


def test_claimable_tables_counts_expired_lease_as_claimable(db, queue_table):
    """lease 切れの claimed も claim 候補 (= claim() の WHERE と同じ判定)。"""
    repo = QueueRepository(db)
    repo.enqueue(queue_table, "p0")
    repo.claim(queue_table, worker_id="w1", limit=1, lease_secs=300)

    # lease 内は候補ではない
    assert repo.claimable_tables([(queue_table, 300)]) == set()

    # claimed_at を過去に倒す (= lease 切れ) と再び候補になる
    with db.transaction() as conn:
        conn.execute(
            f"UPDATE {queue_table} SET claimed_at = '2000-01-01T00:00:00.000000+00:00'"
        )
    assert repo.claimable_tables([(queue_table, 300)]) == {queue_table}


def test_claimable_tables_fails_open_on_missing_table(db):
    """判定に失敗した表は「候補あり」 として返す (= fleet を止めない)。"""
    repo = QueueRepository(db)
    assert repo.claimable_tables([("no_such_queue", 300)]) == {"no_such_queue"}


def test_claimable_tables_empty_input(db):
    assert QueueRepository(db).claimable_tables([]) == set()


# ---------------- 空振り claim の短絡 ----------------


def test_claim_on_empty_queue_returns_empty_without_write(db, queue_table):
    """候補 0 件の claim が UPDATE を発行しないこと。"""
    repo = QueueRepository(db)
    seen: list[str] = []
    orig = SqliteDatabase.transaction

    import contextlib

    from pipeline.db import sqlite as sqlite_mod

    @contextlib.contextmanager
    def spy(self):
        with orig(self) as conn:
            real_execute = conn.execute

            def execute(sql, params=()):
                seen.append(" ".join(sql.split()))
                return real_execute(sql, params)

            conn.execute = execute   # type: ignore[method-assign]
            yield conn

    sqlite_mod.SqliteDatabase.transaction = spy
    try:
        assert repo.claim(queue_table, worker_id="w1", limit=5, lease_secs=300) == []
    finally:
        sqlite_mod.SqliteDatabase.transaction = orig

    assert not any(s.upper().startswith("UPDATE") for s in seen), seen


def test_claim_still_works_when_tasks_exist(db, queue_table):
    """短絡を入れても通常の claim 挙動が変わらないこと。"""
    repo = QueueRepository(db)
    repo.enqueue_many(queue_table, [("a", {"k": 1}), ("b", {})])

    got = repo.claim(queue_table, worker_id="w1", limit=5, lease_secs=300)

    assert {t.pk for t in got} == {"a", "b"}
    assert repo.count_by_state(queue_table) == {"claimed": 2}
    # 2 回目は候補が無いので空
    assert repo.claim(queue_table, worker_id="w2", limit=5, lease_secs=300) == []


# ---------------- runs の bulk ----------------


def test_start_many_and_finish_many(db):
    runs = RunsRepository(db)
    ids = runs.start_many([
        {"workload_slug": "s", "pk": f"p{i}", "worker_id": "w1",
         "attempt": 0, "started_at": "2026-08-07T00:00:00+00:00"}
        for i in range(3)
    ])
    assert len(ids) == 3 and len(set(ids)) == 3
    assert len(runs.list_running(limit=10)) == 3

    updated = runs.finish_many([
        {"run_id": rid, "success": True, "exit_code": 0, "duration_ms": 5,
         "output_json": {"n": 1}}
        for rid in ids
    ])

    assert updated == 3
    assert runs.list_running(limit=10) == []
    rows = runs.list_for_workload("s", limit=10)
    assert all(r["success"] for r in rows)
    assert rows[0]["output_json"] == {"n": 1}


def test_start_many_empty_is_noop(db):
    runs = RunsRepository(db)
    assert runs.start_many([]) == []
    assert runs.finish_many([]) == 0
    assert runs.record_many([]) == []


def test_record_many_inserts_finished_rows(db):
    runs = RunsRepository(db)
    ids = runs.record_many([
        {"workload_slug": "s", "pk": "p0", "worker_id": "w1", "attempt": 0,
         "started_at": "2026-08-07T00:00:00+00:00", "success": False,
         "exit_code": 1, "duration_ms": 3, "error": "boom"},
    ])

    assert len(ids) == 1
    assert runs.list_running(limit=10) == []           # 実行中には残らない
    row = runs.list_for_workload("s", limit=10)[0]
    assert row["success"] is False and row["error"] == "boom"


def test_claimable_tables_fail_open_false_skips_broken_table(db):
    """fail_open=False なら判定できなかった表は候補に含めない (= preempt 誤検知防止)。"""
    repo = QueueRepository(db)
    assert repo.claimable_tables([("no_such_queue", 300)], fail_open=False) == set()


def test_claimable_tables_include_expired_false_ignores_expired_lease(db, queue_table):
    """include_expired=False は state='pending' だけを見る (= 旧 count_by_state 相当)。"""
    repo = QueueRepository(db)
    repo.enqueue(queue_table, "p0")
    repo.claim(queue_table, worker_id="w1", limit=1, lease_secs=300)
    with db.transaction() as conn:
        conn.execute(
            f"UPDATE {queue_table} SET claimed_at = '2000-01-01T00:00:00.000000+00:00'"
        )

    assert repo.claimable_tables([(queue_table, 300)], include_expired=True) == {queue_table}
    assert repo.claimable_tables([(queue_table, 300)], include_expired=False) == set()
