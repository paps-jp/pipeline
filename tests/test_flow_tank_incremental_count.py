"""差分カウント tank: 増えた id だけ数えて足し、 遅れてコミットされた行も漏らさない。

2026-11-02: crawl (5.5億行) の全件 COUNT(*) は 289s かかり max_statement_time で
毎回 1969 になっていた。 TABLE_ROWS の概算は統計の取り直しで -17% まで飛ぶので、
flow_tank_counter に確定件数を持って増分だけ数える方式にした。
"""

from __future__ import annotations

import re
import sys
import types

import pytest

from pipeline.api import flow


class _DB:
    """crawl 表 (コミット済みの id 集合) と flow_tank_counter だけを持つ偽 MariaDB。"""

    def __init__(self):
        self.rows: set[int] = set()
        # tid → [settled_id, count, seen_max, age_s, recount_gen]
        self.counter: dict[str, list] = {}
        self.full_counts = 0
        self.fail_full_count = False


class _Cur:
    def __init__(self, db: _DB):
        self.db = db
        self.res = None

    def execute(self, sql, params=()):
        db = self.db
        if sql.startswith(("SET ", "START ", "CREATE ")):
            return
        if sql.startswith("SELECT settled_id"):
            c = db.counter.get(params[0])
            self.res = tuple(c) if c else None
        elif sql.startswith("SELECT MAX("):
            self.res = (max(db.rows) if db.rows else None,)
        elif re.search(r"WHERE `id` > %s AND `id` <= %s", sql):
            lo, hi = params
            self.res = (sum(lo < i <= hi for i in db.rows),)
        elif re.search(r"WHERE `id` > %s$", sql):
            self.res = (sum(i > params[0] for i in db.rows),)
        elif sql.startswith("SELECT COUNT(*)"):
            db.full_counts += 1
            if db.fail_full_count:
                raise TimeoutError("max_statement_time exceeded")
            self.res = (len(db.rows),)
        elif sql.startswith("UPDATE flow_tank_counter"):
            settled, cnt, seen, tid, gen = params
            c = db.counter.get(tid)
            if c and c[4] == gen:
                c[:3] = [settled, cnt, seen]
        elif sql.startswith("INSERT INTO flow_tank_counter"):
            tid, settled, cnt, seen = params
            gen = db.counter[tid][4] + 1 if tid in db.counter else 0
            db.counter[tid] = [settled, cnt, seen, 0, gen]
        else:
            raise AssertionError(f"unexpected sql: {sql}")

    def fetchone(self):
        return self.res

    def close(self):
        pass


class _Conn:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return _Cur(self.db)

    def commit(self):
        pass

    def close(self):
        pass


@pytest.fixture
def db(monkeypatch):
    d = _DB()
    m = types.ModuleType("mariadb")
    m.connect = lambda **k: _Conn(d)
    monkeypatch.setitem(sys.modules, "mariadb", m)
    monkeypatch.setattr(flow, "_read_db_cfg", lambda: {"host": "x"})
    monkeypatch.setattr(flow, "_tank_cache", {})
    monkeypatch.setattr(flow, "_incr_recount_inflight", set())
    monkeypatch.setattr(flow, "_incr_recount_failed_at", {})
    monkeypatch.setattr(flow, "_incr_ddl_done", False)

    # 数え直しはスレッドを立てずにその場で走らせる
    def _sync_start(tid, table, key):
        with flow._tank_cache_lock:
            if tid in flow._incr_recount_inflight:
                return
            failed = flow._incr_recount_failed_at.get(tid)
            if failed is not None:
                return
            flow._incr_recount_inflight.add(tid)
        flow._incr_recount_bg(tid, table, key)
    monkeypatch.setattr(flow, "_start_recount", _sync_start)
    return d


SPEC = "crawl:id"


def _count(d):
    return flow._incr_count(_Conn(d), "crawl", SPEC)


def test_first_call_recounts_then_counts_new_rows(db):
    db.rows = set(range(1, 11))
    # 初回: 基準行が無い → 全件数え直しを起こし、 その tick は値なし
    assert _count(db) is None
    assert db.counter["crawl"][:3] == [10, 10, 10]
    db.rows |= {11, 12, 20}
    assert _count(db) == 13
    assert db.full_counts == 1           # 2 回目以降は全件を数えない


def test_late_commit_below_seen_max_is_not_lost(db):
    db.rows = set(range(1, 11))
    _count(db)
    # id 13 は振られたがまだコミットされていない。 14 は先にコミットされた。
    db.rows |= {11, 12, 14}
    assert _count(db) == 13
    db.rows.add(13)                      # 13 が遅れてコミット
    assert _count(db) == 14
    assert _count(db) == 14              # 確定済みが 14 まで進んでも二重に数えない
    assert db.counter["crawl"][:2] == [14, 14]


def test_recount_after_interval_corrects_deletes(db):
    db.rows = set(range(1, 11))
    _count(db)
    db.rows -= {2, 3}                    # 削除は増分では反映されない
    assert _count(db) == 10
    db.counter["crawl"][3] = flow._INCR_RECOUNT_INTERVAL_S   # 1 日経過
    # この tick は数え直し前に読んだ値を返すが、 数え直しの結果は上書きしない
    _count(db)
    assert _count(db) == 8


def test_failed_recount_is_not_retried_immediately(db):
    db.rows = set(range(1, 11))
    db.fail_full_count = True
    assert _count(db) is None
    assert _count(db) is None
    assert db.full_counts == 1
    assert "crawl" in flow._incr_recount_failed_at


def test_bad_identifier_is_rejected(db):
    with pytest.raises(ValueError):
        flow._incr_count(_Conn(db), "x", "crawl; DROP TABLE crawl:id")


def test_tank_query_routes_count_table(db):
    db.rows = set(range(1, 6))
    sql = flow._tank_sql({"id": "crawl", "count_table": "crawl"})
    assert flow._tank_query({"crawl": sql})["crawl"] == (None, None)
    db.rows.add(6)
    assert flow._tank_query({"crawl": sql})["crawl"] == (6, None)
