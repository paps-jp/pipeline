"""tank の COUNT(*) は server 側で打ち切られ、 失敗しても連打されない。

2026-10-29: `max_statement_time` (MariaDB では秒) に ms の値を渡していたため
120000 = 33 時間となり実質無制限だった。 driver の read_timeout (125s) で client だけ
諦め、 失敗は cache に残らないので次の snapshot で即再実行 → server 上に
`COUNT(*) FROM crawl_person` が 2 分毎に 1 本ずつ積み上がり、 最長 31 分走っていた。
"""

from __future__ import annotations

import sys
import types

import pytest

from pipeline.api import flow


class _Cur:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql):
        self.conn.executed.append(sql)
        if sql.startswith("SELECT") and self.conn.fail:
            raise TimeoutError("read timeout")

    def fetchone(self):
        return (42,)

    def close(self):
        pass


class _Conn:
    def __init__(self, log, fail):
        self.executed = log
        self.fail = fail

    def cursor(self):
        return _Cur(self)

    def close(self):
        pass


@pytest.fixture
def db(monkeypatch):
    state = {"executed": [], "fail": False}
    m = types.ModuleType("mariadb")
    m.connect = lambda **k: _Conn(state["executed"], state["fail"])
    monkeypatch.setitem(sys.modules, "mariadb", m)
    monkeypatch.setattr(flow, "_read_db_cfg", lambda: {"host": "x"})
    monkeypatch.setattr(flow, "_tank_cache", {})
    return state


def test_statement_timeout_is_passed_in_seconds(db):
    flow._tank_query({"t": "SELECT COUNT(*) FROM crawl_person"},
                     stmt_timeout_ms=flow._TANK_BG_STMT_TIMEOUT_MS)
    assert "SET SESSION max_statement_time=120" in db["executed"]


def test_failed_heavy_tank_is_not_retried_within_ttl(db):
    db["fail"] = True
    sqls = {"t": "SELECT COUNT(*) FROM crawl_person"}
    flow._tank_query(sqls)
    n = sum(s.startswith("SELECT") for s in db["executed"])
    # 2 回目の snapshot: TTL 内なので実行せず、 前回の失敗をそのまま返す
    res = flow._exec_tanks(sqls, {"t": 900.0})
    assert sum(s.startswith("SELECT") for s in db["executed"]) == n
    assert res["t"][0] is None and res["t"][1]


def test_failure_keeps_previous_value(db):
    sqls = {"t": "SELECT COUNT(*) FROM crawl_person"}
    flow._tank_query(sqls)
    db["fail"] = True
    flow._tank_query(sqls)
    assert flow._tank_cache["t"][1] == 42
