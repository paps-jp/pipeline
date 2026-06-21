"""SQLite アダプタの単体テスト。in-memory DB を使うので CI 高速。"""

from __future__ import annotations

import pytest

from pipeline.db import get_db
from pipeline.db.sqlite import SqliteDatabase


def test_open_inmemory() -> None:
    db = get_db("sqlite:///:memory:")
    assert isinstance(db, SqliteDatabase)
    db.ensure_schema()

    with db.transaction() as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r["name"] for r in cur.fetchall()]
    # 全 7 表が作られたか
    expected = {"workloads", "workers", "runs", "users", "sessions", "api_keys", "audit_log"}
    assert expected.issubset(set(tables)), f"missing: {expected - set(tables)}"


def test_workload_queue_creation() -> None:
    db = get_db("sqlite:///:memory:")
    db.ensure_schema()
    db.ensure_workload_queue("test_workload_queue")
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_workload_queue'"
        )
        assert cur.fetchone() is not None


def test_unknown_url_scheme() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        get_db("mongodb://localhost/foo")


def test_postgres_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        get_db("postgresql://user:pw@host/db")
