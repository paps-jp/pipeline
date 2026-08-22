"""workloads.min_resident_workers → min_workers 改名マイグレーションの単体テスト。

改名を ADD COLUMN より先に走らせないと、 既存 DB では値を持った旧列が残ったまま
新列が既定 0 で作られ、 フリートの配分 (= min_workers) が全部 0 に化ける。
"""

from __future__ import annotations

import sqlite3

from pipeline.db import get_db


def _legacy_db(path: str) -> None:
    """改名前 schema の workloads を作る (値付き)。"""
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE workloads ("
        " slug TEXT PRIMARY KEY, name TEXT NOT NULL,"
        " queue_table TEXT NOT NULL DEFAULT '',"
        " executor_type TEXT NOT NULL DEFAULT 'shell')"
    )
    con.execute("ALTER TABLE workloads ADD COLUMN min_resident_workers INTEGER NOT NULL DEFAULT 0")
    con.execute("ALTER TABLE workloads ADD COLUMN max_workers INTEGER")
    con.executemany(
        "INSERT INTO workloads (slug, name, min_resident_workers, max_workers) VALUES (?,?,?,?)",
        [("image-hash-extract", "hash", 2, 20), ("video-face-extract", "vfe", 15, 32)],
    )
    con.commit()
    con.close()


def _columns(path: str) -> set[str]:
    con = sqlite3.connect(path)
    try:
        return {r[1] for r in con.execute("PRAGMA table_info(workloads)")}
    finally:
        con.close()


def test_rename_preserves_values(tmp_path) -> None:
    path = str(tmp_path / "legacy.db")
    _legacy_db(path)

    db = get_db(f"sqlite:///{path}")
    db.ensure_schema()

    cols = _columns(path)
    assert "min_workers" in cols
    assert "min_resident_workers" not in cols

    con = sqlite3.connect(path)
    rows = dict(con.execute("SELECT slug, min_workers FROM workloads"))
    con.close()
    # 改名前の値がそのまま残っていること (0 に潰れていないこと)
    assert rows == {"image-hash-extract": 2, "video-face-extract": 15}


def test_rename_is_idempotent(tmp_path) -> None:
    path = str(tmp_path / "legacy.db")
    _legacy_db(path)

    for _ in range(3):
        get_db(f"sqlite:///{path}").ensure_schema()

    con = sqlite3.connect(path)
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(workloads)")]
        assert cols.count("min_workers") == 1
        assert dict(con.execute("SELECT slug, min_workers FROM workloads"))[
            "video-face-extract"
        ] == 15
    finally:
        con.close()


def test_fresh_db_has_new_column_only() -> None:
    db = get_db("sqlite:///:memory:")
    db.ensure_schema()
    with db.transaction() as conn:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(workloads)").fetchall()}
    assert "min_workers" in cols
    assert "min_resident_workers" not in cols
