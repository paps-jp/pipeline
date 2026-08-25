"""/api/v1/mariadb-tables の統合テスト。

MariaDB は持ち込まず、`app.state.mariadb_admin_backend` に sqlite3 (in-memory) を
`%s` プレースホルダ互換の薄いラッパーで差し込む (= test_api_public_create.py と
同じ「fake backend を app.state に注入する」方針)。 `pipeline.db.mariadb_admin`
が生成する SQL 文そのものを実エンジンで実行して検証する。
"""

from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from pipeline.api.mariadb_tables import _Backend
from pipeline.config import Settings
from pipeline.control.server import create_app
from pipeline.db.mariadb_admin import (
    TABLE_REGISTRY,
    ColumnNotEditableError,
    list_rows,
    update_row,
)


class FakeDb:
    """`pipeline.storage.stores.Db` 互換の薄いラッパー (sqlite3 + %s プレースホルダ変換)。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def ping(self) -> None:
        pass

    def execute(self, sql: str, params: tuple = ()):
        return self._conn.execute(sql.replace("%s", "?"), params)


@pytest.fixture
def fake_db() -> FakeDb:
    # TestClient は endpoint をワーカースレッドで実行するため check_same_thread=False。
    # 実アクセスは呼び出し側の lock (Db 同様スレッド非安全) で直列化される想定。
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(
        "CREATE TABLE crawl_config (id INTEGER PRIMARY KEY, site TEXT, url TEXT, "
        "domain TEXT, type TEXT, enabled INTEGER, next_no INTEGER, "
        "max_pages_per_run INTEGER, memo TEXT, locked_at TEXT, worker_id TEXT)"
    )
    rows = [
        (1, "example_com", "https://example.com", "example.com", "image", 1, 5, 0, None, None, None),
        (2, "blocked_site", "https://blocked.example", "blocked.example", "image", 99, 1, 0, "99 ブロックされている", None, None),
        (3, "asset_host", "https://img.cdn.example", "img.cdn.example", "image", 0, 1, 0, "crawl-host-import (asset host)", None, None),
        (4, "another_com", "https://another.com", "another.com", "image", 1, 2, 0, None, None, None),
    ]
    conn.executemany(
        "INSERT INTO crawl_config VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.execute(
        "CREATE TABLE host_import_queue (id INTEGER PRIMARY KEY, source_host_id INTEGER, "
        "domain TEXT, top_url TEXT, status TEXT, last_error_type TEXT, last_error TEXT, "
        "last_http_status INTEGER, next_retry_at TEXT)"
    )
    conn.commit()
    return FakeDb(conn)


@pytest.fixture
def client(fake_db: FakeDb) -> TestClient:
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    app = create_app(settings)
    app.state.mariadb_admin_backend = _Backend(fake_db)
    return TestClient(app)


# ---------------- pipeline.db.mariadb_admin (unit) ----------------


def test_list_rows_search_and_pagination(fake_db: FakeDb) -> None:
    spec = TABLE_REGISTRY["crawl_config"]
    lock = threading.Lock()

    rows, total = list_rows(fake_db, lock, spec, limit=2, offset=0)
    assert total == 4
    assert len(rows) == 2

    rows, total = list_rows(fake_db, lock, spec, q="example.com", limit=50, offset=0)
    assert total == 1
    assert rows[0]["domain"] == "example.com"


def test_list_rows_enabled_filter(fake_db: FakeDb) -> None:
    spec = TABLE_REGISTRY["crawl_config"]
    lock = threading.Lock()
    rows, total = list_rows(fake_db, lock, spec, enabled=99, limit=50, offset=0)
    assert total == 1
    assert rows[0]["site"] == "blocked_site"


def test_update_row_editable_column(fake_db: FakeDb) -> None:
    spec = TABLE_REGISTRY["crawl_config"]
    lock = threading.Lock()
    updated = update_row(fake_db, lock, spec, 3, {"enabled": 1, "memo": "手動で復活"})
    assert updated["enabled"] == 1
    assert updated["memo"] == "手動で復活"


def test_update_row_rejects_non_editable_column(fake_db: FakeDb) -> None:
    spec = TABLE_REGISTRY["crawl_config"]
    lock = threading.Lock()
    with pytest.raises(ColumnNotEditableError):
        update_row(fake_db, lock, spec, 1, {"next_no": 999})


# ---------------- HTTP 層 ----------------


def test_list_tables(client: TestClient) -> None:
    r = client.get("/api/v1/mariadb-tables/tables")
    assert r.status_code == 200, r.text
    names = {t["name"] for t in r.json()["tables"]}
    assert names == {"crawl_config", "host_import_queue"}


def test_get_rows_http(client: TestClient) -> None:
    r = client.get("/api/v1/mariadb-tables/tables/crawl_config/rows?enabled=99")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1
    assert body["rows"][0]["site"] == "blocked_site"


def test_get_rows_unknown_table_404(client: TestClient) -> None:
    r = client.get("/api/v1/mariadb-tables/tables/no_such_table/rows")
    assert r.status_code == 404


def test_patch_row_http(client: TestClient) -> None:
    r = client.patch("/api/v1/mariadb-tables/tables/crawl_config/rows/2", json={"enabled": 1})
    assert r.status_code == 200, r.text
    assert r.json()["enabled"] == 1


def test_patch_row_rejects_bad_column(client: TestClient) -> None:
    r = client.patch("/api/v1/mariadb-tables/tables/crawl_config/rows/1", json={"next_no": 999})
    assert r.status_code == 400
