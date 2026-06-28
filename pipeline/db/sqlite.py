"""SQLite アダプタ。

dev mode のリファレンス実装。`sqlite:///path/to.db` の URL を受け付け、
WAL mode で開く。
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from pipeline.db.base import Connection, Cursor, Database
from pipeline.db.schema import ALL_DDL, WORKER_METRICS_ALTERS, WORKERS_ALTERS, WORKLOADS_ALTERS, workload_queue_ddl


class SqliteCursor(Cursor):
    def __init__(self, cur: sqlite3.Cursor) -> None:
        self._cur = cur

    def fetchone(self) -> dict[str, Any] | None:
        row = self._cur.fetchone()
        return dict(row) if row else None

    def fetchall(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self._cur.fetchall()]

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount


class SqliteConnection(Connection):
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, params: dict[str, Any] | tuple = ()) -> SqliteCursor:
        return SqliteCursor(self._conn.execute(sql, params))

    def executemany(self, sql: str, seq_params: list[tuple]) -> None:
        self._conn.executemany(sql, seq_params)


class SqliteDatabase(Database):
    def __init__(self, url: str) -> None:
        self.url = url
        # sqlite:///./pipeline.db → ./pipeline.db
        # sqlite:///:memory:      → :memory:
        parsed = urlparse(url)
        path = parsed.path
        if path.startswith("/"):
            # sqlite:///./pipeline.db の path は "/./pipeline.db" になる
            # sqlite:///:memory:        の path は "/:memory:"
            path = path[1:]
        if path in ("", ":memory:"):
            self._path = ":memory:"
        else:
            self._path = str(Path(path).expanduser().resolve())
        # check_same_thread=False で worker thread からも触れる想定。
        # 但し 1 接続を複数 thread が同時操作すると transaction 状態が壊れるので
        # _lock で transaction を直列化 (= LocalDbLogHandler 等の background thread と
        # FastAPI async endpoint の競合を防ぐ)。
        import threading
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA foreign_keys = ON")

    def ensure_schema(self) -> None:
        for ddl in ALL_DDL:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    self._conn.execute(stmt)
        self._conn.commit()
        # 後から追加された列 (= 既存 DB に対する idempotent ALTER)
        for stmt in WORKER_METRICS_ALTERS + WORKLOADS_ALTERS + WORKERS_ALTERS:
            try:
                self._conn.execute(stmt)
            except sqlite3.OperationalError as e:
                # "duplicate column name" は無視 (= 既に追加済)
                if "duplicate column" not in str(e).lower():
                    raise
        self._conn.commit()
        self._migrate_queue_table_naming()
        self._drop_legacy_plugins_table()
        self._drop_input_source_columns()

    def _drop_legacy_plugins_table(self) -> None:
        """plugins テーブル (= 旧 Plugin Registry) を DROP (= Phase C 移行後の cleanup)。

        移行後の SoT は git で管理されたローカル plugins/ ディレクトリ。 SQLite は
        もう不要。 idempotent: テーブルが無ければ no-op。
        """
        try:
            self._conn.execute("DROP TABLE IF EXISTS plugins")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

    def _drop_input_source_columns(self) -> None:
        """workloads.input_source_type / input_source_config column を DROP (= Phase F)。

        どの code path も読まないため完全廃止。 SQLite 3.35+ で ALTER TABLE DROP COLUMN
        が使える。 idempotent: 既に column が無ければ OperationalError を握り潰し。
        """
        for col in ("input_source_type", "input_source_config"):
            try:
                self._conn.execute(f"ALTER TABLE workloads DROP COLUMN {col}")
                self._conn.commit()
            except sqlite3.OperationalError:
                pass

    def _migrate_queue_table_naming(self) -> None:
        """queue_<slug> 形式の旧 queue table を <slug>_queue に rename。

        命名規約変更 (queue prefix → suffix) の idempotent migration。
        起動時に毎回走るが、 対象が無ければ no-op。
        """
        try:
            rows = self._conn.execute(
                "SELECT slug, queue_table FROM workloads WHERE queue_table LIKE 'queue\\_%' ESCAPE '\\'"
            ).fetchall()
        except sqlite3.OperationalError:
            return  # workloads 表が無い (= 初回起動前)
        for row in rows:
            slug = row["slug"]
            old_name = row["queue_table"]
            new_name = f"{slug.replace('-', '_')}_queue"
            if old_name == new_name:
                continue
            # 旧 index も rename (= INDEX 名は old_name_idx_claim)
            try:
                self._conn.execute(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"')
                # 旧 index は新名で自動連動するが、 INDEX 名自体は古いまま残る
                # → 古い INDEX を DROP + 新規 INDEX を CREATE で正規化
                self._conn.execute(f'DROP INDEX IF EXISTS "{old_name}_idx_claim"')
                self._conn.execute(
                    f'CREATE INDEX IF NOT EXISTS "{new_name}_idx_claim" '
                    f'ON "{new_name}" (state, claimed_at, attempt, pk)'
                )
                self._conn.execute(
                    "UPDATE workloads SET queue_table = :new WHERE slug = :slug",
                    {"new": new_name, "slug": slug},
                )
            except sqlite3.OperationalError as exc:
                # rename 衝突 (= 新名のテーブルが既に存在) は無視 (= 手動移行済)
                if "already exists" not in str(exc):
                    raise
        self._conn.commit()

    def ensure_workload_queue(self, queue_table: str) -> None:
        for stmt in workload_queue_ddl(queue_table).split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(stmt)
        self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[SqliteConnection]:
        # 全 transaction を直列化 (= 1 接続を複数 thread が同時 access する時の競合防止)
        with self._lock:
            wrapper = SqliteConnection(self._conn)
            try:
                yield wrapper
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        self._conn.close()

    @property
    def path(self) -> str:
        return self._path
