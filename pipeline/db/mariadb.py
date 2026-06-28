"""MariaDB アダプタ (= 業務 queue 用、 Phase 2-α 実装)。

URL: ``mariadb://user:pass@host:port/dbname`` or ``mysql://...``。
pymysql 経由で接続。 sqlite.py と同じ ``Database / Connection / Cursor`` interface。

設計方針 (= 2026-06-29 ユーザ判断):
- pipeline-oss control plane state (= workloads / workers / runs / sessions / settings 等)
  は SQLite を SoT とする。
- 業務 task queue (= image-*, video-*, paprika-*, embed-write, face-person-link) は
  本 MariaDB adapter で MariaDB に格納。 監視/バックアップ一元化 + flow 図 で metric_sql
  で depth 可視化。
- 準システム queue (= pipeline-supervisor / cpu-lane-scaler) は SQLite 維持。

QueueRepository が ``primary_db`` (= SQLite) と ``secondary_db`` (= MariaDB) を持ち、
workload.queue_backend ('sqlite' or 'mariadb') で transaction を切替える。
"""

from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

from pipeline.db.base import Connection, Cursor, Database


# ---------------------------------------------------------------------------
# paramstyle 変換: SQLite ":name" → pymysql "%(name)s"
# ---------------------------------------------------------------------------
_PARAM_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")


def _convert_named_params(sql: str) -> str:
    """``:name`` 形式の SQLite named パラメータを ``%(name)s`` (pymysql named) に変換。"""
    return _PARAM_RE.sub(r"%(\1)s", sql)


# ---------------------------------------------------------------------------
# UPDATE WHERE pk IN (SELECT ... FROM same_table) の MariaDB 対応:
#   MariaDB は同 table 参照を そのままだと error 1093:
#     "You can't specify target table 'X' for update in FROM clause"
#   → derived-table で 1 段 wrap すれば回避できる。
# ---------------------------------------------------------------------------
_DERIVED_WRAP_RE = re.compile(
    r"(WHERE\s+pk\s+IN\s*\(\s*)(SELECT\s+pk\s+FROM\s+(\w+)\s+WHERE\s+.*?LIMIT\s+%\(\w+\)s)(\s*\))",
    re.IGNORECASE | re.DOTALL,
)


def _wrap_derived_for_mariadb(sql: str) -> str:
    """UPDATE ... WHERE pk IN (SELECT pk FROM same_table ...) を derived-table で wrap。"""
    return _DERIVED_WRAP_RE.sub(
        lambda m: f"{m.group(1)}SELECT pk FROM ({m.group(2)}) AS _tmp_claim_pks{m.group(4)}",
        sql,
    )


# ---------------------------------------------------------------------------
# SQLite "INSERT OR IGNORE" → MariaDB "INSERT IGNORE"
# ---------------------------------------------------------------------------
_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\b", re.IGNORECASE)


def _convert_dialect(sql: str) -> str:
    """SQLite 系 syntax を MariaDB 系に変換。"""
    sql = _INSERT_OR_IGNORE_RE.sub("INSERT IGNORE", sql)
    sql = _convert_named_params(sql)
    sql = _wrap_derived_for_mariadb(sql)
    return sql


# ---------------------------------------------------------------------------
# param 変換: SQLite は datetime を ISO8601 文字列 ("...T...+00:00") で保存するが、
#   MariaDB の DATETIME は 'YYYY-MM-DD HH:MM:SS.ffffff' (T/タイムゾーン不可) 。
#   QueueRepository は UTC 前提なので tz を落として wall-clock を保持する。
#   exact-match claim (WHERE claimed_at=:now) を成立させるため、 対象列は
#   DATETIME(6) にしてマイクロ秒を保持する (= ensure_workload_queue / ALTER 側)。
# ---------------------------------------------------------------------------
_ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)?$"
)


def _coerce_param(v: Any) -> Any:
    """ISO8601 datetime 文字列を MariaDB DATETIME 形式に変換。 それ以外は素通し。"""
    if isinstance(v, str) and _ISO_DT_RE.match(v):
        try:
            return datetime.fromisoformat(v).strftime("%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return v
    return v


def _coerce_params(params: Any) -> Any:
    if isinstance(params, dict):
        return {k: _coerce_param(v) for k, v in params.items()}
    if isinstance(params, (list, tuple)):
        return type(params)(_coerce_param(v) for v in params)
    return params


# ---------------------------------------------------------------------------
# Cursor / Connection / Database 実装
# ---------------------------------------------------------------------------

class MariadbCursor(Cursor):
    def __init__(self, cur) -> None:
        self._cur = cur

    def fetchone(self) -> dict[str, Any] | None:
        return self._cur.fetchone()

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._cur.fetchall())

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount


class MariadbConnection(Connection):
    def __init__(self, conn) -> None:
        self._conn = conn

    def execute(self, sql: str, params: dict[str, Any] | tuple = ()) -> MariadbCursor:
        from pymysql.cursors import DictCursor
        sql_converted = _convert_dialect(sql)
        cur = self._conn.cursor(DictCursor)
        # tuple params も dict params も両対応 (= sqlite.py と同型)
        cur.execute(sql_converted, _coerce_params(params) or None)
        return MariadbCursor(cur)

    def executemany(self, sql: str, seq_params: list[tuple]) -> None:
        sql_converted = _convert_dialect(sql)
        cur = self._conn.cursor()
        try:
            cur.executemany(sql_converted, [_coerce_params(p) for p in seq_params])
        finally:
            cur.close()


class MariadbDatabase(Database):
    def __init__(self, url: str) -> None:
        self.url = url
        parsed = urlparse(url)
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or 3306
        # user/password は URL-encode 前提で unquote (= パスワードに @ : / 等を含む場合)
        self._user = unquote(parsed.username) if parsed.username else ""
        self._password = unquote(parsed.password) if parsed.password else ""
        self._db = (parsed.path or "/").lstrip("/")
        if not self._db:
            raise ValueError(f"mariadb URL に database 名が無い: {url!r}")
        # transaction 単位の lock (= 1 connection 共有 + thread safe)
        self._lock = threading.RLock()
        self._conn = self._make_conn()

    def _make_conn(self):
        import pymysql
        return pymysql.connect(
            host=self._host, port=self._port,
            user=self._user, password=self._password,
            database=self._db, autocommit=False,
            charset="utf8mb4",
        )

    def ensure_schema(self) -> None:
        """control plane schema は SQLite 側、 MariaDB は queue table のみ管理。
        ここでは no-op (= workload 毎に ensure_workload_queue で個別作成)。"""
        return

    def ensure_workload_queue(self, queue_table: str) -> None:
        """workload 専用 queue table を idempotent に作成 (= MariaDB DDL)。

        SQLite 版 (pipeline.db.schema.workload_queue_ddl) と意味的に同型:
          - pk = VARCHAR(255) PRIMARY KEY (= MariaDB PK は VARCHAR 必須)
          - state, claimed_by = VARCHAR
          - claimed_at, enqueued_at, updated_at = DATETIME(6)
          - INDEX は table 内に inline 定義 (= MariaDB syntax)
        """
        ddl = f"""
CREATE TABLE IF NOT EXISTS {queue_table} (
    pk          VARCHAR(255) NOT NULL,
    state       VARCHAR(16)  NOT NULL DEFAULT 'pending',
    claimed_at  DATETIME(6),
    claimed_by  VARCHAR(64),
    attempt     INT          NOT NULL DEFAULT 0,
    last_error  TEXT,
    extra       TEXT         NOT NULL DEFAULT '{{}}',
    enqueued_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (pk),
    KEY {queue_table}_idx_claim (state, claimed_at, attempt, pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
        cur = self._conn.cursor()
        try:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            self._conn.commit()
        finally:
            cur.close()

    @contextmanager
    def transaction(self) -> Iterator[MariadbConnection]:
        # 1 connection を thread lock で直列化。 sqlite.py と同型。
        with self._lock:
            # 接続切れ時は ping reconnect (= server timeout 8h 等 対策)
            try:
                self._conn.ping(reconnect=True)
            except Exception:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = self._make_conn()
            wrapper = MariadbConnection(self._conn)
            try:
                yield wrapper
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
