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

import logging
import os
import queue
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

from pipeline.db.base import Connection, Cursor, Database

log = logging.getLogger("pipeline.db.mariadb")


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

def _norm_value(v: Any) -> Any:
    """MariaDB が返す型を SQLite 互換 (= 文字列/数値) に正規化。

    - DATETIME/DATE → ISO8601 文字列 (SQLite は TEXT で保存するため、 pipeline-oss は
      enqueued_at 等を str 前提で扱う。 これを返さないと ClaimedTaskOut 等で
      pydantic ValidationError(string_type) になる)。
    - Decimal (= COUNT/SUM の戻り) → int (整数) or float。
    """
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    return v


def _norm_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: _norm_value(v) for k, v in row.items()}


class MariadbCursor(Cursor):
    def __init__(self, cur) -> None:
        self._cur = cur

    def fetchone(self) -> dict[str, Any] | None:
        return _norm_row(self._cur.fetchone())

    def fetchall(self) -> list[dict[str, Any]]:
        return [_norm_row(r) for r in self._cur.fetchall()]

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
    """MariaDB connection pool (LifoQueue ベース、 thread safe)。

    元は「単一 connection を RLock で直列化」だった。 これだと全 API request が
    1 個の DB 接続を順番待ちで使い、 workers 16 台 + dispatcher 同時アクセス時に
    2-3 秒の serialization latency が発生していた (POST /tasks/batch 実測 2.8s、
    直 SQL は 0.4ms)。 pool 化で並列度を上げる。

    - pool_size は env `PIPELINE_MARIADB_POOL_SIZE` で調整可 (既定 16)。
    - LifoQueue: warm な接続を優先再利用 (idle timeout 対策)。
    - transaction() の finally で必ず return_conn (broken=True で drop)。
    - transaction 途中の例外 → rollback 試行 → 失敗なら broken 判定で drop & 新規作成。
    """

    def __init__(self, url: str, pool_size: int | None = None) -> None:
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
        # pool_size: default 16 (LAN + workers=16 + dispatcher + supervisor で 20 未満が典型)
        env_size = os.environ.get("PIPELINE_MARIADB_POOL_SIZE")
        if pool_size is None:
            pool_size = int(env_size) if env_size and env_size.isdigit() else 16
        self._pool_size = max(1, pool_size)
        # LifoQueue: 直近使った warm 接続を優先取得 → idle-timeout で切られる確率が減る
        self._pool: queue.LifoQueue = queue.LifoQueue(maxsize=self._pool_size)
        self._created_lock = threading.Lock()
        self._created = 0
        # DDL 等の従来 self._conn 直叩き経路の後方互換のため 1 本を保持
        # (ensure_workload_queue は起動時 1 回だけなので直列でも OK)
        self._ddl_lock = threading.Lock()
        self._ddl_conn = self._make_conn()
        # 統計 (可視化用): pool 待ち回数と累計待ち時間
        self._pool_stats = {"acquire_wait_count": 0, "acquire_wait_total_s": 0.0}
        log.info("mariadb pool: size=%d host=%s db=%s", self._pool_size, self._host, self._db)

    def _make_conn(self):
        import pymysql
        return pymysql.connect(
            host=self._host, port=self._port,
            user=self._user, password=self._password,
            database=self._db, autocommit=False,
            charset="utf8mb4",
        )

    def _acquire(self, timeout: float = 30.0):
        """pool から 1 本取得。 なければ pool_size 未満の間は新規作成、
        pool 満杯 & 全部使用中なら timeout 秒待つ (= max_concurrent の役目)。"""
        # (a) 空きがあれば即取得
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            pass
        # (b) 上限未満なら新規作成
        with self._created_lock:
            if self._created < self._pool_size:
                self._created += 1
                try:
                    return self._make_conn()
                except Exception:
                    self._created -= 1
                    raise
        # (c) 満杯 → 待つ
        import time
        t0 = time.monotonic()
        try:
            conn = self._pool.get(timeout=timeout)
        finally:
            elapsed = time.monotonic() - t0
            self._pool_stats["acquire_wait_count"] += 1
            self._pool_stats["acquire_wait_total_s"] += elapsed
        return conn

    def _release(self, conn, broken: bool = False) -> None:
        """conn を pool へ戻す。 broken=True なら close + created counter を減らす。"""
        if broken:
            try:
                conn.close()
            except Exception:
                pass
            with self._created_lock:
                self._created = max(0, self._created - 1)
            return
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            # pool が満杯なら close (通常は起きない = _acquire で足したものが必ず戻る)
            try:
                conn.close()
            except Exception:
                pass
            with self._created_lock:
                self._created = max(0, self._created - 1)

    def pool_stats(self) -> dict[str, Any]:
        """pool 統計を返す (可視化・トラブルシュート用)。"""
        with self._created_lock:
            created = self._created
        return {
            "pool_size": self._pool_size,
            "in_use": created - self._pool.qsize(),
            "idle_in_pool": self._pool.qsize(),
            "total_created": created,
            "acquire_wait_count": self._pool_stats["acquire_wait_count"],
            "acquire_wait_total_s": round(self._pool_stats["acquire_wait_total_s"], 3),
        }

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
        with self._ddl_lock:
            try:
                self._ddl_conn.ping(reconnect=True)
            except Exception:
                try:
                    self._ddl_conn.close()
                except Exception:
                    pass
                self._ddl_conn = self._make_conn()
            cur = self._ddl_conn.cursor()
            try:
                for stmt in ddl.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
                self._ddl_conn.commit()
            finally:
                cur.close()

    @contextmanager
    def transaction(self) -> Iterator[MariadbConnection]:
        # pool から 1 接続を借りて、 finally で必ず返す。
        # 例外時は rollback を試みて、 失敗したら broken 扱いで pool から除外。
        conn = self._acquire()
        # 接続切れ時は ping reconnect (= server timeout 8h 等 対策)
        try:
            conn.ping(reconnect=True)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            with self._created_lock:
                self._created = max(0, self._created - 1)
            conn = self._acquire()  # 新しく作られる (or 別の warm を取得)
            try:
                conn.ping(reconnect=True)
            except Exception:
                # 2 回目も失敗 = 深刻。 broken で返して例外伝播
                self._release(conn, broken=True)
                raise
        wrapper = MariadbConnection(conn)
        broken = False
        try:
            yield wrapper
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                # rollback 自体が失敗 = connection state 不定 → drop
                broken = True
            raise
        finally:
            self._release(conn, broken=broken)

    def close(self) -> None:
        try:
            self._ddl_conn.close()
        except Exception:
            pass
        # pool 内の全 idle connection を close
        while True:
            try:
                c = self._pool.get_nowait()
            except queue.Empty:
                break
            try:
                c.close()
            except Exception:
                pass
