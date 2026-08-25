"""プラグイン用の有界 MariaDB 接続プール (MariaPool)。

crawl 系プラグインは control plane の DB アダプタ (pipeline.db.mariadb) ではなく、
delian(crawl) DB へ db_cfg で **直接** 接続する。 並列 DL スレッドが 1 スレッド 1 接続
を恒久保持すると 1 worker で parallel_dl 本 (image-pull なら 48 本) の接続を張り、
fleet 全体で max_connections=300 を突き破って OperationalError を撒く
([[mariadb-max-connections-300-ceiling]])。 これを防ぐための共有有界プール。

- プロセス内スレッド間でのみ共有 (接続は socket なのでプロセス跨ぎ不可)。
- LifoQueue に None sentinel を size 個積み、 借用時に遅延接続。 connect 失敗時は
  slot(None) を戻してプールを枯らさない。 acquire 時に ping して死んだ接続を張り直す。
- db_cfg (host/port/user/password/database …) だけに依存し plugin state に非結合。
  2026-07-22 に image-pull の内製 _MariaPool を昇格 (commit a0502c0 が初出)。

使い方::

    from pipeline.db.plugin_pool import MariaPool
    pool = MariaPool(db_cfg, size=12)            # autocommit=True 既定
    db = pool.acquire()
    try:
        cur = db.cursor(); cur.execute(...); ...
    finally:
        pool.release(db)                          # 失敗時は release(db, broken=True)
    ...
    pool.close_all()                              # teardown で
"""
from __future__ import annotations

import queue as _queue
import threading
import time
from typing import Any

DEFAULT_POOL_TIMEOUT_S = 30.0


def _zero_stats() -> dict:
    return {"acquires": 0, "wait_s": 0.0, "wait_max_s": 0.0,
            "ping_s": 0.0, "connect_s": 0.0, "connects": 0}


class MariaPool:
    """有界 MariaDB 接続プール。 size 本を上限に借用/返却する。

    Parameters
    ----------
    db_cfg:
        ``mariadb.connect(**db_cfg)`` に渡す接続設定 (host/port/user/password/database 等)。
    size:
        同時に張る接続の上限。 並列スレッド数 (parallel_dl 等) より小さくしてよい
        (DB 書き込みは短時間なので少数で捌ける)。 既定 12。
    autocommit:
        取得する接続の autocommit。 既定 True。 明示 commit 制御が要る plugin
        (paprika-video-pull 等) は False を渡す。
    mariadb_mod:
        テスト/DI 用に mariadb モジュールを注入できる。 省略時は import mariadb。
    init_sql:
        新規に張った物理接続へ 1 回だけ流す SQL (例:
        ``SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED``)。
        REPEATABLE READ (既定) は UNIQUE 索引への並列 INSERT で gap-lock を取り
        1213 (deadlock) を頻発させる ([[insert-ignore-deadlock-hotspot-fixes]])。
        pool は接続を使い回すので session 変数として 1 回設定すれば以後の
        acquire でも維持される (ping での再接続時は _connect() 経由で再度流れる)。
    """

    def __init__(self, db_cfg: dict, size: int = 12, *,
                 autocommit: bool = True, mariadb_mod: Any = None,
                 init_sql: str | None = None):
        if mariadb_mod is None:
            import mariadb as mariadb_mod  # type: ignore
        self._mariadb = mariadb_mod
        self._cfg = dict(db_cfg)
        self._autocommit = bool(autocommit)
        self._init_sql = init_sql
        self._size = max(1, int(size))
        self._q: "_queue.LifoQueue" = _queue.LifoQueue(maxsize=self._size)
        for _ in range(self._size):
            self._q.put(None)
        # 借用の実測。 「プールが細くて待たされている」のか 「DB 自体が遅い」のか
        # は外から見分けが付かないので、 待ち (queue.get) と ping と connect を
        # 分けて積む。 加算は DB 往復 (ms) に対し十分軽い。
        self._lock = threading.Lock()
        self._stats = _zero_stats()

    @property
    def size(self) -> int:
        return self._size

    def _connect(self):
        db = self._mariadb.connect(**self._cfg)
        db.autocommit = self._autocommit
        if self._init_sql:
            cur = db.cursor()
            try:
                cur.execute(self._init_sql)
            finally:
                cur.close()
        return db

    def acquire(self, timeout: float = DEFAULT_POOL_TIMEOUT_S):
        """接続を 1 本借りる。 全借用中なら空くまで最大 timeout 秒ブロック。"""
        _t0 = time.perf_counter()
        conn = self._q.get(timeout=timeout)
        _wait = time.perf_counter() - _t0
        _ping = 0.0
        _conn_s = 0.0
        _new = 0
        try:
            if conn is None:
                _t1 = time.perf_counter()
                try:
                    out = self._connect()
                except Exception:
                    self._q.put(None)   # connect 失敗: slot を戻して size を保つ
                    raise
                _conn_s = time.perf_counter() - _t1
                _new = 1
                return out
            _t1 = time.perf_counter()
            try:
                conn.ping()             # 死んだ接続を検知して張り直す
                _ping = time.perf_counter() - _t1
                return conn
            except Exception:
                _ping = time.perf_counter() - _t1
                try:
                    conn.close()
                except Exception:
                    pass
                _t2 = time.perf_counter()
                try:
                    out = self._connect()
                except Exception:
                    self._q.put(None)
                    raise
                _conn_s = time.perf_counter() - _t2
                _new = 1
                return out
        finally:
            self._record(_wait, _ping, _conn_s, _new)

    def _record(self, wait_s: float, ping_s: float, connect_s: float, new: int) -> None:
        with self._lock:
            st = self._stats
            st["acquires"] += 1
            st["wait_s"] += wait_s
            st["ping_s"] += ping_s
            st["connect_s"] += connect_s
            st["connects"] += new
            if wait_s > st["wait_max_s"]:
                st["wait_max_s"] = wait_s

    def stats(self) -> dict:
        """借用実測のスナップショット。 size を併せて返す (占有率の判断用)。"""
        with self._lock:
            out = dict(self._stats)
        out["size"] = self._size
        return out

    def reset_stats(self) -> dict:
        """直前の値を返してゼロに戻す。 tick 単位で見るため。"""
        with self._lock:
            out = dict(self._stats)
            self._stats = _zero_stats()
        out["size"] = self._size
        return out

    def release(self, conn, broken: bool = False):
        """接続を返す。 broken=True は close して None(空 slot) を戻す。"""
        if broken and conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
        try:
            self._q.put_nowait(conn)
        except _queue.Full:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def close_all(self) -> int:
        """全接続を閉じる (teardown 専用)。 返り値 = 閉じた本数。
        借用中スレッドが居ない状態 (pool を使う ThreadPoolExecutor の
        shutdown(wait=True) 後) で呼ぶこと。"""
        n = 0
        while True:
            try:
                conn = self._q.get_nowait()
            except _queue.Empty:
                break
            if conn is not None:
                try:
                    conn.close()
                    n += 1
                except Exception:
                    pass
        return n
