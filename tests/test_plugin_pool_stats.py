"""MariaPool の借用実測が 「待ち」 と 「DB 往復」 を分けて出せること。

2026-08-24: image-pull の download 段で max_inflight=96 に対し実働スレッドが
23 前後しか無かった。 候補は (a) プールが細くて待たされている (size=12)、
(b) DB 自体が遅い、 (c) メインスレッドが窓を補充していない —— の 3 つで、
phase_secs からはどれも見えなかった。 (a) と (b) を分けるのがこの計器。

`wait_s` は空き slot を待った時間、 `ping_s` は借りた接続の生存確認、
`connect_s` は張り直しにかかった時間。 3 つを足しても acquire の総時間には
ならない (待ちと往復は別のスレッドで並行する) ので、 比で読むこと。
"""

from __future__ import annotations

import threading
import time

import pytest

from pipeline.db.plugin_pool import MariaPool


class _FakeConn:
    def __init__(self, ping_delay: float = 0.0, ping_ok_times: int | None = None):
        self.autocommit = False
        self.closed = False
        self._ping_delay = ping_delay
        # None = 常に成功。 n = n 回成功したあとは 「サーバが切った」 を模す。
        self._ping_ok_times = ping_ok_times
        self.ping_calls = 0

    def ping(self):
        self.ping_calls += 1
        if self._ping_delay:
            time.sleep(self._ping_delay)
        if (self._ping_ok_times is not None
                and self.ping_calls > self._ping_ok_times):
            raise RuntimeError("server has gone away")

    def close(self):
        self.closed = True


class _FakeMariaDB:
    """connect() のたびに新しい _FakeConn を返す最小のスタブ。"""

    def __init__(self, ping_delay: float = 0.0, connect_delay: float = 0.0,
                 ping_ok_times: int | None = None):
        self.made: list[_FakeConn] = []
        self._ping_delay = ping_delay
        self._connect_delay = connect_delay
        self._ping_ok_times = ping_ok_times

    def connect(self, **cfg):
        if self._connect_delay:
            time.sleep(self._connect_delay)
        c = _FakeConn(self._ping_delay, self._ping_ok_times)
        self.made.append(c)
        return c


def _pool(size=2, **kw):
    return MariaPool({"host": "x"}, size=size, mariadb_mod=_FakeMariaDB(**kw))


# ------------------------------------------------------------ 基本の計数 --

def test_acquires_are_counted():
    p = _pool()
    for _ in range(3):
        p.release(p.acquire())
    assert p.stats()["acquires"] == 3


def test_first_acquire_connects_and_is_timed():
    p = _pool(size=1, connect_delay=0.02)
    p.release(p.acquire())
    st = p.stats()
    assert st["connects"] == 1
    assert st["connect_s"] >= 0.015
    assert st["ping_s"] == 0.0, "新規接続で ping は打たない"


def test_reused_connection_is_pinged_not_reconnected():
    p = _pool(size=1, ping_delay=0.02)
    p.release(p.acquire())          # 1 本目 = connect
    p.release(p.acquire())          # 2 本目 = 再利用 + ping
    st = p.stats()
    assert st["connects"] == 1
    assert st["ping_s"] >= 0.015


def test_size_is_reported_for_occupancy_reading():
    assert _pool(size=7).stats()["size"] == 7


# ------------------------------------------------------- 待ちが出ること --

def test_wait_is_measured_when_pool_is_exhausted():
    """size より多いスレッドが来たら待ちが出る = プールが細いことの証拠。"""
    p = _pool(size=1)
    held = p.acquire()
    got: list[float] = []

    def _worker():
        t0 = time.perf_counter()
        c = p.acquire()
        got.append(time.perf_counter() - t0)
        p.release(c)

    t = threading.Thread(target=_worker)
    t.start()
    time.sleep(0.05)
    p.release(held)
    t.join(timeout=5)
    assert got and got[0] >= 0.04
    assert p.stats()["wait_s"] >= 0.04
    assert p.stats()["wait_max_s"] >= 0.04


def test_no_wait_when_pool_is_idle():
    p = _pool(size=4)
    for _ in range(4):
        p.release(p.acquire())
    assert p.stats()["wait_s"] < 0.01


def test_wait_max_tracks_the_worst_case():
    p = _pool(size=1)
    p.release(p.acquire())
    p._record(0.5, 0.0, 0.0, 0)
    p._record(0.1, 0.0, 0.0, 0)
    assert p.stats()["wait_max_s"] == pytest.approx(0.5)


# ------------------------------------------------------- 失敗経路の計上 --

def test_dead_connection_is_replaced_and_counted():
    """ping 失敗 → 張り直し。 connects が増え、 slot は枯れない。"""
    fake = _FakeMariaDB(ping_ok_times=0)   # 借り直しの ping で必ず落ちる
    p = MariaPool({"host": "x"}, size=1, mariadb_mod=fake)
    first = p.acquire()             # connect (ping は打たない)
    p.release(first)
    c = p.acquire()                 # 再利用 → ping 失敗 → 張り直し
    p.release(c)
    st = p.stats()
    assert st["connects"] == 2
    assert st["acquires"] == 2
    assert first.closed, "死んだ接続は閉じる"
    assert p.acquire() is not None, "slot が枯れていない"


def test_connect_failure_still_records_the_attempt():
    class _Boom:
        def connect(self, **cfg):
            raise RuntimeError("nope")
    p = MariaPool({"host": "x"}, size=1, mariadb_mod=_Boom())
    with pytest.raises(RuntimeError):
        p.acquire()
    assert p.stats()["acquires"] == 1, "失敗も借用試行として数える"


# ------------------------------------------------------------ tick 区切り --

def test_reset_returns_previous_and_zeroes():
    p = _pool()
    p.release(p.acquire())
    prev = p.reset_stats()
    assert prev["acquires"] == 1
    now = p.stats()
    assert now["acquires"] == 0 and now["wait_s"] == 0.0
    assert now["size"] == prev["size"], "size は tick を跨いで残る"


def test_stats_snapshot_is_not_live():
    """返した dict を後から書き換えられても内部が汚れないこと。"""
    p = _pool()
    p.release(p.acquire())
    snap = p.stats()
    snap["acquires"] = 999
    assert p.stats()["acquires"] == 1


def test_concurrent_acquires_do_not_lose_counts():
    """96 スレッドで回すので加算が壊れないことは押さえる。"""
    p = _pool(size=8)
    n = 40

    def _w():
        for _ in range(10):
            p.release(p.acquire())

    ts = [threading.Thread(target=_w) for _ in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=10)
    assert p.stats()["acquires"] == n * 10
