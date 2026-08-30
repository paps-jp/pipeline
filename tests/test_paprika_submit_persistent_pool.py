"""Regression: the POST pool must outlive a single tick.

2026-08-30. `paprika-job-submit` was capped at ~356 jobs/min while the hub fleet
could absorb 1,202/min (532 lanes / 26.6s per job -- only 30% utilised). The
hub was idle (179-279 free lanes, ``accept_new`` true on 12/12 samples) and so
was the submitter's own pool.

py-spy found the whole gap at the *tick boundary*. The pool and the in-flight
map were locals of ``process()``, so every tick ended with
``_futures_wait(inflight, timeout=40)`` -- and 31 of 50 samples sat there with
only **1-3 POSTs still running while 45+ threads idled**:

    [ submit 30s ][ wait for 1-3 stragglers ~20s ][ submit 30s ][ wait ] ...
                   ^ no submissions happen here at all

The tick interior had already been converted from "wave" to a pipeline; the
boundary was the last wave left. Keeping the pool and the in-flight ledger in
``state`` removes it: whatever is still flying is reaped at the head of the
next tick.

These tests pin the invariants that make that safe. They do not drive a real
tick (that needs MariaDB + hub); they cover the pieces that own the lifetime.
"""

from __future__ import annotations

import time

import pytest

from plugins.paprika_job_submit import submit_main as sm


def _state(**over):
    st = {
        "control_url": "http://control.invalid",
        "workload_slug": "paprika-job-submit",
        "submit_parallel": 48,
        "submit_parallel_max": 256,
        "submit_paused": False,
        "gate_last_check": 0.0,
        "pool": None,
        "inflight": {},
    }
    st.update(over)
    return st


# ── プールの寿命 ─────────────────────────────────────────────────────────────

def test_pool_is_created_once_and_reused(sm_state=None):
    """tick ごとに作り直さない。 作り直すと境界で drain が必要になる。"""
    st = _state()
    p1 = sm._ensure_pool(st)
    p2 = sm._ensure_pool(st)
    assert p1 is p2
    assert st["pool"] is p1
    p1.shutdown(wait=False)


def test_pool_is_sized_to_max_not_effective_parallel():
    """プールは物理上限で作る。 実効並列を上げるたびに作り直さないため。"""
    st = _state(submit_parallel=48, submit_parallel_max=200)
    pool = sm._ensure_pool(st)
    assert pool._max_workers == 200
    pool.shutdown(wait=False)


def test_inflight_ledger_survives_in_state():
    """投げ中の台帳は state 側。 process() のローカルにすると tick 境界で失われる。"""
    st = _state()
    pool = sm._ensure_pool(st)
    fut = pool.submit(lambda: ("job", "created"))
    st["inflight"][fut] = {"id": 1}
    assert st["inflight"] is not None and len(st["inflight"]) == 1
    pool.shutdown(wait=True)
    # 次 tick は state から同じ台帳を受け取る
    assert len(st["inflight"]) == 1


# ── 実効並列の動的更新 ───────────────────────────────────────────────────────

def _wire(monkeypatch, init_kwargs: dict):
    import io
    import json as _json

    class _R:
        def read(self):
            return _json.dumps({"executor_config": {"init_kwargs": init_kwargs}}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(sm.urllib.request, "urlopen", lambda *a, **k: _R())


def test_submit_parallel_is_refreshed_from_config(monkeypatch):
    """supervisor の AIMD が書いた値を次 tick で拾う (再起動不要)。"""
    st = _state(submit_parallel=48)
    _wire(monkeypatch, {"submit_parallel": 96})
    sm._refresh_submit_gate(st)
    assert st["submit_parallel"] == 96


def test_submit_parallel_is_clamped_to_pool_max(monkeypatch):
    """プールに存在しない枠は要求できない。"""
    st = _state(submit_parallel=48, submit_parallel_max=64)
    _wire(monkeypatch, {"submit_parallel": 1000})
    sm._refresh_submit_gate(st)
    assert st["submit_parallel"] == 64


def test_submit_parallel_has_a_floor(monkeypatch):
    """0 にはしない。 0 だと需要も失敗率も観測できず AIMD が再発進できない。

    止めたいときは submit_paused を使う (そちらは tick を回したまま投入だけ止める)。
    """
    st = _state(submit_parallel=48)
    _wire(monkeypatch, {"submit_parallel": 1})
    sm._refresh_submit_gate(st)
    assert st["submit_parallel"] == sm.SUBMIT_PARALLEL_MIN


def test_refresh_is_throttled(monkeypatch):
    """毎 tick 叩かない (control plane への無駄な負荷を避ける)。"""
    st = _state(submit_parallel=48)
    _wire(monkeypatch, {"submit_parallel": 96})
    sm._refresh_submit_gate(st)
    assert st["submit_parallel"] == 96
    _wire(monkeypatch, {"submit_parallel": 128})
    sm._refresh_submit_gate(st)          # 直後なので読みに行かない
    assert st["submit_parallel"] == 96


def test_fetch_failure_keeps_current_values(monkeypatch):
    """control plane が落ちても現行値を維持する (投入を止めない)。"""
    st = _state(submit_parallel=96, submit_paused=True)

    def _boom(*a, **k):
        raise OSError("control plane down")

    monkeypatch.setattr(sm.urllib.request, "urlopen", _boom)
    paused = sm._refresh_submit_gate(st)
    assert st["submit_parallel"] == 96
    assert paused is True                # 停止中なら停止のまま = fail-safe


# ── teardown だけが drain する ───────────────────────────────────────────────

def test_teardown_drains_and_clears(monkeypatch):
    """プロセス終了時だけ待つ。 待たずに落とすと claim 済みの行が宙に浮く。"""
    st = _state()
    pool = sm._ensure_pool(st)
    done = []
    fut = pool.submit(lambda: done.append(1) or ("j", "created"))
    st["inflight"][fut] = {"id": 1}
    st["counter"] = 3
    st["hostname"] = "test"
    sm.teardown(st)
    assert done == [1]
    assert st["inflight"] == {}
    assert st["pool"] is None
