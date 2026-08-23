"""GC を tick のクリティカルパスから外す (`gc_async`)。

段別計測で tick の **64%** が GC (gc_prefix 0.158 + gc_keys 0.031 s/job) だと
判明した。 download は 0.091 s/job しか使っていない。 実際「148 秒使って cursor
前進 0」の tick が観測されており、 hub の生産 (380-614 jobs/分) に対して消費が
203 jobs/分 で遅れが広がり続けていた。

GC は省略できない (.47 を空にできるのは image-pull だけ) が、 **判定と実行は
分離できる**。 判定はその tick の download 結果そのものなので tick 内でしか
作れないが、 実行は job_id とキーのリストさえあればいつ誰がやってもよい。

ここで固定するのは 4 点:
  1. **既定はオフ** — 入れただけでは挙動が変わらない
  2. async なら tick は積むだけで返る (実行は常駐スレッド)
  3. **キュー満杯なら同期実行へ縮退する** (backpressure)。 積み続けると .47 が
     溢れるので、 速くする改造が壊れたときに勝手に安全側へ倒れる形にする
  4. 同期/非同期で **実行本体は同一** (`_gc_run_batch`) — 二重実装にしない

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def im():
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")
        img.open = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stub"))
        pil.Image = img
        sys.modules["PIL"] = pil
        sys.modules["PIL.Image"] = img
    spec = importlib.util.spec_from_file_location("image_main_gc_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def calls(im, monkeypatch):
    """GC の下請けを全部差し替えて、 何が呼ばれたかだけ記録する。"""
    rec = {"purge": [], "prefix": [], "keys": [], "delay": 0.0}

    def fake_purge(hub, job_ids, timeout=120.0, chunk=200, concurrency=1):
        if rec["delay"]:
            time.sleep(rec["delay"])
        rec["purge"].append(list(job_ids))
        return (len(job_ids), 3 * len(job_ids), 100 * len(job_ids))

    monkeypatch.setattr(im, "_hub_purge_jobs", fake_purge)
    monkeypatch.setattr(im, "_delete_hub_job_prefixes",
                        lambda c, b, ids: (rec["prefix"].append(list(ids)), (0, 0))[1])
    monkeypatch.setattr(im, "_delete_hub_assets_batch",
                        lambda c, b, ks: (rec["keys"].append(list(ks)), len(ks))[1])
    return rec


def _state(im, **kw):
    st = {
        "paprika_hub": "http://hub.test:8000",
        "hub_ram_client": object(),
        "hub_minio": [object()],
        "hub_bucket": "paprika",
        "purge_chunk": 50,
        "purge_concurrency": 4,
        "gc_async": False,
        "gc_queue_max": im.GC_QUEUE_MAX,
        "gc_queue": None,
        "gc_thread": None,
    }
    st.update(kw)
    return st


def _drain(im, st, timeout=5.0):
    q = st.get("gc_queue")
    if q is None:
        return
    t0 = time.time()
    while not q.empty() and time.time() - t0 < timeout:
        time.sleep(0.01)
    q.join()


# ------------------------------------------------------------ 既定オフ --

def test_default_is_synchronous(im):
    """入れただけでは挙動が変わらないこと。"""
    import inspect
    assert 'kwargs.get("gc_async", 0)' in inspect.getsource(im.setup)


def test_sync_path_executes_inline_and_reports(im, calls):
    st = _state(im)
    out: dict = {}
    im._gc_submit(st, out, ["j1", "j2"], ["jobs/j9/assets/a.jpg"])
    assert calls["purge"] == [["j1", "j2"]]
    assert calls["keys"] == [["jobs/j9/assets/a.jpg"]]
    assert out["hub_gc_prefix_jobs"] == 2
    assert out["hub_gc"] == 1
    assert st["gc_thread"] is None, "同期モードでスレッドを起こさない"


def test_nothing_to_do_is_a_noop(im, calls):
    st = _state(im)
    out: dict = {}
    im._gc_submit(st, out, [], [])
    assert calls["purge"] == [] and calls["keys"] == []
    assert out == {}


# ---------------------------------------------------------- 非同期経路 --

def test_async_returns_without_executing(im, calls):
    """tick は積むだけ。 実行はスレッド側。"""
    calls["delay"] = 0.3
    st = _state(im, gc_async=True)
    out: dict = {}
    t0 = time.time()
    im._gc_submit(st, out, ["j1"], [])
    elapsed = time.time() - t0
    assert elapsed < 0.1, "tick が GC の完了を待ってしまっている"
    assert out["gc_queued"] == 1
    _drain(im, st)
    assert calls["purge"] == [["j1"]]


def test_async_worker_accumulates_stats(im, calls):
    st = _state(im, gc_async=True)
    out: dict = {}
    im._gc_submit(st, out, ["a", "b", "c"], ["k1", "k2"])
    _drain(im, st)
    out2: dict = {}
    im._gc_drain_stats(st, out2)
    done = out2["gc_done"]
    assert done["jobs"] == 3 and done["keys"] == 2 and done["batches"] == 1
    # 一度出したらリセットされる (毎 tick 二重計上しない)
    out3: dict = {}
    im._gc_drain_stats(st, out3)
    assert "gc_done" not in out3


def test_worker_is_reused_not_respawned(im, calls):
    st = _state(im, gc_async=True)
    im._gc_submit(st, {}, ["a"], [])
    th1 = st["gc_thread"]
    im._gc_submit(st, {}, ["b"], [])
    assert st["gc_thread"] is th1
    assert th1.daemon, "daemon でないと worker 終了時に落ちない"
    _drain(im, st)


# ------------------------------------------------------- backpressure --

def test_full_queue_falls_back_to_sync(im, calls, monkeypatch):
    """満杯なら同期実行へ縮退する。 積み続けて .47 を溢れさせない。"""
    monkeypatch.setattr(im, "GC_PUT_TIMEOUT_S", 0.05)
    st = _state(im, gc_async=True, gc_queue_max=1)
    # スレッドを止めた状態でキューを埋める
    q = im._gc_ensure_worker(st)
    st["gc_thread"].join(0)          # 走ってはいるが、 delay で塞ぐ
    calls["delay"] = 2.0
    im._gc_submit(st, {}, ["blocker"], [])   # スレッドがこれを掴んで塞がる
    time.sleep(0.2)
    q.put((["filler"], []))                  # キュー本体を満杯にする
    out: dict = {}
    im._gc_submit(st, out, ["overflow"], [])
    assert out.get("gc_backpressure") is True
    assert ["overflow"] in calls["purge"], "縮退したのに実行されていない"
    calls["delay"] = 0.0


def test_backpressure_reports_queue_depth(im, calls):
    st = _state(im, gc_async=True)
    out: dict = {}
    im._gc_submit(st, out, ["a"], [])
    assert "gc_qdepth" in out, "キュー深さが見えないと詰まりに気づけない"
    _drain(im, st)


# ------------------------------------------------- 実行本体は 1 つだけ --

def test_both_paths_share_one_implementation(im, calls):
    """同期/非同期で結果が一致すること (二重実装だと片方だけ直す事故になる)。"""
    st_sync = _state(im)
    out_sync: dict = {}
    im._gc_submit(st_sync, out_sync, ["x", "y"], ["k"])
    sync_calls = (list(calls["purge"]), list(calls["keys"]))

    calls["purge"].clear(); calls["keys"].clear()
    st_async = _state(im, gc_async=True)
    im._gc_submit(st_async, {}, ["x", "y"], ["k"])
    _drain(im, st_async)
    assert (calls["purge"], calls["keys"]) == sync_calls


def test_worker_survives_a_failing_batch(im, monkeypatch):
    """1 batch が例外でもスレッドは死なない (死ぬと以後 GC が永久停止する)。"""
    boom = {"n": 0}

    def flaky(hub, job_ids, timeout=120.0, chunk=200, concurrency=1):
        boom["n"] += 1
        if boom["n"] == 1:
            raise RuntimeError("transient")
        return (len(job_ids), 0, 0)

    monkeypatch.setattr(im, "_hub_purge_jobs", flaky)
    monkeypatch.setattr(im, "_delete_hub_job_prefixes", lambda c, b, ids: (0, 0))
    monkeypatch.setattr(im, "_delete_hub_assets_batch", lambda c, b, ks: 0)
    st = _state(im, gc_async=True)
    im._gc_submit(st, {}, ["a"], [])
    _drain(im, st)
    im._gc_submit(st, {}, ["b"], [])
    _drain(im, st)
    assert st["gc_thread"].is_alive()
    assert boom["n"] == 2, "2 回目が実行されていない = スレッドが死んだ"
