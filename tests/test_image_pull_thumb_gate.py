"""ライブプレビュー用サムネを 「見られている時だけ・メインスレッド外で」 送る。

2026-08-24 の計測で、 image-pull の download 段の **94%** が
「メインスレッドがサムネを 1 枚ずつ同期 PUT している時間」 だと分かった
(実測 175s / 188s、 挿入 1 枚につき 28-38ms、 1 tick 4,582 枚)。 画像の取得
自体はその裏で並列に流れており律速ではなかった。 しかもこのサムネは UI の
ライブプレビュー用で、 **誰も見ていなくても**送り続けていた。

対策は 3 つ重ねてある:

  1. プレゼンスゲート — 観察者が居ないなら **生成もしない** (19.8ms/枚)
  2. 非同期化         — 送信を別スレッドへ。 メインスレッドは窓の補充に戻る
  3. レート制限       — 見られている時でも毎秒 N 枚に間引く

**GC との決定的な違いは 「落としてよい」 こと。** GC を落とすと .47 が溢れるので
backpressure は同期実行へ縮退させたが、 サムネは捨ててよい (次の画像がすぐ来る)。
だから満杯時は待たずに捨てる —— メインスレッドを止めないことが目的なのに、
満杯で待ったら本末転倒になる。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import queue as _queue
import sys
import threading
import types
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")

SRC = _IM.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def im():
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")

        def _stub_open(*a, **k):
            raise RuntimeError("stub")

        img.open = _stub_open
        pil.Image = img
        sys.modules["PIL"] = pil
        sys.modules["PIL.Image"] = img
    spec = importlib.util.spec_from_file_location("image_main_thumb_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Resp:
    """urlopen の戻りを模す最小のコンテキストマネージャ。"""

    def __init__(self, payload: bytes):
        self._p = payload

    def read(self):
        return self._p

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Alive:
    """既に走っている worker スレッドの代役 (本物を起こさないため)。"""

    def is_alive(self):
        return True


def _state(**kw):
    st = {"live_preview": True, "live_preview_on_demand": True,
          "control_url": "http://ctrl", "workload_slug": "paprika-image-pull",
          "preview_check_ttl_s": 10.0, "thumb_async": True,
          "thumb_queue_max": 4, "thumb_max_per_s": 0.0}
    st.update(kw)
    return st


def _queued_state(maxsize=100, **kw):
    st = _state(**kw)
    st["thumb_queue"] = _queue.Queue(maxsize=maxsize)
    st["thumb_stats"] = {"sent": 0, "errors": 0}
    st["thumb_lock"] = threading.Lock()
    st["thumb_thread"] = _Alive()
    return st


# ============================================ 1. プレゼンスゲート =========

def test_active_observer_enables_preview(im, monkeypatch):
    monkeypatch.setattr(im.urllib.request, "urlopen",
                        lambda *a, **k: _Resp(b'{"active": true}'))
    assert im._preview_active_cached(_state()) is True


def test_no_observer_disables_preview(im, monkeypatch):
    monkeypatch.setattr(im.urllib.request, "urlopen",
                        lambda *a, **k: _Resp(b'{"active": false}'))
    assert im._preview_active_cached(_state()) is False


def test_live_preview_off_never_asks(im, monkeypatch):
    """機能ごと停止。 照会の HTTP すら打たない。"""
    def _boom(*a, **k):
        raise AssertionError("照会してはいけない")
    monkeypatch.setattr(im.urllib.request, "urlopen", _boom)
    assert im._preview_active_cached(_state(live_preview=False)) is False


def test_on_demand_off_is_the_rollback_switch(im, monkeypatch):
    """旧挙動 (常時 PUT) へ戻すスイッチ。 これも照会しない。"""
    def _boom(*a, **k):
        raise AssertionError("照会してはいけない")
    monkeypatch.setattr(im.urllib.request, "urlopen", _boom)
    assert im._preview_active_cached(_state(live_preview_on_demand=False)) is True


def test_404_falls_back_to_old_behaviour(im, monkeypatch):
    """plugin だけ先に配ったとき、 旧 control plane で挙動を変えない。"""
    def _404(*a, **k):
        raise im.urllib.error.HTTPError("u", 404, "nf", None, None)
    monkeypatch.setattr(im.urllib.request, "urlopen", _404)
    assert im._preview_active_cached(_state()) is True


def test_network_failure_favours_throughput(im, monkeypatch):
    """一時的な通信失敗は OFF に倒す (送らない方が安全)。"""
    def _boom(*a, **k):
        raise OSError("unreachable")
    monkeypatch.setattr(im.urllib.request, "urlopen", _boom)
    assert im._preview_active_cached(_state()) is False


def test_result_is_cached_for_the_ttl(im, monkeypatch):
    """毎 asset ではなく TTL に 1 回だけ訊く。"""
    calls = []

    def _one(*a, **k):
        calls.append(1)
        return _Resp(b'{"active": true}')
    monkeypatch.setattr(im.urllib.request, "urlopen", _one)
    st = _state()
    for _ in range(5):
        im._preview_active_cached(st)
    assert len(calls) == 1


def test_cache_expires(im, monkeypatch):
    calls = []

    def _one(*a, **k):
        calls.append(1)
        return _Resp(b'{"active": true}')
    monkeypatch.setattr(im.urllib.request, "urlopen", _one)
    st = _state()
    im._preview_active_cached(st)
    st["_preview_cache_expire"] = 0.0        # 期限切れを模す
    im._preview_active_cached(st)
    assert len(calls) == 2


# =================================== 2. 生成そのものを止めること =========

def test_thumbnail_generation_is_gated():
    """送らないものを作らない。 19.8ms/枚 がワーカ側で丸ごと浮く。"""
    assert "_make_thumb(tmp_path) if make_thumb else None" in SRC


def test_gate_is_evaluated_once_per_tick_not_per_asset():
    """asset ごとに評価すると照会が数千回になる。"""
    assert SRC.count("_thumb_gate = _preview_active_cached(state)") == 1
    i = SRC.index("_thumb_gate = _preview_active_cached(state)")
    j = SRC.index("asset_tasks.append({")
    assert i < j, "task 構築より前に 1 回だけ決めること"


def test_gate_is_passed_to_the_worker():
    assert '"make_thumb": _thumb_gate,' in SRC


def test_gate_is_reported():
    """出力に出ないと 「なぜ速いのか」 が後から分からない。"""
    assert 'out["thumb_gate"] = _thumb_gate' in SRC


# ============================== 3. 非同期化 / 落としてよいこと ===========

def test_queue_full_drops_instead_of_blocking(im):
    """**ここが GC と逆。** 待ったらメインスレッドを止める意味が無い。"""
    st = _queued_state(maxsize=2)
    assert im._thumb_submit(st, 1, b"a") == "queued"
    assert im._thumb_submit(st, 2, b"b") == "queued"
    assert im._thumb_submit(st, 3, b"c") == "dropped"


def test_sync_mode_is_the_rollback(im, monkeypatch):
    sent = []
    monkeypatch.setattr(im, "_put_blob", lambda c, s, k, d: sent.append(k))
    st = _state(thumb_async=False)
    assert im._thumb_submit(st, 7, b"x") == "sent"
    assert sent == ["thumb/7"]


def test_sync_failure_is_swallowed(im, monkeypatch):
    """PUT が落ちても取込は止めない。"""
    def _boom(*a, **k):
        raise OSError("no")
    monkeypatch.setattr(im, "_put_blob", _boom)
    st = _state(thumb_async=False)
    assert im._thumb_submit(st, 7, b"x") == "dropped"


def test_worker_thread_is_daemon_and_named():
    """常駐スレッドが tick を跨いで残り、 プロセス終了を妨げないこと。"""
    i = SRC.index("def _thumb_ensure_worker")
    seg = SRC[i:i + 900]
    assert 'name="imgpull-thumb"' in seg
    assert "daemon=True" in seg


# ============================================ 4. レート制限 ==============

def test_rate_limit_drops_the_excess(im):
    st = _queued_state(thumb_max_per_s=1.0)
    assert im._thumb_submit(st, 1, b"a") == "queued"
    assert im._thumb_submit(st, 2, b"b") == "dropped", "1 秒に 1 枚を超えている"


def test_rate_limit_disabled_by_zero(im):
    st = _queued_state(thumb_max_per_s=0.0)
    assert [im._thumb_submit(st, i, b"a") for i in range(3)] == ["queued"] * 3


def test_rate_limit_applies_before_the_queue(im):
    """間引いたぶんはキューに積まない (積むと非同期側が無駄に働く)。"""
    st = _queued_state(thumb_max_per_s=1.0)
    im._thumb_submit(st, 1, b"a")
    im._thumb_submit(st, 2, b"b")
    assert st["thumb_queue"].qsize() == 1


# ======================================= 5. 配線と kill switch ==========

def test_main_loop_no_longer_puts_synchronously():
    """メインループから直接の _put_blob が消えていること (回帰の本体)。"""
    i = SRC.index("inflight = {pool.submit(_process_one_asset, **t): t")
    j = SRC.index('_ph_mark("download")', i)
    body = SRC[i:j]
    assert "_put_blob(" not in body, "メインスレッドで同期 PUT している"
    assert "_thumb_submit(state, image_id, thumb_bytes)" in body


def test_drops_are_counted_separately():
    """送った数と捨てた数を混ぜると 「効いている」 の判断ができない。"""
    assert '"put_dropped": _w_put_drop,' in SRC


def test_sent_count_reported_from_the_thread():
    """非同期なので tick 末には送信済みとは限らない。 GC と同じ 「前回以降」 方式。"""
    assert "_thumb_drain_stats(state, out)" in SRC
    assert 'out["thumb_sent"]' in SRC


@pytest.mark.parametrize("key", ["thumb_async", "live_preview_on_demand"])
def test_kill_switches_are_dynamic(key):
    """再起動なしで旧挙動へ戻せること。 効かないツマミを作らない。"""
    i = SRC.index("DYN_KEYS: dict[str, tuple[str, int, int]] = {")
    j = SRC.index("\n}", i)
    assert f'"{key}"' in SRC[i:j]


@pytest.mark.parametrize("key", [
    "live_preview", "live_preview_on_demand", "preview_check_ttl_s",
    "thumb_async", "thumb_queue_max", "thumb_max_per_s",
])
def test_setup_writes_every_knob(key):
    """`state.get` で読むだけで setup が書かない = 死んだツマミ。 それを作らない。

    2026-08-24 に `target_tick_s` が同じ形で死んでいるのを見つけたばかりなので、
    新設のツマミは全部 setup() が state に書いていることを固定する。
    """
    assert f'"{key}": ' in SRC, f"{key} を setup() が state に書いていない"
