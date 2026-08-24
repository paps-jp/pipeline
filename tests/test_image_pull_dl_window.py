"""download 窓の占有率計器 —— 「ワーカが遅い」 と 「窓が飢えている」 を分ける。

2026-08-24 の観測: `max_inflight=96` に対し実働スレッドが 23 前後しか無かった
(1 tick の stage 実測 872 スレッド秒 ÷ download 段 37.4 秒)。 候補は 3 つ:

  (a) ワーカが遅い     — DB / GIL / 回線
  (b) プールが細い     — MariaPool(size=12) に 96 スレッドが群がる
  (c) 窓が飢えている   — メインスレッドが後片付けをしている間、 誰も補充しない

`phase_secs` は download 段を 1 つの塊としてしか出さないので、 どれも区別が
付かなかった。 メインループは実際には

    wait(...)          ← ワーカの完了を待つ = ワーカ律速ならここが伸びる
    for fut in done:   ← DB 集計 + **サムネの同期 PUT** = ここは窓が止まる
        pool.submit(...)   ← 補充はこの中でしか起きない

の 2 相なので、 別々に積めば (a)/(c) が分離できる。 (b) は MariaPool 側の
`wait_s` が受け持つ ([[test_plugin_pool_stats]])。

特に `_put_blob` は **ゲート無しの同期 HTTP PUT** で、 挿入画像 1 枚につき 1 回、
メインスレッドで走る (1 tick で最大 2,400 回)。 これが効いているなら (c)。

plugins は別リポジトリでテスト基盤が無いため、 ここからソースを読んで固定する。
"""

from __future__ import annotations

from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")

SRC = _IM.read_text(encoding="utf-8")


def _window_loop() -> str:
    """asset を捌くスライディングウィンドウのループ本体だけ切り出す。"""
    i = SRC.index("inflight = {pool.submit(_process_one_asset, **t): t")
    j = SRC.index('_ph_mark("download")', i)
    return SRC[i:j]


# --------------------------------------------- 2 相が別々に積まれること --

def test_wait_and_post_are_measured_separately():
    """これが分かれていないと (a) と (c) を永久に区別できない。"""
    body = _window_loop()
    assert "_w_wait_s +=" in body
    assert "_w_post_s +=" in body


def test_wait_is_bracketed_around_the_wait_call():
    """計測が wait() を挟んでいること (別の場所を測っていない)。"""
    body = _window_loop()
    i = body.index("done, _ = wait(inflight, return_when=FIRST_COMPLETED)")
    before = body[:i].rstrip().rsplit("\n", 1)[-1].strip()
    after = body[i:].split("\n")[1].strip()
    assert before.startswith("_t_w0 = time.perf_counter()")
    assert after.startswith("_w_wait_s +=")


def test_post_is_closed_at_the_while_level_not_inside_the_for():
    """for の中で閉じると 1 完了ぶんしか積まれず、 補充待ちが漏れる。"""
    body = _window_loop()
    line = [x for x in body.split("\n") if "_w_post_s +=" in x][0]
    indent = len(line) - len(line.lstrip())
    for_line = [x for x in body.split("\n") if x.strip() == "for fut in done:"][0]
    assert indent == len(for_line) - len(for_line.lstrip()), (
        "_w_post_s の加算は for と同じ深さ (= while 本体の末尾) に置くこと")


def test_submit_happens_only_inside_the_post_phase():
    """補充が post 相の中にしかない = post が伸びれば窓が空くという前提の根拠。"""
    body = _window_loop()
    i = body.index("_t_p0 = time.perf_counter()")
    j = body.index("_w_post_s +=")
    assert "pool.submit(_process_one_asset, **nxt)" in body[i:j]


# ------------------------------------------------- 窓の在庫を測ること --

def test_occupancy_sampled_before_waiting():
    """wait の後だと 「完了して減った直後」 を測ることになり過小に出る。"""
    body = _window_loop()
    i = body.index("_w_occ_sum += _n_now")
    j = body.index("done, _ = wait(")
    assert i < j, "占有率の標本化は wait() より前で行うこと"


def test_occupancy_min_is_tracked():
    """平均だけだと 「たまに空になる」 が見えない。"""
    assert "_w_occ_min" in _window_loop()


def test_occupancy_is_reported_against_its_ceiling():
    """占有率は max_inflight と並べないと読めない。"""
    assert '"max_inflight": _inflight' in SRC
    assert '"occ_avg"' in SRC


# ------------------------------------- サムネ PUT がメインスレッド上と分かること --

def test_put_blob_is_timed():
    """(c) の第一容疑。 回数と秒数の両方を出す。"""
    body = _window_loop()
    assert "_w_put_s +=" in body
    assert "_w_put_n += 1" in body


def test_put_blob_timing_is_inside_the_post_phase():
    """ワーカ側ではなくメインスレッド上で起きていることを固定する。"""
    body = _window_loop()
    i = body.index("_t_p0 = time.perf_counter()")
    j = body.index("_w_post_s +=")
    assert "_put_blob(" in body[i:j]


def test_put_blob_failure_still_counted():
    """例外で計測が飛ぶと 「速い」 と誤読する。 加算は except の外。"""
    body = _window_loop()
    i = body.index("_put_blob(")
    seg = body[i:i + 500]
    assert "log.debug(\"viz thumb upload failed" in seg
    k = seg.index("_w_put_s +=")
    assert seg.index("except Exception as _e:") < k, (
        "_w_put_s の加算は except の後 (= 失敗時も積む) に置くこと")


# ------------------------------------------------------ 出力に出ること --

@pytest.mark.parametrize("key", [
    "max_inflight", "occ_avg", "occ_min", "laps",
    "wait_s", "post_s", "put_blob_s", "put_blob_n",
])
def test_dl_window_reports(key):
    i = SRC.index('out["dl_window"] = {')
    assert f'"{key}"' in SRC[i:i + 500]


def test_dl_window_omitted_when_no_work():
    """asset が 0 件の tick で 0 除算しないこと。"""
    assert "if _w_laps:" in SRC


# ------------------------------------------------ DB プール統計の tick 区切り --

def test_pool_stats_reset_before_the_loop():
    """tick 単位で見たいので、 ループ前にゼロ化する。"""
    i = SRC.index("_dbp = state.get(\"db_pool\")")
    j = SRC.index("inflight = {pool.submit(_process_one_asset, **t): t")
    assert "reset_stats()" in SRC[i:j]


def test_pool_stats_read_after_the_loop():
    i = SRC.index('_ph_mark("download")')
    assert 'out["db_pool"]' in SRC[i:i + 1200]


def test_pool_instrumentation_is_optional():
    """計器の無い古い MariaPool でも落ちないこと (配備順を選ばない)。"""
    assert 'hasattr(_dbp, "reset_stats")' in SRC
    assert 'hasattr(_dbp, "stats")' in SRC
