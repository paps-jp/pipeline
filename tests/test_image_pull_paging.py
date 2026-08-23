"""hub の /jobs/completes ページングを途中で打ち切らないこと。

2026-08-23 の実害: backlog が **677 分**あるのに image-pull の cursor が 0.51x
しか進まず、 遅れが拡大し続けた。 原因はページング打ち切りの判定:

    if len(page) < fetch_n:
        break            # 「返りが少ない = 在庫が尽きた」と誤読していた

hub の `/jobs/completes` は **limit を無視して 1 ページ 500 件で打ち切る**
(実測: limit=500 / 1000 / 2000 のどれでも 500 件)。 さらに 1 つの時刻窓に
500 件無ければそれ以下で返る。 つまり件数が少ないことは在庫切れを意味しない。

その結果 1 tick = 1 ページ = 最大 500 job に固定され、
`max_jobs_per_tick_backlog` を 1000 に上げても **構造的に到達不能**だった。
クロール投入を止めると窓あたりの密度が上がって 500 件フルで返る回が増え、
たまたま複数ページ進めていたので、 止めている間は問題が見えなかった。

これは links-pull の watermark 固定点と同じ型の誤り ——
**「返ってきた件数」で在庫の有無を判断している**。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import sys
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
    spec = importlib.util.spec_from_file_location("image_main_paging_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SRC = _IM.read_text(encoding="utf-8")


def _list_loop() -> str:
    """job 一覧を取るページングループの本文だけ切り出す。"""
    i = SRC.index("while (pages_fetched < max_pages")
    j = SRC.index('_ph_mark("list_jobs")', i)
    return SRC[i:j]


# ------------------------------------------------ 打ち切らないこと (回帰) --

def test_short_page_does_not_break_the_loop():
    """`len(page) < fetch_n` で break していない。 これが実害の本体。"""
    body = _list_loop()
    i = body.index("if len(page) < fetch_n:")
    tail = body[i:i + 400]
    # この if の直下に break が無いこと
    stmt = tail.split("\n")[1:4]
    assert not any(x.strip() == "break" for x in stmt), (
        "短いページで break している (hub は limit を無視して 500 件で切るので、"
        " 件数不足は在庫切れを意味しない)")


def test_short_page_is_counted_for_diagnosis():
    """黙って進むのではなく回数を残す (hub の挙動が変わったら気づけるように)。"""
    body = _list_loop()
    assert "short_pages += 1" in body
    assert 'out["short_pages"]' in SRC


# ------------------------------------------- 止まる条件は残っていること --

def test_empty_page_still_stops():
    """在庫が本当に尽きたときは空ページで止まる。"""
    body = _list_loop()
    i = body.index("if not page:")
    assert "break" in body[i:i + 60]


def test_cursor_no_progress_still_stops():
    """cursor が進まない病的ケースの保険が残っている (無限ループ防止)。"""
    body = _list_loop()
    assert body.count('stop_reason = "cursor_no_progress"') == 2
    for i in [k for k in range(len(body))
              if body.startswith('stop_reason = "cursor_no_progress"', k)]:
        assert "break" in body[i:i + 80]


@pytest.mark.parametrize("guard", [
    "pages_fetched < max_pages",     # ページ数の上限
    "len(jobs) < max_jobs",          # job 数の上限
    "_tick_deadline",                # 時間の上限
])
def test_loop_remains_bounded(guard):
    """break を外したぶん、 ループが有界であることは必ず担保する。"""
    assert guard in _list_loop().split("\n")[0] or guard in _list_loop()[:300]


# ----------------------------------------------- stop_reason が正確なこと --

def test_stop_reason_distinguishes_the_bounds():
    """どの上限で止まったかが出力から分かること。

    以前は count_lt_limit で break していたのに stop_reason が empty_page と
    記録されており、 表示と実態が食い違っていた。
    """
    assert 'stop_reason = "max_pages_reached"' in SRC
    assert 'stop_reason = "max_jobs_reached"' in SRC
    assert 'stop_reason = "tick_budget"' in SRC
