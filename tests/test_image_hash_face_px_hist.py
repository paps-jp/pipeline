"""検出 face の短辺ヒストグラム (`face_px_hist`)。

min_face_px (既定 50) は短辺がそれ未満の face を crop すら作らずに捨てる。
2026-08-22 の実測では **検出 22,241 顔のうち 13,353 (60%) がここで消えて**おり、
embedding 数の最大レバーがこの閾値になっている。 ところが従来の出力は
`skipped_tiny_faces` の **件数だけ**で、 捨てた face が 49px なのか 12px なのかが
分からず、 閾値を動かす判断ができなかった。

ここで固定するのは、 バケット境界が min_face_px の候補値と揃っていること
(揃っていないと「32 に下げたら何顔増えるか」が数えられない)。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_PM = (Path(__file__).resolve().parents[1] / "plugins" / "image_hash_extract"
       / "production_main.py")
pytestmark = pytest.mark.skipif(not _PM.exists(), reason="plugins リポジトリ未チェックアウト")


def _bucket(v: int) -> str:
    """production_main の _px_bucket と同じ規則 (関数内定義なので写経)。"""
    for hi in (16, 24, 32, 40, 50, 64, 96, 160):
        if v < hi:
            return "<%d" % hi
    return ">=160"


def test_buckets_align_with_threshold_candidates():
    """境界は min_face_px として現実的に選ぶ値と一致していること。"""
    for cand in (16, 24, 32, 40, 50, 64, 96):
        below = _bucket(cand - 1)
        at = _bucket(cand)
        assert below != at, "%d の前後でバケットが割れていない" % cand


def test_threshold_change_is_countable():
    """「50 -> 32 に下げると何顔増えるか」がヒストグラムの和で出せること。"""
    hist = {"<16": 10, "<24": 20, "<32": 30, "<40": 40, "<50": 50, "<64": 60}
    # 32 に下げると救われるのは [32,50) = "<40" と "<50" のバケット
    gained = hist["<40"] + hist["<50"]
    assert gained == 90
    # 現行 50 で捨てている総数
    dropped = sum(v for k, v in hist.items() if k in ("<16", "<24", "<32", "<40", "<50"))
    assert dropped == 150


def test_source_records_every_detected_face():
    """捨てた face だけでなく **残した face も** 数えていること。

    分母 (検出総数) が無いと「何割を捨てているか」が出せない。
    """
    src = _PM.read_text(encoding="utf-8")
    i_hist = src.index("face_px_hist[_b] = face_px_hist.get(_b, 0) + 1")
    i_skip = src.index("skipped_tiny += 1")
    assert i_hist < i_skip, "ヒストグラム加算は skip 判定より前でなければ分母が欠ける"


def test_threshold_is_emitted_with_the_histogram():
    """閾値そのものを一緒に出す (後から出力だけ見て解釈できるように)。"""
    src = _PM.read_text(encoding="utf-8")
    assert 'out["min_face_px"]' in src
    assert 'out["face_px_hist"]' in src


@pytest.mark.parametrize("v,expected", [
    (0, "<16"), (15, "<16"), (16, "<24"), (31, "<32"), (49, "<50"),
    (50, "<64"), (95, "<96"), (159, "<160"), (160, ">=160"), (4000, ">=160"),
])
def test_bucket_boundaries(v, expected):
    assert _bucket(v) == expected
