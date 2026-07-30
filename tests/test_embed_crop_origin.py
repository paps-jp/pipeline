"""image_embed の crop 原点判定 (_crop_origin) のテスト。

動画顔 embedding が全て margin ずれした crop から計算されていた bug
([[video-face-crop-origin-bug]]) の修正。 本番実測した 3 世代のジオメトリを
そのままケースにしている:

  image 経路      hash-extract が bbox ぴったりに切る                  → 原点 = bbox
  video 新世代    movie2face margin 0.4 crop (bbox 130x192 → 234x206) → 原点 = (ax1,ay1)
  video 旧世代    minio_key が元画像丸ごと (892x1109 に bbox 160x195)  → 原点 = (0,0)

旧世代を margin 式で一律に扱うと「legacy 経路で正しく embed 済の既存 index」を
壊すので、 世代の撃ち分けが正しいことをここで固定する。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "plugins" / "image_embed" / "embed_main.py"

# 本番 plugin は非公開リポジトリ側 (`/plugins/` は .gitignore) なので、 公開リポジトリだけを
# clone した環境には存在しない。 その場合は skip する (テスト自体は SoT 側で実走させる)。
pytestmark = pytest.mark.skipif(
    not _SRC.exists(), reason=f"本番 plugin が無い環境 ({_SRC.name} は非公開リポジトリ側)")


@pytest.fixture(scope="module")
def mod():
    """embed_main を単体 import (torch 等の重い依存は module 直下に無いので読める)。"""
    spec = importlib.util.spec_from_file_location("_embed_main_under_test", _SRC)
    m = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(m)
    return m


def test_image_path_uses_bbox_origin(mod) -> None:
    # hash-extract は img[y1:y2, x1:x2] なので crop 原点 == bbox 原点
    ox, oy, why = mod._crop_origin("image", 100, 200, 180, 300, None, None, 80, 100)
    assert (ox, oy, why) == (100.0, 200.0, "image_bbox")


def test_stored_crop_bbox_wins(mod) -> None:
    # vfe が crop_bbox を保存した行は、 寸法推定より保存値が優先
    ox, oy, why = mod._crop_origin("movie", 100, 200, 180, 300, 68.0, 160.0, 144, 180)
    assert (ox, oy, why) == (68.0, 160.0, "stored")


def test_new_generation_margin_crop_detected(mod) -> None:
    """本番実測: bbox 130x192 @(144,-63) → crop 234x206。

    ay1 = max(0, floor(-63 - 0.4*192)) = 0 で上が clamp されている実例。
    """
    bx1, by1, bx2, by2 = 144.0, -63.0, 274.0, 129.0
    ox, oy, why = mod._crop_origin("movie", bx1, by1, bx2, by2, None, None, 234, 206)
    assert why == "margin"
    assert (ox, oy) == (92.0, 0.0)      # floor(144 - 52) = 92, 上は 0 clamp


def test_margin_formula_matches_measured_crop_size(mod) -> None:
    # crop_with_margin の予測寸が実測 234x206 と一致すること (= 式が本番と同一)
    _, _, pw, ph = mod._margin_origin(144.0, -63.0, 274.0, 129.0)
    assert (pw, ph) == (234, 206)


def test_old_generation_full_frame_gets_zero_origin(mod) -> None:
    """本番実測: minio_key が元画像 892x1109、 bbox 160x195 @(381,282)。

    これを margin 扱いすると原点が (317,204) になり、 legacy 経路で正しく計算済の
    既存 embedding を再計算で壊す。 (0,0) に判定されなければならない。
    """
    ox, oy, why = mod._crop_origin("movie", 381.0, 282.0, 541.0, 477.0, None, None, 892, 1109)
    assert (ox, oy, why) == (0.0, 0.0, "frame")


def test_unknown_geometry_keeps_legacy_behaviour(mod) -> None:
    # どちらとも判定できない形は挙動を変えない (bbox 原点) + unknown として計上
    ox, oy, why = mod._crop_origin("movie", 500.0, 500.0, 600.0, 600.0, None, None, 50, 50)
    assert (ox, oy, why) == (500.0, 500.0, "unknown")


def test_margin_crop_right_edge_clamped(mod) -> None:
    """frame 右端で crop が切れた場合も margin 判定になる (原点は影響を受けない)。"""
    bx1, by1, bx2, by2 = 100.0, 100.0, 200.0, 200.0
    ax1, ay1, pw, ph = mod._margin_origin(bx1, by1, bx2, by2)
    assert (ax1, ay1) == (60.0, 60.0) and (pw, ph) == (180, 180)
    # 実 crop が予測より小さい (右下 clamp) → margin と判定され原点は (60,60)
    ox, oy, why = mod._crop_origin("movie", bx1, by1, bx2, by2, None, None, 150, 170)
    assert (ox, oy, why) == (60.0, 60.0, "margin")


def test_offset_magnitude_is_the_reported_bug(mod) -> None:
    """修正前後の原点差が margin 分 (0.4·bw, 0.4·bh) であることを明示。"""
    bx1, by1, bx2, by2 = 300.0, 400.0, 400.0, 550.0     # bw=100, bh=150
    ox, oy, why = mod._crop_origin("movie", bx1, by1, bx2, by2, None, None, 180, 270)
    assert why == "margin"
    assert (bx1 - ox, by1 - oy) == (40.0, 60.0)          # 0.4*100, 0.4*150
