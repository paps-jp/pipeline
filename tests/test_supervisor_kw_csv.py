"""空文字の CSV 設定がデフォルト値に落ちない (_kw_csv)。

`kwargs.get(key) or default` と書くと空文字が falsy で default に落ちる。 operator が
UI で値を消して「無効化」したつもりが、 むしろ既定値を再適用してしまう。

実障害 2026-08-09: goodput_parked_slugs="" が既定の "video-face-extract" に落ち、
vfe が backlog 9,000 件を抱えたまま plan=min_resident_workers に固定され続けた。
PARK ALERT だけが出続けるので、 設定を見ても原因が分からない形で現れる。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_kwcsv_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_unset_uses_default(sm):
    """未設定 (キー無し) のときだけ既定値を使う = 後方互換。"""
    assert sm._kw_csv({}, "goodput_parked_slugs", "video-face-extract") == ["video-face-extract"]


def test_none_uses_default(sm):
    assert sm._kw_csv({"k": None}, "k", "a,b") == ["a", "b"]


def test_empty_string_means_none_parked(sm):
    """本丸: 空文字は「対象なし」。 デフォルトに落ちてはいけない。"""
    assert sm._kw_csv({"goodput_parked_slugs": ""}, "goodput_parked_slugs", "video-face-extract") == []


def test_whitespace_only_means_none_parked(sm):
    assert sm._kw_csv({"k": "   "}, "k", "x") == []


def test_explicit_values_win(sm):
    assert sm._kw_csv({"k": "a,b,c"}, "k", "x") == ["a", "b", "c"]


def test_strips_and_drops_blanks(sm):
    assert sm._kw_csv({"k": " a , , b "}, "k", "x") == ["a", "b"]


_posix_only = pytest.mark.skipif(
    not hasattr(os, "uname"), reason="setup() が os.uname() を呼ぶため POSIX 前提")


@_posix_only
def test_setup_empty_parked_slugs_unparks(sm):
    """setup() 経由でも空文字が park なしになること (回帰の本体)。"""
    st = sm.setup(goodput_parked_slugs="")
    assert st["goodput_cfg"]["parked_slugs"] == []


@_posix_only
def test_setup_unset_parked_slugs_keeps_default(sm):
    """未設定なら従来どおり vfe が park される (既存運用を壊さない)。"""
    st = sm.setup()
    assert st["goodput_cfg"]["parked_slugs"] == ["video-face-extract"]
