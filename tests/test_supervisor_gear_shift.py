"""elastic (ローギア) と goodput (ハイギア) の変速機。

両者は同じ agent template を書くが判断基準が違う (elastic=backlog÷tasks_per_worker /
goodput=Δthroughput÷Δworkers)。 常時両方 apply すると 30 秒ごとに奪い合う。
実障害 2026-08-08: ai-gpu8/9 の video-face-extract が goodput 1→3 / elastic 3→0 を
往復し、 operator が UI で入れた目標値も数十秒で流された。

_goodput_gear_engaged が slug 単位で受け持ちを切り替える。 cold_start / unknown の
間だけ elastic に運転させるのが要点 (elastic を GPU から外すだけだと、 active=0 の
slug が cold_start の「現状維持」から出られず発進不能になる)。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_gear_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _state(sm, diag: dict, *, age_s: float = 0.0, supersedes: int = 1,
           enabled: int = 1, apply_mode: int = 1, apply_slugs=None) -> dict:
    import time
    return {
        "goodput_cfg": {
            "enabled": enabled, "apply_mode": apply_mode,
            "supersedes_others": supersedes, "apply_slugs": apply_slugs or [],
        },
        "goodput_last_diagnostics": diag,
        "goodput_last_diagnostics_mono": time.monotonic() - age_s,
    }


# ---- ハイギア: goodput が slope を掴んだ slug は elastic を降ろす ----

@pytest.mark.parametrize("gp_state", ["responsive", "saturated", "no_demand", "parked"])
def test_high_gear_hands_over_to_goodput(sm, gp_state):
    st = _state(sm, {"video-face-extract": {"state": gp_state}})
    assert sm._goodput_gear_engaged(st, "video-face-extract") is True


# ---- ローギア: 測れていない間は elastic が運転を続ける ----

@pytest.mark.parametrize("gp_state", ["cold_start", "unknown"])
def test_low_gear_keeps_elastic_driving(sm, gp_state):
    st = _state(sm, {"video-face-extract": {"state": gp_state}})
    assert sm._goodput_gear_engaged(st, "video-face-extract") is False


def test_downshift_when_diagnostics_go_stale(sm):
    """goodput が止まったら (診断が古びたら) ローギアに戻る。

    これが無いと、 supervisor の goodput 側だけが例外で落ちたときに elastic も
    黙ったままになり、 誰も配分を動かせなくなる。
    """
    st = _state(sm, {"video-face-extract": {"state": "responsive"}},
                age_s=sm._GEAR_DIAG_MAX_AGE_S + 1.0)
    assert sm._goodput_gear_engaged(st, "video-face-extract") is False


def test_unknown_slug_stays_with_elastic(sm):
    """goodput の候補外 (CPU slug 等) は elastic の領分。"""
    st = _state(sm, {"video-face-extract": {"state": "responsive"}})
    assert sm._goodput_gear_engaged(st, "paprika-image-pull") is False


def test_no_diagnostics_yet_stays_with_elastic(sm):
    """起動直後で goodput が一度も回っていない = ローギア。"""
    st = _state(sm, {})
    st.pop("goodput_last_diagnostics_mono")
    assert sm._goodput_gear_engaged(st, "video-face-extract") is False


# ---- goodput が権威でないときは変速そのものが起きない ----

@pytest.mark.parametrize("kw", [
    {"enabled": 0}, {"apply_mode": 0}, {"supersedes": 0},
    {"apply_slugs": ["image-embed"]},          # 段階ロールアウト中は他系統を残す
])
def test_never_engages_when_goodput_not_authoritative(sm, kw):
    st = _state(sm, {"video-face-extract": {"state": "responsive"}}, **kw)
    assert sm._goodput_gear_engaged(st, "video-face-extract") is False


# ---- クラッチが _elastic_agent_scale の入口で効いているか ----

def test_agent_scale_skips_when_gear_engaged(sm):
    st = _state(sm, {"video-face-extract": {"state": "responsive"}})
    agents = [{"host": "ai-gpu8", "template": {"workloads": {"video-face-extract": {"count": 1}}}}]
    out = sm._elastic_agent_scale(
        st, agents, "video-face-extract", "gpu", {}, want=4, current=1,
        apply=True, cfg={},
    )
    assert out is not None
    assert out.get("skipped") == "goodput_gear_engaged"
    assert out.get("goodput_state") == "responsive"
    # 実適用していない = template を書き換えていない
    assert "applied" not in out


def test_agent_scale_runs_when_low_gear(sm):
    """cold_start では従来通り elastic が動く (= 発進できる)。"""
    st = _state(sm, {"video-face-extract": {"state": "cold_start"}})
    agents = [{"host": "ai-gpu8", "template": {"workloads": {"video-face-extract": {"count": 1}}}}]
    out = sm._elastic_agent_scale(
        st, agents, "video-face-extract", "gpu", {}, want=4, current=1,
        apply=False, cfg={},
    )
    # skip されずに本体ロジックへ入っている (結果は None でも dict でもよい)
    assert not (isinstance(out, dict) and out.get("skipped") == "goodput_gear_engaged")
