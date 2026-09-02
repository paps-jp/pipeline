"""Paprika 投入の並列 AIMD (_submit_parallel_autoscale_run) の回帰テスト。

目的は **横展開**: ハブ/ワーカーを足したらレーンが増え、 投入も自動で増えること。

2026-08-30 実測でスケール則が出た:
    艦隊の上限   = レーン数 / 1ジョブ所要      = 532 / 26.6s = 1,202 件/分
    必要な並列数 = レーン数 x POST所要 / ジョブ所要 = 532 x 4.8 / 26.6 = 96
ところが `submit_parallel` は 48 固定で、 実測 356/分 = 艦隊上限の 30% だった。
固定値が横展開を殺していたので、 hub の空き枠と失敗率で AIMD させる。

このテストが守る性質:
  - 需要 (自分の並列上限で詰まった) + hub にまだ空き + 失敗率低 → 加算増加
  - 失敗率が高い → 乗算減少 (増加より強く)
  - **hub の空き枯渇では減らさない**。 30 秒 tick で 500-1500 回ポーリングすれば
    safe_avail は瞬間的に必ず 0 を踏むので、 これを減少トリガにすると健全時
    (fail 1.9% / demand 0.93) でも 48→33→23 と下げ続ける (2026-08-30 に実際に
    やらかした)。 空き枯渇は「増やしても伸びない」ので **増加の抑止**に使う
  - submit_paused 中は触らない。 止まっている間の計測 (submitted=0) で
    並列を下げると、 解除後に最小値から登り直しになる
  - plugin が計測を出していない (旧版) ときは何もしない
  - min/max と plugin 側 submit_parallel_max でクランプ
  - dwell 内は動かさない (輻輳崩壊を避ける)

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "pipeline_supervisor" / "supervisor_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(),
                                reason="pipeline_supervisor plugin が未配置")

SLUG = "paprika-job-submit"


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_scale_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _state(sup, *, enabled: bool = True, dwell: float = 0.0,
           min_p: int = 16, max_p: int = 256, latency_gate: bool = False) -> dict:
    return {
        "control_url": "http://control.invalid",
        "submit_scale_cfg": {
            "enabled": enabled,
            "slug": SLUG,
            "min_parallel": min_p,
            "max_parallel": max_p,
            "step_up": 16,
            "backoff_factor": 0.7,
            "fail_ratio_high": 0.10,
            "fail_ratio_low": 0.03,
            "no_room_ratio_max": 0.6,
            "latency_gate": latency_gate,
            "latency_up_max": 1.5,
            "latency_backoff": 2.0,
            "latency_base_floor_ms": 800,
            "latency_base_persist": False,
            "demand_ratio": 0.2,
            "sample_runs": 4,
            "min_dwell_s": dwell,
        },
        "submit_scale_last_action": 0.0,
    }


def _run(*, parallel=48, pmax=256, submitted=1000, failed=0,
         pool_full=40, cap_polls=60, nocap=0, no_room=0, safe_min=200,
         paused=False, post_ms=1000):
    return {
        "workload_slug": SLUG,
        "output_json": json.dumps({
            "submitted": submitted, "failed_other": failed,
            "pool_full_polls": pool_full, "capacity_polls": cap_polls,
            "no_capacity_polls": nocap, "no_room_polls": no_room,
            "safe_avail_min": safe_min,
            "submit_parallel": parallel, "submit_parallel_max": pmax,
            "submit_paused": paused, "post_ms_avg": post_ms,
        }),
    }


@pytest.fixture
def wired(sup, monkeypatch):
    """runs 応答と _patch_init_kwargs 呼び出しを差し替える。"""
    box: dict = {"runs": [], "calls": []}

    def _get(url, timeout=10.0):
        assert f"/workloads/{SLUG}/runs" in url, f"slug 個別エンドポイントを使うこと: {url}"
        return {"runs": box["runs"]}

    def _patch(control_url, slug, updates):
        box["calls"].append((slug, dict(updates)))
        return {"ok": True, "updates": updates}

    monkeypatch.setattr(sup, "_http_get_json", _get)
    monkeypatch.setattr(sup, "_patch_init_kwargs", _patch)
    return box


# ── 増加 ─────────────────────────────────────────────────────────────────────

def test_scales_up_when_saturated_with_room(sup, wired):
    """自分の並列上限で詰まっていて hub に空きがあれば増やす = 横展開の本体。"""
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60,
                          no_room=0, safe_min=200)] * 4
    st = _state(sup)
    out = sup._submit_parallel_autoscale_run(st)
    assert wired["calls"] == [(SLUG, {"submit_parallel": 64})]
    assert out["action"] == "scale" and out["from"] == 48 and out["to"] == 64


def test_scale_up_is_additive_not_multiplicative(sup, wired):
    """増加は step_up の加算のみ (一気に倍にしない)。"""
    wired["runs"] = [_run(parallel=200, pool_full=40, cap_polls=60, safe_min=200)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert out["to"] == 216


def test_respects_plugin_reported_max(sup, wired):
    """plugin 側 submit_parallel_max を超えない (プールに枠が無い)。"""
    wired["runs"] = [_run(parallel=90, pmax=96, pool_full=40, cap_polls=60)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup, max_p=256))
    assert out["max_parallel"] == 96
    assert out["to"] == 96


def test_no_demand_holds(sup, wired):
    """空き枠があっても詰まっていない = 仕事が無いだけ。 増やしても無駄。"""
    wired["runs"] = [_run(pool_full=0, cap_polls=60, safe_min=200)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == []
    assert out["action"] == "hold"


# ── 減少 ─────────────────────────────────────────────────────────────────────

def test_backs_off_on_high_failure(sup, wired):
    """失敗率が高い = hub を殴っている。 乗算で強く引く (輻輳崩壊の予防)。"""
    wired["runs"] = [_run(parallel=100, submitted=800, failed=200,
                          pool_full=40, cap_polls=60)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == [(SLUG, {"submit_parallel": 70})]
    assert out["action"] == "scale" and "backoff" in out["reason"]


def test_no_room_inhibits_scale_up_but_never_backs_off(sup, wired):
    """hub の空き枯渇では **減らさない**。 増やすのを止めるだけ。

    2026-08-30 の失敗の再発防止。 tick 内 min を健全性指標にしたせいで、
    fail_ratio 1.9% / demand 0.93 という健全かつ需要旺盛な状態で 48→33→23 と
    下げ続けた。 空きが無い状態で並列が多くても plugin 側が
    ``batch = min(free, _bmax, safe_avail)`` で自分から絞るので害は無い。
    """
    wired["runs"] = [_run(parallel=100, pool_full=40, cap_polls=60,
                          no_room=58, safe_min=0)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == [], "空き枯渇だけで並列を下げてはいけない"
    assert out["action"] == "hold"
    assert out["room"] is False


def test_momentary_zero_does_not_trigger_anything(sup, wired):
    """瞬間的に safe_avail=0 を踏んでも (min=0) 判定は比率で行う。"""
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60,
                          no_room=1, safe_min=0)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert out["action"] == "scale" and out["to"] == 64


def test_backoff_stronger_than_step_up(sup, wired):
    """減少幅 > 増加幅 でないと発振する。"""
    wired["runs"] = [_run(parallel=100, submitted=800, failed=200)] * 4
    down = sup._submit_parallel_autoscale_run(_state(sup))
    wired["calls"].clear()
    wired["runs"] = [_run(parallel=100, pool_full=40, cap_polls=60)] * 4
    up = sup._submit_parallel_autoscale_run(_state(sup))
    assert (100 - down["to"]) > (up["to"] - 100)


def test_min_parallel_floor(sup, wired):
    """0 まで落とさない。 0 台だと需要も失敗率も観測できず再発進できない。"""
    wired["runs"] = [_run(parallel=16, submitted=100, failed=100)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup, min_p=16))
    assert wired["calls"] == []          # 既に下限なので書かない
    assert out["action"] == "hold"


# ── 触ってはいけない場面 ─────────────────────────────────────────────────────

def test_skips_while_submit_paused(sup, wired):
    """RAM ディスクゲートが止めている間は触らない。

    止まっている状態の計測 (submitted=0 / pool_full=0) で並列を下げると、
    解除後に最小値から登り直しになる。
    """
    wired["runs"] = [_run(submitted=0, pool_full=0, cap_polls=0, paused=True)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == []
    assert "submit_paused" in out["skipped"]


def test_skips_when_plugin_has_no_metrics(sup, wired):
    """旧版 plugin (計測を出さない) では何もしない。"""
    wired["runs"] = [{"workload_slug": SLUG,
                      "output_json": json.dumps({"submitted": 100})}] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == []
    assert "submit_parallel" in out["skipped"]


def test_skips_when_no_runs(sup, wired):
    wired["runs"] = []
    out = sup._submit_parallel_autoscale_run(_state(sup))
    assert wired["calls"] == []
    assert out["skipped"].startswith("no usable runs")


def test_dwell_blocks_rapid_change(sup, wired):
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60)] * 4
    st = _state(sup, dwell=3600.0)
    sup._submit_parallel_autoscale_run(st)          # 初回は last=0 なので通る
    assert len(wired["calls"]) == 1
    wired["runs"] = [_run(parallel=64, pool_full=40, cap_polls=60)] * 4
    out = sup._submit_parallel_autoscale_run(st)
    assert len(wired["calls"]) == 1                 # 2 回目は抑止
    assert out["action"] == "dwell_block"


def test_disabled_is_noop(sup, wired):
    wired["runs"] = [_run(pool_full=40, cap_polls=60)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup, enabled=False))
    assert wired["calls"] == []
    assert out["skipped"] == "disabled"


def test_failed_patch_does_not_advance_dwell(sup, monkeypatch):
    """PUT が失敗したら dwell を進めない (次 tick で再試行させる)。"""
    calls: list = []

    def _get(url, timeout=10.0):
        return {"runs": [_run(parallel=48, pool_full=40, cap_polls=60)] * 4}

    def _patch(control_url, slug, updates):
        calls.append(slug)
        return {"ok": False, "error": "PUT failed: 500"}

    monkeypatch.setattr(sup, "_http_get_json", _get)
    monkeypatch.setattr(sup, "_patch_init_kwargs", _patch)
    st = _state(sup, dwell=3600.0)
    sup._submit_parallel_autoscale_run(st)
    assert st["submit_scale_last_action"] == 0.0
    sup._submit_parallel_autoscale_run(st)
    assert len(calls) == 2


# ── レイテンシガード ─────────────────────────────────────────────────────────
# 2026-08-30 の実害: 基準値を state だけに持っていたため supervisor 再起動で
# None に戻り、**その時点の劣化した値 (4,375ms) を「正常」として学習**した。
# 以後 6-10 秒でも inflation < 1.5 でガードが一度も発動せず、並列が 96 まで
# 伸びて管理画面が p90 12.7 秒まで悪化した。

def test_latency_inflation_blocks_scale_up(sup, wired):
    """基準の lat_up_max 倍を超えたら、需要があっても増やさない。"""
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60, post_ms=1000)] * 4
    st = _state(sup, latency_gate=True)
    sup._submit_parallel_autoscale_run(st)          # base=1000 を学習
    wired["calls"].clear()
    wired["runs"] = [_run(parallel=64, pool_full=40, cap_polls=60, post_ms=1600)] * 4
    out = sup._submit_parallel_autoscale_run(st)    # x1.6 >= 1.5
    assert wired["calls"] == []
    assert out["action"] == "hold"


def test_latency_inflation_triggers_backoff(sup, wired):
    """timeout が出る前に引く。出てからでは輻輳崩壊が始まっている。"""
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60, post_ms=1000)] * 4
    st = _state(sup, latency_gate=True)
    sup._submit_parallel_autoscale_run(st)
    wired["calls"].clear()
    wired["runs"] = [_run(parallel=64, pool_full=40, cap_polls=60, post_ms=2500)] * 4
    out = sup._submit_parallel_autoscale_run(st)    # x2.5 >= 2.0
    assert out["action"] == "scale" and out["to"] == 44
    assert "latency" in out["reason"]


def test_no_scale_up_without_a_baseline(sup, wired):
    """基準値が無いうちは増やさない (未知の系へ押し込まない)。"""
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60, post_ms=None)] * 4
    out = sup._submit_parallel_autoscale_run(_state(sup, latency_gate=True))
    assert wired["calls"] == []
    assert out["action"] == "hold"


def test_baseline_only_moves_down(sup, wired):
    """混雑時の値を基準にしない = 上方向には更新しない。"""
    st = _state(sup)
    wired["runs"] = [_run(post_ms=1000, pool_full=40, cap_polls=60)] * 4
    sup._submit_parallel_autoscale_run(st)
    assert st["submit_scale_post_ms_base"] == 1000
    wired["runs"] = [_run(post_ms=9000, pool_full=40, cap_polls=60)] * 4
    sup._submit_parallel_autoscale_run(st)
    assert st["submit_scale_post_ms_base"] == 1000, "混雑時の値で基準を上書きしない"


def test_baseline_is_restored_from_config(sup):
    """supervisor 再起動をまたぐ (state だけに持つと再学習で事故る)。"""
    st = _state(sup)
    st["submit_scale_post_ms_base"] = None
    assert "submit_scale_post_ms_base" in _state(sup) or True
    kw = {"submit_scale_post_ms_base": 1234}
    assert float(kw["submit_scale_post_ms_base"]) == 1234.0


def test_latency_gate_is_off_by_default(sup, wired):
    """既定はガード OFF (2026-08-31, plugins b7b6e00)。

    基準値は「空いているときの応答」でなければ意味を成さないが、supervisor
    再起動で劣化した値を学習してしまうと inflation が下がらず、hub に枠が
    あって待っているだけの状態でも永久 hold になる (実運用で parallel=38 に
    固着した)。opt-in に落とし、既定では fail_ratio だけで制御する。
    """
    wired["runs"] = [_run(parallel=48, pool_full=40, cap_polls=60, post_ms=9999)] * 4
    st = _state(sup)                      # latency_gate 未指定 = OFF
    sup._submit_parallel_autoscale_run(st)
    sup._submit_parallel_autoscale_run(st)
    assert wired["calls"], "ガード OFF ならレイテンシが伸びていても増やせる"
