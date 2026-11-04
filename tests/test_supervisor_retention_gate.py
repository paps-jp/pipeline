"""Paprika 退役遅れゲート (_retention_gate_run) の回帰テスト。

事故の背景 (2026-11-03): Paprika の jobs 表が 5,280 万行 / 120 GB まで育った。
退役 (job_retention_days 経過後の削除) が流入の 5 分の 1 の速度しか出ず、 1 か月
以上負けっぱなしだったのに、 投入側のゲートは全部「今消化中の量」(hub の空き、
.47 の水位) しか見ておらず、 **消化済みの残骸が何千万行あっても反応しなかった**。
出口 (退役) の遅れを入口 (投入) に伝える神経が無かった。

このテストが守る性質:
  - 信号は行数ではなく「cutoff を過ぎてから消されるまでの遅れ (分)」。 flow 画面の
    タンク paprika_jobs_retention_lag から読む。
  - 制御則は比例 + 不感帯: target 以下は何もしない / target〜pause は遅れに比例して
    submit_parallel の上限を下げる / pause で submit_paused=1。
  - 上限は AIMD (_submit_parallel_autoscale_run) の max_parallel を下げる形で渡す。
    ノブを書く権威は AIMD 1 つ (二重権威の綱引きを作らない)。
  - **ヒステリシス**: pause で停止 → resume まで下がって初めて解除。
  - **min_dwell_s** で連続書き換えを抑える。
  - **fail-open は片側だけ**: 計器が読めなければ上限も新規停止も掛けないが、
    自分が止めたものは維持する。
  - 再開を書くのは **自分が止めた場合だけ** (.47 ゲートの停止を横から解除しない)。

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
NODE = "paprika_jobs_retention_lag"


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_retention_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _state(*, enabled: bool = True, dwell: float = 0.0, max_p: int = 256) -> dict:
    return {
        "control_url": "http://control.invalid",
        "retention_gate_cfg": {
            "enabled": enabled,
            "flow_node_id": NODE,
            "slugs": [SLUG],
            "target_lag_min": 60.0,
            "pause_lag_min": 720.0,
            "resume_lag_min": 360.0,
            "min_parallel": 16,
            "probe_timeout_s": 1.0,
            "probe_interval_s": 0.0,     # キャッシュを効かせない
            "min_dwell_s": dwell,
        },
        "submit_scale_cfg": {
            "enabled": True, "slug": SLUG, "min_parallel": 16, "max_parallel": max_p,
            "step_up": 16, "backoff_factor": 0.7, "fail_ratio_high": 0.10,
            "fail_ratio_low": 0.03, "no_room_ratio_max": 0.6, "latency_gate": False,
            "latency_base_floor_ms": 800, "latency_base_persist": False,
            "demand_ratio": 0.2, "sample_runs": 4, "min_dwell_s": dwell,
        },
        "retention_gate_state": {},
        "retention_gate_probe": None,
        "retention_lag_cap": None,
        "submit_scale_last_action": 0.0,
    }


def _run(*, parallel=128, submitted=1000, failed=0, pool_full=40, cap_polls=60,
         no_room=0, safe_min=200):
    return {
        "workload_slug": SLUG,
        "output_json": json.dumps({
            "submitted": submitted, "failed_other": failed,
            "pool_full_polls": pool_full, "capacity_polls": cap_polls,
            "no_capacity_polls": 0, "no_room_polls": no_room,
            "safe_avail_min": safe_min, "submit_parallel": parallel,
            "submit_parallel_max": 256, "submit_paused": False, "post_ms_avg": 1000,
        }),
    }


@pytest.fixture
def wired(sup, monkeypatch):
    """flow snapshot / runs 応答と _patch_init_kwargs を差し替える。"""
    box: dict = {"lag": 0.0, "snapshot_error": False, "runs": [], "calls": []}

    def _get(url, timeout=10.0):
        if "/flow/snapshot" in url:
            if box["snapshot_error"]:
                raise RuntimeError("snapshot down")
            if box["lag"] is None:
                return {"nodes": []}
            return {"nodes": [{"id": NODE, "pending": box["lag"], "unit": "min"}]}
        assert f"/workloads/{SLUG}/runs" in url, url
        return {"runs": box["runs"]}

    def _patch(control_url, slug, updates):
        box["calls"].append((slug, dict(updates)))
        return {"ok": True, "updates": updates}

    monkeypatch.setattr(sup, "_http_get_json", _get)
    monkeypatch.setattr(sup, "_patch_init_kwargs", _patch)
    return box


# ── 制御則 (純関数) ──────────────────────────────────────────────────────────

def test_cap_is_none_in_deadband(sup):
    assert sup._retention_gate_cap(0.0, 60, 720, 256, 16) is None
    assert sup._retention_gate_cap(60.0, 60, 720, 256, 16) is None


def test_cap_falls_linearly_between_target_and_pause(sup):
    # 中点 (390 分) で半分
    assert sup._retention_gate_cap(390.0, 60, 720, 256, 16) == 128
    # 1/4 進んだところで 3/4
    assert sup._retention_gate_cap(225.0, 60, 720, 256, 16) == 192


def test_cap_never_below_min_parallel(sup):
    assert sup._retention_gate_cap(700.0, 60, 720, 256, 16) == 16
    assert sup._retention_gate_cap(720.0, 60, 720, 256, 16) == 16
    assert sup._retention_gate_cap(9999.0, 60, 720, 256, 16) == 16


def test_cap_degenerate_config_is_no_limit(sup):
    assert sup._retention_gate_cap(500.0, 720, 60, 256, 16) is None


# ── ゲート本体 ───────────────────────────────────────────────────────────────

def test_caught_up_means_no_cap_no_action(sup, wired):
    wired["lag"] = 0.0
    st = _state()
    out = sup._retention_gate_run(st)
    assert out["cap"] is None and st["retention_lag_cap"] is None
    assert wired["calls"] == [] and out["alerts"] == []


def test_lag_above_target_sets_cap_and_warns(sup, wired):
    wired["lag"] = 390.0
    st = _state()
    out = sup._retention_gate_run(st)
    assert st["retention_lag_cap"] == 128 and out["cap"] == 128
    assert wired["calls"] == []          # 上限を渡すだけ、 ノブは書かない
    assert [a["kind"] for a in out["alerts"]] == ["retention_lag_cap"]


def test_pauses_at_pause_threshold(sup, wired):
    wired["lag"] = 720.0
    st = _state()
    out = sup._retention_gate_run(st)
    assert wired["calls"] == [(SLUG, {"submit_paused": 1})]
    assert st["retention_gate_state"]["paused_by_us"] is True
    assert [a["kind"] for a in out["alerts"]] == ["retention_submit_paused"]


def test_hysteresis_resumes_only_below_resume(sup, wired):
    wired["lag"] = 800.0
    st = _state()
    sup._retention_gate_run(st)
    wired["calls"].clear()
    wired["lag"] = 500.0                 # pause 未満だが resume 超 → 維持
    out = sup._retention_gate_run(st)
    assert wired["calls"] == []
    assert st["retention_gate_state"]["paused_by_us"] is True
    assert [a["kind"] for a in out["alerts"]] == ["retention_submit_paused"]
    wired["lag"] = 300.0                 # resume 以下 → 解除
    out = sup._retention_gate_run(st)
    assert wired["calls"] == [(SLUG, {"submit_paused": 0})]
    assert st["retention_gate_state"]["paused_by_us"] is False
    assert "retention_submit_resumed" in [a["kind"] for a in out["alerts"]]


def test_dwell_blocks_flapping(sup, wired):
    wired["lag"] = 800.0
    st = _state(dwell=3600.0)
    sup._retention_gate_run(st)
    wired["calls"].clear()
    wired["lag"] = 0.0
    out = sup._retention_gate_run(st)
    assert wired["calls"] == [] and out.get("dwell_block_s")


def test_probe_failure_is_fail_open_but_holds_own_pause(sup, wired):
    wired["lag"] = 800.0
    st = _state()
    sup._retention_gate_run(st)
    wired["calls"].clear()
    wired["snapshot_error"] = True
    out = sup._retention_gate_run(st)
    assert out["probe"] == "failed" and out["fail_mode"] == "hold"
    assert wired["calls"] == []
    assert st["retention_lag_cap"] is None
    assert [a["kind"] for a in out["alerts"]] == ["retention_submit_probe_failed"]
    # 止めていないときの probe 失敗は何もしない
    st2 = _state()
    out2 = sup._retention_gate_run(st2)
    assert out2["fail_mode"] == "open" and out2["alerts"] == []


def test_missing_tank_is_treated_as_probe_failure(sup, wired):
    wired["lag"] = None
    st = _state()
    out = sup._retention_gate_run(st)
    assert out["probe"] == "failed" and st["retention_lag_cap"] is None


def test_does_not_resume_a_pause_it_did_not_make(sup, wired):
    """.47 ゲートが止めている間に lag が低くても 0 を書かない。"""
    wired["lag"] = 0.0
    st = _state()
    out = sup._retention_gate_run(st)
    assert wired["calls"] == [] and out["paused_by_us"] is False


def test_disabled_clears_cap(sup, wired):
    st = _state(enabled=False)
    st["retention_lag_cap"] = 64
    out = sup._retention_gate_run(st)
    assert out["skipped"] == "disabled" and st["retention_lag_cap"] is None


# ── AIMD との結合 ─────────────────────────────────────────────────────────────

def test_autoscaler_steps_down_to_the_cap(sup, wired):
    wired["lag"] = 390.0                 # cap 128
    st = _state()
    sup._retention_gate_run(st)
    wired["runs"] = [_run(parallel=192)] * 4
    out = sup._submit_parallel_autoscale_run(st)
    assert out["action"] == "scale" and out["to"] == 128
    assert wired["calls"][-1] == (SLUG, {"submit_parallel": 128})
    assert "retention_cap" in out["reason"]


def test_autoscaler_scale_up_is_bounded_by_the_cap(sup, wired):
    wired["lag"] = 390.0                 # cap 128
    st = _state()
    sup._retention_gate_run(st)
    wired["runs"] = [_run(parallel=120)] * 4   # 需要あり・空きあり → 本来 +16
    out = sup._submit_parallel_autoscale_run(st)
    assert out["action"] == "scale" and out["to"] == 128


def test_autoscaler_unaffected_when_caught_up(sup, wired):
    wired["lag"] = 0.0
    st = _state()
    sup._retention_gate_run(st)
    wired["runs"] = [_run(parallel=120)] * 4
    out = sup._submit_parallel_autoscale_run(st)
    assert out["action"] == "scale" and out["to"] == 136
