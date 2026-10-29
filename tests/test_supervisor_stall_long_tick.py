"""stall-watcher が「1 tick が長いだけの健全な self_loop」を SILENT と読まない。

silence_secs は最後に success した run からの経過なので、 run_secs=3600 で回る
pipeline-db-cleanup / crawl-host-import は毎回 20 分を過ぎると
「SILENT だが pending 残 (claimed=1)」 を出し続けていた (2026-10-29 実測、 1 時間に
50 回近く)。 実際は毎時 success で終わっており、 警告は全部誤検知。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")

_NOW = _dt.datetime(2026, 10, 29, 2, 50, tzinfo=_dt.timezone.utc)


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_longtick_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _runs(running_for_s: float, prev_duration_s: float = 3600.0) -> dict:
    start = _NOW - _dt.timedelta(seconds=running_for_s)
    done = [{"started_at": (start - _dt.timedelta(seconds=prev_duration_s * (i + 1))).isoformat(),
             "finished_at": (start - _dt.timedelta(seconds=prev_duration_s * i)).isoformat(),
             "success": True, "duration_ms": int(prev_duration_s * 1000)} for i in range(3)]
    return {"runs": [{"started_at": start.isoformat(), "finished_at": None,
                      "success": None, "worker_id": "w_nas_c2_a8"}] + done}


@pytest.fixture
def fake_runs(sm, monkeypatch):
    box: dict = {}
    monkeypatch.setattr(sm, "_http_get_json", lambda url, timeout=10.0: box["runs"])
    return box


def test_hour_long_tick_is_in_progress(sm, fake_runs):
    # crawl-host-import: lease 1800 だが 1 tick は 3600 秒。 50 分経過は普段どおり
    fake_runs["runs"] = _runs(running_for_s=3000)
    busy = sm._long_tick_in_progress("http://x", "crawl-host-import", 1800, _NOW)
    assert busy is not None and busy["allow_s"] == 5400


def test_tick_far_beyond_usual_is_not_excused(sm, fake_runs):
    fake_runs["runs"] = _runs(running_for_s=6000)
    assert sm._long_tick_in_progress("http://x", "crawl-host-import", 1800, _NOW) is None


def test_lease_is_the_floor_without_history(sm, fake_runs):
    fake_runs["runs"] = {"runs": [{"started_at": (_NOW - _dt.timedelta(seconds=500)).isoformat(),
                                   "finished_at": None}]}
    assert sm._long_tick_in_progress("http://x", "s", 600, _NOW) is not None
    assert sm._long_tick_in_progress("http://x", "s", 300, _NOW) is None


def test_finished_latest_run_means_not_running(sm, fake_runs):
    r = _runs(running_for_s=10)
    r["runs"][0]["finished_at"] = _NOW.isoformat()
    fake_runs["runs"] = r
    assert sm._long_tick_in_progress("http://x", "s", 3600, _NOW) is None


def test_watcher_stays_quiet_during_long_tick(sm, fake_runs, caplog):
    fake_runs["runs"] = _runs(running_for_s=2700)
    state = {
        "stall_watcher_cfg": {"enabled": True, "apply_mode": True},
        "control_url": "http://x",
        "self_loop_cache": {},
        "client": None,
    }
    wl = {"slug": "pipeline-db-cleanup", "enabled": True, "workload_type": "self_loop"}
    ws = {"slug": "pipeline-db-cleanup", "pending": 6, "claimed": 1,
          "throughput_min": 0.0, "silence_secs": 2700, "lease_secs": 3600}
    # 1 回目は enabled_since を記録するだけで silence が 0 に丸められるので、 過去に戻す
    sm._self_loop_stall_watcher(state, [ws], [wl])
    state["stall_enabled_since"]["pipeline-db-cleanup"] -= 10_000
    caplog.clear()
    out = sm._self_loop_stall_watcher(state, [ws], [wl])
    assert "SILENT" not in caplog.text
    assert out["stalled"]["pipeline-db-cleanup"]["case"] == "long_tick_running"
    assert not [a for a in out.get("actions", []) if a.get("slug") == "pipeline-db-cleanup"]
