"""paprika-video-pull の追いつき (並列 result 取得 + 遅れ過ぎの読み飛ばし)。

2026-10-13 時点で video-pull の watermark が 13 日遅れていた。 result を 1 件ずつ
取るため 1 tick (60s) で 200-400 job しか読めず、 paprika の完了ペースに永久に
追いつかない。 取りに行く頃には動画が消えており、 video-face-extract は
download failed を 197 万件出していた。

ここで固定するのは 3 点:
  1. max_lag_s を超えて遅れた watermark は now - max_lag_s へ飛ばす
  2. 遅れが max_lag_s 以内なら飛ばさない
  3. result を並列取得しても、 INSERT と watermark 前進は completed_at の昇順
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_VP = Path(__file__).resolve().parents[1] / "plugins" / "paprika_video_pull" / "bridge_main.py"
pytestmark = pytest.mark.skipif(not _VP.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def vp():
    if "requests" not in sys.modules:
        sys.modules["requests"] = types.ModuleType("requests")
    spec = importlib.util.spec_from_file_location("video_pull_catchup_test", _VP)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Cur:
    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return (1,)

    def close(self):
        pass


class _Db:
    def cursor(self):
        return _Cur()

    def commit(self):
        pass

    def rollback(self):
        pass


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")


@pytest.fixture
def env(vp, monkeypatch):
    rec = {"saved": [], "inserted": [], "lists": [], "jobs": []}

    monkeypatch.setattr(vp, "_save_watermark",
                        lambda db, slug, dt: rec["saved"].append(dt))
    monkeypatch.setattr(vp, "_self_enqueue_next_tick", lambda *a, **k: None)
    monkeypatch.setattr(vp.time, "sleep", lambda s: None)

    def fake_insert(db, job, asset, min_bytes):
        rec["inserted"].append(job["job_id"])
        return "inserted"
    monkeypatch.setattr(vp, "_insert_video", fake_insert)

    def fake_get(url, timeout=0.0):
        if "/jobs/completes" in url:
            rec["lists"].append(url)
            # 1 ページ目だけ返し、 以降は空
            return {"jobs": rec["jobs"]} if len(rec["lists"]) == 1 else {"jobs": []}
        job_id = url.rsplit("/", 2)[-2]
        return {"assets": [{"name": f"{job_id}.mp4", "mime": "video/mp4",
                            "size": 5 << 20, "url": f"https://x/{job_id}"}]}
    monkeypatch.setattr(vp, "_http_get_json", fake_get)
    return rec


def _state(vp, watermark, **kw):
    st = {
        "db": _Db(), "db_cfg": {}, "paprika_hub": "http://hub.test:8000",
        "statuses": "completed,review", "min_video_bytes": 1 << 20,
        "control_url": "http://ctl", "workload_slug": "paprika-video-pull",
        "counter": 0, "hostname": "t", "watermark": watermark,
        "page_limit": 200, "max_pages_per_tick": 5, "tick_budget_s": 0,
        "delete_skipped_small": False, "paprika_stores": [],
        "fetch_concurrency": 8, "max_lag_s": 6 * 3600,
    }
    vp._adapt_init(st, {})
    st.update(kw)
    return st


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def test_far_behind_watermark_skips_ahead(vp, env):
    old = datetime(2026, 9, 30, 4, 0, tzinfo=timezone.utc)
    st = _state(vp, old)
    out = vp.process(None, None, st)
    assert out["skipped_from"] == old.isoformat()
    lag = (_now() - st["watermark"].replace(tzinfo=None)).total_seconds()
    assert 6 * 3600 - 120 < lag <= 6 * 3600 + 5
    # fetch cursor も飛ばした先から始まる
    assert _iso(old) not in env["lists"][0]


def test_recent_watermark_is_not_skipped(vp, env):
    wm = (datetime.now(timezone.utc) - timedelta(hours=1))
    st = _state(vp, wm)
    out = vp.process(None, None, st)
    assert "skipped_from" not in out
    assert st["watermark"] == wm


def test_max_lag_zero_disables_skip(vp, env):
    old = datetime(2026, 9, 30, 4, 0, tzinfo=timezone.utc)
    st = _state(vp, old, max_lag_s=0)
    out = vp.process(None, None, st)
    assert "skipped_from" not in out


def test_parallel_fetch_keeps_ascending_order(vp, env):
    base = datetime.now(timezone.utc) - timedelta(minutes=30)
    env["jobs"] = [
        {"job_id": f"j{i:03d}", "completed_at": _iso(base + timedelta(seconds=i)),
         "progress": {"assets_saved": 1}}
        for i in range(50)
    ]
    st = _state(vp, base - timedelta(seconds=1))
    out = vp.process(None, None, st)
    assert env["inserted"] == [j["job_id"] for j in env["jobs"]]
    assert out["inserted"] == 50 and out["fetch_failed"] == 0
    assert st["watermark"].replace(tzinfo=None) == (base + timedelta(seconds=49)).replace(tzinfo=None)


def test_jobs_without_assets_are_not_fetched(vp, env, monkeypatch):
    base = datetime.now(timezone.utc) - timedelta(minutes=30)
    env["jobs"] = [
        {"job_id": "empty", "completed_at": _iso(base), "progress": {"assets_saved": 0}},
        {"job_id": "full", "completed_at": _iso(base + timedelta(seconds=1)),
         "progress": {"assets_saved": 3}},
    ]
    fetched = []
    orig = vp._http_get_json

    def spy(url, timeout=0.0):
        if "/result" in url:
            fetched.append(url)
        return orig(url, timeout)
    monkeypatch.setattr(vp, "_http_get_json", spy)
    st = _state(vp, base - timedelta(seconds=1))
    vp.process(None, None, st)
    assert len(fetched) == 1 and "/jobs/full/result" in fetched[0]
