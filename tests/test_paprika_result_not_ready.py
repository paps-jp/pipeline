"""完了直後の「結果がまだ書かれていない」/result を空の job と取り違えない。

Paprika hub はワーカーから完了を受け取ると、 先に status=completed と
``progress.assets_saved`` を保存し、 その後で結果 (assets 一覧) を保存する。
その間に ``GET /jobs/{id}/result`` を読むと ``assets: []`` が返る。
2026-11-12 の実測では、 完了直後に 1 回目を読んだ動画 job 83 件のうち 43 件
(52%) がこの空応答だった。

- image-pull はそれを「画像ゼロの job」と読んで hub purge に回し、 後から結果に
  載るはずの動画を全 tier から消していた。 video-face-extract の
  ``download failed`` (NoSuchKey) が完了とほぼ同数だった真因
- video-pull は no_video として読み飛ばし watermark を進めるので、 その動画は
  失敗としても記録されずに失われていた

もう 1 つの経路: hub は yt-dlp の進捗行 100% を受けると、 assets_saved=0 のまま
completed を保存する。 結果はワーカーがアップロードを終えたときに書かれる (実測 2〜125 秒)。
完了直後に空だった動画 job はほぼ全部こちらで、 一覧の ``progress.download_pct`` が付いている。

ここで固定するのは:
  1. 判定: assets_saved>0 または download_pct 付きなのに assets が空で、 completed から間もない
  2. 古い空応答 (max_age 超) と assets_saved=0 は本当に空として扱う (固着しない)
  3. video-pull: 未保存の job は INSERT せず、 watermark をその手前で止め、
     次 tick で結果が書かれていれば取り込む
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1] / "plugins"
_IM = _ROOT / "paprika_image_pull" / "image_main.py"
_VP = _ROOT / "paprika_video_pull" / "bridge_main.py"
pytestmark = pytest.mark.skipif(not (_IM.exists() and _VP.exists()),
                                reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def im():
    stubs = {
        "mariadb": types.ModuleType("mariadb"),
        "requests": types.ModuleType("requests"),
        "PIL": types.ModuleType("PIL"),
        "PIL.Image": types.ModuleType("PIL.Image"),
        "pipeline": types.ModuleType("pipeline"),
        "pipeline.db": types.ModuleType("pipeline.db"),
        "pipeline.db.plugin_pool": types.ModuleType("pipeline.db.plugin_pool"),
    }
    stubs["PIL"].Image = stubs["PIL.Image"]
    stubs["pipeline.db.plugin_pool"].MariaPool = object
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location("_image_main_not_ready", _IM)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        yield m
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


@pytest.fixture(scope="module")
def vp():
    if "requests" not in sys.modules:
        sys.modules["requests"] = types.ModuleType("requests")
    spec = importlib.util.spec_from_file_location("_video_pull_not_ready", _VP)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S.%f")


def _job(job_id: str, completed_at: datetime, assets_saved: int = 3,
         download_pct: "float | None" = None) -> dict:
    return {"job_id": job_id, "status": "completed", "completed_at": _iso(completed_at),
            "progress": {"assets_saved": assets_saved, "download_pct": download_pct}}


# ---------------------------------------------------------------- 判定 (両 plugin 共通)

@pytest.fixture(params=["image", "video"])
def judge(request, im, vp):
    return (im if request.param == "image" else vp)._result_not_ready


def test_empty_result_right_after_completion_is_not_ready(judge):
    now = datetime.now(timezone.utc)
    assert judge(_job("a", now - timedelta(seconds=2)), {"assets": []}, 300, now)


def test_result_with_assets_is_ready(judge):
    now = datetime.now(timezone.utc)
    res = {"assets": [{"name": "v.mp4", "mime": "video/mp4"}]}
    assert not judge(_job("a", now - timedelta(seconds=2)), res, 300, now)


def test_job_that_saved_nothing_is_really_empty(judge):
    now = datetime.now(timezone.utc)
    assert not judge(_job("a", now - timedelta(seconds=2), assets_saved=0),
                     {"assets": []}, 300, now)


def test_ytdlp_completion_with_zero_assets_saved_is_not_ready(judge):
    # yt-dlp 100% で completed になった job: assets_saved=0 だが download_pct が付く
    now = datetime.now(timezone.utc)
    assert judge(_job("a", now - timedelta(seconds=2), assets_saved=0, download_pct=100.0),
                 {"assets": []}, 300, now)


def test_old_ytdlp_completion_that_stayed_empty_is_trusted(judge):
    now = datetime.now(timezone.utc)
    assert not judge(_job("a", now - timedelta(seconds=301), assets_saved=0,
                          download_pct=100.0), {"assets": []}, 300, now)


def test_old_empty_result_is_trusted_so_watermark_cannot_pin(judge):
    now = datetime.now(timezone.utc)
    assert not judge(_job("a", now - timedelta(seconds=301)), {"assets": []}, 300, now)


def test_max_age_zero_disables_the_guard(judge):
    now = datetime.now(timezone.utc)
    assert not judge(_job("a", now - timedelta(seconds=2)), {"assets": []}, 0, now)


# ---------------------------------------------------------------- video-pull の tick

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


@pytest.fixture
def env(vp, monkeypatch):
    rec = {"saved": [], "inserted": [], "jobs": [], "not_ready": set(), "lists": 0}
    monkeypatch.setattr(vp, "_save_watermark", lambda db, slug, dt: rec["saved"].append(dt))
    monkeypatch.setattr(vp, "_self_enqueue_next_tick", lambda *a, **k: None)
    monkeypatch.setattr(vp.time, "sleep", lambda s: None)

    def fake_insert(db, job, asset, min_bytes, crawl_id=0):
        rec["inserted"].append(job["job_id"])
        return "inserted"
    monkeypatch.setattr(vp, "_insert_video", fake_insert)

    def fake_get(url, timeout=0.0):
        if "/jobs/completes" in url:
            # page_limit 未満を返すので、 1 tick に 1 回しか呼ばれない (count_lt_limit)
            rec["lists"] += 1
            return {"jobs": rec["jobs"]}
        job_id = url.rsplit("/", 2)[-2]
        if job_id in rec["not_ready"]:
            return {"job_id": job_id, "status": "completed", "assets": []}
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
        "fetch_concurrency": 4, "max_lag_s": 6 * 3600,
        "result_not_ready_max_age_s": 300.0,
    }
    vp._adapt_init(st, {})
    st.update(kw)
    return st


def test_video_pull_holds_watermark_before_unsaved_result(vp, env):
    base = datetime.now(timezone.utc) - timedelta(seconds=20)
    env["jobs"] = [_job(f"j{i}", base + timedelta(seconds=i)) for i in range(5)]
    env["not_ready"] = {"j2"}
    st = _state(vp, base - timedelta(seconds=1))
    out = vp.process(None, None, st)

    # 未保存の j2 だけ取り込まず、 前後の job は普通に取り込む
    assert env["inserted"] == ["j0", "j1", "j3", "j4"]
    assert out["result_not_ready"] == 1
    # watermark は j2 の手前で止まる (j4 まで進めると j2 を二度と読まない)
    j2_at = base + timedelta(seconds=2)
    assert st["watermark"] < j2_at
    assert st["watermark"] >= base + timedelta(seconds=1)
    assert all(dt < j2_at for dt in env["saved"])

    # 次 tick: 結果が書かれていれば j2 を取り込み、 watermark は末尾まで進む
    env["not_ready"] = set()
    env["inserted"].clear()
    out2 = vp.process(None, None, st)
    assert "j2" in env["inserted"]
    assert out2["result_not_ready"] == 0
    assert st["watermark"] == base + timedelta(seconds=4)


def test_video_pull_holds_watermark_before_ytdlp_completion(vp, env):
    base = datetime.now(timezone.utc) - timedelta(seconds=20)
    env["jobs"] = [_job(f"j{i}", base + timedelta(seconds=i)) for i in range(3)]
    env["jobs"][1] = _job("j1", base + timedelta(seconds=1), assets_saved=0,
                          download_pct=100.0)
    env["not_ready"] = {"j1"}
    st = _state(vp, base - timedelta(seconds=1))
    out = vp.process(None, None, st)
    assert env["inserted"] == ["j0", "j2"]
    assert out["result_not_ready"] == 1
    assert st["watermark"] < base + timedelta(seconds=1)

    env["not_ready"] = set()
    env["inserted"].clear()
    vp.process(None, None, st)
    assert "j1" in env["inserted"]
    assert st["watermark"] == base + timedelta(seconds=2)


def test_video_pull_old_empty_result_does_not_pin_watermark(vp, env):
    base = datetime.now(timezone.utc) - timedelta(minutes=30)
    env["jobs"] = [_job(f"j{i}", base + timedelta(seconds=i)) for i in range(3)]
    env["not_ready"] = {"j1"}
    st = _state(vp, base - timedelta(seconds=1))
    out = vp.process(None, None, st)
    assert out["result_not_ready"] == 0
    assert out["no_video_jobs"] == 1
    assert st["watermark"] == base + timedelta(seconds=2)
