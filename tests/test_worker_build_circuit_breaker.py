"""executor build 失敗時のサーキットブレーカ (fail 焼きの停止 + 指数バックオフ)。

旧挙動は build 失敗で claim 済みタスクを全部 fail にして即 次の claim に進んでいた。
その結果 1 プロセスが 10 秒で 300 件を failed に焼き、 evict→rebuild 反復で VRAM
リークを加速していた (2026-08-01 ai-gpu3 image-embed failed 46,816 件)。
"""

from __future__ import annotations

import asyncio

import pytest

from pipeline.worker.service import WorkerDaemon


class FakeClient:
    """_drain_once が触る ControlClient のうち必要な分だけ。"""

    def __init__(self, workloads: list[dict], tasks_per_claim: int = 3) -> None:
        self._workloads = workloads
        self._tasks_per_claim = tasks_per_claim
        self.claims: list[str] = []
        self.fails: list[tuple[str, str]] = []
        self.runs: list[dict] = []

    async def list_workloads(self, worker_id):
        return self._workloads

    async def claim(self, worker_id, workload_slug, limit):
        self.claims.append(workload_slug)
        return [
            {"pk": f"{workload_slug}-{len(self.claims)}-{i}", "attempt": 1, "extra": {}}
            for i in range(self._tasks_per_claim)
        ]

    async def fail(self, worker_id, workload_slug, pk, error):
        self.fails.append((workload_slug, pk))

    async def record_run(self, worker_id, payload):
        self.runs.append(payload)

    async def peek_higher_pending(self, worker_id, priority):
        return False


def _workload(slug: str = "image-embed") -> dict:
    return {
        "slug": slug,
        "executor_type": "python_module",
        "batch_size": 3,
        "priority": 100,
        "requires_gpu": 0,
        "executor_config": {},
    }


@pytest.fixture()
def daemon(monkeypatch):
    d = WorkerDaemon(control_url="http://127.0.0.1:0")
    d.worker_id = "w_test_1"
    d._starvation_floor_s = 0.0
    # build は常に失敗する = VRAM 枯渇ホストの再現
    def _boom(w):
        raise RuntimeError("plugin embed_main.setup raised: CUBLAS_ALLOC_FAILED")
    monkeypatch.setattr(d, "_get_or_build_executor_observe", _boom)
    monkeypatch.setattr(d, "_evict_if_cached", lambda slug: None)
    return d


def test_build失敗でタスクをfailにしない(daemon):
    fc = FakeClient([_workload()])
    daemon._client = fc
    asyncio.run(daemon._drain_once())

    assert fc.claims == ["image-embed"], "1 サイクルで 1 回だけ claim するはず"
    assert fc.fails == [], "ホスト側の障害なのにタスクを failed に焼いている"
    assert len(fc.runs) == 1, f"runs は 1 イベント 1 行のはず (got {len(fc.runs)})"
    assert fc.runs[0]["success"] is False
    assert "executor build error" in fc.runs[0]["error"]


def test_連続失敗でclaimが停止する(daemon):
    fc = FakeClient([_workload()])
    daemon._client = fc
    asyncio.run(daemon._drain_once())
    assert daemon._build_fail_streak["image-embed"] == 1

    # バックオフ中は claim すらしない (= キューを claimed で埋めない)
    asyncio.run(daemon._drain_once())
    asyncio.run(daemon._drain_once())
    assert fc.claims == ["image-embed"], f"バックオフ中に claim している: {fc.claims}"
    assert len(fc.runs) == 1, "バックオフ中に runs を増やしている"


def test_バックオフが指数で伸びる(daemon):
    fc = FakeClient([_workload()])
    daemon._client = fc
    daemon._build_backoff_base_s = 10.0
    daemon._build_backoff_max_s = 25.0

    seen = []
    for _ in range(4):
        daemon._build_fail_until.clear()      # 猶予明けを模す
        asyncio.run(daemon._drain_once())
        import time as _t
        seen.append(round(daemon._build_fail_until["image-embed"] - _t.monotonic()))

    assert daemon._build_fail_streak["image-embed"] == 4
    # 10, 20, 25(cap), 25(cap) — ±1s の丸め許容
    assert seen[0] == pytest.approx(10, abs=1)
    assert seen[1] == pytest.approx(20, abs=1)
    assert seen[2] == pytest.approx(25, abs=1), "max でクランプされていない"
    assert seen[3] == pytest.approx(25, abs=1)


def test_build成功でバックオフが解除される(daemon, monkeypatch):
    fc = FakeClient([_workload()])
    daemon._client = fc
    asyncio.run(daemon._drain_once())
    assert "image-embed" in daemon._build_fail_until

    # ホスト復帰 (VRAM が空いた) を模す
    class _Ex:
        def supports_batch(self):
            return False
    async def _noop(*a, **k):
        return None
    monkeypatch.setattr(daemon, "_get_or_build_executor_observe", lambda w: (_Ex(), False))
    monkeypatch.setattr(daemon, "_execute_one", _noop)
    monkeypatch.setattr(daemon, "_maybe_report_vram", _noop)
    daemon._build_fail_until.clear()          # 猶予明け

    asyncio.run(daemon._drain_once())
    assert "image-embed" not in daemon._build_fail_streak, "復帰後も streak が残っている"
    assert "image-embed" not in daemon._build_fail_until, "復帰後も claim 停止が残っている"


def test_他のworkloadは巻き添えにならない(daemon):
    fc = FakeClient([_workload("image-embed"), _workload("video-face-extract")])
    daemon._client = fc
    asyncio.run(daemon._drain_once())
    # 1 サイクル目は両方 claim して両方 build 失敗する
    assert fc.claims == ["image-embed", "video-face-extract"]

    daemon._build_fail_until.pop("video-face-extract")   # こちらだけ猶予明け
    asyncio.run(daemon._drain_once())
    assert fc.claims[-1] == "video-face-extract"
    assert fc.claims.count("image-embed") == 1, "停止中の slug を claim している"
