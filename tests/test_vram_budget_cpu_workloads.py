"""workloads_for_worker の VRAM 予算が requires_gpu=False の workload を数えないことの回帰テスト。

CPU workload の observed_vram_mb_peak は観測が来ないので凍結し、 実使用 0MB の worker が
GPU ホストの予算を架空に埋めていた (face-person-link 3284MB、 2026-11-20)。

守りたいこと:
  - CPU workload を回している worker は、 他 worker の予算を埋めない
  - CPU workload 自身も予算で候補から外れない
  - GPU workload どうしの予算判定は従来どおり効く
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app

_CAP_MB = 10_000   # 予算 = 8500 (PIPELINE_VRAM_SAFETY_FRAC 既定 0.85)
_PEAK_MB = 6_000   # 1 つなら収まり、 2 つ足すと超える


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.delenv("PIPELINE_VRAM_SAFETY_FRAC", raising=False)
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    with TestClient(create_app(settings)) as c:
        yield c


def _workload(client, slug: str, *, gpu: bool) -> None:
    r = client.post("/api/v1/workloads", json={
        "slug": slug, "name": slug, "enabled": True, "requires_gpu": gpu,
        "executor_type": "shell", "executor_config": {"command": ["echo", "hi"]},
        "batch_size": 5, "lease_secs": 30, "max_attempts": 2,
    })
    assert r.status_code == 201, r.text
    r = client.post(f"/api/v1/workloads/{slug}/vram_observation",
                    json={"used_mb": _PEAK_MB, "worker_id": None})
    assert r.status_code < 300, r.text
    assert client.get(f"/api/v1/workloads/{slug}").json()["observed_vram_mb_peak"] == _PEAK_MB


def _worker(client, wid: str, running: str | None = None) -> str:
    r = client.post("/api/v1/workers", json={
        "host": "gpuhost", "worker_id": wid, "resources": {"gpu_vram_mb": _CAP_MB}})
    assert r.status_code == 201, r.text
    if running:
        r = client.put(f"/api/v1/workers/{wid}/heartbeat", json={"current_workload": running})
        assert r.status_code == 200, r.text
    return wid


def _offered(client, wid: str) -> set[str]:
    r = client.get(f"/api/v1/workers/{wid}/workloads")
    assert r.status_code == 200, r.text
    return {w["slug"] for w in r.json()["workloads"]}


def test_cpu_workload_does_not_fill_gpu_budget(client):
    _workload(client, "cpu-link", gpu=False)
    _workload(client, "gpu-embed", gpu=True)
    _worker(client, "w_other", running="cpu-link")
    me = _worker(client, "w_me")
    # 旧実装: used 6000 (cpu-link) + 6000 > 8500 で gpu-embed も cpu-link も外れていた
    assert _offered(client, me) == {"cpu-link", "gpu-embed"}


def test_cpu_workload_offered_even_when_gpu_budget_is_full(client):
    _workload(client, "cpu-link", gpu=False)
    _workload(client, "gpu-embed", gpu=True)
    _worker(client, "w_other", running="gpu-embed")
    me = _worker(client, "w_me")
    offered = _offered(client, me)
    assert "cpu-link" in offered        # 旧実装: 6000 + 6000 > 8500 で外れていた
    assert "gpu-embed" not in offered   # GPU どうしの予算判定は従来どおり


def test_gpu_budget_still_applies_between_gpu_workloads(client):
    _workload(client, "gpu-a", gpu=True)
    _workload(client, "gpu-b", gpu=True)
    _worker(client, "w_other", running="gpu-a")
    me = _worker(client, "w_me")
    assert _offered(client, me) == set()
