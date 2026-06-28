"""/api/v1/workloads エンドポイントの統合テスト (TestClient)。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app


@pytest.fixture
def client() -> TestClient:
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    app = create_app(settings)
    return TestClient(app)


def _sample(slug: str = "image-resize") -> dict:
    return {
        "slug": slug,
        "name": "Image Resize",
        "executor_type": "shell",
        "executor_config": {"command": "echo {task.pk}"},
    }


def test_list_empty(client: TestClient) -> None:
    with client:
        r = client.get("/api/v1/workloads")
        assert r.status_code == 200
        body = r.json()
        assert body == {"workloads": [], "total": 0}


def test_create_workload(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/workloads", json=_sample())
        assert r.status_code == 201
        body = r.json()
        assert body["slug"] == "image-resize"
        assert body["queue_table"] == "image_resize_queue"


def test_create_duplicate(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        r = client.post("/api/v1/workloads", json=_sample())
        assert r.status_code == 409


def test_get_workload(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        r = client.get("/api/v1/workloads/image-resize")
        assert r.status_code == 200
        assert r.json()["name"] == "Image Resize"


def test_get_404(client: TestClient) -> None:
    with client:
        r = client.get("/api/v1/workloads/nope")
        assert r.status_code == 404


def test_update_workload(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        upd = _sample()
        upd.pop("slug")  # PUT body には slug は不要
        upd["name"] = "v2"
        upd["priority"] = 200
        r = client.put("/api/v1/workloads/image-resize", json=upd)
        assert r.status_code == 200
        assert r.json()["name"] == "v2"
        assert r.json()["priority"] == 200


def test_patch_enabled(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        r = client.patch("/api/v1/workloads/image-resize/enabled", json={"enabled": True})
        assert r.status_code == 200
        assert r.json()["enabled"] is True


def test_delete(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        r = client.delete("/api/v1/workloads/image-resize")
        assert r.status_code == 204
        assert client.get("/api/v1/workloads/image-resize").status_code == 404


def test_invalid_slug_rejected(client: TestClient) -> None:
    with client:
        bad = _sample(slug="Bad Slug!")
        r = client.post("/api/v1/workloads", json=bad)
        assert r.status_code == 422


def test_vram_observation_peak_smoothing(client: TestClient) -> None:
    """worker self-report の VRAM 観測値が peak に平滑化保存される。
    上昇は即時、 下降は前回 peak の 95% を最低ラインに徐々に追従。
    """
    with client:
        client.post("/api/v1/workloads", json=_sample())
        slug = "image-resize"
        # 1) 1 回目: 2000 MB → peak=2000
        r = client.post(f"/api/v1/workloads/{slug}/vram_observation",
                        json={"used_mb": 2000, "worker_id": "w_test_1"})
        assert r.status_code == 200
        body = r.json()
        assert body["accepted"] is True
        assert body["observed_vram_mb_peak"] == 2000
        assert body["observed_vram_sample_count"] == 1
        # 2) 上昇: 3500 MB → peak=3500 (即時)
        r = client.post(f"/api/v1/workloads/{slug}/vram_observation",
                        json={"used_mb": 3500, "worker_id": "w_test_1"})
        assert r.json()["observed_vram_mb_peak"] == 3500
        # 3) 下降: 1000 MB → max(3500*0.95, 1000) = 3325 (ゆるく降下)
        r = client.post(f"/api/v1/workloads/{slug}/vram_observation",
                        json={"used_mb": 1000, "worker_id": "w_test_1"})
        assert r.json()["observed_vram_mb_peak"] == 3325
        # 4) workload GET で永続化を確認
        w = client.get(f"/api/v1/workloads/{slug}").json()
        assert w["observed_vram_mb_peak"] == 3325
        assert w["observed_vram_sample_count"] == 3
        assert w["observed_vram_updated_at"] is not None


def test_vram_observation_unknown_slug_404(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/workloads/no-such/vram_observation",
                        json={"used_mb": 1000})
        assert r.status_code == 404


def test_vram_observation_validates_used_mb(client: TestClient) -> None:
    with client:
        client.post("/api/v1/workloads", json=_sample())
        # 負値 reject
        r = client.post("/api/v1/workloads/image-resize/vram_observation",
                        json={"used_mb": -5})
        assert r.status_code == 422
        # 上限超過 reject
        r = client.post("/api/v1/workloads/image-resize/vram_observation",
                        json={"used_mb": 999_999})
        assert r.status_code == 422
