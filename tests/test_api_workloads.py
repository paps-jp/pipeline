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
