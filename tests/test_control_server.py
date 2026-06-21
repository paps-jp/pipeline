"""FastAPI app の基本テスト。`TestClient` で in-process に叩く。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app


def _client() -> TestClient:
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    app = create_app(settings)
    return TestClient(app)


def test_root_html() -> None:
    """`/` は React build があれば SPA index.html、無ければ fallback HTML を返す。"""
    with _client() as c:
        r = c.get("/")
        assert r.status_code == 200
        # SPA index.html (build 済み) は <div id="root"> を含む
        # fallback HTML は /docs リンクを含む
        assert ('id="root"' in r.text) or ("/docs" in r.text)
        assert "Pipeline" in r.text or "pipeline" in r.text.lower()


def test_health() -> None:
    with _client() as c:
        r = c.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json()["ok"] is True


def test_status() -> None:
    with _client() as c:
        r = c.get("/api/v1/status")
        assert r.status_code == 200
        body = r.json()
        assert body["mode"] == "dev"
        assert "version" in body
        assert "now" in body


def test_openapi_includes_endpoints() -> None:
    with _client() as c:
        r = c.get("/openapi.json")
        assert r.status_code == 200
        paths = r.json()["paths"]
        assert "/api/v1/status" in paths
        assert "/api/v1/health" in paths
        assert "/api/v1/workloads" in paths
        assert "/api/v1/workloads/{slug}" in paths
