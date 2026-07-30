"""/api/v1/create + /api/v1/api-keys の統合テスト。

MariaDB / MinIO は持ち込まず、 `app.state.create_backend` に fake を差し込んで
「HTTP 層 → 投入契約の呼び出し」までを検証する。 投入契約そのもの (SQL) は
`CreateBackend` を fake DB で叩く test_api_public_backend.py 側で見る。
"""

from __future__ import annotations

import io

import pytest
from fastapi.testclient import TestClient

from pipeline.api_public.backend import CreateError, CreateResult
from pipeline.config import Settings
from pipeline.control.server import create_app

JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
MP4 = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64


class FakeBackend:
    def __init__(self) -> None:
        self.images: list[dict] = []
        self.videos: list[dict] = []
        self.fail_with: CreateError | None = None
        self._next = 1000

    def create_image(self, *, data, url, site, ext):
        if self.fail_with:
            raise self.fail_with
        self.images.append({"bytes": len(data), "url": url, "site": site, "ext": ext})
        self._next += 1
        return CreateResult(self._next, "image", dedup=False, state="queued")

    def create_video(self, *, data, url, site, ext, mime=None, page_url=None):
        if self.fail_with:
            raise self.fail_with
        self.videos.append({"bytes": len(data), "url": url, "site": site, "ext": ext,
                            "mime": mime, "page_url": page_url})
        self._next += 1
        return CreateResult(self._next, "video", dedup=False, state="queued")

    def image_status(self, image_id):
        if image_id != 1001:
            return None
        return {"image_id": 1001, "state": "hashed",
                "faces": [{"face_id": 7, "embedded": True, "adaface_ready": 1}]}

    def video_status(self, video_id):
        return None


@pytest.fixture
def client() -> TestClient:
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    app = create_app(settings)
    app.state.create_backend = FakeBackend()
    return TestClient(app)


def _issue_key(client: TestClient, user_slug: str = "acme") -> str:
    r = client.post("/api/v1/api-keys", json={"user_slug": user_slug, "name": "test"})
    assert r.status_code == 201, r.text
    return r.json()["api_key"]


def _auth(key: str) -> dict:
    return {"Authorization": f"Bearer {key}"}


# ---------------- API キー ----------------


def test_issue_and_list_key(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/api-keys", json={"user_slug": "acme", "name": "batch loader"})
        assert r.status_code == 201
        body = r.json()
        assert body["api_key"].startswith("plk_")
        assert body["create_site"] == "create-acme"

        keys = client.get("/api/v1/api-keys").json()["keys"]
        assert len(keys) == 1
        # 平文もハッシュも一覧には出さない
        assert "key_hash" not in keys[0]
        assert "api_key" not in keys[0]


def test_revoked_key_is_rejected(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/api-keys", json={"user_slug": "acme", "name": "t"}).json()
        key, key_id = r["api_key"], r["id"]
        assert client.get("/api/v1/create/limits", headers=_auth(key)).status_code == 200
        assert client.delete(f"/api/v1/api-keys/{key_id}").status_code == 204
        assert client.get("/api/v1/create/limits", headers=_auth(key)).status_code == 401


def test_expired_key_is_rejected(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/api-keys",
                        json={"user_slug": "acme", "name": "t",
                              "expires_at": "2020-01-01T00:00:00+00:00"}).json()
        assert client.get("/api/v1/create/limits",
                          headers=_auth(r["api_key"])).status_code == 401


# ---------------- 認証 ----------------


def test_create_requires_auth(client: TestClient) -> None:
    with client:
        r = client.post("/api/v1/create/images/url", json={"url": "https://example.com/a.jpg"})
        assert r.status_code == 401


def test_garbage_key_rejected(client: TestClient) -> None:
    with client:
        for bad in ("nonsense", "plk_deadbeef_wrongsecret", "plk__", "plk_a_b_c"):
            r = client.get("/api/v1/create/limits", headers=_auth(bad))
            assert r.status_code == 401, bad


# ---------------- 画像投入 ----------------


def test_upload_image(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images",
                        headers=_auth(key),
                        files={"file": ("a.jpg", io.BytesIO(JPEG), "image/jpeg")})
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["accepted"] == 1 and body["failed"] == 0
        item = body["items"][0]
        assert item["ok"] and item["kind"] == "image" and item["id"] == 1001

        be = client.app.state.create_backend
        assert be.images == [{"bytes": len(JPEG), "url": None,
                              "site": "create-acme", "ext": "jpg"}]


def test_upload_png_is_sniffed_not_trusted_from_name(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        # 名前は .jpg / Content-Type も image/jpeg だが中身は PNG → ext は png になる
        r = client.post("/api/v1/create/images",
                        headers=_auth(key),
                        files={"file": ("a.jpg", io.BytesIO(PNG), "image/jpeg")})
        assert r.status_code == 201
        assert client.app.state.create_backend.images[0]["ext"] == "png"


def test_upload_non_image_rejected(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images",
                        headers=_auth(key),
                        files={"file": ("a.jpg", io.BytesIO(b"not an image at all"),
                                        "image/jpeg")})
        assert r.status_code == 201        # バッチ形なので 201 + item.error
        item = r.json()["items"][0]
        assert item["ok"] is False and item["error"] == "unsupported_format"
        assert client.app.state.create_backend.images == []


def test_upload_empty_rejected(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images", headers=_auth(key),
                        files={"file": ("a.jpg", io.BytesIO(b""), "image/jpeg")})
        assert r.json()["items"][0]["error"] == "empty"


def test_video_upload_uses_video_sniffer(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/videos", headers=_auth(key),
                        files={"file": ("v.mp4", io.BytesIO(MP4), "video/mp4")})
        assert r.status_code == 201, r.text
        assert r.json()["items"][0]["ok"] is True
        assert client.app.state.create_backend.videos[0]["ext"] == "mp4"


def test_jpeg_posted_to_videos_is_rejected(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/videos", headers=_auth(key),
                        files={"file": ("v.mp4", io.BytesIO(JPEG), "video/mp4")})
        assert r.json()["items"][0]["error"] == "unsupported_format"


def test_storage_failure_surfaces_as_item_error(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        client.app.state.create_backend.fail_with = CreateError("storage_failed", "MinIO down")
        r = client.post("/api/v1/create/images", headers=_auth(key),
                        files={"file": ("a.jpg", io.BytesIO(JPEG), "image/jpeg")})
        body = r.json()
        assert body["failed"] == 1
        assert body["items"][0]["error"] == "storage_failed"


# ---------------- URL 投入 (SSRF ガード) ----------------


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/a.jpg",
    "http://localhost/a.jpg",
    "http://10.10.50.7:8001/a.jpg",
    "http://192.168.1.1/a.jpg",
    "http://169.254.169.254/latest/meta-data/",
])
def test_private_urls_are_refused(client: TestClient, url: str) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images/url", headers=_auth(key), json={"url": url})
        assert r.status_code == 201
        item = r.json()["items"][0]
        assert item["ok"] is False
        assert item["error"] in ("private_address", "dns_failed")
        assert client.app.state.create_backend.images == []


def test_non_http_scheme_refused(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images/url", headers=_auth(key),
                        json={"url": "file:///etc/passwd"})
        assert r.json()["items"][0]["error"] == "bad_scheme"


def test_url_or_urls_required(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images/url", headers=_auth(key), json={})
        assert r.status_code == 422


def test_too_many_urls_refused(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/images/url", headers=_auth(key),
                        json={"urls": [f"https://example.com/{i}.jpg" for i in range(101)]})
        assert r.status_code == 422


# ---------------- 状態照会 / limits ----------------


def test_image_status(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.get("/api/v1/create/images/1001", headers=_auth(key))
        assert r.status_code == 200
        assert r.json()["faces"][0]["embedded"] is True
        assert client.get("/api/v1/create/images/9999",
                          headers=_auth(key)).status_code == 404


def test_video_url_create_is_guarded_too(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        r = client.post("/api/v1/create/videos/url", headers=_auth(key),
                        json={"url": "http://10.10.50.8:9100/paprika/x.mp4"})
        assert r.json()["items"][0]["error"] == "private_address"
        assert client.app.state.create_backend.videos == []


def test_video_status_404(client: TestClient) -> None:
    with client:
        key = _issue_key(client)
        assert client.get("/api/v1/create/videos/1",
                          headers=_auth(key)).status_code == 404


def test_limits_reports_site_and_quota(client: TestClient) -> None:
    with client:
        key = _issue_key(client, "beta")
        r = client.get("/api/v1/create/limits", headers=_auth(key))
        assert r.status_code == 200
        body = r.json()
        assert body["site"] == "create-beta"
        assert body["max_image_bytes"] > 0
