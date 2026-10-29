"""plugin_blob_store は MinIO 不在で control plane を道連れにしない。

2026-09-19: .48 (RAM ディスク MinIO) が落ちている間、 サムネ PUT が urllib3 の
backoff 付きリトライ (~20 秒) を event loop 上で待ち、 :8001 全体が accept を
止めた (Recv-Q 900)。 サムネは捨ててよいキャッシュなので、 連続失敗したら暫く
呼ばずに即 BlobStoreUnavailable を返す。 API 側はそれを 503 にする。
"""

from __future__ import annotations

import sys
import types

import pytest


class S3Error(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class _FakeMinio:
    calls = 0
    mode = "down"          # "down" | "nokey" | "ok"

    def __init__(self, *a, **k):
        self.http_client = k.get("http_client")

    def bucket_exists(self, b):
        return True

    def put_object(self, *a, **k):
        type(self).calls += 1
        if type(self).mode == "down":
            raise ConnectionError("EHOSTUNREACH")
        if type(self).mode == "nokey":
            raise S3Error("NoSuchBucket")


@pytest.fixture
def bs(monkeypatch):
    m = types.ModuleType("minio")
    m.Minio = _FakeMinio
    err = types.ModuleType("minio.error")
    err.S3Error = S3Error
    monkeypatch.setitem(sys.modules, "minio", m)
    monkeypatch.setitem(sys.modules, "minio.error", err)
    if "urllib3" not in sys.modules:
        u = types.ModuleType("urllib3")
        u.PoolManager = lambda **k: k
        u.Timeout = lambda **k: k
        monkeypatch.setitem(sys.modules, "urllib3", u)
    from pipeline.storage import plugin_blob_store as mod
    monkeypatch.setattr(mod, "_client", None)
    monkeypatch.setattr(mod, "_env_cache", {"PAPRIKA_MINIO_VIDEO_ENDPOINT": "10.0.0.1:9000"})
    monkeypatch.setattr(mod, "_fail_streak", 0)
    monkeypatch.setattr(mod, "_open_until", 0.0)
    _FakeMinio.calls = 0
    _FakeMinio.mode = "down"
    return mod


def test_breaker_opens_after_consecutive_failures(bs):
    for _ in range(bs._BREAK_AFTER):
        with pytest.raises(bs.BlobStoreUnavailable):
            bs.put("s", "k", b"x", "image/jpeg")
    assert _FakeMinio.calls == bs._BREAK_AFTER
    # 遮断中は MinIO を呼ばずに即返す
    with pytest.raises(bs.BlobStoreUnavailable):
        bs.put("s", "k", b"x", "image/jpeg")
    assert _FakeMinio.calls == bs._BREAK_AFTER


def test_breaker_half_opens_and_recovers(bs, monkeypatch):
    for _ in range(bs._BREAK_AFTER):
        with pytest.raises(bs.BlobStoreUnavailable):
            bs.put("s", "k", b"x", "image/jpeg")
    monkeypatch.setattr(bs, "_open_until", 0.0)     # 遮断時間が過ぎた
    _FakeMinio.mode = "ok"
    bs.put("s", "k", b"x", "image/jpeg")
    assert bs._fail_streak == 0


def test_s3_errors_do_not_count_as_outage(bs):
    """S3 の応答が返る = MinIO は生きている。 遮断の理由にしない。"""
    _FakeMinio.mode = "nokey"
    for _ in range(bs._BREAK_AFTER + 1):
        with pytest.raises(S3Error):
            bs.put("s", "k", b"x", "image/jpeg")
    assert bs._fail_streak == 0 and bs._open_until == 0.0


def test_client_has_short_timeouts_and_no_retries(bs):
    _FakeMinio.mode = "ok"
    bs.put("s", "k", b"x", "image/jpeg")
    http = bs._client.http_client
    assert http is not None
    retries = http.get("retries") if isinstance(http, dict) else http.connection_pool_kw.get("retries")
    # 実物の urllib3 は False を Retry(total=False) に正規化する
    assert retries is False or getattr(retries, "total", None) is False
