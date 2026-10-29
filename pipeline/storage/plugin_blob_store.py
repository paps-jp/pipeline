"""plugin_runtime blob の実体を RAM ディスク MinIO (.48 video-ram) に置く薄いラッパー。

2026-08-25: 従来は plugin_runtime_blob (SQLite) に生バイナリを INSERT していたが、
control plane は単一 connection + threading.RLock で全書込みを直列化しており、
画像サムネイル PUT (~1.6 件/秒) がそのロックを奪い合って全 worker の
heartbeat/queue claim を巻き込み遅延させていた
(paprika-image-pull が「もっさり」と報告された件の真因)。
この blob は UI プレビュー用の短命キャッシュ (旧 TTL 30min) に過ぎないので、
SQLite でなく RAM ディスク MinIO (bucket 分離) へ出す。

2026-09-19: .48 が落ちている間、 既定の urllib3 (backoff 付き 5 回リトライ ≒ 20 秒) で
1 回ごとに待たされ、 :8001 全体が accept を止めた。 サムネは捨ててよいキャッシュなので
短い timeout・リトライ無しで呼び、 連続で失敗したら暫く呼ばずに
``BlobStoreUnavailable`` を即返す (API は 503 にする)。
"""

from __future__ import annotations

import datetime as _dt
import os
import threading
import time
from io import BytesIO
from typing import Any

_BUCKET = "plugin-thumb"
_lock = threading.Lock()
_client: Any = None
_env_cache: dict[str, str] | None = None

_CONNECT_TIMEOUT_S = 2.0
_READ_TIMEOUT_S = 5.0
_BREAK_AFTER = 3          # この回数連続で失敗したら遮断する
_BREAK_FOR_S = 30.0       # 遮断している秒数 (過ぎたら 1 回だけ試す)
_fail_streak = 0
_open_until = 0.0


class BlobStoreUnavailable(RuntimeError):
    """MinIO に届かない / 遮断中。 呼び出し側は 503 にする。"""


def _env() -> dict[str, str]:
    global _env_cache
    if _env_cache is None:
        from pipeline.storage.env import load_env_file
        path = os.environ.get("PIPELINE_ENV_FILE", "/etc/pipeline/.env")
        _env_cache = load_env_file(path)
    return _env_cache


def _cfg(key: str) -> str | None:
    return _env().get(key) or os.environ.get(key)


def _get_client() -> Any:
    global _client
    if _client is not None:
        return _client
    with _lock:
        if _client is not None:
            return _client
        from minio import Minio
        endpoint = _cfg("PAPRIKA_MINIO_VIDEO_ENDPOINT")
        access = _cfg("PAPRIKA_MINIO_ACCESS_KEY")
        secret = _cfg("PAPRIKA_MINIO_SECRET_KEY")
        if not endpoint:
            raise RuntimeError(
                "plugin_blob_store: PAPRIKA_MINIO_VIDEO_ENDPOINT 未設定 (env 不足)")
        import urllib3
        http = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=_CONNECT_TIMEOUT_S, read=_READ_TIMEOUT_S),
            retries=False, maxsize=16)
        c = Minio(endpoint, access_key=access, secret_key=secret, secure=False,
                  http_client=http)
        if not c.bucket_exists(_BUCKET):
            c.make_bucket(_BUCKET)
        _client = c
        return _client


def _object_key(slug: str, key: str) -> str:
    return f"{slug}/{key}"


def _call(fn, *args, **kwargs):
    """遮断器越しに MinIO を呼ぶ。 S3 の応答 (NoSuchKey 等) は MinIO が生きている証拠
    なので失敗に数えず、 そのまま投げ直す。"""
    global _fail_streak, _open_until
    from minio.error import S3Error
    if time.monotonic() < _open_until:
        raise BlobStoreUnavailable("plugin_blob_store: MinIO 遮断中")
    try:
        r = fn(_get_client(), *args, **kwargs)
    except S3Error:
        _fail_streak = 0
        raise
    except Exception as e:
        _fail_streak += 1
        if _fail_streak >= _BREAK_AFTER:
            _open_until = time.monotonic() + _BREAK_FOR_S
        raise BlobStoreUnavailable(f"plugin_blob_store: {type(e).__name__}: {e}") from e
    _fail_streak = 0
    return r


def put(slug: str, key: str, data: bytes, content_type: str) -> None:
    _call(lambda c: c.put_object(_BUCKET, _object_key(slug, key), BytesIO(data),
                                 len(data), content_type=content_type))


def get(slug: str, key: str) -> tuple[bytes, str, str] | None:
    """(data, content_type, updated_at_iso) を返す。 無ければ None。"""
    from minio.error import S3Error

    def _get(c):
        resp = c.get_object(_BUCKET, _object_key(slug, key))
        try:
            return resp, resp.read()
        finally:
            resp.close()
            resp.release_conn()

    try:
        resp, data = _call(_get)
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
    ct = resp.headers.get("Content-Type") or "application/octet-stream"
    lm = resp.headers.get("Last-Modified")
    updated_at = lm or _dt.datetime.now(_dt.timezone.utc).isoformat()
    return data, ct, updated_at


def delete(slug: str, key: str) -> None:
    try:
        _call(lambda c: c.remove_object(_BUCKET, _object_key(slug, key)))
    except BlobStoreUnavailable:
        raise
    except Exception:
        pass  # 元々 SQLite 版も未存在キーの DELETE はエラーにしていなかった


def purge_older_than(ttl_minutes: int = 30) -> int:
    """ttl_minutes より古い object を削除して件数を返す。"""
    if time.monotonic() < _open_until:
        raise BlobStoreUnavailable("plugin_blob_store: MinIO 遮断中")
    c = _get_client()
    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=ttl_minutes)
    n = 0
    for obj in c.list_objects(_BUCKET, recursive=True):
        lm = obj.last_modified
        if lm is None:
            continue
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=_dt.timezone.utc)
        if lm < cutoff:
            c.remove_object(_BUCKET, obj.object_name)
            n += 1
    return n
