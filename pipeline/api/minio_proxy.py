"""/api/v1/minio/{key} — control plane が MinIO 上のオブジェクトをプロキシ配信.

ユースケース: プラグイン UI から MinIO 上の顔サムネ等を `<img src>` で
直接見せたい。 MinIO は anonymous read を許してないので、 制御プレーンが env の
クレデンシャルで取って stream する。

セキュリティ:
- LAN 信頼の社内環境を前提。 外部公開時は AuthZ 追加が必要。
- 安全のため allowed-prefix を環境変数で固定 (= 既定 `crawl_face/`)。
  prefix に該当しないキーは 403。
- bucket は env `MINIO_BUCKET` (= `crawl`) で固定。

依存:
- env: MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
       MINIO_REGION (任意), MINIO_VERIFY_TLS (任意, "false" で SSL 検証 OFF)
- pip: boto3
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Iterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

log = logging.getLogger("pipeline.api.minio_proxy")
router = APIRouter(prefix="/api/v1/minio", tags=["minio_proxy"])

# allowed prefix: カンマ区切り。 既定は crawl_face/ (= 顔サムネのみ)
_ALLOWED_PREFIXES = tuple(
    p.strip() for p in os.environ.get("PIPELINE_MINIO_PROXY_PREFIXES", "crawl_face/").split(",") if p.strip()
)


def _load_env_file(path: str) -> dict[str, str]:
    """軽量 .env reader (= 既存依存に python-dotenv 無しのため自前実装)。"""
    try:
        out: dict[str, str] = {}
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
        return out
    except OSError:
        return {}


def _env(key: str, file_env: dict[str, str]) -> str | None:
    return os.environ.get(key) or file_env.get(key) or None


@lru_cache(maxsize=1)
def _client_and_bucket() -> tuple[object, str]:
    """boto3 client を遅延生成 (= モジュール import 時に env が無くても起動できるように)。
    systemd unit に MINIO_* が無いケース用に `/home/paps-ai/ai/.env` (or env で上書き)
    からの fallback 読み込みもする。"""
    try:
        import boto3  # type: ignore
        from botocore.config import Config  # type: ignore
    except ImportError as e:
        raise HTTPException(503, detail=f"boto3 not installed: {e}") from e

    env_file = os.environ.get("PIPELINE_MINIO_ENV_FILE", "/home/paps-ai/ai/.env")
    fe = _load_env_file(env_file)
    endpoint = _env("MINIO_ENDPOINT_URL", fe) or _env("MINIO_ENDPOINT", fe)
    access = _env("MINIO_ACCESS_KEY", fe)
    secret = _env("MINIO_SECRET_KEY", fe)
    region = _env("MINIO_REGION", fe) or "us-east-1"
    bucket = _env("MINIO_BUCKET", fe)
    verify_str = _env("MINIO_VERIFY_TLS", fe) or "true"
    verify = (verify_str.lower() not in ("0", "false", "no"))

    if not (endpoint and access and secret and bucket):
        raise HTTPException(503, detail="MinIO env not configured (need MINIO_ENDPOINT_URL/ACCESS_KEY/SECRET_KEY/BUCKET)")

    # endpoint に scheme が無ければ http:// を補う
    if not endpoint.startswith(("http://", "https://")):
        endpoint = "http://" + endpoint

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=region,
        verify=verify,
        config=Config(signature_version="s3v4", retries={"max_attempts": 2}),
    )
    return client, bucket


def _check_key(key: str) -> None:
    if not key or ".." in key or key.startswith("/"):
        raise HTTPException(400, detail=f"invalid key: {key!r}")
    if not any(key.startswith(p) for p in _ALLOWED_PREFIXES):
        raise HTTPException(403, detail=f"prefix not allowed; allowed={_ALLOWED_PREFIXES}")


@router.get("/{key:path}")
def get_object(key: str) -> StreamingResponse:
    _check_key(key)
    client, bucket = _client_and_bucket()
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        # NoSuchKey や AccessDenied は 404 として返す (= UI 側の onerror で済む)
        msg = str(e)[:200]
        if "NoSuchKey" in msg or "404" in msg:
            raise HTTPException(404, detail=f"not found: {key}") from e
        log.warning("minio get failed key=%s: %s", key, msg)
        raise HTTPException(502, detail=f"minio error: {msg}") from e

    body = obj["Body"]
    ctype = obj.get("ContentType") or "application/octet-stream"

    def _iter() -> Iterator[bytes]:
        try:
            for chunk in body.iter_chunks(chunk_size=64 * 1024):
                yield chunk
        finally:
            try:
                body.close()
            except Exception:
                pass

    headers = {"Cache-Control": "public, max-age=300"}
    return StreamingResponse(_iter(), media_type=ctype, headers=headers)
