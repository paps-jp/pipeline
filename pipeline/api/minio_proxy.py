"""/api/v1/minio/{key} — control plane が MinIO 上のオブジェクトをプロキシ配信.

ユースケース: プラグイン UI から MinIO 上の顔サムネ等を `<img src>` で
直接見せたい。 MinIO は anonymous read を許してないので、 制御プレーンが env の
クレデンシャルで取って stream する。

セキュリティ:
- LAN 信頼の社内環境を前提。 外部公開時は AuthZ 追加が必要。
- 安全のため allowed-prefix を環境変数で固定 (= 既定 `crawl_face/`)。
  prefix に該当しないキーは 403。 **ワイルドカードは受け付けない**
  (_parse_prefixes 参照)。
- bucket は env `MINIO_BUCKET` (= `crawl`) で固定。
- Content-Type は **保存値ではなく拡張子からサーバ側で決める**。 併せて
  nosniff / CSP / X-Frame-Options を付ける。 ここを緩めると、 取り込み側が
  JPEG 再エンコードをやめた瞬間に stored XSS の経路になる。

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

# 配信してよい Content-Type。 **保存側の値は信用しない**。
#
# 2026-09-02: ここは以前 `obj["ContentType"]` (= オブジェクトに保存された値) を
# そのまま media_type にしていた。 保存された画像が信用できる (= 取り込み時に
# JPEG へ再エンコードしている) 間は問題にならないが、 その前提が崩れると
# 「攻撃者が Content-Type を左右できる同一オリジン配信」 になり stored XSS の
# 経路になる。 拡張子から **サーバ側で決め打ち**し、 未知の拡張子は
# octet-stream + attachment に落とす。
_SAFE_CONTENT_TYPES = {
    "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
    "gif": "image/gif", "webp": "image/webp", "avif": "image/avif",
}


def _parse_prefixes(raw: str) -> tuple[str, ...]:
    """許可プレフィックスを読む。 **ワイルドカードは受け付けない**。

    2026-09-02: 本番の env が `PIPELINE_MINIO_PROXY_PREFIXES=*` になっていた。
    判定は `key.startswith(p)` の literal 比較なので `"*"` は何にも前方一致せず、
    **「全許可のつもりで全拒否」** になっていた (実測: 全キー 403)。

    黙って全許可に読み替えるのは危険なので (= 認証無しで bucket 全体が
    読めるようになる) 、 ここでは **落として ERROR で鳴らす**。 意図が
    「全部見せたい」ならプレフィックスを列挙するか、 AuthZ を足してから開ける。
    """
    out: list[str] = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        if "*" in p or "?" in p:
            log.error(
                "[minio-proxy] PIPELINE_MINIO_PROXY_PREFIXES にワイルドカード %r が"
                " 指定されているが未対応 (前方一致のみ)。 このエントリは無視する。"
                " 全公開したいならプレフィックスを列挙すること", p,
            )
            continue
        out.append(p)
    if not out:
        log.error("[minio-proxy] 許可プレフィックスが 1 件も無い → 全キーを 403 で拒否する")
    return tuple(out)


# allowed prefix: カンマ区切り。 既定は crawl_face/ (= 顔サムネのみ)
_ALLOWED_PREFIXES = _parse_prefixes(
    os.environ.get("PIPELINE_MINIO_PROXY_PREFIXES", "crawl_face/")
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
    systemd unit に MINIO_* が無いケース用に PIPELINE_ENV_FILE (既定 /etc/pipeline/.env)
    からの fallback 読み込みもする。"""
    try:
        import boto3  # type: ignore
        from botocore.config import Config  # type: ignore
    except ImportError as e:
        raise HTTPException(503, detail=f"boto3 not installed: {e}") from e

    env_file = os.environ.get(
        "PIPELINE_MINIO_ENV_FILE",
        os.environ.get("PIPELINE_ENV_FILE", "/etc/pipeline/.env"))
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
    # Content-Type は **保存値を使わず** 拡張子からサーバ側で決める
    # (_SAFE_CONTENT_TYPES 参照)。 未知の拡張子は octet-stream + attachment。
    ext = key.rsplit(".", 1)[-1].lower() if "." in key.rsplit("/", 1)[-1] else ""
    ctype = _SAFE_CONTENT_TYPES.get(ext)

    def _iter() -> Iterator[bytes]:
        try:
            for chunk in body.iter_chunks(chunk_size=64 * 1024):
                yield chunk
        finally:
            try:
                body.close()
            except Exception:
                pass

    headers = {
        "Cache-Control": "public, max-age=300",
        # ブラウザに Content-Type を推測させない。 これが無いと、 画像のはずの
        # バイト列が HTML と判定されて同一オリジンで実行されうる。
        "X-Content-Type-Options": "nosniff",
        # 万一 HTML として解釈されてもスクリプトも参照も走らせない。
        "Content-Security-Policy": "default-src 'none'; sandbox",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "no-referrer",
    }
    if ctype is None:
        # 画像として名乗れないものは、 表示させずダウンロード扱いにする。
        ctype = "application/octet-stream"
        headers["Content-Disposition"] = "attachment"
    return StreamingResponse(_iter(), media_type=ctype, headers=headers)
