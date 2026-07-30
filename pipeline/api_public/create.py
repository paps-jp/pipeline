"""/api/v1/create — 外部から画像 / 動画を既存パイプラインへ投入する API。

投入されたものは Paprika クローラー由来と**完全に同じ経路**を通る:
  画像: crawl_image → image-hash-extract → crawl_face → image-embed → embed-write
  動画: crawl_video → video-face-extract → crawl_image (顔 crop) → 上と同じ

入力は「multipart のファイル」または「JSON の URL」。 どちらも受け付ける。
実際の投入契約と一意制約の扱いは [[pipeline.api_public.backend]] のドキストリング参照。
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any, Literal

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from pydantic import BaseModel, Field

from pipeline.api_public import fetch as _fetch
from pipeline.api_public.auth import consume_quota, quota_used, require_api_key
from pipeline.api_public.backend import CreateBackend, CreateError, site_for

log = logging.getLogger("pipeline.api_public.create")

router = APIRouter(prefix="/api/v1/create", tags=["create"])

MAX_IMAGE_BYTES = int(os.environ.get("PIPELINE_CREATE_MAX_IMAGE_BYTES", str(32 * 1024 * 1024)))
MAX_VIDEO_BYTES = int(os.environ.get("PIPELINE_CREATE_MAX_VIDEO_BYTES", str(2 * 1024 * 1024 * 1024)))
MAX_URLS_PER_REQUEST = int(os.environ.get("PIPELINE_CREATE_MAX_URLS", "100"))

_ENV_FILE = os.environ.get("PIPELINE_ENV_FILE", "/etc/pipeline/.env")


# ---------------- backend の解決 (lazy + テスト差し替え可) ----------------


def get_backend(request: Request) -> CreateBackend:
    """app.state に無ければ env から 1 度だけ作って載せる。

    control plane 起動時に作らないのは、 MariaDB / MinIO が未設定の環境 (OSS の単体
    起動やテスト) でも server が上がるようにするため。 テストは
    `app.state.create_backend` に fake を差し込めば良い。
    """
    be = getattr(request.app.state, "create_backend", None)
    if be is not None:
        return be
    try:
        be = CreateBackend.from_env_file(_ENV_FILE)
    except Exception as e:  # noqa: BLE001
        log.warning("create backend の初期化に失敗: %s", e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"create backend 未設定 ({type(e).__name__})",
        ) from e
    request.app.state.create_backend = be
    return be


# ---------------- スキーマ ----------------


class UrlCreateRequest(BaseModel):
    url: str | None = Field(None, description="単一 URL")
    urls: list[str] | None = Field(None, description="複数 URL (最大 100)")
    page_url: str | None = Field(None, description="出所ページ URL (任意・動画のみ利用)")

    def targets(self) -> list[str]:
        out = [u.strip() for u in ([self.url] if self.url else []) + (self.urls or []) if u and u.strip()]
        if not out:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="url または urls が必要")
        if len(out) > MAX_URLS_PER_REQUEST:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"URL は 1 リクエスト {MAX_URLS_PER_REQUEST} 件まで")
        return out


class CreatedItem(BaseModel):
    ok: bool
    kind: Literal["image", "video"]
    id: int | None = None
    state: str | None = None
    dedup: bool = False
    source: str | None = None
    error: str | None = None
    detail: str | None = None


class CreateResponse(BaseModel):
    accepted: int
    dedup: int
    failed: int
    items: list[CreatedItem]


# ---------------- 共通処理 ----------------


def _one(be: CreateBackend, *, kind: str, data: bytes, url: str | None,
         site: str, ext: str | None, mime: str | None = None,
         page_url: str | None = None) -> CreatedItem:
    """1 件を投入して CreatedItem に落とす (例外は item の error に閉じ込める)。"""
    max_bytes = MAX_IMAGE_BYTES if kind == "image" else MAX_VIDEO_BYTES
    if not data:
        return CreatedItem(ok=False, kind=kind, source=url, error="empty",
                            detail="0 バイト")
    if len(data) > max_bytes:
        return CreatedItem(ok=False, kind=kind, source=url, error="too_large",
                            detail=f"{len(data)} > {max_bytes}")
    sniffed = _fetch.sniff_ext(data, kind=kind)
    if sniffed is None:
        return CreatedItem(ok=False, kind=kind, source=url, error="unsupported_format",
                            detail=f"{kind} として判別できない先頭バイト")
    try:
        if kind == "image":
            r = be.create_image(data=data, url=url, site=site, ext=sniffed)
        else:
            r = be.create_video(data=data, url=url, site=site, ext=sniffed,
                                mime=mime, page_url=page_url)
    except CreateError as e:
        return CreatedItem(ok=False, kind=kind, source=url, error=e.reason, detail=e.detail)
    except Exception as e:  # noqa: BLE001
        log.exception("create 失敗 kind=%s url=%s", kind, url)
        return CreatedItem(ok=False, kind=kind, source=url, error="internal",
                            detail=f"{type(e).__name__}: {e}"[:300])
    return CreatedItem(ok=True, kind=kind, id=r.id, state=r.state, dedup=r.dedup, source=url)


def _summarize(items: list[CreatedItem]) -> CreateResponse:
    return CreateResponse(
        accepted=sum(1 for i in items if i.ok and not i.dedup),
        dedup=sum(1 for i in items if i.ok and i.dedup),
        failed=sum(1 for i in items if not i.ok),
        items=items,
    )


def _create_urls(be: CreateBackend, urls: list[str], *, kind: str, site: str,
                 page_url: str | None) -> list[CreatedItem]:
    max_bytes = MAX_IMAGE_BYTES if kind == "image" else MAX_VIDEO_BYTES
    items: list[CreatedItem] = []
    for u in urls:
        try:
            got = _fetch.fetch(u, max_bytes=max_bytes)
        except _fetch.FetchError as e:
            items.append(CreatedItem(ok=False, kind=kind, source=u,
                                      error=e.reason, detail=e.detail))
            continue
        items.append(_one(be, kind=kind, data=got.data, url=got.final_url, site=site,
                          ext=None, mime=got.content_type, page_url=page_url))
    return items


# ---------------- 画像 ----------------


@router.post("/images", response_model=CreateResponse,
             status_code=status.HTTP_201_CREATED)
def create_image_file(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    file: Annotated[UploadFile, File(description="画像ファイル")],
    url: Annotated[str | None, Form(description="出所 URL (任意・dedup に使われる)")] = None,
) -> CreateResponse:
    """multipart で画像ファイルを 1 枚投入する。"""
    be = get_backend(request)
    consume_quota(principal["id"])
    site = site_for(principal["user_slug"])
    data = file.file.read(MAX_IMAGE_BYTES + 1)
    item = _one(be, kind="image", data=data, url=url, site=site, ext=None)
    return _summarize([item])


@router.post("/images/url", response_model=CreateResponse,
             status_code=status.HTTP_201_CREATED)
def create_image_urls(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    body: UrlCreateRequest,
) -> CreateResponse:
    """URL (単数 / 複数) から画像を取得して投入する。"""
    be = get_backend(request)
    urls = body.targets()
    consume_quota(principal["id"], len(urls))
    site = site_for(principal["user_slug"])
    return _summarize(_create_urls(be, urls, kind="image", site=site, page_url=None))


@router.get("/images/{image_id}")
def get_image_status(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    image_id: int,
) -> dict[str, Any]:
    """投入した画像の進行状況。 `faces[].embedded` が embedding 完了の判定。"""
    st = get_backend(request).image_status(image_id)
    if st is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="image_id 不明")
    return st


# ---------------- 動画 ----------------


@router.post("/videos", response_model=CreateResponse,
             status_code=status.HTTP_201_CREATED)
def create_video_file(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    file: Annotated[UploadFile, File(description="動画ファイル")],
    url: Annotated[str | None, Form(description="出所 URL (任意・dedup に使われる)")] = None,
    page_url: Annotated[str | None, Form(description="出所ページ URL (任意)")] = None,
) -> CreateResponse:
    """multipart で動画ファイルを 1 本投入する。

    大きい動画をここに流すと control plane のプロセスがバッファを持つので、 数百 MB を
    超えるものは URL 投入 (または将来の presigned PUT) を使うこと。
    """
    be = get_backend(request)
    consume_quota(principal["id"])
    site = site_for(principal["user_slug"])
    data = file.file.read(MAX_VIDEO_BYTES + 1)
    item = _one(be, kind="video", data=data, url=url, site=site, ext=None,
                mime=file.content_type, page_url=page_url)
    return _summarize([item])


@router.post("/videos/url", response_model=CreateResponse,
             status_code=status.HTTP_201_CREATED)
def create_video_urls(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    body: UrlCreateRequest,
) -> CreateResponse:
    """URL (単数 / 複数) から動画を取得して投入する。"""
    be = get_backend(request)
    urls = body.targets()
    consume_quota(principal["id"], len(urls))
    site = site_for(principal["user_slug"])
    return _summarize(_create_urls(be, urls, kind="video", site=site,
                                   page_url=body.page_url))


@router.get("/videos/{video_id}")
def get_video_status(
    request: Request,
    principal: Annotated[dict, Depends(require_api_key)],
    video_id: int,
) -> dict[str, Any]:
    st = get_backend(request).video_status(video_id)
    if st is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="video_id 不明")
    return st


# ---------------- 自己申告 ----------------


@router.get("/limits")
def get_limits(
    principal: Annotated[dict, Depends(require_api_key)],
) -> dict[str, Any]:
    """呼び出し側が事前に確認できる上限と残クォータ。"""
    from pipeline.api_public.auth import DAILY_LIMIT

    return {
        "site": site_for(principal["user_slug"]),
        "max_image_bytes": MAX_IMAGE_BYTES,
        "max_video_bytes": MAX_VIDEO_BYTES,
        "max_urls_per_request": MAX_URLS_PER_REQUEST,
        "daily_limit": DAILY_LIMIT,
        "daily_used": quota_used(principal["id"]),
        "image_formats": ["jpg", "png", "gif", "bmp", "webp"],
        "video_formats": ["mp4", "mov", "webm", "flv"],
    }
