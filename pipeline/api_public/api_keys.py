"""/api/v1/api-keys — 外部投入 API 用のキー発行 / 一覧 / 失効 (管理面)。

このルータ自身は control plane の他の管理 API と同じ「LAN 信頼」の扱い (= 無認証)。
外部に出すのは `/api/v1/create/*` だけで、 nginx 側でこのパスは通さないこと。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from pipeline.repositories.api_keys import ApiKeyRepository

router = APIRouter(prefix="/api/v1/api-keys", tags=["api_keys"])


class CreateKeyRequest(BaseModel):
    user_slug: str = Field(..., min_length=1, max_length=64,
                           description="所有者。 投入 site 名 (create-<slug>) になる")
    name: str = Field(..., min_length=1, max_length=128, description="用途メモ")
    expires_at: str | None = Field(None, description="ISO8601。 未指定 = 無期限")


class CreateKeyResponse(BaseModel):
    api_key: str = Field(..., description="平文キー。 この応答でしか取得できない")
    id: str
    user_slug: str
    name: str
    create_site: str


@router.post("", response_model=CreateKeyResponse, status_code=status.HTTP_201_CREATED)
def create_key(body: CreateKeyRequest, request: Request) -> CreateKeyResponse:
    from pipeline.api_public.backend import site_for

    repo = ApiKeyRepository(request.app.state.db)
    raw, meta = repo.create(user_slug=body.user_slug, name=body.name,
                            expires_at=body.expires_at)
    return CreateKeyResponse(api_key=raw, id=meta["id"], user_slug=meta["user_slug"],
                             name=meta["name"], create_site=site_for(body.user_slug))


@router.get("")
def list_keys(request: Request) -> dict:
    return {"keys": ApiKeyRepository(request.app.state.db).list()}


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_key(key_id: str, request: Request) -> None:
    if not ApiKeyRepository(request.app.state.db).revoke(key_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="key_id 不明")
