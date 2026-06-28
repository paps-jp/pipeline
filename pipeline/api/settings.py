"""/api/v1/settings — システム設定(LLM 接続情報等) の CRUD。

- GET    /api/v1/settings           → 全 settings (secret は mask)
- PATCH  /api/v1/settings/{key}     → 単一 key の value 更新
- POST   /api/v1/settings/llm/test  → LLM endpoint への疎通テスト
- GET    /api/v1/llm_calls          → LLM 呼び出し履歴 (直近 N 件)
- GET    /api/v1/llm_calls/{id}     → 1 件詳細
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from pipeline.repositories.llm_calls import LlmCallsRepository
from pipeline.repositories.settings import LLM_DEFAULTS, SettingsRepository


router = APIRouter(prefix="/api/v1", tags=["settings"])


def _srepo(request: Request) -> SettingsRepository:
    return SettingsRepository(request.app.state.db)


def _lrepo(request: Request) -> LlmCallsRepository:
    return LlmCallsRepository(request.app.state.db)


class SetValueRequest(BaseModel):
    value: str | None = None
    updated_by: str | None = None


class SettingItem(BaseModel):
    key: str
    value: str | None = None       # is_secret=1 のときは None
    value_masked: str | None = None
    description: str | None = None
    is_secret: int = 0
    updated_at: str | None = None
    updated_by: str | None = None


class SettingsResponse(BaseModel):
    settings: list[SettingItem]


@router.get("/settings", response_model=SettingsResponse)
def list_settings(request: Request) -> SettingsResponse:
    repo = _srepo(request)
    # 初回アクセス時に LLM_DEFAULTS を seed (= 空 DB を救う)
    repo.ensure_seed()
    items = [SettingItem(**r) for r in repo.list_all_masked()]
    return SettingsResponse(settings=items)


@router.patch("/settings/{key}", response_model=SettingItem)
def set_setting(key: str, body: SetValueRequest, request: Request) -> SettingItem:
    if key not in LLM_DEFAULTS:
        # 任意 key 追加は安全性のため不可
        raise HTTPException(400, detail=f"unknown setting key: {key}")
    try:
        row = _srepo(request).set_value(key, body.value, body.updated_by)
    except KeyError as e:
        raise HTTPException(404, detail=str(e)) from e
    return SettingItem(**row)


class LlmTestRequest(BaseModel):
    # 任意の上書き (= 設定保存前に試したい場合)。 未指定なら DB から取得。
    endpoint: str | None = None
    api_key: str | None = None
    model: str | None = None
    timeout_s: int = 30


class LlmTestResponse(BaseModel):
    ok: bool
    status_code: int | None = None
    latency_ms: int
    model: str
    response_excerpt: str | None = None
    error: str | None = None


@router.post("/settings/llm/test", response_model=LlmTestResponse)
def test_llm(body: LlmTestRequest, request: Request) -> LlmTestResponse:
    """提供された(or 保存済みの) LLM 設定で 1 回 chat completion を投げて成否確認。
    `body.api_key` は明示指定時のみ使う (= 未指定 = DB の生値を使う)。
    """
    srepo = _srepo(request)
    cfg = srepo.get_llm_config()
    endpoint = body.endpoint or cfg.get("endpoint") or ""
    api_key = body.api_key or cfg.get("api_key") or ""
    model = body.model or cfg.get("model") or ""
    if not endpoint or not api_key or not model:
        return LlmTestResponse(
            ok=False, latency_ms=0, model=model,
            error="missing endpoint / api_key / model",
        )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a connectivity test. Reply with exactly: OK"},
            {"role": "user", "content": "ping"},
        ],
        "max_tokens": 10,
        "temperature": 0,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    t0 = time.monotonic()
    try:
        with httpx.Client(timeout=int(body.timeout_s)) as client:
            r = client.post(endpoint, json=payload, headers=headers)
            latency_ms = int((time.monotonic() - t0) * 1000)
            if r.status_code >= 400:
                return LlmTestResponse(
                    ok=False, status_code=r.status_code, latency_ms=latency_ms,
                    model=model, error=r.text[:500],
                )
            data = r.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return LlmTestResponse(
                ok=True, status_code=r.status_code, latency_ms=latency_ms,
                model=model, response_excerpt=str(text)[:200],
            )
    except Exception as e:
        return LlmTestResponse(
            ok=False, latency_ms=int((time.monotonic() - t0) * 1000),
            model=model, error=str(e)[:500],
        )


# ---------------- LLM call audit log ----------------

# ---------------- 内部 API: supervisor 専用、 secret 生値を返す ----------------
# control plane と同一ホスト + 同一 SQLite を共有する supervisor (= @5 worker daemon
# が動くホスト)が呼ぶことを想定。 認証が無いので 公開 nginx には出さないこと
# (= prefix /api/v1/_internal/ で nginx 側で reject する想定)。

@router.get("/_internal/llm_config", response_model=dict[str, Any])
def _internal_llm_config(request: Request) -> dict[str, Any]:
    """supervisor が LLM 呼び出し用に生 api_key 込みで取得する。"""
    cfg = _srepo(request).get_llm_config()
    return cfg


class LlmCallRecordRequest(BaseModel):
    provider: str
    model: str
    prompt_messages: list[dict[str, Any]]
    response_text: str | None = None
    analysis: str | None = None
    actions: list[dict[str, Any]] = []
    actions_applied: int = 0
    success: bool
    error: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    duration_ms: int = 0


@router.post("/_internal/llm_call_record", response_model=dict[str, Any])
def _internal_llm_call_record(body: LlmCallRecordRequest, request: Request) -> dict[str, Any]:
    """supervisor が 1 コール終了後にここに POST して監査記録を残す。"""
    rid = _lrepo(request).record(
        provider=body.provider, model=body.model,
        prompt_messages=body.prompt_messages,
        response_text=body.response_text, analysis=body.analysis,
        actions=body.actions, actions_applied=body.actions_applied,
        success=body.success, error=body.error,
        prompt_tokens=body.prompt_tokens, completion_tokens=body.completion_tokens,
        total_tokens=body.total_tokens, duration_ms=body.duration_ms,
    )
    return {"id": rid}


class LlmCallApplyPatch(BaseModel):
    actions_applied: int


@router.patch("/_internal/llm_call_record/{call_id}", response_model=dict[str, Any])
def _internal_llm_call_patch(call_id: int, body: LlmCallApplyPatch,
                              request: Request) -> dict[str, Any]:
    db = request.app.state.db
    with db.transaction() as conn:
        conn.execute(
            "UPDATE llm_calls SET actions_applied = :n WHERE id = :id",
            {"n": int(body.actions_applied), "id": int(call_id)},
        )
    return {"ok": True}


@router.get("/llm_calls", response_model=dict[str, Any])
def list_llm_calls(request: Request, limit: int = 50) -> dict[str, Any]:
    items = _lrepo(request).list_recent(limit=limit)
    return {"calls": items, "total": len(items)}


@router.get("/llm_calls/{call_id}", response_model=dict[str, Any] | None)
def get_llm_call(call_id: int, request: Request) -> dict[str, Any]:
    item = _lrepo(request).get(call_id)
    if not item:
        raise HTTPException(404, detail="llm call not found")
    return item
