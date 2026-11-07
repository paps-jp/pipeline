"""/api/v1/config — 宣言的構成管理 (export / plan / apply)。

- GET  /export … 今の DB を pipeline.config.yaml 形式の YAML で返す
- POST /plan   … YAML と DB の差分を返す (DB は変えない)
- POST /apply  … plan を実行する (prune=true のときだけ「ファイルに無いもの」を消す)
"""

from __future__ import annotations

import threading
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from pipeline.declarative.engine import (
    ConfigError,
    apply_config,
    dump_yaml,
    export_config,
    parse_yaml,
    plan_config,
)

router = APIRouter(prefix="/api/v1/config", tags=["config"])

# apply は 1 本ずつ (同時に 2 つ走ると、 同じ行を互いに上書きしうる)
_apply_lock = threading.Lock()


class ConfigBody(BaseModel):
    yaml: str
    prune: bool = False


def _parse(body: ConfigBody):
    try:
        return parse_yaml(body.yaml)
    except ConfigError as e:
        raise HTTPException(422, detail={"errors": e.messages}) from e


@router.get("/export", response_class=PlainTextResponse)
def export(request: Request) -> str:
    return dump_yaml(export_config(request.app.state.db))


@router.post("/plan")
def plan(body: ConfigBody, request: Request) -> dict[str, Any]:
    cfg = _parse(body)
    try:
        return plan_config(request.app.state.db, cfg, prune=body.prune).to_dict()
    except ConfigError as e:
        raise HTTPException(422, detail={"errors": e.messages}) from e


@router.post("/apply")
def apply(body: ConfigBody, request: Request) -> dict[str, Any]:
    cfg = _parse(body)
    if not _apply_lock.acquire(blocking=False):
        raise HTTPException(409, detail={"errors": ["別の apply が実行中です"]})
    try:
        result = apply_config(request.app.state.db, cfg, prune=body.prune, by="declarative")
    except ConfigError as e:
        raise HTTPException(422, detail={"errors": e.messages}) from e
    finally:
        _apply_lock.release()
    if not result.ok:
        # 途中で失敗: 適用済み/未適用は changes に残っている
        raise HTTPException(500, detail=result.to_dict())
    return result.to_dict()
