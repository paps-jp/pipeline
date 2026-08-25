"""/api/v1/mariadb-tables — 外部 MariaDB (crawl_config 等) の汎用テーブル admin。

`host_policy` 等の管理 API は制御プレーン自身の SQLite しか触らないが、
crawl_config は外部の本番 MariaDB (.20) 上にある。 接続は
`pipeline.api_public.create.get_backend()` と同じ lazy 生成パターン
(app.state に無ければ env から 1 度だけ作る。 MariaDB 未設定の環境でも
control plane 自体は起動できるようにするため)。

対象テーブル・列は `pipeline.db.mariadb_admin.TABLE_REGISTRY` に宣言したものだけ。
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from pipeline.db.mariadb_admin import (
    TABLE_REGISTRY,
    ColumnNotEditableError,
    TableNotFoundError,
    get_spec,
    list_rows,
    update_row,
)

log = logging.getLogger("pipeline.api.mariadb_tables")

router = APIRouter(prefix="/api/v1/mariadb-tables", tags=["mariadb-tables"])

_ENV_FILE = os.environ.get("PIPELINE_ENV_FILE", "/etc/pipeline/.env")


class _Backend:
    def __init__(self, db: Any) -> None:
        self.db = db
        self.lock = threading.Lock()


def get_backend(request: Request) -> _Backend:
    """app.state に無ければ env から 1 度だけ作って載せる (create.get_backend と同一方針)。"""
    be = getattr(request.app.state, "mariadb_admin_backend", None)
    if be is not None:
        return be
    try:
        from pipeline.storage import StorageRegistry
        reg = StorageRegistry.from_env_file(_ENV_FILE)
        be = _Backend(reg.db())
    except Exception as e:  # noqa: BLE001
        log.warning("mariadb-tables backend の初期化に失敗: %s", e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"mariadb-tables backend 未設定 ({type(e).__name__})",
        ) from e
    request.app.state.mariadb_admin_backend = be
    return be


# ---------------- スキーマ ----------------

class ColumnView(BaseModel):
    name: str
    editable: bool
    kind: str


class TableMeta(BaseModel):
    name: str
    label: str
    pk: str
    columns: list[ColumnView]
    searchable: list[str]


class TableListResponse(BaseModel):
    tables: list[TableMeta]


class RowsResponse(BaseModel):
    rows: list[dict[str, Any]]
    total: int


def _table_meta(name: str) -> TableMeta:
    spec = TABLE_REGISTRY[name]
    return TableMeta(
        name=spec.name, label=spec.label, pk=spec.pk,
        columns=[ColumnView(name=c.name, editable=c.editable, kind=c.kind)
                for c in spec.columns],
        searchable=list(spec.searchable),
    )


# ---------------- エンドポイント ----------------

@router.get("/tables", response_model=TableListResponse)
def list_tables() -> TableListResponse:
    return TableListResponse(tables=[_table_meta(n) for n in TABLE_REGISTRY])


@router.get("/tables/{table}/rows", response_model=RowsResponse)
def get_rows(table: str, q: str = "", enabled: int | None = None,
             limit: int = 50, offset: int = 0,
             backend: _Backend = Depends(get_backend)) -> RowsResponse:
    try:
        spec = get_spec(table)
    except TableNotFoundError:
        raise HTTPException(status_code=404, detail=f"未登録のテーブル: {table}") from None
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    rows, total = list_rows(backend.db, backend.lock, spec,
                             q=q, enabled=enabled, limit=limit, offset=offset)
    return RowsResponse(rows=rows, total=total)


@router.patch("/tables/{table}/rows/{pk}")
def patch_row(table: str, pk: int, payload: dict[str, Any],
              backend: _Backend = Depends(get_backend)) -> dict[str, Any]:
    try:
        spec = get_spec(table)
    except TableNotFoundError:
        raise HTTPException(status_code=404, detail=f"未登録のテーブル: {table}") from None
    try:
        return update_row(backend.db, backend.lock, spec, pk, payload)
    except ColumnNotEditableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
