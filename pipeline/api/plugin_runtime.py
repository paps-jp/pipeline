"""/api/v1/plugins/{slug}/ — プラグインのライブ UI 用一時ストア + 静的配信.

3 つの汎用機能を提供:

1. **state**: プラグインが任意の JSON state を key で保存・取得 (動画進捗・最新顔ID等)
2. **blob**: バイナリ (画像 JPEG/PNG 等) を key で保存・取得 (スクショ等)
3. **web**: plugins/<slug>/web/ 以下を静的配信 (panel.html, .js, .css)

state/blob は control plane の SQLite に保存され、reaper で TTL 経過分を delete。
プラグインは worker から HTTP POST で書き込み、 UI iframe が GET でポーリングする。
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel

log = logging.getLogger("pipeline.api.plugin_runtime")
router = APIRouter(prefix="/api/v1/plugins", tags=["plugin_runtime"])

# key は英数 / _ / - / . / / のみ許可 (パストラバーサル防止)
_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
_SAFE_SLUG_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _check_slug(slug: str) -> None:
    if not _SAFE_SLUG_RE.match(slug):
        raise HTTPException(400, detail=f"invalid slug: {slug!r}")


def _check_key(key: str) -> None:
    if not _SAFE_KEY_RE.match(key) or ".." in key:
        raise HTTPException(400, detail=f"invalid key: {key!r}")


def _plugin_root() -> Path:
    return Path(os.environ.get("PIPELINE_PLUGIN_ROOT", "/opt/pipeline/plugins"))


def _resolve_plugin_dir(slug: str) -> Path | None:
    """slug → 物理ディレクトリ。 ハイフンとアンダースコアを相互に解決する。"""
    root = _plugin_root()
    for cand in (slug, slug.replace("-", "_"), slug.replace("_", "-")):
        d = root / cand
        if d.is_dir():
            return d
    return None


def _canonical_slug(slug: str) -> str:
    """state/blob テーブル上の slug は アンダースコア版に統一 (manifest name と分離)。"""
    d = _resolve_plugin_dir(slug)
    return d.name if d else slug


# ---------------- state (JSON) ----------------


class StateUpsert(BaseModel):
    value: Any


@router.put("/{slug}/state/{key:path}", status_code=204)
def upsert_state(slug: str, key: str, body: StateUpsert, request: Request) -> Response:
    """プラグインが任意の JSON 値を key で保存。 上書き。"""
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    payload = json.dumps(body.value, ensure_ascii=False)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO plugin_runtime_state (slug, key, value_json, updated_at)
            VALUES (:s, :k, :v, :u)
            ON CONFLICT(slug, key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at = excluded.updated_at
            """,
            {"s": slug, "k": key, "v": payload,
             "u": datetime.now(timezone.utc).isoformat()},
        )
    return Response(status_code=204)


@router.get("/{slug}/state/{key:path}")
def get_state(slug: str, key: str, request: Request) -> dict[str, Any]:
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT value_json, updated_at FROM plugin_runtime_state "
            "WHERE slug = :s AND key = :k",
            {"s": slug, "k": key},
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, detail="not found")
    return {"value": json.loads(row["value_json"]), "updated_at": row["updated_at"]}


@router.get("/{slug}/state")
def list_state(slug: str, request: Request) -> dict[str, Any]:
    """slug 配下の全 state を { key: {value, updated_at} } で返す。"""
    _check_slug(slug)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT key, value_json, updated_at FROM plugin_runtime_state "
            "WHERE slug = :s",
            {"s": slug},
        )
        rows = cur.fetchall()
    return {
        "items": {
            r["key"]: {
                "value": json.loads(r["value_json"]),
                "updated_at": r["updated_at"],
            }
            for r in rows
        }
    }


@router.delete("/{slug}/state/{key:path}", status_code=204)
def delete_state(slug: str, key: str, request: Request) -> Response:
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    with db.transaction() as conn:
        conn.execute(
            "DELETE FROM plugin_runtime_state WHERE slug = :s AND key = :k",
            {"s": slug, "k": key},
        )
    return Response(status_code=204)


# ---------------- blob (binary) ----------------

MAX_BLOB_BYTES = 5 * 1024 * 1024  # 5 MB / blob


@router.put("/{slug}/blob/{key:path}", status_code=204)
async def upsert_blob(slug: str, key: str, request: Request) -> Response:
    """raw binary body を key で保存。 Content-Type も保存。"""
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    data = await request.body()
    if len(data) > MAX_BLOB_BYTES:
        raise HTTPException(413, detail=f"too large (max {MAX_BLOB_BYTES} bytes)")
    ct = request.headers.get("content-type", "application/octet-stream")
    db = request.app.state.db
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO plugin_runtime_blob (slug, key, content_type, data, size_bytes, updated_at)
            VALUES (:s, :k, :ct, :d, :sz, :u)
            ON CONFLICT(slug, key) DO UPDATE SET
                content_type = excluded.content_type,
                data         = excluded.data,
                size_bytes   = excluded.size_bytes,
                updated_at   = excluded.updated_at
            """,
            {"s": slug, "k": key, "ct": ct, "d": data,
             "sz": len(data),
             "u": datetime.now(timezone.utc).isoformat()},
        )
    return Response(status_code=204)


@router.get("/{slug}/blob/{key:path}")
def get_blob(slug: str, key: str, request: Request) -> Response:
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT content_type, data, updated_at FROM plugin_runtime_blob "
            "WHERE slug = :s AND key = :k",
            {"s": slug, "k": key},
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, detail="not found")
    return Response(
        content=row["data"],
        media_type=row["content_type"],
        headers={
            "Cache-Control": "no-store",
            "X-Updated-At": row["updated_at"],
        },
    )


@router.delete("/{slug}/blob/{key:path}", status_code=204)
def delete_blob(slug: str, key: str, request: Request) -> Response:
    _check_slug(slug)
    _check_key(key)
    slug = _canonical_slug(slug)
    db = request.app.state.db
    with db.transaction() as conn:
        conn.execute(
            "DELETE FROM plugin_runtime_blob WHERE slug = :s AND key = :k",
            {"s": slug, "k": key},
        )
    return Response(status_code=204)


# ---------------- web (静的配信) ----------------


_DEFAULT_PANEL_HTML = Path(__file__).parent / "_default_plugin_panel.html"


@router.get("/{slug}/web/{path:path}")
def serve_plugin_web(slug: str, path: str) -> FileResponse:
    """plugins/<slug>/web/<path> を静的配信。

    panel.html が plugin に無い場合は system 汎用 panel をフォールバック配信する
    (= plugin.yaml の `ui_panel: true` だけ宣言すれば共通 UI で可視化できる)。
    """
    _check_slug(slug)
    if not path:
        path = "panel.html"
    if ".." in path or path.startswith("/"):
        raise HTTPException(400, detail="invalid path")
    pdir = _resolve_plugin_dir(slug)
    if pdir is None:
        raise HTTPException(404, detail=f"plugin not found: {slug}")
    root = pdir / "web"
    file_path = (root / path).resolve()
    try:
        file_path.relative_to(root.resolve())
    except ValueError:
        raise HTTPException(400, detail="path escapes plugin web/")
    # panel.html (= 開発中によく差し替わる) は no-cache 必須。 ブラウザが iframe を
    # 強キャッシュすると、 ホスト側ハードリロードでも古い HTML が居座る事故が起きる。
    no_cache_headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }
    if not file_path.exists() or not file_path.is_file():
        # panel.html の場合だけ system 汎用 panel をフォールバック配信
        if path == "panel.html" and _DEFAULT_PANEL_HTML.exists():
            return FileResponse(_DEFAULT_PANEL_HTML, media_type="text/html",
                                headers=no_cache_headers)
        raise HTTPException(404, detail=f"not found: {path}")
    headers = no_cache_headers if path.endswith(".html") else None
    return FileResponse(file_path, headers=headers)


# ---------------- TTL reaper ----------------


def reap_old_runtime_data(db, *, state_ttl_hours: int = 24,
                          blob_ttl_minutes: int = 30) -> dict[str, int]:
    """古い runtime データを削除。 state=24h, blob=30min がデフォルト。
    blob は画像で容量を食うので短め。"""
    now = datetime.now(timezone.utc)
    state_cutoff = (now - timedelta(hours=state_ttl_hours)).isoformat()
    blob_cutoff = (now - timedelta(minutes=blob_ttl_minutes)).isoformat()
    deleted = {"state": 0, "blob": 0}
    with db.transaction() as conn:
        c1 = conn.execute(
            "DELETE FROM plugin_runtime_state WHERE updated_at < :cut",
            {"cut": state_cutoff},
        )
        deleted["state"] = c1.rowcount or 0
        c2 = conn.execute(
            "DELETE FROM plugin_runtime_blob WHERE updated_at < :cut",
            {"cut": blob_cutoff},
        )
        deleted["blob"] = c2.rowcount or 0
    return deleted
