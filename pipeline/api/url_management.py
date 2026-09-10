"""/api/v1/url-management — シンプルな URL 追加 UI とサイト状況一覧。

宛先は既存の crawl_config だが、 admin 画面 (`/api/v1/mariadb-tables`) は
必須列が多く「新しいサイトを追加したい」だけの operator には過剰。 本 router
は URL 1 本を受け取って以下を自動で埋め、 crawl_config へ 1 行 INSERT する:

    site   = URL のホスト (www. と port を落とし、 sub.example.com は
             sub.example.com のまま保持)
    domain = 同上
    type   = "image" 固定
    enabled = 50 (priority tier — 新規サイトは submit filler が先頭注入する)

登録直後に crawl 表へ種を撒く動作は create_row (= 既存 admin と同経路) に任せる。

サイト一覧では `enabled >= 1` の crawl_config 行を返し、 各サイトについて
「今キューに載っている URL 数」(= paprika_job_q からの集計。 大きくても
数千行なので高速) と priority tier を添える。 crawl 側の詳細 (未投入残高
や直近投入数) は index が薄く 130M 行の全走査になりうるので、 一覧では
出さない (必要なら reseed ボタンや個別ページで拾う)。
"""

from __future__ import annotations

import logging
import urllib.parse
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from pipeline.api.mariadb_tables import _Backend, get_backend
from pipeline.db.mariadb_admin import (
    DuplicateRowError,
    MissingRequiredFieldError,
    create_row,
    get_spec,
)

log = logging.getLogger("pipeline.api.url_management")

router = APIRouter(prefix="/api/v1/url-management", tags=["url-management"])


# ---------------- スキーマ ----------------

class RegisterRequest(BaseModel):
    url: str = Field(..., min_length=4, description="https:// 付きの登録 URL")
    enabled: int | None = Field(
        None,
        description="省略時は 50 (priority tier)。 通常投入で良ければ 1、"
                    "明示的に高優先度にするなら 90 等",
    )
    memo: str | None = None


class RegisterResponse(BaseModel):
    id: int
    site: str
    url: str
    domain: str
    enabled: int


class SiteRow(BaseModel):
    id: int
    site: str
    url: str
    domain: str | None
    enabled: int          # >1 = priority tier
    memo: str | None
    created_at: str | None
    queued: int           # paprika_job_q に載っている本数 (未 claim + claim 済み合計)
    queued_unclaimed: int  # うち未 claim (= まだ POST されていない) 分
    is_priority: bool     # enabled > 1
    is_currently_crawling: bool  # queued > 0 (= submit ループが今このサイトを扱っている)


class SitesResponse(BaseModel):
    sites: list[SiteRow]
    total: int
    priority_count: int
    crawling_count: int


# ---------------- URL → host 正規化 ----------------

def _host_of(url: str) -> str:
    """URL からレート制限単位の host を返す (paprika_job_submit の _domain_of と同じ考え方)。

    - scheme が付いていなければ暗黙 https を仮定
    - www. を落とす
    - port を落とす
    - user@ を落とす
    - 空 / パースエラーなら空文字
    """
    if not url:
        return ""
    u = url.strip()
    if "://" not in u:
        u = "https://" + u
    try:
        host = urllib.parse.urlsplit(u).netloc.lower()
    except Exception:
        return ""
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    host = host.split(":", 1)[0].strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _canonical_url(url: str) -> str:
    """入力 URL を DB 保存用に軽く正規化する。 scheme が無ければ https を付け、
    末尾に / が無ければ足す (crawl_host_import と同じ最小限の整形)。"""
    u = (url or "").strip()
    if not u:
        return u
    if "://" not in u:
        u = "https://" + u
    parts = urllib.parse.urlsplit(u)
    if not parts.path:
        u = urllib.parse.urlunsplit((parts.scheme, parts.netloc, "/",
                                     parts.query, parts.fragment))
    return u


# ---------------- endpoint: 新規登録 ----------------

@router.post("/register", response_model=RegisterResponse, status_code=201)
def register(payload: RegisterRequest,
             backend: _Backend = Depends(get_backend)) -> RegisterResponse:
    """URL 1 本 → crawl_config に INSERT + 種撒き。 詳細はモジュール docstring 参照。"""
    host = _host_of(payload.url)
    if not host or "." not in host:
        raise HTTPException(status_code=400,
                            detail=f"URL からホスト名を取り出せません: {payload.url!r}")
    canon_url = _canonical_url(payload.url)
    enabled = payload.enabled if payload.enabled is not None else 50
    if enabled < 1:
        raise HTTPException(status_code=400,
                            detail="enabled は 1 以上を指定してください (< 1 は無効=登録済み扱い)")

    spec = get_spec("crawl_config")
    fields: dict[str, Any] = {
        "site": host,
        "url": canon_url,
        "domain": host,
        "enabled": enabled,
    }
    if payload.memo:
        fields["memo"] = payload.memo

    try:
        row = create_row(backend.db, backend.lock, spec, fields)
    except DuplicateRowError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except MissingRequiredFieldError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return RegisterResponse(
        id=int(row["id"]),
        site=str(row["site"]),
        url=str(row["url"]),
        domain=str(row["domain"] or ""),
        enabled=int(row["enabled"] or 0),
    )


# ---------------- endpoint: サイト一覧 (アクティビティ付き) ----------------

@router.get("/sites", response_model=SitesResponse)
def list_sites(q: str | None = None,
                only_active: bool = False,
                backend: _Backend = Depends(get_backend)) -> SitesResponse:
    """enabled >= 1 のサイト一覧 + paprika_job_q からのアクティビティ集計。

    - ``q`` : site/domain/memo/url にマッチする LIKE 検索
    - ``only_active`` : queued > 0 (= 今クロール中扱い) のサイトだけ返す
    """
    like = f"%{q}%" if q else None
    with backend.lock:
        backend.db.ping()

        params: list[Any] = []
        where = "WHERE enabled >= 1"
        if like:
            where += " AND (site LIKE %s OR domain LIKE %s OR memo LIKE %s OR url LIKE %s)"
            params.extend([like, like, like, like])

        cur = backend.db.execute(
            f"SELECT id, site, url, domain, enabled, memo, created_at "
            f"FROM crawl_config {where} "
            f"ORDER BY enabled DESC, id DESC",
            tuple(params),
        )
        try:
            cc_rows = cur.fetchall()
        finally:
            cur.close()

        # paprika_job_q は数千行の小テーブルなので 1 shot で全部集計する
        # (site ごとに個別 COUNT を打つより遥かに速い)。 未 claim (= まだ POST
        # されていない) と claim 済み (= 今 POST 中) を分けて出すと「詰まって
        # いるサイト」の区別が付く。
        qcur = backend.db.execute(
            "SELECT site, "
            "  SUM(claimed_at IS NULL) AS unclaimed, "
            "  COUNT(*) AS total "
            "FROM paprika_job_q GROUP BY site"
        )
        try:
            q_counts: dict[str, tuple[int, int]] = {
                str(r[0] or ""): (int(r[1] or 0), int(r[2] or 0))
                for r in qcur.fetchall()
            }
        finally:
            qcur.close()

    sites: list[SiteRow] = []
    priority_count = 0
    crawling_count = 0
    for r in cc_rows:
        cid, site, url, domain, enabled, memo, created_at = r
        unclaimed, total = q_counts.get(str(site or ""), (0, 0))
        is_priority = int(enabled or 0) > 1
        is_crawling = total > 0
        if is_priority:
            priority_count += 1
        if is_crawling:
            crawling_count += 1
        if only_active and not is_crawling:
            continue
        sites.append(SiteRow(
            id=int(cid),
            site=str(site or ""),
            url=str(url or ""),
            domain=str(domain) if domain else None,
            enabled=int(enabled or 0),
            memo=str(memo) if memo else None,
            created_at=str(created_at) if created_at else None,
            queued=total,
            queued_unclaimed=unclaimed,
            is_priority=is_priority,
            is_currently_crawling=is_crawling,
        ))

    # 表示順: クロール中を先頭 → priority tier → id DESC。 SQL では
    # 「enabled DESC, id DESC」で取っているので、 クロール中優先だけ
    # ここで stable sort として重ねる。
    sites.sort(key=lambda s: (0 if s.is_currently_crawling else 1))

    return SitesResponse(
        sites=sites,
        total=len(cc_rows),
        priority_count=priority_count,
        crawling_count=crawling_count,
    )
