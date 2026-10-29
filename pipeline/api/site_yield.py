"""/api/v1/site-yield — サイト収率 (Thompson Sampling) の可視化用 read-only API。

paprika-job-submit はサイトを腕とする多腕バンディットで投入先を選び、
paprika-image-pull が「job から新規画像が 1 枚以上取れたか」を報酬として
crawl_config.yield_s / yield_f に積む (壁時計割引付き)。 ここはその事後分布と、
lane (ts=Thompson 枠 / rr=ローテ枠 / pr=priority / vg=virgin) 別の毎分集計
(crawl_yield_stat) を UI 向けに読むだけ。 書き込みは一切しない。

割引 (半減期) と事前分布の擬似カウントは plugin 側の init_kwargs が権威なので、
UI が workload 定義から読んで query で渡す (省略時は plugin の既定値と同じ)。
"""

from __future__ import annotations

import logging
import math
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from pipeline.api.mariadb_tables import _Backend, get_backend

log = logging.getLogger("pipeline.api.site_yield")

router = APIRouter(prefix="/api/v1/site-yield", tags=["site-yield"])

DEFAULT_HALF_LIFE_H = 48.0
DEFAULT_PRIOR = 0.5
LANES = ("ts", "rr", "pr", "vg", "na")

# 「枯渇」判定の閾値 (表示用。 plugin の選択には使わない)
DEAD_MEAN = 0.05
DEAD_MIN_N = 20.0
LOW_MEAN = 0.2
LOW_MIN_N = 10.0
# 一般向け表示の「成績」区分 (UI と揃える)。 n が少ないうちは判定しない。
GRADE_MIN_N = 3.0
GRADE_GOOD = 0.6
GRADE_OK = 0.3


def _grade(mean: float, n: float) -> str:
    if n < GRADE_MIN_N:
        return "unknown"
    if mean >= GRADE_GOOD:
        return "good"
    if mean >= GRADE_OK:
        return "ok"
    return "poor"


# ---------------- スキーマ ----------------

class SiteYieldRow(BaseModel):
    id: int
    site: str
    enabled: int
    memo: str | None
    s: float            # 割引後の成功カウント
    f: float            # 割引後の失敗カウント
    s_raw: float
    f_raw: float
    n: float            # s + f (有効観測数)
    mean: float         # (s+prior) / (s+f+2*prior)
    lo: float           # 近似 80% 区間 (正規近似)
    hi: float
    jobs: int           # 累積 job 数 (減衰なし)
    new_images: int     # 累積新規画像数 (減衰なし)
    age_s: int | None   # 最終更新からの経過秒
    updated_at: str | None
    grade: str          # good / ok / poor / unknown (_grade 参照)


class SiteYieldSummary(BaseModel):
    total: int          # enabled>=1 のサイト数
    observed: int       # n >= 1
    dead: int           # mean < DEAD_MEAN かつ n >= DEAD_MIN_N
    low: int            # mean < LOW_MEAN かつ n >= LOW_MIN_N (dead 含む)
    unknown: int        # n < 1 (未観測 / 減衰しきった)
    jobs: int
    new_images: int
    buckets: list[dict[str, Any]]   # 観測ありサイトの mean ヒストグラム (10 区間)
    grades: dict[str, int]          # good / ok / poor / unknown の件数


class SitesResponse(BaseModel):
    available: bool
    error: str | None
    sites: list[SiteYieldRow]
    total: int
    half_life_h: float
    prior: float
    summary: SiteYieldSummary


class StatRow(BaseModel):
    ts: str
    lane: str
    jobs: int
    productive: int
    new_images: int
    dup_images: int
    no_image_jobs: int


class StatsResponse(BaseModel):
    available: bool
    error: str | None
    rows: list[StatRow]
    hours: float
    bucket_min: int
    lanes: list[str]
    first_seen: dict[str, str]      # lane -> 最初に観測が入った分 (全期間)。 「切替からの経過」表示用


# ---------------- helpers ----------------

def _decay(s: float, f: float, age_s: float | None, half_life_h: float) -> tuple[float, float]:
    if age_s is None or age_s <= 0 or half_life_h <= 0:
        return s, f
    k = 0.5 ** (float(age_s) / (3600.0 * half_life_h))
    return s * k, f * k


def _beta_summary(s: float, f: float, prior: float) -> tuple[float, float, float]:
    a = max(1e-9, s + prior)
    b = max(1e-9, f + prior)
    mean = a / (a + b)
    var = (a * b) / ((a + b) ** 2 * (a + b + 1.0))
    sd = math.sqrt(max(0.0, var))
    lo = max(0.0, mean - 1.2816 * sd)
    hi = min(1.0, mean + 1.2816 * sd)
    return mean, lo, hi


def _empty_summary() -> SiteYieldSummary:
    return SiteYieldSummary(total=0, observed=0, dead=0, low=0, unknown=0,
                            jobs=0, new_images=0, buckets=[],
                            grades={"good": 0, "ok": 0, "poor": 0, "unknown": 0})


_ORDERS = {
    "mean_asc": lambda r: (r.mean, -r.n),
    "mean_desc": lambda r: (-r.mean, -r.n),
    "n_desc": lambda r: (-r.n, r.mean),
    "jobs_desc": lambda r: (-r.jobs, r.mean),
    "images_desc": lambda r: (-r.new_images, r.mean),
    "updated_desc": lambda r: ((r.age_s if r.age_s is not None else 10 ** 12), r.site),
    "site": lambda r: (r.site,),
}


# ---------------- endpoint: サイト別事後分布 ----------------

@router.get("/sites", response_model=SitesResponse)
def list_sites(
    half_life_h: float = Query(DEFAULT_HALF_LIFE_H, gt=0),
    prior: float = Query(DEFAULT_PRIOR, gt=0),
    q: str | None = None,
    order: str = "mean_asc",
    limit: int = Query(500, ge=1, le=20000),
    backend: _Backend = Depends(get_backend),
) -> SitesResponse:
    """enabled>=1 の全サイトの割引後 (S, F) と事後平均。 summary は limit 前の全件から計算。"""
    like = f"%{q}%" if q else None
    params: list[Any] = []
    where = "WHERE type='image' AND enabled >= 1"
    if like:
        where += " AND (site LIKE %s OR domain LIKE %s OR memo LIKE %s)"
        params.extend([like, like, like])
    try:
        with backend.lock:
            backend.db.ping()
            cur = backend.db.execute(
                "SELECT id, site, enabled, memo, yield_s, yield_f, yield_jobs, "
                "       yield_new_images, yield_updated_at, "
                "       TIMESTAMPDIFF(SECOND, yield_updated_at, NOW(6)) "
                f"FROM crawl_config {where}",
                tuple(params),
            )
            try:
                raw = cur.fetchall()
            finally:
                cur.close()
    except Exception as e:  # noqa: BLE001 — 列/テーブル未作成 (plugin 未デプロイ) を含む
        log.warning("site-yield sites query failed: %s", e)
        return SitesResponse(available=False, error=f"{type(e).__name__}: {e}"[:300],
                             sites=[], total=0, half_life_h=half_life_h, prior=prior,
                             summary=_empty_summary())

    rows: list[SiteYieldRow] = []
    for r in raw:
        (cid, site, enabled, memo, ys, yf, jobs, imgs, upd, age) = r
        s_raw = float(ys or 0.0)
        f_raw = float(yf or 0.0)
        age_i = int(age) if age is not None else None
        s, f = _decay(s_raw, f_raw, age_i, half_life_h)
        mean, lo, hi = _beta_summary(s, f, prior)
        rows.append(SiteYieldRow(
            id=int(cid), site=str(site or ""), enabled=int(enabled or 0),
            memo=str(memo) if memo else None,
            s=round(s, 3), f=round(f, 3), s_raw=round(s_raw, 3), f_raw=round(f_raw, 3),
            n=round(s + f, 3), mean=round(mean, 4), lo=round(lo, 4), hi=round(hi, 4),
            jobs=int(jobs or 0), new_images=int(imgs or 0),
            age_s=age_i, updated_at=str(upd) if upd else None,
            grade=_grade(mean, s + f),
        ))

    observed = [r for r in rows if r.n >= 1.0]
    buckets = [{"lo": i / 10, "hi": (i + 1) / 10, "count": 0} for i in range(10)]
    for r in observed:
        idx = min(9, int(r.mean * 10))
        buckets[idx]["count"] += 1
    grades = {"good": 0, "ok": 0, "poor": 0, "unknown": 0}
    for r in rows:
        grades[r.grade] = grades.get(r.grade, 0) + 1
    summary = SiteYieldSummary(
        total=len(rows),
        observed=len(observed),
        dead=sum(1 for r in rows if r.mean < DEAD_MEAN and r.n >= DEAD_MIN_N),
        low=sum(1 for r in rows if r.mean < LOW_MEAN and r.n >= LOW_MIN_N),
        unknown=len(rows) - len(observed),
        jobs=sum(r.jobs for r in rows),
        new_images=sum(r.new_images for r in rows),
        buckets=buckets,
        grades=grades,
    )
    key = _ORDERS.get(order) or _ORDERS["mean_asc"]
    rows.sort(key=key)
    return SitesResponse(available=True, error=None, sites=rows[:limit], total=len(rows),
                         half_life_h=half_life_h, prior=prior, summary=summary)


# ---------------- endpoint: lane 別時系列 ----------------

def _auto_bucket(hours: float) -> int:
    if hours <= 6:
        return 5
    if hours <= 48:
        return 15
    return 60


@router.get("/stats", response_model=StatsResponse)
def get_stats(
    hours: float = Query(24.0, gt=0, le=24 * 60),
    bucket_min: int | None = Query(None, ge=1, le=24 * 60),
    backend: _Backend = Depends(get_backend),
) -> StatsResponse:
    """crawl_yield_stat を bucket_min 分でまとめて返す (lane 別)。 ts は DB のローカル時刻。"""
    b = int(bucket_min or _auto_bucket(hours))
    minutes = int(math.ceil(hours * 60))
    span = 60 * b
    try:
        with backend.lock:
            backend.db.ping()
            cur = backend.db.execute(
                "SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts_min) / %s) * %s) AS b, lane, "
                "       SUM(jobs), SUM(productive), SUM(new_images), SUM(dup_images), "
                "       SUM(no_image_jobs) "
                "FROM crawl_yield_stat "
                f"WHERE ts_min >= NOW() - INTERVAL {minutes} MINUTE "
                "GROUP BY b, lane ORDER BY b ASC, lane ASC",
                (span, span),
            )
            try:
                raw = cur.fetchall()
            finally:
                cur.close()
            fcur = backend.db.execute(
                "SELECT lane, MIN(ts_min) FROM crawl_yield_stat GROUP BY lane")
            try:
                first_seen = {str(r[0] or "na"): str(r[1]) for r in fcur.fetchall() if r[1]}
            finally:
                fcur.close()
    except Exception as e:  # noqa: BLE001
        log.warning("site-yield stats query failed: %s", e)
        return StatsResponse(available=False, error=f"{type(e).__name__}: {e}"[:300],
                             rows=[], hours=hours, bucket_min=b, lanes=[], first_seen={})
    rows = [
        StatRow(ts=str(r[0]), lane=str(r[1] or "na"), jobs=int(r[2] or 0),
                productive=int(r[3] or 0), new_images=int(r[4] or 0),
                dup_images=int(r[5] or 0), no_image_jobs=int(r[6] or 0))
        for r in raw
    ]
    lanes = sorted({r.lane for r in rows},
                   key=lambda ln: (LANES.index(ln) if ln in LANES else 99, ln))
    return StatsResponse(available=True, error=None, rows=rows, hours=hours,
                         bucket_min=b, lanes=lanes, first_seen=first_seen)
