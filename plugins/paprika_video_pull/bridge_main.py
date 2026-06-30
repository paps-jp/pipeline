"""paprika-video-pull: paprika hub の completed/review ジョブを poll し
 動画 asset を crawl_video へ INSERT する pipeline-oss plugin。

crawl.py の polling/pull が「自分が投入した job」 しか追跡しない問題を解消。
誰が投入した paprika job (user 手動 / 別 worker / 旧 worker 遺産) でも、
status が completed/review なら pipeline へ流す。

INSERT は `source_url_sha256` UNIQUE 制約で自動 dedup
(= 同 URL を何度 ingest しようとしても 1 行のみ)。

# 取得方式 (= 2026-07-01 watermark 方式)

Paprika /jobs API の `completed_after` パラメータで差分取得する。
MariaDB の `paprika_pull_watermark` テーブルに最後に処理した
`completed_at` を保存し、次 tick ではその値以降の job だけを取得。

- 取りこぼし防止: watermark - SAFETY_MARGIN_S 秒を使って境界付近を再確認
- デdup: crawl_video.source_url_sha256 UNIQUE 制約で INSERT IGNORE
- fetch_failed: seen に入れないため次 tick で自動リトライ
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://10.10.50.7:8001"
DEFAULT_PAPRIKA_HUB = "http://10.10.50.34:8000"
DEFAULT_MIN_VIDEO_BYTES = 1024 * 1024  # 1 MB
SAFETY_MARGIN_S = 60  # watermark から何秒前まで遡って確認するか


# -------------------- adaptive interval --------------------

def _adapt_init(state: dict, kwargs: dict) -> None:
    state["adapt"] = {
        "interval_s": int(kwargs.get("interval_s") or 360),
        "interval_min_s": int(kwargs.get("interval_min_s") or 360),
        "interval_max_s": int(kwargs.get("interval_max_s") or 1800),
        "miss_streak": 0,
    }


def _adapt_after_tick(state: dict, hit: bool) -> None:
    a = state["adapt"]
    if hit:
        a["miss_streak"] = 0
        a["interval_s"] = max(a["interval_min_s"], int(a["interval_s"] * 0.6))
    else:
        a["miss_streak"] += 1
        if a["miss_streak"] >= 2:
            a["interval_s"] = min(a["interval_max_s"], int(a["interval_s"] * 1.8))


def _adapt_snapshot(state: dict) -> dict:
    a = state["adapt"]
    return {"interval_s": a["interval_s"], "miss_streak": a["miss_streak"]}


# -------------------- watermark --------------------

def _load_watermark(db, slug: str) -> datetime | None:
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT last_completed_at FROM paprika_pull_watermark WHERE slug=%s",
            (slug,),
        )
        row = cur.fetchone()
        if row and row[0]:
            dt = row[0]
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    finally:
        cur.close()
    return None


def _save_watermark(db, slug: str, completed_at: datetime) -> None:
    cur = db.cursor()
    try:
        cur.execute(
            """INSERT INTO paprika_pull_watermark (slug, last_completed_at, updated_at)
               VALUES (%s, %s, NOW(6))
               ON DUPLICATE KEY UPDATE last_completed_at=%s, updated_at=NOW(6)""",
            (slug, completed_at, completed_at),
        )
        db.commit()
    finally:
        cur.close()


# -------------------- helpers --------------------

def _load_env_file(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path).expanduser()
    if not p.exists():
        return {}
    out: dict[str, str] = {}
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _http_get_json(url: str, timeout: float = 30.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _site_slug_from_url(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "unknown").lower()
        return host.replace(".", "_").replace("-", "_")[:64] or "unknown"
    except Exception:
        return "unknown"


_PAPRIKA_JID_RE = re.compile(r"(?:paprika://|/jobs/)([0-9a-f]{8,32})")


def _self_enqueue_next_tick(control_url: str, workload_slug: str, tick_id: int) -> None:
    pk = f"tick-{tick_id}-{int(time.time())}"
    req = urllib.request.Request(
        f"{control_url}/api/v1/workloads/{workload_slug}/tasks",
        data=json.dumps({"pk": pk}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as e:
        log.warning("self-enqueue failed: %s", e)


def _is_video_asset(asset: dict) -> bool:
    mime = (asset.get("mime") or "").lower()
    if mime.startswith("video/"):
        return True
    name = (asset.get("name") or "").lower()
    return name.endswith((".mp4", ".webm", ".mov", ".mkv", ".m4v"))


def _ext_from_name(name: str) -> str:
    n = (name or "").lower()
    for e in (".mp4", ".webm", ".mov", ".mkv", ".m4v"):
        if n.endswith(e):
            return e[1:]
    return ""


def _insert_video(db, job: dict, asset: dict, min_bytes: int) -> str:
    source_url = asset.get("url") or ""
    name = asset.get("name") or ""
    job_id = job.get("job_id")
    storage_url = f"/jobs/{job_id}/assets/{name}" if job_id and name else ""

    if not source_url and storage_url:
        source_url = f"paprika://{job_id}/{name}"
    if not source_url:
        return "no_url"

    size_bytes = int(asset.get("size") or 0)
    status = "pending" if size_bytes >= min_bytes else "skipped_small"
    site = _site_slug_from_url(job.get("url") or "")
    page_url = (job.get("url") or "")[:2048]
    mime = (asset.get("mime") or "")[:64]
    ext = _ext_from_name(name)[:8]

    cur = db.cursor()
    try:
        cur.execute(
            """
            INSERT IGNORE INTO crawl_video
              (crawl_id, site, source_url, storage_url, page_url, mime, ext,
               size_bytes, download_status)
            VALUES (0, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (site, source_url, storage_url[:2048] or None,
             page_url, mime, ext, size_bytes, status),
        )
        n = cur.rowcount
    finally:
        cur.close()
    return "inserted" if n > 0 else "dup"


def _ensure_db_alive(state: dict) -> Any:
    db = state.get("db")
    try:
        cur = db.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        return db
    except Exception as e:
        log.warning("DB connection dead (%s); reconnecting...", e)
        try:
            db.close()
        except Exception:
            pass
        import mariadb
        db = mariadb.connect(**state["db_cfg"])
        db.autocommit = False
        state["db"] = db
        return db


def setup(**kwargs) -> dict[str, Any]:
    env = _load_env_file(kwargs.get("db_env_file"))
    db_cfg = {
        "host": env.get("DB_HOST") or kwargs.get("db_host"),
        "port": int(env.get("DB_PORT") or kwargs.get("db_port") or 3306),
        "user": env.get("DB_USER") or kwargs.get("db_user"),
        "password": env.get("DB_PASS") or kwargs.get("db_pass"),
        "database": env.get("DB_NAME") or kwargs.get("db_name"),
    }
    if not all(db_cfg[k] for k in ("host", "user", "password", "database")):
        raise RuntimeError("paprika-video-pull: DB credentials required")
    import mariadb
    db = mariadb.connect(**db_cfg)
    db.autocommit = False

    workload_slug = kwargs.get("workload_slug") or "paprika-video-pull"
    watermark = _load_watermark(db, workload_slug)
    log.info("ingest: watermark=%s", watermark)

    state = {
        "db": db,
        "db_cfg": db_cfg,
        "paprika_hub": (kwargs.get("paprika_hub") or DEFAULT_PAPRIKA_HUB).rstrip("/"),
        "statuses": (kwargs.get("statuses") or "completed,review").strip(),
        "min_video_bytes": int(kwargs.get("min_video_bytes") or DEFAULT_MIN_VIDEO_BYTES),
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": workload_slug,
        "counter": 0,
        "hostname": os.uname().nodename,
        "watermark": watermark,
        "page_limit": int(kwargs.get("page_limit") or 200),
    }
    _adapt_init(state, kwargs)

    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("ingest: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("ingest: bootstrap enqueue failed: %s", e)
    return state


def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)

    watermark: datetime | None = state["watermark"]
    # SAFETY_MARGIN_S 秒前まで遡って境界付近の取りこぼしを防ぐ
    if watermark:
        fetch_from = watermark - timedelta(seconds=SAFETY_MARGIN_S)
    else:
        fetch_from = None

    status_csv = urllib.parse.quote(state["statuses"], safe="")
    page_limit = state["page_limit"]

    inserted = 0
    dup = 0
    no_url = 0
    no_video = 0
    fetch_failed = 0
    skipped_assets_zero = 0
    pages_fetched = 0
    jobs_seen_total = 0
    max_completed_at: datetime | None = watermark
    stop_reason = "empty_page"

    page_idx = 0
    while True:
        offset = page_idx * page_limit
        params = f"status={status_csv}&limit={page_limit}&offset={offset}"
        if fetch_from:
            params += "&completed_after=" + urllib.parse.quote(
                fetch_from.strftime("%Y-%m-%dT%H:%M:%S.%f")
            )
        list_url = f"{state['paprika_hub']}/jobs?{params}"
        try:
            resp = _http_get_json(list_url, timeout=20.0)
            jobs = resp.get("jobs") or []
        except Exception as e:
            out["paprika_list_error"] = str(e)[:200]
            stop_reason = "list_error"
            break

        pages_fetched += 1
        if not jobs:
            stop_reason = "empty_page"
            break
        jobs_seen_total += len(jobs)

        for j in jobs:
            job_id = j.get("job_id")
            if not job_id:
                continue

            # completed_at で watermark を更新
            job_completed_at = _parse_dt(j.get("completed_at"))
            if job_completed_at:
                if max_completed_at is None or job_completed_at > max_completed_at:
                    max_completed_at = job_completed_at

            assets_saved = int(((j.get("progress") or {}).get("assets_saved") or 0))
            if assets_saved <= 0:
                skipped_assets_zero += 1
                continue

            try:
                res = _http_get_json(
                    f"{state['paprika_hub']}/jobs/{job_id}/result",
                    timeout=10.0,
                )
            except Exception as e:
                fetch_failed += 1
                log.debug("fetch result failed job=%s: %s", job_id, e)
                # watermark を進めない（次 tick で再試行される）
                if job_completed_at and max_completed_at and job_completed_at >= max_completed_at:
                    max_completed_at = job_completed_at - timedelta(seconds=1)
                continue

            assets = res.get("assets") or []
            videos = [a for a in assets if _is_video_asset(a)]
            if not videos:
                no_video += 1
                continue

            for v in videos:
                try:
                    r = _insert_video(db, j, v, state["min_video_bytes"])
                    db.commit()
                except Exception as e:
                    log.warning("INSERT crawl_video failed job=%s: %s", job_id, str(e)[:120])
                    db.rollback()
                    continue
                if r == "inserted":
                    inserted += 1
                elif r == "dup":
                    dup += 1
                else:
                    no_url += 1

        page_idx += 1

    # watermark を更新
    if max_completed_at and (watermark is None or max_completed_at > watermark):
        _save_watermark(db, state["workload_slug"], max_completed_at)
        state["watermark"] = max_completed_at
        out["watermark_updated"] = max_completed_at.isoformat()

    out["watermark"] = state["watermark"].isoformat() if state["watermark"] else None
    out["pages_fetched"] = pages_fetched
    out["jobs_seen"] = jobs_seen_total
    out["inserted"] = inserted
    out["dup"] = dup
    out["no_url"] = no_url
    out["no_video_jobs"] = no_video
    out["skipped_assets_zero"] = skipped_assets_zero
    out["fetch_failed"] = fetch_failed
    out["dispatch_secs"] = round(time.time() - started, 2)

    hit = inserted > 0
    _adapt_after_tick(state, hit)
    out["adapt"] = _adapt_snapshot(state)

    log.info(
        "ingest: +%d videos (dup=%d no_vid=%d) pages=%d jobs=%d "
        "skipped_assets0=%d fetch_failed=%d stop=%s watermark=%s in %.2fs",
        inserted, dup, no_video, pages_fetched, jobs_seen_total,
        skipped_assets_zero, fetch_failed, stop_reason,
        out["watermark"], out["dispatch_secs"],
    )

    sleep_s = max(1, int(state["adapt"]["interval_s"]) - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    _self_enqueue_next_tick(
        state["control_url"], state["workload_slug"], state["counter"] + 1,
    )
    out["next_tick_scheduled"] = True
    return out


def teardown(state) -> None:
    db = state.get("db")
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
    log.info("paprika_video_pull: done %d ticks on %s",
             state.get("counter", 0), state.get("hostname"))
