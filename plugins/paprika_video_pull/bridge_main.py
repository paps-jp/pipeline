"""paprika-video-pull: paprika hub の completed/review ジョブを poll し
 動画 asset を crawl_video へ INSERT する pipeline-oss plugin。

crawl.py の polling/pull が「自分が投入した job」 しか追跡しない問題を解消。
誰が投入した paprika job (user 手動 / 別 worker / 旧 worker 遺産) でも、
status が completed/review なら pipeline へ流す。

INSERT は `source_url_sha256` UNIQUE 制約で自動 dedup
(= 同 URL を何度 ingest しようとしても 1 行のみ)。

# 取りこぼし対策 (= 2026-06-25)

paprika hub の dispatch rate (~50+ jobs/min) に対し旧版は最新
page_limit=200 件しか poll しなかった。 60s tick の遅延 / restart 直後 /
バースト時にジョブが該当窓を滑り抜けると、 そのジョブは created_at DESC
の sort で深い offset に流され二度と再訪されない (= 例 job 00a6f58b351d)。

対策: `seen_job_ids` を保持し、 1 tick で「新規が 0 件のページ」が出るまで
paginate (max_pages 上限)。 startup 時に DB の `crawl_video` から既処理
job_id を pre-load し、 bootstrap コストも抑える。
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from collections import OrderedDict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://10.10.50.7:8001"
DEFAULT_PAPRIKA_HUB = "http://10.10.50.34:8000"
DEFAULT_MIN_VIDEO_BYTES = 1024 * 1024  # 1 MB
# in-memory seen-job cap (LRU); 1 entry ~ 64B = ~6 MB at 100k
SEEN_IDS_CAP = 100_000


# -------------------- AIMD adaptive controller --------------------
# 負荷に応じて interval / page_limit / max_pages を自動調整。
# hit (= 新規が拾えた) ならアグレッシブに (interval↓ / page_limit↑ / max_pages↑)、
# 2連続 miss (= 全部既処理 or 新規 0) なら multiplicative backoff (interval↑、 page/max は緩く↓)。
# 即発火 = hit 1 回で前進、 落ち着き = miss 連発で広く間隔を空ける。

def _adapt_init(state: dict, kwargs: dict, *, with_pages: bool = False) -> None:
    a = {
        "interval_s": int(kwargs.get("interval_s") or 60),
        "interval_min_s": int(kwargs.get("interval_min_s") or 5),
        "interval_max_s": int(kwargs.get("interval_max_s") or 600),
        "page_limit": int(kwargs.get("page_limit") or 200),
        "page_limit_min": int(kwargs.get("page_limit_min") or 50),
        "page_limit_max": int(kwargs.get("page_limit_max") or 500),
        "miss_streak": 0,
    }
    if with_pages:
        a["max_pages"] = int(kwargs.get("max_pages") or 50)
        a["max_pages_min"] = int(kwargs.get("max_pages_min") or 2)
        a["max_pages_max"] = int(kwargs.get("max_pages_max") or 50)
    state["adapt"] = a


def _adapt_after_tick(state: dict, hit: bool) -> None:
    a = state["adapt"]
    if hit:
        a["miss_streak"] = 0
        a["interval_s"] = max(a["interval_min_s"], int(a["interval_s"] * 0.6))
        a["page_limit"] = min(
            a["page_limit_max"],
            max(a["page_limit_min"], int(a["page_limit"] * 1.3)),
        )
        if "max_pages_max" in a:
            a["max_pages"] = min(a["max_pages_max"], a["max_pages"] + 2)
    else:
        a["miss_streak"] += 1
        if a["miss_streak"] >= 2:
            a["interval_s"] = min(a["interval_max_s"], int(a["interval_s"] * 1.8))
            a["page_limit"] = max(a["page_limit_min"], int(a["page_limit"] * 0.8))
            if "max_pages_max" in a:
                a["max_pages"] = max(a["max_pages_min"], a["max_pages"] // 2)


def _adapt_snapshot(state: dict) -> dict:
    a = state["adapt"]
    out = {
        "interval_s": a["interval_s"],
        "page_limit": a["page_limit"],
        "miss_streak": a["miss_streak"],
    }
    if "max_pages" in a:
        out["max_pages"] = a["max_pages"]
    return out


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


def _site_slug_from_url(url: str) -> str:
    # crawl.py の site 命名規則に合わせて hostname の . を _ にする
    # 例: hsbk.cc → hsbk_cc / m.vkvideo.ru → m_vkvideo_ru
    try:
        host = (urlparse(url).hostname or "unknown").lower()
        # よくある TLD 短縮はそのまま (.com / .net 等もそのまま _ に変換)
        return host.replace(".", "_").replace("-", "_")[:64] or "unknown"
    except Exception:
        return "unknown"


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
    db.autocommit = True
    log.info("ingest: connected to MariaDB %s/%s", db_cfg["host"], db_cfg["database"])

    statuses_csv = (kwargs.get("statuses") or "completed,review").strip()
    state = {
        "db": db,
        "db_cfg": db_cfg,
        "paprika_hub": (kwargs.get("paprika_hub") or DEFAULT_PAPRIKA_HUB).rstrip("/"),
        "statuses": statuses_csv,
        "min_video_bytes": int(kwargs.get("min_video_bytes") or DEFAULT_MIN_VIDEO_BYTES),
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "paprika-video-pull",
        "counter": 0,
        "hostname": os.uname().nodename,
        # OrderedDict as LRU: key=job_id, value=None (= cheap insertion-order set)
        "seen_job_ids": OrderedDict(),
    }
    _adapt_init(state, kwargs, with_pages=True)
    # Bootstrap seen-set from DB so the first tick after a restart doesn't
    # re-/result every paprika-source row already in crawl_video.
    try:
        _bootstrap_seen_ids(state)
        log.info("ingest: bootstrapped %d seen job_ids from DB",
                 len(state["seen_job_ids"]))
    except Exception as e:
        log.warning("ingest: seen-id bootstrap failed: %s", e)
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("ingest: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("ingest: bootstrap enqueue failed: %s", e)
    return state


# storage_url の "/jobs/<jid>/assets/..." or source_url の "paprika://<jid>/..."
# から job_id を抽出。 paprika job_id は 12 桁の hex (= 短縮表現)。
_PAPRIKA_JID_RE = re.compile(r"(?:paprika://|/jobs/)([0-9a-f]{8,32})")


def _extract_job_id(source_url: str, storage_url: str | None) -> str | None:
    for s in (source_url or "", storage_url or ""):
        if not s:
            continue
        m = _PAPRIKA_JID_RE.search(s)
        if m:
            return m.group(1)
    return None


def _bootstrap_seen_ids(state: dict) -> None:
    """DB から paprika-source な crawl_video 行を読み、 既処理 job_id を
    seen-set に追加。 これで restart 直後の tick が深く paginate しても
    /result を再フェッチせず済む (= 不必要な http 殺到を防ぐ)。

    取得上限は SEEN_IDS_CAP。 古い分は溢れて再 /result される (= 害なし、
    INSERT IGNORE で dedup)。
    """
    db = state["db"]
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT source_url, storage_url FROM crawl_video "
            "WHERE source_url LIKE 'paprika://%' "
            "ORDER BY id DESC LIMIT %s",
            (SEEN_IDS_CAP,),
        )
        seen: OrderedDict[str, None] = state["seen_job_ids"]
        for src, stg in cur.fetchall():
            jid = _extract_job_id(src or "", stg or "")
            if jid:
                seen[jid] = None
    finally:
        cur.close()


def _remember_seen(state: dict, job_id: str) -> None:
    """LRU 風に seen-set に追加。 cap 超で先頭を pop。"""
    seen: OrderedDict[str, None] = state["seen_job_ids"]
    if job_id in seen:
        seen.move_to_end(job_id)
        return
    seen[job_id] = None
    while len(seen) > SEEN_IDS_CAP:
        seen.popitem(last=False)


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
        db.autocommit = True
        state["db"] = db
        return db


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
    # kind は paprika /jobs/{id}/result の assets には付かない (= 別 logic)
    # video 判定: mime が video/* で始まる、 もしくは拡張子で判定
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
    """1 つの video asset を crawl_video に INSERT。 戻り値: 'inserted'/'dup'/'no_url'."""
    # source_url は asset.url が原則だが、 paprika が yt-dlp で取得した動画は
    # 元 URL 不明の場合がある (= asset.url = null)。 その場合は storage_url
    # (= /jobs/{id}/assets/{name}) を source_url 代用にして dedup keyを確保。
    source_url = asset.get("url") or ""
    name = asset.get("name") or ""
    job_id = job.get("job_id")
    storage_url = f"/jobs/{job_id}/assets/{name}" if job_id and name else ""

    if not source_url and storage_url:
        # yt-dlp 取得物 (= asset.url null) は storage_url を source_url 代用に
        # する。 これで同 job 内の同 asset を 2 度 ingest しない。
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
        # INSERT IGNORE: source_url_sha256 UNIQUE で dedup される
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


def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)

    status_csv = urllib.parse.quote(state["statuses"], safe="")
    page_limit = int(state["adapt"]["page_limit"])
    max_pages = int(state["adapt"]["max_pages"])

    inserted = 0
    dup = 0
    no_url = 0
    no_video = 0
    fetch_failed = 0
    skipped_assets_zero = 0
    skipped_seen = 0
    pages_fetched = 0
    jobs_seen_total = 0
    stop_reason = "max_pages_reached"

    # paprika hub に対し offset を増やしながら paginate。
    # 連続して「新規(=seen 以外)が 1 件も無いページ」が出たら打ち切り
    # (= dispatch rate 越えて再走しない安全策)。
    consecutive_all_seen_pages = 0
    CONSEC_ALL_SEEN_STOP = 1   # 1 ページ丸ごと既処理なら抜ける (= 履歴の連続性が確認できた)

    for page_idx in range(max_pages):
        offset = page_idx * page_limit
        list_url = (
            f"{state['paprika_hub']}/jobs"
            f"?status={status_csv}&limit={page_limit}&offset={offset}"
        )
        try:
            resp = _http_get_json(list_url, timeout=20.0)
            jobs = resp.get("jobs", []) or []
        except Exception as e:
            out["paprika_list_error"] = str(e)[:200]
            stop_reason = "list_error"
            break

        pages_fetched += 1
        if not jobs:
            stop_reason = "empty_page"
            break
        jobs_seen_total += len(jobs)

        new_in_page = 0
        for j in jobs:
            job_id = j.get("job_id")
            if not job_id:
                continue
            if job_id in state["seen_job_ids"]:
                skipped_seen += 1
                continue
            new_in_page += 1
            # asset 0 件のジョブには動画は無いので /result を呼ばない
            # (= 大幅な http 削減。 動画 yt-dlp 経路は必ず assets_saved >= 1)
            assets_saved = int(((j.get("progress") or {}).get("assets_saved") or 0))
            if assets_saved <= 0:
                skipped_assets_zero += 1
                _remember_seen(state, job_id)
                continue

            try:
                res = _http_get_json(
                    f"{state['paprika_hub']}/jobs/{job_id}/result",
                    timeout=10.0,
                )
            except Exception as e:
                fetch_failed += 1
                log.debug("fetch result failed job=%s: %s", job_id, e)
                # seen には入れない (= 次 tick でリトライ)
                continue

            assets = res.get("assets") or []
            videos = [a for a in assets if _is_video_asset(a)]
            if not videos:
                no_video += 1
                _remember_seen(state, job_id)
                continue

            for v in videos:
                try:
                    r = _insert_video(db, j, v, state["min_video_bytes"])
                except Exception as e:
                    log.warning("INSERT crawl_video failed job=%s name=%s: %s",
                                job_id, v.get("name"), str(e)[:120])
                    continue
                if r == "inserted":
                    inserted += 1
                elif r == "dup":
                    dup += 1
                else:
                    no_url += 1
            _remember_seen(state, job_id)

        if new_in_page == 0:
            consecutive_all_seen_pages += 1
            if consecutive_all_seen_pages >= CONSEC_ALL_SEEN_STOP:
                stop_reason = "all_seen"
                break
        else:
            consecutive_all_seen_pages = 0

    out["pages_fetched"] = pages_fetched
    out["jobs_seen"] = jobs_seen_total
    out["inserted"] = inserted
    out["dup"] = dup
    out["no_url"] = no_url
    out["no_video_jobs"] = no_video
    out["skipped_seen"] = skipped_seen
    out["skipped_assets_zero"] = skipped_assets_zero
    out["fetch_failed"] = fetch_failed
    out["seen_cache_size"] = len(state["seen_job_ids"])
    out["stop_reason"] = stop_reason
    out["dispatch_secs"] = round(time.time() - started, 2)

    # adapt: hit = 新規拾えた OR まだ深堀必要 (= max まで paginate した)
    hit = (inserted > 0) or (stop_reason == "max_pages_reached")
    _adapt_after_tick(state, hit)
    out["adapt"] = _adapt_snapshot(state)

    if inserted > 0 or pages_fetched > 1:
        log.info(
            "ingest: +%d videos (dup=%d no_vid=%d) pages=%d jobs=%d "
            "skipped_seen=%d skipped_assets0=%d stop=%s adapt=%s in %.2fs",
            inserted, dup, no_video, pages_fetched, jobs_seen_total,
            skipped_seen, skipped_assets_zero, stop_reason, out["adapt"],
            out["dispatch_secs"],
        )

    # sleep + 次 tick self-enqueue
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
