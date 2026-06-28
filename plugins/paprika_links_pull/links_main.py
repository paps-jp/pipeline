"""paprika-links-pull: paprika hub の completed/review ジョブから links を pull
し、 同 domain かつ crawlable な URL を `crawl` table に INSERT する plugin。

URL discovery 役 ＝ crawl table の新規 URL を継続供給する事で、
paprika-job-submit が処理対象を持ち続けられる (= 枯渇しない)。

dedup は crawl.url_sha256 UNIQUE で自動。 INSERT IGNORE。
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlsplit, urlunsplit

import requests

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://10.10.50.7:8001"
DEFAULT_PAPRIKA_HUB = "http://10.10.50.34:8000"

IMAGE_EXTS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.avif')


# -------------------- AIMD adaptive controller --------------------
def _adapt_init(state: dict, kwargs: dict) -> None:
    state["adapt"] = {
        "interval_s": int(kwargs.get("interval_s") or 60),
        "interval_min_s": int(kwargs.get("interval_min_s") or 5),
        "interval_max_s": int(kwargs.get("interval_max_s") or 600),
        "page_limit": int(kwargs.get("page_limit") or 100),
        "page_limit_min": int(kwargs.get("page_limit_min") or 30),
        "page_limit_max": int(kwargs.get("page_limit_max") or 500),
        "miss_streak": 0,
    }


def _adapt_after_tick(state: dict, hit: bool) -> None:
    a = state["adapt"]
    if hit:
        a["miss_streak"] = 0
        a["interval_s"] = max(a["interval_min_s"], int(a["interval_s"] * 0.6))
        a["page_limit"] = min(
            a["page_limit_max"],
            max(a["page_limit_min"], int(a["page_limit"] * 1.3)),
        )
    else:
        a["miss_streak"] += 1
        if a["miss_streak"] >= 2:
            a["interval_s"] = min(a["interval_max_s"], int(a["interval_s"] * 1.8))
            a["page_limit"] = max(a["page_limit_min"], int(a["page_limit"] * 0.8))


def _adapt_snapshot(state: dict) -> dict:
    a = state["adapt"]
    return {
        "interval_s": a["interval_s"],
        "page_limit": a["page_limit"],
        "miss_streak": a["miss_streak"],
    }


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
        raise RuntimeError("paprika-links-pull: DB credentials required")
    import mariadb
    db = mariadb.connect(**db_cfg)
    db.autocommit = True
    log.info("links-pull: connected to MariaDB %s/%s", db_cfg["host"], db_cfg["database"])

    state = {
        "db": db,
        "db_cfg": db_cfg,
        "paprika_hub": (kwargs.get("paprika_hub") or DEFAULT_PAPRIKA_HUB).rstrip("/"),
        "statuses": (kwargs.get("statuses") or "completed,review").strip(),
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "paprika-links-pull",
        "counter": 0,
        "hostname": os.uname().nodename,
        "site_cache": _build_site_cache(db),   # domain → site_name (= startup 一括 build)
    }
    _adapt_init(state, kwargs)
    log.info("links-pull: site cache loaded (%d domains)", len(state["site_cache"]))
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("links-pull: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("links-pull: bootstrap enqueue failed: %s", e)
    return state


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
        state["site_cache"] = _build_site_cache(db)
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


# ---------------- URL helpers (= crawl.py 移植) ---------------- #

def _normalize_domain(url_or_domain: str) -> str:
    if not url_or_domain:
        return ""
    s = url_or_domain
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    try:
        host = urlparse(s).netloc.lower()
    except Exception:
        return ""
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _make_url_key(url: str) -> str:
    """URL 正規化: SPA hash 以外の fragment を除去。 crawl.py 移植。"""
    try:
        s = urlsplit(url)
    except Exception:
        return url
    frag = s.fragment or ""
    keep = frag.startswith("/") or frag.startswith("!/")
    return urlunsplit((
        (s.scheme or "").lower(),
        (s.netloc or "").lower(),
        s.path or "",
        s.query or "",
        frag if keep else "",
    ))


def _is_crawlable_url(url: str) -> bool:
    """簡易版: http/https scheme + 拡張子で除外。"""
    if not url:
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https"):
        return False
    return True


def _looks_like_image_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return path.endswith(IMAGE_EXTS)


# ---------------- site lookup (= crawl_config から domain → site) ---------------- #

def _build_site_cache(db) -> dict[str, str]:
    """crawl_config 全 enabled site の domain → site name dict を構築。
    domain 列が NULL の site は url 列から normalize して導く
    (= crawl.py の cf class と同じロジック)。 crawl_config 404 site 中
    19 件しか domain 埋まってないので fallback 必須。
    """
    cache: dict[str, str] = {}
    cur = db.cursor()
    try:
        cur.execute("SELECT site, url, domain FROM crawl_config WHERE enabled=1")
        for site, url, domain in cur.fetchall():
            d = _normalize_domain(domain) if domain else (_normalize_domain(url) if url else "")
            if d:
                cache[d] = site
    finally:
        cur.close()
    return cache


def _site_for_domain(domain: str, cache: dict) -> str | None:
    """domain → site name。 cache から完全一致 or 親 domain も試す (subdomain)."""
    if not domain:
        return None
    if domain in cache:
        return cache[domain]
    parts = domain.split(".")
    for i in range(1, len(parts) - 1):
        parent = ".".join(parts[i:])
        if parent in cache:
            cache[domain] = cache[parent]
            return cache[parent]
    cache[domain] = None
    return None


# ---------------- paprika hub helpers ---------------- #

def _http_get_json(url: str, timeout: float = 20.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _insert_crawl_row(db, site: str, url: str) -> int:
    """crawl table に INSERT IGNORE。 戻り値 1=新規、 0=dup。"""
    cur = db.cursor()
    try:
        cur.execute(
            "INSERT IGNORE INTO crawl (site, url) VALUES (%s, %s)",
            (site, url[:11000]),  # url longtext だが超巨大は念のため切詰
        )
        return cur.rowcount
    finally:
        cur.close()


# ---------------- main process ---------------- #

def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)

    # 1. paprika hub から最近の completed/review job 一覧
    status_csv = urllib.parse.quote(state["statuses"], safe="")
    list_url = (
        f"{state['paprika_hub']}/jobs"
        f"?status={status_csv}&limit={state['adapt']['page_limit']}&offset=0"
    )
    try:
        resp = _http_get_json(list_url, timeout=20.0)
        jobs = resp.get("jobs", []) or []
    except Exception as e:
        out["paprika_list_error"] = str(e)[:200]
        jobs = []
    out["jobs_seen"] = len(jobs)

    inserted = 0
    dup = 0
    filtered_domain = 0
    filtered_image = 0
    filtered_non_crawlable = 0
    no_site = 0
    fetch_failed = 0
    no_links = 0

    for j in jobs:
        job_id = j.get("job_id")
        parent_url = j.get("url") or ""
        if not job_id or not parent_url:
            continue
        parent_domain = _normalize_domain(parent_url)
        site_name = _site_for_domain(parent_domain, state["site_cache"])
        if not site_name:
            no_site += 1
            continue

        # 2. paprika /jobs/{id}/links 取得
        try:
            res = _http_get_json(
                f"{state['paprika_hub']}/jobs/{job_id}/links",
                timeout=10.0,
            )
        except Exception as e:
            fetch_failed += 1
            log.debug("fetch links failed job=%s: %s", job_id, e)
            continue
        links = res.get("links") or []
        if not links:
            no_links += 1
            continue

        # 3. each link → same domain check + crawlable check + INSERT
        for link in links:
            href = link.get("href") or ""
            if not href:
                continue
            target_domain = _normalize_domain(href)
            # 同 domain only (= サブドメイン含む)
            if not (target_domain == parent_domain
                    or target_domain.endswith("." + parent_domain)):
                filtered_domain += 1
                continue
            if not _is_crawlable_url(href):
                filtered_non_crawlable += 1
                continue
            if _looks_like_image_url(href):
                filtered_image += 1
                continue
            normalized = _make_url_key(href)
            try:
                n = _insert_crawl_row(db, site_name, normalized)
            except Exception as e:
                log.warning("INSERT crawl failed site=%s url=%s err=%s",
                            site_name, normalized[:60], str(e)[:100])
                continue
            if n > 0:
                inserted += 1
            else:
                dup += 1

    out["inserted"] = inserted
    out["dup"] = dup
    out["filtered_domain"] = filtered_domain
    out["filtered_image"] = filtered_image
    out["filtered_non_crawlable"] = filtered_non_crawlable
    out["no_site_in_config"] = no_site
    out["jobs_no_links"] = no_links
    out["fetch_failed"] = fetch_failed
    out["dispatch_secs"] = round(time.time() - started, 2)

    # adapt: hit = 新規 URL を 1 件以上発見
    _adapt_after_tick(state, hit=(inserted > 0))
    out["adapt"] = _adapt_snapshot(state)

    if inserted > 0:
        log.info("links-pull: +%d new URLs (dup=%d filtered_dom=%d) from %d jobs adapt=%s in %.2fs",
                 inserted, dup, filtered_domain, len(jobs), out["adapt"],
                 out["dispatch_secs"])

    # 4. sleep + 次 tick self-enqueue
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
    log.info("paprika_links_pull: done %d ticks on %s",
             state.get("counter", 0), state.get("hostname"))
