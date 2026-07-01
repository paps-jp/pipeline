"""paprika-image-pull: paprika hub から画像 asset を pull → crawl_image INSERT
+ raw/{dir_no}/{image_id}.jpg 保存。

crawl.py の画像 ingest (= crawl_page 画像 loop) を B-full コピー移植。
依存: image_url_normalizer.py (= 同 dir に同梱), PIL, requests, mariadb。

1 tick = page_limit ジョブを処理 → sleep → 次 tick 自己 enqueue。
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import tempfile
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from PIL import Image

# 同 dir からの local import (= crawl.py の URL 正規化 logic)
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from image_url_normalizer import normalize_image_url  # noqa: E402

log = logging.getLogger(__name__)

SAFETY_MARGIN_S = 60


def _parse_dt(s) -> "datetime | None":
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _load_watermark(db, slug) -> "datetime | None":
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT last_completed_at FROM paprika_pull_watermark WHERE slug=%s",
            (slug,)
        )
        row = cur.fetchone()
        cur.close()
        if row and row[0]:
            v = row[0]
            if isinstance(v, datetime):
                return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
            return _parse_dt(str(v))
    except Exception:
        pass
    return None


def _save_watermark(db, slug, completed_at: "datetime") -> None:
    if completed_at is None:
        return
    try:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO paprika_pull_watermark (slug, last_completed_at, updated_at)"
            " VALUES (%s, %s, NOW(6))"
            " ON DUPLICATE KEY UPDATE"
            "   last_completed_at=IF(VALUES(last_completed_at)>last_completed_at, VALUES(last_completed_at), last_completed_at),"
            "   updated_at=NOW(6)",
            (slug, completed_at.strftime("%Y-%m-%d %H:%M:%S.%f"))
        )
        cur.close()
        db.commit()
    except Exception as e:
        log.debug("_save_watermark failed: %s", e)


# -------------------- AIMD adaptive controller --------------------
INTERVAL_MIN_S = 5
INTERVAL_MAX_S = 3600
PAGE_LIMIT_MIN = 5
PAGE_LIMIT_MAX = 500


def _adapt_init(state: dict, kwargs: dict) -> None:
    state["adapt"] = {
        "interval_s": int(kwargs.get("interval_s") or 60),
        "page_limit": int(kwargs.get("page_limit") or 30),
        "miss_streak": 0,
    }


def _adapt_after_tick(state: dict, hit: bool) -> None:
    a = state["adapt"]
    if hit:
        a["miss_streak"] = 0
        a["interval_s"] = max(INTERVAL_MIN_S, int(a["interval_s"] * 0.6))
        a["page_limit"] = min(PAGE_LIMIT_MAX, max(PAGE_LIMIT_MIN, int(a["page_limit"] * 1.3)))
    else:
        a["miss_streak"] += 1
        if a["miss_streak"] >= 2:
            a["interval_s"] = min(INTERVAL_MAX_S, int(a["interval_s"] * 1.8))
            a["page_limit"] = max(PAGE_LIMIT_MIN, int(a["page_limit"] * 0.8))


def _adapt_snapshot(state: dict) -> dict:
    a = state["adapt"]
    return {
        "interval_s": a["interval_s"],
        "page_limit": a["page_limit"],
        "miss_streak": a["miss_streak"],
    }


DEFAULT_CONTROL_URL = "http://10.10.50.7:8001"
DEFAULT_PAPRIKA_HUB = "http://10.10.50.34:8000"


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
        raise RuntimeError("paprika-image-pull: DB credentials required")
    import mariadb
    db = mariadb.connect(**{**db_cfg, "reconnect": True})
    db.autocommit = True
    log.info("image-pull: connected to MariaDB %s/%s", db_cfg["host"], db_cfg["database"])

    raw_dir = Path(kwargs.get("raw_dir") or "/mnt/paps-ai-data/crawl/raw")
    tmp_dir = Path(kwargs.get("tmp_dir") or str(raw_dir / "work"))
    tmp_dir = tmp_dir / f"image_pull_{os.getpid()}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    sess = requests.Session()

    state = {
        "db": db,
        "db_cfg": db_cfg,
        "paprika_hub": (kwargs.get("paprika_hub") or DEFAULT_PAPRIKA_HUB).rstrip("/"),
        "raw_dir": raw_dir,
        "tmp_dir": tmp_dir,
        "statuses": (kwargs.get("statuses") or "completed,review").strip(),
        "dir_base": int(kwargs.get("dir_base") or 1000),
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "paprika-image-pull",
        "counter": 0,
        "hostname": os.uname().nodename,
        "session": sess,
        "site_cache": _build_site_cache(db),
        "parallel_dl": int(kwargs.get("parallel_dl") or 8),
        "max_jobs_per_tick": int(kwargs.get("max_jobs_per_tick") or 2000),
    }
    _adapt_init(state, kwargs)
    state["watermark"] = _load_watermark(db, state["workload_slug"])
    log.info("image-pull: site cache loaded (%d domains), watermark=%s",
             len(state["site_cache"]), state["watermark"])
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("image-pull: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("image-pull: bootstrap enqueue failed: %s", e)
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
        db = mariadb.connect(**{**state["db_cfg"], "reconnect": True})
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


# ---------------- helpers (= crawl.py 移植) ---------------- #

def _normalize_domain(url: str) -> str:
    if not url:
        return ""
    s = url if url.startswith(("http://", "https://")) else "https://" + url
    try:
        host = urlparse(s).netloc.lower()
    except Exception:
        return ""
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _build_site_cache(db) -> dict[str, str]:
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


def _data_url_to_sha256(data_url: str) -> bytes | None:
    try:
        _, encoded = data_url.split(",", 1)
        return hashlib.sha256(base64.b64decode(encoded)).digest()
    except Exception:
        return None


def _get_dir_no(file_no: int, base: int) -> int:
    return (((file_no - 1) // base) + 1) * base


def _next_file_no(db, site: str) -> int | None:
    """crawl_config.next_no を atomic に inc して 1 個取得。
    crawl.py の get_next_file_no 移植 (= site lock 無しの単純 UPDATE で並列安全)."""
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE crawl_config SET next_no = next_no + 1 WHERE site=%s",
            (site,),
        )
        if cur.rowcount == 0:
            return None
        cur.execute("SELECT next_no FROM crawl_config WHERE site=%s", (site,))
        row = cur.fetchone()
        if not row:
            return None
        # +1 した結果の値。 crawl.py: 「結果 = 新しい next_no」 → 採番 = (新next_no - 1)
        return int(row[0]) - 1
    finally:
        cur.close()


# ---------------- image dedup / INSERT ---------------- #

def _find_existing_image(db, image_url: str, data_sha256: bytes | None) -> int | None:
    """既存 crawl_image を url_sha256 or data_sha256 で検索。 戻り値 image_id or None。"""
    cur = db.cursor()
    try:
        if data_sha256:
            cur.execute(
                "SELECT id FROM crawl_image WHERE data_sha256=%s LIMIT 1",
                (data_sha256,),
            )
        else:
            cur.execute(
                "SELECT id FROM crawl_image WHERE url_sha256 = UNHEX(SHA2(%s, 256)) LIMIT 1",
                (image_url,),
            )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close()


def _insert_crawl_image(db, image_url: str, crawl_id: int, site: str,
                        file_no: int, data_sha256: bytes | None) -> int | None:
    """新規 crawl_image INSERT。 戻り値 image_id or None (= race/dup で失敗)。

    `INSERT IGNORE` は MariaDB InnoDB で UNIQUE 列の gap-lock 昇格を伴い
    並列 worker 間で deadlock (1213) を頻発させる。 通常 INSERT に切り替えて
    重複エラー (1062) は silent に catch する (= 正確性より throughput 優先、
    race で失う行は次 tick の cache pre-SELECT で拾える可能性が残る)。
    deadlock (1213) は短い jitter sleep で 1 度だけ retry。
    """
    import random as _rnd
    import time as _t

    if data_sha256:
        sql = ("INSERT INTO crawl_image "
               "(url, crawl_ids, site, file_no, download_status, data_sha256) "
               "VALUES (%s, %s, %s, %s, 'processing', %s)")
        params = (image_url, str(crawl_id), site, file_no, data_sha256)
    else:
        sql = ("INSERT INTO crawl_image "
               "(url, crawl_ids, site, file_no, download_status) "
               "VALUES (%s, %s, %s, %s, 'processing')")
        params = (image_url, str(crawl_id), site, file_no)

    for attempt in range(2):
        cur = db.cursor()
        try:
            cur.execute(sql, params)
            return cur.lastrowid
        except Exception as e:
            errno = getattr(e, "errno", None)
            if errno == 1062:           # ER_DUP_ENTRY = 別 worker が同じ行を入れた
                return None
            if errno in (1213, 1205) and attempt == 0:   # deadlock / lock-wait
                _t.sleep(0.03 + _rnd.random() * 0.05)
                continue
            raise
        finally:
            cur.close()
    return None


def _find_existing_images_batch(db, urls: list[str]) -> set[str]:
    """url_sha256 で一括 dedup check。 既存 URL の set を返す。

    1 ジョブ毎に 1 回呼び出して、 後段の per-asset SELECT + INSERT を激減。
    crawl_image.url_sha256 は VIRTUAL GENERATED + UNIQUE なので index seek 1 発。
    """
    if not urls:
        return set()
    cur = db.cursor()
    try:
        placeholders = ",".join(["UNHEX(SHA2(%s, 256))"] * len(urls))
        sql = f"SELECT url FROM crawl_image WHERE url_sha256 IN ({placeholders})"
        cur.execute(sql, tuple(urls))
        rows = cur.fetchall()
        return {r[0] for r in rows if r and r[0] is not None}
    finally:
        cur.close()


def _mark_image_downloaded(db, image_id: int) -> None:
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE crawl_image SET download_status=NULL, downloaded_at=NOW() WHERE id=%s",
            (image_id,),
        )
    finally:
        cur.close()


def _delete_image_row(db, image_id: int) -> None:
    cur = db.cursor()
    try:
        cur.execute("DELETE FROM crawl_image WHERE id=%s", (image_id,))
    finally:
        cur.close()


# ---------------- image download (= crawl.py 移植) ---------------- #

def _download_image(session: requests.Session, url: str, tmp_dir: Path,
                    auth_session: requests.Session | None = None) -> tuple[bool, str | None, Path | None]:
    """画像 download → PIL 検証 → jpg 変換 → tempfile に保存。
    crawl.py の download_image_to_tmp 移植。 戻り値 (ok, err, tmp_path)。"""
    s = auth_session or session
    response = None
    tmp_path = None
    try:
        response = s.get(url, timeout=(5, 8))
        if response.status_code != 200:
            return False, f"HTTP {response.status_code}", None
        data = response.content
        if len(data) < 2048:
            return False, f"tiny_image body={len(data)} bytes", None
        img = Image.open(BytesIO(data))
        img.load()
        if getattr(img, "is_animated", False):
            img.seek(0)
        rgb = img.convert("RGB")
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False, dir=str(tmp_dir)) as f:
            tmp_path = Path(f.name)
        rgb.save(str(tmp_path), format="JPEG", quality=90)
        return True, None, tmp_path
    except requests.exceptions.Timeout:
        return False, "timeout", None
    except requests.exceptions.RequestException as e:
        return False, f"req_error:{type(e).__name__}", None
    except Exception as e:
        return False, f"save_error:{type(e).__name__}:{str(e)[:80]}", None
    finally:
        if response is not None:
            response.close()


# ---------------- paprika hub helpers ---------------- #

def _http_get_json(url: str, timeout: float = 20.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _is_image_asset(asset: dict) -> bool:
    mime = (asset.get("mime") or "").lower()
    if mime.startswith("image/"):
        return True
    name = (asset.get("name") or "").lower()
    return name.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif"))


# ---------------- parallel asset worker ---------------- #

def _process_one_asset(
    *,
    asset: dict,
    site: str,
    crawl_id: int,
    parent_url: str,
    known_urls: set,
    paprika_hub: str,
    raw_dir: Path,
    tmp_dir: Path,
    dir_base: int,
    db_cfg: dict,
) -> dict:
    """1 asset を DL → DB INSERT → ファイル移動。ThreadPoolExecutor 内で実行。
    MariaDB 接続はスレッドごとに独立して生成・破棄する。"""
    import mariadb as _mariadb

    result = {"inserted": 0, "dup": 0, "dl_failed": 0, "new_entry": None, "thumb": None}
    asset_url = (asset.get("url") or "").strip()
    href = asset.get("href") or ""

    sess = requests.Session()
    try:
        if asset_url.startswith("data:"):
            sha = _data_url_to_sha256(asset_url)
            if not sha:
                return result
            store_url = f"data-image:{sha.hex()}"
            download_src = f"{paprika_hub}{href}" if href else None
            if not download_src:
                return result
            ok, _err, tmp_path = _download_image(sess, download_src, tmp_dir)
            if not ok:
                result["dl_failed"] = 1
                return result
        else:
            if not asset_url:
                return result
            normalized = normalize_image_url(asset_url)
            sha = None
            store_url = normalized
            if normalized in known_urls:
                result["dup"] = 1
                return result
            download_src = f"{paprika_hub}{href}" if href else normalized
            ok, _err, tmp_path = _download_image(sess, download_src, tmp_dir)
            if not ok and download_src != normalized:
                ok, _err, tmp_path = _download_image(sess, normalized, tmp_dir)
            if not ok:
                result["dl_failed"] = 1
                return result

        db = _mariadb.connect(**db_cfg)
        db.autocommit = True
        try:
            if asset_url.startswith("data:") and _find_existing_image(db, store_url, sha):
                result["dup"] = 1
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()
                return result
            file_no = _next_file_no(db, site)
            if not file_no:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()
                return result
            image_id = _insert_crawl_image(db, store_url, crawl_id, site, file_no, sha)
            if not image_id:
                result["dup"] = 1
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()
                return result
            thumb = _make_thumb(tmp_path)
            _move_to_raw(tmp_path, raw_dir, image_id, dir_base)
            _mark_image_downloaded(db, image_id)
            result["inserted"] = 1
            result["new_entry"] = {
                "image_id": image_id, "site": site,
                "ts": int(time.time()), "page_url": parent_url[:200],
            }
            if thumb:
                result["thumb"] = (image_id, thumb)
        finally:
            db.close()
    except Exception as e:
        log.debug("_process_one_asset error: %s", e)
        result["dl_failed"] = 1
    finally:
        sess.close()

    return result


# ---------------- main process ---------------- #

def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)
    raw_dir = state["raw_dir"]
    tmp_dir = state["tmp_dir"]
    dir_base = state["dir_base"]

    # 1. paprika hub から最近の completed/review job 一覧 (watermark でインクリメンタル取得)
    status_csv = urllib.parse.quote(state["statuses"], safe="")
    page_limit = state["adapt"]["page_limit"]
    watermark: "datetime | None" = state.get("watermark")
    completed_after_param = ""
    if watermark is not None:
        wm_s = (watermark.timestamp() - SAFETY_MARGIN_S)
        wm_dt = datetime.fromtimestamp(wm_s, tz=timezone.utc)
        completed_after_param = "&completed_after=" + urllib.parse.quote(
            wm_dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"), safe=""
        )

    max_jobs = state.get("max_jobs_per_tick", 2000)
    jobs: list = []
    offset = 0
    while True:
        list_url = (
            f"{state['paprika_hub']}/jobs"
            f"?status={status_csv}&limit={page_limit}&offset={offset}"
            f"{completed_after_param}"
        )
        try:
            resp = _http_get_json(list_url, timeout=20.0)
            page = resp.get("jobs", []) or []
        except Exception as e:
            out["paprika_list_error"] = str(e)[:200]
            break
        jobs.extend(page)
        if len(page) < page_limit or len(jobs) >= max_jobs:
            break
        offset += page_limit
    out["jobs_seen"] = len(jobs)

    # watermark 候補 (fetch_failed でないジョブの completed_at の最大値)
    new_watermark_candidate: "datetime | None" = watermark

    inserted = 0
    dup = 0
    downloaded = 0
    dl_failed = 0
    no_site = 0
    no_image_jobs = 0
    fetch_failed = 0
    new_entries: list[dict] = []

    # Phase 1 (並列): job result 取得 — HTTP fetch を並列化してレイテンシを削減
    def _fetch_one_job(j: dict) -> "dict | None":
        job_id = j.get("job_id")
        if not job_id:
            return None
        try:
            res = _http_get_json(
                f"{state['paprika_hub']}/jobs/{job_id}/result",
                timeout=5.0,
            )
            return {"j": j, "res": res}
        except Exception as e:
            log.debug("fetch result failed job=%s: %s", job_id, e)
            return None

    n_fetch = min(state.get("parallel_dl", 8) * 2, 32)
    with ThreadPoolExecutor(max_workers=n_fetch) as fetch_pool:
        fetch_results = list(fetch_pool.map(_fetch_one_job, jobs))

    db = _ensure_db_alive(state)

    asset_tasks: list[dict] = []
    for fetched in fetch_results:
        if fetched is None:
            fetch_failed += 1
            continue
        j = fetched["j"]
        res = fetched["res"]
        parent_url = j.get("url") or ""
        parent_domain = _normalize_domain(parent_url)
        site = _site_for_domain(parent_domain, state["site_cache"])
        if not site:
            no_site += 1
            continue

        # watermark 候補: fetch 成功したジョブの completed_at を追跡
        _cat = _parse_dt(j.get("completed_at"))
        if _cat is not None:
            if new_watermark_candidate is None or _cat > new_watermark_candidate:
                new_watermark_candidate = _cat
        assets = res.get("assets") or []
        images = [a for a in assets if _is_image_asset(a)]
        if not images:
            no_image_jobs += 1
            continue

        crawl_id = _find_crawl_id(db, parent_url) or 0

        _candidate_urls: list[str] = []
        for _a in images:
            _u = (_a.get("url") or "").strip()
            if _u and not _u.startswith("data:"):
                _candidate_urls.append(normalize_image_url(_u))
        try:
            known_urls = _find_existing_images_batch(db, _candidate_urls)
        except Exception as e:
            log.warning("batch dedup failed job=%s: %s", j.get("job_id"), str(e)[:120])
            known_urls = set()

        for asset in images:
            asset_tasks.append({
                "asset": asset,
                "site": site,
                "crawl_id": crawl_id,
                "parent_url": parent_url,
                "known_urls": known_urls,
                "paprika_hub": state["paprika_hub"],
                "raw_dir": raw_dir,
                "tmp_dir": tmp_dir,
                "dir_base": dir_base,
                "db_cfg": state["db_cfg"],
            })

    # Phase 2 (並列): DL + DB INSERT
    n_workers = state.get("parallel_dl", 8)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_process_one_asset, **t): t for t in asset_tasks}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as e:
                log.debug("asset worker exception: %s", e)
                dl_failed += 1
                continue
            inserted += res["inserted"]
            dup += res["dup"]
            dl_failed += res["dl_failed"]
            if res["inserted"]:
                downloaded += 1
                if res["new_entry"]:
                    new_entries.append(res["new_entry"])
                if res["thumb"]:
                    image_id, thumb_bytes = res["thumb"]
                    try:
                        _put_blob(state["control_url"], state["workload_slug"],
                                  f"thumb/{image_id}", thumb_bytes)
                    except Exception as _e:
                        log.debug("viz thumb upload failed id=%s: %s", image_id, _e)

    out["inserted"] = inserted
    out["dup"] = dup
    out["downloaded"] = downloaded
    out["dl_failed"] = dl_failed
    out["no_site_in_config"] = no_site
    out["jobs_no_images"] = no_image_jobs
    out["fetch_failed"] = fetch_failed
    out["dispatch_secs"] = round(time.time() - started, 2)

    # adapt: hit = 画像を実際に DL+INSERT できた
    _adapt_after_tick(state, hit=(downloaded > 0))
    out["adapt"] = _adapt_snapshot(state)

    if inserted > 0:
        log.info("image-pull: +%d images (dup=%d dl_failed=%d) from %d jobs adapt=%s in %.2fs",
                 inserted, dup, dl_failed, len(jobs), out["adapt"], out["dispatch_secs"])

    if new_entries:
        try:
            _update_feed(state["control_url"], state["workload_slug"], new_entries)
        except Exception as _e:
            log.debug("viz feed update failed: %s", _e)

    # watermark 保存
    if new_watermark_candidate is not None and new_watermark_candidate != watermark:
        _save_watermark(db, state["workload_slug"], new_watermark_candidate)
        state["watermark"] = new_watermark_candidate
    out["watermark"] = str(new_watermark_candidate) if new_watermark_candidate else None

    sleep_s = max(1, int(state["adapt"]["interval_s"]) - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    _self_enqueue_next_tick(
        state["control_url"], state["workload_slug"], state["counter"] + 1,
    )
    out["next_tick_scheduled"] = True
    return out


def _find_crawl_id(db, page_url: str) -> int | None:
    """親 page URL に対応する crawl row id を取得。 無ければ None。"""
    if not page_url:
        return None
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT id FROM crawl WHERE url_sha256 = UNHEX(SHA2(%s, 256)) LIMIT 1",
            (page_url,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close()


def _move_to_raw(tmp_path: Path, raw_dir: Path, image_id: int, dir_base: int) -> None:
    dir_no = _get_dir_no(image_id, dir_base)
    final = raw_dir / str(dir_no) / f"{image_id}.jpg"
    final.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp_path.rename(final)
    except Exception:
        # cross-device rename fall back to copy
        import shutil
        shutil.copy(str(tmp_path), str(final))
        try:
            tmp_path.unlink()
        except Exception:
            pass


# ---- 可視化ヘルパー ----

def _make_thumb(tmp_path: Path, max_size: int = 300) -> bytes | None:
    """tmp_path の JPEG を縮小した JPEG bytes を返す。失敗時 None。"""
    try:
        img = Image.open(str(tmp_path))
        img.thumbnail((max_size, max_size), Image.LANCZOS)
        buf = BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=72)
        return buf.getvalue()
    except Exception as e:
        log.debug("thumb failed: %s", e)
        return None


def _put_blob(control_url: str, slug: str, key: str, data: bytes) -> None:
    req = urllib.request.Request(
        f"{control_url}/api/v1/plugins/{slug}/blob/{key}",
        data=data,
        headers={"Content-Type": "image/jpeg"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        r.read()


def _update_feed(control_url: str, slug: str, new_entries: list[dict], max_entries: int = 200) -> None:
    """new_entries を feed 先頭に prepend して max_entries に截断し PUT する。"""
    if not new_entries:
        return
    try:
        req = urllib.request.Request(
            f"{control_url}/api/v1/plugins/{slug}/state/feed",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            current: list[dict] = (json.loads(r.read()) or {}).get("value") or []
        if not isinstance(current, list):
            current = []
    except Exception:
        current = []
    combined = (new_entries + current)[:max_entries]
    payload = json.dumps({"value": combined}).encode("utf-8")
    req = urllib.request.Request(
        f"{control_url}/api/v1/plugins/{slug}/state/feed",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        r.read()


def teardown(state) -> None:
    db = state.get("db")
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
    sess = state.get("session")
    if sess is not None:
        try:
            sess.close()
        except Exception:
            pass
    tmp_dir = state.get("tmp_dir")
    if tmp_dir:
        try:
            import shutil
            shutil.rmtree(str(tmp_dir), ignore_errors=True)
        except Exception:
            pass
    log.info("paprika_image_pull: done %d ticks on %s",
             state.get("counter", 0), state.get("hostname"))
