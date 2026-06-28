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


# -------------------- AIMD adaptive controller --------------------
def _adapt_init(state: dict, kwargs: dict) -> None:
    state["adapt"] = {
        "interval_s": int(kwargs.get("interval_s") or 60),
        "interval_min_s": int(kwargs.get("interval_min_s") or 5),
        "interval_max_s": int(kwargs.get("interval_max_s") or 600),
        "page_limit": int(kwargs.get("page_limit") or 30),
        "page_limit_min": int(kwargs.get("page_limit_min") or 10),
        "page_limit_max": int(kwargs.get("page_limit_max") or 200),
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
    db = mariadb.connect(**db_cfg)
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
    }
    _adapt_init(state, kwargs)
    log.info("image-pull: site cache loaded (%d domains)", len(state["site_cache"]))
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
        response = s.get(url, timeout=(5, 15))
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


# ---------------- main process ---------------- #

def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)
    session = state["session"]
    raw_dir = state["raw_dir"]
    tmp_dir = state["tmp_dir"]
    dir_base = state["dir_base"]

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
    downloaded = 0
    dl_failed = 0
    no_site = 0
    no_image_jobs = 0
    fetch_failed = 0

    for j in jobs:
        job_id = j.get("job_id")
        parent_url = j.get("url") or ""
        if not job_id:
            continue
        parent_domain = _normalize_domain(parent_url)
        site = _site_for_domain(parent_domain, state["site_cache"])
        if not site:
            no_site += 1
            continue

        # 2. assets.json 取得
        try:
            res = _http_get_json(
                f"{state['paprika_hub']}/jobs/{job_id}/result",
                timeout=10.0,
            )
        except Exception as e:
            fetch_failed += 1
            log.debug("fetch result failed job=%s: %s", job_id, e)
            continue
        assets = res.get("assets") or []
        images = [a for a in assets if _is_image_asset(a)]
        if not images:
            no_image_jobs += 1
            continue

        # 3. crawl_id (= 親 URL に対応する crawl table id) を見つける
        crawl_id = _find_crawl_id(db, parent_url) or 0

        # 4. ジョブ単位で 1 回だけ batch SELECT — 既知 URL を fast-path skip
        #    INSERT IGNORE 並列で 96% が dup だった頃の lock pressure を
        #    根本的に消す。 残った race (=batch SELECT 後に他 worker が入れた行)
        #    だけが INSERT に来る → deadlock 確率激減。
        _candidate_urls: list[str] = []
        for _a in images:
            _u = (_a.get("url") or "").strip()
            if _u and not _u.startswith("data:"):
                _candidate_urls.append(normalize_image_url(_u))
        try:
            _known_urls = _find_existing_images_batch(db, _candidate_urls)
        except Exception as e:
            log.warning("batch dedup failed job=%s: %s", job_id, str(e)[:120])
            _known_urls = set()

        # 4. each image 処理
        for asset in images:
            asset_url = (asset.get("url") or "").strip()
            asset_name = (asset.get("name") or "").strip()
            href = asset.get("href") or ""

            if asset_url.startswith("data:"):
                sha = _data_url_to_sha256(asset_url)
                if not sha:
                    continue
                store_url = f"data-image:{sha.hex()}"
                # dedup check
                if _find_existing_image(db, store_url, sha):
                    dup += 1
                    continue
                # data: URL は paprika が assets/{name} で実 byte 提供してるので、
                # paprika 経由 download
                download_src = f"{state['paprika_hub']}{href}" if href else None
                if not download_src:
                    continue
                ok, err, tmp_path = _download_image(session, download_src, tmp_dir)
                if not ok:
                    dl_failed += 1
                    continue
                file_no = _next_file_no(db, site)
                if not file_no:
                    continue
                image_id = _insert_crawl_image(db, store_url, crawl_id, site, file_no, sha)
                if not image_id:
                    dup += 1
                    if tmp_path and tmp_path.exists(): tmp_path.unlink()
                    continue
                # rename to final path
                _move_to_raw(tmp_path, raw_dir, image_id, dir_base)
                _mark_image_downloaded(db, image_id)
                inserted += 1
                downloaded += 1
            else:
                if not asset_url:
                    continue
                normalized = normalize_image_url(asset_url)
                # batch SELECT で既知なら fast-path skip (= per-asset SELECT 省略)
                if normalized in _known_urls:
                    dup += 1
                    continue
                # download: paprika asset 経由 が安定 (= サイト直リンクは 403/cookie 問題)
                download_src = f"{state['paprika_hub']}{href}" if href else normalized
                ok, err, tmp_path = _download_image(session, download_src, tmp_dir)
                if not ok and download_src != normalized:
                    ok, err, tmp_path = _download_image(session, normalized, tmp_dir)
                if not ok:
                    dl_failed += 1
                    continue
                file_no = _next_file_no(db, site)
                if not file_no:
                    if tmp_path and tmp_path.exists(): tmp_path.unlink()
                    continue
                image_id = _insert_crawl_image(db, normalized, crawl_id, site, file_no, None)
                if not image_id:
                    dup += 1
                    if tmp_path and tmp_path.exists(): tmp_path.unlink()
                    continue
                _move_to_raw(tmp_path, raw_dir, image_id, dir_base)
                _mark_image_downloaded(db, image_id)
                inserted += 1
                downloaded += 1

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
