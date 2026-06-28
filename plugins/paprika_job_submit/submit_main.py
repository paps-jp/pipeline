"""paprika-job-submit: crawl_config から site を lock + crawl URL を paprika に
投入する pipeline plugin。

crawl.py の `acquire_site → process_site → crawl_page 上半分 (= 投入のみ)`
を移植。 paprika 内 fetch + 結果 pull は別 plugin (paprika-video-pull /
paprika-image-pull / paprika-links-pull) が担当 ＝ push / pull の責務分離。

1 tick = 1 site 処理 (= max_pages_per_run 件投入) → 次 tick で次 site。
site lock は crawl_config.locked_at で排他 (= 既存 crawl.py 互換)。
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

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
        raise RuntimeError("paprika-job-submit: DB credentials required")
    import mariadb
    db = mariadb.connect(**db_cfg)
    db.autocommit = True
    log.info("submit: connected to MariaDB %s/%s", db_cfg["host"], db_cfg["database"])

    state = {
        "db": db,
        "db_cfg": db_cfg,
        "paprika_hub": (kwargs.get("paprika_hub") or DEFAULT_PAPRIKA_HUB).rstrip("/"),
        "max_pages_per_run": int(kwargs.get("max_pages_per_run") or 20),
        "lock_timeout_minutes": int(kwargs.get("lock_timeout_minutes") or 120),
        "interval_s": int(kwargs.get("interval_s") or 30),
        "download_video": bool(kwargs.get("download_video", True)),
        "min_asset_size_bytes": int(kwargs.get("min_asset_size_bytes") or 2048),
        "head_check_enabled": bool(kwargs.get("head_check_enabled", True)),
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "paprika-job-submit",
        "counter": 0,
        "hostname": os.uname().nodename,
        # v3 threaded options
        "init_threads": int(kwargs.get("init_threads") or 2),
        "hard_cap": int(kwargs.get("hard_cap") or 10),
        "batch_size": int(kwargs.get("batch_size") or 5),
        "scaling_interval_s": int(kwargs.get("scaling_interval_s") or 5),
        "cap_cache_ttl_s": float(kwargs.get("cap_cache_ttl_s") or 1.0),
        "lease_seconds_actual": int(kwargs.get("lease_seconds_actual") or 55),
    }
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("submit: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("submit: bootstrap enqueue failed: %s", e)
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


# ---------------- DB helpers ---------------- #

def _acquire_site(db, worker_id: str, lock_timeout_minutes: int) -> dict | None:
    """crawl_config から enabled な 1 site を取って lock。 取れなければ None。"""
    cur = db.cursor()
    cur.execute(
        """SELECT id, site, url, domain, max_pages_per_run, toppage_fetched_at, locked_at
           FROM crawl_config
           WHERE type='image' AND enabled=1
             AND (locked_at IS NULL OR locked_at < DATE_SUB(NOW(), INTERVAL %s MINUTE))
           ORDER BY updated_at ASC LIMIT 20""",
        (lock_timeout_minutes,),
    )
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close()
    for row_tuple in rows:
        row = dict(zip(cols, row_tuple))
        # lock 取得 (compare-and-swap)
        cur = db.cursor()
        cur.execute(
            """UPDATE crawl_config SET locked_at=NOW(), worker_id=%s
               WHERE id=%s AND enabled=1
                 AND (locked_at IS NULL OR locked_at < DATE_SUB(NOW(), INTERVAL %s MINUTE))""",
            (worker_id, row["id"], lock_timeout_minutes),
        )
        affected = cur.rowcount
        cur.close()
        if affected > 0:
            return row
    return None


def _release_site(db, site_id: int) -> None:
    cur = db.cursor()
    try:
        cur.execute("UPDATE crawl_config SET locked_at=NULL, worker_id=NULL WHERE id=%s", (site_id,))
    finally:
        cur.close()


def _get_target_urls(db, site_name: str, limit: int) -> list[dict]:
    """未取得 URL 優先 + fallback で from_top_at リンク。 max LIMIT 件返す。"""
    cur = db.cursor()
    cur.execute(
        """SELECT id, url FROM crawl
           WHERE site=%s AND downloaded_at IS NULL
             AND id NOT IN (SELECT url_id FROM crawl_error_selenium WHERE site=%s)
           ORDER BY id ASC LIMIT %s""",
        (site_name, site_name, limit),
    )
    rows = [{"id": r[0], "url": r[1]} for r in cur.fetchall()]
    cur.close()
    if not rows:
        cur = db.cursor()
        cur.execute(
            """SELECT id, url FROM crawl
               WHERE site=%s AND from_top_at IS NOT NULL
                 AND id NOT IN (SELECT url_id FROM crawl_error_selenium WHERE site=%s)
               ORDER BY id ASC LIMIT %s""",
            (site_name, site_name, limit),
        )
        rows = [{"id": r[0], "url": r[1]} for r in cur.fetchall()]
        cur.close()
    return rows


def _mark_url_done(db, crawl_id: int) -> None:
    cur = db.cursor()
    try:
        cur.execute("UPDATE crawl SET downloaded_at=NOW() WHERE id=%s", (crawl_id,))
    finally:
        cur.close()


def _mark_url_404(db, crawl_id: int) -> None:
    cur = db.cursor()
    try:
        cur.execute("UPDATE crawl SET downloaded_at='0000-00-00 00:00:00' WHERE id=%s", (crawl_id,))
    finally:
        cur.close()


def _mark_site_toppage_done(db, site_id: int, result: str) -> None:
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE crawl_config SET toppage_fetched_at=NOW(), toppage_fetch_result=%s WHERE id=%s",
            (result[:255] if result else None, site_id),
        )
    finally:
        cur.close()


# ---------------- paprika hub helpers ---------------- #

def _adopt_existing_job(paprika_hub: str, page_url: str) -> str | None:
    """409 を受けた時 paprika hub から既存 job_id を引き取る。 running > review > completed の順で 1 件。"""
    try:
        q = urllib.parse.quote(page_url, safe='')
        resp = requests.get(f"{paprika_hub}/jobs?q={q}&limit=20", timeout=10)
        if resp.status_code != 200:
            return None
        jobs = (resp.json() or {}).get("jobs", []) or []
        for st in ("running", "review", "completed"):
            for j in jobs:
                if j.get("url") == page_url and j.get("status") == st:
                    return j.get("job_id")
        return None
    except Exception as e:
        log.debug("adopt failed: %s", e)
        return None


def _create_paprika_job(paprika_hub: str, page_url: str, download_video: bool, min_size: int) -> tuple[str | None, str]:
    """paprika に create_job POST。 戻り値: (job_id or None, status_str)。
    status_str: 'created' / 'adopted' / 'denied(404)' / 'error(...)'
    """
    payload = {
        "url": page_url,
        "options": {
            "download_video": bool(download_video),
            "min_asset_size_bytes": int(min_size),
        },
    }
    try:
        resp = requests.post(f"{paprika_hub}/jobs", json=payload, timeout=30)
        if resp.status_code == 409:
            adopted = _adopt_existing_job(paprika_hub, page_url)
            if adopted:
                return adopted, "adopted"
            return None, "409_no_existing"
        if 400 <= resp.status_code < 500:
            return None, f"client_error({resp.status_code})"
        resp.raise_for_status()
        return resp.json().get("job_id"), "created"
    except requests.RequestException as e:
        return None, f"error({type(e).__name__})"


def _http_head_404_or_410(url: str, timeout: float = 10.0) -> bool:
    """HEAD して 404/410 なら True、 それ以外/失敗は False (= submit 続行)。"""
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        return r.status_code in (404, 410)
    except requests.RequestException:
        return False


# ---------------- threaded coordinator (= v3 multi-thread design) ---------------- #

import threading
import statistics


class _CapCache:
    """paprika /workers/capacity の TTL cache (= 多 thread 同時 GET 抑制)."""
    def __init__(self, paprika_hub: str, ttl_s: float):
        self.paprika_hub = paprika_hub
        self.ttl_s = ttl_s
        self._data: dict | None = None
        self._fetched_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> dict:
        now = time.time()
        with self._lock:
            if self._data is None or (now - self._fetched_at) > self.ttl_s:
                try:
                    resp = requests.get(
                        f"{self.paprika_hub}/workers/capacity", timeout=3,
                    )
                    if resp.status_code == 200:
                        self._data = resp.json()
                except Exception:
                    pass  # 失敗時は古い data 継続使用
                self._fetched_at = now
            return self._data or {"accept_new": False}


class _Coordinator:
    """multi-thread 投入 + accept_new gate + 動的 scale-up."""
    def __init__(self, state: dict):
        self.state = state
        self.cap_cache = _CapCache(state["paprika_hub"], state["cap_cache_ttl_s"])
        self.running = True
        self.stats_lock = threading.Lock()
        self.stats = {
            "submitted": 0,
            "adopted": 0,
            "create_failed": 0,
            "head_404": 0,
            "wait_ms_samples": [],  # accept_new=False で待った時間
            "site_ticks": 0,
            "no_site": 0,
        }
        self.target_n = state["init_threads"]
        self.workers: list[threading.Thread] = []

    def start(self) -> None:
        for _ in range(self.target_n):
            self._spawn()
        t = threading.Thread(target=self._scaler_loop, daemon=True)
        t.start()
        self._scaler = t

    def _spawn(self) -> None:
        t = threading.Thread(target=self._worker_loop, daemon=True)
        t.start()
        self.workers.append(t)

    def _record_wait(self, wait_ms: float) -> None:
        with self.stats_lock:
            self.stats["wait_ms_samples"].append(wait_ms)

    def _bump(self, key: str, n: int = 1) -> None:
        with self.stats_lock:
            self.stats[key] += n

    def _worker_loop(self) -> None:
        import mariadb
        db = mariadb.connect(**self.state["db_cfg"])
        db.autocommit = True
        worker_id = f"{self.state['hostname']}:{os.getpid()}:{threading.get_ident()}"
        try:
            while self.running:
                cap = self.cap_cache.get()
                if not cap.get("accept_new"):
                    wait_start = time.time()
                    while self.running:
                        time.sleep(0.5)
                        cap = self.cap_cache.get()
                        if cap.get("accept_new"):
                            break
                    if not self.running:
                        break
                    self._record_wait((time.time() - wait_start) * 1000.0)
                    continue
                # accept_new=True → site lock + 投入
                site = _acquire_site(db, worker_id, self.state["lock_timeout_minutes"])
                if not site:
                    self._bump("no_site")
                    time.sleep(2)
                    continue
                self._bump("site_ticks")
                try:
                    self._submit_one_site(db, site)
                finally:
                    _release_site(db, site["id"])
        except Exception as e:
            log.exception("worker thread crashed: %s", e)
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _submit_one_site(self, db, site: dict) -> None:
        # top page check
        top_url = site.get("url")
        if top_url:
            topfetched = site.get("toppage_fetched_at")
            need_top = topfetched is None
            if topfetched:
                try:
                    elapsed = (datetime.datetime.now() - topfetched).total_seconds()
                    need_top = elapsed >= 23 * 3600
                except Exception:
                    need_top = True
            if need_top and self.cap_cache.get().get("accept_new"):
                jid, st = _create_paprika_job(
                    self.state["paprika_hub"], top_url,
                    self.state["download_video"], self.state["min_asset_size_bytes"],
                )
                if jid:
                    _mark_site_toppage_done(db, site["id"], st)
                    self._bump("submitted" if st == "created" else "adopted")
                else:
                    self._bump("create_failed")
        # 通常 URL: site 内 batch_size 件
        url_list = _get_target_urls(db, site["site"], self.state["batch_size"])
        for row in url_list:
            if not self.running:
                break
            if not self.cap_cache.get().get("accept_new"):
                break  # 投入中断
            url = row["url"]
            crawl_id = row["id"]
            if self.state["head_check_enabled"] and _http_head_404_or_410(url):
                _mark_url_404(db, crawl_id)
                self._bump("head_404")
                continue
            jid, st = _create_paprika_job(
                self.state["paprika_hub"], url,
                self.state["download_video"], self.state["min_asset_size_bytes"],
            )
            if jid is None:
                self._bump("create_failed")
                continue
            _mark_url_done(db, crawl_id)
            self._bump("submitted" if st == "created" else "adopted")

    def _scaler_loop(self) -> None:
        """scaling_interval_s 毎に target_n を ±1 調整 (1 tick 内は scale-up のみ)."""
        while self.running:
            time.sleep(self.state["scaling_interval_s"])
            if not self.running:
                break
            with self.stats_lock:
                samples = list(self.stats["wait_ms_samples"])
                self.stats["wait_ms_samples"].clear()
            if not samples:
                # reject 0 = 余裕、 scale up
                new_n = min(self.target_n + 1, int(self.state.get("adapt_hard_cap_eff") or self.state["hard_cap"]))
                action = "up (no rejects)"
            else:
                avg_wait = statistics.mean(samples)
                if avg_wait < 100:
                    new_n = min(self.target_n + 1, int(self.state.get("adapt_hard_cap_eff") or self.state["hard_cap"]))
                    action = f"up (avg_wait={int(avg_wait)}ms)"
                elif avg_wait > 1000:
                    # 縮小は次 tick reset で実現、 ここでは記録だけ
                    new_n = self.target_n
                    action = f"hold (avg_wait={int(avg_wait)}ms, would shrink)"
                else:
                    new_n = self.target_n
                    action = f"hold (avg_wait={int(avg_wait)}ms)"
            if new_n > self.target_n:
                for _ in range(new_n - self.target_n):
                    self._spawn()
                log.info("scaler: %d → %d threads, %s", self.target_n, new_n, action)
                self.target_n = new_n

    def stop(self) -> None:
        self.running = False
        for t in self.workers:
            t.join(timeout=5)

    def snapshot(self) -> dict:
        with self.stats_lock:
            s = dict(self.stats)
            s.pop("wait_ms_samples", None)
        return s


# ---------------- main process ---------------- #

def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}

    lease = max(10, int(state["lease_seconds_actual"]) - 5)  # 5s 余裕で end
    coord = _Coordinator(state)
    coord.start()

    while time.time() - started < lease:
        time.sleep(1)

    coord.stop()
    stats = coord.snapshot()
    out.update(stats)
    out["final_target_n"] = coord.target_n
    out["dispatch_secs"] = round(time.time() - started, 2)

    # ----- adapt: paprika 受付状況に応じて hard_cap + tick 間隔を可変 -----
    # 内部 scaler は wait_ms ベースで thread 数を上下するが、 paprika 側が
    # 拒否 (= create_failed が多い) しているときは hard_cap 自体を絞らないと
    # scaler が常に上限まで膨らんで失敗を量産する。
    # AIMD: fail_ratio > 0.4 を 2 連続で hard_cap *= 0.7、 < 0.15 で +1。
    submitted = int(stats.get("submitted") or 0)
    failed = int(stats.get("create_failed") or 0)
    total = submitted + failed
    fail_ratio = failed / total if total > 0 else 0.0

    state.setdefault("adapt_fail_streak", 0)
    state.setdefault("adapt_idle_streak", 0)
    state.setdefault("adapt_hard_cap_eff", int(state["hard_cap"]))
    state.setdefault("adapt_interval_s", 1)   # tick 間 sleep (起動時は最短)
    state.setdefault("adapt_interval_min_s", int(state.get("adapt_interval_min_s") or 1))
    state.setdefault("adapt_interval_max_s", int(state.get("adapt_interval_max_s") or 120))

    if total >= 5 and fail_ratio > 0.4:
        state["adapt_fail_streak"] += 1
        state["adapt_idle_streak"] = 0
        if state["adapt_fail_streak"] >= 2:
            new_cap = max(2, int(state["adapt_hard_cap_eff"] * 0.7))
            if new_cap != state["adapt_hard_cap_eff"]:
                log.info("submit: fail_ratio=%.0f%% → hard_cap %d→%d",
                         fail_ratio*100, state["adapt_hard_cap_eff"], new_cap)
                state["adapt_hard_cap_eff"] = new_cap
            state["adapt_interval_s"] = min(
                state["adapt_interval_max_s"], int(state["adapt_interval_s"] * 1.8) or 5,
            )
    elif submitted == 0 and total <= 1:
        # 完全 idle (paprika 側 capacity 無 or 全部 adopted) → tick 間隔を延長
        state["adapt_fail_streak"] = 0
        state["adapt_idle_streak"] += 1
        if state["adapt_idle_streak"] >= 2:
            state["adapt_interval_s"] = min(
                state["adapt_interval_max_s"], int(state["adapt_interval_s"] * 1.5) or 5,
            )
    else:
        # 健康 (submitted > 0, fail_ratio 低)
        state["adapt_fail_streak"] = 0
        state["adapt_idle_streak"] = 0
        state["adapt_hard_cap_eff"] = min(
            int(state["hard_cap"]), state["adapt_hard_cap_eff"] + 1,
        )
        state["adapt_interval_s"] = max(
            state["adapt_interval_min_s"], int(state["adapt_interval_s"] * 0.6),
        )

    out["adapt"] = {
        "hard_cap_eff": state["adapt_hard_cap_eff"],
        "interval_s": state["adapt_interval_s"],
        "fail_streak": state["adapt_fail_streak"],
        "idle_streak": state["adapt_idle_streak"],
        "fail_ratio_pct": round(fail_ratio * 100, 1),
    }

    log.info("submit: submitted=%d adopted=%d failed=%d head404=%d final_n=%d adapt=%s in %.1fs",
             stats["submitted"], stats["adopted"], stats["create_failed"],
             stats["head_404"], coord.target_n, out["adapt"], out["dispatch_secs"])

    # tick 間 backoff sleep (= 通常は 1s、 失敗多発時は最大 120s)
    time.sleep(int(state["adapt_interval_s"]))

    # next tick (1 個だけ、 self-loop 維持)
    _self_enqueue_next_tick(
        state["control_url"], state["workload_slug"], state["counter"] + 1,
    )
    out["next_tick_scheduled"] = True
    return out


def _get_paprika_capacity(paprika_hub: str) -> dict:
    """paprika /workers/capacity を取得。 失敗時は accept_new=False 扱い."""
    try:
        resp = requests.get(f"{paprika_hub}/workers/capacity", timeout=5)
        if resp.status_code != 200:
            return {"accept_new": False}
        return resp.json() or {}
    except Exception:
        return {"accept_new": False}


def _get_queue_pending(control_url: str, workload_slug: str) -> int:
    """自分の queue table の pending 件数を pipeline-oss API 経由で取得。"""
    try:
        resp = requests.get(
            f"{control_url}/api/v1/workloads/{workload_slug}/queue", timeout=3,
        )
        if resp.status_code != 200:
            return 0
        d = resp.json() or {}
        # 形式: {"pending": N, ...} or {"depth": N, ...}
        return int(d.get("pending") or d.get("depth") or 0)
    except Exception:
        return 0


def _sleep_and_enqueue(state: dict, started: float) -> None:
    """補充モデル: queue depth を target 並列度に保つように不足分だけ enqueue。

    target = min(paprika.recommended_free / max_pages_per_run, PARALLEL_CAP)
    need = max(1, target - current_pending)   ← 最低 1 (= self-loop 維持)

    これで毎 tick 累積する fan-out 暴走を防ぐ。
    """
    elapsed = int(time.time() - started)
    sleep_s = max(1, int(state["interval_s"]) - elapsed)
    time.sleep(sleep_s)

    cap = _get_paprika_capacity(state["paprika_hub"])
    accept = bool(cap.get("accept_new"))
    free = int(cap.get("recommended_free") or 0)
    per_site = max(1, int(state["max_pages_per_run"]))
    PARALLEL_CAP = 9

    if not accept:
        # 投入停止: 1 個だけ enqueue (= self-loop 維持、 次 tick で再判定)
        target = 0
    else:
        target = min(free // per_site, PARALLEL_CAP)

    current = _get_queue_pending(state["control_url"], state["workload_slug"])
    # 不足分だけ enqueue (= 最低 1 個で self-loop 切らさない)
    need = max(1, target - current)

    base_id = state["counter"] + 1
    for i in range(need):
        _self_enqueue_next_tick(
            state["control_url"], state["workload_slug"], base_id + i,
        )
    if need > 1:
        log.info("submit: refill +%d (target=%d, current_pending=%d, paprika_free=%d)",
                 need, target, current, free)


def teardown(state) -> None:
    db = state.get("db")
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
    log.info("paprika_job_submit: done %d ticks on %s",
             state.get("counter", 0), state.get("hostname"))
