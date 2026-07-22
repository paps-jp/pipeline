"""rss-news-dispatcher: RSS / Atom feeds を 定期 fetch → 新エントリだけ enqueue。

self-loop 方式: 1 tick 完了 → sleep(interval_s) → 次 tick を pipeline-oss API へ
self-enqueue する。

dedup は URL 単位で per-plugin SQLite に永続化 (`state_db_path`)。
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _load_feeds(path: str) -> list[dict[str, str]]:
    """feeds_path から [{name, url}, ...] を返す。 YAML / TXT 両対応。"""
    p = Path(path).expanduser()
    if not p.exists():
        raise RuntimeError(f"rss-news-dispatcher: feeds_path not found: {p}")
    text = p.read_text(encoding="utf-8", errors="replace")

    # YAML を試す。 未インストールなら TXT として 1 行 1 URL で読む。
    feeds: list[dict[str, str]] = []
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
        if isinstance(data, dict) and "feeds" in data:
            data = data["feeds"]
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    feeds.append({"name": _name_from_url(item), "url": item})
                elif isinstance(item, dict) and item.get("url"):
                    feeds.append(
                        {
                            "name": str(item.get("name") or _name_from_url(item["url"])),
                            "url": str(item["url"]),
                        }
                    )
        if feeds:
            return feeds
    except Exception as e:
        log.debug("feeds_path: YAML parse failed (%s), trying as TXT", e)

    # TXT fallback
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        feeds.append({"name": _name_from_url(line), "url": line})
    return feeds


def _name_from_url(url: str) -> str:
    try:
        from urllib.parse import urlparse

        host = urlparse(url).netloc or url
        return host.removeprefix("www.")
    except Exception:
        return url[:40]


def _open_state_db(path: str) -> sqlite3.Connection:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(p), check_same_thread=False, timeout=10)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            url        TEXT PRIMARY KEY,
            feed_name  TEXT NOT NULL,
            title      TEXT,
            seen_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.commit()
    return db


def _fetch_feed(url: str, timeout_s: int) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "pipeline-rss-news-dispatcher/0.1 (+https://paps-jp.github.io/pipeline/)",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        return r.read()


def _parse_feed(raw: bytes) -> list[dict[str, Any]]:
    """raw bytes → [{url, title, summary, published}, ...] feedparser 必須。"""
    import feedparser  # type: ignore

    fp = feedparser.parse(raw)
    out: list[dict[str, Any]] = []
    for e in fp.entries or []:
        url = (getattr(e, "link", "") or "").strip()
        if not url:
            continue
        out.append(
            {
                "url": url,
                "title": (getattr(e, "title", "") or "").strip(),
                "summary": (getattr(e, "summary", "") or "").strip(),
                "published": (getattr(e, "published", "") or "").strip(),
            }
        )
    return out


def _self_enqueue_next_tick(control_url: str, slug: str, tick_id: int) -> None:
    pk = f"tick-{tick_id}-{int(time.time())}"
    req = urllib.request.Request(
        f"{control_url}/api/v1/workloads/{slug}/tasks",
        data=json.dumps({"pk": pk}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as e:
        log.warning("self-enqueue failed: %s", e)


def _post_batch(control_url: str, slug: str, items: list[dict]) -> int:
    if not items:
        return 0
    req = urllib.request.Request(
        f"{control_url}/api/v1/workloads/{slug}/tasks/batch",
        data=json.dumps({"items": items}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        return int(d.get("inserted") or 0)
    except Exception as e:
        log.warning("batch enqueue failed for %s: %s", slug, e)
        return 0


def setup(**kwargs) -> dict[str, Any]:
    feeds = _load_feeds(str(kwargs["feeds_path"]))
    if not feeds:
        raise RuntimeError("rss-news-dispatcher: no feeds parsed from feeds_path")

    state: dict[str, Any] = {
        "feeds": feeds,
        "db": _open_state_db(str(kwargs["state_db_path"])),
        "target_workload": str(kwargs.get("target_workload") or "rss-news-summarize"),
        "interval_s": int(kwargs.get("interval_s") or 1800),
        "max_entries_per_feed": int(kwargs.get("max_entries_per_feed") or 30),
        "per_feed_timeout_s": int(kwargs.get("per_feed_timeout_s") or 15),
        "control_url": str(kwargs.get("control_url") or "http://localhost:8001").rstrip("/"),
        "workload_slug": str(kwargs.get("workload_slug") or "rss-news-dispatcher"),
        "counter": 0,
    }
    log.info("rss-news-dispatcher: %d feeds loaded; interval=%ds", len(feeds), state["interval_s"])

    # bootstrap: enqueue our own first tick so process() ever runs at all.
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("rss-news-dispatcher: bootstrap tick-1 enqueued")
    except Exception as e:
        log.warning("bootstrap enqueue failed: %s", e)
    return state


def _fetch_one(feed: dict[str, str], timeout_s: int) -> tuple[dict[str, str], list[dict[str, Any]], str | None]:
    try:
        raw = _fetch_feed(feed["url"], timeout_s)
        entries = _parse_feed(raw)
        return feed, entries, None
    except Exception as e:
        return feed, [], f"{type(e).__name__}: {e}"[:200]


def process(task, ctx, state) -> dict[str, Any]:
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {
        "tick": state["counter"],
        "feeds_total": len(state["feeds"]),
    }

    feeds = state["feeds"]
    max_per_feed = state["max_entries_per_feed"]
    timeout_s = state["per_feed_timeout_s"]

    # 並列 fetch (1 dispatcher 内なので 軽い 4 並列)
    fetched: list[tuple[dict[str, str], list[dict[str, Any]]]] = []
    errors: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(4, len(feeds))) as ex:
        futs = [ex.submit(_fetch_one, f, timeout_s) for f in feeds]
        for fut in as_completed(futs):
            feed, entries, err = fut.result()
            if err:
                errors.append({"feed": feed["name"], "error": err})
                continue
            fetched.append((feed, entries[:max_per_feed]))

    # dedup: URL 既見を SQLite で フィルタ
    db = state["db"]
    new_items: list[dict[str, Any]] = []
    seen_inserts: list[tuple[str, str, str]] = []
    cur = db.cursor()
    for feed, entries in fetched:
        for e in entries:
            url = e["url"]
            row = cur.execute("SELECT 1 FROM seen WHERE url = ? LIMIT 1", (url,)).fetchone()
            if row:
                continue
            new_items.append(
                {
                    "pk": url,
                    "extra": {
                        "feed_name": feed["name"],
                        "feed_url": feed["url"],
                        "title": e["title"][:300],
                        "summary_html": e["summary"][:4000],
                        "published": e["published"][:64],
                    },
                }
            )
            seen_inserts.append((url, feed["name"], e["title"][:300]))

    inserted = 0
    if new_items:
        inserted = _post_batch(state["control_url"], state["target_workload"], new_items)
        # mark as seen ONLY for the URLs the API accepted? simpler: assume all-or-nothing
        # batch endpoint, mark whatever we tried. Re-running tick will dedup against pipeline
        # queue's UNIQUE (slug, pk) anyway, so a double-mark causes no harm.
        cur.executemany(
            "INSERT OR IGNORE INTO seen (url, feed_name, title) VALUES (?, ?, ?)",
            seen_inserts,
        )
        db.commit()

    out["entries_seen"] = sum(len(e) for _, e in fetched)
    out["entries_new"] = len(new_items)
    out["enqueued"] = inserted
    out["errors"] = errors
    out["dispatch_secs"] = round(time.time() - started, 2)

    # sleep then self-enqueue next tick
    sleep_s = max(1, state["interval_s"] - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    _self_enqueue_next_tick(state["control_url"], state["workload_slug"], state["counter"] + 1)
    out["next_tick_scheduled"] = True
    return out


def cleanup(state) -> None:
    db = state.get("db") if isinstance(state, dict) else None
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
