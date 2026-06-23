"""rss-news-summarize: 1 RSS エントリの本文を取得して Claude haiku で 要約。

ペアの dispatcher が `rss-news-dispatcher`。 dispatcher が新エントリを
URL ベースで dedup して enqueue し、 こちらが 1 task = 1 エントリで動く。

本文取得は graceful fallback:
    trafilatura (best) → BeautifulSoup (good) → 正規表現 strip (raw) → RSS summary_html
どれも 失敗時は RSS の summary を そのまま 入力にする。

結果は plugin-local SQLite + runs.output_json の 両方に 落ちる。
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
import urllib.request
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ------------------------------------------------------------ HTML → text

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")
_NL_RE = re.compile(r"\n{3,}")


def _strip_html_basic(html: str) -> str:
    """trafilatura / BeautifulSoup が無い時の 最低限 fallback。"""
    if not html:
        return ""
    # script/style ブロック丸ごと除去
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = _TAG_RE.sub(" ", html)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    text = _WS_RE.sub(" ", text)
    text = _NL_RE.sub("\n\n", text)
    return text.strip()


def _extract_article_text(html: str) -> str:
    # 1) trafilatura — 一番きれいに 本文が取れる
    try:
        import trafilatura  # type: ignore

        out = trafilatura.extract(html, include_comments=False, favor_recall=True)
        if out and len(out.strip()) > 80:
            return out.strip()
    except Exception:
        pass

    # 2) BeautifulSoup — <article> / <main> / longest <p> cluster を 拾う
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header", "aside"]):
            tag.decompose()
        target = (
            soup.find("article")
            or soup.find("main")
            or soup.find(attrs={"role": "main"})
            or soup
        )
        text = "\n\n".join(
            p.get_text(separator=" ", strip=True) for p in target.find_all("p")
        ).strip()
        if len(text) > 80:
            return text
    except Exception:
        pass

    # 3) 正規表現 strip
    return _strip_html_basic(html)


# ------------------------------------------------------------ HTTP fetch


def _fetch_article(url: str, timeout_s: int) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (compatible; pipeline-rss-news-summarize/0.1; "
                "+https://paps-jp.github.io/pipeline/)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        ctype = r.headers.get("Content-Type", "")
        raw = r.read()
    # 文字コード推定: HTTP ヘッダ → meta → utf-8 fallback
    enc = "utf-8"
    m = re.search(r"charset=([\w\-]+)", ctype, flags=re.I)
    if m:
        enc = m.group(1).strip()
    else:
        m2 = re.search(rb'charset=["\']?([\w\-]+)', raw[:2048], flags=re.I)
        if m2:
            enc = m2.group(1).decode("ascii", errors="ignore")
    try:
        return raw.decode(enc, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


# ------------------------------------------------------------ SQLite store


def _open_store(path: str) -> sqlite3.Connection:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(p), check_same_thread=False, timeout=10)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS summaries (
            url           TEXT PRIMARY KEY,
            feed_name     TEXT,
            title         TEXT,
            published     TEXT,
            model         TEXT,
            summary       TEXT,
            input_chars   INTEGER,
            output_tokens INTEGER,
            input_tokens  INTEGER,
            latency_ms    INTEGER,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.commit()
    return db


# ------------------------------------------------------------ Anthropic call


_SYS_TMPL_JA = (
    "あなたは ニュース 要約の プロ。 与えられた 本文を、 第 1 行 = 鋭い見出し、 "
    "第 2 行以降 = 重要な 事実 / 数字 / 固有名詞だけを 端的に 並べる 形式で 全 {n} 行に 要約する。 "
    "私見・推測・「興味深い」 等の 装飾語は 禁止。 本文に 無い 情報を 加えない。"
)
_SYS_TMPL_EN = (
    "You are a senior news editor. Summarize the article in exactly {n} lines: "
    "line 1 is a sharp headline; lines 2+ are concrete facts / numbers / proper nouns. "
    "No opinion, no speculation, no padding words. Do not invent facts not in the source."
)


def _looks_japanese(s: str) -> bool:
    if not s:
        return False
    jp = sum(1 for c in s[:400] if "぀" <= c <= "ヿ" or "一" <= c <= "鿿")
    return jp >= 8


def _call_claude(
    client: Any,
    model: str,
    article_text: str,
    title: str,
    n_lines: int,
    language: str,
) -> tuple[str, dict[str, int]]:
    if language == "ja":
        sys_p = _SYS_TMPL_JA.format(n=n_lines)
    elif language == "en":
        sys_p = _SYS_TMPL_EN.format(n=n_lines)
    else:
        sys_p = (_SYS_TMPL_JA if _looks_japanese(article_text + " " + title) else _SYS_TMPL_EN).format(
            n=n_lines
        )

    user_p = (
        (f"# 元タイトル\n{title}\n\n" if title else "")
        + f"# 本文\n{article_text}"
    )
    resp = client.messages.create(
        model=model,
        max_tokens=400,
        system=sys_p,
        messages=[{"role": "user", "content": user_p}],
    )
    # text を 全 content block から 抽出
    parts: list[str] = []
    for blk in resp.content:
        if getattr(blk, "type", "") == "text":
            parts.append(blk.text)
    text = "\n".join(parts).strip()
    usage = {
        "input_tokens": int(getattr(resp.usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(resp.usage, "output_tokens", 0) or 0),
    }
    return text, usage


# ------------------------------------------------------------ plugin hooks


def setup(**kwargs) -> dict[str, Any]:
    api_key = os.environ.get(str(kwargs.get("anthropic_api_key_env") or "ANTHROPIC_API_KEY"))
    if not api_key:
        raise RuntimeError(
            "rss-news-summarize: ANTHROPIC_API_KEY (or configured env) not set"
        )
    try:
        import anthropic  # type: ignore

        client = anthropic.Anthropic(api_key=api_key)
    except ImportError as e:
        raise RuntimeError(
            "rss-news-summarize: `anthropic` package required — `pip install anthropic`"
        ) from e

    state: dict[str, Any] = {
        "client": client,
        "model": str(kwargs.get("model") or "claude-haiku-4-5-20251001"),
        "summary_sentences": int(kwargs.get("summary_sentences") or 3),
        "summary_language": str(kwargs.get("summary_language") or "auto"),
        "fetch_full_article": bool(kwargs.get("fetch_full_article", True)),
        "fetch_timeout_s": int(kwargs.get("fetch_timeout_s") or 12),
        "max_input_chars": int(kwargs.get("max_input_chars") or 8000),
        "db": _open_store(str(kwargs["store_db_path"])),
    }
    log.info("rss-news-summarize: model=%s", state["model"])
    return state


def process(task, ctx, state) -> dict[str, Any]:
    url = task.pk
    extra = task.extra or {}
    title = str(extra.get("title") or "")
    feed_name = str(extra.get("feed_name") or "")
    published = str(extra.get("published") or "")
    summary_html = str(extra.get("summary_html") or "")

    out: dict[str, Any] = {
        "url": url,
        "feed": feed_name,
        "title": title,
        "published": published,
        "model": state["model"],
    }

    # --- 1) 既に DB に 入ってる? = 再要約 スキップ (cheap idempotency)
    db = state["db"]
    row = db.execute("SELECT summary FROM summaries WHERE url = ?", (url,)).fetchone()
    if row:
        out["summary"] = row[0]
        out["cached"] = True
        return out

    # --- 2) 本文を 取りに行く (graceful fallback)
    article_text = ""
    fetch_err = None
    if state["fetch_full_article"]:
        try:
            html = _fetch_article(url, state["fetch_timeout_s"])
            article_text = _extract_article_text(html)
        except Exception as e:
            fetch_err = f"{type(e).__name__}: {e}"[:200]

    if len(article_text) < 80:
        # 失敗 / 本文薄い → RSS summary_html を 入力に使う
        article_text = _extract_article_text(summary_html) or summary_html
        out["used_rss_summary_fallback"] = True

    if fetch_err:
        out["fetch_error"] = fetch_err

    if not article_text:
        out["skipped"] = "no article text"
        return out

    if len(article_text) > state["max_input_chars"]:
        article_text = article_text[: state["max_input_chars"]] + "\n…(truncated)"
    out["input_chars"] = len(article_text)

    # --- 3) Claude を 呼ぶ
    t0 = time.time()
    summary, usage = _call_claude(
        state["client"],
        state["model"],
        article_text,
        title,
        state["summary_sentences"],
        state["summary_language"],
    )
    out["latency_ms"] = int((time.time() - t0) * 1000)
    out["summary"] = summary
    out["input_tokens"] = usage["input_tokens"]
    out["output_tokens"] = usage["output_tokens"]

    # --- 4) DB に 永続化
    db.execute(
        """
        INSERT OR REPLACE INTO summaries
          (url, feed_name, title, published, model, summary,
           input_chars, output_tokens, input_tokens, latency_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            url, feed_name, title, published, state["model"], summary,
            out["input_chars"], usage["output_tokens"], usage["input_tokens"], out["latency_ms"],
        ),
    )
    db.commit()
    return out


def cleanup(state) -> None:
    db = state.get("db") if isinstance(state, dict) else None
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
