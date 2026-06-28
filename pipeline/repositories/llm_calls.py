"""LLM advisor の呼び出し履歴 CRUD。 監査 + コスト追跡 + UI 表示用。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LlmCallsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def record(
        self,
        *,
        provider: str,
        model: str,
        prompt_messages: list[dict[str, Any]],
        response_text: str | None,
        analysis: str | None,
        actions: list[dict[str, Any]] | None,
        actions_applied: int,
        success: bool,
        error: str | None,
        prompt_tokens: int | None,
        completion_tokens: int | None,
        total_tokens: int | None,
        duration_ms: int,
    ) -> int:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO llm_calls (
                    called_at, provider, model,
                    prompt_tokens, completion_tokens, total_tokens, duration_ms,
                    success, error,
                    prompt_json, response_text, actions_json, actions_applied,
                    analysis
                ) VALUES (
                    :ca, :p, :m,
                    :pt, :ct, :tt, :dur,
                    :ok, :err,
                    :prompt, :resp, :acts, :napps,
                    :ana
                )""",
                {
                    "ca": _utcnow_iso(), "p": provider, "m": model,
                    "pt": prompt_tokens, "ct": completion_tokens,
                    "tt": total_tokens, "dur": int(duration_ms),
                    "ok": 1 if success else 0, "err": (error or None),
                    "prompt": json.dumps(prompt_messages, ensure_ascii=False)[:60000],
                    "resp": (response_text or "")[:60000],
                    "acts": json.dumps(actions or [], ensure_ascii=False)[:20000],
                    "napps": int(actions_applied),
                    "ana": (analysis or "")[:8000],
                },
            )
            # 独自 SqliteCursor ラッパは lastrowid を持たないので、 SQL で直接取得。
            cur = conn.execute("SELECT last_insert_rowid() AS rowid")
            row = cur.fetchone()
            return int(row["rowid"]) if row else 0

    def list_recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id, called_at, provider, model, prompt_tokens, completion_tokens, "
                "total_tokens, duration_ms, success, error, actions_applied, analysis "
                "FROM llm_calls ORDER BY id DESC LIMIT :n",
                {"n": int(limit)},
            )
            return [dict(r) for r in cur.fetchall()]

    def get(self, call_id: int) -> dict[str, Any] | None:
        """1 件の詳細(prompt + response 含む) を返す。"""
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT * FROM llm_calls WHERE id = :id", {"id": int(call_id)},
            )
            row = cur.fetchone()
            if row is None:
                return None
            out = dict(row)
            for k in ("prompt_json", "actions_json"):
                v = out.get(k)
                if isinstance(v, str) and v:
                    try: out[k] = json.loads(v)
                    except Exception: pass
            return out

    def prune_old(self, keep_n: int = 1000) -> int:
        """直近 keep_n 件以外を削除。"""
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM llm_calls WHERE id NOT IN "
                "(SELECT id FROM llm_calls ORDER BY id DESC LIMIT :n)",
                {"n": int(keep_n)},
            )
            return cur.rowcount
