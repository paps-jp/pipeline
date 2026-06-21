"""runs テーブル: task の処理履歴 (成功/失敗の per-attempt 記録)。"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _new_run_id() -> str:
    # 短くて URL safe、衝突確率は問題ない範囲
    return "r_" + secrets.token_hex(8)


class RunsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def record(
        self,
        *,
        workload_slug: str,
        pk: str,
        worker_id: str,
        attempt: int,
        started_at: str,
        success: bool,
        exit_code: int | None,
        duration_ms: int,
        stdout: str | None,
        stderr: str | None,
        output_json: dict[str, Any] | None,
        error: str | None,
    ) -> str:
        """1 件 INSERT し、生成した run id を返す。"""
        run_id = _new_run_id()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    id, workload_slug, pk, worker_id, attempt,
                    started_at, finished_at,
                    success, exit_code, duration_ms,
                    stdout, stderr, output_json, error
                ) VALUES (
                    :id, :ws, :pk, :wid, :att,
                    :s_at, :f_at,
                    :ok, :ec, :dur,
                    :so, :se, :oj, :er
                )
                """,
                {
                    "id": run_id,
                    "ws": workload_slug,
                    "pk": str(pk),
                    "wid": worker_id,
                    "att": int(attempt),
                    "s_at": started_at,
                    "f_at": _utcnow_iso(),
                    "ok": 1 if success else 0,
                    "ec": exit_code,
                    "dur": int(duration_ms),
                    "so": stdout,
                    "se": stderr,
                    "oj": json.dumps(output_json) if output_json else None,
                    "er": error,
                },
            )
        return run_id

    def list_for_workload(self, slug: str, limit: int = 50) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, workload_slug, pk, worker_id, attempt,
                       started_at, finished_at, success, exit_code, duration_ms,
                       stdout, stderr, output_json, error
                FROM runs
                WHERE workload_slug = :ws
                ORDER BY started_at DESC, id DESC
                LIMIT :lim
                """,
                {"ws": slug, "lim": int(limit)},
            )
            rows = cur.fetchall()
        return [self._row(r) for r in rows]

    def list_recent(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, workload_slug, pk, worker_id, attempt,
                       started_at, finished_at, success, exit_code, duration_ms,
                       stdout, stderr, output_json, error
                FROM runs
                ORDER BY started_at DESC, id DESC
                LIMIT :lim
                """,
                {"lim": int(limit)},
            )
            rows = cur.fetchall()
        return [self._row(r) for r in rows]

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": r["id"],
            "workload_slug": r["workload_slug"],
            "pk": r["pk"],
            "worker_id": r["worker_id"],
            "attempt": int(r["attempt"]),
            "started_at": r["started_at"],
            "finished_at": r["finished_at"],
            "success": bool(r["success"]) if r["success"] is not None else None,
            "exit_code": r["exit_code"],
            "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
            "stdout": r["stdout"],
            "stderr": r["stderr"],
            "output_json": json.loads(r["output_json"]) if r["output_json"] else None,
            "error": r["error"],
        }
