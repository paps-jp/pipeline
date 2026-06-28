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

    def start(
        self,
        *,
        workload_slug: str,
        pk: str,
        worker_id: str,
        attempt: int,
        started_at: str,
    ) -> str:
        """処理開始時に finished_at=NULL で INSERT。run id を返す。"""
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
                    :s_at, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL
                )
                """,
                {
                    "id": run_id,
                    "ws": workload_slug,
                    "pk": str(pk),
                    "wid": worker_id,
                    "att": int(attempt),
                    "s_at": started_at,
                },
            )
        return run_id

    def finish(
        self,
        run_id: str,
        *,
        success: bool,
        exit_code: int | None,
        duration_ms: int,
        stdout: str | None,
        stderr: str | None,
        output_json: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        """処理完了時に結果を UPDATE。"""
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at  = :f_at,
                    success      = :ok,
                    exit_code    = :ec,
                    duration_ms  = :dur,
                    stdout       = :so,
                    stderr       = :se,
                    output_json  = :oj,
                    error        = :er
                WHERE id = :id
                """,
                {
                    "id": run_id,
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
        """後方互換: 1 回で INSERT+完了 (executor build 失敗等の即 fail 用)。"""
        run_id = self.start(
            workload_slug=workload_slug,
            pk=pk,
            worker_id=worker_id,
            attempt=attempt,
            started_at=started_at,
        )
        self.finish(
            run_id,
            success=success,
            exit_code=exit_code,
            duration_ms=duration_ms,
            stdout=stdout,
            stderr=stderr,
            output_json=output_json,
            error=error,
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

    def list_since(self, started_after_iso: str) -> list[dict[str, Any]]:
        # 時刻ベース取得。 limit ベースだと高頻度 workload (= image-embed) が枠を
        # 食い尽くし、 長 interval workload (= paprika-links-pull) の最新 run を
        # 押し出して flow の throughput=0/state=idle 誤判定を起こす (2026-06-27)。
        # 5min カットオフを引数として渡す前提。
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, workload_slug, pk, worker_id, attempt,
                       started_at, finished_at, success, exit_code, duration_ms,
                       stdout, stderr, output_json, error
                FROM runs
                WHERE started_at >= :since
                ORDER BY started_at DESC, id DESC
                """,
                {"since": str(started_after_iso)},
            )
            rows = cur.fetchall()
        return [self._row(r) for r in rows]

    def list_recent_failures(self, limit: int = 10) -> list[dict[str, Any]]:
        # list_recent(limit=300) で fold すると、 高スループット workload で recent window が
        # 数分しか無く成功で埋まり failure が見えなくなる (=ダッシュボード "失敗はありません"
        # が常時 false 表示)。 success=0 を直接 ORDER BY で取得する。
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, workload_slug, pk, worker_id, attempt,
                       started_at, finished_at, success, exit_code, duration_ms,
                       stdout, stderr, output_json, error
                FROM runs
                WHERE success = 0
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
