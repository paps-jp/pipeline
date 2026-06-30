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

    def throughput_counts(self, since_iso: str) -> dict[str, int]:
        """直近窓に開始した成功 run を slug 別に COUNT (= flow throughput 用)。

        started_at index のみ使い、 行を Python にロードしないので、 runs が
        数百万行ある高スループット時でも軽い。 返り値 = {slug: 件数} (= runs/分)。
        """
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT workload_slug AS s, COUNT(*) AS c FROM runs "
                "WHERE started_at >= :since AND success = 1 GROUP BY workload_slug",
                {"since": str(since_iso)},
            )
            return {r["s"]: int(r["c"]) for r in cur.fetchall()}

    def metric_sum_by_slug(
        self,
        slug_fields: dict[str, list[str]],
        since_iso: str,
    ) -> dict[str, float]:
        """slug 毎に指定 field 群 を SUM (= 直近窓に捌いた「件数」)。

        slug_fields = {slug: [field, ...]} (= 例: image-dispatcher → [hash_detect_enqueued, ...])。
        runs.output_json[field] を直近 1min で SUM。 返り値 = {slug: 件数/分}。
        slug_fields が空 dict なら何も走らせず {} を返す (= flow snapshot fast path)。

        1 回の SQL で全 slug を集計する: WHERE workload_slug IN (...) で絞ることで
        通常 5800 runs/秒 のうち主要 slug 分だけスキャン。 既存 throughput_counts
        が GROUP BY workload_slug で同じ index を引いてるため、 cost は線形。
        """
        if not slug_fields:
            return {}
        # 全 slug に登場する field の union を SELECT で SUM 列に並べる。
        # SQLite には dynamic column 名がないので、 field ごとに SUM 列を作る。
        all_fields = sorted({f for fields in slug_fields.values() for f in fields})
        sum_cols = ", ".join(
            f'SUM(COALESCE(JSON_EXTRACT(output_json, "$.{f}"), 0)) AS "m_{f}"'
            for f in all_fields
        )
        placeholders = ", ".join(f":s{i}" for i in range(len(slug_fields)))
        params: dict[str, Any] = {"since": str(since_iso)}
        for i, slug in enumerate(slug_fields.keys()):
            params[f"s{i}"] = slug
        sql = (
            f"SELECT workload_slug AS s, {sum_cols} "
            f"FROM runs WHERE started_at >= :since AND success = 1 "
            f"AND workload_slug IN ({placeholders}) GROUP BY workload_slug"
        )
        out: dict[str, float] = {}
        with self.db.transaction() as conn:
            cur = conn.execute(sql, params)
            for r in cur.fetchall():
                slug = r["s"]
                fields = slug_fields.get(slug, [])
                total = 0.0
                for f in fields:
                    v = r[f"m_{f}"]
                    if v is not None:
                        total += float(v)
                out[slug] = total
        return out

    def latest_by_slug(self, since_iso: str) -> dict[str, dict[str, Any]]:
        """直近窓で slug ごとの最新 run 1 件を返す (= flow の state/last_output 用)。

        window 関数 (ROW_NUMBER) で slug 数分の行だけ返すため、 全行ロードしない。
        必要列のみ (stdout/stderr は除外)。 返り値 = {slug: row dict}。
        """
        out: dict[str, dict[str, Any]] = {}
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT workload_slug, started_at, finished_at, success, output_json FROM ("
                "  SELECT workload_slug, started_at, finished_at, success, output_json,"
                "         ROW_NUMBER() OVER (PARTITION BY workload_slug"
                "                            ORDER BY started_at DESC) AS rn"
                "  FROM runs WHERE started_at >= :since"
                ") t WHERE rn = 1",
                {"since": str(since_iso)},
            )
            for r in cur.fetchall():
                oj = r["output_json"]
                out[r["workload_slug"]] = {
                    "workload_slug": r["workload_slug"],
                    "started_at": r["started_at"],
                    "finished_at": r["finished_at"],
                    "success": bool(r["success"]) if r["success"] is not None else None,
                    "output_json": json.loads(oj) if oj else None,
                }
        return out

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
