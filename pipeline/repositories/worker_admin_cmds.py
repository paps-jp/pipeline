"""worker_admin_cmds: daemon に投げる admin コマンドのキュー."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class WorkerAdminCmdsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def enqueue(self, *, target_host: str, cmd_type: str,
                cmd_payload: dict[str, Any], ttl_secs: int = 600) -> int:
        """daemon に投げる admin コマンドを enqueue。 戻り値は cmd id。"""
        deadline = (datetime.now(timezone.utc) + timedelta(seconds=ttl_secs)).isoformat(timespec="seconds")
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO worker_admin_cmds
                    (target_host, cmd_type, cmd_payload, deadline_at)
                VALUES (:host, :ty, :pl, :dl)
                """,
                {"host": target_host, "ty": cmd_type,
                 "pl": json.dumps(cmd_payload, ensure_ascii=False),
                 "dl": deadline},
            )
            # last_insert_rowid (= SQLite)
            cur = conn.execute("SELECT last_insert_rowid()")
            row = cur.fetchone()
            return int(list(row.values())[0]) if isinstance(row, dict) else int(row[0])

    def claim_next(self, host: str, worker_id: str) -> dict[str, Any] | None:
        """host (or '*') 宛の pending command を 1 件 claim。 無ければ None."""
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            # claim 候補 = pending && (target_host = host OR target_host = '*')
            cur = conn.execute(
                """
                SELECT id, target_host, cmd_type, cmd_payload, deadline_at
                FROM worker_admin_cmds
                WHERE state = 'pending'
                  AND (target_host = :host OR target_host = '*')
                  AND (deadline_at IS NULL OR deadline_at > :now)
                ORDER BY id ASC
                LIMIT 1
                """,
                {"host": host, "now": now},
            )
            row = cur.fetchone()
            if not row:
                return None
            cid = int(row["id"])
            conn.execute(
                """
                UPDATE worker_admin_cmds
                SET state='claimed', claimed_by=:wid, claimed_at=:ts
                WHERE id=:id AND state='pending'
                """,
                {"id": cid, "wid": worker_id, "ts": now},
            )
        return {
            "id": cid,
            "target_host": row["target_host"],
            "cmd_type": row["cmd_type"],
            "cmd_payload": json.loads(row["cmd_payload"]) if row["cmd_payload"] else {},
            "deadline_at": row["deadline_at"],
        }

    def complete(self, cmd_id: int, *, success: bool, exit_code: int | None = None,
                 stdout: str | None = None, stderr: str | None = None,
                 error: str | None = None) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE worker_admin_cmds
                SET state = :st, completed_at = :ts, exit_code = :ec,
                    stdout = :so, stderr = :se, error = :er
                WHERE id = :id
                """,
                {"st": "done" if success else "failed",
                 "ts": _utcnow_iso(), "ec": exit_code,
                 "so": (stdout or "")[:65535] or None,
                 "se": (stderr or "")[:65535] or None,
                 "er": error, "id": int(cmd_id)},
            )

    def list_recent(self, target_host: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        where = "WHERE target_host = :host" if target_host else ""
        params: dict[str, Any] = {"lim": int(limit)}
        if target_host:
            params["host"] = target_host
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT id, target_host, cmd_type, state, claimed_by, claimed_at,
                       completed_at, exit_code, error, created_at, deadline_at
                FROM worker_admin_cmds
                {where}
                ORDER BY id DESC
                LIMIT :lim
                """,
                params,
            )
            rows = cur.fetchall()
        return [
            {"id": int(r["id"]), "target_host": r["target_host"],
             "cmd_type": r["cmd_type"], "state": r["state"],
             "claimed_by": r["claimed_by"], "claimed_at": r["claimed_at"],
             "completed_at": r["completed_at"], "exit_code": r["exit_code"],
             "error": r["error"], "created_at": r["created_at"],
             "deadline_at": r["deadline_at"]}
            for r in rows
        ]

    def get(self, cmd_id: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, target_host, cmd_type, cmd_payload, state,
                       claimed_by, claimed_at, completed_at, exit_code,
                       stdout, stderr, error, created_at, deadline_at
                FROM worker_admin_cmds WHERE id = :id
                """,
                {"id": int(cmd_id)},
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row["id"]), "target_host": row["target_host"],
            "cmd_type": row["cmd_type"],
            "cmd_payload": json.loads(row["cmd_payload"]) if row["cmd_payload"] else {},
            "state": row["state"], "claimed_by": row["claimed_by"],
            "claimed_at": row["claimed_at"], "completed_at": row["completed_at"],
            "exit_code": row["exit_code"], "stdout": row["stdout"],
            "stderr": row["stderr"], "error": row["error"],
            "created_at": row["created_at"], "deadline_at": row["deadline_at"],
        }
