"""deploy_targets テーブル: 配信先 GPU 箱のレジストリ."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeployTargetsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def list_all(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        where = " WHERE enabled = 1" if enabled_only else ""
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT id, label, host, ssh_user, ssh_port, enabled, notes,
                       last_deploy_at, last_deploy_ok, created_at, updated_at
                FROM deploy_targets
                {where}
                ORDER BY label ASC, id ASC
                """,
            )
            rows = cur.fetchall()
        return [self._row(r) for r in rows]

    def get(self, target_id: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, label, host, ssh_user, ssh_port, enabled, notes,
                       last_deploy_at, last_deploy_ok, created_at, updated_at
                FROM deploy_targets WHERE id = :id
                """,
                {"id": int(target_id)},
            )
            row = cur.fetchone()
        return self._row(row) if row else None

    def create(self, *, label: str, host: str, ssh_user: str = "root",
               ssh_port: int = 22, enabled: bool = True,
               notes: str | None = None) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO deploy_targets (label, host, ssh_user, ssh_port, enabled, notes)
                VALUES (:lbl, :host, :user, :port, :en, :notes)
                """,
                {"lbl": label, "host": host, "user": ssh_user, "port": int(ssh_port),
                 "en": 1 if enabled else 0, "notes": notes},
            )
            return int(cur.rowcount)  # SQLite では lastrowid 取得は別 API、 とりあえず

    def update(self, target_id: int, **fields) -> int:
        # 許可フィールドのみ
        allowed = {"label", "host", "ssh_user", "ssh_port", "enabled", "notes"}
        sets = []
        params: dict[str, Any] = {"id": int(target_id)}
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k == "enabled":
                v = 1 if v else 0
            sets.append(f"{k} = :{k}")
            params[k] = v
        if not sets:
            return 0
        sets.append("updated_at = :upd")
        params["upd"] = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"UPDATE deploy_targets SET {', '.join(sets)} WHERE id = :id",
                params,
            )
            return int(cur.rowcount)

    def delete(self, target_id: int) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM deploy_targets WHERE id = :id",
                {"id": int(target_id)},
            )
            return int(cur.rowcount)

    def record_deploy_result(self, host: str, success: bool) -> None:
        """deploy 完了時に各 host の last_deploy_* を更新。"""
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE deploy_targets
                SET last_deploy_at = :ts, last_deploy_ok = :ok
                WHERE host = :host
                """,
                {"ts": _utcnow_iso(), "ok": 1 if success else 0, "host": host},
            )

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(r["id"]),
            "label": r["label"],
            "host": r["host"],
            "ssh_user": r["ssh_user"],
            "ssh_port": int(r["ssh_port"]),
            "enabled": bool(r["enabled"]),
            "notes": r["notes"],
            "last_deploy_at": r["last_deploy_at"],
            "last_deploy_ok": bool(r["last_deploy_ok"]) if r["last_deploy_ok"] is not None else None,
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        }
