"""deploy_paths テーブル: 配信パス (src→dst + setup_command + service_command)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeployPathsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def list_all(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        where = " WHERE enabled = 1" if enabled_only else ""
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT id, label, src_path, dst_path, enabled, delete_mode,
                       setup_command, service_command, notes,
                       last_synced_at, last_synced_ok,
                       created_at, updated_at
                FROM deploy_paths
                {where}
                ORDER BY label ASC, id ASC
                """,
            )
            rows = cur.fetchall()
        return [self._row(r) for r in rows]

    def get(self, path_id: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT id, label, src_path, dst_path, enabled, delete_mode,
                       setup_command, service_command, notes,
                       last_synced_at, last_synced_ok,
                       created_at, updated_at
                FROM deploy_paths WHERE id = :id
                """,
                {"id": int(path_id)},
            )
            row = cur.fetchone()
        return self._row(row) if row else None

    def create(self, *, label: str, src_path: str, dst_path: str,
               enabled: bool = True, delete_mode: bool = False,
               setup_command: str | None = None,
               service_command: str | None = None,
               notes: str | None = None) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO deploy_paths
                    (label, src_path, dst_path, enabled, delete_mode,
                     setup_command, service_command, notes)
                VALUES
                    (:lbl, :src, :dst, :en, :del,
                     :setup, :svc, :notes)
                """,
                {"lbl": label, "src": src_path, "dst": dst_path,
                 "en": 1 if enabled else 0,
                 "del": 1 if delete_mode else 0,
                 "setup": setup_command, "svc": service_command,
                 "notes": notes},
            )

    def update(self, path_id: int, **fields) -> int:
        allowed = {"label", "src_path", "dst_path", "enabled", "delete_mode",
                   "setup_command", "service_command", "notes"}
        sets = []
        params: dict[str, Any] = {"id": int(path_id)}
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k in ("enabled", "delete_mode"):
                v = 1 if v else 0
            sets.append(f"{k} = :{k}")
            params[k] = v
        if not sets:
            return 0
        sets.append("updated_at = :upd")
        params["upd"] = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"UPDATE deploy_paths SET {', '.join(sets)} WHERE id = :id",
                params,
            )
            return int(cur.rowcount)

    def delete(self, path_id: int) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM deploy_paths WHERE id = :id",
                {"id": int(path_id)},
            )
            return int(cur.rowcount)

    def record_sync_result(self, path_id: int, success: bool) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE deploy_paths
                SET last_synced_at = :ts, last_synced_ok = :ok
                WHERE id = :id
                """,
                {"ts": _utcnow_iso(), "ok": 1 if success else 0, "id": int(path_id)},
            )

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(r["id"]),
            "label": r["label"],
            "src_path": r["src_path"],
            "dst_path": r["dst_path"],
            "enabled": bool(r["enabled"]),
            "delete_mode": bool(r["delete_mode"]),
            "setup_command": r["setup_command"],
            "service_command": r["service_command"],
            "notes": r["notes"],
            "last_synced_at": r["last_synced_at"],
            "last_synced_ok": bool(r["last_synced_ok"]) if r["last_synced_ok"] is not None else None,
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        }
