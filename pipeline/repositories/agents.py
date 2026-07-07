"""agent_desired: pipeline-agent の desired 配信 + 最新報告状態の永続化。

P2: agent はローカル desired.json でなく control plane からこの行を取得する
(POST /agents/{host}/sync)。 supervisor が VRAM 安全な desired を算定して set_desired し、
agent ホストの GPU 子数を中央制御する。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AgentRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def sync(
        self,
        host: str,
        *,
        vram_total_mb: int | None,
        vram_free_mb: int | None,
        children: list[dict[str, Any]] | None,
    ) -> dict[str, Any] | None:
        """agent の sync: 最新状態を記録し、 現在の desired (dict) を返す (無ければ None)。"""
        ch_json = json.dumps(children or [], ensure_ascii=False)
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO agent_desired "
                "  (host, last_seen_at, last_vram_total_mb, last_vram_free_mb, last_children_json) "
                "VALUES (:h, :now, :vt, :vf, :ch) "
                "ON CONFLICT(host) DO UPDATE SET last_seen_at=:now, "
                "  last_vram_total_mb=:vt, last_vram_free_mb=:vf, last_children_json=:ch",
                {"h": host, "now": _now(), "vt": vram_total_mb,
                 "vf": vram_free_mb, "ch": ch_json},
            )
            row = conn.execute(
                "SELECT desired_json FROM agent_desired WHERE host=:h", {"h": host}
            ).fetchone()
        if row and row["desired_json"]:
            try:
                return json.loads(row["desired_json"])
            except Exception:
                return None
        return None

    def set_desired(self, host: str, desired: dict[str, Any], by: str) -> None:
        """desired を upsert (supervisor / operator が呼ぶ)。"""
        dj = json.dumps(desired, ensure_ascii=False)
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO agent_desired (host, desired_json, updated_at, updated_by) "
                "VALUES (:h, :d, :now, :by) "
                "ON CONFLICT(host) DO UPDATE SET desired_json=:d, updated_at=:now, updated_by=:by",
                {"h": host, "d": dj, "now": _now(), "by": by},
            )

    def get(self, host: str) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM agent_desired WHERE host=:h", {"h": host}
            ).fetchone()
        return self._row(row) if row else None

    def list_all(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM agent_desired ORDER BY host"
            ).fetchall()
        return [self._row(r) for r in rows]

    @staticmethod
    def _row(r: Any) -> dict[str, Any]:
        d = dict(r)
        for k in ("desired_json", "last_children_json"):
            if d.get(k):
                try:
                    d[k.replace("_json", "")] = json.loads(d[k])
                except Exception:
                    d[k.replace("_json", "")] = None
        return d
