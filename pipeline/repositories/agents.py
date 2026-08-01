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

    @staticmethod
    def _clamp_to_template(effective: dict[str, Any], template: dict[str, Any]) -> dict[str, Any]:
        """effective を template の上限で頭打ちにする (planner の削減は保持)。

        template に無い slug は落とす。 count は min(effective, template)。
        """
        t_wl = (template or {}).get("workloads") or {}
        e_wl = (effective or {}).get("workloads") or {}
        out: dict[str, Any] = {}
        for slug, t_spec in t_wl.items():
            spec = dict(t_spec)
            e_spec = e_wl.get(slug)
            if isinstance(e_spec, dict) and e_spec.get("count") is not None:
                try:
                    spec["count"] = min(int(e_spec["count"]), int(t_spec.get("count") or 0))
                except (TypeError, ValueError):
                    pass
            out[slug] = spec
        return {"workloads": out}

    def set_template(self, host: str, template: dict[str, Any], by: str) -> None:
        """operator/supervisor が上限テンプレ(max intent)を設定。

        初回 (INSERT) は planner 未介入なので effective もテンプレで埋める。
        既存行の UPDATE では effective をテンプレで上書きしない。 elastic の
        AGENT-SCALE がこの経路で毎 tick テンプレを書くため、 上書きすると
        planner が算定した VRAM 予算内の effective が数秒で消え、 物理的に
        入らない台数が agent に配られ続ける (2026-08-01: 16GB のカードに 22GB
        分の plan が復活し続けた)。 template が下がった場合だけ effective を
        その上限まで引き下げる。
        """
        tj = json.dumps(template, ensure_ascii=False)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT desired_json FROM agent_desired WHERE host=:h", {"h": host}
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO agent_desired (host, template_json, desired_json, updated_at, updated_by) "
                    "VALUES (:h, :t, :t, :now, :by)",
                    {"h": host, "t": tj, "now": _now(), "by": by},
                )
                return
            effective = None
            if row["desired_json"]:
                try:
                    effective = json.loads(row["desired_json"])
                except Exception:
                    effective = None
            dj = tj if effective is None else json.dumps(
                self._clamp_to_template(effective, template), ensure_ascii=False
            )
            conn.execute(
                "UPDATE agent_desired SET template_json=:t, desired_json=:d, "
                "  updated_at=:now, updated_by=:by WHERE host=:h",
                {"h": host, "t": tj, "d": dj, "now": _now(), "by": by},
            )

    def set_effective(self, host: str, desired: dict[str, Any], by: str) -> None:
        """planner が VRAM から算定した effective desired を設定 (template は触らない)。"""
        dj = json.dumps(desired, ensure_ascii=False)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE agent_desired SET desired_json=:d, updated_at=:now, updated_by=:by "
                "WHERE host=:h",
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
        for k in ("desired_json", "template_json", "last_children_json"):
            if d.get(k):
                try:
                    d[k.replace("_json", "")] = json.loads(d[k])
                except Exception:
                    d[k.replace("_json", "")] = None
        return d
