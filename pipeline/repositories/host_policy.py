"""host_policy の CRUD (= ホスト単位の能力/配置ポリシー、 operator の手動上書き面)。

agent_desired の last_* (agent が自動検出した VRAM/型番) を "auto 面"、 この表を
"手動上書き面" とし、 API/supervisor が両者を merge して "effective" を出す。
手動値 NULL の項目は auto に従う ([[supervisor-scaledown-stabilization]] の続きで、
異機種フリートの配置を supervisor が正しく扱えるようにするための土台)。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# UI/API に出す編集可能フィールドと型 (手動上書き面)。
_MANUAL_FIELDS = ("tier", "max_gpu_workers", "vram_override_mb", "labels", "notes", "enabled")


class HostPolicyRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def _row_to_dict(self, r: Any) -> dict[str, Any]:
        d = {
            "host": r["host"],
            "tier": r["tier"],
            "max_gpu_workers": r["max_gpu_workers"],
            "vram_override_mb": r["vram_override_mb"],
            "labels": json.loads(r["labels"]) if r["labels"] else [],
            "notes": r["notes"],
            "enabled": int(r["enabled"]) if r["enabled"] is not None else 1,
            "updated_at": r["updated_at"],
            "updated_by": r["updated_by"],
        }
        return d

    def list(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM host_policy ORDER BY host")
            return [self._row_to_dict(r) for r in cur.fetchall()]

    def get(self, host: str) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM host_policy WHERE host = :h", {"h": host})
            r = cur.fetchone()
            return self._row_to_dict(r) if r else None

    def upsert(self, host: str, fields: dict[str, Any], updated_by: str | None = None) -> dict[str, Any]:
        """host の手動ポリシーを部分更新 (指定された key のみ)。 存在しなければ作成。"""
        now = _utcnow_iso()
        # 正規化: labels は JSON 文字列に、 enabled は 0/1 に。
        norm: dict[str, Any] = {}
        for k in _MANUAL_FIELDS:
            if k not in fields:
                continue
            v = fields[k]
            if k == "labels":
                norm[k] = json.dumps(v or [])
            elif k == "enabled":
                norm[k] = 1 if v else 0
            elif k in ("max_gpu_workers", "vram_override_mb"):
                norm[k] = int(v) if v is not None and v != "" else None
            else:
                norm[k] = v if v != "" else None
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT 1 FROM host_policy WHERE host = :h", {"h": host})
            exists = cur.fetchone() is not None
            if exists:
                if norm:
                    sets = ", ".join(f"{k} = :{k}" for k in norm)
                    conn.execute(
                        f"UPDATE host_policy SET {sets}, updated_at = :ua, updated_by = :ub WHERE host = :h",
                        {**norm, "ua": now, "ub": updated_by, "h": host},
                    )
                else:
                    conn.execute(
                        "UPDATE host_policy SET updated_at = :ua, updated_by = :ub WHERE host = :h",
                        {"ua": now, "ub": updated_by, "h": host},
                    )
            else:
                cols = ["host", "labels", "enabled", "updated_at", "updated_by"]
                vals: dict[str, Any] = {
                    "host": host,
                    "labels": norm.get("labels", "[]"),
                    "enabled": norm.get("enabled", 1),
                    "updated_at": now,
                    "updated_by": updated_by,
                }
                for k in ("tier", "max_gpu_workers", "vram_override_mb", "notes"):
                    if k in norm:
                        cols.append(k)
                        vals[k] = norm[k]
                placeholders = ", ".join(f":{c}" for c in cols)
                conn.execute(
                    f"INSERT INTO host_policy ({', '.join(cols)}) VALUES ({placeholders})",
                    vals,
                )
        return self.get(host)  # type: ignore[return-value]
