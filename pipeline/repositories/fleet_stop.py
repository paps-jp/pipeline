"""fleet_stop_state — 「全停止」の記録。

停止時に enabled=1 だった workload の slug を残し、 再開時にそれだけを戻す。
「もともと止めていたもの」 を勝手に動かさないための記録である。

resumed_at IS NULL の行が 「現在停止中」 を表す。 同時に 1 行だけ存在しうる
(= stop 時に既存の active 行があれば呼出側が弾く)。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class FleetStopRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def active(self) -> dict[str, Any] | None:
        """現在停止中ならその記録を返す。 停止していなければ None。"""
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id, stopped_at, stopped_by, slugs_json "
                "FROM fleet_stop_state WHERE resumed_at IS NULL "
                "ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
        if row is None:
            return None
        rec = dict(row)
        try:
            rec["slugs"] = json.loads(rec.pop("slugs_json") or "[]")
        except Exception:
            rec["slugs"] = []
        return rec

    def begin(self, slugs: list[str], stopped_by: str | None = None) -> dict[str, Any]:
        """停止を記録して id を返す。"""
        now = _utcnow()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO fleet_stop_state (stopped_at, stopped_by, slugs_json) "
                "VALUES (:t, :u, :s)",
                {"t": now, "u": (stopped_by or "operator")[:64],
                 "s": json.dumps(sorted(slugs))},
            )
            # 独自 SqliteCursor ラッパは lastrowid を持たないので SQL で取得する。
            cur = conn.execute("SELECT last_insert_rowid() AS rowid")
            rid = int(dict(cur.fetchone())["rowid"])
        return {"id": rid, "stopped_at": now, "slugs": sorted(slugs)}

    def finish(self, rid: int, resumed_by: str | None = None) -> None:
        """再開済みとして閉じる。"""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE fleet_stop_state SET resumed_at = :t, resumed_by = :u "
                "WHERE id = :i AND resumed_at IS NULL",
                {"t": _utcnow(), "u": (resumed_by or "operator")[:64], "i": int(rid)},
            )

    def history(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id, stopped_at, stopped_by, slugs_json, resumed_at, resumed_by "
                "FROM fleet_stop_state ORDER BY id DESC LIMIT :n",
                {"n": int(limit)},
            )
            rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            try:
                r["slugs"] = json.loads(r.pop("slugs_json") or "[]")
            except Exception:
                r["slugs"] = []
        return rows
