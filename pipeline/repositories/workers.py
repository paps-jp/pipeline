"""workers テーブル CRUD (worker daemon の registry).

worker daemon が起動時に register、5 秒毎に heartbeat、shutdown 時に deregister。
control plane は 90 秒見えない worker を state='lost' に。
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _new_worker_id(host: str) -> str:
    """host だけを識別子に。 同一 host に同時 2 worker は systemd instance suffix
    (= "ai-gpu1-3" 等の hostname に番号入り) で既に区別済なので、 ランダム suffix は
    冗長で UI/ログを読みにくくする (2026-06-28 operator 要望で短縮)。
    """
    safe_host = "".join(c if c.isalnum() else "_" for c in host)[:32]
    return f"w_{safe_host}"


class WorkerNotFound(LookupError):
    pass


class WorkerRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def register(
        self,
        *,
        host: str,
        pid: int | None = None,
        tags: list[str] | None = None,
        resources: dict[str, Any] | None = None,
        worker_id: str | None = None,
        env_filter: list[str] | None = None,
    ) -> dict[str, Any]:
        """新規 worker を登録 (or 既存 worker_id なら updated)。
        worker_id 未指定なら deterministic な ID を生成。
        env_filter は worker daemon の systemd env (PIPELINE_WORKLOAD_FILTER) で、
        DB filter が null の時の fallback として使われる。 None = env 未設定 (= 全 workload claim 可)。
        """
        wid = worker_id or _new_worker_id(host)
        now = _utcnow_iso()
        # env_filter を JSON エンコード (= None なら NULL)
        env_filter_json = (json.dumps(sorted(set(env_filter)))
                           if env_filter else None)
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id FROM workers WHERE id = :id", {"id": wid}
            )
            existing = cur.fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO workers (
                        id, host, pid, tags, resources,
                        state, started_at, last_seen_at, env_filter
                    ) VALUES (
                        :id, :host, :pid, :tags, :resources,
                        :state, :now, :now, :ef
                    )
                    """,
                    {
                        "id": wid,
                        "host": host,
                        "pid": pid,
                        "tags": json.dumps(tags or []),
                        "resources": json.dumps(resources or {}),
                        "state": "active",
                        "now": now,
                        "ef": env_filter_json,
                    },
                )
            else:
                conn.execute(
                    """
                    UPDATE workers SET
                        host = :host, pid = :pid,
                        tags = :tags, resources = :resources,
                        state = 'active', last_seen_at = :now,
                        started_at = COALESCE(started_at, :now),
                        env_filter = :ef
                    WHERE id = :id
                    """,
                    {
                        "id": wid,
                        "host": host,
                        "pid": pid,
                        "tags": json.dumps(tags or []),
                        "resources": json.dumps(resources or {}),
                        "now": now,
                        "ef": env_filter_json,
                    },
                )
        return self.get(wid)

    def heartbeat(
        self,
        worker_id: str,
        *,
        current_workload: str | None = None,
        current_phase: str | None = None,
        rows_processed_delta: int = 0,
        errors_total_delta: int = 0,
    ) -> dict[str, Any]:
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE workers
                SET last_seen_at = :now,
                    current_workload = :cw,
                    current_phase = :cp,
                    rows_processed = rows_processed + :rd,
                    errors_total   = errors_total + :ed,
                    state = CASE WHEN state = 'lost' THEN 'active' ELSE state END
                WHERE id = :id
                """,
                {
                    "id": worker_id, "now": now,
                    "cw": current_workload, "cp": current_phase,
                    "rd": int(rows_processed_delta), "ed": int(errors_total_delta),
                },
            )
            if cur.rowcount == 0:
                raise WorkerNotFound(worker_id)
        return self.get(worker_id)

    def deregister(self, worker_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM workers WHERE id = :id", {"id": worker_id})

    def mark_lost(self, worker_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE workers SET state = 'lost' WHERE id = :id",
                {"id": worker_id},
            )

    def prune_stale(self, lost_after_s: int = 60, delete_after_s: int = 600,
                    metrics_retain_s: int = 86400) -> dict[str, int]:
        """heartbeat が止まった worker を state='lost' に、 更に古ければ DELETE。
        worker_metrics の orphan (= 既に worker テーブルに無い worker_id) + 古い row も削除。

        - last_seen_at < now - lost_after_s 且つ state='active' → state='lost'
        - last_seen_at < now - delete_after_s → 完全 DELETE
        - worker_metrics.ts < now - metrics_retain_s → 削除 (= 既定 24h)
        - worker_metrics.worker_id NOT IN (workers) → 削除 (= orphan)
        """
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        lost_threshold = (now - timedelta(seconds=lost_after_s)).isoformat()
        delete_threshold = (now - timedelta(seconds=delete_after_s)).isoformat()
        metrics_threshold = (now - timedelta(seconds=metrics_retain_s)).isoformat()
        lost_n = 0
        deleted_n = 0
        metrics_deleted_n = 0
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE workers SET state = 'lost' WHERE last_seen_at < :t AND state = 'active'",
                {"t": lost_threshold},
            )
            lost_n = cur.rowcount
            cur = conn.execute(
                "DELETE FROM workers WHERE last_seen_at < :t",
                {"t": delete_threshold},
            )
            deleted_n = cur.rowcount
            # metrics cleanup (= orphan + 古い ts)
            try:
                cur = conn.execute(
                    "DELETE FROM worker_metrics WHERE ts < :t "
                    "OR worker_id NOT IN (SELECT id FROM workers)",
                    {"t": metrics_threshold},
                )
                metrics_deleted_n = cur.rowcount
            except Exception:
                pass  # table 未存在 (= migration 前) は無視
        return {"marked_lost": lost_n, "deleted": deleted_n, "metrics_deleted": metrics_deleted_n}

    def get(self, worker_id: str) -> dict[str, Any]:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM workers WHERE id = :id", {"id": worker_id})
            row = cur.fetchone()
            if row is None:
                raise WorkerNotFound(worker_id)
            return self._row(row)

    def list_all(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT * FROM workers ORDER BY started_at DESC, id"
            )
            return [self._row(r) for r in cur.fetchall()]

    # ---------------- workload filter (= 自動切替 SoT) ----------------

    def set_filter(
        self,
        worker_id: str,
        *,
        filter_list: list[str] | None,
        mode: str = "replace",
        updated_by: str | None = None,
    ) -> dict[str, Any]:
        """worker の workload_filter を変更。

        mode:
          - "replace" (default): filter_list で完全上書き。 None で「解除 (= env fallback)」。
          - "add":     filter_list の各 slug を現在の filter に **追加**。
                       現 filter=None (= env fallback) なら **env_filter を base**
                       にした上で union する (= 元担当を奪わず安全に追加)。
          - "remove":  filter_list の各 slug を現在の filter から **除去**。
                       現 filter=None (= env fallback) なら env_filter を base に
                       した上で差分。 結果が env_filter と同じなら null に戻す
                       (= キレイに env fallback に戻す)。

        worker が居なければ WorkerNotFound。
        無効な mode は ValueError。
        """
        if mode not in ("replace", "add", "remove"):
            raise ValueError(f"invalid mode: {mode}")
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT workload_filter, env_filter FROM workers WHERE id = :id",
                {"id": worker_id},
            )
            row = cur.fetchone()
            if row is None:
                raise WorkerNotFound(worker_id)
            cur_db = row["workload_filter"]
            cur_env = row["env_filter"]
            try:
                cur_set = set(json.loads(cur_db)) if cur_db else None
            except Exception:
                cur_set = None
            try:
                env_set = set(json.loads(cur_env)) if cur_env else set()
            except Exception:
                env_set = set()
            req = {str(s).strip() for s in (filter_list or []) if str(s).strip()}

            if mode == "replace":
                if filter_list is None:
                    new_json: str | None = None
                else:
                    new_json = json.dumps(sorted(req))
            elif mode == "add":
                # 現 filter=None なら env_filter を base、 そうでなければ現 filter を base
                base = cur_set if cur_set is not None else env_set
                merged = sorted(base | req)
                new_json = json.dumps(merged)
            else:  # remove
                base = cur_set if cur_set is not None else env_set
                after = sorted(base - req)
                # env_filter と同じなら null に戻して env fallback の意味を保つ
                if env_set and set(after) == env_set:
                    new_json = None
                else:
                    new_json = json.dumps(after)

            prev = cur_db
            if (prev or None) == (new_json or None):
                return self.get(worker_id)
            conn.execute(
                "UPDATE workers SET workload_filter = :wf, "
                "filter_updated_at = :now, filter_updated_by = :by "
                "WHERE id = :id",
                {"id": worker_id, "wf": new_json, "now": _utcnow_iso(),
                 "by": (updated_by or "operator")[:64]},
            )
        return self.get(worker_id)

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        out = dict(r)
        for c in ("tags", "resources"):
            v = out.get(c)
            if isinstance(v, str):
                try:
                    out[c] = json.loads(v)
                except Exception:
                    out[c] = []
        # workload_filter は JSON list[str] (= None なら "no filter")
        wf = out.get("workload_filter")
        if isinstance(wf, str) and wf:
            try:
                out["workload_filter"] = json.loads(wf)
            except Exception:
                out["workload_filter"] = None
        elif wf in (None, ""):
            out["workload_filter"] = None
        # env_filter (= systemd PIPELINE_WORKLOAD_FILTER で固定された fallback)
        ef = out.get("env_filter")
        if isinstance(ef, str) and ef:
            try:
                out["env_filter"] = json.loads(ef)
            except Exception:
                out["env_filter"] = None
        elif ef in (None, ""):
            out["env_filter"] = None
        return out
