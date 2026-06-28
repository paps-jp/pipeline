"""<slug>_queue 表の CRUD (claim / complete / fail / enqueue / count)。

<slug>_queue 表は WorkloadRepository.create 時に
`Database.ensure_workload_queue(queue_table)` で自動作成される。

設計メモ (design.md §7.3 と差分):
- attempt のインクリメントは **fail 時** に行う (claim 時ではない)。
  → 初回 attempt=0、fail 後 1、再 claim→fail で 2 …。
- SQLite は `FOR UPDATE SKIP LOCKED` 非対応なので、CAS 風の
  UPDATE-WHERE + SELECT-by-claimed_by/claimed_at で並行を抑える。
  並行 worker 数が多くなったら PostgreSQL 移行で本来の SKIP LOCKED に。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _validate_queue_table(name: str) -> None:
    """インジェクション防止: 呼出側の slug 由来テーブル名を簡易チェック。"""
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"invalid queue table name: {name!r}")


class ClaimedTask:
    """claim() の戻り値。dict より型がはっきりして使いやすい。"""

    __slots__ = ("pk", "attempt", "extra", "enqueued_at")

    def __init__(self, pk: str, attempt: int, extra: dict[str, Any], enqueued_at: str) -> None:
        self.pk = pk
        self.attempt = attempt
        self.extra = extra
        self.enqueued_at = enqueued_at


class QueueRepository:
    """workload queue (= <slug>_queue) の CRUD。

    Phase 2-α (2026-06-29): 「workload 別 backend 切替」 をサポート。
    - primary_db: SQLite (= pipeline-oss control plane と共有、 既存挙動と互換)
    - secondary_db: MariaDB (= 業務 queue 用、 optional)
    - set_backend(queue_table, 'mariadb') で 個別 workload を MariaDB に振り替え

    既存 init (= QueueRepository(db) 1 引数) は SQLite-only モードで動く。
    """

    def __init__(self, db: Database, secondary_db: Database | None = None) -> None:
        self.db = db
        self.secondary_db = secondary_db
        # queue_table → 'primary' (default) or 'secondary'。 drain.py / control が
        # 起動時に workloads.queue_backend を読んで set_backend() で配線。
        self._backend_map: dict[str, str] = {}

    def set_backend(self, queue_table: str, backend: str) -> None:
        """queue_table の backend を切替 ('primary' or 'secondary')。

        'secondary' を指定したのに secondary_db=None の場合は primary に fallback。
        """
        if backend not in ("primary", "secondary"):
            raise ValueError(f"backend must be 'primary' or 'secondary', got {backend!r}")
        if backend == "secondary" and self.secondary_db is None:
            # secondary 未配線なら無視 (= primary 動作継続)
            self._backend_map.pop(queue_table, None)
            return
        self._backend_map[queue_table] = backend

    def _get_db(self, queue_table: str) -> Database:
        """queue_table に対応する DB instance を返す (= primary or secondary)。"""
        if self._backend_map.get(queue_table) == "secondary" and self.secondary_db is not None:
            return self.secondary_db
        return self.db

    def wire_from_workloads(self, workloads: Iterable[Any]) -> None:
        """各 workload.queue_backend を見て backend を一括配線。

        'mariadb' → secondary、 それ以外 (= 'sqlite' / 想定外値) → primary。
        secondary_db=None なら set_backend が secondary を無視するので、
        全 workload が primary 動作のまま (= 後方互換)。 起動時と workload reload で呼ぶ。

        各 workload は ``slug`` ではなく ``queue_table`` 名で配線する (= QueueRepository
        の API が queue_table 単位のため)。 queue_backend 属性が無いオブジェクトは
        'sqlite' 扱い (= primary)。
        """
        for w in workloads:
            backend = "secondary" if getattr(w, "queue_backend", "sqlite") == "mariadb" else "primary"
            self.set_backend(w.queue_table, backend)

    def enqueue(self, queue_table: str, pk: str, extra: dict[str, Any] | None = None) -> bool:
        """重複 pk は INSERT OR IGNORE で黙ってスキップ。1 件挿入できたら True。"""
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"INSERT OR IGNORE INTO {queue_table} (pk, extra) VALUES (:pk, :extra)",
                {"pk": str(pk), "extra": json.dumps(extra or {})},
            )
            return cur.rowcount == 1

    def enqueue_many(self, queue_table: str, items: list[tuple[str, dict[str, Any]]]) -> int:
        """bulk 投入。挿入件数 (重複除く) を返す。"""
        _validate_queue_table(queue_table)
        if not items:
            return 0
        inserted = 0
        with self._get_db(queue_table).transaction() as conn:
            for pk, extra in items:
                cur = conn.execute(
                    f"INSERT OR IGNORE INTO {queue_table} (pk, extra) VALUES (:pk, :extra)",
                    {"pk": str(pk), "extra": json.dumps(extra or {})},
                )
                if cur.rowcount == 1:
                    inserted += 1
        return inserted

    def claim(
        self,
        queue_table: str,
        worker_id: str,
        limit: int,
        lease_secs: int,
    ) -> list[ClaimedTask]:
        """state='pending' か、claim 期限切れの 'claimed' を limit 件取り、自分のものにする。"""
        _validate_queue_table(queue_table)
        if limit <= 0:
            return []
        now_iso = _utcnow_iso()
        lease_cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(1, lease_secs))
        ).isoformat(timespec="microseconds")

        with self._get_db(queue_table).transaction() as conn:
            conn.execute(
                f"""
                UPDATE {queue_table}
                SET state='claimed',
                    claimed_at=:now,
                    claimed_by=:wid,
                    updated_at=:now
                WHERE pk IN (
                    SELECT pk FROM {queue_table}
                    WHERE state='pending'
                       OR (state='claimed' AND claimed_at < :cutoff)
                    ORDER BY enqueued_at, pk
                    LIMIT :lim
                )
                """,
                {"now": now_iso, "wid": worker_id, "cutoff": lease_cutoff, "lim": int(limit)},
            )
            cur = conn.execute(
                f"""
                SELECT pk, attempt, extra, enqueued_at
                FROM {queue_table}
                WHERE state='claimed' AND claimed_by=:wid AND claimed_at=:now
                ORDER BY enqueued_at, pk
                """,
                {"wid": worker_id, "now": now_iso},
            )
            rows = cur.fetchall()

        return [
            ClaimedTask(
                pk=r["pk"],
                attempt=int(r["attempt"]),
                extra=json.loads(r["extra"]) if r["extra"] else {},
                enqueued_at=r["enqueued_at"],
            )
            for r in rows
        ]

    def complete(self, queue_table: str, pk: str) -> None:
        """成功時に row を DELETE。"""
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            conn.execute(f"DELETE FROM {queue_table} WHERE pk = :pk", {"pk": str(pk)})

    def fail(self, queue_table: str, pk: str, max_attempts: int, error: str | None) -> str:
        """失敗時: attempt+1。max 未満なら pending に戻す、max に達したら failed で残置。

        戻り値: 'pending'(retry 可) or 'failed'(打切り)。
        """
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"SELECT attempt FROM {queue_table} WHERE pk=:pk", {"pk": str(pk)}
            )
            row = cur.fetchone()
            if row is None:
                return "failed"  # 既に消えてる
            new_attempt = int(row["attempt"]) + 1
            now = _utcnow_iso()
            if new_attempt >= int(max_attempts):
                conn.execute(
                    f"""
                    UPDATE {queue_table}
                    SET state='failed', attempt=:a, last_error=:e,
                        claimed_at=NULL, claimed_by=NULL, updated_at=:now
                    WHERE pk=:pk
                    """,
                    {"a": new_attempt, "e": (error or "")[:4000], "pk": str(pk), "now": now},
                )
                return "failed"
            conn.execute(
                f"""
                UPDATE {queue_table}
                SET state='pending', attempt=:a, last_error=:e,
                    claimed_at=NULL, claimed_by=NULL, updated_at=:now
                WHERE pk=:pk
                """,
                {"a": new_attempt, "e": (error or "")[:4000], "pk": str(pk), "now": now},
            )
            return "pending"

    def count_by_state(self, queue_table: str) -> dict[str, int]:
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"SELECT state, COUNT(*) as c FROM {queue_table} GROUP BY state"
            )
            rows = cur.fetchall()
        return {r["state"]: int(r["c"]) for r in rows}

    def peek(self, queue_table: str, limit: int = 20) -> list[dict[str, Any]]:
        """admin 用: queue の中身を limit 件覗く。"""
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT pk, state, attempt, claimed_by, claimed_at,
                       enqueued_at, last_error, extra
                FROM {queue_table}
                ORDER BY enqueued_at DESC, pk
                LIMIT :lim
                """,
                {"lim": int(limit)},
            )
            return [
                {
                    "pk": r["pk"],
                    "state": r["state"],
                    "attempt": int(r["attempt"]),
                    "claimed_by": r["claimed_by"],
                    "claimed_at": r["claimed_at"],
                    "enqueued_at": r["enqueued_at"],
                    "last_error": r["last_error"],
                    "extra": json.loads(r["extra"]) if r["extra"] else {},
                }
                for r in cur.fetchall()
            ]
