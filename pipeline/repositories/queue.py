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
import logging
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from pipeline.db.base import Database

log = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _validate_queue_table(name: str) -> None:
    """インジェクション防止: 呼出側の slug 由来テーブル名を簡易チェック。"""
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"invalid queue table name: {name!r}")


# 1 文 に埋める bind 変数の上限。 SQLite の SQLITE_MAX_VARIABLE_NUMBER は既定 999
# (3.32 以降は 32766 だがビルド依存)、 MariaDB も max_prepared_stmt_count がある。
# batch_size は最大 10000 を許すので、 IN (...) は必ずこの単位に刻む。
_IN_CHUNK = 400


def _chunks(items: list[Any], size: int = _IN_CHUNK) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


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

    def enqueue_many_strict(
        self, queue_table: str, items: list[tuple[str, dict[str, Any]]]
    ) -> dict[str, int]:
        """bulk 投入の strict 版。INSERT OR IGNORE を使わず plain INSERT する。

        pk の一意性は呼出側 (= source を CAS claim する dispatcher) が保証する前提。
        既存 pk と衝突した場合は黙ってスキップ (IGNORE) せず WARN ログに残し collided として
        数える (= 取りこぼしを不可視にしない)。1 行の衝突で batch 全体を巻き戻さないよう
        行単位で握る (duplicate-key は statement レベルエラーで txn は継続可能)。
        戻り値: {"inserted": n, "collided": m}。
        """
        _validate_queue_table(queue_table)
        if not items:
            return {"inserted": 0, "collided": 0}
        inserted = 0
        collided = 0
        with self._get_db(queue_table).transaction() as conn:
            for pk, extra in items:
                try:
                    conn.execute(
                        f"INSERT INTO {queue_table} (pk, extra) VALUES (:pk, :extra)",
                        {"pk": str(pk), "extra": json.dumps(extra or {})},
                    )
                    inserted += 1
                except Exception as e:
                    collided += 1
                    log.warning("strict enqueue collision %s pk=%s: %s", queue_table, pk, e)
        return {"inserted": inserted, "collided": collided}

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

        # MariaDB (secondary) backend は 2 段 SELECT ... FOR UPDATE SKIP LOCKED で claim する。
        # 旧実装の UPDATE-WHERE-pk-IN-(SELECT ... ORDER BY LIMIT) は並行 worker が同一候補行
        # (ORDER BY enqueued_at の先頭) を奪い合い Deadlock(1213) を頻発させる (= 並行 claim が
        # 増えると claim が 500 で失敗しスループット天井になる)。 SKIP LOCKED で互いに素な行を
        # 掴めば deadlock が構造的に消える (dispatcher._claim_pending_videos と同じパターン)。
        # SQLite は SKIP LOCKED 非対応なので従来の CAS 風 UPDATE-WHERE を維持する。
        secondary = (
            self._backend_map.get(queue_table) == "secondary"
            and self.secondary_db is not None
        )
        # 空振り claim から書き込みを取り除く (2026-08-07 ボトルネック調査)。
        # SQLite 経路は候補 0 件でも UPDATE を必ず発行していたため、 task が 1 件も無い
        # fleet でも空振り claim が 178 回/秒 発生し、 control plane の直列区間の 72% を
        # 消費していた (perf/control-plane-bottleneck.md §3)。 先に候補の有無だけを読み、
        # 無ければ書き込みを発行せず返す。 候補がある時に増えるのは SELECT 1 本だけで、
        # UPDATE は従来通り 1 回なので二重取得は起きない。
        # secondary (MariaDB) 経路は元々 SELECT ... FOR UPDATE が先頭にあり空振りでも
        # 書き込まないので、 往復を増やさないようこの短絡は通さない。
        if not secondary:
            with self._get_db(queue_table).transaction() as conn:
                probe = conn.execute(
                    f"""
                    SELECT 1 AS x FROM {queue_table}
                    WHERE state='pending'
                       OR (state='claimed' AND claimed_at < :cutoff)
                    LIMIT 1
                    """,
                    {"cutoff": lease_cutoff},
                )
                if probe.fetchone() is None:
                    return []

        with self._get_db(queue_table).transaction() as conn:
            if secondary:
                sel = conn.execute(
                    f"""
                    SELECT pk FROM {queue_table}
                    WHERE state='pending'
                       OR (state='claimed' AND claimed_at < :cutoff)
                    ORDER BY enqueued_at, pk
                    LIMIT :lim
                    FOR UPDATE SKIP LOCKED
                    """,
                    {"cutoff": lease_cutoff, "lim": int(limit)},
                )
                pks = [r["pk"] for r in sel.fetchall()]
                if not pks:
                    return []
                ph = ", ".join(f":p{i}" for i in range(len(pks)))
                pk_params = {f"p{i}": pk for i, pk in enumerate(pks)}
                conn.execute(
                    f"""
                    UPDATE {queue_table}
                    SET state='claimed',
                        claimed_at=:now,
                        claimed_by=:wid,
                        updated_at=:now
                    WHERE pk IN ({ph})
                    """,
                    {"now": now_iso, "wid": worker_id, **pk_params},
                )
                cur = conn.execute(
                    f"""
                    SELECT pk, attempt, extra, enqueued_at
                    FROM {queue_table}
                    WHERE pk IN ({ph})
                    ORDER BY enqueued_at, pk
                    """,
                    pk_params,
                )
                rows = cur.fetchall()
            else:
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

    def complete_many(self, queue_table: str, pks: list[str]) -> int:
        """成功した pk 群を 1 transaction でまとめて DELETE。 戻り値 = 削除行数。

        1 件ずつ complete() を呼ぶと pk の数だけ transaction が張られる。 SQLite は
        単一接続を RLock で直列化しているので、 これがそのまま control plane の
        直列区間になっていた (perf/control-plane-bottleneck.md §2)。
        bind 変数上限があるので IN (...) は _IN_CHUNK 単位に刻むが、 transaction は
        1 本にまとめる (= chunk 境界で部分 commit しない)。
        """
        _validate_queue_table(queue_table)
        if not pks:
            return 0
        deleted = 0
        with self._get_db(queue_table).transaction() as conn:
            for chunk in _chunks([str(p) for p in pks]):
                ph = ", ".join(f":p{i}" for i in range(len(chunk)))
                cur = conn.execute(
                    f"DELETE FROM {queue_table} WHERE pk IN ({ph})",
                    {f"p{i}": pk for i, pk in enumerate(chunk)},
                )
                deleted += int(cur.rowcount or 0)
        return deleted

    def fail_many(
        self, queue_table: str, items: list[tuple[str, str | None]], max_attempts: int
    ) -> dict[str, str]:
        """失敗した (pk, error) 群を 1 transaction で処理。 fail() の bulk 版。

        戻り値 = {pk: 'pending' (retry 可) or 'failed' (打切り)}。 既に queue から
        消えている pk は 'failed' 扱い (= fail() と同じ)。 attempt の判定は行ごとに
        必要なので SELECT は pk 数だけ走るが、 transaction は 1 本に畳む。
        """
        _validate_queue_table(queue_table)
        if not items:
            return {}
        out: dict[str, str] = {}
        now = _utcnow_iso()
        with self._get_db(queue_table).transaction() as conn:
            for pk, error in items:
                cur = conn.execute(
                    f"SELECT attempt FROM {queue_table} WHERE pk=:pk", {"pk": str(pk)}
                )
                row = cur.fetchone()
                if row is None:
                    out[pk] = "failed"   # 既に消えてる
                    continue
                new_attempt = int(row["attempt"]) + 1
                terminal = new_attempt >= int(max_attempts)
                conn.execute(
                    f"""
                    UPDATE {queue_table}
                    SET state=:st, attempt=:a, last_error=:e,
                        claimed_at=NULL, claimed_by=NULL, updated_at=:now
                    WHERE pk=:pk
                    """,
                    {
                        "st": "failed" if terminal else "pending",
                        "a": new_attempt,
                        "e": (error or "")[:4000],
                        "pk": str(pk),
                        "now": now,
                    },
                )
                out[pk] = "failed" if terminal else "pending"
        return out

    def claimable_tables(
        self,
        specs: list[tuple[str, int]],
        include_expired: bool = True,
        fail_open: bool = True,
    ) -> set[str]:
        """(queue_table, lease_secs) の列を受け、 claim 候補が 1 件以上ある表名を返す。

        include_expired=False なら state='pending' だけを見る (= lease 切れの claimed を
        数えない)。 「pending があるか」 だけを知りたい呼出側 (= higher-pending の
        preemption 判定) が、 従来の count_by_state と同じ意味論を保つために使う。

        `GET /workers/{id}/workloads` が 「pending が無い workload まで offer する」 ため、
        worker は 1 cycle ごとに workload 数だけ空振り claim を撃っていた。 その判定を
        ここで一括に行う。 表ごとに count_by_state を呼ぶ (= 表の数だけ transaction) のを
        避けるため、 **backend ごとに transaction 1 本** にまとめて EXISTS だけ問い合わせる。

        fail_open=True (既定): 表が無い等で問い合わせに失敗した表は 「候補あり」 として返す。
        判定に失敗したせいで workload が offer から消える (= fleet が止まる) 方が
        空振り claim より遥かに悪いため。 逆に 「候補ありと誤判定すると困る」 呼出側
        (= preemption 判定。 誤検知すると worker が現 workload を無限に手放す) は
        fail_open=False を渡して従来の 「失敗した表は skip」 に倒す。
        """
        if not specs:
            return set()
        # backend (= 実 DB instance) ごとにまとめる。 id() ではなく _get_db の戻りで束ねる。
        by_db: dict[int, tuple[Any, list[tuple[str, int]]]] = {}
        for queue_table, lease_secs in specs:
            _validate_queue_table(queue_table)
            db = self._get_db(queue_table)
            by_db.setdefault(id(db), (db, []))[1].append((queue_table, lease_secs))

        out: set[str] = set()
        now = datetime.now(timezone.utc)
        for db, group in by_db.values():
            try:
                with db.transaction() as conn:
                    for queue_table, lease_secs in group:
                        cutoff = (
                            now - timedelta(seconds=max(1, lease_secs))
                        ).isoformat(timespec="microseconds")
                        try:
                            if include_expired:
                                cur = conn.execute(
                                    f"""
                                    SELECT 1 AS x FROM {queue_table}
                                    WHERE state='pending'
                                       OR (state='claimed' AND claimed_at < :cutoff)
                                    LIMIT 1
                                    """,
                                    {"cutoff": cutoff},
                                )
                            else:
                                cur = conn.execute(
                                    f"SELECT 1 AS x FROM {queue_table} "
                                    f"WHERE state='pending' LIMIT 1"
                                )
                            if cur.fetchone() is not None:
                                out.add(queue_table)
                        except Exception:
                            log.debug("claimable probe failed for %s (fail_open=%s)",
                                      queue_table, fail_open)
                            if fail_open:
                                out.add(queue_table)
            except Exception:
                # transaction そのものが張れない = backend 障害。 group 全体を fail-open。
                log.warning("claimable probe transaction failed for %d tables (fail_open=%s)",
                            len(group), fail_open)
                if fail_open:
                    out.update(t for t, _ in group)
        return out

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

    def reset_failed(self, queue_table: str) -> int:
        """failed 状態のタスクを pending に戻す (attempt=0 にリセットして再試行可能化)。

        一時障害 (worker 側 build error / OOM / 上流の一過性エラー等) で max_attempts に
        達し terminal 化した failed を回収する。 supervisor の autoreset が周期呼び出す。
        戻り値 = pending に戻した件数。
        """
        _validate_queue_table(queue_table)
        now = datetime.now(timezone.utc).isoformat()
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"""
                UPDATE {queue_table}
                SET state='pending', attempt=0, last_error=NULL,
                    claimed_by=NULL, claimed_at=NULL, updated_at=:now
                WHERE state='failed'
                """,
                {"now": now},
            )
            return int(cur.rowcount)

    def peek(self, queue_table: str, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        """admin 用: queue の中身を limit 件覗く。offset でページネーション可。"""
        _validate_queue_table(queue_table)
        with self._get_db(queue_table).transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT pk, state, attempt, claimed_by, claimed_at,
                       enqueued_at, last_error, extra
                FROM {queue_table}
                ORDER BY enqueued_at DESC, pk
                LIMIT :lim OFFSET :off
                """,
                {"lim": int(limit), "off": int(offset)},
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
