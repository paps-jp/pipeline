"""In-process worker: drain loop を asyncio.Task として control server に同居させる。

設計メモ (design.md §7.2 と差分):
- MVP は in-process worker (control + worker 同一プロセス)。
- HTTP register / heartbeat は P2 で実装。worker_id は起動時に決定。
- executor.run は同期 subprocess なので asyncio.to_thread で外に逃がす。
- 1 iter で「enabled workload 全部 → 各 batch_size 件 claim → 並列に実行」する単純化版。
- workload ごとの host_affinity / resources / priority は MVP では無視 (将来の dispatcher へ)。

# executor instance cache

`python_module` のような重い executor (model load 数十秒) は workload 毎に 1 度だけ
build して、以降は同じインスタンスを再利用する。`_executor_cache` が (slug, config_hash)
を key にして executor を保持。config を編集 (workload PUT) すると hash が変わるので
古い executor は `close()` してから新規 build に切り替わる (rolling restart 風)。

shell executor のような stateless でも cache してよい (副作用は無い)。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pipeline.db.base import Database
from pipeline.executors import ExecutionContext, Executor, Task, create_executor
from pipeline.executors.base import ExecutionResult
from pipeline.repositories.queue import ClaimedTask, QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import WorkloadRepository

_SUPPORTED_EXECUTORS = {"shell", "python_module"}


def _config_hash(executor_type: str, config: dict[str, Any]) -> str:
    """executor_type + config から決定的なハッシュを得る。
    config 変更を検知するための version 印。short SHA256 prefix。
    """
    blob = json.dumps(
        {"t": executor_type, "c": config}, sort_keys=True, ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]

log = logging.getLogger("pipeline.worker.drain")


def _new_worker_id() -> str:
    return f"w_{os.getpid()}_{secrets.token_hex(2)}"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _eval_success(criteria: dict[str, Any], result: ExecutionResult) -> bool:
    """success_criteria の評価。

    - type=exit_code (default): result.exit_code == expected で判定。
      ただし exit_code=None (python_module / http_post 等 subprocess を持たない
      executor の場合) では、result.success フラグを尊重する。
    - type=output_json (将来): result.output_json の field を見る等。
    - 未知 type は result.success に従う (executor 自身の判断)。
    """
    typ = (criteria or {}).get("type", "exit_code")
    if typ == "exit_code":
        if result.exit_code is None:
            return result.success
        expected = (criteria or {}).get("expected", 0)
        return result.exit_code == expected
    return result.success


class Worker:
    """単一プロセスの drain loop。

    開始: `await worker.start()`
    停止: `await worker.stop()`  (Ctrl-C / FastAPI lifespan の cleanup から)
    """

    def __init__(
        self,
        db: Database,
        *,
        secondary_db: Database | None = None,
        worker_id: str | None = None,
        idle_sleep_s: float = 1.0,
        log_workdir_keep: bool = False,
    ) -> None:
        self.db = db
        self.worker_id = worker_id or _new_worker_id()
        self.idle_sleep_s = idle_sleep_s
        self._log_workdir_keep = log_workdir_keep
        self._stop_evt = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._workloads = WorkloadRepository(db)
        # secondary_db (= MariaDB) 指定時は業務 queue を backend 切替可能に。
        # 未指定なら SQLite-only (= 後方互換)。 配線は _wire_queue_backends で。
        self._queue = QueueRepository(db, secondary_db)
        self._runs = RunsRepository(db)
        # executor instance cache: slug -> (config_hash, executor)
        # config が変わると hash が変わって自動で再構築される。
        self._executor_cache: dict[str, tuple[str, Executor]] = {}
        # 観測用カウンタ (将来の /workers エンドポイント用)
        self.processed_total = 0
        self.success_total = 0
        self.failure_total = 0

    async def start(self) -> None:
        if self._task is not None:
            return
        log.info("worker %s starting drain loop", self.worker_id)
        self._task = asyncio.create_task(self._run(), name=f"worker.{self.worker_id}")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_evt.set()
        try:
            await asyncio.wait_for(self._task, timeout=10)
        except asyncio.TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        # cached executor の plugin cleanup を呼ぶ
        self._invalidate_all_executors()
        log.info("worker %s stopped", self.worker_id)

    def _close_executor(self, ex: Executor) -> None:
        """executor が close() を持ってれば呼ぶ。state を解放。"""
        close = getattr(ex, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                log.exception("executor close() raised; ignored")

    def _invalidate_all_executors(self) -> None:
        for slug, (_h, ex) in list(self._executor_cache.items()):
            self._close_executor(ex)
        self._executor_cache.clear()

    def _get_or_build_executor(self, w: Any) -> Executor:
        """workload に対する executor を 1 度だけ build して cache。
        config が変わってたら旧 executor を close → 新規 build。
        """
        wanted_hash = _config_hash(w.executor_type, w.executor_config)
        cached = self._executor_cache.get(w.slug)
        if cached is not None and cached[0] == wanted_hash:
            return cached[1]
        if cached is not None:
            log.info("executor config changed for %s; rebuilding (%s → %s)",
                     w.slug, cached[0], wanted_hash)
            self._close_executor(cached[1])
        ex = create_executor(w.executor_type, w.executor_config)
        self._executor_cache[w.slug] = (wanted_hash, ex)
        return ex

    # ---------------- internal ----------------

    def _wire_queue_backends(self) -> None:
        """workloads.queue_backend に従い QueueRepository の backend を配線。

        secondary_db 未指定 (= SQLite-only) なら何もしない (= 余計な query もしない)。
        dynamic: 毎 iteration 呼ぶので Session 2 で queue_backend を 'mariadb' に
        切替えると次 tick から MariaDB 経路に乗る (= プロセス再起動不要)。
        """
        if self._queue.secondary_db is None:
            return
        self._queue.wire_from_workloads(self._workloads.list_all())

    async def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self._wire_queue_backends()
                did = await self._drain_once()
            except Exception:
                log.exception("worker %s drain iteration failed", self.worker_id)
                did = False
            if not did:
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=self.idle_sleep_s)
                except asyncio.TimeoutError:
                    pass

    async def _drain_once(self) -> bool:
        """1 イテレーション。何か 1 件でも処理したら True。"""
        workloads = self._workloads.list_all()
        any_work = False
        seen_slugs: set[str] = set()
        for w in workloads:
            seen_slugs.add(w.slug)
            if not w.enabled:
                # disable された workload の executor を解放
                self._evict_if_cached(w.slug)
                continue
            if w.executor_type not in _SUPPORTED_EXECUTORS:
                log.debug("workload %s executor_type=%s not supported yet; skip",
                          w.slug, w.executor_type)
                continue
            tasks = self._queue.claim(
                w.queue_table,
                worker_id=self.worker_id,
                limit=w.batch_size,
                lease_secs=w.lease_secs,
            )
            if not tasks:
                continue
            any_work = True
            try:
                executor = self._get_or_build_executor(w)
            except Exception as e:
                # config が壊れてる / plugin import 失敗 / setup() で例外
                # → 全タスク即 fail (max_attempts 関係なく 1 回扱い)
                log.warning("workload %s executor build failed: %s", w.slug, e)
                self._evict_if_cached(w.slug)
                for t in tasks:
                    self._queue.fail(w.queue_table, t.pk, max_attempts=1, error=str(e))
                    self._runs.record(
                        workload_slug=w.slug,
                        pk=t.pk,
                        worker_id=self.worker_id,
                        attempt=t.attempt,
                        started_at=_utcnow_iso(),
                        success=False,
                        exit_code=None,
                        duration_ms=0,
                        stdout=None,
                        stderr=None,
                        output_json=None,
                        error=f"executor build error: {e}",
                    )
                continue
            # 同一 workload 内は逐次 (batch_size 多くなったら gather 化検討)
            for t in tasks:
                await self._execute_one(w, executor, t)
        # 削除された workload の executor を解放
        for slug in list(self._executor_cache):
            if slug not in seen_slugs:
                self._evict_if_cached(slug)
        return any_work

    def _evict_if_cached(self, slug: str) -> None:
        cached = self._executor_cache.pop(slug, None)
        if cached is not None:
            log.info("evicting cached executor for %s", slug)
            self._close_executor(cached[1])

    async def _execute_one(self, w: Any, executor: Any, t: ClaimedTask) -> None:
        started = _utcnow_iso()
        # task workdir (一時ディレクトリ)
        workdir = Path(tempfile.mkdtemp(prefix=f"pipeline-{w.slug}-"))
        deadline = datetime.now(timezone.utc) + timedelta(seconds=int(w.lease_secs))
        ctx = ExecutionContext(
            deadline=deadline,
            workdir=workdir,
            env=dict(os.environ),
            workload_config=w.executor_config,
        )
        task = Task(pk=t.pk, workload_slug=w.slug, attempt=t.attempt, extra=t.extra)

        try:
            # 同期 subprocess を別 thread で走らせる
            result: ExecutionResult = await asyncio.to_thread(executor.run, task, ctx)
        except Exception as e:
            result = ExecutionResult(success=False, error=f"executor raised: {e!s}")
            log.exception("workload %s task %s executor raised", w.slug, t.pk)
        finally:
            if not self._log_workdir_keep:
                self._rmtree_quiet(workdir)

        is_success = _eval_success(w.success_criteria, result)
        self.processed_total += 1
        if is_success:
            self.success_total += 1
            self._queue.complete(w.queue_table, t.pk)
        else:
            self.failure_total += 1
            self._queue.fail(
                w.queue_table, t.pk, max_attempts=w.max_attempts, error=(result.error or result.stderr or "non-zero exit")[:4000],
            )

        self._runs.record(
            workload_slug=w.slug,
            pk=t.pk,
            worker_id=self.worker_id,
            attempt=t.attempt,
            started_at=started,
            success=is_success,
            exit_code=result.exit_code,
            duration_ms=result.duration_ms,
            stdout=result.stdout or None,
            stderr=result.stderr or None,
            output_json=result.output_json,
            error=result.error,
        )

    @staticmethod
    def _rmtree_quiet(p: Path) -> None:
        import shutil

        try:
            shutil.rmtree(p, ignore_errors=True)
        except Exception:
            pass
