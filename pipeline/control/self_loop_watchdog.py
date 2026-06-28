"""self_loop workload watchdog — idle dispatcher 自動 bootstrap.

plugin.yaml に `workload_type: self_loop` を宣言した workload は、
process() の最後で自分で次の tick を enqueue する設計。
ワーカー restart や process() 中の SIGTERM で「次の tick 投入前」に
死んだ場合、 queue が空になり永久に停止する (= dead lock)。

このウォッチドッグは control plane の lifespan で起動し、 60 秒ごとに
全 enabled workload を走査:
  1. plugin.yaml の workload_type が self_loop か確認
  2. queue が pending=0 かつ claimed=0 (= 完全 idle) なら
  3. 5 分間 idle 状態が続いていれば 1 件 bootstrap tick を投入

bootstrap 後は self-loop が復活して次の tick を自分で投入するので、
通常は 1 回の bootstrap で十分。

env:
  PIPELINE_SELFLOOP_WATCHDOG_DISABLE=1  → 無効化
  PIPELINE_SELFLOOP_WATCHDOG_INTERVAL_S=60  → 監視周期
  PIPELINE_SELFLOOP_WATCHDOG_IDLE_THRESHOLD_S=300  → idle と判定するまでの秒数
  PIPELINE_SELFLOOP_WATCHDOG_REBOOT_COOLDOWN_S=300  → 再 bootstrap 抑止 (= 同じ slug)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from pipeline.api.plugins_local import read_plugin_workload_type
from pipeline.repositories.queue import QueueRepository, _validate_queue_table
from pipeline.repositories.workloads import WorkloadRepository

log = logging.getLogger("pipeline.control.self_loop_watchdog")


class SelfLoopWatchdog:
    def __init__(self, db) -> None:
        self.db = db
        self.interval_s = int(os.environ.get(
            "PIPELINE_SELFLOOP_WATCHDOG_INTERVAL_S", "60"))
        self.idle_threshold_s = int(os.environ.get(
            "PIPELINE_SELFLOOP_WATCHDOG_IDLE_THRESHOLD_S", "300"))
        self.reboot_cooldown_s = int(os.environ.get(
            "PIPELINE_SELFLOOP_WATCHDOG_REBOOT_COOLDOWN_S", "300"))
        # slug → 最初に idle 検出した unix time
        self._first_idle_at: dict[str, float] = {}
        # slug → 最後に bootstrap した unix time
        self._last_bootstrap_at: dict[str, float] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if os.environ.get("PIPELINE_SELFLOOP_WATCHDOG_DISABLE", "").lower() in ("1", "true", "yes"):
            log.info("self_loop watchdog DISABLED via env")
            return
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="self_loop_watchdog")
        log.info("self_loop watchdog started (interval=%ds, idle_threshold=%ds, cooldown=%ds)",
                 self.interval_s, self.idle_threshold_s, self.reboot_cooldown_s)

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=3.0)
            except asyncio.TimeoutError:
                self._task.cancel()

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception:
                log.exception("self_loop watchdog tick raised")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        """1 周分のチェックを別スレッドで sync 実行 (DB は sync API)。"""
        await asyncio.to_thread(self._check_all)

    def _check_all(self) -> None:
        wrepo = WorkloadRepository(self.db)
        qrepo = QueueRepository(self.db)
        try:
            workloads = wrepo.list_all()
        except Exception:
            log.exception("workload list failed")
            return

        active_slugs = set()
        now = time.time()
        for w in workloads:
            if not w.enabled:
                continue
            sp = (w.executor_config or {}).get("source_path")
            wtype = read_plugin_workload_type(sp)
            if wtype != "self_loop":
                continue
            active_slugs.add(w.slug)

            # queue depth 確認
            try:
                _validate_queue_table(w.queue_table)
                depth = qrepo.count_by_state(w.queue_table)
            except Exception:
                log.debug("queue count_by_state failed for %s", w.slug, exc_info=False)
                continue

            pending = int(depth.get("pending", 0))
            claimed = int(depth.get("claimed", 0))

            if pending > 0 or claimed > 0:
                # 動いてる → idle カウンタをリセット
                self._first_idle_at.pop(w.slug, None)
                continue

            # idle 状態
            first_idle = self._first_idle_at.get(w.slug)
            if first_idle is None:
                self._first_idle_at[w.slug] = now
                continue
            idle_dur = now - first_idle
            if idle_dur < self.idle_threshold_s:
                continue

            # cooldown 確認 (= 直近 bootstrap から N 秒経過)
            last_boot = self._last_bootstrap_at.get(w.slug, 0)
            if now - last_boot < self.reboot_cooldown_s:
                continue

            # 自動 bootstrap
            pk = f"tick-watchdog-{int(now)}"
            try:
                inserted = qrepo.enqueue(w.queue_table, pk, {"watchdog": True})
            except Exception:
                log.exception("watchdog enqueue failed for %s", w.slug)
                continue
            self._last_bootstrap_at[w.slug] = now
            log.warning("self_loop watchdog: bootstrap %s (idle %.0fs, pk=%s, enqueued=%s)",
                        w.slug, idle_dur, pk, inserted)

        # 削除された workload の counter を掃除
        for slug in list(self._first_idle_at):
            if slug not in active_slugs:
                self._first_idle_at.pop(slug, None)
        for slug in list(self._last_bootstrap_at):
            if slug not in active_slugs:
                self._last_bootstrap_at.pop(slug, None)
