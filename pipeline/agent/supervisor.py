"""agent の中核: worker 子プロセスの spawn/kill/monitor + VRAM 実測ゲート + reconcile。

P1 の割り切り:
- 子 = 既存 `pipeline worker` の subprocess (無改修)。 workload は PIPELINE_WORKLOAD_FILTER env。
- crash は再 spawn せず検知ログのみ (= 決定①-B の予行、 再開始は次 tick の desired 判定に委ねる)。
- GPU workload は spawn 直前に nvidia-smi で実 VRAM を確認 (案A の on-demand ゲート)。
  in-flight spawn (子がまだモデル未ロード) の二重計上を避けるため GPU spawn は 1 tick 1 つに制限。
"""
from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass

from pipeline.agent.desired import Desired

log = logging.getLogger("pipeline.agent")


@dataclass
class Child:
    child_id: str
    workload: str
    gpu: bool
    proc: subprocess.Popen
    started_at: float
    terminating: bool = False


class AgentSupervisor:
    def __init__(
        self,
        *,
        host: str,
        pipeline_exe: str,
        control_url: str,
        gpu_index: str = "0",
        vram_safety_mult: float = 1.3,
        vram_floor_mb: int = 500,
        cache_root: str = "/tmp/pipeline-agent-cache",
    ) -> None:
        self.host = host
        self.pipeline_exe = pipeline_exe
        self.control_url = control_url
        self.gpu_index = gpu_index
        self.vram_safety_mult = vram_safety_mult
        self.vram_floor_mb = vram_floor_mb
        self.cache_root = cache_root
        self.children: dict[str, Child] = {}
        self._seq = 0

    # ---------------- VRAM 実測ゲート (案A on-demand) ----------------

    def _gpu_free_mb(self) -> int | None:
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=memory.free",
                 "--format=csv,noheader,nounits", "-i", self.gpu_index],
                timeout=5, text=True,
            )
            return int(out.strip().splitlines()[0].strip())
        except Exception as e:
            log.warning("[agent] nvidia-smi free query failed: %s", e)
            return None

    def _vram_room_for(self, vram_mb: int) -> bool:
        """spawn 直前の実 VRAM で「今 入るか」を判定。 不明なら False (OOM 回避で spawn しない)。"""
        free = self._gpu_free_mb()
        if free is None:
            return False
        need = int(vram_mb * self.vram_safety_mult) + self.vram_floor_mb
        ok = free >= need
        if not ok:
            log.info("[agent] VRAM gate: free=%dMB < need=%dMB (vram_mb=%d) → spawn 見送り",
                     free, need, vram_mb)
        return ok

    # ---------------- 子プロセス lifecycle ----------------

    def _spawn(self, workload: str, gpu: bool) -> str:
        self._seq += 1
        child_id = f"w_{self.host.replace('-', '_')}_a{self._seq}"
        cache_dir = f"{self.cache_root}-{child_id}"
        env = dict(os.environ)
        env["PIPELINE_WORKLOAD_FILTER"] = workload
        env["PIPELINE_PLUGIN_CACHE_DIR"] = cache_dir
        env["CUDA_VISIBLE_DEVICES"] = self.gpu_index if gpu else "-1"
        cmd = [
            self.pipeline_exe, "worker",
            "--control-url", self.control_url,
            "--hostname", self.host,
            "--worker-id", child_id,
            "--skip-pip-install",
            "--log-level", "INFO",
        ]
        proc = subprocess.Popen(cmd, env=env, start_new_session=True)
        self.children[child_id] = Child(child_id, workload, gpu, proc, time.monotonic())
        log.info("[agent] SPAWN child=%s workload=%s gpu=%s pid=%s", child_id, workload, gpu, proc.pid)
        return child_id

    def _signal(self, child_id: str, graceful: bool) -> None:
        c = self.children.get(child_id)
        if not c:
            return
        try:
            if graceful:
                c.proc.terminate()   # SIGTERM: 現タスク完走してから終了
            else:
                c.proc.kill()        # SIGKILL
        except Exception as e:
            log.debug("[agent] signal child=%s failed: %s", child_id, e)

    def _reap(self) -> None:
        """終了した子を回収。 crash (rc!=0 かつ非 terminating) を検知 (P1 は報告のみ)。"""
        dead: list[str] = []
        for cid, c in self.children.items():
            rc = c.proc.poll()
            if rc is None:
                continue
            dead.append(cid)
            if c.terminating:
                log.info("[agent] child=%s exited (graceful rc=%s)", cid, rc)
            elif rc == 0:
                log.info("[agent] child=%s exited rc=0 (self-exit)", cid)
            else:
                log.warning("[agent] CRASH child=%s workload=%s rc=%s "
                            "(P1: 再spawnせず次tickのdesired判定に委ねる)", cid, c.workload, rc)
        for cid in dead:
            self.children.pop(cid, None)

    def _active_by_workload(self) -> dict[str, list[Child]]:
        out: dict[str, list[Child]] = {}
        for c in self.children.values():
            if not c.terminating:
                out.setdefault(c.workload, []).append(c)
        return out

    # ---------------- reconcile (desired へ収束) ----------------

    def reconcile(self, desired: Desired) -> None:
        self._reap()
        active = self._active_by_workload()
        desired_slugs = {wd.slug for wd in desired.workloads}
        gpu_spawned_this_tick = False  # GPU は 1 tick 1 spawn (in-flight VRAM 二重計上回避)

        for wd in desired.workloads:
            cur = len(active.get(wd.slug, []))
            if cur < wd.count:
                need = wd.count - cur
                for _ in range(need):
                    if wd.gpu:
                        if gpu_spawned_this_tick:
                            break  # 次 tick で続き (前 spawn のロードが free に反映されてから)
                        if not self._vram_room_for(wd.vram_mb):
                            break
                        self._spawn(wd.slug, wd.gpu)
                        gpu_spawned_this_tick = True
                        break
                    else:
                        self._spawn(wd.slug, wd.gpu)  # CPU は制限なし
            elif cur > wd.count:
                # 超過 → graceful kill (新しい方 = 高 seq から)
                victims = sorted(active[wd.slug], key=lambda c: c.started_at, reverse=True)
                for c in victims[: cur - wd.count]:
                    c.terminating = True
                    self._signal(c.child_id, graceful=True)
                    log.info("[agent] DRAIN child=%s workload=%s (超過 %d>%d)",
                             c.child_id, wd.slug, cur, wd.count)

        # desired に無い workload の子は全部 graceful kill
        for slug, children in active.items():
            if slug not in desired_slugs:
                for c in children:
                    c.terminating = True
                    self._signal(c.child_id, graceful=True)
                    log.info("[agent] DRAIN child=%s workload=%s (desired 外)", c.child_id, slug)

    def status(self) -> dict:
        active = self._active_by_workload()
        return {
            "host": self.host,
            "children": len(self.children),
            "by_workload": {s: len(cs) for s, cs in active.items()},
            "terminating": sum(1 for c in self.children.values() if c.terminating),
        }

    def shutdown(self) -> None:
        """agent 終了時: 全子を kill (孤児防止)。 SIGTERM → 猶予 → SIGKILL。"""
        for cid in list(self.children):
            self._signal(cid, graceful=True)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and any(
            c.proc.poll() is None for c in self.children.values()
        ):
            time.sleep(0.5)
        for cid in list(self.children):
            c = self.children[cid]
            if c.proc.poll() is None:
                self._signal(cid, graceful=False)
        log.info("[agent] shutdown: all children signaled")
