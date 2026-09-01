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
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

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
    # SIGTERM を送った monotonic 時刻。 猶予超過で SIGKILL へ昇格する判定に使う。
    terminating_at: float | None = None


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
        drain_grace_s: float = 60.0,
        wedged_min_age_s: float = 180.0,
        wedged_no_success_s: float = 300.0,
        wedged_cooldown_s: float = 600.0,
        health_source: Callable[[str], Any] | None = None,
        health_forget: Callable[[str], None] | None = None,
        cache_root: str = "/tmp/pipeline-agent-cache",
    ) -> None:
        self.host = host
        self.pipeline_exe = pipeline_exe
        self.control_url = control_url
        self.gpu_index = gpu_index
        self.vram_safety_mult = vram_safety_mult
        self.vram_floor_mb = vram_floor_mb
        self.drain_grace_s = drain_grace_s
        self.wedged_min_age_s = wedged_min_age_s
        self.wedged_no_success_s = wedged_no_success_s
        self.wedged_cooldown_s = wedged_cooldown_s
        self.health_source = health_source
        self.health_forget = health_forget
        self.cache_root = cache_root
        self.children: dict[str, Child] = {}
        # wedged で殺した slug は暫く spawn しない (= kill→spawn→build失敗 の churn 防止)
        self._wedged_cooldown_until: dict[str, float] = {}
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

    def _gpu_total_mb(self) -> int | None:
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=memory.total",
                 "--format=csv,noheader,nounits", "-i", self.gpu_index],
                timeout=5, text=True,
            )
            return int(out.strip().splitlines()[0].strip())
        except Exception:
            return None

    def _disk_stats(self) -> tuple[int | None, int | None]:
        """ルート FS の (total_mb, free_mb)。 取得不能なら (None, None)。

        VRAM だけ報告していた時代は、 ローカル FS が満杯になっても制御プレーンから
        一切見えなかった。 worker は heartbeat を出し続けるので alive に見え、
        実際には ``tempfile.mkdtemp()`` が ENOENT で全 batch が落ちている
        (2026-09-02 ai-gpu3: / が 100% で 1 週間無音停止)。

        free は **非特権プロセスが実際に使える量** (f_bavail) を採る。 ext4 は既定で
        5% を root 予約に残すので f_bfree だと 「まだ空きがある」 と誤報告になる。
        """
        try:
            st = os.statvfs("/")
        except OSError as e:
            log.warning("[agent] statvfs('/') failed: %s", e)
            return (None, None)
        mb = st.f_frsize / (1024 * 1024)
        return (int(st.f_blocks * mb), int(st.f_bavail * mb))

    def children_status(self) -> list[dict]:
        """sync packet 用: 子一覧 (child_id, workload, gpu, alive)。"""
        out = []
        for c in self.children.values():
            out.append({
                "child_id": c.child_id, "workload": c.workload,
                "gpu": c.gpu, "alive": c.proc.poll() is None,
            })
        return out

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

    def _spawn(self, workload: str, gpu: bool, cvd: str | None = None) -> str:
        self._seq += 1
        child_id = f"w_{self.host.replace('-', '_')}_a{self._seq}"
        cache_dir = f"{self.cache_root}-{child_id}"
        env = dict(os.environ)
        env["PIPELINE_WORKLOAD_FILTER"] = workload
        env["PIPELINE_PLUGIN_CACHE_DIR"] = cache_dir
        # cvd 明示指定を最優先 (= CPU 実行だが GPU レーン資格が要る子: cvd="0")。
        # 未指定なら従来: gpu なら gpu_index、 CPU なら "-1" (= GPU 非搭載レーン)。
        if cvd is not None:
            env["CUDA_VISIBLE_DEVICES"] = cvd
        else:
            env["CUDA_VISIBLE_DEVICES"] = self.gpu_index if gpu else "-1"
        if gpu:
            # systemd gpu@ の drop-in (mps.conf / dynamo-disable.conf) 相当を子に付与。
            # MPS を経由させないと単一 GPU で複数子が排他 CUDA context を作り init が stuck する
            # (PoC 2026-07-06 で再現)。 既に env にあれば尊重、無ければ標準パスを補う。
            env.setdefault("CUDA_MPS_PIPE_DIRECTORY", "/tmp/nvidia-mps")
            env.setdefault("CUDA_MPS_LOG_DIRECTORY", "/tmp/nvidia-mps-log")
            env.setdefault("TORCHDYNAMO_DISABLE", "1")
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
                if c.terminating_at is None:
                    c.terminating_at = time.monotonic()
            else:
                # 子は start_new_session=True (= 自身が pgid リーダー) なので
                # プロセスグループごと殺す。 proc.kill() だと ONNX/CUDA が張った
                # 孫プロセスが取り残され VRAM を握ったままになる。
                try:
                    os.killpg(os.getpgid(c.proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    c.proc.kill()    # pgid 取得不可 (既に消滅等) は直下だけ
        except Exception as e:
            log.debug("[agent] signal child=%s failed: %s", child_id, e)

    def _reap(self) -> None:
        """終了した子を回収。 crash (rc!=0 かつ非 terminating) を検知 (P1 は報告のみ)。

        SIGTERM を猶予 (drain_grace_s) 内に無視した子は SIGKILL へ昇格する。
        worker は sleep / blocking I/O 中や executor build 失敗のリトライループ中は
        SIGTERM で死なず、 _active_by_workload() の台数からは消えるのに VRAM は
        握り続ける。 昇格が無いと free VRAM が単調減少し、 VRAM ゲートが全 GPU
        spawn を永久拒否する状態 (2026-08-01 の ai-gpu4/5: terminating 33/27 体)
        に固着して自己回復できない。
        """
        dead: list[str] = []
        now = time.monotonic()
        for cid, c in self.children.items():
            rc = c.proc.poll()
            if rc is None:
                if (c.terminating and c.terminating_at is not None
                        and now - c.terminating_at > self.drain_grace_s):
                    log.warning("[agent] KILL child=%s workload=%s "
                                "(SIGTERM を %.0fs 無視 → SIGKILL 昇格)",
                                cid, c.workload, now - c.terminating_at)
                    self._signal(cid, graceful=False)
                    # 再送抑止: D state 等で SIGKILL も効かない子への毎tick連打を防ぐ
                    c.terminating_at = now
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
            if self.health_forget is not None:
                try:
                    self.health_forget(cid)
                except Exception:
                    log.debug("[agent] health_forget failed for %s", cid)

    def cleanup_orphans(self) -> int:
        """起動時: 前世代 agent が crash して残した孤児子 worker を kill する。
        子は cmdline の `--worker-id w_{host}_a<N>` で識別 (agent 専用の命名)。
        孤児は管理外なので graceful を待たず SIGKILL (in-flight は lease 失効で回収)。"""
        marker = f"--worker-id w_{self.host.replace('-', '_')}_a"
        try:
            out = subprocess.check_output(["pgrep", "-af", "pipeline"], timeout=5, text=True)
        except subprocess.CalledProcessError:
            return 0  # 該当プロセスなし
        except Exception as e:
            log.warning("[agent] orphan pgrep failed: %s", e)
            return 0
        me = os.getpid()
        killed = 0
        for line in out.strip().splitlines():
            parts = line.split(None, 1)
            if len(parts) < 2 or marker not in parts[1]:
                continue
            try:
                pid = int(parts[0])
            except ValueError:
                continue
            if pid == me:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
                killed += 1
                log.warning("[agent] orphan child killed pid=%s (%s)", pid, parts[1][:100])
            except ProcessLookupError:
                pass
            except Exception as e:
                log.warning("[agent] orphan kill pid=%s failed: %s", pid, e)
        if killed:
            log.info("[agent] cleanup_orphans: %d 孤児を kill", killed)
        return killed

    # ---------------- wedged 判定 ----------------

    def _is_wedged(self, c: Child) -> bool:
        """「生きてはいるが仕事が成立していない子」を判定する。

        典型は VRAM 枯渇で executor build が通らない子。 プロセスは生きているので
        reconcile の台数カウントは満たされ、 一方で VRAM は握り続けるため、 その
        ホストの GPU spawn が VRAM ゲートで永久に弾かれる (2026-08-01 ai-gpu1:
        wedged 2 体が 15.7GB を保持し image-embed が 0 件成功のまま数時間)。

        判定材料は proxy が中継した run の結果のみ (worker 無改修):
          build error を出しており、 かつ一定時間 成功 run が 1 件も無い。
        起動直後 (モデルロード中) を誤検知しないよう min_age を置く。
        """
        if self.health_source is None:
            return False
        now = time.monotonic()
        if now - c.started_at < self.wedged_min_age_s:
            return False
        h = self.health_source(c.child_id)
        if h is None or getattr(h, "build_errors", 0) <= 0:
            return False  # build error を出していない = 単に暇なだけ (無罪)
        # 成功実績が無ければ起動時刻を基準にする (= 一度も通っていない子)
        ref = getattr(h, "last_success_at", None) or c.started_at
        return (now - ref) > self.wedged_no_success_s

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
            mine = active.get(wd.slug, [])
            # wedged は「台数に数えない」 かつ 「殺す」 の両方をやる。 片方だけだと
            # 「カウントから消えたのに VRAM は握ったまま」 という最悪の状態を自分で作る。
            wedged = [c for c in mine if self._is_wedged(c)]
            for c in wedged:
                log.warning("[agent] WEDGED child=%s workload=%s "
                            "(build error 継続・成功 run 無し → 置換; %.0fs は再spawnしない)",
                            c.child_id, c.workload, self.wedged_cooldown_s)
                c.terminating = True
                self._signal(c.child_id, graceful=True)  # 死ななければ drain_grace で SIGKILL
                self._wedged_cooldown_until[wd.slug] = time.monotonic() + self.wedged_cooldown_s
            healthy = [c for c in mine if c not in wedged]
            cur = len(healthy)
            # cooldown 中は spawn しない。 VRAM 状況が変わっていないのに詰め直すと
            # kill→spawn→build失敗 の churn になり、 上流キューを消費するだけ速くなる。
            # (超過 DRAIN は cooldown 中でも行う = 減らす方向は常に許可)
            cool_until = self._wedged_cooldown_until.get(wd.slug)
            cooling = cool_until is not None and time.monotonic() < cool_until
            if cur < wd.count and not cooling:
                need = wd.count - cur
                for _ in range(need):
                    if wd.gpu:
                        if gpu_spawned_this_tick:
                            break  # 次 tick で続き (前 spawn のロードが free に反映されてから)
                        if not self._vram_room_for(wd.vram_mb):
                            break
                        self._spawn(wd.slug, wd.gpu, wd.cvd)
                        gpu_spawned_this_tick = True
                        break
                    else:
                        self._spawn(wd.slug, wd.gpu, wd.cvd)  # CPU は制限なし
            elif cur > wd.count:
                # 超過 → graceful kill (新しい方 = 高 seq から)。 wedged は既に処理済み
                # なので健全な子だけを対象にする。
                victims = sorted(healthy, key=lambda c: c.started_at, reverse=True)
                for c in victims[: cur - wd.count]:
                    c.terminating = True
                    self._signal(c.child_id, graceful=True)
                    log.info("[agent] DRAIN child=%s workload=%s (超過 %d>%d)",
                             c.child_id, wd.slug, cur, wd.count)

        # desired に無い workload の子は全部 graceful kill。
        # ただし 「信用できない desired」 (= control plane と話せないまま bootstrap の
        # desired.json へ落ちた状態) では削除しない。 あのファイルは実運用 desired の
        # 部分集合でしかないので、 載っていない workload を 「desired 外」 と読むと
        # 稼働中の子を巻き添えに殺す。 増やす方向 (spawn / 超過 DRAIN) はそのまま。
        if not desired.authoritative:
            if active.keys() - desired_slugs:
                log.warning("[agent] desired が非信用 (sync 未確立) のため 「desired 外」 "
                            "削除を抑止: %s", sorted(active.keys() - desired_slugs))
            return
        for slug, children in active.items():
            if slug not in desired_slugs:
                for c in children:
                    c.terminating = True
                    self._signal(c.child_id, graceful=True)
                    log.info("[agent] DRAIN child=%s workload=%s (desired 外)", c.child_id, slug)

    def status(self) -> dict:
        active = self._active_by_workload()
        now = time.monotonic()
        cooling = {s: int(u - now) for s, u in self._wedged_cooldown_until.items() if u > now}
        out = {
            "host": self.host,
            "children": len(self.children),
            "by_workload": {s: len(cs) for s, cs in active.items()},
            "terminating": sum(1 for c in self.children.values() if c.terminating),
        }
        if cooling:
            out["wedged_cooldown_s"] = cooling
        return out

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
