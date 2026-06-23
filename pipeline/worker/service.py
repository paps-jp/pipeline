"""Worker daemon: control plane に HTTP 接続して claim/execute する独立プロセス.

ローカル `Worker` (control plane 同居) と違って、SQLite を直接触らず
すべて `/api/v1/workers/{id}/...` 経由で通信する。リモートホストで動かす想定。

CLI:
    pipeline worker \\
        --control-url http://10.10.50.7:8001 \\
        [--hostname myhost] \\
        [--worker-id w_myhost_abcd] \\
        [--token <auth>] \\
        [--idle-sleep 1.0] \\
        [--cache-dir ~/.pipeline/plugins] \\
        [--pip-index-url https://pypi.org/simple] \\
        [--skip-pip-install]

シャットダウン (Ctrl-C / SIGTERM):
    1. heartbeat 停止
    2. 進行中 task を最後まで完走させる (graceful)
    3. plugin cleanup() 呼出
    4. DELETE /api/v1/workers/{id} で deregister
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import socket
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from pipeline.executors import (
    ExecutionContext,
    ExecutionResult,
    Executor,
    Task,
    create_executor,
)
from pipeline.worker.admin_executor import execute_admin_cmd as _execute_admin_cmd
import subprocess as _subprocess


import re as _re

# OOM 検知パターン: PyTorch / CUDA / cupy / general allocator メッセージを広めに拾う。
# 過剰に拾うと false-positive (= 別原因の例外で peak が無闇に上がる) になるので、
# 明確に「メモリ不足」 を示す表現に限定。
_OOM_RE = _re.compile(
    r"(out of memory|OutOfMemoryError|cudaErrorMemoryAllocation|"
    r"CUDA out of memory|HIP out of memory|memory allocation failed|"
    r"cannot allocate.*\d+.*(MB|GiB|bytes))",
    _re.I,
)


def _looks_like_oom(*messages: str) -> bool:
    return any(m and _OOM_RE.search(m) for m in messages)


def _measure_self_vram_mb(pid: int) -> int | None:
    """自プロセス (`pid`) が GPU 上で使ってる VRAM (MB) を nvidia-smi で取得。
    複数 GPU に跨る場合は合算。 取得失敗時は None。
    """
    try:
        p = _subprocess.run(
            ["nvidia-smi", "--query-compute-apps=pid,used_memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=4,
        )
        if p.returncode != 0:
            return None
    except (FileNotFoundError, _subprocess.TimeoutExpired, Exception):
        return None
    total = 0
    found = False
    for line in p.stdout.strip().splitlines():
        parts = [s.strip() for s in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            row_pid = int(parts[0])
            mb = int(parts[1])
        except ValueError:
            continue
        if row_pid == pid:
            total += mb
            found = True
    return total if found else None


def _nvidia_smi_gpus() -> list[dict[str, Any]]:
    """nvidia-smi で GPU 温度/使用率/メモリ/電力等を取得。 GPU 無し / コマンド無し時は空 list。"""
    try:
        p = _subprocess.run(
            ["nvidia-smi",
             "--query-gpu=index,temperature.gpu,utilization.gpu,utilization.memory,"
             "memory.used,memory.total,power.draw,clocks.current.sm,clocks.current.memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=4,
        )
        if p.returncode != 0:
            return []
    except (FileNotFoundError, _subprocess.TimeoutExpired, Exception):
        return []

    def _f(s: str) -> float | None:
        s = s.strip()
        if not s or s.lower() in ("[not supported]", "n/a"):
            return None
        try:
            return float(s)
        except ValueError:
            return None

    def _i(s: str) -> int | None:
        v = _f(s)
        return int(v) if v is not None else None

    out: list[dict[str, Any]] = []
    for line in p.stdout.strip().splitlines():
        parts = [s.strip() for s in line.split(",")]
        if len(parts) < 4:
            continue
        try:
            idx = int(parts[0])
        except (ValueError, IndexError):
            continue
        row: dict[str, Any] = {
            "gpu_idx": idx,
            "temp_c": _f(parts[1]) if len(parts) > 1 else None,
            "util_pct": _f(parts[2]) if len(parts) > 2 else None,
            "mem_util_pct": _f(parts[3]) if len(parts) > 3 else None,
            "mem_used_mb": _i(parts[4]) if len(parts) > 4 else None,
            "mem_total_mb": _i(parts[5]) if len(parts) > 5 else None,
            "power_w": _f(parts[6]) if len(parts) > 6 else None,
            "sm_clock_mhz": _i(parts[7]) if len(parts) > 7 else None,
            "mem_clock_mhz": _i(parts[8]) if len(parts) > 8 else None,
        }
        out.append(row)
    return out

log = logging.getLogger("pipeline.worker.daemon")

HEARTBEAT_INTERVAL_S = 5.0
DEFAULT_IDLE_SLEEP_S = 1.0


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _config_hash(executor_type: str, config: dict[str, Any]) -> str:
    blob = json.dumps(
        {"t": executor_type, "c": config}, sort_keys=True, ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]


def _eval_success(criteria: dict[str, Any], result: ExecutionResult) -> bool:
    typ = (criteria or {}).get("type", "exit_code")
    if typ == "exit_code":
        if result.exit_code is None:
            return result.success
        expected = (criteria or {}).get("expected", 0)
        return result.exit_code == expected
    return result.success


class ControlClient:
    """control plane HTTP API の薄いラッパ."""

    def __init__(self, control_url: str, *, token: str | None = None, timeout_s: float = 30.0) -> None:
        self.base = control_url.rstrip("/")
        self._headers = {"Content-Type": "application/json"}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.AsyncClient(timeout=timeout_s, headers=self._headers)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def register(self, payload: dict) -> dict:
        r = await self._client.post(f"{self.base}/api/v1/workers", json=payload)
        r.raise_for_status()
        return r.json()

    async def heartbeat(self, worker_id: str, payload: dict) -> None:
        r = await self._client.put(
            f"{self.base}/api/v1/workers/{worker_id}/heartbeat", json=payload
        )
        r.raise_for_status()

    async def deregister(self, worker_id: str) -> None:
        try:
            await self._client.delete(f"{self.base}/api/v1/workers/{worker_id}")
        except Exception:
            log.exception("deregister failed; ignored")

    async def list_workloads(self, worker_id: str) -> list[dict]:
        r = await self._client.get(f"{self.base}/api/v1/workers/{worker_id}/workloads")
        r.raise_for_status()
        return r.json().get("workloads", [])

    async def peek_higher_pending(self, worker_id: str, priority: int) -> bool:
        """Lv2 preemption: priority > `priority` の workload に pending あるか。"""
        try:
            r = await self._client.get(
                f"{self.base}/api/v1/workers/{worker_id}/higher-pending",
                params={"than": int(priority)},
                timeout=3.0,
            )
            r.raise_for_status()
            return bool(r.json().get("has_pending"))
        except Exception:
            return False  # 失敗時は preempt 無しで続行 (= 安全側)

    async def claim(self, worker_id: str, workload_slug: str, limit: int) -> list[dict]:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/claim",
            json={"workload_slug": workload_slug, "limit": limit},
        )
        r.raise_for_status()
        return r.json().get("tasks", [])

    async def complete(self, worker_id: str, workload_slug: str, pks: list[str]) -> None:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/complete",
            json={"workload_slug": workload_slug, "pks": pks},
        )
        r.raise_for_status()

    async def fail(self, worker_id: str, workload_slug: str, pk: str, error: str | None) -> None:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/fail",
            json={"workload_slug": workload_slug, "pk": pk, "error": error},
        )
        r.raise_for_status()

    async def start_run(self, worker_id: str, payload: dict) -> str | None:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/runs/start", json=payload
        )
        r.raise_for_status()
        return r.json().get("id")

    async def finish_run(self, worker_id: str, run_id: str, payload: dict) -> None:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/runs/{run_id}/finish", json=payload
        )
        r.raise_for_status()

    async def record_run(self, worker_id: str, payload: dict) -> None:
        r = await self._client.post(
            f"{self.base}/api/v1/workers/{worker_id}/runs", json=payload
        )
        r.raise_for_status()

    async def poll_admin_cmd(self, worker_id: str, host: str) -> dict | None:
        """long poll: pending admin cmd を待つ (= 最大 25s)、 無ければ None."""
        try:
            r = await self._client.get(
                f"{self.base}/api/v1/workers/{worker_id}/admin-cmd?host={host}",
                timeout=30,
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            log.warning("poll_admin_cmd failed", exc_info=False)
            return None

    async def post_vram_observation(self, slug: str, used_mb: int, worker_id: str | None) -> None:
        """自プロセスの VRAM 占有を workload に報告。 control plane が peak を平滑化保存。"""
        try:
            await self._client.post(
                f"{self.base}/api/v1/workloads/{slug}/vram_observation",
                json={"used_mb": int(used_mb), "worker_id": worker_id},
            )
        except Exception:
            log.warning("post_vram_observation failed slug=%s", slug, exc_info=False)

    async def complete_admin_cmd(self, worker_id: str, cmd_id: int,
                                 success: bool, exit_code: int | None = None,
                                 stdout: str | None = None, stderr: str | None = None,
                                 error: str | None = None) -> None:
        try:
            await self._client.post(
                f"{self.base}/api/v1/workers/{worker_id}/admin-cmd/{cmd_id}/complete",
                json={"success": success, "exit_code": exit_code,
                      "stdout": stdout, "stderr": stderr, "error": error},
            )
        except Exception:
            log.exception("complete_admin_cmd failed (cmd_id=%s)", cmd_id)


class WorkerDaemon:
    def __init__(
        self,
        *,
        control_url: str,
        hostname: str | None = None,
        worker_id: str | None = None,
        token: str | None = None,
        idle_sleep_s: float = DEFAULT_IDLE_SLEEP_S,
    ) -> None:
        self.control_url = control_url
        base_host = hostname or socket.gethostname()
        # 同一 GPU に複数 worker daemon を立てる時、 systemd template が
        # `PIPELINE_WORKER_INSTANCE=%i` を渡してくる。 hostname に suffix を付けて
        # worker registry / state key 衝突を回避する。
        inst = os.environ.get("PIPELINE_WORKER_INSTANCE", "").strip()
        if inst and not base_host.endswith(f"-{inst}"):
            self.hostname = f"{base_host}-{inst}"
        else:
            self.hostname = base_host
        # plugin から `os.environ["PIPELINE_WORKER_HOSTNAME"]` で同じ値を見られるようにする
        os.environ["PIPELINE_WORKER_HOSTNAME"] = self.hostname
        self.worker_id_hint = worker_id
        self.worker_id: str | None = None
        self.idle_sleep_s = idle_sleep_s
        self._client = ControlClient(control_url, token=token)
        self._stop_evt = asyncio.Event()
        self._executor_cache: dict[str, tuple[str, Executor]] = {}
        self.processed_total = 0
        self.success_total = 0
        self.failure_total = 0
        self._pending_rows = 0
        self._pending_errs = 0
        # 現在処理中の workload slug (= heartbeat に乗せて Workers UI に出す)。
        # _drain_once でタスク実行直前にセット、 完了後に None に戻す。
        self._current_workload: str | None = None
        # workload ごとの自プロセス VRAM 占有 peak と、 直近 report 時刻 (rate limit 用)
        self._vram_peaks: dict[str, int] = {}
        self._vram_last_report: dict[str, float] = {}

    async def run(self) -> int:
        """blocking entry — register → loops → deregister."""
        self._install_signals()
        try:
            registered = await self._client.register({
                "host": self.hostname,
                "pid": os.getpid(),
                "tags": [],
                "resources": {},
                "worker_id": self.worker_id_hint,
            })
        except Exception:
            log.exception("register failed; cannot start worker")
            await self._client.aclose()
            return 2
        self.worker_id = registered["id"]
        log.info("worker registered: id=%s host=%s", self.worker_id, self.hostname)

        try:
            await asyncio.gather(
                self._heartbeat_loop(),
                self._drain_loop(),
                self._admin_loop(),
                self._self_update_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()
        return 0

    def _snapshot_source_mtimes(self) -> dict[str, float]:
        """pipeline/ + worker/ の Python source の mtime をスナップショット."""
        roots = [Path("/opt/pipeline/pipeline")]
        out: dict[str, float] = {}
        for root in roots:
            if not root.exists():
                continue
            for p in root.rglob("*.py"):
                try:
                    out[str(p)] = p.stat().st_mtime
                except Exception:
                    pass
        return out

    async def _self_update_loop(self) -> None:
        """自分のコード変更を検知 → exit 42 で自己 restart (systemd auto-restart 前提)."""
        snapshot = self._snapshot_source_mtimes()
        log.info("self-update watch: %d files baseline", len(snapshot))
        while not self._stop_evt.is_set():
            try:
                await asyncio.wait_for(self._stop_evt.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            else:
                return  # stop_evt set → 通常 shutdown
            current = self._snapshot_source_mtimes()
            # 変更/追加/削除を検出
            changed = [p for p, m in current.items() if snapshot.get(p) != m]
            removed = [p for p in snapshot if p not in current]
            if changed or removed:
                log.warning("source change detected: %d changed, %d removed → exit 42 (systemd will restart)",
                            len(changed), len(removed))
                if changed[:3]:
                    log.warning("  examples: %s", changed[:3])
                # graceful 終了をシグナルする
                self._stop_evt.set()
                # 1 秒待って systemd に明示の exit code で死ぬ
                await asyncio.sleep(1)
                os._exit(42)

    async def _admin_loop(self) -> None:
        """control plane から admin コマンド (= shell exec, fetch_archive 等) を long poll で受信."""
        while not self._stop_evt.is_set():
            try:
                cmd = await self._client.poll_admin_cmd(self.worker_id, self.hostname)
            except Exception:
                log.warning("admin poll failed; retry in 5s", exc_info=False)
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass
                continue
            if cmd is None:
                continue  # 204 = nothing, すぐ次の poll
            log.info("admin cmd received: id=%s type=%s host=%s",
                     cmd.get("id"), cmd.get("cmd_type"), cmd.get("target_host"))
            try:
                result = await asyncio.to_thread(_execute_admin_cmd, cmd, self.control_url)
                await self._client.complete_admin_cmd(
                    self.worker_id, cmd["id"],
                    success=result["success"],
                    exit_code=result.get("exit_code"),
                    stdout=result.get("stdout"),
                    stderr=result.get("stderr"),
                    error=result.get("error"),
                )
                log.info("admin cmd %s done: success=%s exit=%s",
                         cmd["id"], result["success"], result.get("exit_code"))
            except Exception as e:
                log.exception("admin cmd %s exec raised", cmd.get("id"))
                await self._client.complete_admin_cmd(
                    self.worker_id, cmd["id"],
                    success=False, error=f"daemon exception: {e}"[:1000],
                )

    # ---------------- loops ----------------

    async def _heartbeat_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                rd, ed = self._pending_rows, self._pending_errs
                self._pending_rows = 0
                self._pending_errs = 0
                payload: dict[str, Any] = {
                    "rows_processed_delta": rd,
                    "errors_total_delta": ed,
                    # heartbeat 時点の処理中 workload (= None=idle)。 サーバ側 repository は
                    # 渡された値で上書きするので、 タスク間アイドル時に "—" 表示に戻る。
                    "current_workload": self._current_workload,
                }
                gpu = _nvidia_smi_gpus()
                if gpu:
                    payload["gpu_metrics"] = gpu
                await self._client.heartbeat(self.worker_id, payload)
            except Exception:
                log.warning("heartbeat failed (will retry)", exc_info=False)
            try:
                await asyncio.wait_for(self._stop_evt.wait(), timeout=HEARTBEAT_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    async def _drain_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                did = await self._drain_once()
            except Exception:
                log.exception("drain iteration failed")
                did = False
            if not did:
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=self.idle_sleep_s)
                except asyncio.TimeoutError:
                    pass

    async def _drain_once(self) -> bool:
        try:
            workloads = await self._client.list_workloads(self.worker_id)
        except Exception:
            log.exception("list_workloads failed; sleeping")
            return False
        any_work = False
        seen_slugs: set[str] = set()
        for w in workloads:
            seen_slugs.add(w["slug"])
            if w["executor_type"] not in {"shell", "python_module"}:
                continue
            tasks = await self._client.claim(self.worker_id, w["slug"], w["batch_size"])
            if not tasks:
                continue
            any_work = True
            try:
                executor, just_built = self._get_or_build_executor_observe(w)
            except Exception as e:
                log.warning("executor build failed for %s: %s", w["slug"], e)
                self._evict_if_cached(w["slug"])
                for t in tasks:
                    await self._client.fail(self.worker_id, w["slug"], t["pk"], str(e))
                    await self._client.record_run(self.worker_id, {
                        "workload_slug": w["slug"],
                        "pk": t["pk"],
                        "attempt": int(t["attempt"]),
                        "started_at": _utcnow_iso(),
                        "success": False,
                        "exit_code": None,
                        "duration_ms": 0,
                        "error": f"executor build error: {e}",
                    })
                    self._pending_errs += 1
                continue
            # build 直後 (= plugin.setup() 完了直後) のベース VRAM を即測って報告
            if just_built and w["executor_type"] == "python_module":
                await self._maybe_report_vram(w["slug"], first_run=True)
            # plugin が process_batch を持ってれば batch 実行 (= GPU 推論等で N 倍速)
            self._current_workload = w["slug"]
            try:
                if hasattr(executor, "supports_batch") and executor.supports_batch() and len(tasks) > 1:
                    await self._execute_batch(w, executor, tasks)
                else:
                    for t in tasks:
                        await self._execute_one(w, executor, t)
            finally:
                self._current_workload = None
            # task 実行後 (= ピーク VRAM 含む)。 rate limit 内で控えめに POST
            if w["executor_type"] == "python_module":
                await self._maybe_report_vram(w["slug"], first_run=False)
            # Lv2 preemption: batch 完了直後に「より高 priority に pending あるか?」 を peek。
            # あれば 残 workload を諦めて _drain_once を抜け、 次回 _drain_loop で
            # priority 降順 再 fetch → 最高 priority から再開 (= effective preempt at batch boundary)。
            try:
                current_priority = int(w.get("priority") or 0)
            except Exception:
                current_priority = 0
            if await self._client.peek_higher_pending(self.worker_id, current_priority):
                log.info("preempt: higher-priority workload pending; yielding from %s (p=%d)",
                         w["slug"], current_priority)
                break
        # 削除/disable された workload の executor を解放
        for slug in list(self._executor_cache):
            if slug not in seen_slugs:
                self._evict_if_cached(slug)
        return any_work

    # ---------------- VRAM self-report ----------------

    async def _report_oom_bump(self, slug: str, bump_ratio: float = 1.2) -> None:
        """OOM 例外を検知した時に呼ぶ。 現在 peak の bump_ratio 倍を control plane に
        POST → control plane の `record_vram_observation` は incoming が prev*0.95 より
        高ければそのまま採用するので、 結果的に observed_vram_mb_peak が +20% 上昇する。
        次回 install-multi-worker.sh で N が下がる方向に効く (= self-healing)。
        """
        current = self._vram_peaks.get(slug, 0)
        if current <= 0:
            # まだベース観測値が無い → nvidia-smi で取り直す
            sampled = await asyncio.to_thread(_measure_self_vram_mb, os.getpid())
            current = sampled or 0
        if current <= 0:
            log.warning("OOM detected for %s but no current peak available; skipping bump", slug)
            return
        bumped = int(current * bump_ratio)
        log.warning("OOM detected for %s; bumping observed VRAM peak %d → %d MB",
                    slug, current, bumped)
        try:
            await self._client.post_vram_observation(slug, bumped, self.worker_id)
            self._vram_peaks[slug] = bumped
        except Exception:
            log.warning("OOM bump post failed slug=%s", slug, exc_info=False)

    async def _maybe_report_vram(self, slug: str, *, first_run: bool = False) -> None:
        """自プロセスの GPU VRAM 占有を測って peak を更新。 30s に1回 control plane に POST。
        `first_run=True` で「executor build 直後」呼び出し時は rate limit を無視して即報告する
        (= setup() 完了直後のベース値を素早く control plane に反映)。
        """
        used = await asyncio.to_thread(_measure_self_vram_mb, os.getpid())
        if used is None or used <= 0:
            return
        prev = self._vram_peaks.get(slug, 0)
        if used > prev:
            self._vram_peaks[slug] = used
        now = time.monotonic()
        last = self._vram_last_report.get(slug, 0.0)
        if not first_run and (now - last) < 30.0:
            return
        self._vram_last_report[slug] = now
        peak = self._vram_peaks.get(slug, used)
        try:
            await self._client.post_vram_observation(slug, peak, self.worker_id)
        except Exception:
            log.warning("vram observation post failed slug=%s peak=%s", slug, peak,
                        exc_info=False)

    # ---------------- executor cache ----------------

    def _get_or_build_executor(self, w: dict) -> Executor:
        ex, _ = self._get_or_build_executor_observe(w)
        return ex

    def _get_or_build_executor_observe(self, w: dict) -> tuple[Executor, bool]:
        """`_get_or_build_executor` + 「新規 build したか」を返す版。
        新規 build = plugin.setup() が走った → VRAM 占有のベース値を即測定すべきタイミング。
        """
        wanted_hash = _config_hash(w["executor_type"], w["executor_config"])
        cached = self._executor_cache.get(w["slug"])
        if cached is not None and cached[0] == wanted_hash:
            return cached[1], False
        if cached is not None:
            log.info("config changed for %s; rebuild (%s → %s)", w["slug"], cached[0], wanted_hash)
            self._close_executor(cached[1])
            # config が変わった = peak もリセット (= 新 model size になり得るので)
            self._vram_peaks.pop(w["slug"], None)
            self._vram_last_report.pop(w["slug"], None)
        config = dict(w["executor_config"])
        # python_module: PythonModuleExecutor 本体が config.source_path を直接読むので daemon 側は素通し
        ex = create_executor(w["executor_type"], config)
        self._executor_cache[w["slug"]] = (wanted_hash, ex)
        return ex, True

    def _close_executor(self, ex: Executor) -> None:
        close = getattr(ex, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                log.exception("executor close() raised; ignored")

    def _evict_if_cached(self, slug: str) -> None:
        cached = self._executor_cache.pop(slug, None)
        if cached is not None:
            log.info("evicting cached executor for %s", slug)
            self._close_executor(cached[1])

    # ---------------- task execution ----------------

    async def _execute_one(self, w: dict, executor: Executor, t: dict) -> None:
        started = _utcnow_iso()
        run_id: str | None = None
        try:
            run_id = await self._client.start_run(self.worker_id, {
                "workload_slug": w["slug"],
                "pk": t["pk"],
                "attempt": int(t["attempt"]),
                "started_at": started,
            })
        except Exception:
            log.exception("start_run HTTP failed; will record at finish")

        workdir = Path(tempfile.mkdtemp(prefix=f"pipeline-{w['slug']}-"))
        deadline = datetime.now(timezone.utc) + timedelta(seconds=int(w["lease_secs"]))
        ctx = ExecutionContext(
            deadline=deadline,
            workdir=workdir,
            env=dict(os.environ),
            workload_config=dict(w["executor_config"]),
        )
        task = Task(pk=t["pk"], workload_slug=w["slug"], attempt=int(t["attempt"]), extra=dict(t.get("extra") or {}))
        try:
            result = await asyncio.to_thread(executor.run, task, ctx)
        except Exception as e:
            result = ExecutionResult(success=False, error=f"executor raised: {e}")
            log.exception("workload %s task %s executor raised", w["slug"], t["pk"])
        finally:
            try:
                import shutil as _sh
                _sh.rmtree(workdir, ignore_errors=True)
            except Exception:
                pass

        is_success = _eval_success(w["success_criteria"], result)
        self.processed_total += 1
        self._pending_rows += 1
        # OOM 検知 → observed VRAM peak を bump (= 次回 install-multi-worker で N が下がり self-healing)
        if not is_success and _looks_like_oom(result.error or "", result.stderr or ""):
            await self._report_oom_bump(w["slug"])
        try:
            if is_success:
                self.success_total += 1
                await self._client.complete(self.worker_id, w["slug"], [t["pk"]])
            else:
                self.failure_total += 1
                self._pending_errs += 1
                await self._client.fail(
                    self.worker_id, w["slug"], t["pk"],
                    (result.error or result.stderr or "non-zero exit")[:4000],
                )
        except Exception:
            log.exception("complete/fail HTTP failed; will rely on lease expiry")

        try:
            if run_id:
                await self._client.finish_run(self.worker_id, run_id, {
                    "success": is_success,
                    "exit_code": result.exit_code,
                    "duration_ms": result.duration_ms,
                    "stdout": result.stdout or None,
                    "stderr": result.stderr or None,
                    "output_json": result.output_json,
                    "error": result.error,
                })
            else:
                await self._client.record_run(self.worker_id, {
                    "workload_slug": w["slug"],
                    "pk": t["pk"],
                    "attempt": int(t["attempt"]),
                    "started_at": started,
                    "success": is_success,
                    "exit_code": result.exit_code,
                    "duration_ms": result.duration_ms,
                    "stdout": result.stdout or None,
                    "stderr": result.stderr or None,
                    "output_json": result.output_json,
                    "error": result.error,
                })
        except Exception:
            log.exception("finish_run HTTP failed; lost")

    async def _execute_batch(self, w: dict, executor: Executor, tasks: list[dict]) -> None:
        """N task を 1 度の executor 呼び出しで処理 (GPU batch 推論用)。

        全 task で共通の ctx (workdir/deadline) を使う。 plugin の `process_batch`
        は tasks と同じ長さの list[dict] を返す約束。
        """
        started = _utcnow_iso()

        run_ids: dict[str, str | None] = {}
        for t in tasks:
            try:
                rid = await self._client.start_run(self.worker_id, {
                    "workload_slug": w["slug"],
                    "pk": t["pk"],
                    "attempt": int(t["attempt"]),
                    "started_at": started,
                })
                run_ids[t["pk"]] = rid
            except Exception:
                log.exception("start_run HTTP failed for pk=%s", t["pk"])
                run_ids[t["pk"]] = None

        workdir = Path(tempfile.mkdtemp(prefix=f"pipeline-{w['slug']}-batch-"))
        deadline = datetime.now(timezone.utc) + timedelta(seconds=int(w["lease_secs"]))
        ctx = ExecutionContext(
            deadline=deadline,
            workdir=workdir,
            env=dict(os.environ),
            workload_config=dict(w["executor_config"]),
        )
        task_objs = [
            Task(pk=t["pk"], workload_slug=w["slug"], attempt=int(t["attempt"]),
                 extra=dict(t.get("extra") or {}))
            for t in tasks
        ]
        try:
            results = await asyncio.to_thread(executor.run_batch, task_objs, ctx)
        except Exception as e:
            log.exception("workload %s batch (%d tasks) executor raised", w["slug"], len(tasks))
            results = [ExecutionResult(success=False, error=f"executor.run_batch raised: {e}")
                       for _ in tasks]
        finally:
            try:
                import shutil as _sh
                _sh.rmtree(workdir, ignore_errors=True)
            except Exception:
                pass

        # OOM 検知: batch 全体で 1 回だけ bump (= 同じ workload で連発の防止)
        if any(_looks_like_oom(r.error or "", r.stderr or "") for r in results):
            await self._report_oom_bump(w["slug"])
        success_pks: list[str] = []
        for t, result in zip(tasks, results):
            is_success = _eval_success(w["success_criteria"], result)
            self.processed_total += 1
            self._pending_rows += 1
            if is_success:
                self.success_total += 1
                success_pks.append(t["pk"])
            else:
                self.failure_total += 1
                self._pending_errs += 1
                try:
                    await self._client.fail(
                        self.worker_id, w["slug"], t["pk"],
                        (result.error or result.stderr or "non-zero exit")[:4000],
                    )
                except Exception:
                    log.exception("fail HTTP failed; will rely on lease expiry")
            rid = run_ids.get(t["pk"])
            try:
                if rid:
                    await self._client.finish_run(self.worker_id, rid, {
                        "success": is_success,
                        "exit_code": result.exit_code,
                        "duration_ms": result.duration_ms,
                        "stdout": result.stdout or None,
                        "stderr": result.stderr or None,
                        "output_json": result.output_json,
                        "error": result.error,
                    })
                else:
                    await self._client.record_run(self.worker_id, {
                        "workload_slug": w["slug"],
                        "pk": t["pk"],
                        "attempt": int(t["attempt"]),
                        "started_at": started,
                        "success": is_success,
                        "exit_code": result.exit_code,
                        "duration_ms": result.duration_ms,
                        "stdout": result.stdout or None,
                        "stderr": result.stderr or None,
                        "output_json": result.output_json,
                        "error": result.error,
                    })
            except Exception:
                log.exception("finish_run HTTP failed; lost")
        # 成功分は 1 リクエストで一括 complete (= 軽量化)
        if success_pks:
            try:
                await self._client.complete(self.worker_id, w["slug"], success_pks)
            except Exception:
                log.exception("complete HTTP failed; will rely on lease expiry")

    # ---------------- shutdown ----------------

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_evt.set)
            except NotImplementedError:
                # Windows 等で add_signal_handler が無いケース
                signal.signal(sig, lambda *_: self._stop_evt.set())

    async def _shutdown(self) -> None:
        log.info("worker shutting down")
        for slug in list(self._executor_cache):
            self._close_executor(self._executor_cache[slug][1])
        self._executor_cache.clear()
        if self.worker_id:
            try:
                await self._client.deregister(self.worker_id)
            except Exception:
                log.exception("deregister failed; ignored")
        await self._client.aclose()
        log.info("worker stopped (processed=%d success=%d fail=%d)",
                 self.processed_total, self.success_total, self.failure_total)


async def run_worker_cli(args) -> int:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx / urllib3 の HTTP request log は heartbeat ノイズになるので抑制
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    daemon = WorkerDaemon(
        control_url=args.control_url,
        hostname=args.hostname,
        worker_id=args.worker_id,
        token=args.token,
        idle_sleep_s=args.idle_sleep,
    )
    # control plane へ log push (service-logs テーブル経由で UI 表示用)
    from pipeline.worker.log_pusher import attach_log_pusher
    attach_log_pusher(
        control_url=args.control_url,
        service="pipeline-worker-gpu",
        host=args.hostname or socket.gethostname(),
        worker_id_getter=lambda: getattr(daemon, "worker_id", None),
        token=args.token,
    )
    return await daemon.run()
