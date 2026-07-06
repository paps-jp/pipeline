"""pipeline-agent CLI: 1 host = 1 プロセスで worker 子プロセスを spawn/監督する (P1 最小 PoC)。

起動: python -m pipeline.agent.main --desired /etc/pipeline-agent/desired.json \
        --pipeline-exe /home/www/face_search/bin/pipeline
"""
from __future__ import annotations

import argparse
import logging
import signal
import socket
import time

from pipeline.agent.desired import load_desired
from pipeline.agent.supervisor import AgentSupervisor

log = logging.getLogger("pipeline.agent")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeline-agent")
    p.add_argument("--host", default=None, help="host 名 (default: socket.gethostname())")
    p.add_argument("--desired", default="/etc/pipeline-agent/desired.json",
                   help="P1: desired state の JSON ファイル")
    p.add_argument("--pipeline-exe", default="pipeline",
                   help="子 worker を起動する pipeline CLI パス")
    p.add_argument("--gpu-index", default="0")
    p.add_argument("--interval", type=float, default=10.0, help="reconcile 周期 (秒)")
    p.add_argument("--vram-safety-mult", type=float, default=1.3)
    p.add_argument("--vram-floor-mb", type=int, default=500)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    host = args.host or socket.gethostname()

    d0 = load_desired(args.desired)
    sup = AgentSupervisor(
        host=host,
        pipeline_exe=args.pipeline_exe,
        control_url=d0.control_url,
        gpu_index=args.gpu_index,
        vram_safety_mult=args.vram_safety_mult,
        vram_floor_mb=args.vram_floor_mb,
    )

    stop = {"v": False}

    def _sig(signum, _frame):
        log.info("[agent] signal %s → shutdown", signum)
        stop["v"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    log.info("[agent] start host=%s desired=%s interval=%.0fs exe=%s",
             host, args.desired, args.interval, args.pipeline_exe)
    tick = 0
    try:
        while not stop["v"]:
            tick += 1
            try:
                d = load_desired(args.desired)
                sup.control_url = d.control_url
                sup.reconcile(d)
                if tick % 6 == 1:  # 約1分毎に status ログ
                    log.info("[agent] status: %s", sup.status())
            except Exception:
                log.exception("[agent] reconcile failed")
            slept = 0.0
            while slept < args.interval and not stop["v"]:
                time.sleep(0.5)
                slept += 0.5
    finally:
        log.info("[agent] shutting down children")
        sup.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
