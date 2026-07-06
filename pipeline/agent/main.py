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
    p.add_argument("--proxy", action="store_true",
                   help="P2-1: agent 内アグリゲータを起動し、子を proxy 経由で control plane に繋ぐ")
    p.add_argument("--proxy-port", type=int, default=8799)
    p.add_argument("--coalesce-heartbeat", action="store_true",
                   help="proxy で heartbeat を refresh 周期に合体 (control plane の lost 閾値確認後に)")
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

    # P2-1: アグリゲータ proxy。 有効時は子の control-url を proxy に向け、 上流は本物の control plane。
    proxy = None
    if args.proxy:
        from pipeline.agent.proxy import AggregatorProxy
        proxy = AggregatorProxy(
            upstream=d0.control_url,
            port=args.proxy_port,
            refresh_s=args.interval,
            coalesce_heartbeat=args.coalesce_heartbeat,
        )
        proxy.start()
        sup.control_url = proxy.base_url  # 子はここを向く (reconcile で上書きしない)

    stop = {"v": False}

    def _sig(signum, _frame):
        log.info("[agent] signal %s → shutdown", signum)
        stop["v"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    log.info("[agent] start host=%s desired=%s interval=%.0fs exe=%s",
             host, args.desired, args.interval, args.pipeline_exe)
    n_orphan = sup.cleanup_orphans()  # 前世代 agent の孤児子を回収してから clean start
    if n_orphan:
        log.info("[agent] 起動時に孤児 %d を回収", n_orphan)
    tick = 0
    try:
        while not stop["v"]:
            tick += 1
            try:
                d = load_desired(args.desired)
                if proxy is not None:
                    proxy.upstream = d.control_url.rstrip("/")  # 子は proxy 固定、 上流だけ追従
                else:
                    sup.control_url = d.control_url
                sup.reconcile(d)
                if tick % 6 == 1:  # 約1分毎に status ログ
                    st = sup.status()
                    if proxy is not None:
                        st["proxy"] = proxy.stats()
                    log.info("[agent] status: %s", st)
            except Exception:
                log.exception("[agent] reconcile failed")
            slept = 0.0
            while slept < args.interval and not stop["v"]:
                time.sleep(0.5)
                slept += 0.5
    finally:
        log.info("[agent] shutting down children")
        sup.shutdown()
        if proxy is not None:
            proxy.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
