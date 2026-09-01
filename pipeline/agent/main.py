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

from pipeline.agent.desired import Desired, fetch_desired_via_sync, load_desired
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
    p.add_argument("--drain-grace-s", type=float, default=60.0,
                   help="graceful kill (SIGTERM) 後この秒数で死ななければ SIGKILL へ昇格")
    p.add_argument("--wedged-min-age-s", type=float, default=180.0,
                   help="この秒数より若い子は wedged 判定しない (モデルロード中の誤検知防止)")
    p.add_argument("--wedged-no-success-s", type=float, default=300.0,
                   help="build error を出しつつこの秒数 成功 run が無ければ wedged とみなす")
    p.add_argument("--wedged-cooldown-s", type=float, default=600.0,
                   help="wedged で子を落とした slug をこの秒数 再 spawn しない")
    p.add_argument("--proxy", action="store_true",
                   help="P2-1: agent 内アグリゲータを起動し、子を proxy 経由で control plane に繋ぐ")
    p.add_argument("--proxy-port", type=int, default=8799)
    p.add_argument("--coalesce-heartbeat", action="store_true",
                   help="proxy で heartbeat を refresh 周期に合体 (control plane の lost 閾値確認後に)")
    p.add_argument("--no-sync", action="store_true",
                   help="P2 sync を使わずローカル desired.json のみ (bootstrap/検証用)")
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
        drain_grace_s=args.drain_grace_s,
        wedged_min_age_s=args.wedged_min_age_s,
        wedged_no_success_s=args.wedged_no_success_s,
        wedged_cooldown_s=args.wedged_cooldown_s,
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
        # wedged 判定の材料は proxy が中継する run 結果だけ (worker 無改修)。
        # proxy 無効時は health_source=None のまま = wedged 判定オフ (従来挙動)。
        sup.health_source = proxy.health
        sup.health_forget = proxy.forget

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
    sync_control_url = d0.control_url  # sync は本物の control plane へ (proxy 経由でない)
    tick = 0
    # 直近で sync に成功した desired。 sync が落ちたときはこれを据え置く。
    # ローカル desired.json は **bootstrap 専用** (control plane と一度も話せていない間だけ)
    # であり、 実運用の desired の部分集合でしかない。 sync 一発の失敗でここへ落とすと
    # 「ファイルに書いていない workload = desired 外」 とみなされ、 稼働中の子が
    # SIGTERM → SIGKILL される。 faiss-index-build が数時間かけたビルドを
    # 毎回それで飛ばしていた (2026-09-02)。
    last_synced: Desired | None = None
    sync_fail_streak = 0

    if not args.no_sync:
        # 起動直後の 1 発目が失敗すると bootstrap ファイルへ落ちてしまうので、
        # reconcile に入る前に少しだけ粘って last_synced を種付けする。
        # (control plane は重いクエリで数十秒詰まることがある)
        for attempt in range(1, 7):
            try:
                last_synced = fetch_desired_via_sync(
                    sync_control_url, host,
                    vram_total_mb=sup._gpu_total_mb(), vram_free_mb=sup._gpu_free_mb(),
                    children=sup.children_status(),
                    **dict(zip(("disk_total_mb", "disk_free_mb"), sup._disk_stats())),
                )
                if last_synced is not None:
                    log.info("[agent] 起動時 sync 確立 (%d 回目)", attempt)
                    break
                log.warning("[agent] 起動時 sync: desired 未設定 (%d/6)", attempt)
            except Exception as e:
                log.warning("[agent] 起動時 sync 失敗 (%d/6): %s", attempt, e)
            if stop["v"]:
                break
            time.sleep(5.0)
        else:
            log.warning("[agent] 起動時 sync を確立できず — bootstrap desired.json で開始する "
                        "(「desired 外」 削除は抑止)")

    try:
        while not stop["v"]:
            tick += 1
            try:
                # P2: control plane から desired を取得 (状態を報告しつつ)。 失敗/未設定なら
                # 直近の sync 結果を据え置く (それも無い bootstrap 時だけローカルファイル)。
                d = None
                if not args.no_sync:
                    try:
                        d = fetch_desired_via_sync(
                            sync_control_url, host,
                            vram_total_mb=sup._gpu_total_mb(),
                            vram_free_mb=sup._gpu_free_mb(),
                            children=sup.children_status(),
                            **dict(zip(("disk_total_mb", "disk_free_mb"),
                                       sup._disk_stats())),
                        )
                    except Exception as e:
                        sync_fail_streak += 1
                        # 1発目は WARNING で出す。 以降は 1 分毎 (連続失敗中の氾濫を防ぐ)。
                        if sync_fail_streak == 1 or sync_fail_streak % 6 == 0:
                            log.warning("[agent] sync 失敗 (%d 回連続): %s — desired は %s",
                                        sync_fail_streak, e,
                                        "直近の sync 結果を据え置き" if last_synced is not None
                                        else "ローカル desired.json (bootstrap)")
                    else:
                        # d is None = 「control plane は応答したが desired 未設定」。
                        # これも据え置き対象にする (空応答で最後の desired を捨てない)。
                        if d is not None:
                            if sync_fail_streak:
                                log.info("[agent] sync 復帰 (%d 回連続失敗のあと)", sync_fail_streak)
                            sync_fail_streak = 0
                            last_synced = d
                if d is None:
                    # sync 実績があるならそれを据え置く。 ローカルファイルへは落とさない。
                    # それも無い (起動直後に sync が通っていない) 場合だけファイルを使うが、
                    # 非権威扱いにして 「desired 外」 削除は行わせない。
                    d = last_synced if last_synced is not None else load_desired(
                        args.desired, authoritative=args.no_sync)
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
