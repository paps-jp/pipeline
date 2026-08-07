#!/usr/bin/env python3
"""control plane (= REST API + SQLite) のスループット / ボトルネック計測ハーネス。

worker daemon (pipeline/worker/service.py) が出す HTTP 呼び出し列を N 並列で再現し、
  - tasks/sec (= fleet 全体の実効スループット)
  - endpoint 別 latency (p50 / p95 / max)
  - endpoint 別の DB transaction 数 / SQL 実行数 / 待ち時間
を測る。 perf/control-plane-bottleneck.md の数字はこのスクリプトで取ったもの。

使い方:

    # 16 worker / 12 workload で 15 秒回す (= 既定の負荷試験)
    python scripts/bench_control_plane.py load --workers 16 --dur 15

    # worker 数を振ってスケール曲線を出す
    for n in 1 2 4 8 16; do
      python scripts/bench_control_plane.py load --workers $n --dur 12 --port $((9100+n))
    done

    # task が 1 件も無い状態のポーリング費用だけを測る
    python scripts/bench_control_plane.py load --workers 16 --idle

    # 「1 task あたり 3 request」 を 「1 batch あたり 1 request」 に畳んだ場合の上限
    python scripts/bench_control_plane.py load --workers 16 --fused

    # API 1 リクエストあたりの SQL / transaction 回数を数える
    python scripts/bench_control_plane.py sqlcount

`sqlcount` は secondary_db (= 本番の MariaDB queue backend) の有無で
transaction 数がどう変わるかも出す。 secondary は SQLite ファイルで代用する
(= 計測したいのは backend の速度ではなく発行される transaction の本数のため)。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import contextvars
import os
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

os.environ.setdefault("PIPELINE_INPROC_WORKER", "0")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from pipeline.config import Settings  # noqa: E402
from pipeline.control.server import create_app  # noqa: E402
from pipeline.db import sqlite as sqlite_mod  # noqa: E402

# --------------------------------------------------------------- 計測フック
# endpoint 名を contextvar に入れておき、 DB 層のフックから参照して
# 「どの endpoint が何本 transaction を張ったか」 を紐付ける。
CURRENT_EP: contextvars.ContextVar[str] = contextvars.ContextVar("ep", default="-")

_STATS_LOCK = threading.Lock()
TX_COUNT: dict[str, int] = defaultdict(int)
TX_WAIT: dict[str, float] = defaultdict(float)    # transaction() を呼んでから conn を掴むまで
TX_HELD: dict[str, float] = defaultdict(float)    # conn を掴んでから commit 完了まで
SQL_COUNT: dict[str, int] = defaultdict(int)
SQL_BY_TEXT: dict[str, int] = defaultdict(int)
TX_HELD_TOTAL = [0.0]

_orig_tx = sqlite_mod.SqliteDatabase.transaction
_orig_exec = sqlite_mod.SqliteConnection.execute


def _patched_execute(self, sql, params=()):
    ep = CURRENT_EP.get()
    norm = " ".join(sql.split())[:90]
    with _STATS_LOCK:
        SQL_COUNT[ep] += 1
        SQL_BY_TEXT[norm] += 1
    return _orig_exec(self, sql, params)


def _record(ep: str, t0: float, t1: float, t2: float) -> None:
    with _STATS_LOCK:
        TX_COUNT[ep] += 1
        TX_WAIT[ep] += t1 - t0
        TX_HELD[ep] += t2 - t1
        TX_HELD_TOTAL[0] += t2 - t1


@contextlib.contextmanager
def _patched_transaction(self):
    """本来の transaction() をそのまま包んで wait / held を分けて測る。"""
    ep = CURRENT_EP.get()
    t0 = time.perf_counter()
    cm = _orig_tx(self)
    conn = cm.__enter__()
    t1 = time.perf_counter()
    try:
        yield conn
    except BaseException:
        _record(ep, t0, t1, time.perf_counter())
        if not cm.__exit__(*sys.exc_info()):
            raise
        return
    cm.__exit__(None, None, None)
    _record(ep, t0, t1, time.perf_counter())


def install_hooks() -> None:
    sqlite_mod.SqliteConnection.execute = _patched_execute
    sqlite_mod.SqliteDatabase.transaction = _patched_transaction


def reset_stats() -> None:
    with _STATS_LOCK:
        TX_COUNT.clear()
        TX_WAIT.clear()
        TX_HELD.clear()
        SQL_COUNT.clear()
        SQL_BY_TEXT.clear()
        TX_HELD_TOTAL[0] = 0.0


# --------------------------------------------------------------- app 組み立て
def _label_for(method: str, path: str) -> str:
    parts = path.split("/")
    if len(parts) > 4 and parts[3] == "workers":
        if len(parts) > 6 and parts[5] == "runs":
            return f"{method} workers/*/runs/*/{parts[7] if len(parts) > 7 else ''}"
        tail = "/".join(parts[5:]) if len(parts) > 5 else ""
        return f"{method} workers/*/{tail}" if tail else f"{method} workers/*"
    return f"{method} {path}"


def build_app(db_path: str, with_fused: bool):
    app = create_app(Settings(db_url=f"sqlite:///{db_path}", mode="control"))

    if with_fused:
        # headroom 実験用の仮想 endpoint: 1 batch 分の run 記録 + complete を
        # 1 request / 1 transaction で済ませる。 本番 API には存在しない。
        import secrets

        from fastapi import Request as _Req
        from pydantic import BaseModel as _BM

        from pipeline.repositories.workloads import WorkloadRepository

        class FusedBody(_BM):
            workload_slug: str
            results: list[dict]

        @app.post("/bench/fused/{worker_id}", include_in_schema=False)
        def fused(worker_id: str, body: FusedBody, request: _Req):
            w = WorkloadRepository(request.app.state.db).get(body.workload_slug)
            with request.app.state.db.transaction() as conn:
                for r in body.results:
                    conn.execute(
                        "INSERT INTO runs (id, workload_slug, pk, worker_id, attempt,"
                        " started_at, finished_at, success, exit_code, duration_ms,"
                        " stdout, stderr, output_json, error)"
                        " VALUES (:id,:ws,:pk,:wid,:att,:sa,:fa,1,0,:dur,NULL,NULL,NULL,NULL)",
                        {"id": "r_" + secrets.token_hex(8), "ws": w.slug, "pk": r["pk"],
                         "wid": worker_id, "att": r.get("attempt", 0),
                         "sa": r["started_at"], "fa": r["started_at"],
                         "dur": r.get("duration_ms", 1)},
                    )
                ph = ", ".join(f":k{i}" for i in range(len(body.results)))
                conn.execute(
                    f"DELETE FROM {w.queue_table} WHERE pk IN ({ph})",
                    {f"k{i}": r["pk"] for i, r in enumerate(body.results)},
                )
            return {"n": len(body.results)}

    @app.middleware("http")
    async def tag_endpoint(request, call_next):
        CURRENT_EP.set(_label_for(request.method, request.url.path))
        return await call_next(request)

    return app


# --------------------------------------------------------------- 負荷ドライバ
LAT: dict[str, list[float]] = defaultdict(list)


async def _timed(label, coro):
    t0 = time.perf_counter()
    r = await coro
    LAT[label].append((time.perf_counter() - t0) * 1000)
    return r


async def _seed(base: str, n_workloads: int, n_tasks: int) -> None:
    async with httpx.AsyncClient(base_url=base, timeout=60) as c:
        for i in range(n_workloads):
            slug = f"wl{i:02d}"
            r = await c.post("/api/v1/workloads", json={
                "slug": slug, "name": slug, "enabled": True,
                "executor_type": "shell", "executor_config": {"command": ["true"]},
                "batch_size": 10, "lease_secs": 300, "max_attempts": 3,
                "priority": 100 - i, "weight": 1.0,
            })
            r.raise_for_status()
            if n_tasks <= 0:
                continue
            r = await c.post(f"/api/v1/workloads/{slug}/tasks/batch", json={
                "items": [{"pk": f"{slug}-{j}", "extra": {}} for j in range(n_tasks)]})
            r.raise_for_status()


async def _worker_sim(base: str, idx: int, stop_at: float, counters, fused: bool) -> None:
    """service.py の _drain_loop / _drain_once と同じ順序で API を叩く。"""
    async with httpx.AsyncClient(base_url=base, timeout=60) as c:
        r = await c.post("/api/v1/workers", json={
            "host": f"bench-gpu{idx % 4}-{idx}", "pid": 1000 + idx,
            "resources": {"gpu_vram_mb": 24000}})
        wid = r.json()["id"]
        last_hb = 0.0
        while time.monotonic() < stop_at:
            now = time.monotonic()
            if now - last_hb > 5:                       # HEARTBEAT_INTERVAL_S
                last_hb = now
                await _timed("PUT heartbeat",
                             c.put(f"/api/v1/workers/{wid}/heartbeat", json={}))
            r = await _timed("GET workloads", c.get(f"/api/v1/workers/{wid}/workloads"))
            wls = r.json()["workloads"]
            if not wls:
                await asyncio.sleep(0.05)
                continue
            did = False
            for w in wls:
                if time.monotonic() >= stop_at:
                    break
                r = await _timed("POST claim", c.post(
                    f"/api/v1/workers/{wid}/claim",
                    json={"workload_slug": w["slug"], "limit": w["batch_size"]}))
                tasks = r.json()["tasks"]
                if not tasks:
                    counters["empty_claim"] += 1
                    continue
                did = True
                if fused:
                    await _timed("POST fused", c.post(
                        f"/bench/fused/{wid}",
                        json={"workload_slug": w["slug"],
                              "results": [{"pk": t["pk"], "attempt": t["attempt"],
                                           "started_at": "2026-01-01T00:00:00+00:00",
                                           "duration_ms": 1} for t in tasks]}))
                    counters["done"] += len(tasks)
                    continue
                for t in tasks:
                    rr = await _timed("POST runs/start", c.post(
                        f"/api/v1/workers/{wid}/runs/start",
                        json={"workload_slug": w["slug"], "pk": t["pk"],
                              "attempt": t["attempt"],
                              "started_at": "2026-01-01T00:00:00+00:00"}))
                    rid = rr.json()["id"]
                    await _timed("POST complete", c.post(
                        f"/api/v1/workers/{wid}/complete",
                        json={"workload_slug": w["slug"], "pks": [t["pk"]]}))
                    await _timed("POST runs/finish", c.post(
                        f"/api/v1/workers/{wid}/runs/{rid}/finish",
                        json={"success": True, "exit_code": 0, "duration_ms": 1}))
                    counters["done"] += 1
                r = await _timed("GET higher-pending", c.get(
                    f"/api/v1/workers/{wid}/higher-pending", params={"than": w["priority"]}))
                if r.json().get("has_pending"):
                    break                                # Lv2 preemption
            if not did:
                await asyncio.sleep(0.02)


def _pct(v: list[float], p: int) -> float:
    if not v:
        return 0.0
    s = sorted(v)
    return s[min(len(s) - 1, int(len(s) * p / 100))]


async def _wait_ready(base: str) -> None:
    for _ in range(200):
        try:
            async with httpx.AsyncClient(timeout=2) as c:
                await c.get(f"{base}/api/v1/health")
            return
        except Exception:
            await asyncio.sleep(0.05)
    raise RuntimeError("server did not come up")


async def cmd_load(a: argparse.Namespace) -> None:
    tmpdir = Path(a.tmpdir or tempfile.mkdtemp(prefix="pipeline-bench-"))
    tmpdir.mkdir(parents=True, exist_ok=True)
    db = tmpdir / "bench.db"
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db) + suffix)
        if p.exists():
            p.unlink()

    if a.secondary:
        sec = tmpdir / "bench-secondary.db"
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(sec) + suffix)
            if p.exists():
                p.unlink()
        os.environ["PIPELINE_SECONDARY_DB_URL"] = f"sqlite:///{sec}"
    else:
        os.environ.pop("PIPELINE_SECONDARY_DB_URL", None)

    app = build_app(str(db), with_fused=a.fused)
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=a.port,
                                        log_level="warning", access_log=False))
    threading.Thread(target=srv.run, daemon=True).start()
    base = f"http://127.0.0.1:{a.port}"
    await _wait_ready(base)

    await _seed(base, a.workloads, 0 if a.idle else a.tasks)
    reset_stats()                                        # seed 分を除外
    LAT.clear()

    counters: Counter = Counter()
    t0 = time.monotonic()
    await asyncio.gather(*[_worker_sim(base, i, t0 + a.dur, counters, a.fused)
                           for i in range(a.workers)])
    elapsed = time.monotonic() - t0

    mode = []
    if a.idle:
        mode.append("idle")
    if a.fused:
        mode.append("fused")
    if a.secondary:
        mode.append("secondary_db")
    print(f"\n=== workers={a.workers} workloads={a.workloads} dur={elapsed:.1f}s "
          f"{'[' + ','.join(mode) + ']' if mode else ''} ===")
    print(f"tasks completed : {counters['done']}  ({counters['done'] / elapsed:.1f} tasks/s)")
    print(f"empty claims    : {counters['empty_claim']}")
    print(f"tx held total   : {TX_HELD_TOTAL[0]:.2f}s / {elapsed:.1f}s wall "
          f"= {100 * TX_HELD_TOTAL[0] / elapsed:.1f}%  "
          f"(SQLite は単一接続 + RLock 直列なので 100% が天井)")

    print("\n--- HTTP latency (ms) ---")
    print(f"{'endpoint':<24}{'n':>7}{'p50':>9}{'p95':>9}{'max':>9}{'total_s':>10}")
    for k in sorted(LAT, key=lambda k: -sum(LAT[k])):
        v = LAT[k]
        print(f"{k:<24}{len(v):>7}{_pct(v, 50):>9.1f}{_pct(v, 95):>9.1f}"
              f"{max(v):>9.1f}{sum(v) / 1000:>10.2f}")

    print("\n--- DB work per endpoint (held = 直列区間、 wait = その順番待ち) ---")
    print(f"{'endpoint':<34}{'tx':>8}{'sql':>9}{'held_s':>9}{'wait_s':>9}")
    for ep in sorted(TX_COUNT, key=lambda e: -TX_HELD[e]):
        print(f"{ep:<34}{TX_COUNT[ep]:>8}{SQL_COUNT[ep]:>9}"
              f"{TX_HELD[ep]:>9.2f}{TX_WAIT[ep]:>9.2f}")

    print("\n--- top SQL by execution count ---")
    for sql, n in sorted(SQL_BY_TEXT.items(), key=lambda kv: -kv[1])[:12]:
        print(f"{n:>8}  {sql}")

    srv.should_exit = True
    await asyncio.sleep(0.3)


# --------------------------------------------------------------- SQL カウント
def cmd_sqlcount(a: argparse.Namespace) -> None:
    from fastapi.testclient import TestClient

    for secondary in (False, True):
        os.environ.pop("PIPELINE_SECONDARY_DB_URL", None)
        tmp = Path(tempfile.mkdtemp(prefix="pipeline-bench-sql-"))
        if secondary:
            os.environ["PIPELINE_SECONDARY_DB_URL"] = f"sqlite:///{tmp / 'secondary.db'}"
        print(f"\n=== secondary_db (= 本番の MariaDB queue backend) "
              f"{'ON' if secondary else 'OFF'} / batch={a.batch} / "
              f"workloads={a.workloads} ===")

        with TestClient(create_app(Settings(db_url="sqlite:///:memory:", mode="control"))) as c:
            for i in range(a.workloads):
                c.post("/api/v1/workloads", json={
                    "slug": f"w{i:02d}", "name": "x", "enabled": True,
                    "executor_type": "shell", "executor_config": {"command": ["true"]},
                    "batch_size": a.batch, "lease_secs": 300, "max_attempts": 3})
            c.post("/api/v1/workloads/w00/tasks/batch", json={
                "items": [{"pk": f"p{j}", "extra": {}} for j in range(a.batch)]})
            wid = c.post("/api/v1/workers", json={"host": "h1"}).json()["id"]
            pks = [t["pk"] for t in c.post(
                f"/api/v1/workers/{wid}/claim",
                json={"workload_slug": "w00", "limit": a.batch}).json()["tasks"]]

            probes = [
                ("GET  workloads", lambda w=wid: c.get(f"/api/v1/workers/{w}/workloads")),
                ("POST claim (empty)", lambda w=wid: c.post(
                    f"/api/v1/workers/{w}/claim",
                    json={"workload_slug": "w01", "limit": a.batch})),
                (f"POST complete (pks={a.batch})", lambda w=wid, p=pks: c.post(
                    f"/api/v1/workers/{w}/complete",
                    json={"workload_slug": "w00", "pks": p})),
                ("GET  higher-pending", lambda w=wid: c.get(
                    f"/api/v1/workers/{w}/higher-pending", params={"than": 0})),
                ("PUT  heartbeat", lambda w=wid: c.put(
                    f"/api/v1/workers/{w}/heartbeat", json={})),
            ]
            for label, fn in probes:
                reset_stats()
                fn()
                tx = sum(TX_COUNT.values())
                sql = sum(SQL_COUNT.values())
                top = "; ".join(f"{n}x {s[:46]}" for s, n in
                                sorted(SQL_BY_TEXT.items(), key=lambda kv: -kv[1])[:2])
                print(f"  {label:<28} tx={tx:>3}  sql={sql:>3}   {top}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    lo = sub.add_parser("load", help="N worker 相当の負荷をかけて tasks/s と latency を測る")
    lo.add_argument("--workers", type=int, default=16)
    lo.add_argument("--workloads", type=int, default=12)
    lo.add_argument("--tasks", type=int, default=6000, help="workload あたりの投入 task 数")
    lo.add_argument("--dur", type=float, default=15.0)
    lo.add_argument("--port", type=int, default=8731)
    lo.add_argument("--idle", action="store_true", help="task を投入せず空振り polling だけ測る")
    lo.add_argument("--fused", action="store_true",
                    help="1 task=3 request を 1 batch=1 request に畳んだ場合の上限を測る")
    lo.add_argument("--secondary", action="store_true",
                    help="PIPELINE_SECONDARY_DB_URL を立てて _qrepo の配線経路を有効化")
    lo.add_argument("--tmpdir", default=None)

    sc = sub.add_parser("sqlcount", help="API 1 リクエストあたりの SQL / tx 回数を数える")
    sc.add_argument("--workloads", type=int, default=12)
    sc.add_argument("--batch", type=int, default=10)

    a = ap.parse_args()
    install_hooks()
    if a.cmd == "load":
        asyncio.run(cmd_load(a))
    else:
        cmd_sqlcount(a)


if __name__ == "__main__":
    main()
