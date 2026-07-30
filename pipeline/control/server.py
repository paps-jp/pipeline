"""FastAPI app — REST API + 静的 web UI 配信。

control plane の status server に相当。dispatcher / optimizer の loop は
別 thread で同居予定 (control mode の時、F2 以降)。
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from pipeline import __version__
from pipeline.api import admin_cmds, admin_deploy, agents as agents_api, dashboard, fleet, flow, host_policy as host_policy_api, plugin_runtime, plugins_local, service_logs, settings as settings_api, system, workers, workloads
from pipeline.config import Settings
from pipeline.db import get_db
from pipeline.worker.drain import Worker

log = logging.getLogger("pipeline.control.server")

# 開発時に React ビルド出力を置く想定の場所
_WEB_STATIC_DIR = Path(__file__).resolve().parents[1] / "web" / "static"


_FALLBACK_HTML = """<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<title>Pipeline</title>
<style>
  body { font-family: -apple-system, "Segoe UI", "Hiragino Sans", sans-serif;
         max-width: 720px; margin: 4rem auto; padding: 0 1rem; color: #1f2937; }
  h1 { font-size: 1.5rem; margin-bottom: .25rem; }
  .sub { color: #6b7280; margin-bottom: 2rem; }
  a { color: #4338ca; text-decoration: none; }
  a:hover { text-decoration: underline; }
  li { margin: .5rem 0; }
  code { background: #f3f4f6; padding: .1rem .35rem; border-radius: 4px; font-size: .85em; }
</style></head>
<body>
<h1>Pipeline %(version)s</h1>
<div class="sub">React 管理画面はまだビルドされていません (web/static/ が空)。</div>

<h2>使えるリンク</h2>
<ul>
  <li>API ドキュメント (Swagger): <a href="/docs">/docs</a></li>
  <li>OpenAPI スキーマ: <a href="/openapi.json">/openapi.json</a></li>
  <li>ヘルスチェック: <a href="/api/v1/health">/api/v1/health</a></li>
  <li>システム状態: <a href="/api/v1/status">/api/v1/status</a></li>
  <li>Workload 一覧: <a href="/api/v1/workloads">/api/v1/workloads</a></li>
</ul>

<h2>React UI 開発を始める</h2>
<pre><code>cd web
npm install
npm run dev   # http://localhost:5173 (Vite が /api を proxy)</code></pre>

<h2>本番ビルド</h2>
<pre><code>cd web
npm run build   # 出力は ../pipeline/web/static/</code></pre>
</body></html>
"""


def create_app(settings: Settings) -> FastAPI:
    """FastAPI app を組み立てる。"""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("pipeline starting: db=%s mode=%s", settings.db_url, settings.mode)
        db = get_db(settings.db_url)
        db.ensure_schema()
        app.state.settings = settings
        app.state.db = db
        # 業務 queue 用 secondary DB (= MariaDB)。 PIPELINE_SECONDARY_DB_URL 未設定なら
        # None = SQLite-only (= 後方互換)。 workload.queue_backend='mariadb' の queue だけ
        # QueueRepository.wire_from_workloads 経由でこちらに振り替わる。
        secondary_db = None
        _secondary_url = os.environ.get("PIPELINE_SECONDARY_DB_URL", "").strip()
        if _secondary_url:
            secondary_db = get_db(_secondary_url)
            log.info("secondary queue DB enabled (scheme=%s)", _secondary_url.split(":", 1)[0])
        app.state.secondary_db = secondary_db
        # control plane の log を service_logs に直接書く (= UI Pipeline タブで表示用)
        from pipeline.control.local_log_handler import attach_control_plane_logger
        attach_control_plane_logger(db, service="pipeline-oss-control")
        # in-process worker は dev mode のみ default 有効。 本番 (mode=control) は
        # worker daemon (`pipeline worker --control-url ...`) を別プロセスで起動する想定。
        # 明示的に env で上書き可: PIPELINE_INPROC_WORKER=1 / 0
        inproc_default = "1" if settings.mode == "dev" else "0"
        inproc_enabled = os.environ.get("PIPELINE_INPROC_WORKER", inproc_default) == "1"
        worker = None
        if inproc_enabled:
            log.info("starting in-process worker (set PIPELINE_INPROC_WORKER=0 to disable)")
            worker = Worker(db, secondary_db=secondary_db)
            await worker.start()
            app.state.worker = worker
        else:
            log.info("in-process worker disabled; use `pipeline worker --control-url ...` daemon")
            app.state.worker = None

        # stale worker reaper: 60s ごとに hb 止 worker を state=lost に、 10 min 古ければ DELETE
        import asyncio as _asyncio
        from pipeline.repositories.workers import WorkerRepository
        _reaper_stop = _asyncio.Event()
        async def _reaper_loop() -> None:
            repo = WorkerRepository(db)
            while not _reaper_stop.is_set():
                try:
                    # lost 化は 60s 後、 完全 DELETE は 180s 後 (= 旧 600s だと再起動後の
                    # 旧 worker が 10 分残り Workers 画面のヘッダ件数が膨らむ問題への対処)
                    r = repo.prune_stale(lost_after_s=60, delete_after_s=180)
                    if r["marked_lost"] or r["deleted"]:
                        log.info("workers reaper: lost=%d deleted=%d", r["marked_lost"], r["deleted"])
                except Exception:
                    log.exception("workers reaper failed")
                try:
                    await _asyncio.wait_for(_reaper_stop.wait(), timeout=60)
                except _asyncio.TimeoutError:
                    pass
        reaper_task = _asyncio.create_task(_reaper_loop())

        # self_loop workload watchdog: idle dispatcher 自動 bootstrap
        from pipeline.control.self_loop_watchdog import SelfLoopWatchdog
        selfloop_watchdog = SelfLoopWatchdog(db)
        selfloop_watchdog.start()

        # VRAM aggregator: vram_observations から workloads.observed_vram_mb_avg/p95
        # を 60s 周期で再計算 + 60min より古い raw を prune (= 配置設計用データ)
        from pipeline.repositories.workloads import WorkloadRepository as _WR
        _vram_stop = _asyncio.Event()
        async def _vram_aggregator_loop() -> None:
            wlrepo = _WR(db)
            while not _vram_stop.is_set():
                try:
                    updated = wlrepo.aggregate_vram_avg_p95(window_minutes=60)
                    pruned = wlrepo.prune_vram_observations(retain_minutes=60)
                    if updated or pruned:
                        log.info("vram aggregator: updated=%d pruned=%d",
                                 updated, pruned)
                    # observed_vram_mb_peak の poison-pill 自己修復 (2026-07-26):
                    # robust 再推定 + stale 減衰 + hard ceiling で「化けた peak が居座り
                    # workload を全 host から締め出す」 現象を毎周期補正する。
                    reconciled = wlrepo.reconcile_vram_peaks()
                    if reconciled:
                        log.warning(
                            "vram peak reconcile: %s",
                            [(c["slug"], c["from"], c["to"], c["reason"])
                             for c in reconciled],
                        )
                except Exception:
                    log.exception("vram aggregator failed")
                try:
                    await _asyncio.wait_for(_vram_stop.wait(), timeout=60)
                except _asyncio.TimeoutError:
                    pass
        vram_agg_task = _asyncio.create_task(_vram_aggregator_loop())

        # metric rate aggregator (2026-06-30): 30s 周期で runs.output_json から
        # WORKLOAD_METRIC_FIELDS を SUM して workloads.observed_rate に書く。
        # 1 run = 1 件想定の slug (= 未宣言) は throughput_counts (= runs/min) を
        # そのまま rate として書く。 flow.py が読み出して「捌いた件数/min」 表示。
        from pipeline.models.metric_fields import WORKLOAD_METRIC_FIELDS as _WMF
        from pipeline.repositories.runs import RunsRepository as _RR
        import datetime as _dt
        _metric_stop = _asyncio.Event()
        async def _metric_aggregator_loop() -> None:
            wlrepo = _WR(db)
            runsrepo = _RR(db)
            # 窓 20min / 20 で「件/min 平均」 にする。 paprika-links-pull のように
            # sleep 含む 1 tick が 10min 超かかる workload でも前の completed tick が
            # 必ず窓内に入り tpm が 0 にならない (= 5min だと started_at が窓外になる)。
            WINDOW_MIN = 20
            while not _metric_stop.is_set():
                try:
                    since = (_dt.datetime.utcnow() -
                             _dt.timedelta(minutes=WINDOW_MIN)).isoformat()
                    # 1) SUM(field) base = 宣言ある slug
                    summed = runsrepo.metric_sum_by_slug(_WMF, since)
                    # 2) runs/min fallback = 未宣言 slug (= 1 run = 1 件)
                    counts = runsrepo.throughput_counts(since)
                    rates: dict[str, float] = {}
                    for slug, cnt in counts.items():
                        if slug in _WMF:
                            rates[slug] = float(summed.get(slug, 0.0)) / WINDOW_MIN
                        else:
                            rates[slug] = float(cnt) / WINDOW_MIN
                    # 宣言あるが直近窓に run が無い slug は 0 を書いて idle 化
                    for slug in _WMF.keys():
                        rates.setdefault(slug, 0.0)
                    n = wlrepo.update_observed_rates(rates)
                    if n:
                        log.debug("metric aggregator: updated=%d", n)
                except Exception:
                    log.exception("metric aggregator failed")
                try:
                    await _asyncio.wait_for(_metric_stop.wait(), timeout=30)
                except _asyncio.TimeoutError:
                    pass
        metric_agg_task = _asyncio.create_task(_metric_aggregator_loop())

        # maintenance loop: 各テーブルの定期 purge を 5 分周期で実行。
        # 追加/変更は pipeline/repositories/maintenance.py のみ編集すればよい。
        from pipeline.repositories.maintenance import MaintenanceRepository as _MR
        _maintenance_stop = _asyncio.Event()
        async def _maintenance_loop() -> None:
            repo = _MR(db)
            while not _maintenance_stop.is_set():
                try:
                    deleted = repo.purge_all()
                    nonempty = {k: v for k, v in deleted.items() if v}
                    if nonempty:
                        log.info("maintenance purge: %s", nonempty)
                except Exception:
                    log.exception("maintenance purge failed")
                try:
                    await _asyncio.wait_for(_maintenance_stop.wait(), timeout=300)
                except _asyncio.TimeoutError:
                    pass
        maintenance_task = _asyncio.create_task(_maintenance_loop())

        # flow_rate_1m aggregator (2026-07-07): 60s 毎に「直近数分」の各 slug rate を
        # runs (index 済) から集約して flow_rate_1m へ upsert。 flow API はこの表を引くので
        # 複数ブラウザのアクセスでも runs を再スキャンしない (= サーバ 1 分 1 回集約=キャッシュ)。
        # long-format のためノード増減は行追加で吸収。 late-finishing run を拾うため直近3分を再集約。
        _flow_rate_stop = _asyncio.Event()
        async def _flow_rate_1m_loop() -> None:
            from pipeline.repositories.flow_rate import FlowRateRepository as _FRR
            frepo = _FRR(db)
            while not _flow_rate_stop.is_set():
                try:
                    base = _dt.datetime.now(_dt.timezone.utc).replace(second=0, microsecond=0)
                    for k in range(1, 4):   # 直近3分を再集約 (upsert で late 完了を反映)
                        frepo.sample_minute(base - _dt.timedelta(minutes=k), _WMF)
                except Exception:
                    log.exception("flow_rate_1m aggregator failed")
                try:
                    await _asyncio.wait_for(_flow_rate_stop.wait(), timeout=60)
                except _asyncio.TimeoutError:
                    pass
        flow_rate_task = _asyncio.create_task(_flow_rate_1m_loop())

        # paprika-job-submit のフロー表示を、 pipeline の submitted 件数でなく hub が
        # 直近60秒に受け入れた job 数に置換する (2026-07-08)。
        # submitted は failed_other(hub timeout)/adopted 等のノイズを含み、 かつ legacy
        # crawl fleet 分を含まない。 hub の受入数が「実効的に捌かれ始めた数」。
        # 2026-07-09: 旧実装は /jobs?status=running & completed,failed を limit=2000 で
        # 引き started_at∈[now-60s] を手カウントしていたが、 2クエリの一方が単発 empty を
        # 返すと後勝ち upsert で分バケットが 0 に化ける欠陥があった。 Paprika が権威 endpoint
        # GET /jobs/throughput?window_s=60 (accepted_last_min, source=redis) を提供したので
        # それを 1 回叩くだけに置換 (単発 empty→0 化・limit truncation を根絶)。
        # hub の所在はサイト固有なので env 必須。 未設定ならこのループは起動しない。
        _hub_rate_stop = _asyncio.Event()
        _hub_url = (os.environ.get("PAPRIKA_HUB") or "").rstrip("/")

        async def _hub_submit_rate_loop() -> None:
            import httpx as _httpx
            from pipeline.repositories.flow_rate import FlowRateRepository as _FRR
            frepo = _FRR(db)
            while not _hub_rate_stop.is_set():
                try:
                    now = _dt.datetime.utcnow()
                    async with _httpx.AsyncClient(timeout=15.0) as cli:
                        r = await cli.get(f"{_hub_url}/jobs/throughput?window_s=60")
                    r.raise_for_status()
                    j = r.json()
                    # accepted_last_min を主に、無ければ accepted_last_60s をフォールバック。
                    total = j.get("accepted_last_min")
                    if total is None:
                        total = j.get("accepted_last_60s", 0)
                    frepo.upsert(now, "paprika-job-submit",
                                 "hub_started_per_min", float(total))
                except Exception:
                    log.exception("hub submit rate loop failed")
                try:
                    await _asyncio.wait_for(_hub_rate_stop.wait(), timeout=30)
                except _asyncio.TimeoutError:
                    pass
        hub_rate_task = (_asyncio.create_task(_hub_submit_rate_loop())
                         if _hub_url else None)
        if not _hub_url:
            log.info("PAPRIKA_HUB 未設定のため hub submit rate loop は起動しない")

        # ストレージ使用量 (2026-07-21): RAM ディスク MinIO (.47 画像 / .48 動画) と
        # .17 raw の容量を 60s 毎に storage_capacity へ upsert する。 flow の tank ノードは
        # metric_sql でこの表を引くだけなので、 snapshot API は MinIO を一切叩かない。
        #
        # ここを snapshot 内で同期呼び出しにしてはいけない: 2026-07-20 に .47 が
        # 「TCP は受けるが HTTP 無応答」 のハングを起こしており、 その状態で snapshot が
        # MinIO を待つとダッシュボード全体が固まる。 収集は必ず此処で隔離する。
        _storage_stop = _asyncio.Event()

        async def _storage_capacity_loop() -> None:
            from pipeline.repositories.storage_capacity import (
                StorageCapacityRepository as _SCR,
            )
            while not _storage_stop.is_set():
                try:
                    # MinIO SDK は同期 API なのでスレッドへ逃がす (event loop を塞がない)。
                    await _asyncio.to_thread(_SCR.collect_and_upsert)
                except Exception:
                    log.exception("storage capacity loop failed")
                try:
                    await _asyncio.wait_for(_storage_stop.wait(), timeout=60)
                except _asyncio.TimeoutError:
                    pass
        storage_task = _asyncio.create_task(_storage_capacity_loop())

        try:
            yield
        finally:
            log.info("pipeline shutting down")
            _reaper_stop.set()
            _vram_stop.set()
            _metric_stop.set()
            _maintenance_stop.set()
            _flow_rate_stop.set()
            _hub_rate_stop.set()
            _storage_stop.set()
            try:
                await _asyncio.wait_for(flow_rate_task, timeout=3)
            except Exception:
                pass
            try:
                if hub_rate_task is not None:
                    await _asyncio.wait_for(hub_rate_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(storage_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(reaper_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(vram_agg_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(metric_agg_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(maintenance_task, timeout=3)
            except Exception:
                pass
            try:
                await selfloop_watchdog.stop()
            except Exception:
                pass
            if worker is not None:
                await worker.stop()
            db.close()

    app = FastAPI(
        title="Pipeline",
        version=__version__,
        description="GUI-first batch fleet for non-programmers.",
        lifespan=lifespan,
    )

    # dev では React 開発サーバ (Vite, 5173) からの xhr を許可
    if settings.mode == "dev":
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # routers
    app.include_router(system.router)
    app.include_router(workloads.router)
    app.include_router(plugins_local.router)
    app.include_router(workers.router)
    app.include_router(service_logs.router)
    app.include_router(admin_deploy.router)
    app.include_router(admin_deploy.bootstrap_router)
    app.include_router(admin_cmds.router)
    app.include_router(admin_cmds.admin_router)
    app.include_router(dashboard.router)
    app.include_router(flow.router)
    app.include_router(agents_api.router)
    app.include_router(plugin_runtime.router)
    app.include_router(settings_api.router)
    app.include_router(host_policy_api.router)
    app.include_router(fleet.router)
    # MinIO プロキシ (= プラグイン UI が顔サムネ等を <img> で見るため)
    from pipeline.api import minio_proxy as _mp
    app.include_router(_mp.router)
    # 外部投入 API (= Paprika クローラー以外からの画像/動画投入)。 /api/v1/create だけが
    # API キー必須で、 /api/v1/api-keys は他の管理 API と同じ LAN 信頼扱い。
    from pipeline.api_public import api_keys as _ak
    from pipeline.api_public import create as _cr
    app.include_router(_cr.router)
    app.include_router(_ak.router)

    # 静的アセット (React build が存在する時のみマウント)
    if _WEB_STATIC_DIR.exists() and any(_WEB_STATIC_DIR.iterdir()):
        app.mount("/assets", StaticFiles(directory=_WEB_STATIC_DIR / "assets"),
                  name="assets")

        # root 直下に置いた static (logo.png / favicon.ico 等) を返す
        # spa_fallback より先に判定する。 ディレクトリ traversal は
        # ".." を含む path で防ぐ (= FastAPI が path param に許可しても
        # resolve 後に _WEB_STATIC_DIR 外なら 404)。
        from fastapi.responses import FileResponse

        @app.get("/", include_in_schema=False)
        @app.get("/{path:path}", include_in_schema=False)
        def spa_fallback(path: str = "") -> "HTMLResponse | FileResponse":
            if path and "/" not in path and ".." not in path:
                candidate = (_WEB_STATIC_DIR / path).resolve()
                try:
                    candidate.relative_to(_WEB_STATIC_DIR.resolve())
                except ValueError:
                    candidate = None
                if candidate and candidate.is_file():
                    return FileResponse(candidate)
            index = _WEB_STATIC_DIR / "index.html"
            if index.exists():
                return HTMLResponse(index.read_text(encoding="utf-8"))
            return HTMLResponse(_FALLBACK_HTML % {"version": __version__})
    else:

        @app.get("/", include_in_schema=False)
        def index() -> HTMLResponse:
            return HTMLResponse(_FALLBACK_HTML % {"version": __version__})

    return app


def app_factory() -> FastAPI:
    """uvicorn 用の factory (`pipeline.control.server:app_factory`)."""
    settings = Settings.from_env()
    return create_app(settings)
