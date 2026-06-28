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
from pipeline.api import admin_cmds, admin_deploy, dashboard, flow, plugin_runtime, plugins_local, service_logs, settings as settings_api, system, workers, workloads
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
                except Exception:
                    log.exception("vram aggregator failed")
                try:
                    await _asyncio.wait_for(_vram_stop.wait(), timeout=60)
                except _asyncio.TimeoutError:
                    pass
        vram_agg_task = _asyncio.create_task(_vram_aggregator_loop())

        try:
            yield
        finally:
            log.info("pipeline shutting down")
            _reaper_stop.set()
            _vram_stop.set()
            try:
                await _asyncio.wait_for(reaper_task, timeout=3)
            except Exception:
                pass
            try:
                await _asyncio.wait_for(vram_agg_task, timeout=3)
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
    app.include_router(plugin_runtime.router)
    app.include_router(settings_api.router)
    # MinIO プロキシ (= プラグイン UI が顔サムネ等を <img> で見るため)
    from pipeline.api import minio_proxy as _mp
    app.include_router(_mp.router)

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
