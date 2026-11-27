"""maintenance purge は event loop の外 (別スレッド) で回す。

2026-11-27: _maintenance_loop が repo.purge_all() を event loop 上で直に呼んでいたため、
plugin-thumb の 1 件ずつの remove_object (5 分毎に約 6,000 件) の間 :8001 全体が
7〜11 秒応答せず、5 秒 timeout の machine-status-export などが落ちていた。
"""

from __future__ import annotations

import asyncio
import threading

from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app
from pipeline.repositories.maintenance import MaintenanceRepository


def test_purge_all_runs_outside_the_event_loop(monkeypatch):
    seen: dict[str, bool] = {}
    ran = threading.Event()

    def fake_purge_all(self):
        try:
            asyncio.get_running_loop()
            seen["on_loop"] = True
        except RuntimeError:
            seen["on_loop"] = False
        ran.set()
        return {}

    monkeypatch.setattr(MaintenanceRepository, "purge_all", fake_purge_all)
    settings = Settings(db_url="sqlite:///:memory:", mode="dev")
    with TestClient(create_app(settings)) as c:
        assert ran.wait(10), "maintenance loop did not run purge_all"
        assert c.get("/api/v1/workers/cpu").status_code == 200
    assert seen["on_loop"] is False
