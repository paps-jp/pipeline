"""In-process worker drain loop の E2E (shell executor + queue + runs).

CI でもポータブルに動かすため subprocess は `sys.executable -c "..."` のみ使う。
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from pipeline.db.sqlite import SqliteDatabase
from pipeline.models.workload import WorkloadCreate, queue_table_for
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import WorkloadRepository
from pipeline.worker.drain import Worker


@pytest.fixture()
def db():
    d = SqliteDatabase("sqlite:///:memory:")
    d.ensure_schema()
    yield d
    d.close()


def _make_echo_workload(db, *, max_attempts: int = 2, lease: int = 30) -> str:
    repo = WorkloadRepository(db)
    repo.create(
        WorkloadCreate(
            slug="echo",
            name="Echo",
            enabled=True,
            executor_type="shell",
            executor_config={
                "command": [sys.executable, "-c", "print('out={task.pk}')"],
                "timeout_secs": 10,
            },
            success_criteria={"type": "exit_code", "expected": 0},
            batch_size=10,
            lease_secs=lease,
            max_attempts=max_attempts,
        )
    )
    return queue_table_for("echo")


@pytest.mark.asyncio
async def test_worker_processes_enqueued_task(db):
    qt = _make_echo_workload(db)
    q = QueueRepository(db)
    runs = RunsRepository(db)
    q.enqueue(qt, "abc")

    w = Worker(db, idle_sleep_s=0.1)
    await w.start()
    # 完了まで poll (タイムアウト 5s)
    for _ in range(50):
        if q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)
    await w.stop()

    assert q.count_by_state(qt) == {}
    out = runs.list_for_workload("echo")
    assert len(out) == 1
    assert out[0]["pk"] == "abc"
    assert out[0]["success"] is True
    assert out[0]["exit_code"] == 0
    assert "out=abc" in out[0]["stdout"]


@pytest.mark.asyncio
async def test_worker_marks_failure_after_max_attempts(db):
    repo = WorkloadRepository(db)
    repo.create(
        WorkloadCreate(
            slug="boom",
            name="Boom",
            enabled=True,
            executor_type="shell",
            # 必ず exit 1
            executor_config={"command": [sys.executable, "-c", "raise SystemExit(1)"]},
            batch_size=10,
            lease_secs=30,
            max_attempts=2,
        )
    )
    qt = queue_table_for("boom")
    q = QueueRepository(db)
    runs = RunsRepository(db)
    q.enqueue(qt, "x")

    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    for _ in range(60):
        states = q.count_by_state(qt)
        if states.get("failed", 0) == 1:
            break
        await asyncio.sleep(0.1)
    await w.stop()

    assert q.count_by_state(qt) == {"failed": 1}
    history = runs.list_for_workload("boom")
    # max_attempts=2 なので 2 回 runs に記録される
    assert len(history) >= 2
    assert all(h["success"] is False for h in history)


@pytest.mark.asyncio
async def test_worker_skips_disabled_workload(db):
    repo = WorkloadRepository(db)
    repo.create(
        WorkloadCreate(
            slug="disabled",
            name="Disabled",
            enabled=False,
            executor_type="shell",
            executor_config={"command": [sys.executable, "-c", "print('hi')"]},
            batch_size=10,
            lease_secs=30,
            max_attempts=1,
        )
    )
    qt = queue_table_for("disabled")
    q = QueueRepository(db)
    q.enqueue(qt, "a")

    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    await asyncio.sleep(0.5)
    await w.stop()

    # disabled なので task は pending のまま
    assert q.count_by_state(qt) == {"pending": 1}
