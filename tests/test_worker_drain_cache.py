"""drain loop の executor cache テスト.

ポイント:
- python_module plugin の setup() が呼ばれるのは 1 度だけ (cache 効いてる)
- config を変えると executor が rebuild される (cleanup() が呼ばれる)
- workload を disable / 削除すると executor が解放される
- 複数 task で state が persist する
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from pipeline.db.sqlite import SqliteDatabase
from pipeline.models.workload import WorkloadCreate, queue_table_for
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import WorkloadRepository
from pipeline.worker.drain import Worker


PLUGIN_COUNTER = '''
_setup_count = [0]
_cleanup_count = [0]

def setup(**kwargs):
    _setup_count[0] += 1
    return {"setup_n": _setup_count[0], "processed": 0, "marker": kwargs.get("marker", "v0")}

def process(task, ctx, state):
    state["processed"] += 1
    return {"pk": task.pk, "n": state["processed"], "marker": state["marker"], "setup_n": state["setup_n"]}

def cleanup(state):
    _cleanup_count[0] += 1
    # cleanup の証跡として外部 file に書く (test 側で確認)
    import os
    log_path = os.environ.get("PLUGIN_LOG_PATH")
    if log_path:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"cleanup setup_n={state['setup_n']} processed={state['processed']}\\n")
'''


def _write_plugin(root: Path, name: str) -> None:
    p = root / name
    p.mkdir(parents=True, exist_ok=True)
    (p / "__init__.py").write_text(PLUGIN_COUNTER, encoding="utf-8")


@pytest.fixture()
def db():
    d = SqliteDatabase("sqlite:///:memory:")
    d.ensure_schema()
    yield d
    d.close()


def _make_workload(db, slug: str, *, marker: str, plugin_root: Path, plugin_name: str):
    repo = WorkloadRepository(db)
    return repo.create(
        WorkloadCreate(
            slug=slug,
            name=slug,
            enabled=True,
            executor_type="python_module",
            executor_config={
                "module": plugin_name,
                "module_search_path": str(plugin_root),
                "init_kwargs": {"marker": marker},
            },
            success_criteria={"type": "exit_code", "expected": 0},
            batch_size=10,
            lease_secs=30,
            max_attempts=2,
        )
    )


@pytest.mark.asyncio
async def test_setup_called_once_for_many_tasks(db, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PLUGIN_LOG_PATH", str(tmp_path / "cleanup.log"))
    plug_root = tmp_path / "plug"
    plug_root.mkdir()
    _write_plugin(plug_root, "p_counter_t1")
    _make_workload(db, "w1", marker="A", plugin_root=plug_root, plugin_name="p_counter_t1")
    qt = queue_table_for("w1")
    q = QueueRepository(db)
    runs = RunsRepository(db)
    for pk in ["a", "b", "c", "d", "e"]:
        q.enqueue(qt, pk)

    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    for _ in range(60):
        if q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)
    await w.stop()

    out = runs.list_for_workload("w1")
    assert len(out) == 5
    # setup_n は全て 1 (1 度しか setup されてない)
    assert all(r["output_json"]["setup_n"] == 1 for r in out)
    # processed は 1..5 のどこか (順序は drain order)
    ns = sorted(r["output_json"]["n"] for r in out)
    assert ns == [1, 2, 3, 4, 5]
    # stop 時に cleanup が呼ばれてるはず
    log_text = (tmp_path / "cleanup.log").read_text(encoding="utf-8") if (tmp_path / "cleanup.log").exists() else ""
    assert "cleanup" in log_text and "processed=5" in log_text


@pytest.mark.asyncio
async def test_config_change_rebuilds_executor(db, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PLUGIN_LOG_PATH", str(tmp_path / "cleanup.log"))
    plug_root = tmp_path / "plug"
    plug_root.mkdir()
    _write_plugin(plug_root, "p_counter_t2")
    repo = WorkloadRepository(db)
    _make_workload(db, "w2", marker="A", plugin_root=plug_root, plugin_name="p_counter_t2")
    qt = queue_table_for("w2")
    q = QueueRepository(db)
    runs = RunsRepository(db)
    q.enqueue(qt, "a")

    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    # 最初の task を処理させる
    for _ in range(30):
        if q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)

    # config を変えて marker B に
    from pipeline.models.workload import WorkloadUpdate
    cur = repo.get("w2")
    upd = WorkloadUpdate(
        name=cur.name,
        description=cur.description,
        enabled=True,
        executor_type=cur.executor_type,
        executor_config={**cur.executor_config, "init_kwargs": {"marker": "B"}},
        success_criteria=cur.success_criteria,
        priority=cur.priority,
        weight=cur.weight,
        batch_size=cur.batch_size,
        lease_secs=cur.lease_secs,
        max_attempts=cur.max_attempts,
        resources=cur.resources,
        host_affinity=cur.host_affinity,
        on_success=cur.on_success,
        on_failure=cur.on_failure,
    )
    repo.update("w2", upd)

    # 新しい task → rebuild されて marker=B + setup_n=2 で動く
    q.enqueue(qt, "b")
    for _ in range(30):
        if q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)
    await w.stop()

    out = sorted(runs.list_for_workload("w2"), key=lambda r: r["pk"])
    assert len(out) == 2
    assert out[0]["pk"] == "a" and out[0]["output_json"]["marker"] == "A"
    assert out[1]["pk"] == "b" and out[1]["output_json"]["marker"] == "B"
    # rebuild 時に cleanup が呼ばれているはず
    log_text = (tmp_path / "cleanup.log").read_text(encoding="utf-8") if (tmp_path / "cleanup.log").exists() else ""
    # marker A で 1 回 processed=1 cleanup + marker B で 1 回 processed=1 cleanup
    assert log_text.count("cleanup") >= 2


@pytest.mark.asyncio
async def test_disabling_workload_evicts_executor(db, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PLUGIN_LOG_PATH", str(tmp_path / "cleanup.log"))
    plug_root = tmp_path / "plug"
    plug_root.mkdir()
    _write_plugin(plug_root, "p_counter_t3")
    repo = WorkloadRepository(db)
    _make_workload(db, "w3", marker="A", plugin_root=plug_root, plugin_name="p_counter_t3")
    qt = queue_table_for("w3")
    q = QueueRepository(db)
    q.enqueue(qt, "a")

    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    # 1 件処理
    for _ in range(30):
        if q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)

    # cache 効いてる
    assert "w3" in w._executor_cache

    # disable
    repo.set_enabled("w3", False)
    # drain iteration を 1 周回させる
    await asyncio.sleep(0.3)

    # cache から evict されてる
    assert "w3" not in w._executor_cache
    await w.stop()

    log_text = (tmp_path / "cleanup.log").read_text(encoding="utf-8") if (tmp_path / "cleanup.log").exists() else ""
    assert "cleanup" in log_text


@pytest.mark.asyncio
async def test_plugin_setup_error_fails_tasks_and_does_not_cache(db, tmp_path: Path):
    plug_root = tmp_path / "plug"
    plug_root.mkdir()
    bad = plug_root / "p_bad"
    bad.mkdir()
    (bad / "__init__.py").write_text(
        "def setup(**k):\n    raise RuntimeError('boom')\n"
        "def process(task, ctx, state):\n    return None\n",
        encoding="utf-8",
    )
    repo = WorkloadRepository(db)
    repo.create(
        WorkloadCreate(
            slug="w_bad", name="bad", enabled=True,
            executor_type="python_module",
            executor_config={"module": "p_bad", "module_search_path": str(plug_root)},
            batch_size=10, lease_secs=30, max_attempts=2,
        )
    )
    qt = queue_table_for("w_bad")
    q = QueueRepository(db)
    runs = RunsRepository(db)
    q.enqueue(qt, "x")
    w = Worker(db, idle_sleep_s=0.05)
    await w.start()
    # max_attempts=1 扱いで fail させる仕様
    for _ in range(30):
        if q.count_by_state(qt).get("failed", 0) == 1 or q.count_by_state(qt) == {}:
            break
        await asyncio.sleep(0.1)
    await w.stop()

    assert "w_bad" not in w._executor_cache  # build 失敗で cache されない
    history = runs.list_for_workload("w_bad")
    assert len(history) == 1
    assert history[0]["success"] is False
    assert "boom" in (history[0]["error"] or "")
