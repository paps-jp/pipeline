"""python_module executor のテスト.

tmp_path に simple plugin を書き、それを module_search_path で指して読み込む。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.executors import (
    ExecutionContext,
    ExecutionResult,
    PluginConfigError,
    PluginRuntimeError,
    PythonModuleExecutor,
    Task,
    create_executor,
)


def _ctx(tmp_path: Path) -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=10),
        workdir=tmp_path,
        env={},
        workload_config={},
    )


def _task(pk: str = "x", extra: dict | None = None) -> Task:
    return Task(pk=pk, workload_slug="t", attempt=0, extra=extra or {})


# ---------------- plugin source helpers ----------------

def _write_plugin(root: Path, name: str, body: str) -> None:
    """root/<name>/__init__.py に body を書く + main.py も同じ body。"""
    p = root / name
    p.mkdir(parents=True, exist_ok=True)
    (p / "__init__.py").write_text(body, encoding="utf-8")
    (p / "main.py").write_text(body, encoding="utf-8")


PLUGIN_BASIC = '''
def setup(**kwargs):
    return {"counter": 0, "init": dict(kwargs)}

def process(task, ctx, state):
    state["counter"] += 1
    return {"pk": task.pk, "count": state["counter"], "init": state["init"]}
'''

PLUGIN_NO_SETUP = '''
def process(task, ctx, state):
    return {"pk": task.pk, "state_is": state}
'''

PLUGIN_RAISES_IN_SETUP = '''
def setup(**kwargs):
    raise RuntimeError("setup boom")

def process(task, ctx, state):
    return None
'''

PLUGIN_RAISES_IN_PROCESS = '''
def setup(**kwargs):
    return None

def process(task, ctx, state):
    raise ValueError("process boom")
'''

PLUGIN_RETURNS_BOOL = '''
def process(task, ctx, state):
    return task.pk == "good"
'''

PLUGIN_WITH_CLEANUP = '''
_log = []
def setup(**kwargs):
    return {"id": "abc", "log": _log}

def process(task, ctx, state):
    state["log"].append("p:" + task.pk)
    return {"len": len(state["log"])}

def cleanup(state):
    state["log"].append("c")
'''


# ---------------- tests ----------------

def test_basic_plugin_state_persists(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_basic", PLUGIN_BASIC)
    ex = PythonModuleExecutor({
        "module": "p_basic",
        "module_search_path": str(tmp_path),
        "init_kwargs": {"gpu_id": 0, "name": "demo"},
    })
    r1 = ex.run(_task("a"), _ctx(tmp_path))
    r2 = ex.run(_task("b"), _ctx(tmp_path))
    r3 = ex.run(_task("c"), _ctx(tmp_path))
    assert r1.success and r1.output_json == {"pk": "a", "count": 1, "init": {"gpu_id": 0, "name": "demo"}}
    assert r2.output_json["count"] == 2
    assert r3.output_json["count"] == 3
    ex.close()


def test_plugin_without_setup_uses_none_state(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_no_setup", PLUGIN_NO_SETUP)
    ex = PythonModuleExecutor({
        "module": "p_no_setup",
        "module_search_path": str(tmp_path),
    })
    r = ex.run(_task("x"), _ctx(tmp_path))
    assert r.success
    assert r.output_json == {"pk": "x", "state_is": None}


def test_missing_module_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(PluginConfigError, match="cannot import"):
        PythonModuleExecutor({"module": "nope_does_not_exist_xyz", "module_search_path": str(tmp_path)})


def test_missing_callable_raises_config_error(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_no_callable", PLUGIN_BASIC)
    with pytest.raises(PluginConfigError, match="not callable"):
        PythonModuleExecutor({
            "module": "p_no_callable",
            "callable": "does_not_exist",
            "module_search_path": str(tmp_path),
        })


def test_setup_raising_is_propagated_as_runtime_error(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_setup_boom", PLUGIN_RAISES_IN_SETUP)
    with pytest.raises(PluginRuntimeError, match="setup raised"):
        PythonModuleExecutor({"module": "p_setup_boom", "module_search_path": str(tmp_path)})


def test_process_raising_marks_failure(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_proc_boom", PLUGIN_RAISES_IN_PROCESS)
    ex = PythonModuleExecutor({"module": "p_proc_boom", "module_search_path": str(tmp_path)})
    r = ex.run(_task(), _ctx(tmp_path))
    assert r.success is False
    assert r.error and "process boom" in r.error
    assert r.stderr and "ValueError" in r.stderr


def test_bool_return_drives_success_flag(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_bool", PLUGIN_RETURNS_BOOL)
    ex = PythonModuleExecutor({"module": "p_bool", "module_search_path": str(tmp_path)})
    assert ex.run(_task("good"), _ctx(tmp_path)).success is True
    assert ex.run(_task("bad"), _ctx(tmp_path)).success is False


def test_cleanup_called_on_close(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_cleanup", PLUGIN_WITH_CLEANUP)
    ex = PythonModuleExecutor({"module": "p_cleanup", "module_search_path": str(tmp_path)})
    ex.run(_task("a"), _ctx(tmp_path))
    ex.run(_task("b"), _ctx(tmp_path))
    # cleanup 前
    state = ex._state
    assert state["log"] == ["p:a", "p:b"]
    ex.close()
    # cleanup 後 → "c" が log に積まれてる
    assert state["log"] == ["p:a", "p:b", "c"]


def test_create_executor_via_factory(tmp_path: Path) -> None:
    _write_plugin(tmp_path, "p_via_factory", PLUGIN_BASIC)
    ex = create_executor("python_module", {
        "module": "p_via_factory",
        "module_search_path": str(tmp_path),
    })
    r = ex.run(_task("z"), _ctx(tmp_path))
    assert r.success
    assert r.output_json["pk"] == "z"


def test_max_duration_exceeded_marks_failure(tmp_path: Path) -> None:
    # plugin が 100ms かかる → max_duration_ms=1 で必ず超過
    body = '''
import time
def process(task, ctx, state):
    time.sleep(0.1)
    return {"ok": True}
'''
    _write_plugin(tmp_path, "p_slow", body)
    ex = PythonModuleExecutor({
        "module": "p_slow",
        "module_search_path": str(tmp_path),
        "max_duration_ms": 1,
    })
    r = ex.run(_task(), _ctx(tmp_path))
    assert r.success is False
    assert r.error and "max_duration_ms" in r.error
