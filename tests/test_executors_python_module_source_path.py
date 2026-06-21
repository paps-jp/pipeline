"""PythonModuleExecutor の source_path 経路 test (= Phase A の新方式)."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from pipeline.executors import create_executor
from pipeline.executors.base import ExecutionContext, Task
from pipeline.executors.python_module import PluginConfigError


@pytest.fixture()
def echo_plugin(tmp_path: Path) -> Path:
    """tmp に echo plugin (= setup + process + cleanup) を作って path を返す。"""
    plugin_dir = tmp_path / "echo_plugin"
    plugin_dir.mkdir()
    (plugin_dir / "main.py").write_text(textwrap.dedent("""
        def setup(**kwargs):
            return {"prefix": kwargs.get("prefix", "echo")}

        def process(task, ctx, state):
            return {"pk": task.pk, "msg": f"{state['prefix']}:{task.pk}"}

        def cleanup(state):
            state.clear()
    """).strip(), encoding="utf-8")
    return plugin_dir


def test_source_path_imports_local_module(echo_plugin: Path):
    """source_path 指定でローカルディレクトリから import + setup/process が動く。"""
    ex = create_executor("python_module", {
        "source_path": str(echo_plugin),
        "module": "main",
        "init_kwargs": {"prefix": "TEST"},
    })
    task = Task(pk="task-1", workload_slug="echo", attempt=0, extra={})
    ctx = ExecutionContext(deadline=None, workdir=Path("."), env={}, workload_config={})
    result = ex.run(task, ctx)
    assert result.success
    assert result.output_json == {"pk": "task-1", "msg": "TEST:task-1"}
    ex.close()


def test_source_path_takes_precedence_over_module_search_path(tmp_path: Path):
    """source_path と module_search_path 両方あれば source_path 優先。"""
    p1 = tmp_path / "from_source_path"
    p1.mkdir()
    (p1 / "winner.py").write_text("def process(t, c, state): return {'src': 'source_path'}\n", encoding="utf-8")
    p2 = tmp_path / "from_module_search_path"
    p2.mkdir()
    (p2 / "winner.py").write_text("def process(t, c, state): return {'src': 'module_search_path'}\n", encoding="utf-8")

    # winner module の重複を避けるため、 既存 import を消す
    sys.modules.pop("winner", None)
    try:
        ex = create_executor("python_module", {
            "source_path": str(p1),
            "module_search_path": str(p2),
            "module": "winner",
        })
        task = Task(pk="x", workload_slug="w", attempt=0, extra={})
        ctx = ExecutionContext(deadline=None, workdir=Path("."), env={}, workload_config={})
        result = ex.run(task, ctx)
        assert result.success
        assert result.output_json == {"src": "source_path"}
    finally:
        sys.modules.pop("winner", None)


def test_source_path_missing_module_raises(tmp_path: Path):
    """source_path 配下に module が無いと PluginConfigError。"""
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(PluginConfigError, match="cannot import"):
        create_executor("python_module", {
            "source_path": str(empty),
            "module": "does_not_exist",
        })
