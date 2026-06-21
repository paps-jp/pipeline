"""ShellExecutor のテンプレ展開 + subprocess 実行."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipeline.executors import ExecutionContext, Task, create_executor


def _ctx(tmp_path: Path, env: dict[str, str] | None = None) -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.utcnow() + timedelta(seconds=10),
        workdir=tmp_path,
        env=env or {},
        workload_config={},
    )


def _task(pk: str = "abc123", extra: dict | None = None, attempt: int = 1) -> Task:
    return Task(pk=pk, workload_slug="t", attempt=attempt, extra=extra or {})


def test_unknown_executor_raises() -> None:
    with pytest.raises(ValueError, match="unknown executor"):
        create_executor("does_not_exist", {})


def test_shell_requires_command_list() -> None:
    with pytest.raises(ValueError, match="non-empty list"):
        create_executor("shell", {"command": "echo hi"})  # string not allowed
    with pytest.raises(ValueError, match="non-empty list"):
        create_executor("shell", {})


def test_shell_echoes_task_pk(tmp_path: Path) -> None:
    ex = create_executor("shell", {"command": [sys.executable, "-c", "print('pk={task.pk}')"]})
    res = ex.run(_task(pk="x42"), _ctx(tmp_path))
    assert res.success is True
    assert res.exit_code == 0
    assert "pk=x42" in res.stdout


def test_shell_expands_extra_and_env(tmp_path: Path) -> None:
    ex = create_executor(
        "shell",
        {
            "command": [sys.executable, "-c", "import os; print(os.environ['MSG'])"],
            "env": {"MSG": "hello-{task.extra.name}-{env.SUFFIX}"},
        },
    )
    res = ex.run(_task(extra={"name": "world"}), _ctx(tmp_path, env={"SUFFIX": "ok"}))
    assert res.success
    assert "hello-world-ok" in res.stdout


def test_shell_non_zero_exit_marks_failure(tmp_path: Path) -> None:
    ex = create_executor("shell", {"command": [sys.executable, "-c", "raise SystemExit(7)"]})
    res = ex.run(_task(), _ctx(tmp_path))
    assert res.success is False
    assert res.exit_code == 7


def test_shell_command_not_found(tmp_path: Path) -> None:
    ex = create_executor("shell", {"command": ["this-binary-does-not-exist-xyz"]})
    res = ex.run(_task(), _ctx(tmp_path))
    assert res.success is False
    assert res.error and "not found" in res.error


def test_shell_timeout_marks_failure(tmp_path: Path) -> None:
    ex = create_executor(
        "shell",
        {
            "command": [sys.executable, "-c", "import time; time.sleep(5)"],
            "timeout_secs": 0.5,
        },
    )
    res = ex.run(_task(), _ctx(tmp_path))
    assert res.success is False
    assert res.error and "timeout" in res.error


def test_shell_truncates_long_stdout(tmp_path: Path) -> None:
    ex = create_executor(
        "shell",
        {
            "command": [sys.executable, "-c", "print('x' * 5000)"],
            "max_stdout_bytes": 100,
        },
    )
    res = ex.run(_task(), _ctx(tmp_path))
    assert res.success
    assert "truncated" in res.stdout
    assert len(res.stdout) < 5000


def test_shell_unknown_var_passes_through(tmp_path: Path) -> None:
    """未知の placeholder は素通しでも実行は続く。"""
    ex = create_executor(
        "shell", {"command": [sys.executable, "-c", "print('{unknown.thing}')"]}
    )
    res = ex.run(_task(), _ctx(tmp_path))
    assert res.success
    assert "{unknown.thing}" in res.stdout
