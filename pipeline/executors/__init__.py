"""Executor 実装。MVP は shell のみ。

新しい executor を足したら EXECUTORS に登録する。
"""

from __future__ import annotations

from typing import Any

from .base import ExecutionContext, ExecutionResult, Executor, StatefulExecutor, Task
from .python_module import PluginConfigError, PluginRuntimeError, PythonModuleExecutor
from .shell import ShellExecutor

EXECUTORS: dict[str, type[Executor]] = {
    "shell": ShellExecutor,
    "python_module": PythonModuleExecutor,
}


def create_executor(executor_type: str, config: dict[str, Any]) -> Executor:
    cls = EXECUTORS.get(executor_type)
    if cls is None:
        raise ValueError(f"unknown executor type: {executor_type!r} (known: {sorted(EXECUTORS)})")
    return cls(config)


__all__ = [
    "EXECUTORS",
    "ExecutionContext",
    "ExecutionResult",
    "Executor",
    "PluginConfigError",
    "PluginRuntimeError",
    "PythonModuleExecutor",
    "ShellExecutor",
    "StatefulExecutor",
    "Task",
    "create_executor",
]
