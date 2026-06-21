"""Executor 抽象基底クラス (Protocol)。

各 executor は run(task, ctx) → ExecutionResult を実装する。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol


@dataclass
class Task:
    pk: Any
    workload_slug: str
    attempt: int = 1
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionContext:
    deadline: datetime
    workdir: Path
    env: dict[str, str] = field(default_factory=dict)
    workload_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    success: bool
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    output_json: dict[str, Any] | None = None
    duration_ms: int = 0
    error: str | None = None


class Executor(Protocol):
    def __init__(self, config: dict[str, Any]) -> None: ...

    def run(self, task: Task, ctx: ExecutionContext) -> ExecutionResult: ...


class StatefulExecutor(Executor, Protocol):
    """worker プロセス内で長寿命の state (model 等) を持つ executor。

    drain loop が `Worker._executor_cache` で同一 workload に対する
    インスタンスを再利用する。close() は worker shutdown / config 変更時に呼ぶ。
    """

    def close(self) -> None: ...


# Plugin protocol (python_module executor が import する module の規約):
#
#   def setup(**init_kwargs) -> Any:
#       """worker 起動時に 1 度だけ呼ばれる。state を返す。
#       重 ML model は ここで load して state['model'] に格納。
#       """
#
#   def process(task: Task, ctx: ExecutionContext, state: Any) -> dict | None:
#       """1 task を実行。例外は drain loop で捕捉して fail 扱い。
#       戻り値は ExecutionResult.output_json に格納される (None でも可)。
#       """
#
#   def cleanup(state: Any) -> None:           # 任意
#       """worker shutdown / config 変更時に呼ばれる。
#       DB connection close, GPU memory 解放, ファイルハンドラ close 等。
#       """
