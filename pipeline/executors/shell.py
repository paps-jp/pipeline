"""Shell executor: subprocess.run で外部コマンドを起動する MVP 版。

config 例 (UI のプリセットと合わせる):

    {
        "command": ["echo", "{task.pk}"],   # list-of-str。各要素にテンプレ展開
        "cwd": null,                        # workdir に対する相対 / 絶対パス, optional
        "env": {"KEY": "VAL"},              # 環境変数 (テンプレ展開対象)
        "timeout_secs": 60,                 # default 60s
        "max_stdout_bytes": 1048576         # default 1 MiB で truncate
    }

テンプレ変数 (`{...}` 単中括弧、KeyError なら空文字に fallback):
- `{task.pk}` — task の primary key
- `{task.attempt}` — 何度目の試行か
- `{task.workload_slug}`
- `{task.extra.<col>}` — input_source が enqueue 時に詰めた extra 列
- `{env.<NAME>}` — ctx.env から
"""

from __future__ import annotations

import re
import subprocess
import time
from typing import Any

from .base import ExecutionContext, ExecutionResult, Task

_VAR_RE = re.compile(r"\{([a-zA-Z][a-zA-Z0-9_.]*)\}")

_DEFAULT_TIMEOUT_S = 60
_DEFAULT_MAX_STDOUT = 1024 * 1024


def _lookup(path: str, task: Task, ctx: ExecutionContext) -> str:
    """`{task.pk}` 等の path を解決。未知ならそのまま戻して、置換せずに残す。"""
    parts = path.split(".")
    head = parts[0]
    if head == "task":
        if len(parts) >= 2 and parts[1] == "extra" and len(parts) == 3:
            return str(task.extra.get(parts[2], ""))
        if len(parts) == 2 and parts[1] in {"pk", "attempt", "workload_slug"}:
            return str(getattr(task, parts[1]))
    elif head == "env" and len(parts) == 2:
        return str(ctx.env.get(parts[1], ""))
    elif head == "workload" and len(parts) == 2:
        return str(ctx.workload_config.get(parts[1], ""))
    return "{" + path + "}"


def _expand(template: str, task: Task, ctx: ExecutionContext) -> str:
    return _VAR_RE.sub(lambda m: _lookup(m.group(1), task, ctx), template)


def _truncate(b: bytes, max_bytes: int) -> str:
    if len(b) <= max_bytes:
        return b.decode("utf-8", errors="replace")
    return b[:max_bytes].decode("utf-8", errors="replace") + f"\n…[truncated {len(b) - max_bytes} bytes]"


class ShellExecutor:
    def __init__(self, config: dict[str, Any]) -> None:
        cmd = config.get("command")
        if not isinstance(cmd, list) or not cmd or not all(isinstance(x, str) for x in cmd):
            raise ValueError("shell executor: 'command' must be a non-empty list of strings")
        self._cmd_template: list[str] = list(cmd)
        self._cwd: str | None = config.get("cwd") or None
        self._env_template: dict[str, str] = dict(config.get("env") or {})
        self._timeout_s: float = float(config.get("timeout_secs") or _DEFAULT_TIMEOUT_S)
        self._max_stdout: int = int(config.get("max_stdout_bytes") or _DEFAULT_MAX_STDOUT)

    def run(self, task: Task, ctx: ExecutionContext) -> ExecutionResult:
        argv = [_expand(a, task, ctx) for a in self._cmd_template]
        env = {**ctx.env, **{k: _expand(v, task, ctx) for k, v in self._env_template.items()}}
        started = time.monotonic()
        try:
            cp = subprocess.run(
                argv,
                cwd=self._cwd or str(ctx.workdir),
                env=env,
                capture_output=True,
                timeout=self._timeout_s,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            return ExecutionResult(
                success=False,
                exit_code=None,
                stdout=_truncate(e.stdout or b"", self._max_stdout),
                stderr=_truncate(e.stderr or b"", self._max_stdout),
                duration_ms=int((time.monotonic() - started) * 1000),
                error=f"timeout after {self._timeout_s}s",
            )
        except FileNotFoundError as e:
            return ExecutionResult(
                success=False,
                duration_ms=int((time.monotonic() - started) * 1000),
                error=f"command not found: {e.filename or argv[0]}",
            )
        return ExecutionResult(
            success=cp.returncode == 0,
            exit_code=cp.returncode,
            stdout=_truncate(cp.stdout or b"", self._max_stdout),
            stderr=_truncate(cp.stderr or b"", self._max_stdout),
            duration_ms=int((time.monotonic() - started) * 1000),
        )
