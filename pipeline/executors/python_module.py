"""python_module executor: 自前 Python module を worker プロセス内で常駐させる。

重 ML 用 — 起動時に 1 度 model load → タスク毎に process() で再利用。

config 例 (= 新方式 / source_path):

    {
        "module": "dispatch_main",
        "source_path": "/opt/pipeline/plugins/crawl_image_dispatcher",
        "init_kwargs": {"interval_s": 30},
    }

config 例 (= 旧方式 / plugin_version cache 経由、 Phase C で廃止予定):

    {
        "module": "hash_detect.main",
        "module_search_path": "/var/cache/pipeline-plugins/hash-detect@7f3a91",
    }

`source_path` (= 新方式、 ローカルディレクトリ直接指定) or
`module_search_path` (= 旧方式、 Plugin Registry が cache した path)
を sys.path 先頭に追加し、`module` を importlib で load する。

Plugin module の規約は executors/base.py のコメント参照。
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable

from .base import ExecutionContext, ExecutionResult, Task

log = logging.getLogger("pipeline.executors.python_module")


class PluginConfigError(ValueError):
    """plugin の config が不正、or module が見つからない。"""


class PluginRuntimeError(RuntimeError):
    """setup / process が例外を投げた。"""


class PythonModuleExecutor:
    """worker プロセス内で plugin を常駐させる stateful executor。"""

    def __init__(self, config: dict[str, Any]) -> None:
        module_name = config.get("module")
        if not isinstance(module_name, str) or not module_name:
            raise PluginConfigError("python_module executor: 'module' is required (str)")
        self._module_name = module_name
        self._callable_name: str = config.get("callable", "process")
        self._init_kwargs: dict[str, Any] = dict(config.get("init_kwargs") or {})
        self._max_duration_ms: int = int(config.get("max_duration_ms") or 0)
        # source_path (= 新方式 / Phase A〜) を優先、 module_search_path (= 旧方式 / Phase C で廃止) は後方互換
        self._search_path: str | None = (
            config.get("source_path") or config.get("module_search_path") or None
        )

        # plugin module 名空間の衝突防止:
        # 複数 plugin が同名 module (例: 2 つの dispatcher が両方 `dispatch_main.py`)
        # を持つと、 importlib.import_module() は `sys.modules` キャッシュを共有する
        # ので、 最初に load された側のコードが両方の workload で動いてしまう
        # (= 2026-06-23 video-dispatcher が image dispatcher のコードを実行する
        #    incident で発覚)。
        #
        # 対策: source_path 指定時は `spec_from_file_location` + plugin slug 入りの
        # unique `sys.modules` キー (`_pipeline_plugin_<slug>_<module>`) で load し、
        # キャッシュ衝突を物理的に切り離す。
        # source_path が無い旧方式 (= module_search_path) はそのまま import_module。
        sp_resolved: str | None = None
        if self._search_path:
            sp_resolved = str(Path(self._search_path).expanduser().resolve())
            if sp_resolved not in sys.path:
                # plugin 内 sub-import (from .lib import xxx 等) のため sys.path にも追加。
                # ただし top-level module は下の spec ベース load で別キーに置くので
                # 衝突は起きない。
                sys.path.insert(0, sp_resolved)
        try:
            if sp_resolved:
                plugin_slug = Path(sp_resolved).name  # e.g. "crawl_video_dispatcher"
                unique_key = f"_pipeline_plugin_{plugin_slug}_{module_name}"
                module_file = Path(sp_resolved) / f"{module_name}.py"
                if not module_file.exists():
                    # `.py` ファイルがない場合は package (= __init__.py) の可能性
                    pkg_dir = Path(sp_resolved) / module_name
                    pkg_init = pkg_dir / "__init__.py"
                    if pkg_init.exists():
                        module_file = pkg_init
                    else:
                        raise PluginConfigError(
                            f"python_module executor: cannot find "
                            f"{module_name!r} under {sp_resolved!r}"
                        )
                spec = importlib.util.spec_from_file_location(
                    unique_key, str(module_file)
                )
                if spec is None or spec.loader is None:
                    raise PluginConfigError(
                        f"python_module executor: spec_from_file_location failed for "
                        f"{module_file}"
                    )
                self._module = importlib.util.module_from_spec(spec)
                # exec 前に sys.modules に登録 → plugin 内の sub-import が正しく解決
                sys.modules[unique_key] = self._module
                # `module_name` でも参照したい sub-import 用に top-level alias を立てる
                # (= 例: `from dispatch_main import _foo` 形式の self-reference 救済)。
                # ただし、 上書きすると他 plugin の同名 module 参照を壊すので、 既存
                # entry がある場合は alias を作らず unique_key だけ生かす。
                sys.modules.setdefault(module_name, self._module)
                try:
                    spec.loader.exec_module(self._module)
                except Exception:
                    sys.modules.pop(unique_key, None)
                    raise
            else:
                # 旧方式: module_search_path のみ → import_module で OK
                try:
                    self._module = importlib.import_module(module_name)
                except ImportError as e:
                    raise PluginConfigError(
                        f"python_module executor: cannot import {module_name!r}: {e}"
                    ) from e
        finally:
            # sp_resolved は sys.path に残す (= plugin sub-import の解決経路)
            pass

        process_fn = getattr(self._module, self._callable_name, None)
        if not callable(process_fn):
            raise PluginConfigError(
                f"python_module executor: {module_name}.{self._callable_name} is not callable"
            )
        self._process_fn: Callable[..., Any] = process_fn

        setup_fn = getattr(self._module, "setup", None)
        try:
            self._state: Any = setup_fn(**self._init_kwargs) if callable(setup_fn) else None
        except Exception as e:
            raise PluginRuntimeError(
                f"plugin {module_name}.setup raised: {e}\n{traceback.format_exc()}"
            ) from e

        log.info(
            "python_module executor ready: module=%s callable=%s state=%s",
            module_name, self._callable_name, type(self._state).__name__ if self._state is not None else None,
        )

    def run(self, task: Task, ctx: ExecutionContext) -> ExecutionResult:
        started = time.monotonic()
        try:
            out = self._process_fn(task, ctx, state=self._state)
        except Exception as e:
            return ExecutionResult(
                success=False,
                duration_ms=int((time.monotonic() - started) * 1000),
                error=f"plugin {self._module_name}.{self._callable_name} raised: {e}",
                stderr=traceback.format_exc()[:8192],
            )

        elapsed_ms = int((time.monotonic() - started) * 1000)
        if self._max_duration_ms and elapsed_ms > self._max_duration_ms:
            return ExecutionResult(
                success=False,
                duration_ms=elapsed_ms,
                error=f"plugin exceeded max_duration_ms={self._max_duration_ms} (actual={elapsed_ms})",
                output_json=out if isinstance(out, dict) else None,
            )

        # 戻り値の型 normalize:
        #   - dict       → output_json に
        #   - None       → 単なる成功
        #   - bool       → success フラグ上書き
        #   - ExecutionResult を直接返す plugin もサポート
        if isinstance(out, ExecutionResult):
            # plugin が ExecutionResult を作って返す高度なケース
            if out.duration_ms == 0:
                out.duration_ms = elapsed_ms
            return out
        if isinstance(out, bool):
            return ExecutionResult(success=out, duration_ms=elapsed_ms)
        return ExecutionResult(
            success=True,
            duration_ms=elapsed_ms,
            output_json=out if isinstance(out, dict) else None,
        )

    def supports_batch(self) -> bool:
        """plugin が process_batch(tasks, ctx, state) を定義してれば batch 実行可能。"""
        fn = getattr(self._module, "process_batch", None)
        return callable(fn)

    def run_batch(self, tasks: list[Task], ctx: ExecutionContext) -> list[ExecutionResult]:
        """N task を一括処理。

        plugin に `process_batch(tasks, ctx, state) -> list[dict]` がある場合に呼ぶ。
        戻り値は tasks と同じ長さの list で、 各要素は output_json (dict) 想定。
        plugin 内で例外を投げると **全 task が fail** 扱い (= retry のリスクあり)。
        部分失敗を表現したい時は plugin が個別に `{'_error': str}` 等を入れて返す。
        """
        if not self.supports_batch():
            # backward compat: 1 件ずつ run() を呼ぶ
            return [self.run(t, ctx) for t in tasks]

        started = time.monotonic()
        batch_fn: Callable[..., Any] = getattr(self._module, "process_batch")
        try:
            outs = batch_fn(tasks, ctx, state=self._state)
        except Exception as e:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            err = f"plugin {self._module_name}.process_batch raised: {e}"
            tb = traceback.format_exc()[:8192]
            # 全 task fail
            return [ExecutionResult(success=False, duration_ms=elapsed_ms, error=err, stderr=tb)
                    for _ in tasks]

        elapsed_ms = int((time.monotonic() - started) * 1000)
        if not isinstance(outs, list) or len(outs) != len(tasks):
            err = (f"plugin {self._module_name}.process_batch: 戻り値が list[len={len(tasks)}] でない "
                   f"(got type={type(outs).__name__} len={len(outs) if hasattr(outs, '__len__') else '?'})")
            return [ExecutionResult(success=False, duration_ms=elapsed_ms, error=err) for _ in tasks]

        results: list[ExecutionResult] = []
        per_ms = elapsed_ms // max(1, len(tasks))
        for out in outs:
            if isinstance(out, ExecutionResult):
                if out.duration_ms == 0:
                    out.duration_ms = per_ms
                results.append(out)
            elif isinstance(out, dict):
                success = True
                err = None
                # plugin が個別失敗を表現する規約: {'_error': '...'}
                if "_error" in out:
                    success = False
                    err = str(out.get("_error"))[:500]
                results.append(ExecutionResult(success=success, duration_ms=per_ms,
                                               output_json=out, error=err))
            elif out is None:
                results.append(ExecutionResult(success=True, duration_ms=per_ms))
            else:
                results.append(ExecutionResult(success=True, duration_ms=per_ms,
                                               output_json={"result": out}))
        return results

    def close(self) -> None:
        """worker shutdown / config 変更時に呼ぶ。plugin の cleanup() を呼ぶ。"""
        cleanup_fn = getattr(self._module, "cleanup", None)
        if callable(cleanup_fn):
            try:
                cleanup_fn(self._state)
            except Exception:
                log.exception("plugin %s.cleanup raised; ignored", self._module_name)
        self._state = None
