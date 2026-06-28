"""ローカル plugin ディレクトリの listing endpoint.

UI WorkloadFormModal の plugin/module 選択 dropdown + dynamic form のデータ源。
{PIPELINE_PLUGIN_ROOT} (= 既定 /opt/pipeline/plugins) を ls して各 plugin の
slug / path / modules / has_requirements / manifest を返す。

manifest は plugin/<slug>/plugin.yaml を parse した結果 (= init_kwargs の type 情報)。
yaml が無ければ null (= UI は raw JSON editor にフォールバック)。

dev (= ローカル開発) 時は env で plugin root を上書きできる:
    PIPELINE_PLUGIN_ROOT=C:/Users/kazuna/pipeline/plugins
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger("pipeline.api.plugins_local")
router = APIRouter(prefix="/api/v1/plugins", tags=["plugins"])


class PluginKwargField(BaseModel):
    key: str
    type: str  # int / float / str / path / bool / enum / secret
    default: Any | None = None
    label: str | None = None
    help: str | None = None
    min: float | None = None
    max: float | None = None
    options: list[Any] | None = None
    required: bool = False


class PluginManifest(BaseModel):
    name: str | None = None
    description: str | None = None
    init_kwargs: list[PluginKwargField] = []
    hidden_kwargs: list[str] = []
    ui_panel: bool = False
    # "video" (default): 稼働中タブ + 最近処理タブ / "image": 単一リスト (現在 + 履歴)
    ui_panel_mode: str = "video"
    # "self_loop": process() の最後で自分で次の tick を enqueue するタイプ
    #   → ワーカー再起動で queue が空になると永久停止するので、 control plane の
    #     watchdog が idle 検出時に自動 bootstrap する
    # None / "event_driven" (default): dispatcher や外部から enqueue されるタイプ (= 監視対象外)
    workload_type: str | None = None


class AvailablePlugin(BaseModel):
    slug: str
    path: str
    modules: list[str]
    has_requirements: bool
    has_ui_panel: bool = False
    manifest: PluginManifest | None = None


class AvailablePluginsResponse(BaseModel):
    root: str
    plugins: list[AvailablePlugin]


def _plugin_root() -> Path:
    return Path(os.environ.get("PIPELINE_PLUGIN_ROOT", "/opt/pipeline/plugins"))


def read_plugin_workload_type(source_path: str | None) -> str | None:
    """workload.executor_config.source_path から plugin.yaml の workload_type を読む。

    self_loop watchdog 等から呼ばれる軽量ヘルパー。
    source_path は GPU worker 視点 (e.g. /opt/pipeline/plugins/<slug>) で、
    control plane (= .7) にはそのパスが存在しないことが多いので、
    パス完全一致 → basename を PIPELINE_PLUGIN_ROOT 配下で解決のフォールバックを試す。
    """
    if not source_path:
        return None
    p = Path(source_path) / "plugin.yaml"
    if not p.exists():
        # fallback: basename (= plugin slug) を control plane の plugin root と組み合わせる
        basename = Path(source_path).name
        if basename:
            p = _plugin_root() / basename / "plugin.yaml"
            if not p.exists():
                return None
        else:
            return None
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    v = data.get("workload_type")
    return str(v) if v else None


def _load_manifest(plugin_dir: Path) -> PluginManifest | None:
    yaml_path = plugin_dir / "plugin.yaml"
    if not yaml_path.exists():
        return None
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    except ImportError:
        log.warning("PyYAML not installed; plugin.yaml ignored for %s", plugin_dir.name)
        return None
    except Exception as e:
        log.warning("manifest parse failed for %s: %s", plugin_dir.name, e)
        return None
    return PluginManifest(
        name=data.get("name"),
        description=data.get("description"),
        init_kwargs=[PluginKwargField(**k) for k in (data.get("init_kwargs") or [])],
        hidden_kwargs=list(data.get("hidden_kwargs") or []),
        ui_panel=bool(data.get("ui_panel", False)),
        ui_panel_mode=str(data.get("ui_panel_mode") or "video"),
        workload_type=(str(data["workload_type"]) if data.get("workload_type") else None),
    )


@router.get("/available", response_model=AvailablePluginsResponse)
def list_available_plugins() -> AvailablePluginsResponse:
    root = _plugin_root()
    plugins: list[AvailablePlugin] = []
    if root.exists():
        for d in sorted(root.iterdir()):
            if not d.is_dir() or d.name.startswith("."):
                continue
            modules = sorted(
                p.stem for p in d.glob("*.py")
                if not p.name.startswith("_") and p.is_file()
            )
            manifest = _load_manifest(d)
            # ui_panel: plugin.yaml で宣言されていれば true
            # (panel.html が plugin に無くても system 側の汎用 panel が fallback で出る)
            has_ui = bool(manifest and manifest.ui_panel)
            plugins.append(AvailablePlugin(
                slug=d.name,
                path=str(d),
                modules=modules,
                has_requirements=(d / "requirements.txt").exists(),
                has_ui_panel=has_ui,
                manifest=manifest,
            ))
    return AvailablePluginsResponse(root=str(root), plugins=plugins)
