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


class AvailablePlugin(BaseModel):
    slug: str
    path: str
    modules: list[str]
    has_requirements: bool
    manifest: PluginManifest | None = None


class AvailablePluginsResponse(BaseModel):
    root: str
    plugins: list[AvailablePlugin]


def _plugin_root() -> Path:
    return Path(os.environ.get("PIPELINE_PLUGIN_ROOT", "/opt/pipeline/plugins"))


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
            plugins.append(AvailablePlugin(
                slug=d.name,
                path=str(d),
                modules=modules,
                has_requirements=(d / "requirements.txt").exists(),
                manifest=_load_manifest(d),
            ))
    return AvailablePluginsResponse(root=str(root), plugins=plugins)
