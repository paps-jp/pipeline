"""/api/v1/plugin-dist — host の agent がプラグインのコードを取得するための配信 API。

- GET /{name}/manifest         … 内容ダイジェスト (sha256)・ファイル数・サイズ・requirements の有無
- GET /{name}/archive.tar.gz   … プラグイン一式 (tar.gz。 展開すると直下にファイルが並ぶ)

配信元は control plane の PIPELINE_PLUGIN_ROOT (既定 /opt/pipeline/plugins) 配下のディレクトリ。
name は「ディレクトリ名 1 つ」だけを許す (パス区切り・親参照は 400)。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from pipeline.api.plugins_local import _plugin_root
from pipeline.plugin_dist import NAME_RE, PluginDistError, build_archive, digest_dir

router = APIRouter(prefix="/api/v1/plugin-dist", tags=["plugin-dist"])

# ダイジェストは全ファイルを読むので、 (相対パス, サイズ, mtime) が変わらない間は使い回す
_cache: dict[str, tuple[tuple, dict[str, Any]]] = {}


def _plugin_dir(name: str) -> Path:
    if not NAME_RE.match(name):
        raise HTTPException(400, detail="不正なプラグイン名です")
    root = _plugin_root().resolve()
    d = (root / name).resolve()
    if d.parent != root or not d.is_dir():
        raise HTTPException(404, detail=f"プラグインが見つかりません: {name}")
    return d


def _signature(d: Path) -> tuple:
    from pipeline.plugin_dist import iter_files

    return tuple((rel, p.stat().st_size, p.stat().st_mtime_ns) for rel, p in iter_files(d))


def _manifest(name: str, d: Path) -> dict[str, Any]:
    sig = _signature(d)
    hit = _cache.get(name)
    if hit and hit[0] == sig:
        return hit[1]
    sha, n_files, n_bytes = digest_dir(d)
    m = {
        "name": name,
        "sha256": sha,
        "files": n_files,
        "bytes": n_bytes,
        "has_requirements": (d / "requirements.txt").is_file(),
    }
    _cache[name] = (sig, m)
    return m


@router.get("/{name}/manifest")
def get_manifest(name: str) -> dict[str, Any]:
    return _manifest(name, _plugin_dir(name))


@router.get("/{name}/archive.tar.gz")
def get_archive(name: str) -> Response:
    d = _plugin_dir(name)
    try:
        data = build_archive(d)
    except PluginDistError as e:
        raise HTTPException(413, detail=str(e)) from e
    return Response(content=data, media_type="application/gzip",
                    headers={"X-Plugin-Sha256": _manifest(name, d)["sha256"]})
