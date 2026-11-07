"""プラグイン配布の共通部品 (control plane の配信 API と host の agent の両方が使う)。

- digest_dir     : プラグインディレクトリの内容ダイジェスト (sha256)
- build_archive  : tar.gz にする (配信側)
- safe_extract   : 受け取った tar.gz を安全に展開する (受信側。 パストラバーサル・リンクを拒否)

標準ライブラリだけで書く (agent は軽い環境で動くため)。
"""

from __future__ import annotations

import hashlib
import io
import re
import tarfile
from pathlib import Path

# 配布に含めないもの
_EXCLUDE_DIRS = {"__pycache__", ".git", ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules"}
_EXCLUDE_SUFFIX = (".pyc", ".pyo", ".bak", ".swp", ".tmp")
# 受信側が「配布で入れたもの」と見分けるための印 (内容ダイジェスト)。 ダイジェスト自体には含めない
MARKER_NAME = ".pipeline-plugin.sha256"

# プラグインのディレクトリ名として許す文字 (パス区切り・親参照を拒否)
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$")

# 配布するプラグインの上限サイズ (展開前の合計バイト)。 誤ってモデル重み等を置いた場合の保険
MAX_PLUGIN_BYTES = 256 * 1024 * 1024


class PluginDistError(Exception):
    pass


def _excluded(rel_parts: tuple[str, ...], name: str) -> bool:
    if any(p in _EXCLUDE_DIRS for p in rel_parts[:-1]):
        return True
    if name == MARKER_NAME or name.endswith(_EXCLUDE_SUFFIX) or ".bak-" in name:
        return True
    return False


def iter_files(root: Path) -> list[tuple[str, Path]]:
    """配布対象ファイルを (相対パス posix, 実パス) で、 相対パス順に返す。"""
    out: list[tuple[str, Path]] = []
    for p in root.rglob("*"):
        if not p.is_file() or p.is_symlink():
            continue
        rel = p.relative_to(root)
        if _excluded(rel.parts, p.name):
            continue
        out.append((rel.as_posix(), p))
    out.sort(key=lambda t: t[0])
    return out


def digest_dir(root: Path) -> tuple[str, int, int]:
    """(sha256, ファイル数, 合計バイト)。 パスと中身の両方が変われば変わる。"""
    h = hashlib.sha256()
    total = 0
    files = iter_files(root)
    for rel, p in files:
        data = p.read_bytes()
        total += len(data)
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(hashlib.sha256(data).digest())
    return h.hexdigest(), len(files), total


def build_archive(root: Path) -> bytes:
    """root 直下に展開される tar.gz (arcname は相対パス)。 上限超過なら PluginDistError。"""
    files = iter_files(root)
    total = sum(p.stat().st_size for _, p in files)
    if total > MAX_PLUGIN_BYTES:
        raise PluginDistError(
            f"プラグインが大きすぎます ({total} bytes > {MAX_PLUGIN_BYTES})。"
            " 配布対象にモデル重み等が入っていないか確認してください")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel, p in files:
            tar.add(str(p), arcname=rel, recursive=False)
    return buf.getvalue()


def safe_extract(archive: bytes, dest: Path) -> None:
    """tar.gz を dest に展開する。 絶対パス・親参照・リンク・特殊ファイルは拒否する。"""
    dest.mkdir(parents=True, exist_ok=True)
    base = dest.resolve()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        members = tar.getmembers()
        for m in members:
            if not (m.isfile() or m.isdir()):
                raise PluginDistError(f"許可されないエントリです: {m.name} (リンク/特殊ファイル)")
            target = (base / m.name).resolve()
            if target != base and base not in target.parents:
                raise PluginDistError(f"展開先の外を指すエントリです: {m.name}")
        tar.extractall(dest, members=members, filter="data")
