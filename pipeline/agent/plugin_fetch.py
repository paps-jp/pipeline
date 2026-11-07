"""agent: ワークロードのコードを control plane から取得してから起動する (opt-in)。

子 worker を spawn する直前に ensure(slug) を呼ぶ。 やることは次のとおり:

  1. workload 定義から executor_config.source_path (= このホスト上のプラグイン置き場) を得る
  2. その場所に「ディレクトリが無い」なら、 control plane から取得して展開する
  3. 「配布で入れたもの (印あり)」で、 control plane の内容と違うなら、 取得し直す
  4. **印の無い既存ディレクトリには触らない** (手動の rsync/scp で入れたものを勝手に上書きしない)

返り値が False のとき agent はその slug の spawn を見送る (次 tick に再試行)。
取得は「展開 → ダイジェスト検証 → 入れ替え」の順で、 途中で失敗しても元のディレクトリは壊さない。
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from pipeline.plugin_dist import (
    MARKER_NAME,
    NAME_RE,
    PluginDistError,
    digest_dir,
    safe_extract,
)

log = logging.getLogger("pipeline.agent.plugin_fetch")

DEFAULT_PLUGIN_ROOT = "/opt/pipeline/plugins"


class PluginFetcher:
    def __init__(
        self,
        base_url: Callable[[], str],
        *,
        plugin_root: str | None = None,
        client: httpx.Client | None = None,
        install_requirements: bool = True,
        pip_timeout_s: float = 600.0,
        ok_ttl_s: float = 60.0,
        fail_backoff_s: float = 30.0,
    ) -> None:
        self._base_url = base_url
        self.plugin_root = Path(
            plugin_root or os.environ.get("PIPELINE_PLUGIN_ROOT", DEFAULT_PLUGIN_ROOT)
        ).resolve()
        self._client = client or httpx.Client(timeout=60.0)
        self.install_requirements = install_requirements
        self.pip_timeout_s = pip_timeout_s
        self.ok_ttl_s = ok_ttl_s
        self.fail_backoff_s = fail_backoff_s
        self._ok_until: dict[str, float] = {}      # slug → この時刻まで「確認済み」
        self._retry_at: dict[str, float] = {}      # slug → この時刻まで再試行しない (失敗直後)

    # ---------------------------------------------------------------- HTTP

    def _get(self, path: str) -> httpx.Response:
        r = self._client.get(self._base_url().rstrip("/") + path)
        r.raise_for_status()
        return r

    def _source_dir(self, slug: str) -> Path | None:
        """slug の workload が使うプラグイン置き場。 取得の対象でなければ None。"""
        wl: dict[str, Any] = self._get(f"/api/v1/workloads/{slug}").json()
        if wl.get("executor_type") != "python_module":
            return None
        src = (wl.get("executor_config") or {}).get("source_path")
        if not src:
            return None
        d = Path(src)
        # 取得先は plugin_root 直下の 1 ディレクトリだけ (任意パスへの書き込みを許さない)
        if not NAME_RE.match(d.name) or d.parent.resolve() != self.plugin_root:
            log.warning("[plugin-fetch] %s: source_path=%s は plugin_root(%s) 直下ではないため取得しない",
                        slug, src, self.plugin_root)
            return None
        return self.plugin_root / d.name

    # ---------------------------------------------------------------- 本体

    def ensure(self, slug: str) -> bool:
        """spawn してよい状態なら True。 取得に失敗したら False (spawn 見送り)。"""
        now = time.monotonic()
        if self._ok_until.get(slug, 0.0) > now:
            return True
        if self._retry_at.get(slug, 0.0) > now:
            return False
        try:
            dest = self._source_dir(slug)
            if dest is None:
                self._ok_until[slug] = now + self.ok_ttl_s
                return True
            self._ensure_dir(slug, dest)
        except Exception as e:  # noqa: BLE001 - 失敗は spawn 見送りで表す
            log.warning("[plugin-fetch] %s: 取得に失敗 → spawn を見送ります: %s", slug, e)
            self._retry_at[slug] = now + self.fail_backoff_s
            return False
        self._ok_until[slug] = time.monotonic() + self.ok_ttl_s
        return True

    def _ensure_dir(self, slug: str, dest: Path) -> None:
        marker = dest / MARKER_NAME
        present = dest.is_dir() and any(dest.iterdir())
        if present and not marker.exists():
            return  # 手動配布 (印なし) は触らない
        name = dest.name
        man = self._get(f"/api/v1/plugin-dist/{name}/manifest").json()
        want = man["sha256"]
        if present and marker.read_text().strip() == want:
            return  # 最新
        log.info("[plugin-fetch] %s: %s を取得します (%s, %s files, %s bytes)",
                 slug, name, "更新" if present else "新規", man.get("files"), man.get("bytes"))
        self._download(name, dest, want, bool(man.get("has_requirements")), present)

    def _download(self, name: str, dest: Path, want_sha: str, has_req: bool, replace: bool) -> None:
        tmp = self.plugin_root / f".{name}.dl-{os.getpid()}"
        old = self.plugin_root / f".{name}.old-{os.getpid()}"
        for p in (tmp, old):
            shutil.rmtree(p, ignore_errors=True)
        try:
            data = self._get(f"/api/v1/plugin-dist/{name}/archive.tar.gz").content
            safe_extract(data, tmp)
            got, _, _ = digest_dir(tmp)
            if got != want_sha:
                raise PluginDistError(f"ダイジェストが一致しません (期待 {want_sha[:12]} / 実際 {got[:12]})")
            if has_req and self.install_requirements:
                self._pip_install(tmp / "requirements.txt")
            (tmp / MARKER_NAME).write_text(want_sha + "\n")
            # 入れ替え: 既存は退避してから置く。 置けなければ退避を戻す
            if replace and dest.exists():
                dest.rename(old)
            try:
                tmp.rename(dest)
            except Exception:
                if old.exists() and not dest.exists():
                    old.rename(dest)
                raise
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.rmtree(old, ignore_errors=True)

    def _pip_install(self, req: Path) -> None:
        if not req.is_file():
            return
        log.info("[plugin-fetch] pip install -r %s", req)
        r = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
            capture_output=True, text=True, timeout=self.pip_timeout_s,
        )
        if r.returncode != 0:
            raise PluginDistError(f"pip install に失敗: {r.stderr.strip()[-300:]}")
