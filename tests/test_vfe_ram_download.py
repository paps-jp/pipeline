"""video-face-extract の動画 DL を RAM (tmpfs) へ向ける経路の回帰テスト。

2026-11-20: vfe が DL した動画を root ディスクに書いており、 ai-gpu1 は base の
コンシューマ SSD (Predator GM7) に 1.76TB/日、 ai-gpu3 は depot の 256GB クライアント
NVMe に 0.5TB/日を書いていた。 動画は数分で消えるので RAM に置く。

守りたいこと:
  - 未設定なら従来どおり download_dir だけを使う (既定 OFF)
  - RAM に置くのはサイズ上限以下・MemAvailable に余裕あり・tmpfs に空きあり、 の全部を満たすときだけ
  - サイズ不明 (stat 失敗) はディスク
  - RAM 側で ENOSPC/ENOMEM になったら同じ store からディスクで取り直す
    (download failed にすると動画は failed のまま戻らない)
  - 容量以外のエラーではディスクで取り直さず次の store へ進む
"""

from __future__ import annotations

import errno
import importlib.util
import os
import pathlib
from types import SimpleNamespace

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "video_face_extract" / "extract_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="video_face_extract plugin が未配置")

MB = 1048576


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("vfe_ram_dl_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def dirs(tmp_path):
    disk = tmp_path / "disk"
    ram = tmp_path / "ram"
    disk.mkdir()
    ram.mkdir()
    return disk, ram


def _state(dirs, *, max_mb=1024, min_avail_mb=8192, stores=None):
    disk, ram = dirs
    return {
        "download_dir": disk,
        "ram_download": {"dir": ram, "max_bytes": max_mb * MB,
                         "min_avail_bytes": min_avail_mb * MB},
        "paprika_minio": object(),
        "paprika_bucket": "b",
        "_stores": stores or [],
    }


@pytest.fixture
def plenty(monkeypatch, m):
    """MemAvailable 64GB・tmpfs 空き 32GB。"""
    monkeypatch.setattr(m, "_mem_available_bytes", lambda: 64 * 1024 * MB)
    monkeypatch.setattr(m.os, "statvfs",
                        lambda p: SimpleNamespace(f_bavail=32 * 1024, f_frsize=MB), raising=False)


# ── 設定 ──────────────────────────────────────────────

def test_config_off_by_default(m, monkeypatch):
    monkeypatch.delenv("VFE_RAM_DL_DIR", raising=False)
    assert m._ram_download_config({}) is None


def test_config_from_env(m, monkeypatch, tmp_path):
    d = tmp_path / "shm" / "crawl_video_dl"
    monkeypatch.setenv("VFE_RAM_DL_DIR", str(d))
    monkeypatch.setenv("VFE_RAM_DL_MAX_MB", "512")
    monkeypatch.delenv("VFE_RAM_DL_MIN_AVAIL_MB", raising=False)
    cfg = m._ram_download_config({})
    assert cfg["dir"] == d and d.is_dir()
    assert cfg["max_bytes"] == 512 * MB
    assert cfg["min_avail_bytes"] == 8192 * MB


def test_config_kwargs_win_over_env(m, monkeypatch, tmp_path):
    monkeypatch.setenv("VFE_RAM_DL_DIR", str(tmp_path / "env"))
    cfg = m._ram_download_config({"ram_download_dir": str(tmp_path / "kw")})
    assert cfg["dir"] == tmp_path / "kw"


def test_config_bad_number_disables(m, monkeypatch, tmp_path):
    monkeypatch.setenv("VFE_RAM_DL_DIR", str(tmp_path / "r"))
    monkeypatch.setenv("VFE_RAM_DL_MAX_MB", "lots")
    assert m._ram_download_config({}) is None


# ── 置き場所の判定 ─────────────────────────────────────

def test_pick_ram_when_everything_fits(m, dirs, plenty):
    assert m._pick_download_dir(_state(dirs), 500 * MB) == dirs[1]


def test_pick_disk_when_ram_disabled(m, dirs, plenty):
    st = _state(dirs)
    st["ram_download"] = None
    assert m._pick_download_dir(st, 10 * MB) == dirs[0]


@pytest.mark.parametrize("size", [None, 0])
def test_pick_disk_when_size_unknown(m, dirs, plenty, size):
    assert m._pick_download_dir(_state(dirs), size) == dirs[0]


def test_pick_disk_when_over_size_cap(m, dirs, plenty):
    assert m._pick_download_dir(_state(dirs, max_mb=1024), 1025 * MB) == dirs[0]


def test_pick_disk_when_memory_would_drop_below_floor(m, dirs, monkeypatch):
    monkeypatch.setattr(m, "_mem_available_bytes", lambda: 8500 * MB)
    monkeypatch.setattr(m.os, "statvfs",
                        lambda p: SimpleNamespace(f_bavail=32 * 1024, f_frsize=MB), raising=False)
    # 8500 - 400 = 8100 < 8192
    assert m._pick_download_dir(_state(dirs), 400 * MB) == dirs[0]
    assert m._pick_download_dir(_state(dirs), 200 * MB) == dirs[1]


def test_pick_disk_when_meminfo_unreadable(m, dirs, monkeypatch):
    monkeypatch.setattr(m, "_mem_available_bytes", lambda: None)
    assert m._pick_download_dir(_state(dirs), 10 * MB) == dirs[0]


def test_pick_disk_when_tmpfs_full(m, dirs, monkeypatch):
    monkeypatch.setattr(m, "_mem_available_bytes", lambda: 64 * 1024 * MB)
    monkeypatch.setattr(m.os, "statvfs",
                        lambda p: SimpleNamespace(f_bavail=100, f_frsize=MB), raising=False)
    assert m._pick_download_dir(_state(dirs), 90 * MB) == dirs[0]


# ── ダウンロード本体 ───────────────────────────────────

class _Client:
    """fget_object の振る舞いを呼び出し順に指定する偽 MinIO。"""

    def __init__(self, *behaviours):
        self.behaviours = list(behaviours)
        self.paths: list[str] = []

    def fget_object(self, bucket, key, path):
        self.paths.append(path)
        b = self.behaviours.pop(0)
        if isinstance(b, BaseException):
            # 実物と同じく partial を残してから落ちる
            pathlib.Path(path + ".etag-1.part.minio").write_bytes(b"x")
            raise b
        pathlib.Path(path).write_bytes(b"v" * 10)


@pytest.fixture
def stores(monkeypatch, m):
    holder = {}
    monkeypatch.setattr(m, "_video_stores", lambda state: holder["stores"])
    return holder


def test_download_goes_to_ram(m, dirs, plenty, stores):
    c = _Client("ok")
    stores["stores"] = [(".48", c)]
    p = m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4", size=10 * MB)
    assert p == dirs[1] / "7.mp4" and p.exists()


def test_download_without_size_goes_to_disk(m, dirs, plenty, stores):
    c = _Client("ok")
    stores["stores"] = [(".48", c)]
    p = m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4")
    assert p == dirs[0] / "7.mp4"


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.ENOMEM])
def test_space_error_on_ram_retries_same_store_on_disk(m, dirs, plenty, stores, code):
    c48 = _Client(OSError(code, "full"), "ok")
    c8 = _Client()
    stores["stores"] = [(".48", c48), (".8", c8)]
    p = m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4", size=10 * MB)
    assert p == dirs[0] / "7.mp4" and p.exists()
    assert c48.paths == [str(dirs[1] / "7.mp4"), str(dirs[0] / "7.mp4")]
    assert c8.paths == []
    # RAM 側の partial は消えている
    assert list(dirs[1].iterdir()) == []


def test_wrapped_space_error_is_recognised(m):
    try:
        try:
            raise OSError(errno.ENOSPC, "No space left on device")
        except OSError as inner:
            raise RuntimeError("minio wrapper") from inner
    except RuntimeError as e:
        assert m._is_space_error(e)
    assert not m._is_space_error(RuntimeError("NoSuchKey"))
    assert not m._is_space_error(OSError(errno.EACCES, "denied"))


def test_other_error_moves_to_next_store_without_disk_retry(m, dirs, plenty, stores):
    c48 = _Client(RuntimeError("NoSuchKey"))
    c8 = _Client("ok")
    stores["stores"] = [(".48", c48), (".8", c8)]
    p = m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4", size=10 * MB)
    assert c48.paths == [str(dirs[1] / "7.mp4")]
    assert p == dirs[1] / "7.mp4"
    assert not list(dirs[1].glob("*.part.minio"))


def test_all_stores_fail_returns_none_and_cleans_up(m, dirs, plenty, stores):
    c48 = _Client(RuntimeError("NoSuchKey"))
    c8 = _Client(OSError(errno.ENOSPC, "full"), RuntimeError("NoSuchKey"))
    stores["stores"] = [(".48", c48), (".8", c8)]
    assert m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4",
                             size=10 * MB) is None
    assert list(dirs[0].iterdir()) == [] and list(dirs[1].iterdir()) == []


def test_space_error_on_disk_is_not_retried(m, dirs, plenty, stores):
    c48 = _Client(OSError(errno.ENOSPC, "full"))
    c8 = _Client(RuntimeError("NoSuchKey"))
    stores["stores"] = [(".48", c48), (".8", c8)]
    assert m._download_video(_state(dirs), "/jobs/1/assets/a.mp4", 7, "mp4") is None
    assert c48.paths == [str(dirs[0] / "7.mp4")]


def test_real_meminfo_is_parsed(m):
    if not os.path.exists("/proc/meminfo"):
        pytest.skip("/proc/meminfo が無い")
    v = m._mem_available_bytes()
    assert v is None or v > 0
