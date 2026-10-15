"""image-hash-extract: 上限超の画像を一時失敗扱いで再試行し続けない回帰テスト。

2026-10-15 実障害: raw MinIO (.17) に実体があるのに 18〜28MB で max_image_bytes (16MB)
を超えると _fetch_raw_minio が None (= 不在と同じ) を返し、 外部 URL へ fallback して
origin から同じ画像を落とし、 同じ "image too large" で失敗していた。 これが一時失敗
扱い (_error) なので attempt 4+ まで再試行され、 直近 500 run の 75% を占めた。

守る性質:
  - raw MinIO が「在るが上限超」なら origin へ fallback しない。
  - 上限超は ignore_reason='too_large' を立てて終端 (= dispatcher が二度と選ばない)。
  - URL fetch 側の上限超も同様に終端。 403/5xx などの一時失敗は従来どおり retry。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

_PM = (Path(__file__).resolve().parents[1]
       / "plugins" / "image_hash_extract" / "production_main.py")
pytestmark = pytest.mark.skipif(not _PM.exists(), reason="plugins リポジトリ未チェックアウト")

_MAX = 16 * 1024 * 1024


@pytest.fixture(scope="module")
def pm():
    spec = importlib.util.spec_from_file_location("hash_too_large_under_test", _PM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Cur:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params=()):
        self.db.executed.append((sql, tuple(params)))

    def close(self):
        pass


class _DB:
    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []

    def cursor(self):
        return _Cur(self)

    def commit(self):
        pass


def _wire(pm, monkeypatch, *, raw, url_exc=None):
    db = _DB()
    calls = {"url": 0}
    monkeypatch.setattr(pm, "_sample_active_cached", lambda st: (False, 0))
    monkeypatch.setattr(pm, "_ensure_db", lambda st: db)
    monkeypatch.setattr(pm, "_lookup_image", lambda db_, iid: {
        "url": "https://example.invalid/a.jpg", "site": "s", "file_no": 1,
        "ext": "jpg", "ignore_reason": None})
    monkeypatch.setattr(pm, "_fetch_raw_minio", lambda st, iid, mb: raw)
    monkeypatch.setattr(pm, "_fetch_minio", lambda *a, **k: None)

    def _url(url, timeout_s, max_bytes):
        calls["url"] += 1
        raise url_exc or AssertionError("URL fetch は呼ばれないはず")
    monkeypatch.setattr(pm, "_fetch_url", _url)
    state = {"counter": 0, "hostname": "h", "dry_write": False,
             "max_image_bytes": _MAX, "fetch_timeout_s": 5.0}
    return db, calls, state


def _ignore_updates(db):
    return [p for s, p in db.executed if "ignore_reason" in s]


def test_raw_minio_reports_oversize_instead_of_absent(pm):
    """在るが上限超は None (不在) と区別して返す。"""
    class _Resp:
        def read(self, n):
            return b"x" * n            # 常に max_bytes+1 を返す = 上限超

        def close(self):
            pass

        def release_conn(self):
            pass

    client = SimpleNamespace(get_object=lambda bucket, key: _Resp())
    st = {"raw_minio": client, "raw_minio_bucket": "raw", "raw_dir_base": 1000}
    r = pm._fetch_raw_minio(st, 436236703, 1024)
    assert r is not None
    assert r[0] is pm._RAW_TOO_LARGE
    assert r[1] == "436237000/436236703.jpg"


def test_raw_oversize_is_terminal_without_origin_fetch(pm, monkeypatch):
    db, calls, st = _wire(pm, monkeypatch, raw=(pm._RAW_TOO_LARGE, "k"))
    out = pm.process(SimpleNamespace(pk=436236703), None, st)

    assert calls["url"] == 0                      # origin へ行かない
    assert out["fetch_source"] == "raw_minio_too_large"
    assert out.get("ignore_reason_set") == "too_large"
    assert "_error" not in out                    # 一時失敗扱いにしない (= retry しない)
    assert _ignore_updates(db) == [("too_large", 436236703)]


def test_url_oversize_is_terminal(pm, monkeypatch):
    db, calls, st = _wire(pm, monkeypatch, raw=None,
                          url_exc=ValueError(f"image too large (>{_MAX} bytes)"))
    out = pm.process(SimpleNamespace(pk=1), None, st)

    assert calls["url"] == 1
    assert out.get("ignore_reason_set") == "too_large"
    assert "_error" not in out
    assert _ignore_updates(db) == [("too_large", 1)]


def test_url_404_still_marks_dead_link(pm, monkeypatch):
    db, _, st = _wire(pm, monkeypatch, raw=None,
                      url_exc=RuntimeError("404 Client Error: Not Found"))
    out = pm.process(SimpleNamespace(pk=2), None, st)

    assert out.get("ignore_reason_set") == "dead_link"
    assert _ignore_updates(db) == [("dead_link", 2)]


def test_url_transient_failure_still_retries(pm, monkeypatch):
    """403 / 5xx は一時失敗のまま (_error → retry)。 終端にしてはいけない。"""
    db, _, st = _wire(pm, monkeypatch, raw=None,
                      url_exc=RuntimeError("403 Client Error: Forbidden"))
    out = pm.process(SimpleNamespace(pk=3), None, st)

    assert out["_error"].startswith("fetch failed: 403")
    assert "ignore_reason_set" not in out
    assert _ignore_updates(db) == []
