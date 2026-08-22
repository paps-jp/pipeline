"""hub purge の分割と並列化 (image-pull の `_hub_purge_jobs`)。

2026-08-22 の段別計測で、 image-pull の tick の **90-96%** が hub の
POST /jobs/purge 待ちだと判明した (download は 1-7%)。 200 job を 1 本の POST で
送って 81 秒 = 約 440ms/job、 消費 160 jobs/分 に対し hub 生産 500-610 jobs/分 で
永久に追いつけない形になっていた。

hub 側 (`server/hub/routes/jobs/lifecycle.py` の `purge_jobs`) は
``sem = asyncio.Semaphore(10)`` を **関数ローカル**で作る。 10 並列はプロセス
全体の上限ではなく**リクエストごと**なので、 分割して同時に投げれば実効並列度が
上がる。 400 job での実測は conc=4 で 4.16x、 conc=8 で 7.98x。

ここで固定するのは 4 点:
  1. 既定は現状維持 (chunk=200 / conc=1) — ロールアウトで挙動が変わらない
  2. chunk で分割され、 全チャンクぶんが合算される
  3. concurrency>1 で本当に同時に飛ぶ (直列化されていない)
  4. 404/405 (purge を持たない hub) は **部分成果を返さず** (0,0,0) に畳む
     — 呼び出し側が旧経路 (直 MinIO 削除) へフォールバックできなくなるため

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import threading
import time
import urllib.error
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")

HUB = "http://hub.test:8000"


@pytest.fixture(scope="module")
def im():
    spec = importlib.util.spec_from_file_location("image_main_purge_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Resp:
    """urlopen のコンテキストマネージャ互換のダミー応答。"""

    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(im, monkeypatch, *, per_call_sleep=0.0, error=None, record=None):
    """POST 本文の job_ids 数を記録しつつ、 1 job=1 obj=10 bytes を返す。"""
    import json as _json

    lock = threading.Lock()
    live = {"now": 0, "peak": 0}

    def _open(req, timeout=None):
        body = _json.loads(req.data.decode())
        ids = body["job_ids"]
        with lock:
            live["now"] += 1
            live["peak"] = max(live["peak"], live["now"])
            if record is not None:
                record.append(len(ids))
        try:
            if error is not None:
                raise error
            if per_call_sleep:
                time.sleep(per_call_sleep)
            return _Resp(_json.dumps({
                "purged": len(ids),
                "minio_objects": len(ids),
                "minio_bytes": 10 * len(ids),
                "local_bytes": 0,
            }).encode())
        finally:
            with lock:
                live["now"] -= 1

    monkeypatch.setattr(im.urllib.request, "urlopen", _open)
    return live


def _ids(n):
    return ["job%04d" % i for i in range(n)]


# ---------------------------------------------------- 既定は現状維持 --

def test_defaults_are_unchanged(im):
    assert im.PURGE_CHUNK == 200
    assert im.PURGE_CONCURRENCY == 1


def test_default_single_request_under_chunk(im, monkeypatch):
    sizes: list[int] = []
    _fake_urlopen(im, monkeypatch, record=sizes)
    got = im._hub_purge_jobs(HUB, _ids(150))
    assert sizes == [150]          # 1 本にまとまる
    assert got == (150, 150, 1500)


# ------------------------------------------------------------ 分割 --

def test_splits_by_chunk_and_sums(im, monkeypatch):
    sizes: list[int] = []
    _fake_urlopen(im, monkeypatch, record=sizes)
    got = im._hub_purge_jobs(HUB, _ids(250), chunk=100)
    assert sorted(sizes) == [50, 100, 100]
    assert got == (250, 250, 2500)   # 端数チャンクも合算される


def test_empty_input_makes_no_request(im, monkeypatch):
    sizes: list[int] = []
    _fake_urlopen(im, monkeypatch, record=sizes)
    assert im._hub_purge_jobs(HUB, []) == (0, 0, 0)
    assert im._hub_purge_jobs("", _ids(10)) == (0, 0, 0)
    assert sizes == []


# ------------------------------------------------------------ 並列 --

def test_concurrency_actually_overlaps(im, monkeypatch):
    """conc=4 なら 4 本が同時に飛ぶ (直列に畳まれていない)。"""
    live = _fake_urlopen(im, monkeypatch, per_call_sleep=0.15)
    t0 = time.time()
    got = im._hub_purge_jobs(HUB, _ids(400), chunk=100, concurrency=4)
    wall = time.time() - t0
    assert got == (400, 400, 4000)
    assert live["peak"] == 4          # 実際に 4 本が重なった
    assert wall < 0.15 * 4 * 0.8      # 直列 (0.6s) より明確に速い


def test_concurrency_one_is_serial(im, monkeypatch):
    live = _fake_urlopen(im, monkeypatch, per_call_sleep=0.02)
    im._hub_purge_jobs(HUB, _ids(400), chunk=100, concurrency=1)
    assert live["peak"] == 1


def test_concurrency_capped_by_part_count(im, monkeypatch):
    """チャンクが 2 個しか無いのに 8 本立てない。"""
    live = _fake_urlopen(im, monkeypatch, per_call_sleep=0.05)
    im._hub_purge_jobs(HUB, _ids(150), chunk=100, concurrency=8)
    assert live["peak"] == 2


# -------------------------------------------- 404 は部分成果を返さない --

@pytest.mark.parametrize("code", [404, 405])
def test_missing_endpoint_collapses_to_zero(im, monkeypatch, code):
    """purge を持たない hub。 部分成果を返すと呼び出し側が旧経路に落ちない。"""
    err = urllib.error.HTTPError(HUB, code, "no", None, None)
    _fake_urlopen(im, monkeypatch, error=err)
    assert im._hub_purge_jobs(HUB, _ids(400), chunk=100, concurrency=4) == (0, 0, 0)


def test_one_chunk_404_collapses_whole_call(im, monkeypatch):
    """並列中の 1 本だけが 404 でも全体を (0,0,0) に畳む。"""
    import json as _json
    seen = {"n": 0}
    lock = threading.Lock()

    def _open(req, timeout=None):
        with lock:
            seen["n"] += 1
            first = seen["n"] == 1
        if first:
            raise urllib.error.HTTPError(HUB, 404, "no", None, None)
        ids = _json.loads(req.data.decode())["job_ids"]
        return _Resp(_json.dumps({
            "purged": len(ids), "minio_objects": len(ids),
            "minio_bytes": 10 * len(ids), "local_bytes": 0}).encode())

    monkeypatch.setattr(im.urllib.request, "urlopen", _open)
    assert im._hub_purge_jobs(HUB, _ids(400), chunk=100, concurrency=4) == (0, 0, 0)


def test_transient_error_keeps_other_chunks(im, monkeypatch):
    """500 や timeout は「その 1 本が落ちた」だけ。 残りの成果は返す。"""
    import json as _json
    seen = {"n": 0}
    lock = threading.Lock()

    def _open(req, timeout=None):
        with lock:
            seen["n"] += 1
            first = seen["n"] == 1
        if first:
            raise urllib.error.HTTPError(HUB, 503, "busy", None, None)
        ids = _json.loads(req.data.decode())["job_ids"]
        return _Resp(_json.dumps({
            "purged": len(ids), "minio_objects": len(ids),
            "minio_bytes": 10 * len(ids), "local_bytes": 0}).encode())

    monkeypatch.setattr(im.urllib.request, "urlopen", _open)
    purged, objs, nbytes = im._hub_purge_jobs(
        HUB, _ids(400), chunk=100, concurrency=1)
    assert (purged, objs, nbytes) == (300, 300, 3000)


# ------------------------------------------------ dyn で回せること --

def test_purge_knobs_are_dynamically_tunable(im):
    """supervisor / UI から再起動なしで回せる側に置く。"""
    assert "purge_chunk" in im.DYN_KEYS
    assert "purge_concurrency" in im.DYN_KEYS
    assert im.DYN_KEYS["purge_chunk"][2] <= 1000      # hub 側の上限
    assert im.DYN_KEYS["purge_concurrency"][1] >= 1
