"""image_embed の crop 並列プリフェッチのテスト。

直列 MinIO 取得 (実測 185ms/顔) が律速で GPU util 0-35% だったため並列化した。
並列化で壊しやすいのは **順序** と **fallback の意味** なので、 そこを固定する:

  - `_fetch_crop_image` は「MinIO で取れれば (img, None)」「取れない/decode 不能なら
    .17 raw の再 crop (img, jpeg)」「どちらも駄目なら None」
  - プリフェッチ結果は rows の順序を保つ (aligned_list と meta_face_ids の対応が崩れると
    embedding が別の face_id に紐づく = 検索が壊れる)
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# cv2 / numpy は GPU worker 側 (face_search venv) の依存。 開発機には無いのでその環境では
# skip し、 cv2 を持つ環境 (GPU ホスト等) でのみ実走させる。
np = pytest.importorskip("numpy")
pytest.importorskip("cv2")

_SRC = Path(__file__).resolve().parents[1] / "plugins" / "image_embed" / "embed_main.py"

# 本番 plugin は非公開リポジトリ側 (`/plugins/` は .gitignore) なので、 公開リポジトリだけを
# clone した環境には存在しない。
if not _SRC.exists():
    pytest.skip(f"本番 plugin が無い環境 ({_SRC.name} は非公開リポジトリ側)",
                allow_module_level=True)


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("_embed_prefetch_under_test", _SRC)
    m = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(m)
    return m


def _jpeg(w=8, h=8):
    import cv2
    ok, buf = cv2.imencode(".jpg", np.full((h, w, 3), 128, dtype=np.uint8))
    assert ok
    return buf.tobytes()


class FakeResp:
    def __init__(self, data):
        self._d = data

    def read(self):
        return self._d

    def close(self):
        pass

    def release_conn(self):
        pass


class FakeMinio:
    """key ごとに bytes を返す。 存在しない key は例外 (= .16 不在を模す)。"""

    def __init__(self, table):
        self.table = table
        self.gets = []

    def get_object(self, bucket, key):
        self.gets.append(key)
        if key not in self.table:
            raise RuntimeError("NoSuchKey")
        return FakeResp(self.table[key])


def _state(table, recrop=None):
    return {"minio": FakeMinio(table), "minio_bucket": "crawl",
            "_recrop_result": recrop, "prefetch_parallel": 4, "prefetch_chunk": 3}


def test_fetch_returns_image_and_no_recrop(mod):
    st = _state({"k1": _jpeg()})
    got = mod._fetch_crop_image(st, "k1", 1, 0, 0, 10, 10)
    assert got is not None
    img, recrop_jpeg = got
    assert img.shape[2] == 3 and recrop_jpeg is None


def test_fetch_falls_back_to_raw_recrop(mod, monkeypatch):
    """.16 に無い画像顔は .17 raw から再 crop する (従来の意味を保つ)。"""
    fake_img = np.zeros((4, 4, 3), dtype=np.uint8)
    monkeypatch.setattr(mod, "_recrop_from_raw",
                        lambda *a, **k: (fake_img, b"jpegbytes"))
    st = _state({})                       # MinIO は常に失敗
    got = mod._fetch_crop_image(st, "missing", 99, 0, 0, 10, 10)
    assert got is not None
    img, recrop_jpeg = got
    assert img is fake_img and recrop_jpeg == b"jpegbytes"


def test_fetch_returns_none_when_both_fail(mod, monkeypatch):
    monkeypatch.setattr(mod, "_recrop_from_raw", lambda *a, **k: None)
    st = _state({})
    assert mod._fetch_crop_image(st, "missing", 99, 0, 0, 10, 10) is None


def test_corrupt_bytes_trigger_recrop_not_success(mod, monkeypatch):
    """bytes は取れたが decode 不能 → 成功扱いにせず recrop へ落とす。"""
    calls = []

    def _rc(*a, **k):
        calls.append(1)
        return (np.zeros((2, 2, 3), dtype=np.uint8), b"j")

    monkeypatch.setattr(mod, "_recrop_from_raw", _rc)
    st = _state({"bad": b"not an image"})
    got = mod._fetch_crop_image(st, "bad", 7, 0, 0, 10, 10)
    assert got is not None and calls == [1]


def test_pool_is_reused_across_calls(mod):
    st = {"prefetch_parallel": 3}
    p1 = mod._prefetch_pool(st)
    p2 = mod._prefetch_pool(st)
    assert p1 is p2                       # プロセス内で使い回す (毎 batch 作らない)
    assert st["_embed_prefetch_pool"] is p1
    p1.shutdown(wait=False)


def test_prefetch_preserves_row_order(mod):
    """並列取得でも rows の順序を保つ。 崩れると embedding が別 face に紐づく。

    実装と同じ chunk + submit/zip の流れを再現して検証する。
    """
    n = 10
    table = {f"k{i}": _jpeg(w=4 + i) for i in range(n)}
    st = _state(table)
    rows = [(100 + i, f"k{i}", "{}", 0, 0, 10, 10, None, None, None) for i in range(n)]

    pool = mod._prefetch_pool(st)
    chunk_n = int(st["prefetch_chunk"])
    ordered = []
    for s in range(0, len(rows), chunk_n):
        chunk = rows[s:s + chunk_n]
        futs = [pool.submit(mod._fetch_crop_image, st, r[1], r[7], r[3], r[4], r[5], r[6])
                for r in chunk]
        for r, f in zip(chunk, futs):
            ordered.append((r, f.result()))
    pool.shutdown(wait=True)

    assert [r[0] for r, _ in ordered] == [100 + i for i in range(n)]
    # 幅が 4+i の画像を入れてあるので、 順序が崩れていれば幅がずれる
    assert [g[0].shape[1] for _, g in ordered] == [4 + i for i in range(n)]
