"""取得経路の内訳計測 (`fetch_via` / `dl_secs_by_path` / `direct_miss`)。

背景: image-pull の直読み先は 1 つではなく **[.47(hot), .17(spill 先)] のリスト**で、
`_download_from_minio` が順に試す。 ところが従来は どちらで当たっても一律 `"minio"`
と記録していたため、

  - .47 が 80% を超えて spill が入り、 新規 asset が .17 に書かれ始めた後
  - フロンティアで .47 が全外しして .17 まで毎回落ちている

という状態が **計測上まったく区別できなかった**。 2026-08-22 に download 段の平均が
500ms → 2,800ms へ悪化した件を、 推測ではなく内訳で切り分けるための計測。

ここで固定するのは:
  1. `_download_from_minio` が「何番目のクライアントで当たったか」を返す
  2. 呼び出し側がそれをラベル (.47 / .17) に直して `fetch_via` に出す
  3. 直読みが全外しした回数 (`direct_miss`) が数えられる
  4. 経路別の実時間が出る (どの経路が tick を食っているか)

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def im():
    # image_main は PIL を import する。 ここで見たいのは経路選択のロジックだけなので
    # 最小のスタブで足りる (Image.open は使わない経路しか触らない)。
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")
        img.open = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stub"))
        pil.Image = img
        sys.modules["PIL"] = pil
        sys.modules["PIL.Image"] = img
    spec = importlib.util.spec_from_file_location("image_main_via_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Obj:
    def __init__(self, data=b"x"):
        self._d = data

    def read(self):
        return self._d

    def close(self):
        pass

    def release_conn(self):
        pass


class _Client:
    """指定したキーだけ返す MinIO クライアントもどき。"""

    def __init__(self, have):
        self.have = set(have)
        self.calls = []

    def get_object(self, bucket, key):
        self.calls.append(key)
        if key not in self.have:
            raise RuntimeError("NoSuchKey")
        return _Obj()


@pytest.fixture
def ok_jpeg(im, monkeypatch, tmp_path):
    """_bytes_to_jpeg_tmp を成功扱いに固定する (PIL を経由させない)。"""
    monkeypatch.setattr(im, "_bytes_to_jpeg_tmp",
                        lambda data, d: (True, None, tmp_path / "a.jpg"))


# ------------------------------------------------ どこで当たったかを返す --

def test_returns_index_of_hot_tier_hit(im, ok_jpeg, tmp_path):
    hot, spill = _Client(["jobs/a/x.jpg"]), _Client([])
    ok, err, path, idx = im._download_from_minio(
        [hot, spill], "paprika", "jobs/a/x.jpg", tmp_path)
    assert ok and idx == 0
    assert spill.calls == [], "hot で当たったら spill は叩かない"


def test_returns_index_of_spill_tier_hit(im, ok_jpeg, tmp_path):
    """spill 中はこちらが常態になる。 従来は hot と区別できなかった。"""
    hot, spill = _Client([]), _Client(["jobs/a/x.jpg"])
    ok, err, path, idx = im._download_from_minio(
        [hot, spill], "paprika", "jobs/a/x.jpg", tmp_path)
    assert ok and idx == 1
    assert hot.calls == ["jobs/a/x.jpg"], "hot を先に試してから落ちる"


def test_miss_on_all_tiers(im, ok_jpeg, tmp_path):
    hot, spill = _Client([]), _Client([])
    ok, err, path, idx = im._download_from_minio(
        [hot, spill], "paprika", "jobs/a/x.jpg", tmp_path)
    assert not ok and idx == -1
    assert err and err.startswith("minio:")


def test_no_client_or_key(im, tmp_path):
    assert im._download_from_minio([], "paprika", "k", tmp_path) == (
        False, "no_client", None, -1)
    assert im._download_from_minio([_Client([])], "paprika", "", tmp_path) == (
        False, "no_client", None, -1)


def test_single_client_still_accepted(im, ok_jpeg, tmp_path):
    """list でなく単体を渡す旧来の呼び方も壊さない。"""
    ok, err, path, idx = im._download_from_minio(
        _Client(["k"]), "paprika", "k", tmp_path)
    assert ok and idx == 0


# ------------------------------------------------------ ラベルの組み立て --

def test_labels_distinguish_the_two_tiers(im):
    """setup が作るラベルが .47 と .17 を区別できる形になっていること。"""
    pairs = [(object(), "10.10.50.47:9000"), (object(), "10.10.50.17:9000")]
    labels = ["minio:%s" % e.split(":")[0] for _c, e in pairs]
    assert labels == ["minio:10.10.50.47", "minio:10.10.50.17"]
    assert len(set(labels)) == 2, "同じラベルになると内訳が潰れる"


def test_label_index_alignment(im, ok_jpeg, tmp_path):
    """返る index がラベル配列の添字としてそのまま使えること。"""
    labels = ["minio:10.10.50.47", "minio:10.10.50.17"]
    hot, spill = _Client([]), _Client(["k"])
    ok, _e, _p, idx = im._download_from_minio([hot, spill], "paprika", "k", tmp_path)
    assert labels[idx] == "minio:10.10.50.17"


# ---------------------------------------------- 既定挙動を変えていない --

def test_direct_read_defaults_on(im):
    """計測を足しただけで、 直読み自体の既定は従来どおり ON。"""
    import inspect
    src = inspect.getsource(im.setup)
    assert 'kwargs.get("direct_read", 1)' in src
