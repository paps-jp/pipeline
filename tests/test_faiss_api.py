"""faiss-api プラグイン (plugins/faiss_api/faiss_app.py) の検索。

守るもの:
  - nprobe は共有の index に書き込まず、検索ごとに渡す (index は読むだけ)
  - nprobe の違う検索が並行に走っても落ちず、結果が nprobe ごとに毎回同じ
    (以前は検索のたびに index の nprobe を書き換えていて、値の違う検索が重なると
     `IndexIVF.cpp: 'key < (idx_t)nlist' failed` でプロセスごと落ちた。2026-11-23 に本番で発生)
  - 呼び出しごとの指定が使えない作りの index では、要求された nprobe を無視して index の値で検索する

plugin は非公開リポジトリの入れ子 clone なので、未配置なら skip する。faiss / fastapi が無い環境でも skip する。
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest

faiss = pytest.importorskip("faiss")
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "faiss_api" / "faiss_app.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="faiss_api plugin が未配置")

DIM = 512
NLIST = 128
N = 30_000
NPROBES = (1, 4, 16, 64)


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("faiss_app_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_index(path: pathlib.Path, opq: bool = False):
    """本番と同じ作り: IndexIDMap2(IndexIVFPQ)、内積。opq=True なら OPQ を前に付けた作り"""
    rng = np.random.default_rng(1)
    xb = rng.standard_normal((N, DIM)).astype("float32")
    faiss.normalize_L2(xb)
    if opq:
        inner = faiss.index_factory(DIM, f"OPQ16_64,IVF{NLIST},PQ16", faiss.METRIC_INNER_PRODUCT)
    else:
        quantizer = faiss.IndexFlatIP(DIM)
        inner = faiss.IndexIVFPQ(quantizer, DIM, NLIST, 32, 8)
        inner.metric_type = faiss.METRIC_INNER_PRODUCT
    index = faiss.IndexIDMap2(inner)
    index.train(xb[:12_000])
    index.add_with_ids(xb, np.arange(1_000_000, 1_000_000 + N).astype("int64"))
    faiss.write_index(index, str(path))
    return xb


@pytest.fixture(scope="module")
def world(tmp_path_factory, m):
    d = tmp_path_factory.mktemp("faiss")
    xb = _build_index(d / "test.index")
    (d / "version.json").write_text(json.dumps({"index_file": "test.index", "finished_at": "2026-11-23 00:00:00"}))
    state = m.IndexState(str(d), str(d / "version.json"))
    assert state.reload(force=True), state.last_error
    queries = xb[:24] + 0.05 * np.random.default_rng(2).standard_normal((24, DIM)).astype("float32")

    # 正解: 手元だけの index の写しに nprobe を設定して、1 本ずつ検索した結果
    ref_index = faiss.read_index(str(d / "test.index"))
    ref: dict[tuple[int, int], tuple[list[int], list[float]]] = {}
    for nprobe in NPROBES:
        faiss.ParameterSpace().set_index_parameter(ref_index, "nprobe", nprobe)
        for qi, q in enumerate(queries):
            x = np.ascontiguousarray(q.reshape(1, -1), dtype="float32")
            faiss.normalize_L2(x)
            dist, ids = ref_index.search(x, 50)
            ref[(nprobe, qi)] = (ids[0].tolist(), dist[0].tolist())
    return {"dir": d, "state": state, "queries": queries, "ref": ref, "client": TestClient(m.build_app(state))}


def _index_nprobe(state) -> int:
    idx = faiss.downcast_index(state.index)
    return int(faiss.downcast_index(idx.index).nprobe)


def _same(got: dict, want: tuple[list[int], list[float]]) -> bool:
    """検索結果が正解と同じか。点数がまったく同じ候補どうしは、並び順が実行ごとに入れ替わりうるので、
    点数は全部の位置で比べ、顔 ID は「その点数が一覧の中で 1 つしか無い位置」だけで比べる。"""
    ids, scores = got["ids"], got["scores"]
    want_ids, want_scores = want
    if len(ids) != len(want_ids) or not np.allclose(scores, want_scores, rtol=0, atol=1e-6):
        return False
    keys = [round(float(v), 6) for v in want_scores]
    return all(a == b for a, b, k in zip(ids, want_ids, keys) if keys.count(k) == 1)


def _search(world, qi: int, nprobe: int | None, topk: int = 50) -> dict:
    body = {"vector": [world["queries"][qi].tolist()], "topk": topk}
    if nprobe is not None:
        body["nprobe"] = nprobe
    r = world["client"].post("/search", json=body)
    assert r.status_code == 200, r.text
    return r.json()


# ----------------------------------------------------------------------
def test_per_call_nprobe_is_available(world, m):
    st = world["state"]
    assert st.per_call_nprobe is True and st.bundle[0] is st.index and st.bundle[1] is not None
    assert world["client"].get("/version").json()["per_call_nprobe"] is True
    assert _index_nprobe(st) == m.DEFAULT_NPROBE


def test_results_match_reference_per_nprobe(world):
    for nprobe in NPROBES:
        for qi in range(len(world["queries"])):
            got = _search(world, qi, nprobe)
            assert got["nprobe"] == nprobe
            assert _same(got, world["ref"][(nprobe, qi)]), (nprobe, qi)
    # 探索範囲が違えば結果も違う (= 指定が本当に効いている)
    assert any(world["ref"][(1, qi)][1] != world["ref"][(64, qi)][1] for qi in range(len(world["queries"])))


def test_default_nprobe_and_response_shape(world, m):
    got = _search(world, 0, None)
    assert got["nprobe"] == m.DEFAULT_NPROBE and _same(got, world["ref"][(m.DEFAULT_NPROBE, 0)])
    assert set(got) == {"ids", "scores", "version", "nprobe"} and len(got["ids"]) == len(got["scores"]) == 50
    assert got["scores"] == sorted(got["scores"], reverse=True)


def test_search_does_not_write_to_the_shared_index(world, m):
    for nprobe in (1, 256, 1024, 7):
        _search(world, 3, nprobe)
        assert _index_nprobe(world["state"]) == m.DEFAULT_NPROBE


def test_concurrent_searches_with_different_nprobe(world):
    """値の違う検索を並行に大量に流しても落ちず、nprobe ごとに毎回同じ結果になる"""
    jobs = [(qi % len(world["queries"]), NPROBES[(qi * 7 + t) % len(NPROBES)])
            for t in range(16) for qi in range(150)]

    def one(job):
        qi, nprobe = job
        return job, _search(world, qi, nprobe)

    with ThreadPoolExecutor(16) as ex:
        results = list(ex.map(one, jobs))
    wrong = [job for job, got in results if not _same(got, world["ref"][(job[1], job[0])])]
    assert not wrong, f"{len(wrong)} / {len(results)} 件が正解と違う: {wrong[:5]}"


def test_validation(world):
    c = world["client"]
    assert c.post("/search", json={"vector": [[0.0] * 3], "topk": 1}).status_code == 400
    assert c.post("/search", json={"vector": [world["queries"][0].tolist()], "topk": 0}).status_code == 422
    assert c.post("/search", json={"vector": [world["queries"][0].tolist()], "topk": 1, "nprobe": 0}).status_code == 422


def test_falls_back_to_index_nprobe_when_per_call_is_unavailable(world, m):
    st = world["state"]
    saved = st.bundle
    try:
        st.bundle = (st.index, None)        # 呼び出しごとの指定が使えない作りのまね
        got = _search(world, 5, 1)
        assert got["nprobe"] == m.DEFAULT_NPROBE                    # 実際に使った値を返す
        assert _same(got, world["ref"][(m.DEFAULT_NPROBE, 5)])      # 要求 (1) ではなく index の値で検索
        assert _index_nprobe(st) == m.DEFAULT_NPROBE
    finally:
        st.bundle = saved


def test_params_builder_handles_pretransform(tmp_path, m):
    """OPQ を前に付けた作り (IndexIDMap2(IndexPreTransform(IVFPQ))) でも、呼び出しごとの指定が効く"""
    _build_index(tmp_path / "opq.index", opq=True)
    (tmp_path / "version.json").write_text(json.dumps({"index_file": "opq.index", "finished_at": "v-opq"}))
    st = m.IndexState(str(tmp_path), str(tmp_path / "version.json"))
    assert st.reload(force=True), st.last_error
    assert st.per_call_nprobe is True
    idx, build = st.bundle
    x = np.random.default_rng(3).standard_normal((1, DIM)).astype("float32")
    faiss.normalize_L2(x)
    _, a = idx.search(x, 30, params=build(1))
    _, b = idx.search(x, 30, params=build(NLIST))
    ref_index = faiss.read_index(str(tmp_path / "opq.index"))
    faiss.ParameterSpace().set_index_parameter(ref_index, "nprobe", NLIST)
    _, want = ref_index.search(x, 30)
    assert b.tolist() == want.tolist() and a.tolist() != b.tolist()


def test_params_builder_returns_none_without_ivf(m):
    flat = faiss.IndexIDMap2(faiss.IndexFlatIP(DIM))
    assert m.make_params_builder(flat) is None


def test_reload_swaps_bundle(world, m):
    st = world["state"]
    old = st.bundle
    assert st.reload(force=True)
    assert st.bundle[0] is st.index and st.bundle[0] is not old[0] and st.per_call_nprobe is True
    assert _same(_search(world, 1, 4), world["ref"][(4, 1)])


# ----------------------------------------------------------------------
# 以前の作り (検索のたびに index の nprobe を書き換える) が、本当に落ちることの再現。
#   別プロセスで動かす (落ちるとプロセスごと終わるため)。タイミング次第なので、決めた回数の中で
#   落ちなければ skip にする (失敗にはしない)。
# ----------------------------------------------------------------------
_OLD_STYLE = textwrap.dedent("""
    import sys, threading
    import faiss, numpy as np
    idx = faiss.read_index(sys.argv[1])
    rng = np.random.default_rng(0)
    xs = rng.standard_normal((64, 512)).astype("float32")
    faiss.normalize_L2(xs)
    def worker(t):
        for i in range(4000):
            nprobe = (1, 128)[(i + t) % 2]
            faiss.ParameterSpace().set_index_parameter(idx, "nprobe", nprobe)   # 以前の作り
            idx.search(xs[i % 64: i % 64 + 1], 50)
    ths = [threading.Thread(target=worker, args=(t,)) for t in range(16)]
    [t.start() for t in ths]; [t.join() for t in ths]
    print("survived")
""")


def test_old_style_shared_nprobe_crashes(world):
    r = subprocess.run([sys.executable, "-c", _OLD_STYLE, str(world["dir"] / "test.index")],
                       capture_output=True, text=True, timeout=600)
    if r.returncode == 0:
        pytest.skip("以前の作りが、今回の回数の中では落ちなかった (タイミング次第)")
    assert "nlist" in r.stderr or r.returncode < 0, r.stderr[-500:]
