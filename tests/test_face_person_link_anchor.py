"""face-person-link が identity-expand の anchor の person を候補にしないことの回帰テスト。

守りたいこと (設計は非公開リポジトリ側):
  - anchor の person に属する候補は、どれだけ近くても assign 先にしない (次に近いふつうの person か新しい person)
  - respect_anchors=0 なら今までどおり
  - anchor の一覧を読めないときは空 (今までどおり) で、警告は 1 回だけ。読み直しは anchor_ttl_s ごと

plugin は非公開リポジトリの入れ子 clone なので、未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "face_person_link" / "link_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="face_person_link plugin が未配置")


@pytest.fixture(scope="module")
def m():
    pytest.importorskip("httpx")
    pytest.importorskip("numpy")
    spec = importlib.util.spec_from_file_location("face_person_link_anchor_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Cur:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params=()):
        self.db.calls += 1
        if self.db.fail:
            raise RuntimeError("Table 'delian.identity_state' doesn't exist")

    def fetchall(self):
        return [(a,) for a in self.db.anchors]

    def close(self):
        pass


class _Db:
    def __init__(self, anchors=(), fail=False):
        self.anchors, self.fail, self.calls = list(anchors), fail, 0

    def cursor(self):
        return _Cur(self)


def _state(m, monkeypatch, db, **kw):
    st = {"rerank_exact": True, "respect_anchors": True, "anchor_ttl_s": 60.0, "db": db}
    st.update(kw)
    monkeypatch.setattr(m, "_ensure_db", lambda state: state["db"])
    return st


def _vecs():
    import numpy as np
    q = np.zeros(512, np.float32)
    q[0] = 1.0
    near = q.copy()
    near[1] = 0.1          # cos ≈ 0.995
    mid = q.copy()
    mid[1] = 0.5           # cos ≈ 0.894
    return q, near, mid


def test_anchor_person_is_not_chosen(m, monkeypatch):
    q, near, mid = _vecs()
    st = _state(m, monkeypatch, _Db(anchors=[900]))
    # 1 の person (900) は anchor。いちばん近くても選ばない
    pid, cos, _, _ = m._pick_best(st, q, [(1, 0.5), (2, 0.4)], {1: 900, 2: 55}, {1: near, 2: mid}, 0.75)
    assert pid == 55 and cos == pytest.approx(0.894, abs=1e-3)
    # 候補が anchor だけなら、どこにも入れない (= 新しい person)
    pid, *_ = m._pick_best(st, q, [(1, 0.5)], {1: 900}, {1: near}, 0.75)
    assert pid is None


def test_respect_anchors_off_keeps_old_behaviour(m, monkeypatch):
    q, near, mid = _vecs()
    st = _state(m, monkeypatch, _Db(anchors=[900]), respect_anchors=False)
    pid, *_ = m._pick_best(st, q, [(1, 0.5), (2, 0.4)], {1: 900, 2: 55}, {1: near, 2: mid}, 0.75)
    assert pid == 900


def test_anchor_list_unreadable_is_empty_and_cached(m, monkeypatch, caplog):
    q, near, _ = _vecs()
    db = _Db(fail=True)
    st = _state(m, monkeypatch, db)
    for _ in range(3):
        pid, *_ = m._pick_best(st, q, [(1, 0.5)], {1: 900}, {1: near}, 0.75)
        assert pid == 900                    # 読めなければ今までどおり
    assert db.calls == 1                     # anchor_ttl_s のあいだは読み直さない
    assert sum("anchor" in r.getMessage() for r in caplog.records) <= 1


class _BatchCur:
    def __init__(self, rows_by_prefix):
        self.rows_by_prefix, self.rows = rows_by_prefix, []

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.rows = next((r for p, r in self.rows_by_prefix.items() if s.startswith(p)), [])

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        pass


class _BatchDb:
    def __init__(self, rows_by_prefix):
        self.rows_by_prefix = rows_by_prefix

    def cursor(self):
        return _BatchCur(self.rows_by_prefix)


def test_anchor_member_is_not_relinked(m, monkeypatch):
    """anchor に入っている顔がキューに戻ってきても、付け直さない (FAISS も呼ばない)。"""
    import numpy as np

    db = _BatchDb({
        "SELECT face_id, shard_id, row_index FROM crawl_embedding_index": [(5, 0, 0)],
        "SELECT id, image_id, video_id, movie_id, person_id FROM crawl_face": [(5, 1, None, None, 900)],
        "SELECT anchor_person_id FROM identity_state": [(900,)],
        "SELECT image_id, video_id, movie_id, person_id FROM crawl_face": [(1, None, None, 900)],
        "SELECT shard_id, row_index FROM crawl_embedding_index": [(0, 0)],
    })
    st = _state(m, monkeypatch, db)
    st.update({"topk": 10, "counter": 0})
    monkeypatch.setattr(m, "_load_shard_embedding", lambda state, s, r: np.ones(512, np.float32))
    called = []
    monkeypatch.setattr(m, "_faiss_knn", lambda *a, **k: called.append(1) or ([], []))

    class _T:
        pk = "5"

    out = m.process_batch([_T()], None, st)
    assert out == [{"face_id": 5, "skip": "anchor_member"}] and called == []
    assert m.process(_T(), None, st)["skip"] == "anchor_member" and called == []
