"""identity-expand plugin (確認済みの顔から同じ人の写真を集める) の回帰テスト。

守りたいこと (設計は非公開リポジトリ側):
  - 判定は「別人の正解の方が近ければ捨てる」「auto_threshold 以上は auto」「review_floor 以上は review」
  - 矛盾 (同じ顔が本人と別人 / ほぼ同じ顔が本人と別人 / 1 つの顔が 2 人の本人) を見つける
  - 1 人の「本人」に別の人が混ざっていたら止める
  - 小さい外れのまとまりは、止めずに正解から外す
  - 確認済みの顔は候補として書かない。auto / review の書き込みは人の確認結果を上書きしない
  - faiss_api のレジストリから、新しく ready な窓口だけを、いちばん新しい索引の版で使う

plugin は非公開リポジトリの入れ子 clone なので、未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib

import numpy as np
import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "identity_expand" / "identity_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="identity_expand plugin が未配置")

DIM = 512


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("identity_expand_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _person(rng, n, spread=0.5):
    """同じ人の顔らしいベクトル n 本 (互いの cos はおよそ 1 / (1 + spread^2) = 0.8)。別の人とはおよそ 0。"""
    base = rng.standard_normal(DIM)
    base /= np.linalg.norm(base)
    v = base + spread * rng.standard_normal((n, DIM)) / np.sqrt(DIM)
    return v / np.linalg.norm(v, axis=1, keepdims=True)


# ----------------------------------------------------------------------
# 判定
# ----------------------------------------------------------------------
def test_decide_rejects_when_negative_is_closer(m):
    assert m.decide(0.9, 0.95, 0.6, 0.6) is None
    assert m.decide(0.9, 0.9, 0.6, 0.6) is None   # 同点も捨てる (別人の正解を優先)


def test_decide_thresholds(m):
    assert m.decide(0.61, -1.0, 0.6, 0.6) == "auto"
    assert m.decide(0.59, -1.0, 0.6, 0.6) is None
    # auto_threshold を上げると、その差の帯が review になる
    assert m.decide(0.7, -1.0, 0.85, 0.6) == "review"
    assert m.decide(0.9, -1.0, 0.85, 0.6) == "auto"


def test_score_compares_with_gallery_and_negatives(m):
    rng = np.random.default_rng(0)
    a = _person(rng, 6)
    b = _person(rng, 4)
    mx, top, neg = m.score(a[:2], a[2:], b)
    assert (mx > 0.6).all() and (neg < 0.3).all()
    assert (top <= mx + 1e-6).all()
    _, _, neg_none = m.score(a[:2], a[2:], None)
    assert (neg_none == -1.0).all()


# ----------------------------------------------------------------------
# 矛盾と混ざり
# ----------------------------------------------------------------------
def test_find_conflicts(m):
    rng = np.random.default_rng(1)
    p = _person(rng, 3)
    q = _person(rng, 2)
    emb = {1: p[0], 2: p[1], 3: p[0] * 0.999 + p[2] * 0.001, 10: q[0], 11: q[1], 20: q[0]}
    labels = {
        "k1": {"self": {1, 2, 5}, "not": {3, 5, 10}},   # 5 は両方 / 3 は 1 とほぼ同じ顔なのに別人
        "k2": {"self": {20, 2}, "not": set()},          # 2 は k1 の本人にも入っている
    }
    c = m.find_conflicts(labels, emb, dup_cos=0.97)
    assert c["k1"][5] == "both"
    assert c["k1"][1] == "near_dup" and c["k1"][3] == "near_dup"
    assert c["k1"][2] == "multi_identity" and c["k2"][2] == "multi_identity"
    assert 10 not in c["k1"] and 20 not in c["k2"]


def test_check_mixing_detects_two_people(m):
    rng = np.random.default_rng(2)
    G = np.vstack([_person(rng, 36), _person(rng, 60)])
    r = m.check_mixing(G)
    assert r["mixed"] and "混ざっている" in r["reason"]
    assert sorted(r["groups"], reverse=True)[:2] == [60, 36]


def test_check_mixing_single_person_and_outlier(m):
    rng = np.random.default_rng(3)
    G = np.vstack([_person(rng, 40), _person(rng, 1)])
    r = m.check_mixing(G)
    assert not r["mixed"]
    assert r["outliers"] == [40]          # 外れの 1 件だけを外す
    r1 = m.check_mixing(_person(rng, 30))
    assert not r1["mixed"] and r1["outliers"] == [] and r1["groups"] == [30]


def test_holdout_recall(m):
    rng = np.random.default_rng(4)
    rec = m.holdout_recall(_person(rng, 50), [0.6, 0.999])
    assert rec[0.6] == 1.0 and rec[0.999] == 0.0
    assert m.holdout_recall(_person(rng, 3), [0.6]) == {}   # 少なすぎるときは測らない


# ----------------------------------------------------------------------
# faiss_api のレジストリ
# ----------------------------------------------------------------------
def test_pick_endpoints(m):
    now = 1_800_000_000.0
    reg = {"items": {
        "ep/a": {"value": {"url": "http://a:9000/", "ready": True, "index_version": "2026-11-22 05:26:50", "ts": now - 10}},
        "ep/b": {"value": {"url": "http://b:9000", "ready": True, "index_version": "2026-11-20 01:00:00", "ts": now - 10}},
        "ep/c": {"value": {"url": "http://c:9000", "ready": False, "index_version": "2026-11-22 05:26:50", "ts": now}},
        "ep/d": {"value": {"url": "http://d:9000", "ready": True, "index_version": "2026-11-22 05:26:50", "ts": now - 999}},
        "other": {"value": {"url": "http://e:9000", "ready": True, "index_version": "2026-11-23 00:00:00", "ts": now}},
    }}
    eps, version = m.pick_endpoints(reg, now)
    assert version == "2026-11-22 05:26:50"
    assert eps == ["http://a:9000"]          # 古い版・ready でない・報告が古い窓口は使わない
    assert m.pick_endpoints({"items": {}}, now) == ([], None)


# ----------------------------------------------------------------------
# DB に書く行
# ----------------------------------------------------------------------
class _Cur:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=()):
        self.log.append(("execute", sql, tuple(params)))

    def executemany(self, sql, rows):
        self.log.append(("many", sql, list(rows)))

    def fetchall(self):
        return []

    def close(self):
        pass


class _Db:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cur(self.log)


def _state(m):
    return {"auto_threshold": 0.6, "review_floor": 0.6, "dry_run": True}


def test_write_candidates_skips_labeled_faces_and_writes_dry_run(m, monkeypatch):
    rng = np.random.default_rng(5)
    me = _person(rng, 8)
    other = _person(rng, 2)
    ident = {"G": me[:4], "N": other[:1], "excluded": {1, 2, 3, 4, 900}}
    emb = {100: me[5], 101: me[6], 102: other[1], 900: me[7]}
    monkeypatch.setattr(m, "load_embeddings", lambda state, db, ids: {f: emb[f] for f in ids if f in emb})
    db = _Db()
    st = m.write_candidates(_state(m), db, "k" * 32, ident, {100: 1, 101: 2, 102: 3, 900: 4}, "seed", "v1")
    assert st == {"candidates": 3, "auto": 2, "review": 0}      # 900 は確認済みなので候補にしない
    many = [e for e in db.log if e[0] == "many"]
    assert len(many) == 1
    rows = many[0][2]
    assert sorted(r[1] for r in rows) == [100, 101]              # 別人 (102) は書かない
    assert all(r[2] == "auto" and r[3] == "seed" and r[8] == "v1" and r[9] == 1 for r in rows)


def test_upsert_auto_does_not_override_human_labels(m):
    sql = " ".join(m.UPSERT_AUTO.split())
    assert "decision = IF(decision IN ('auto','review'), VALUES(decision), decision)" in sql
    # decision は最後に代入する (先に変えると、後ろの IF が新しい値を見てしまう)
    assert sql.rstrip().endswith("VALUES(decision), decision)")
    for col in ("rule", "max_cos", "top3_cos", "neg_cos", "index_version", "dry_run"):
        assert f"{col} = IF(decision IN ('auto','review')" in sql
