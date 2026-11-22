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


def test_search_seeds_judges_each_face_once(m, monkeypatch):
    """別の起点の検索にも出た顔を、1 回の中で重ねて判定・数えない。"""
    rng = np.random.default_rng(6)
    me = _person(rng, 3)
    state = {"label_emb": {1: me[0], 2: me[1], 3: me[2]}, "max_seeds": 100, "seed_batch": 2,
             "topk": 10, "nprobe": 8}
    ident = {"self": [1, 2, 3], "paused": False}
    monkeypatch.setattr(m, "_knn", lambda url, vec, topk, nprobe: [100, 101, 102])
    got: list[list[int]] = []
    monkeypatch.setattr(m, "write_candidates",
                        lambda st, db, key, idn, cand, rule, ver: (got.append(sorted(cand)),
                                                                   {"candidates": len(cand), "auto": len(cand),
                                                                    "review": 0})[1])
    st = m.search_seeds(state, _Db(), "k" * 32, ident, ["http://a:9000"], "v1", deadline=float("inf"))
    assert st["seeds_searched"] == 3 and st["seeds_left"] == 0
    assert got == [[100, 101, 102], []]          # 2 つ目の束では、もう判定した顔を渡さない
    assert st["auto"] == 3


# ----------------------------------------------------------------------
# 1 回のタスクの刻み方 (空回りしない)
# ----------------------------------------------------------------------
def _pace_state():
    return {"counter": 0, "loop_s": 1, "idle_s": 0.3, "control_url": "http://x", "workload_slug": "identity-expand"}


def test_process_stays_for_loop_s_when_idle(m, monkeypatch):
    """やることが無くても loop_s 秒は留まり、次のタスクは最後に 1 回だけ積む (初回デプロイで空回りした)。"""
    calls = {"pass": 0, "enqueue": 0}

    def fake_pass(state, db, deadline):
        calls["pass"] += 1
        return {"identities": 1, "seed_auto": 0}, False

    monkeypatch.setattr(m, "run_pass", fake_pass)
    monkeypatch.setattr(m, "_get_db", lambda state: None)
    monkeypatch.setattr(m, "_self_enqueue_next_tick",
                        lambda *a: calls.__setitem__("enqueue", calls["enqueue"] + 1))
    t0 = __import__("time").time()
    out = m.process(None, None, _pace_state())
    took = __import__("time").time() - t0
    assert took >= 0.95
    assert 2 <= calls["pass"] <= 5               # idle_s ごとに 1 巡。空回りしない
    assert calls["enqueue"] == 1
    assert out["passes"] == calls["pass"]


def test_process_waits_after_errors_too(m, monkeypatch):
    """VIEW が読めない等で毎回失敗しても、すぐに次を積まずに待つ。"""
    calls = {"pass": 0, "enqueue": 0}

    def failing_pass(state, db, deadline):
        calls["pass"] += 1
        raise RuntimeError("SELECT command denied")

    monkeypatch.setattr(m, "run_pass", failing_pass)
    monkeypatch.setattr(m, "_get_db", lambda state: None)
    monkeypatch.setattr(m, "_self_enqueue_next_tick",
                        lambda *a: calls.__setitem__("enqueue", calls["enqueue"] + 1))
    t0 = __import__("time").time()
    out = m.process(None, None, _pace_state())
    assert __import__("time").time() - t0 >= 0.95
    assert calls["pass"] <= 5 and calls["enqueue"] == 1
    assert out["errors"] == calls["pass"] and "denied" in out["last_error"]


def test_upsert_auto_does_not_override_human_labels(m):
    sql = " ".join(m.UPSERT_AUTO.split())
    assert "decision = IF(decision IN ('auto','review'), VALUES(decision), decision)" in sql
    # decision は最後に代入する (先に変えると、後ろの IF が新しい値を見てしまう)
    assert sql.rstrip().endswith("VALUES(decision), decision)")
    for col in ("rule", "max_cos", "top3_cos", "neg_cos", "index_version", "dry_run"):
        assert f"{col} = IF(decision IN ('auto','review')" in sql


# ----------------------------------------------------------------------
# person (段階 3): anchor への出し入れ
# ----------------------------------------------------------------------
def test_plan_person_moves_adds_and_removes(m):
    want = {"a": {1, 2, 3}, "b": {10}}
    members = {"a": {2, 3, 4}, "b": set()}
    plan = m.plan_person_moves(want, members, frozen=set(), limit=100)
    assert plan["a"] == {"add": [1], "remove": [4]}
    assert plan["b"] == {"add": [10], "remove": []}


def test_plan_person_moves_skips_ambiguous_and_frozen(m):
    # 5 は a と b の両方に入れたい → どちらにも入れない。a に入っていれば外す
    want = {"a": {1, 5}, "b": {5, 6}, "c": {7}}
    members = {"a": {5}, "c": {8}}
    plan = m.plan_person_moves(want, members, frozen={"c"}, limit=100)
    assert plan["a"] == {"add": [1], "remove": [5]}
    assert plan["b"] == {"add": [6], "remove": []}
    assert "c" not in plan                      # 止めている対象者は動かさない


def test_plan_person_moves_moves_directly_between_anchors(m):
    # a の anchor にいる 3 が b に入れたい顔になった → a からは「外す」にせず、b へ直接移す
    plan = m.plan_person_moves({"a": set(), "b": {3}}, {"a": {3}, "b": set()}, frozen=set(), limit=100)
    assert plan == {"b": {"add": [3], "remove": []}}


def test_plan_person_moves_limit_removes_first(m):
    plan = m.plan_person_moves({"a": {1, 2, 3}}, {"a": {7, 8}}, frozen=set(), limit=3)
    assert plan["a"]["remove"] == [7, 8] and plan["a"]["add"] == [1]


def test_anchor_centroid_picks_closest_self_face(m):
    rng = np.random.default_rng(7)
    G = _person(rng, 5)
    c, rep = m.anchor_centroid({"G": G, "self": [11, 12, 13, 14, 15]})
    assert abs(np.linalg.norm(c) - 1) < 1e-5
    assert rep == [11, 12, 13, 14, 15][int(np.argmax(G @ c))]
    assert m.anchor_centroid({"G": np.zeros((0, DIM), np.float32), "self": []}) is None


class _TxCur:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0
        self.lastrowid = None

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.db.log.append((s, tuple(params)))
        self.rowcount = self.db.rowcounts.pop(0) if s.startswith("UPDATE crawl_face") and self.db.rowcounts else 1

    def fetchall(self):
        return []

    def close(self):
        pass


class _TxDb:
    def __init__(self, rowcounts=()):
        self.log, self.rowcounts = [], list(rowcounts)
        self.autocommit = True
        self.commits = self.rollbacks = 0

    def cursor(self):
        return _TxCur(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _meta(pid, video=False, site="s", fid=42):
    return {"id": fid, "person_id": pid, "site": site, "has_image": not video, "is_video": video}


def test_move_face_into_anchor_updates_both_persons_and_logs(m):
    db = _TxDb()
    meta = _meta(7)
    assert m.move_face(db, "k" * 32, meta, 900, "auto", 0.8)
    sqls = [s for s, _ in db.log]
    assert sqls[0].startswith("UPDATE crawl_face SET person_id=?")
    assert db.log[0][1] == (900, 0.8, 42, 7)                 # 読んだときと同じ person のときだけ書く
    assert any("GREATEST(face_count, 1) - 1" in s for s in sqls)        # 元の person を減らす
    assert any(s.startswith("UPDATE crawl_person SET face_count = face_count + 1") for s in sqls)
    assert any("INSERT INTO crawl_person_appearance" in s for s in sqls)
    assert any("INSERT INTO identity_person_move" in s for s in sqls)
    assert db.commits == 1 and db.rollbacks == 0 and db.autocommit is True
    assert meta["person_id"] == 900


def test_move_face_release_requeues_for_face_person_link(m):
    db = _TxDb()
    assert m.move_face(db, "k" * 32, _meta(900), None, "release", None)
    sqls = [s for s, _ in db.log]
    assert any(s.startswith("INSERT INTO face_person_link_queue") for s in sqls)
    assert not any(s.startswith("UPDATE crawl_person SET face_count = face_count + 1") for s in sqls)


def test_move_face_raced_does_nothing(m):
    db = _TxDb(rowcounts=[0])     # face-person-link が先に書いていた
    assert not m.move_face(db, "k" * 32, _meta(7), 900, "auto", 0.8)
    assert len(db.log) == 1 and db.rollbacks == 1 and db.commits == 0 and db.autocommit is True


def _person_state(**kw):
    st = {"dry_run": False, "person_write": False, "person_release_all": False, "person_move_batch": 100}
    st.update(kw)
    return st


def _fake_q(rows_by_prefix):
    def q(db, sql, params=()):
        s = " ".join(sql.split())
        for prefix, rows in rows_by_prefix.items():
            if s.startswith(prefix):
                return rows
        return []
    return q


WANT = "SELECT identity_key, face_id, decision, max_cos FROM identity_match"
ANCHORS = "SELECT identity_key, anchor_person_id FROM identity_state"


def test_sync_persons_counts_only_unless_person_write(m, monkeypatch):
    monkeypatch.setattr(m, "_q", _fake_q({WANT: [("k", 1, "label_self", None), ("k", 2, "auto", 0.7)],
                                          ANCHORS: []}))
    moved = []
    monkeypatch.setattr(m, "move_face", lambda *a, **k: moved.append(a) or True)
    idents = {"k": {"paused": False, "self": [1], "G": _person(np.random.default_rng(8), 1)}}
    st = m.sync_persons(_person_state(), None, idents)
    assert st["would_add"] == 2 and st["added"] == 0 and moved == []
    st = m.sync_persons(_person_state(dry_run=True), None, idents)
    assert st["would_add"] == 0                          # 試運転中は person に触らない


def test_sync_persons_writes_into_new_anchor(m, monkeypatch):
    monkeypatch.setattr(m, "_q", _fake_q({WANT: [("k", 1, "label_self", None), ("k", 2, "auto", 0.7)],
                                          ANCHORS: []}))
    monkeypatch.setattr(m, "_face_meta", lambda db, ids: {f: _meta(None, fid=f) for f in ids})
    monkeypatch.setattr(m, "ensure_anchor", lambda db, key, ident: 900)
    moved = []
    monkeypatch.setattr(m, "move_face",
                        lambda db, key, meta, to, reason, score: moved.append((meta["id"], to, reason, score)) or True)
    idents = {"k": {"paused": False, "self": [1], "G": _person(np.random.default_rng(9), 1)}}
    st = m.sync_persons(_person_state(person_write=True), None, idents)
    assert st["anchors_created"] == 1 and st["added"] == 2
    assert moved == [(1, 900, "label_self", 1.0), (2, 900, "auto", 0.7)]


def test_sync_persons_release_all(m, monkeypatch):
    monkeypatch.setattr(m, "_q", _fake_q({WANT: [("k", 1, "label_self", None)], ANCHORS: [("k", 900)],
                                          "SELECT id FROM crawl_face WHERE person_id=?": [(1,), (2,)]}))
    monkeypatch.setattr(m, "_face_meta", lambda db, ids: {f: _meta(900, fid=f) for f in ids})
    moved = []
    monkeypatch.setattr(m, "move_face",
                        lambda db, key, meta, to, reason, score: moved.append((meta["id"], to, reason)) or True)
    idents = {"k": {"paused": True, "self": [1], "G": _person(np.random.default_rng(10), 1)}}
    st = m.sync_persons(_person_state(person_write=True, person_release_all=True), None, idents)
    assert st["removed"] == 2 and moved == [(1, None, "release"), (2, None, "release")]


# ----------------------------------------------------------------------
# 登録語 (段階 4)
# ----------------------------------------------------------------------
def test_norm_text_folds_width_case_kana_and_separators(m):
    assert m.norm_text("ＡＢＣ－１２３") == m.norm_text("abc123") == "abc123"
    assert m.norm_text("テスト 花子") == m.norm_text("てすと花子")
    assert m.norm_text("ﾃｽﾄ") == m.norm_text("テスト")


def test_prepare_and_match_aliases(m):
    al = m.prepare_aliases(["テスト 花子", "ABC-123", "A B"])
    assert [a for a, _ in al] == ["テスト 花子", "ABC-123"]          # 正規化して 3 文字未満の語は使わない
    assert m.match_aliases("【新作】abc123 てすと花子 出演", al) == ["テスト 花子", "ABC-123"]
    assert m.match_aliases("無関係なページ", al) == []
    assert m.match_aliases(None, al) == []


def test_like_patterns_escape_and_variants(m):
    pats = m.like_patterns("50%_off テスト")
    assert "%50!%!_off テスト%" in pats            # LIKE の記号は ! でエスケープ
    assert "%50!%off%" not in pats and any("テスト%" in p for p in pats)
    assert m.like_patterns("ABC-123") == ["%ABC-123%", "%ABC123%"]


def test_term_occurrences_tell_whole_words_from_parts_of_longer_names(m):
    """語がそのまま出ているか、長い名前の一部として出ているか (2026-11-22、設計 §7.10)。

    本番の一致 14,218 ページのうち、語がそのまま出ていたのは 12%。残りは別の人のフルネームの一部などだった。
    """
    n = m.norm_text("ハナコ")
    S, W = m.STRONG, m.WEAK
    assert m.term_occurrences("【新作】ハナコ 写真集", n) == [(S, "")]
    assert m.term_occurrences("ハナコちゃんの日記", n) == [(S, "")]          # 敬称
    assert m.term_occurrences("人気のはなこ出演作", n) == [(S, "")]          # 前が助詞・後ろが漢字
    assert m.term_occurrences("佐藤はなこ 写真集", n) == [(W, "佐藤はなこ")]  # 前に別の苗字
    assert m.term_occurrences("はなこみみ", n) == [(W, "はなこみみ")]        # 後ろに名前が続く
    code = m.norm_text("ABC-123")
    assert m.term_occurrences("ABC-1234 発売", code) == [(W, "abc-1234")]     # 別の品番
    assert m.term_occurrences("abc123 発売", code) == [(S, "")]
    # 照合は norm_text と同じ (区切りの違いは無視)、前後は元の文字で見る
    assert m.term_occurrences("テスト・花子", m.norm_text("テスト 花子")) == [(S, "")]
    assert m.term_occurrences("ハナコ と 佐藤ハナコ", n) == [(S, ""), (W, "佐藤はなこ")]
    assert m.term_occurrences(None, n) == [] and m.term_occurrences("別の話", n) == []


def test_is_search_url(m):
    assert m.is_search_url("https://ex.test/?word=x&page=2")
    assert m.is_search_url("https://ex.test/search?search_type=photos&search_query=x")
    assert m.is_search_url("https://ex.test/search/?genre=1")
    assert not m.is_search_url("https://ex.test/video/123/title")
    assert not m.is_search_url("https://ex.test/researcher/1") and not m.is_search_url(None)


def test_classify_page_takes_the_strongest_field_and_ignores_search_pages(m):
    al = m.prepare_aliases(["ハナコ", "ABC-123"])
    res = m.classify_page({"title": "佐藤ハナコ", "body": "出演: ハナコ / 関連 ABC-1234"}, "https://ex.test/v/1", al)
    assert res == [("ハナコ", m.STRONG, "body", ""), ("ABC-123", m.WEAK, "body", "abc-1234")]
    # 検索結果のページは一致を記録するが、手がかりにしない
    assert m.classify_page({"title": "ハナコ"}, "https://ex.test/?q=ハナコ", al) == [("ハナコ", m.NONE, "title", "")]
    # URL にだけ語があっても当てない
    assert m.classify_page({"title": "関係ないページ"}, "https://ex.test/hanako/abc-123", al) == []


def test_scan_text_hits_reads_titles_only_records_strength_and_rescans_once(m, monkeypatch):
    """走査はタイトル・説明文だけを LIKE で見て、語の出方の強さを identity_text_page に書く。
    URL は見ない。判定を変えたので、登録語が同じでも既存のページを 1 回走査し直す (TEXT_SCAN_VERSION)。"""
    import json
    aliases = {"k" * 32: m.prepare_aliases(["ハナコ"])}
    cursor = {"textscan": json.dumps({"sig": "sig-from-version-2", "next": 999, "max": 999, "win": 1000})}
    sqls, written = [], []

    def q(db, sql, params=()):
        sqls.append((sql, list(params)))
        if "DISTINCT identity_key" in sql:
            return [("k" * 32, "消した語")]
        if sql.startswith("SELECT MAX(id) FROM crawl"):
            return [(10,)]
        return [(7, "ハナコ 写真集", None, "https://ex.test/7"),
                (8, "佐藤ハナコ", None, "https://ex.test/8"),
                (9, "ハナコ", None, "https://ex.test/?word=ハナコ")]
    deleted = []
    monkeypatch.setattr(m, "_q", q)
    monkeypatch.setattr(m, "_exec", lambda db, sql, params=(): deleted.append(params) or 1)
    monkeypatch.setattr(m, "_exec_many", lambda db, sql, rows: written.extend(rows))
    monkeypatch.setattr(m, "_cursor_get", lambda db, name: cursor.get(name))
    monkeypatch.setattr(m, "_cursor_set", lambda db, name, v: cursor.__setitem__(name, v))
    state = {"text_scan_window": 1000, "text_scan_budget_s": 5, "text_scan_target_s": 2}
    st = m.scan_text_hits(state, None, aliases, deadline=__import__("time").time() + 5)
    scan = [x for x in sqls if x[0].startswith("SELECT id, title, description, url FROM crawl")]
    assert scan and scan[0][1][:2] == [0, 1000]       # 走査し直し: 0 から
    assert "url LIKE" not in scan[0][0]
    K = "k" * 32
    assert written == [(K, 7, "ハナコ", 2, "title", "", "scan"), (K, 8, "ハナコ", 1, "title", "佐藤はなこ", "scan"),
                       (K, 9, "ハナコ", 0, "title", "", "scan")]
    assert st["hits"] == 3 and st["removed"] == 1 and deleted[0] == (K, "消した語")
    assert json.loads(cursor["textscan"])["sig"] == m._alias_sig(aliases)


def test_upsert_text_page_keeps_the_stronger_row(m):
    """同じページ・同じ語は強いほうを残す。field 等は strength を上げる前に書く (MariaDB は左から評価する)。"""
    sql = m.UPSERT_TEXT_PAGE
    assert sql.index("field    = IF(VALUES(strength) > strength") < sql.index("strength = GREATEST(")
    assert sql.index("src      = IF(") < sql.index("strength = GREATEST(")


def test_decide_with_text(m):
    S, W = m.STRONG, m.WEAK
    # 顔だけで auto のものはそのまま
    assert m.decide_with_text(0.7, -1, 0.6, 0.6, 0.2, 0) == "auto"
    # 語がそのまま出ていれば text_floor から、長い名前の一部なら weak_floor から review
    assert m.decide_with_text(0.25, -1, 0.6, 0.6, 0.2, S, 0.5) == "review"
    assert m.decide_with_text(0.25, -1, 0.6, 0.6, 0.2, W, 0.5) is None
    assert m.decide_with_text(0.55, -1, 0.6, 0.6, 0.2, W, 0.5) == "review"
    assert m.decide_with_text(0.55, -1, 0.6, 0.6, 0.2, 0) is None
    # True は STRONG (旧い呼び方)。text_floor 未満・別人の正解の方が近いものは捨てる
    assert m.decide_with_text(0.25, -1, 0.6, 0.6, 0.2, True) == "review"
    assert m.decide_with_text(0.15, -1, 0.6, 0.6, 0.2, S) is None
    assert m.decide_with_text(0.55, 0.58, 0.6, 0.6, 0.2, S) is None


def _gray_face(m, rng, target):
    G = _person(rng, 4)
    base = G.mean(axis=0)
    base /= np.linalg.norm(base)
    noise = rng.standard_normal(DIM)
    noise -= noise.dot(base) * base
    noise /= np.linalg.norm(noise)
    return G, (target * base + np.sqrt(1 - target ** 2) * noise).astype(np.float32)


def test_write_candidates_uses_the_strength_of_the_text(m, monkeypatch):
    rng = np.random.default_rng(11)
    G, gray = _gray_face(m, rng, 0.45)
    mx = float((G @ gray).max())
    assert 0.3 <= mx < 0.5, mx
    emb = {200: gray, 201: gray, 202: gray}
    monkeypatch.setattr(m, "load_embeddings", lambda state, db, ids: {f: emb[f] for f in ids if f in emb})
    monkeypatch.setattr(m, "face_page_text", lambda db, ids: {
        200: [("ハナコ 出演作品", None, "https://ex.test/1")],
        201: [("佐藤ハナコ 出演作品", None, "https://ex.test/2")],      # 長い名前の一部 → 0.5 未満は出さない
        202: [("ハナコ", None, "https://ex.test/?q=ハナコ")]})          # 検索ページ → 出さない
    ident = {"G": G, "N": np.zeros((0, DIM), np.float32), "excluded": set(), "aliases": m.prepare_aliases(["ハナコ"])}
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.2, "text_floor_weak": 0.5, "dry_run": False}
    db = _Db()
    st = m.write_candidates(state, db, "k" * 32, ident, {200: 1, 201: 1, 202: 1}, "newface", None)
    assert st == {"candidates": 3, "auto": 0, "review": 1}
    rows = [e for e in db.log if e[0] == "many"][0][2]
    assert len(rows) == 1 and rows[0][1] == 200 and rows[0][2] == "review"
    assert rows[0][3] == "newface+text" and rows[0][10] == 1 and rows[0][11] == m.STRONG


def test_text_hits_for_combines_first_page_text_faces_and_ingested_images(m, monkeypatch):
    """最初のページの文字・一致ページに載っていた顔 (identity_text_face)・取り込みで一致した画像 (identity_text_image)
    のうち、一番強い手がかりを顔ごとに返す。強さ 0 (検索ページ) は手がかりにしない。"""
    monkeypatch.setattr(m, "face_page_text", lambda db, ids: {
        1: [("佐藤ハナコ", None, "https://ex.test/1")], 2: [("ハナコ", None, "https://ex.test/?q=x")]})

    def q(db, sql, params=()):
        if "FROM identity_text_face" in sql:
            return [(3, 1)]
        if "identity_text_image" in sql:
            return [(1, 2), (4, 0)]
        return []
    monkeypatch.setattr(m, "_q", q)
    looked, s = m.text_hits_for(None, "k" * 32, m.prepare_aliases(["ハナコ"]), [1, 2, 3, 4, 5])
    assert looked == {1, 2, 3, 4} and s == {1: m.STRONG, 3: m.WEAK}


def test_consume_page_candidates_records_pages_and_images_then_deletes(m, monkeypatch):
    """取り込みで残った本文を判定し、ページ (identity_text_page) と画像 (identity_text_image) を書いて行を消す。
    alt に語がそのまま出ている画像は、ページが「長い名前の一部」でも STRONG。もう顔がある画像はここで判定する。"""
    import hashlib
    import json
    K = "k" * 32
    imgs = [{"u": "https://cdn/a.jpg", "alt": "ハナコ", "main": 1}, {"u": "https://cdn/b.jpg", "alt": "", "main": 0}]
    rows = [(1, 500, "https://ex.test/v/1", "佐藤ハナコ と共演", None, None, None, "本文", json.dumps(imgs))]
    calls = {"q": 0}
    written, deleted = {}, []

    def q(db, sql, params=()):
        if sql.startswith("SELECT id, crawl_id"):
            calls["q"] += 1
            return rows if calls["q"] == 1 else []
        if "FROM crawl_image ci JOIN crawl_face" in sql:
            assert params[0] in (hashlib.sha256(b"https://cdn/a.jpg").digest(), hashlib.sha256(b"https://cdn/b.jpg").digest())
            return [(900,)]
        return []
    monkeypatch.setattr(m, "_q", q)
    monkeypatch.setattr(m, "_exec", lambda db, sql, params=(): deleted.append((sql, list(params))) or 0)
    monkeypatch.setattr(m, "_exec_many", lambda db, sql, r: written.setdefault(sql.split()[2], []).extend(r))
    judged = {}
    monkeypatch.setattr(m, "write_candidates", lambda state, db, key, ident, cand, rule, ver:
                        judged.update({key: (sorted(cand), rule)}) or {"auto": 0, "review": 1})
    idents = {K: {"paused": False, "G": np.zeros((3, DIM), np.float32)}}
    state = {"page_cand_budget_s": 5, "page_cand_batch": 10}
    st = m.consume_page_candidates(state, None, {K: m.prepare_aliases(["ハナコ"])}, idents,
                                   deadline=__import__("time").time() + 5)
    assert written["identity_text_page"] == [(K, 500, "ハナコ", m.WEAK, "title", "佐藤はなこ", "ingest")]
    img = sorted(written["identity_text_image"], key=lambda r: r[3])
    assert [(r[1], r[3], r[4]) for r in img] == [(hashlib.sha256(b"https://cdn/b.jpg").digest(), m.WEAK, 0),
                                                 (hashlib.sha256(b"https://cdn/a.jpg").digest(), m.STRONG, 1)]
    assert judged == {K: ([900], "page")}
    assert any("DELETE FROM page_text_candidate WHERE id IN" in s and p == [1] for s, p in deleted)
    assert st["pages"] == 1 and st["weak"] == 1 and st["images"] == 2 and st["review"] == 1


def test_consume_page_candidates_skips_search_pages(m, monkeypatch):
    import json
    K = "k" * 32
    rows = [(1, 500, "https://ex.test/?word=ハナコ", "ハナコ", None, None, None, None,
             json.dumps([{"u": "https://cdn/a.jpg", "alt": "ハナコ"}]))]
    calls = {"q": 0}
    written = {}

    def q(db, sql, params=()):
        if sql.startswith("SELECT id, crawl_id"):
            calls["q"] += 1
            return rows if calls["q"] == 1 else []
        return []
    monkeypatch.setattr(m, "_q", q)
    monkeypatch.setattr(m, "_exec", lambda db, sql, params=(): 0)
    monkeypatch.setattr(m, "_exec_many", lambda db, sql, r: written.setdefault(sql.split()[2], []).extend(r))
    st = m.consume_page_candidates({"page_cand_budget_s": 5, "page_cand_batch": 10}, None,
                                   {K: m.prepare_aliases(["ハナコ"])}, {}, deadline=__import__("time").time() + 5)
    assert written["identity_text_page"][0][3] == m.NONE and written.get("identity_text_image", []) == []
    assert st["search"] == 1


def test_run_pass_scans_pages_for_clients_without_face_labels(m, monkeypatch):
    """顔の確認が 1 件も無い相談者でも、登録語があればページを照合する (2026-11-22 本番で走査が始まらなかった)。"""
    monkeypatch.setattr(m, "sync_labels", lambda state, db: {})
    monkeypatch.setattr(m, "load_aliases", lambda state, db: {"k" * 32: m.prepare_aliases(["テスト 花子"])})
    monkeypatch.setattr(m, "_faiss", lambda state: ([], None))
    monkeypatch.setattr(m, "match_new_faces", lambda state, db, idents, deadline: {"new_faces": 0, "auto": 0, "review": 0})
    monkeypatch.setattr(m, "sync_persons", lambda state, db, idents: {})
    monkeypatch.setattr(m, "daily_eval", lambda state, db, idents: 0)
    got = {}
    monkeypatch.setattr(m, "scan_text_hits",
                        lambda state, db, aliases, deadline: got.update(aliases) or {"scanned_to": 5, "hits": 1})
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.5, "newface_batch": 100}
    out, _busy = m.run_pass(state, None, deadline=__import__("time").time() + 5)
    assert list(got) == ["k" * 32] and out["text_scanned_to"] == 5 and out["text_hits"] == 1


def test_record_text_faces_routes_faces_to_the_identities_of_their_pages(m, monkeypatch):
    """一致したページに載っていた顔は、そのページに語が一致した対象者だけの候補にする。"""
    A, B = "a" * 32, "b" * 32
    called = {}
    monkeypatch.setattr(m, "write_candidates",
                        lambda state, db, key, ident, cand, rule, ver: called.update({key: (sorted(cand), rule)})
                        or {"candidates": len(cand), "auto": 0, "review": len(cand)})
    db = _Db()
    st = {"faces": 0, "auto": 0, "review": 0}
    pages = {10: {A: 2}, 20: {A: 1, B: 1}, 11: {A: 1}}
    m._record_text_faces({}, db, {A: {}, B: {}}, {100: [11, 10], 101: [20], 102: [99]}, pages, st)
    assert called == {A: ([100, 101], "page"), B: ([101], "page")}     # 102 はどの一致ページにも無い
    rows = sorted(r for e in db.log if e[0] == "many" for r in e[2])
    # 顔が何ページにも載っていれば、一番強いページを残す (100 は 11 で 1、10 で 2)
    assert rows == [(A, 100, 10, 2), (A, 101, 20, 1), (B, 101, 20, 1)]
    assert st == {"faces": 3, "auto": 0, "review": 3}


def test_run_pass_stays_busy_while_text_face_scan_has_sites_left(m, monkeypatch):
    monkeypatch.setattr(m, "sync_labels", lambda state, db: {})
    monkeypatch.setattr(m, "load_aliases", lambda state, db: {})
    monkeypatch.setattr(m, "_faiss", lambda state: ([], None))
    monkeypatch.setattr(m, "match_new_faces", lambda state, db, idents, deadline: {"new_faces": 0, "auto": 0, "review": 0})
    monkeypatch.setattr(m, "sync_persons", lambda state, db, idents: {})
    monkeypatch.setattr(m, "daily_eval", lambda state, db, idents: 0)
    monkeypatch.setattr(m, "scan_text_hits", lambda state, db, aliases, deadline: {"scanned_to": 0})
    left = {"n": 3}
    monkeypatch.setattr(m, "scan_text_faces", lambda state, db, idents, deadline:
                        {"sites": 1, "sites_left": left["n"], "faces": 7, "review": 2})
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.2, "newface_batch": 100}
    out, busy = m.run_pass(state, None, deadline=__import__("time").time() + 5)
    assert busy and out["textface_faces"] == 7 and out["textface_review"] == 2
    left["n"] = 0
    _out, busy = m.run_pass(state, None, deadline=__import__("time").time() + 5)
    assert not busy


def test_run_pass_consumes_page_candidates_and_stays_busy_while_rows_are_left(m, monkeypatch):
    """取り込みで残った本文 (page_text_candidate) を毎巡読む。読み残しがあれば待たずに次の巡へ。"""
    monkeypatch.setattr(m, "sync_labels", lambda state, db: {})
    monkeypatch.setattr(m, "load_aliases", lambda state, db: {"k" * 32: m.prepare_aliases(["ハナコ"])})
    monkeypatch.setattr(m, "_faiss", lambda state: ([], None))
    monkeypatch.setattr(m, "match_new_faces", lambda state, db, idents, deadline: {"new_faces": 0, "auto": 0, "review": 0})
    monkeypatch.setattr(m, "sync_persons", lambda state, db, idents: {})
    monkeypatch.setattr(m, "daily_eval", lambda state, db, idents: 0)
    monkeypatch.setattr(m, "scan_text_hits", lambda state, db, aliases, deadline: {"scanned_to": 0})
    monkeypatch.setattr(m, "scan_text_faces", lambda state, db, idents, deadline: {"sites_left": 0})
    stats = []
    monkeypatch.setattr(m, "refresh_alias_stats", lambda state, db, aliases: stats.append(sorted(aliases)) or 0)
    left = {"n": 1}
    monkeypatch.setattr(m, "consume_page_candidates", lambda state, db, aliases, idents, deadline:
                        {"pages": 40, "strong": 3, "weak": 9, "left": left["n"]})
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.2, "newface_batch": 100}
    out, busy = m.run_pass(state, None, deadline=__import__("time").time() + 5)
    assert busy and out["ingest_pages"] == 40 and out["ingest_weak"] == 9 and "ingest_left" not in out
    assert stats == [["k" * 32]]
    left["n"] = 0
    _out, busy = m.run_pass(state, None, deadline=__import__("time").time() + 5)
    assert not busy
