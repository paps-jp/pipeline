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


def test_url_text_matches_terms_in_encoded_and_slug_urls(m):
    """URL の中の登録語も当てる (2026-11-22)。%エンコードを戻し、区切りの中の - _ . は無視する。"""
    import urllib.parse
    al = m.prepare_aliases(["テスト 花子", "Hanako Yamada"])
    enc = "https://ex.test/tag/" + urllib.parse.quote("テスト花子") + "/page/2"
    assert m.match_aliases(m.page_text(None, None, enc), al) == ["テスト 花子"]
    assert m.match_aliases(m.page_text(None, None, "https://ex.test/v/hanako-yamada.html?x=1"), al) == ["Hanako Yamada"]
    assert m.match_aliases(m.page_text(None, None, "https://ex.test/search?q=hanako+yamada"), al) == ["Hanako Yamada"]


def test_url_text_does_not_join_across_separators(m):
    """URL の区切りどうしや、タイトルと説明文の継ぎ目で語が偶然つながらない。"""
    al = m.prepare_aliases(["hanako"])
    assert m.match_aliases(m.page_text(None, None, "https://ex.test/han/ako"), al) == []
    assert m.match_aliases(m.page_text(None, None, "https://ex.test/?q=han&ako=1"), al) == []
    assert m.match_aliases(m.page_text("... han", "ako ...", None), al) == []
    assert m.match_aliases(m.page_text(None, None, "https://hanako.ex.test/"), al) == ["hanako"]


def test_url_like_patterns_cover_percent_encoded_and_kana_forms(m):
    import urllib.parse
    pats = m.url_like_patterns("テスト 花子")
    q = lambda s: "%" + urllib.parse.quote(s, safe="").replace("%", "!%") + "%"  # noqa: E731
    assert q("テスト花子") in pats and q("てすと花子") in pats and q("テスト 花子") in pats
    assert "%" + urllib.parse.quote_plus("テスト 花子", safe="").replace("%", "!%") + "%" in pats
    assert m.url_like_patterns("Hanako Yamada") == ["%Hanako Yamada%", "%HanakoYamada%"]
    assert m.url_like_patterns("a_b%c") == ["%a!_b!%c%", "%ab!%c%"]


def test_scan_text_hits_also_looks_at_urls_and_rescans_once(m, monkeypatch):
    """走査の SQL は url も LIKE で見て、URL にだけ語があるページも identity_text_hit に入る。
    見る欄が増えたので、登録語が同じでも既存のページを 1 回走査し直す (TEXT_SCAN_VERSION)。"""
    import json
    import urllib.parse
    aliases = {"k" * 32: m.prepare_aliases(["テスト 花子"])}
    cursor = {"textscan": json.dumps({"sig": "old-sig-before-url", "next": 999, "max": 999, "win": 1000})}
    sqls, written = [], []

    def q(db, sql, params=()):
        sqls.append((sql, list(params)))
        if "DISTINCT identity_key" in sql:
            return []
        if sql.startswith("SELECT MAX(id) FROM crawl"):
            return [(10,)]
        return [(7, None, None, "https://ex.test/" + urllib.parse.quote("テスト花子")),
                (8, "関係ないページ", None, "https://ex.test/other")]
    monkeypatch.setattr(m, "_q", q)
    monkeypatch.setattr(m, "_exec", lambda db, sql, params=(): 0)
    monkeypatch.setattr(m, "_exec_many", lambda db, sql, rows: written.extend(rows))
    monkeypatch.setattr(m, "_cursor_get", lambda db, name: cursor.get(name))
    monkeypatch.setattr(m, "_cursor_set", lambda db, name, v: cursor.__setitem__(name, v))
    state = {"text_scan_window": 1000, "text_scan_budget_s": 5, "text_scan_target_s": 2}
    st = m.scan_text_hits(state, None, aliases, deadline=__import__("time").time() + 5)
    scan = [x for x in sqls if x[0].startswith("SELECT id, title, description, url FROM crawl")]
    assert scan and scan[0][1][:2] == [0, 1000]   # 走査し直し: 0 から
    assert "url LIKE ? ESCAPE '!'" in scan[0][0]
    assert written == [("k" * 32, 7, "テスト 花子")] and st["hits"] == 1
    assert json.loads(cursor["textscan"])["sig"] == m._alias_sig(aliases)


def test_decide_with_text(m):
    # 顔だけで auto のものはそのまま
    assert m.decide_with_text(0.7, -1, 0.6, 0.6, 0.5, False) == "auto"
    # 0.5〜0.6 は登録語が一致したときだけ review
    assert m.decide_with_text(0.55, -1, 0.6, 0.6, 0.5, True) == "review"
    assert m.decide_with_text(0.55, -1, 0.6, 0.6, 0.5, False) is None
    # text_floor 未満・別人の正解の方が近いものは、登録語が一致しても捨てる
    assert m.decide_with_text(0.45, -1, 0.6, 0.6, 0.5, True) is None
    assert m.decide_with_text(0.55, 0.58, 0.6, 0.6, 0.5, True) is None


def test_write_candidates_reviews_gray_faces_with_text(m, monkeypatch):
    rng = np.random.default_rng(11)
    G = _person(rng, 4)
    base = G.mean(axis=0)
    base /= np.linalg.norm(base)
    noise = rng.standard_normal(DIM)
    noise -= noise.dot(base) * base
    noise /= np.linalg.norm(noise)
    gray = 0.57 * base + np.sqrt(1 - 0.57 ** 2) * noise   # 本人の正解との最大がおよそ 0.5〜0.6
    emb = {200: gray.astype(np.float32), 201: gray.astype(np.float32)}
    monkeypatch.setattr(m, "load_embeddings", lambda state, db, ids: {f: emb[f] for f in ids if f in emb})
    monkeypatch.setattr(m, "face_page_text", lambda db, ids: {200: "テスト花子 出演作品", 201: "別の人のページ"})
    ident = {"G": G, "N": np.zeros((0, DIM), np.float32), "excluded": set(),
             "aliases": m.prepare_aliases(["テスト 花子"])}
    mx = float((G @ gray).max())
    assert 0.5 <= mx < 0.6, mx
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.5, "dry_run": False}
    db = _Db()
    st = m.write_candidates(state, db, "k" * 32, ident, {200: 1, 201: 1}, "newface", None)
    assert st == {"candidates": 2, "auto": 0, "review": 1}
    rows = [e for e in db.log if e[0] == "many"][0][2]
    assert len(rows) == 1 and rows[0][1] == 200 and rows[0][2] == "review"
    assert rows[0][3] == "newface+text" and rows[0][10] == 1


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
    pages = {10: {A}, 20: {A, B}}
    m._record_text_faces({}, db, {A: {}, B: {}}, {100: [11, 10], 101: [20], 102: [99]}, pages, st)
    assert called == {A: ([100, 101], "page"), B: ([101], "page")}     # 102 はどの一致ページにも無い
    rows = sorted(r for e in db.log if e[0] == "many" for r in e[2])
    assert rows == [(A, 100, 10), (A, 101, 20), (B, 101, 20)]
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
