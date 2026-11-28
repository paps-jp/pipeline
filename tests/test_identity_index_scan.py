"""identity-expand: あとから登録した語で、索引から過去のページを引く (identity personing 設計 §7.11 の 5) を固定する。

- 登録語の N グラムは image-pull (imgpull_page_text.query_ngrams) と同じ番号になる
- 索引の読み方は page-keyword-compact の決まりどおり (merged/ のある日はそれだけ、month= のある月は day= を読まない)
- N グラムが全部そろうページは WEAK、語の索引で語そのものが出たページは STRONG、検索結果のページは NONE。src='index'
- 閉じた日を 1 日ずつ引き、進みを identity_index_scan に書いて続きから。尽きたら育っている分と語の索引を引き (open)、
  index_tail_s 秒後にもう 1 度 (tail) 引いて終わり。消えた語の進みは消す
- index_mode=1 のときだけ動き、時間切れで残りがあれば待たずに次の巡へ
"""

from __future__ import annotations

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

_PLUG = Path(__file__).resolve().parents[1] / "plugins"
_IE = _PLUG / "identity_expand" / "identity_main.py"
_PT = _PLUG / "paprika_image_pull" / "imgpull_page_text.py"
pytestmark = pytest.mark.skipif(not _IE.exists(), reason="plugins リポジトリ未チェックアウト")
K = "k" * 32


def _has(mod: str) -> bool:
    if mod in sys.modules:
        return True
    try:
        return importlib.util.find_spec(mod) is not None
    except (ImportError, ValueError):
        return False


needs_duck = pytest.mark.skipif(not (_has("duckdb") and _has("pyarrow")), reason="DuckDB / pyarrow が無い")


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("identity_expand_index_test", _IE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def pt():
    sys.path.insert(0, str(_PT.parent))
    if not _has("image_url_normalizer"):
        stub = types.ModuleType("image_url_normalizer")
        stub.normalize_image_url = lambda u: u
        sys.modules["image_url_normalizer"] = stub
    import imgpull_page_text
    return imgpull_page_text


@pytest.mark.parametrize("term", ["山田花子", "やまだ はなこ", "ヤマダハナコ", "Yui Hatano", "yuihatano", "PPPD-173",
                                  "Mary Jane Watson", "山田Hanako", "김민지", "ｱｲｳｴｵ"])
def test_query_ngrams_matches_image_pull(m, pt, term):
    assert m.query_ngrams(term) == pt.query_ngrams(term)


def test_ng_units_reads_merged_days_and_hourly_and_tick_files(m, tmp_path):
    ng = tmp_path / "ng-v2"
    for f in ("day=2026-11-23/merged/grams-0000.parquet", "day=2026-11-23/merged/.plan.json",
              "day=2026-11-23/hour=05/grams.parquet",           # 消す前の残り。merged があれば読まない
              "day=2026-11-24/merged/grams-0000.parquet", "day=2026-11-24/merged/grams-0001.parquet",
              "day=2026-11-25/hour=00/grams.parquet", "day=2026-11-25/hour=01/grams.parquet",
              "day=2026-11-25/.merged.tmp/grams-0000.parquet",  # まとめ直しの途中は読まない
              "2026-11-25/02/grams-1-1.parquet"):
        (ng / f).parent.mkdir(parents=True, exist_ok=True)
        (ng / f).write_bytes(b"")
    closed, open_files = m.ng_units(str(ng))
    assert [(d, [Path(f).name for f in fs]) for d, fs in closed] == [
        ("2026-11-23", ["grams-0000.parquet"]), ("2026-11-24", ["grams-0000.parquet", "grams-0001.parquet"])]
    assert [str(Path(f).relative_to(ng)).replace("\\", "/") for f in open_files] == [
        "day=2026-11-25/hour=00/grams.parquet", "day=2026-11-25/hour=01/grams.parquet", "2026-11-25/02/grams-1-1.parquet"]


def test_kw_files_skip_days_of_merged_months(m, tmp_path):
    for f in ("month=2026-11/terms-0000.parquet", "month=2026-11/pages-0000.parquet",
              "day=2026-11-30/hour=23/terms.parquet",           # 11 月はまとめ直してあるので読まない
              "day=2026-12-01/hour=00/terms.parquet", "day=2026-12-01/hour=00/pages.parquet",
              "work/2026-12-01/01/terms-1-1.parquet"):
        (tmp_path / f).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / f).write_bytes(b"")
    assert [str(Path(f).relative_to(tmp_path)).replace("\\", "/") for f in m.kw_files(str(tmp_path))] == [
        "month=2026-11/terms-0000.parquet", "day=2026-12-01/hour=00/terms.parquet", "work/2026-12-01/01/terms-1-1.parquet"]


# ---- 本物の Parquet と、偽の DB で通す ---------------------------------------------
def _ng_file(path, pages):
    """pages = {crawl_id: [(語, 欄の番号)]} → 1 時間・1 日のファイル (gram 順)"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from collections import Counter
    rows = []
    for cid, items in pages.items():
        best: dict[int, int] = {}
        cnt: Counter = Counter()
        for text, fi in items:
            for g in _grams_of(text):
                best[g] = min(best.get(g, 9), fi)
                cnt[g] += 1
        rows += [(cid, g, best[g], cnt[g]) for g in best]
    rows.sort(key=lambda r: (r[1], r[0]))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"crawl_id": pa.array([r[0] for r in rows], pa.int64()),
                             "gram": pa.array([r[1] for r in rows], pa.int64()),
                             "field": pa.array([r[2] for r in rows], pa.uint8()),
                             "tf": pa.array([r[3] for r in rows], pa.uint16())}), str(path))


_PTMOD = {}


def _grams_of(text):
    """ページの側の切り方 (imgpull_page_text.extract_ngrams)"""
    return [g for g, _f, _n in _PTMOD["pt"].extract_ngrams({"body": text})]


def _kw_file(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"crawl_id": pa.array([r[0] for r in rows], pa.int64()),
                             "term_id": pa.array([r[1] for r in rows], pa.uint32()),
                             "field": pa.array([r[2] for r in rows], pa.uint8()),
                             "weight": pa.array([1.0] * len(rows), pa.float32()),
                             "tf": pa.array([1] * len(rows), pa.uint16())}), str(path))


class FakeDb:
    def __init__(self):
        self.scan: dict[tuple[str, str], tuple] = {}
        self.pages: dict[tuple[str, int, str], tuple] = {}
        self.urls = {}
        self.kw_term = {}

    def q(self, db, sql, params=()):
        if sql.startswith("SELECT identity_key, alias, version"):
            return [(k, a, *v) for (k, a), v in self.scan.items()]
        if sql.startswith("SELECT id, url FROM crawl"):
            return [(c, self.urls.get(c, f"https://ex.test/p/{c}")) for c in params]
        if sql.startswith("SELECT term_id FROM kw_term"):
            t = self.kw_term.get(params[0])
            return [(t,)] if t else []
        raise AssertionError(sql)

    def exec(self, db, sql, params=()):
        if sql.lstrip().startswith("INSERT INTO identity_index_scan"):
            k, a, *v = params
            self.scan[(k, a)] = tuple(v)
            return 1
        if sql.startswith("DELETE FROM identity_index_scan"):
            return 1 if self.scan.pop(tuple(params), None) else 0
        raise AssertionError(sql)

    def exec_many(self, db, sql, rows):
        assert "INSERT INTO identity_text_page" in sql
        for k, cid, a, s, fld, longer, src in rows:
            old = self.pages.get((k, cid, a))
            if old is None or s > old[0]:
                self.pages[(k, cid, a)] = (s, fld, src)


@pytest.fixture
def env(m, pt, tmp_path, monkeypatch):
    _PTMOD["pt"] = pt
    fdb = FakeDb()
    monkeypatch.setattr(m, "_q", fdb.q)
    monkeypatch.setattr(m, "_exec", fdb.exec)
    monkeypatch.setattr(m, "_exec_many", fdb.exec_many)
    state = {"index_root": str(tmp_path), "index_ng_root": str(tmp_path / "ng-v2"), "index_budget_s": 60,
             "index_tail_s": 7200, "index_memory_limit": "256MB", "index_threads": 1}
    return fdb, state, tmp_path


@needs_duck
def test_scan_index_hits_writes_weak_ngram_pages_strong_keyword_pages_and_search_pages(m, env):
    fdb, state, root = env
    ng = root / "ng-v2"
    _ng_file(ng / "day=2026-11-23/merged/grams-0000.parquet", {
        1: [("出演 山田花子 さん", 0)],               # 当たり (title)
        2: [("山田 さんと 花子", 5)],                 # 欠けている窓 (田花) があるので当たらない
        3: [("山田花子みみ", 5)]})                    # 長い名前の一部も N グラムでは当たる (WEAK)
    _ng_file(ng / "day=2026-11-24/merged/grams-0000.parquet", {4: [("山田花子", 5)]})
    _ng_file(ng / "day=2026-11-25/hour=00/grams.parquet", {5: [("山田花子", 4)]})      # 育っている日 (alt)
    _ng_file(ng / "2026-11-25/01/grams-1-1.parquet", {6: [("やまだはなこ", 5)]})        # 別の書き方は当たらない
    _kw_file(root / "day=2026-11-22/hour=10/terms.parquet", [(9, 77, 0), (1, 77, 5), (8, 12, 0)])   # N グラムより前
    fdb.kw_term["山田花子"] = 77
    fdb.urls[4] = "https://ex.test/search?q=x"
    st = m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, time.time() + 60)
    got = {cid: v for (_k, cid, _a), v in fdb.pages.items()}
    assert got == {1: (m.STRONG, "body", "index"),      # N グラムは WEAK・title だが、語の索引で 1 語として出ていた → STRONG
                   3: (m.WEAK, "body", "index"),
                   4: (m.NONE, "body", "index"),        # 検索結果のページ
                   5: (m.WEAK, "alt", "index"),
                   9: (m.STRONG, "title", "index")}
    assert st["days"] == 2 and st["opens"] == 1 and st["left"] == 0
    assert fdb.scan[(K, "山田花子")][:4] == (m.INDEX_SCAN_VERSION, "2026-11-24", fdb.scan[(K, "山田花子")][2], 0)
    # すぐもう 1 度呼んでも引かない。index_tail_s たったら育っている分をもう 1 度引いて終わり
    assert m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, time.time() + 60)["pages"] == 0
    _ng_file(ng / "day=2026-11-25/hour=01/grams.parquet", {7: [("山田花子", 5)]})
    k = (K, "山田花子")
    fdb.scan[k] = (fdb.scan[k][0], fdb.scan[k][1], time.time() - 7201, 0, fdb.scan[k][4])
    st = m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, time.time() + 60)
    assert st["tails"] == 1 and 7 in {cid for (_k, cid, _a) in fdb.pages} and fdb.scan[k][3] == 1
    n = len(fdb.pages)
    assert m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, time.time() + 60)["pages"] == 0
    assert len(fdb.pages) == n
    # 語が消えたら進みも消す
    assert m.scan_index_hits(state, None, {}, time.time() + 60)["removed"] == 1 and fdb.scan == {}


@needs_duck
def test_scan_index_hits_resumes_from_the_last_day_after_running_out_of_time(m, env, monkeypatch):
    fdb, state, root = env
    for d, cid in (("2026-11-23", 1), ("2026-11-24", 2), ("2026-11-25", 3)):
        _ng_file(root / "ng-v2" / f"day={d}/merged/grams-0000.parquet", {cid: [("山田花子", 5)]})
    real = time.time
    t = {"n": 0}

    def clock():                                  # 呼ぶたびに 6 秒進む: 1 日目は引けて、2 日目の前で時間切れ
        t["n"] += 1
        return real() + 6 * t["n"]
    state["index_budget_s"] = 15
    monkeypatch.setattr(m.time, "time", clock)
    st = m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, real() + 600)
    monkeypatch.setattr(m.time, "time", real)
    assert (st["left"], st["days"], st["opens"]) == (1, 1, 0)
    assert fdb.scan[(K, "山田花子")][1] == "2026-11-23"
    st = m.scan_index_hits(state, None, {K: m.prepare_aliases(["山田花子"])}, real() + 60)
    assert (st["left"], st["days"], st["opens"]) == (0, 2, 1)
    assert {cid for (_k, cid, _a) in fdb.pages} == {1, 2, 3}


def test_run_pass_scans_the_index_only_in_index_mode_and_stays_busy_while_left(m, monkeypatch):
    monkeypatch.setattr(m, "sync_labels", lambda state, db: {})
    monkeypatch.setattr(m, "load_aliases", lambda state, db: {K: m.prepare_aliases(["ハナコ"])})
    monkeypatch.setattr(m, "_faiss", lambda state: ([], None))
    monkeypatch.setattr(m, "match_new_faces", lambda state, db, idents, deadline: {"new_faces": 0, "auto": 0, "review": 0})
    monkeypatch.setattr(m, "sync_persons", lambda state, db, idents: {})
    monkeypatch.setattr(m, "daily_eval", lambda state, db, idents: 0)
    monkeypatch.setattr(m, "scan_text_hits", lambda state, db, aliases, deadline: {"scanned_to": 0})
    monkeypatch.setattr(m, "scan_text_faces", lambda state, db, idents, deadline: {"sites_left": 0})
    monkeypatch.setattr(m, "consume_page_candidates", lambda state, db, aliases, idents, deadline: {"left": 0})
    monkeypatch.setattr(m, "refresh_alias_stats", lambda state, db, aliases: 0)
    left = {"n": 1}
    calls = []
    monkeypatch.setattr(m, "scan_index_hits", lambda state, db, aliases, deadline:
                        calls.append(1) or {"pages": 5, "weak": 5, "left": left["n"]})
    state = {"auto_threshold": 0.6, "review_floor": 0.6, "text_floor": 0.2, "newface_batch": 100}
    _out, busy = m.run_pass(state, None, deadline=time.time() + 5)
    assert calls == [] and not busy
    state["index_mode"] = True
    out, busy = m.run_pass(state, None, deadline=time.time() + 5)
    assert calls == [1] and busy and out["index_pages"] == 5 and "index_left" not in out
    left["n"] = 0
    _out, busy = m.run_pass(state, None, deadline=time.time() + 5)
    assert not busy


def test_index_mode_is_off_by_default(m, monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DB_PASS=x\nFAISS_EMBEDS_DIR=/tmp\n")
    monkeypatch.setattr(m, "_reconnect", lambda state: None)
    monkeypatch.setattr(m, "_q", lambda db, sql, params=(): [])
    monkeypatch.setattr(m, "_self_enqueue_next_tick", lambda *a: None)
    st = m.setup(db_env_file=str(env_file))
    assert st["index_mode"] is False and st["index_ng_root"].replace("\\", "/") == "/srv/page-keywords/ng-v2"
    m.teardown(st)
