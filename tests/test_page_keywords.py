"""ページのキーワード索引 (identity personing 設計 §7.11): 語の取り出し・ID・tick の Parquet・1 時間ごとのまとめ。

固定するのは:
  1. 語の取り出し (imgpull_page_text.extract_keywords): 名詞と品番を残し、数詞・名詞以外は捨てる。続いた固有名詞・
     辞書にない語は**つないだ語句**も持つ (「佐藤」「花子」→「佐藤花子」。無いと登録語「花子」が単独の語として当たる)。
     語は norm_text の形、欄は一番重いもの、回数を数える
  2. 重み: 欄の重み × (1 + log 回数) を 0-255 に
  3. ID: 手元 → kw_term の SELECT → 無い語だけ INSERT IGNORE (既にある語で AUTO_INCREMENT を進めない)。手元は上限まで
  4. tick の Parquet: 語の ID 順、画像は crawl_image の id、.tmp から置き換え、置き場所がいっぱいなら書かない
  5. 1 時間ごとのまとめ: 終わった時間だけ、本文のサイト共通語を落とし、語の ID 順、元を消す
Sudachi・pyarrow・DuckDB が無い手元では、その部分のテストは飛ばす (使い捨ての容器で通す)。
"""

from __future__ import annotations

import importlib.util
import sys
import types
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1] / "plugins"
_IP = _ROOT / "paprika_image_pull"
_PKC = _ROOT / "page_keyword_compact" / "compact_main.py"
pytestmark = pytest.mark.skipif(not (_IP / "imgpull_page_text.py").exists(), reason="plugins リポジトリ未チェックアウト")


def _has(mod: str) -> bool:
    if mod in sys.modules:                     # ほかのテストが入れた差し替え (__spec__ が無い) も「ある」
        return True
    try:
        return importlib.util.find_spec(mod) is not None
    except (ImportError, ValueError):
        return False


@pytest.fixture(scope="module")
def pt():
    sys.path.insert(0, str(_IP))
    import imgpull_page_text
    return imgpull_page_text


@pytest.fixture(scope="module")
def im():
    if not _has("PIL"):
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")
        pil.Image = img
        sys.modules.setdefault("PIL", pil)
        sys.modules.setdefault("PIL.Image", img)
    spec = importlib.util.spec_from_file_location("image_main_page_kw_test", _IP / "image_main.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def pkc():
    spec = importlib.util.spec_from_file_location("page_keyword_compact_test", _PKC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- 1. 語の取り出し (偽の分かち書きで規則を固定する) -------------------------
_POS = {
    "P": (("名詞", "固有名詞", "人名", "一般", "*", "*"), False),
    "N": (("名詞", "普通名詞", "一般", "*", "*", "*"), False),
    "O": (("名詞", "普通名詞", "一般", "*", "*", "*"), True),     # 辞書にない語
    "U": (("名詞", "数詞", "*", "*", "*", "*"), False),
    "V": (("動詞", "一般", "*", "*", "*", "*"), False),
    "J": (("助詞", "格助詞", "*", "*", "*", "*"), False),
}


class _M:
    def __init__(self, surface, code):
        self._s, (self._pos, self._oov) = surface, _POS[code]

    def surface(self):
        return self._s

    def part_of_speech(self):
        return self._pos

    def is_oov(self):
        return self._oov


class _FakeTok:
    """「語|品詞 語|品詞 …」を形態素に分ける。品詞は _POS の 1 文字。"""

    def tokenize(self, text, mode):
        out = []
        for w in text.split():
            s, _, c = w.partition("|")
            out.append(_M(s, c or "N"))
        return out


@pytest.fixture
def fake_tok(pt, monkeypatch):
    monkeypatch.setattr(pt, "_TOK", (_FakeTok(), None))
    return pt


def test_extract_keywords_keeps_nouns_and_joins_runs_of_proper_nouns(fake_tok):
    pt = fake_tok
    kw = pt.extract_keywords({
        "title": "佐藤|P 花子|P 写真集|N",
        "body": "佐藤|P 花子|P の|J 出演|N する|V 3|U 本|N ABC-123|N テスト|O 新人|O",
    })
    got = {t: (f, n) for t, f, n in kw}
    fi = {n: i for i, n in enumerate(pt.KW_FIELDS)}
    # 続いた固有名詞をつないだ語句 (タイトルと本文の 2 回)。欄は重いほう (タイトル)
    assert got["佐藤花子"] == (fi["title"], 2)
    assert got["佐藤"] == (fi["title"], 2) and got["花子"] == (fi["title"], 2)
    assert got["写真集"] == (fi["title"], 1) and got["出演"] == (fi["body"], 1)
    # 辞書にない語が続けば語句になる。norm_text の形 (カタカナ → ひらがな)
    assert "てすと" in got and "新人" in got and "てすと新人" in got
    # 品番は正規表現でも拾う (norm_text で区切りが落ちる)
    assert got["abc123"][1] >= 1
    # 数詞・助詞・動詞・1 文字は残さない
    assert "3" not in got and "の" not in got and "する" not in got and "本" not in got


def test_extract_keywords_joins_pairs_even_in_long_runs(fake_tok):
    """名前のあとに固有名詞が続いても (5 語以上)、となり合う 2 語の語句は残る。全体をつなぐのは 3〜4 語まで。"""
    pt = fake_tok
    kw = {t for t, _f, _n in pt.extract_keywords({"title": "あい|P かき|P さし|P たち|P なに|P"})}
    assert {"あいかき", "かきさし", "さしたち", "たちなに"} <= kw
    assert not any(len(k) >= 6 for k in kw)          # 3 語以上の全体は持たない (5 語の続き)
    kw = {t for t, _f, _n in pt.extract_keywords({"title": "あい|P かき|P さし|P"})}
    assert "あいかきさし" in kw and "あいかき" in kw and "かきさし" in kw


def test_extract_keywords_without_sudachi_returns_none(pt, monkeypatch):
    monkeypatch.setattr(pt, "_TOK", False)
    assert pt.extract_keywords({"title": "x"}) is None


def test_parse_page_returns_keywords_only_when_asked(fake_tok):
    pt = fake_tok
    html = b"<title>\xe4\xbd\x90\xe8\x97\xa4|P \xe8\x8a\xb1\xe5\xad\x90|P</title><p>x</p>"
    assert pt.parse_page(html, "https://ex.test/", [], [])["kw"] is None
    kw = pt.parse_page(html, "https://ex.test/", [], [], True)["kw"]
    assert {"佐藤", "花子", "佐藤花子"} <= {t for t, _f, _n in kw}


@pytest.mark.skipif(not (_has("sudachipy") and _has("sudachidict_core")), reason="Sudachi が無い")
def test_extract_keywords_with_real_sudachi(pt, monkeypatch):
    monkeypatch.setattr(pt, "_TOK", None)
    kw = {t: f for t, f, _n in pt.extract_keywords({"title": "東京タワーの写真集 ABC-123", "body": "新作を発売"})}
    assert "東京たわー" in kw or "東京" in kw          # 辞書次第で 1 語か 2 語
    assert "写真集" in kw and "abc123" in kw and "の" not in kw


# ---- 2. 重み --------------------------------------------------------------------
def test_kw_weight(im):
    assert im.kw_weight(0, 1) == 48                 # タイトル 3 × 1 × 16
    assert im.kw_weight(5, 1) == 16                 # 本文 1
    assert im.kw_weight(5, 3) == round((1 + __import__("math").log(3)) * 16)
    assert im.kw_weight(0, 10 ** 9) == 255 and im.kw_weight(99, 1) == 16


# ---- 3. 語の ID -----------------------------------------------------------------
class _KwDb:
    """kw_term と crawl_image の偽物。SELECT は IN の語・sha を引き、INSERT IGNORE は無い語だけ足す。"""

    def __init__(self, terms=None, images=None):
        self.terms = dict(terms or {})
        self.images = dict(images or {})
        self.next_id = max(self.terms.values(), default=0) + 1
        self.log = []

    def cursor(self):
        return _KwCur(self)

    def commit(self):
        pass

    def rollback(self):
        pass


class _KwCur:
    def __init__(self, db):
        self.db, self.rows = db, []

    def execute(self, sql, params=()):
        self.db.log.append(("execute", sql.split()[0], len(params)))
        if sql.startswith("SELECT term, term_id FROM kw_term"):
            self.rows = [(t, self.db.terms[t]) for t in params if t in self.db.terms]
        elif sql.startswith("SELECT url_sha256, id FROM crawl_image"):
            self.rows = [(h, self.db.images[h]) for h in params if h in self.db.images]
        else:
            self.rows = []

    def executemany(self, sql, rows):
        self.db.log.append(("executemany", sql.split()[0], len(rows)))
        if sql.startswith("INSERT IGNORE INTO kw_term"):
            for t, _day in rows:
                if t not in self.db.terms:
                    self.db.terms[t] = self.db.next_id
                    self.db.next_id += 1

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        pass


def _kw_state(im, tmp_path, **kw):
    st = {}
    im._page_kw_init(st, {"page_kw_mode": 0, "page_kw_root": str(tmp_path), **kw}, _KwDb())
    st["page_kw_mode"] = 1
    return st


def test_kw_term_ids_uses_cache_then_select_then_inserts_only_missing(im, tmp_path):
    from collections import Counter
    st = _kw_state(im, tmp_path, page_kw_cache_max=1000)
    db = _KwDb(terms={"はなこ": 7})
    s = Counter()
    ids = im._kw_term_ids(st, db, {"はなこ", "写真集"}, s)
    assert ids == {"はなこ": 7, "写真集": 8} and s["kw_new_terms"] == 1
    ins = [x for x in db.log if x[1] == "INSERT"]
    assert ins == [("executemany", "INSERT", 1)]          # 既にある語は INSERT しない
    db.log.clear()
    ids = im._kw_term_ids(st, db, {"はなこ", "写真集"}, s)
    assert ids == {"はなこ": 7, "写真集": 8} and db.log == [] and s["kw_cache_hit"] == 2   # 2 回目は DB に行かない


def test_kw_cache_is_bounded(im, tmp_path):
    from collections import Counter
    st = _kw_state(im, tmp_path, page_kw_cache_max=1000)
    db = _KwDb()
    im._kw_term_ids(st, db, {f"語{i:04d}" for i in range(1500)}, Counter())
    assert len(st["kw_cache"]) == 1000


# ---- 4. tick の Parquet -----------------------------------------------------------
@pytest.mark.skipif(not _has("pyarrow"), reason="pyarrow が無い")
def test_page_kw_write_writes_sorted_terms_and_page_images(im, tmp_path):
    import hashlib
    from collections import Counter
    import pyarrow.parquet as pq
    st = _kw_state(im, tmp_path)
    a, b = "https://cdn.test/a.jpg", "https://cdn.test/b.jpg"
    db = _KwDb(terms={"写真集": 5}, images={hashlib.sha256(a.encode()).digest(): 900})
    s = Counter()
    pages = [(11, "s1", [("写真集", 0, 1), ("はなこ", 5, 3)], [a, b]),     # b はまだ crawl_image に無い
             (10, "s2", [("写真集", 5, 1)], [])]
    im._page_kw_write(st, db, pages, s)
    files = sorted(p.name for p in (tmp_path / "work").rglob("*.parquet"))
    assert len(files) == 2 and files[0].startswith("pages-") and files[1].startswith("terms-")
    assert not list((tmp_path / "work").rglob("*.tmp"))
    t = pq.read_table(next((tmp_path / "work").rglob("terms-*.parquet"))).to_pylist()
    tid = {v: k for k, v in db.terms.items()}
    assert [(tid[r["term_id"]], r["crawl_id"]) for r in t] == [("写真集", 10), ("写真集", 11), ("はなこ", 11)]
    assert t[1]["field"] == 0 and t[1]["weight"] == 48 and t[2]["tf"] == 3
    p = {r["crawl_id"]: r for r in pq.read_table(next((tmp_path / "work").rglob("pages-*.parquet"))).to_pylist()}
    assert p[11]["image_ids"] == [900] and p[11]["n_terms"] == 2 and p[10]["site"] == "s2"
    assert s["kw_pages"] == 2 and s["kw_rows"] == 3 and s["kw_new_terms"] == 1
    hour_dir = next((tmp_path / "work").iterdir())
    datetime.strptime(hour_dir.name, "%Y-%m-%d")      # 作業ディレクトリは UTC の日/時


def test_page_kw_write_skips_when_the_disk_is_full(im, tmp_path, monkeypatch):
    from collections import Counter
    st = _kw_state(im, tmp_path, page_kw_disk_max_pct=50)
    monkeypatch.setattr(im, "_kw_disk_full", lambda state: True)
    s = Counter()
    if not _has("pyarrow"):
        pytest.skip("pyarrow が無い")
    im._page_kw_write(st, _KwDb(), [(1, "s", [("x", 0, 1)], [])], s)
    assert s["kw_disk_full"] == 1 and not (tmp_path / "work").exists()


def test_collect_passes_keywords_to_the_writer(im, tmp_path, monkeypatch):
    st = {}
    im._page_text_init(st, {"page_text_mode": 1, "page_text_wait_s": 0.01}, _KwDb())
    st.update({"page_kw_mode": 1, "page_kw_root": str(tmp_path)})
    got = []
    monkeypatch.setattr(im, "_page_kw_write", lambda state, db, pages, s: got.extend(pages))

    def done(r):
        f = Future()
        f.set_result(r)
        return f
    base = {"title": "T", "description": "", "h1": "", "ld": "", "body": "", "body_len": 0, "imgs": [],
            "imgs_dom": 0, "imgs_matched": 0, "hit": False, "cpu_ms": 1.0}
    st["pt_pending"] = [(done({**base, "kw": [("はなこ", 0, 1)]}), 10, "s", "u", "j1", ["https://cdn/a.jpg"]),
                        (done({**base, "kw": None}), 11, "s", "u", "j2", [])]
    out = {}
    im._page_text_collect(st, _KwDb(), out)
    assert got == [(10, "s", [("はなこ", 0, 1)], ["https://cdn/a.jpg"])]
    assert out["page_text"]["kw_none"] == 1


class _Store:
    """hub の MinIO の偽物 (page.html だけ)。"""

    def __init__(self, b):
        self.b = b

    def get_object(self, bucket, key):
        b = self.b
        return types.SimpleNamespace(read=lambda n: b[:n], close=lambda: None, release_conn=lambda: None)


def test_submit_asks_the_child_for_keywords_only_in_kw_mode(im):
    for mode in (0, 1):
        st = {}
        im._page_text_init(st, {"page_text_mode": 1}, _KwDb())
        st.update({"page_kw_mode": mode, "hub_ram_client": _Store(b"<title>t</title>"), "hub_bucket": "paprika",
                   "pt_alias_at": 1e18})                        # 登録語は読みに行かない
        sent = []
        st["pt_pool"] = types.SimpleNamespace(submit=lambda req: sent.append(req) or Future(), restarts=0)
        im._page_text_submit(st, _KwDb(), "j", 10, "s", "https://ex.test/", ["https://cdn/a.jpg"])
        assert sent[0][4] is bool(mode)
        assert st["pt_pending"][0][5] == ["https://cdn/a.jpg"]       # 画像 URL を持ち越す (pages の image_ids)


def test_page_kw_mode_is_off_by_default(im, tmp_path):
    st = {}
    im._page_kw_init(st, {}, _KwDb())
    assert st["page_kw_mode"] == 0


# ---- 5. 1 時間ごとのまとめ --------------------------------------------------------
def test_closed_hours_waits_for_the_hour_and_grace(pkc, tmp_path):
    for d, h in (("2026-11-23", "03"), ("2026-11-23", "04"), ("2026-11-23", "05")):
        (tmp_path / "work" / d / h).mkdir(parents=True)
    (tmp_path / "work" / "junk").mkdir()
    now = datetime(2026, 11, 23, 5, 3, tzinfo=timezone.utc)
    got = [(d, h) for d, h, _p in pkc.closed_hours(tmp_path, now, grace_s=300)]
    assert got == [("2026-11-23", "03")]                  # 04 時は 05:05 から、05 時はまだ書いている途中
    now = datetime(2026, 11, 23, 5, 6, tzinfo=timezone.utc)
    assert [(d, h) for d, h, _p in pkc.closed_hours(tmp_path, now, grace_s=300)][-1] == ("2026-11-23", "04")


@pytest.mark.skipif(not (_has("duckdb") and _has("pyarrow")), reason="DuckDB / pyarrow が無い")
def test_compact_hour_drops_site_wide_body_terms_and_sorts(pkc, tmp_path):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    wd = tmp_path / "work" / "2026-11-23" / "03"
    wd.mkdir(parents=True)
    # サイト s1 は 8 ページ。語 100 (メニュー) は 8 ページすべての本文に、語 200 は 2 ページの本文に、
    # 語 100 はページ 1 のタイトルにも出る。s2 は 2 ページだけなので共通語を落とさない
    t_rows, p_rows = [], []
    for i in range(1, 9):
        t_rows.append((i, 100, 5, 16, 1))
        p_rows.append((i, "s1", 1, [i * 10]))
    t_rows += [(1, 200, 5, 16, 1), (2, 200, 5, 16, 1), (1, 100, 0, 48, 1), (20, 100, 5, 16, 1), (21, 100, 5, 16, 1)]
    p_rows += [(20, "s2", 1, []), (21, "s2", 1, [])]

    def write(name, rows, cols, types_):
        pq.write_table(pa.table({c: pa.array([r[k] for r in rows], t) for k, (c, t) in enumerate(zip(cols, types_))}),
                       str(wd / name))
    half = len(t_rows) // 2
    tcols = ("crawl_id", "term_id", "field", "weight", "tf")
    ttypes = (pa.int64(), pa.uint32(), pa.uint8(), pa.uint8(), pa.uint16())
    write("terms-1-1.parquet", t_rows[:half], tcols, ttypes)          # tick 2 回分
    write("terms-2-1.parquet", t_rows[half:], tcols, ttypes)
    write("pages-1-1.parquet", p_rows, ("crawl_id", "site", "n_terms", "image_ids"),
          (pa.int64(), pa.string(), pa.uint16(), pa.list_(pa.int64())))
    (wd / ".terms-3-1.parquet.tmp").write_bytes(b"half written")       # 書きかけは読まない
    con = duckdb.connect()
    st = pkc.compact_hour(con, tmp_path, "2026-11-23", "03", wd, min_site_pages=8, common_ratio=0.5)
    out = tmp_path / "day=2026-11-23" / "hour=03"
    rows = pq.read_table(str(out / "terms.parquet")).to_pylist()
    got = [(r["term_id"], r["crawl_id"], r["field"]) for r in rows]
    # s1 の本文の語 100 は落ち、タイトルの 100 と、半分未満の 200 と、s2 の 100 は残る。語の ID 順
    assert got == [(100, 1, 0), (100, 20, 5), (100, 21, 5), (200, 1, 5), (200, 2, 5)]
    assert st["rows_in"] == 13 and st["rows_out"] == 5 and st["pages"] == 10
    assert [r["crawl_id"] for r in pq.read_table(str(out / "pages.parquet")).to_pylist()] == list(range(1, 9)) + [20, 21]
    assert not wd.exists() and not (tmp_path / "work" / "2026-11-23").exists()


def test_process_loop_does_not_spin_when_hours_are_left(pkc, monkeypatch):
    calls = []
    monkeypatch.setattr(pkc, "run_pass", lambda state, deadline: calls.append(deadline) or
                        {"hours": 0, "hours_left": 3})
    monkeypatch.setattr(pkc, "_self_enqueue_next_tick", lambda *a: None)
    clock = [0.0]
    monkeypatch.setattr(pkc.time, "monotonic", lambda: clock.__setitem__(0, clock[0] + 100) or clock[0])
    monkeypatch.setattr(pkc.time, "sleep", lambda s: None)
    st = {"counter": 0, "loop_s": 600, "interval_s": 300, "control_url": "x", "workload_slug": "w"}
    out = pkc.process(None, None, st)
    assert len(calls) < 10 and out["hours_left"] == 3
