"""image-pull が page.html を消す前に読む (identity personing 設計 §7.10)。

Paprika のページは crawl にタイトルも説明文も残っていなかった (2026-06 下旬〜)。page.html は image-pull が
取り込みのあと job の prefix ごと消すので、消す前に読んで要約する。ここで固定するのは:

  1. 解析 (imgpull_page_text.parse_page): タイトル・説明文・h1・JSON-LD の名前・本文・画像ごとの alt と本文の中か
  2. 照合の正規化が identity-expand の norm_text と同じであること (違うと絞り込みで取りこぼす)
  3. 子プロセスとの受け答えの形式
  4. tick 末の書き込み: mode=1 は数えるだけ、mode=2 はタイトルを埋め (旧クローラのタイトルは書き換えない)、
     登録語を含むかもしれないページだけ page_text_candidate に入れる。終わっていない解析は次の tick へ
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from concurrent.futures import Future
from pathlib import Path

import pytest

_DIR = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull"
_IM = _DIR / "image_main.py"
_IE = Path(__file__).resolve().parents[1] / "plugins" / "identity_expand" / "identity_main.py"
pytestmark = pytest.mark.skipif(not (_DIR / "imgpull_page_text.py").exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def pt():
    sys.path.insert(0, str(_DIR))
    import imgpull_page_text
    return imgpull_page_text


@pytest.fixture(scope="module")
def im():
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")
        img.open = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stub"))
        pil.Image = img
        sys.modules["PIL"] = pil
        sys.modules["PIL.Image"] = img
    spec = importlib.util.spec_from_file_location("image_main_page_text_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


HTML = """<!doctype html><html><head><title>ハナコ 写真集 | サイト</title>
<meta name="description" content="ハナコの新作">
<meta property="og:title" content="使わない (title がある)">
<script type="application/ld+json">{"@type": "Movie", "name": "作品A", "sku": "ABC-123",
  "actor": [{"@type": "Person", "name": "テスト花子"}], "description": "長い説明は入れない"}</script>
<style>.x { color: red }</style></head>
<body><nav>メニュー ログイン</nav>
<main><h1>ハナコ <small>2026</small></h1>
<figure><img src="/img/a.jpg" alt="ハナコの写真"><figcaption>撮影: 太郎</figcaption></figure>
<p>本文の 1 行目。<br>2 行目</p><script>var secret = "スクリプトは本文にしない";</script>
<img data-src="https://cdn.ex.test/b.jpg?w=300" alt="">
</main>
<aside><img srcset="//cdn.ex.test/c-small.jpg 1x, //cdn.ex.test/c.jpg 2x" alt="関連作品"></aside>
</body></html>"""


def test_parse_page_extracts_fields_images_and_hit(pt):
    norm = pt.normalize_image_url
    assets = [norm("https://ex.test/img/a.jpg"), norm("https://cdn.ex.test/b.jpg?w=300"),
              norm("https://cdn.ex.test/c.jpg"), norm("https://ex.test/css-bg.jpg")]
    r = pt.parse_page(HTML.encode(), "https://ex.test/p/1", assets, pt.norm_aliases(["ハナコ"]))
    assert r["title"] == "ハナコ 写真集 | サイト"
    assert r["description"] == "ハナコの新作"
    assert r["h1"] == "ハナコ 2026"
    assert r["ld"] == "作品A / ABC-123 / テスト花子"          # name・sku・actor の name だけ
    assert "本文の 1 行目。" in r["body"] and "2 行目" in r["body"] and "メニュー" in r["body"]
    assert "スクリプト" not in r["body"] and "color" not in r["body"]
    assert r["hit"] is True
    imgs = {i["u"]: i for i in r["imgs"]}
    assert imgs[assets[0]] == {"u": assets[0], "alt": "ハナコの写真 撮影: 太郎", "main": 1}   # figcaption も alt に
    assert imgs[assets[1]]["main"] == 1                     # data-src (遅延読み込み)
    assert imgs[assets[2]] == {"u": assets[2], "alt": "関連作品", "main": 0}                 # srcset・本文の外
    assert imgs[assets[3]] == {"u": assets[3], "alt": "", "main": None}                     # DOM に無い asset
    assert r["imgs_matched"] == 3


def test_parse_page_returns_body_only_when_a_term_may_be_there(pt):
    r = pt.parse_page(HTML.encode(), "https://ex.test/p/1", [], pt.norm_aliases(["別の人の名前"]))
    assert r["hit"] is False and r["body"] == "" and r["imgs"] == []
    assert r["title"] and r["body_len"] > 0                 # タイトルは hit でなくても返す (crawl.title を埋める)
    r = pt.parse_page(HTML.encode(), "https://ex.test/p/1", [], [])
    assert r["hit"] is False


def test_parse_page_without_closing_head_and_broken_markup(pt):
    """</head> を書かないページでも本文が取れる。壊れた HTML・JSON-LD でも落ちない。"""
    html = "<html><head><title>T</title><script type='application/ld+json'>{broken</script><body><p>本文 ハナコ<div>"
    r = pt.parse_page(html.encode(), "https://ex.test/", [], pt.norm_aliases(["ハナコ"]))
    assert r["title"] == "T" and "本文 ハナコ" in r["body"] and r["ld"] == "" and r["hit"]
    r = pt.parse_page(b"\xff\xfe\x00garbage<<<", "https://ex.test/", [], [])
    assert r["title"] == "" and r["hit"] is False


def test_norm_text_is_the_same_as_identity_expand(pt):
    """image-pull の絞り込みと identity-expand の照合で正規化が違うと、取りこぼす。"""
    spec = importlib.util.spec_from_file_location("ie_for_page_text_test", _IE)
    ie = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ie)
    for s in ["テスト 花子", "ＡＢＣ－１２３", "Hanako_Yamada", "メイー・ﾊﾅｺ", "a/b|c．d", "　全角空白\tタブ"]:
        assert pt.norm_text(s) == ie.norm_text(s), s
    assert pt.norm_aliases(["テスト 花子", "ab", "ABC-123", "abc123"]) == sorted({ie.norm_text("テスト 花子"), "abc123"})


def test_helper_message_roundtrip(pt):
    import pickle
    buf = io.BytesIO()
    pt._write_msg(buf, pickle.dumps(("x", 1)))
    buf.seek(0)
    assert pickle.loads(pt._read_msg(buf)) == ("x", 1)
    assert pt._read_msg(io.BytesIO(b"\x00\x00")) is None            # 途中で切れたら終わり


class _Cur:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0

    def execute(self, sql, params=()):
        self.db.log.append((" ".join(sql.split()), tuple(params)))

    def executemany(self, sql, rows):
        self.db.log.append((" ".join(sql.split()), list(rows)))

    def fetchone(self):
        return (self.db.cand_n,)

    def fetchall(self):
        return [(a,) for a in self.db.aliases]

    def close(self):
        pass


class _Db:
    def __init__(self, aliases=(), cand_n=0):
        self.log = []
        self.aliases = list(aliases)
        self.cand_n = cand_n

    def cursor(self):
        return _Cur(self)

    def commit(self):
        pass

    def rollback(self):
        pass


def _state(im, mode, **kw):
    st = {}
    im._page_text_init(st, {"page_text_mode": mode, "page_text_wait_s": 0.01, **kw}, _Db())
    return st


def _done(result):
    f = Future()
    f.set_result(result)
    return f


def _res(title="", desc="", hit=False, body="", imgs=()):
    return {"title": title, "description": desc, "h1": "", "ld": "", "body": body, "body_len": len(body),
            "imgs": list(imgs), "imgs_dom": 0, "imgs_matched": 0, "hit": hit, "cpu_ms": 3.0}


def test_collect_mode1_only_counts(im):
    st = _state(im, 1)
    st["pt_pending"] = [(_done(_res("T", "D", hit=True, body="本文")), 10, "s", "https://ex.test/1", "j1", [])]
    db, out = _Db(), {}
    im._page_text_collect(st, db, out)
    assert db.log == []
    assert out["page_text"]["parsed"] == 1 and out["page_text"]["hit"] == 1 and out["page_text"]["with_title"] == 1


def test_collect_mode2_fills_titles_and_writes_candidates_and_carries_pending(im):
    st = _state(im, 2)
    waiting = Future()                                      # まだ終わっていない解析
    imgs = [{"u": "https://cdn/a.jpg", "alt": "ハナコ", "main": 1}]
    st["pt_pending"] = [
        (_done(_res("ハナコ 写真集", "説明", hit=True, body="本文 ハナコ", imgs=imgs)), 10, "s", "https://ex.test/1", "j1", []),
        (_done(_res("別のページ", "")), 11, "s", "https://ex.test/2", "j2", []),
        (_done({"error": "TimeoutError"}), 12, "s", "https://ex.test/3", "j3", []),
        (waiting, 13, "s", "https://ex.test/4", "j4", []),
    ]
    db, out = _Db(), {}
    im._page_text_collect(st, db, out)
    upd = [x for x in db.log if x[0].startswith("UPDATE crawl SET title")]
    assert len(upd) == 1 and "WHERE id = %s AND (title IS NULL OR title = '')" in upd[0][0]   # 旧クローラは書き換えない
    assert upd[0][1] == [("ハナコ 写真集", "説明", 10), ("別のページ", None, 11)]
    ins = [x for x in db.log if x[0].startswith("INSERT INTO page_text_candidate")]
    assert len(ins) == 1 and len(ins[0][1]) == 1
    row = ins[0][1][0]
    assert row[:6] == (10, "s", "https://ex.test/1", "j1", "ハナコ 写真集", "説明") and row[8] == "本文 ハナコ"
    assert json.loads(row[9]) == imgs
    pt = out["page_text"]
    assert pt["parsed"] == 2 and pt["parse_err"] == 1 and pt["cand_written"] == 1 and pt["pending"] == 1
    assert [p[1] for p in st["pt_pending"]] == [13]          # 終わっていないものは持ち越す


def test_collect_stops_writing_candidates_when_the_table_is_full(im):
    """identity-expand が止まっていても page_text_candidate が際限なく膨らまない。"""
    st = _state(im, 2, page_text_candidate_max=5)
    st["pt_pending"] = [(_done(_res("T", hit=True, body="b")), 10, "s", "u", "j", [])]
    db, out = _Db(cand_n=5), {}
    im._page_text_collect(st, db, out)
    assert not [x for x in db.log if x[0].startswith("INSERT INTO page_text_candidate")]
    assert out["page_text"]["cand_capped"] == 1


class _Resp:
    def __init__(self, b):
        self.b = b

    def read(self, n):
        return self.b[:n]

    def close(self):
        pass

    def release_conn(self):
        pass


class _Store:
    def __init__(self, objs):
        self.objs = objs
        self.keys = []

    def get_object(self, bucket, key):
        self.keys.append((bucket, key))
        if key not in self.objs:
            raise KeyError("NoSuchKey")
        return _Resp(self.objs[key])


def test_submit_reads_page_html_of_the_job_before_the_prefix_is_deleted(im):
    st = _state(im, 1, page_text_max_bytes=10)
    store = _Store({"jobs/j1/page.html": b"<title>ab</title>0123456789"})
    st.update({"hub_ram_client": store, "hub_bucket": "paprika"})
    sent = []
    st["pt_pool"] = types.SimpleNamespace(submit=lambda req: sent.append(req) or _done(_res()), restarts=0)
    db = _Db(aliases=["ハナコ"])
    im._page_text_submit(st, db, "j1", 10, "s", "https://ex.test/1", ["https://cdn/a.jpg"])
    im._page_text_submit(st, db, "j2", 11, "s", "https://ex.test/2", [])        # page.html が無い
    im._page_text_submit(st, db, "j3", 0, "s", "https://ex.test/3", [])         # crawl の行が無い
    assert store.keys == [("paprika", "jobs/j1/page.html"), ("paprika", "jobs/j2/page.html")]
    assert len(sent) == 1
    html, base, assets, aliases, want_kw, want_ng = sent[0]
    assert want_kw is False and want_ng is False  # page_kw_mode=0・page_ng_mode=0 なら語も N グラムも頼まない
    assert html == b"<title>ab</title>0123456789"[:10] and base == "https://ex.test/1" and assets == ["https://cdn/a.jpg"]
    assert aliases == im._ptext.norm_aliases(["ハナコ"])
    s = st["pt_stats"]
    assert s["got"] == 1 and s["miss"] == 1 and s["no_page_id"] == 1 and s["truncated"] == 1
    assert [p[1] for p in st["pt_pending"]] == [10]


def test_page_text_is_off_by_default(im):
    st = {}
    im._page_text_init(st, {}, _Db())
    assert st["page_text_mode"] == 0


def test_pool_parses_in_process_when_inline(im, pt):
    pool = im._PageTextPool(1, 5.0)
    pool.inline = True
    r = pool.submit((HTML.encode(), "https://ex.test/", [], pt.norm_aliases(["ハナコ"]))).result(5)
    pool.close()
    assert r["hit"] and r["title"].startswith("ハナコ")


@pytest.mark.skipif(sys.platform == "win32", reason="子プロセスとの受け答えは select を使うので POSIX のみ")
def test_pool_runs_helper_process_and_recovers_from_timeout(im, pt, monkeypatch):
    pool = im._PageTextPool(1, 10.0)
    r = pool.submit((HTML.encode(), "https://ex.test/", [], pt.norm_aliases(["ハナコ"]))).result(30)
    assert r["hit"] and r["title"].startswith("ハナコ")
    pool.timeout_s = 0.0                                      # 応答を待たずに作り直す
    r = pool.submit((b"<title>x</title>", "https://ex.test/", [], [])).result(30)
    assert r == {"error": "TimeoutError"} and pool.restarts == 1
    pool.timeout_s = 10.0
    r = pool.submit((b"<title>x</title>", "https://ex.test/", [], [])).result(30)
    assert r["title"] == "x"
    pool.close()
