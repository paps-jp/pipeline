"""crawl-host-import: 判定 → crawl_config → crawl の状態遷移を固定する。

このプラグインが埋めているのは「登録したサイトに最初の 1 本を撒く」段で、
そこが抜けていると **enabled=1 なのに永久に何も起きない** サイトができる
(2026-06-30 に crawl.py が retire してから実際に発生していた)。 よって
「200 で登録できたら crawl にも 1 行入る」ことをテストの中心に置く。

DB は fake cursor で駆動する。 見たいのは MariaDB の挙動ではなく、
どの判定でどの文が走るかの状態機械。
"""

import datetime

import pytest

from plugins.crawl_host_import import host_import_main as hi


# ---------------- fakes ---------------- #

class _FakeCursor:
    def __init__(self, db):
        self.db = db
        self._rows = []
        self.rowcount = 0
        self.lastrowid = 0

    def execute(self, sql, params=()):
        self.db.log.append((" ".join(sql.split()), tuple(params)))
        self._rows = []
        self.rowcount = 0
        self.lastrowid = 0
        for pattern, handler in self.db.routes:
            if pattern in sql:
                handler(self, params)
                return

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _FakeDb:
    def __init__(self):
        self.log = []
        self.routes = []
        self.autocommit = True

    def cursor(self):
        return _FakeCursor(self)

    def find(self, needle):
        for sql, params in self.log:
            if needle in sql:
                return sql, params
        return None, None

    def count(self, needle):
        return sum(1 for sql, _ in self.log if needle in sql)


def _db_for_classify(existing_config=False):
    """classify 段が触る SELECT に応答する fake。"""
    db = _FakeDb()

    def _queue_rows(cur, params):
        cur._rows = [(101, 19405, "example.com", "https://example.com")]

    def _config_by_site(cur, params):
        cur._rows = [(7,)] if existing_config else []

    def _config_by_url(cur, params):
        cur._rows = []

    def _insert_config(cur, params):
        cur.rowcount = 1
        cur.lastrowid = 42

    def _insert_crawl(cur, params):
        cur.rowcount = 1

    db.routes = [
        ("FROM host_import_queue", _queue_rows),
        ("FROM crawl_config WHERE site", _config_by_site),
        ("FROM crawl_config WHERE url", _config_by_url),
        ("INSERT INTO crawl_config", _insert_config),
        ("INSERT IGNORE INTO crawl ", _insert_crawl),
    ]
    return db


class _FakeResponse:
    def __init__(self, status_code, text="", url="https://example.com/"):
        self.status_code = status_code
        self.text = text
        self.url = url


class _FakeSession:
    """get() を渡された順に返す。 年齢確認の 2 段取得もこれで表現する。"""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.urls = []

    def get(self, url, timeout=None, allow_redirects=True):
        self.urls.append(url)
        r = self._responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _state(db, **over):
    st = {
        "db": db,
        "db_cfg": {},
        "session": _FakeSession(),
        "classify_limit": 10,
        "classify_concurrency": 1,
        "register_enabled": 1,
        "seed_backfill_limit": 0,
        "retry_months": 6,
        "request_timeout_s": 5.0,
        "do_seed": True,
        "counter": 1,
        "shard_siblings": {},
        "exclude_roots": frozenset(),
    }
    st.update(over)
    return st


# ---------------- 設定の読み取り ---------------- #

def test_db_cfg_prefix_maps_to_init_kwargs():
    """DB_ / DELETE_DB_ の 2 系統が、それぞれ正しい init_kwargs キーに落ちること。

    接頭辞の組み立てを 1 文字間違えると (db__host のように) 全部 None になり、
    「認証情報が未設定」で起動しなくなる。
    """
    kwargs = {
        "db_host": "10.10.50.20", "db_port": 3306, "db_user": "u", "db_pass": "p",
        "db_name": "delian",
        "delete_db_host": "10.10.60.2", "delete_db_port": 13306,
        "delete_db_user": "du", "delete_db_pass": "dp", "delete_db_name": "delian_sakura",
    }
    assert hi._db_cfg({}, kwargs, "DB_", 3306) == {
        "host": "10.10.50.20", "port": 3306, "user": "u",
        "password": "p", "database": "delian",
    }
    assert hi._db_cfg({}, kwargs, "DELETE_DB_", 13306) == {
        "host": "10.10.60.2", "port": 13306, "user": "du",
        "password": "dp", "database": "delian_sakura",
    }


def test_env_file_beats_init_kwargs():
    env = {"DELETE_DB_HOST": "10.10.60.2", "DELETE_DB_NAME": "delian_sakura",
           "DELETE_DB_PORT": "13306", "DELETE_DB_USER": "e", "DELETE_DB_PASS": "e"}
    cfg = hi._db_cfg(env, {"delete_db_host": "ignored"}, "DELETE_DB_", 13306)
    assert cfg["host"] == "10.10.60.2"


# ---------------- URL 正規化 ---------------- #

@pytest.mark.parametrize("raw,expect", [
    ("https://www.Example.com/", "example.com"),
    ("EXAMPLE.com", "example.com"),
    ("http://sub.example.co.jp/path", "sub.example.co.jp"),
    ("", ""),
])
def test_normalize_domain(raw, expect):
    assert hi._normalize_domain(raw) == expect


def test_sanitize_strips_session_and_tracking_query():
    """セッション ID を残すと同じページが毎回別 URL になり crawl の UNIQUE が効かない。"""
    out = hi._sanitize_candidate_url(
        "https://example.com/top?PHPSESSID=abc&utm_source=x&page=2&fbclid=z")
    assert out == "https://example.com/top?page=2"


def test_age_gate_follows_yes_link():
    html = '<p>あなたは18歳以上ですか</p><a href="/enter">はい</a><a href="/out">いいえ</a>'
    assert hi._age_gate_next_url("https://example.com/", html) == "https://example.com/enter"


def test_age_gate_ignores_page_without_markers():
    assert hi._age_gate_next_url("https://example.com/", '<a href="/x">はい</a>') == ""


# ---------------- HTTP 判定 ---------------- #

def test_200_is_registerable():
    s = _FakeSession(_FakeResponse(200, "<title>t</title>", "https://example.com/top"))
    v = hi._classify_by_http(s, "https://example.com", 5.0)
    assert v["category"] == "NOT_FILE_HOSTING"
    assert v["access_status"] == "SUCCESS"
    assert v["checked_url"] == "https://example.com/top"


def test_403_is_registerable_because_paprika_uses_a_real_browser():
    s = _FakeSession(_FakeResponse(403, "", "https://example.com/blocked"))
    v = hi._classify_by_http(s, "https://example.com", 5.0)
    assert v["category"] == "BROWSER_REQUIRED"
    assert v["access_status"] == "HTTP_403"


@pytest.mark.parametrize("code,expect", [
    (404, "HTTP_404"), (429, "HTTP_429"), (503, "HTTP_5XX"), (302, "UNKNOWN_ERROR"),
])
def test_status_code_mapping(code, expect):
    s = _FakeSession(_FakeResponse(code))
    assert hi._classify_by_http(s, "https://example.com", 5.0)["access_status"] == expect


def test_connection_reset_is_blocked_not_retryable():
    """「相手に切られた」は再試行しても無駄なので SKIPPED 側に倒す。"""
    import requests
    s = _FakeSession(requests.exceptions.ConnectionError("Connection reset by peer"))
    v = hi._classify_by_http(s, "https://example.com", 5.0)
    assert v["category"] == "BLOCKED_SITE"
    assert v["access_status"] == "CONNECTION_RESET"


def test_dns_error_is_retryable():
    import requests
    s = _FakeSession(requests.exceptions.ConnectionError("Name or service not known"))
    assert hi._classify_by_http(s, "https://example.com", 5.0)["access_status"] == "DNS_ERROR"


def test_age_gate_uses_second_response_for_verdict():
    html = '<p>18歳以上</p><a href="/in">はい</a>'
    s = _FakeSession(
        _FakeResponse(200, html, "https://example.com/"),
        _FakeResponse(200, "<title>inside</title>", "https://example.com/in"),
    )
    v = hi._classify_by_http(s, "https://example.com", 5.0)
    assert v["checked_url"] == "https://example.com/in"
    assert v["reason"] == "age_gate_followed"
    assert s.urls == ["https://example.com", "https://example.com/in"]


# ---------------- 開始 URL の決定 ---------------- #

def test_403_keeps_the_original_top_url():
    """403 の final_url はブロックページであってトップではない。"""
    v = hi._verdict("BROWSER_REQUIRED", "HTTP_403", "", "https://example.com/blocked", 403)
    assert hi._crawl_start_url("example.com", "https://example.com/", v) \
        == "https://example.com/"


def test_200_prefers_the_redirected_url():
    v = hi._verdict("NOT_FILE_HOSTING", "SUCCESS", "", "https://example.com/ja/top", 200)
    assert hi._crawl_start_url("example.com", "https://example.com/", v) \
        == "https://example.com/ja/top"


# ---------------- classify 段の状態遷移 ---------------- #

def test_success_registers_config_and_seeds_crawl():
    """本題: 登録できたら crawl にも 1 行入ること。 ここが抜けると何も起きない。"""
    db = _db_for_classify()
    st = _state(db, session=_FakeSession(
        _FakeResponse(200, "<title>t</title>", "https://example.com/top")))
    out = {}
    hi._stage_classify(st, out, deadline=None)

    sql, params = db.find("INSERT INTO crawl_config")
    assert params == ("example_com", "https://example.com/top", "example.com", 1)

    sql, params = db.find("INSERT IGNORE INTO crawl ")
    assert params == ("example_com", "https://example.com/top")

    assert out["classify_registered"] == 1
    assert out["classify_seeded"] == 1
    assert db.find("UPDATE host_import_queue")[1][0] == "IMPORTED"


def test_existing_config_is_not_reinserted():
    db = _db_for_classify(existing_config=True)
    st = _state(db, session=_FakeSession(_FakeResponse(200, "", "https://example.com/")))
    out = {}
    hi._stage_classify(st, out, deadline=None)
    assert db.count("INSERT INTO crawl_config") == 0
    assert out["classify_registered"] == 0
    # 既存でも queue は IMPORTED にして滞留から外す
    assert db.find("UPDATE host_import_queue")[1][0] == "IMPORTED"


def test_register_enabled_zero_skips_seeding():
    """enabled=0 で登録するなら種を撒かない (撒いても job-submit が拾わないため)。"""
    db = _db_for_classify()
    st = _state(db, register_enabled=0,
                session=_FakeSession(_FakeResponse(200, "", "https://example.com/")))
    out = {}
    hi._stage_classify(st, out, deadline=None)
    assert db.find("INSERT INTO crawl_config")[1][3] == 0
    assert db.count("INSERT IGNORE INTO crawl ") == 0
    assert out["classify_seeded"] == 0


def test_404_is_skipped_without_touching_crawl_config():
    db = _db_for_classify()
    st = _state(db, session=_FakeSession(_FakeResponse(404)))
    out = {}
    hi._stage_classify(st, out, deadline=None)
    assert db.count("INSERT INTO crawl_config") == 0
    assert out["classify_skipped"] == 1
    assert db.find("UPDATE host_import_queue")[1][0] == "SKIPPED"


def test_5xx_goes_to_retry_wait_with_a_future_deadline():
    db = _db_for_classify()
    st = _state(db, session=_FakeSession(_FakeResponse(503)))
    out = {}
    hi._stage_classify(st, out, deadline=None)
    assert out["classify_retry"] == 1
    _, params = db.find("UPDATE host_import_queue")
    assert params[0] == "RETRY_WAIT"
    assert isinstance(params[4], datetime.datetime)
    assert params[4] > datetime.datetime.now() + datetime.timedelta(days=150)


def test_retry_wait_rows_are_picked_up_again():
    """旧 classify は RETRY_WAIT の条件がコメントアウトされていて永久に再評価されなかった。"""
    db = _FakeDb()
    db.routes = [("FROM host_import_queue", lambda cur, p: None)]
    hi._pick_queue_rows(db, 10)
    sql, _ = db.find("FROM host_import_queue")
    assert "status = 'NEW'" in sql
    assert "RETRY_WAIT" in sql and "next_retry_at <= NOW()" in sql


# ---------------- CDN / 画像ホスティングの除外 ---------------- #
#
# 規則は queue 全 17,538 件に当てて調整したもの。 数字はその実測値。

_NO_SIBLINGS: dict = {}
_MANY_SIBLINGS = {"pixhost.to": 155, "static-file.com": 18, "fc2.com": 60}


@pytest.mark.parametrize("domain,reason", [
    ("img.example.com", "ASSET_SUBDOMAIN"),
    ("cdn.example.com", "ASSET_SUBDOMAIN"),
    ("static.example.com", "ASSET_SUBDOMAIN"),
    ("assets9.cdnhop.com", "ASSET_SUBDOMAIN"),      # 数字付きラベル
    ("photos.dtiblog.com", "ASSET_SUBDOMAIN"),      # ブログ運営元の画像サーバー
    ("media.tumblr.com", "ASSET_SUBDOMAIN"),
    ("storage1000.contents.fc2.com", "ASSET_SUBDOMAIN"),
])
def test_asset_subdomains_are_excluded(domain, reason):
    assert hi._asset_host_reason(domain, _NO_SIBLINGS, frozenset()) == reason


def test_numbered_shard_needs_siblings():
    """兄弟が少ないうちは普通のサイトかもしれないので落とさない。"""
    assert hi._asset_host_reason("t58.pixhost.to", _MANY_SIBLINGS, frozenset()) == "CDN_SHARD"
    assert hi._asset_host_reason("a18.chip.jp", {"chip.jp": 1}, frozenset()) is None


def test_user_blogs_are_not_mistaken_for_shards():
    """`^[a-z]{1,6}\\d{1,4}$` だとユーザーブログを 413 件巻き込んだ。

    stem 2 文字まで + ラベル 3 個ちょうど、で 0 件になる。
    """
    for d in ("blog123.fc2.com", "av121.blog102.fc2.com", "love2010.blog.fc2.com"):
        assert hi._asset_host_reason(d, _MANY_SIBLINGS, frozenset()) is None


def test_bare_registrable_domain_is_never_an_asset_host():
    for d in ("example.com", "pixhost.to"):
        assert hi._asset_host_reason(d, _MANY_SIBLINGS, frozenset()) is None


def test_exclude_roots_catch_the_image_hosting_site_itself():
    """root は HTML を返すので規則では落ちない。 設定で明示的に外す。"""
    roots = frozenset({"pixhost.to"})
    assert hi._asset_host_reason("pixhost.to", _NO_SIBLINGS, roots) == "EXCLUDED_DOMAIN"
    # 配下も同じ理由で落ちる
    assert hi._asset_host_reason("gallery.pixhost.to", _NO_SIBLINGS, roots) == "EXCLUDED_DOMAIN"
    assert hi._asset_host_reason("example.com", _NO_SIBLINGS, roots) is None


def test_excluded_rows_skip_the_http_fetch_entirely():
    """除外は HTTP を叩く前に効くこと。 叩いてから捨てるのは無駄な負荷。"""
    db = _FakeDb()
    db.routes = [
        ("FROM host_import_queue", lambda cur, p: setattr(
            cur, "_rows", [(101, 1, "img.example.com", "https://img.example.com")])),
    ]
    session = _FakeSession()      # get() を呼ぶと IndexError = 呼ばれたら失敗
    st = _state(db, session=session)
    out = {}
    hi._stage_classify(st, out, deadline=None)

    assert session.urls == []
    assert out["classify_excluded"] == 1
    assert out["classify_registered"] == 0
    assert db.count("INSERT INTO crawl_config") == 0
    _, params = db.find("UPDATE host_import_queue")
    assert params[0] == "SKIPPED"
    assert params[1] == "ASSET_SUBDOMAIN"


def test_shard_sibling_map_counts_only_three_label_shards():
    db = _FakeDb()
    rows = [("t58.pixhost.to",), ("t59.pixhost.to",), ("pixhost.to",),
            ("av121.blog102.fc2.com",), ("img.example.com",)]
    db.routes = [("FROM host_import_queue", lambda cur, p: setattr(cur, "_rows", rows))]
    assert hi._build_shard_siblings(db) == {"pixhost.to": 2}


# ---------------- 重複 tick ---------------- #

def test_duplicate_tick_folds_without_scheduling_a_successor():
    """self_loop が増殖したときに 1 本へ収束すること。

    worker を kill するとリース回収で task が queue に戻り、再起動時の bootstrap と
    足し算になって loop が増える。2026-08-16 に 4 本まで増え、4 スレッドが単一の
    mariadb 接続を共有した。畳むときは **次 tick を積まない** ことが収束の条件。
    """
    hi._TICK_LOCK.acquire()
    try:
        out = hi.process(None, None, {"hostname": "nas-c2", "counter": 0})
    finally:
        hi._TICK_LOCK.release()
    assert out["skipped"] == "duplicate_tick"
    assert "next_tick_scheduled" not in out


def test_lock_is_released_even_if_the_tick_raises():
    """例外で lock を持ったままだと、以降の tick が全部 duplicate 扱いで永久停止する。"""
    def _boom(task, ctx, state):
        raise RuntimeError("boom")

    orig = hi._process_locked
    hi._process_locked = _boom
    try:
        with pytest.raises(RuntimeError):
            hi.process(None, None, {"hostname": "h"})
    finally:
        hi._process_locked = orig
    assert hi._TICK_LOCK.acquire(blocking=False)
    hi._TICK_LOCK.release()


# ---------------- backfill ---------------- #

def test_backfill_seeds_enabled_sites_that_have_no_crawl_rows():
    db = _FakeDb()

    def _targets(cur, params):
        cur._rows = [("gosexpod_com", "https://gosexpod.com/")]

    def _insert_crawl(cur, params):
        cur.rowcount = 1

    db.routes = [
        ("FROM crawl_config cc", _targets),
        ("INSERT IGNORE INTO crawl ", _insert_crawl),
    ]
    out = {}
    hi._stage_seed_backfill(_state(db, seed_backfill_limit=50), out)
    assert out["backfill_seeded"] == 1
    assert db.find("INSERT IGNORE INTO crawl ")[1] == ("gosexpod_com", "https://gosexpod.com")


def test_backfill_disabled_by_zero_limit():
    db = _FakeDb()
    out = {}
    hi._stage_seed_backfill(_state(db, seed_backfill_limit=0), out)
    assert out == {}
    assert db.log == []
