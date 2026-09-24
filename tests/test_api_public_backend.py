"""CreateBackend の投入契約テスト (= 既存パイプラインの入口と噛み合っているか)。

MariaDB / MinIO は fake。 検証したいのは **SQL の中身と手順の順序**:
  - crawl_image は 'processing' で入り、 MinIO put 成功後に初めて downloaded_at が入る
    (= 途中で落ちた行が orphan_reconcile → image-hash-extract に流れない)
  - raw MinIO のキーが image_hash_extract._fetch_raw_minio と同じ算出になっている
  - 一意制約 (data_sha256 / url_sha256 / site_file_no) の 3 種を撃ち分けている
"""

from __future__ import annotations

import hashlib
import io
import os
import time
from typing import Any

import pytest

from pipeline.api_public.backend import (
    CreateBackend,
    CreateError,
    raw_object_key,
    site_for,
)

JPEG = b"\xff\xd8\xff\xe0payload"
MP4 = b"\x00\x00\x00\x18ftypmp42payload"


class DupEntry(Exception):
    """mariadb ドライバの ER_DUP_ENTRY 相当。"""

    def __init__(self) -> None:
        super().__init__("Duplicate entry")
        self.errno = 1062


class GoneAway(Exception):
    """wait_timeout で切られた接続を使ったときの 2006 相当。"""

    def __init__(self) -> None:
        super().__init__("MySQL server has gone away")
        self.errno = 2006


class FakeCursor:
    def __init__(self, rows: list, rowcount: int, lastrowid: int) -> None:
        self._rows = rows
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self.closed = False

    def fetchall(self) -> list:
        return self._rows

    def close(self) -> None:
        self.closed = True


class FakeDb:
    """SQL 断片で分岐する最小の MariaDB スタブ。 実行順を `log` に残す。"""

    def __init__(self) -> None:
        self.log: list[tuple[str, tuple]] = []
        self.next_no = 5
        self.config_rows: list = [(1,)]         # crawl_config が既にある想定
        self.existing_image: int | None = None
        self.existing_video: int | None = None
        self.insert_image_errors = 0            # 先頭 N 回の crawl_image INSERT を 1062 に
        self.insert_video_dup = False
        self.insert_video_error: Exception | None = None   # 重複以外の INSERT 失敗を注入
        self.pinged = 0
        self.deleted_images = 0
        self.connected = False                  # 実 Db と同じ「触ると張る / close で捨てる」
        self.closes = 0
        self.uploading_rows: list = []          # recover_uploads が見る 'uploading' 行
        self.recent_create_rows: list = []      # recover_uploads の spool 突合用 (id, key, mime, status)
        self.video_status_updates: list[tuple] = []   # (id, status, error)
        self.update_video_errors = 0            # 先頭 N 回の UPDATE crawl_video を失敗させる
        self.video_download_status: str | None = "pending"   # バックアップ後の再確認で読む値

    def ping(self) -> None:
        self.pinged += 1

    def close(self) -> None:
        self.connected = False
        self.closes += 1

    def execute(self, sql: str, params: tuple = ()) -> FakeCursor:
        self.connected = True
        self.log.append((" ".join(sql.split()), params))
        if "FROM crawl_config" in sql:
            return FakeCursor([(self.next_no,)] if "next_no" in sql else self.config_rows, 1, 0)
        if sql.startswith("INSERT INTO crawl_config"):
            self.config_rows = [(1,)]
            return FakeCursor([], 1, 1)
        if sql.startswith("UPDATE crawl_config"):
            if not self.config_rows:
                return FakeCursor([], 0, 0)
            self.next_no += 1
            return FakeCursor([], 1, 0)
        if "FROM crawl_image" in sql and "SELECT id" in sql:
            return FakeCursor([(self.existing_image,)] if self.existing_image else [], 1, 0)
        if sql.startswith("INSERT INTO crawl_image"):
            if self.insert_image_errors > 0:
                self.insert_image_errors -= 1
                raise DupEntry()
            return FakeCursor([], 1, 1001)
        if sql.startswith("UPDATE crawl_image"):
            return FakeCursor([], 1, 0)
        if sql.startswith("DELETE FROM crawl_image"):
            self.deleted_images += 1
            return FakeCursor([], 1, 0)
        if "SELECT download_status FROM crawl_video" in sql:
            st = self.video_download_status
            return FakeCursor([(st,)] if st is not None else [], 1, 0)
        if "SELECT id, storage_url, mime, download_status FROM crawl_video" in sql:
            return FakeCursor(list(self.recent_create_rows), 1, 0)
        if "SELECT id, storage_url, mime FROM crawl_video" in sql:
            return FakeCursor(list(self.uploading_rows), 1, 0)
        if sql.startswith("UPDATE crawl_video"):
            if self.update_video_errors > 0:
                self.update_video_errors -= 1
                raise GoneAway()
            status, error, vid, _ = params
            self.video_status_updates.append((vid, status, error))
            return FakeCursor([], 1, 0)
        if "FROM crawl_video" in sql:
            return FakeCursor([(self.existing_video,)] if self.existing_video else [], 1, 0)
        if sql.startswith("INSERT INTO crawl_video"):
            if self.insert_video_dup:
                raise DupEntry()
            if self.insert_video_error is not None:
                raise self.insert_video_error
            return FakeCursor([], 1, 7001)
        if "FROM crawl_face" in sql:
            return FakeCursor([], 1, 0)
        raise AssertionError(f"未知の SQL: {sql}")

    # ---- 検証用ヘルパー ----
    def sqls(self) -> list[str]:
        return [s for s, _ in self.log]

    def find(self, needle: str) -> tuple[str, tuple]:
        for entry in self.log:
            if needle in entry[0]:
                return entry
        raise AssertionError(f"{needle!r} を含む SQL が無い: {self.sqls()}")

    def index_of(self, needle: str) -> int:
        for i, (s, _) in enumerate(self.log):
            if needle in s:
                return i
        raise AssertionError(f"{needle!r} を含む SQL が無い")


class FakeStore:
    def __init__(self, *, fail: bool = False, observe: Any = None) -> None:
        self.puts: list[tuple[str, int, str]] = []
        self.removed: list[str] = []
        self.fail = fail
        self.calls: list[str] = []
        self.observe = observe      # put の最中の状態を覗くフック (接続保持の検査用)
        self.present: set[str] = set()   # exists() が True を返すキー
        self.endpoint = "fake:9000"

    def put_bytes(self, key: str, data: bytes, content_type: str = "") -> None:
        self.calls.append(f"put:{key}")
        if self.observe is not None:
            self.observe()
        if self.fail:
            raise RuntimeError("MinIO down")
        self.puts.append((key, len(data), content_type))

    def put_file(self, key: str, path: str, content_type: str = "") -> None:
        self.calls.append(f"put_file:{key}")
        if self.observe is not None:
            self.observe()
        if self.fail:
            raise RuntimeError("MinIO down")
        with open(path, "rb") as f:
            self.puts.append((key, len(f.read()), content_type))
        self.present.add(key)

    def exists(self, key: str) -> bool:
        return key in self.present

    def remove(self, key: str) -> None:
        self.removed.append(key)
        self.present.discard(key)


@pytest.fixture
def be() -> CreateBackend:
    return CreateBackend(db=FakeDb(), raw=FakeStore(), paprika=FakeStore())


# ---------------- site / key の算出 ----------------


def test_site_is_namespaced_per_tenant() -> None:
    assert site_for("acme") == "create-acme"
    assert site_for("a/b c") == "create-a-b-c"          # 危険文字は落とす
    assert len(site_for("x" * 200)) <= 64               # crawl_video.site は varchar(64)


def test_raw_key_matches_hash_extract_formula() -> None:
    # image_hash_extract._fetch_raw_minio: ((iid - 1) // dir_base + 1) * dir_base
    assert raw_object_key(1, 1000) == "1000/1.jpg"
    assert raw_object_key(1000, 1000) == "1000/1000.jpg"
    assert raw_object_key(1001, 1000) == "2000/1001.jpg"
    assert raw_object_key(125_264_775, 1000) == "125265000/125264775.jpg"


# ---------------- 画像 ----------------


def test_image_insert_then_put_then_mark(be: CreateBackend) -> None:
    r = be.create_image(data=JPEG, url=None, site="create-acme", ext="jpg")
    assert (r.id, r.dedup, r.state) == (1001, False, "queued")

    db, raw = be.db, be.raw
    # 1. INSERT は download_status='processing' で入る
    sql, params = db.find("INSERT INTO crawl_image")
    assert "'processing'" in sql
    assert "data_sha256" in sql
    assert params[1] == "create-acme" and params[2] == 5      # site, 採番された file_no
    assert params[4] == hashlib.sha256(JPEG).digest()

    # 2. raw MinIO へ hash-extract と同じキーで put
    assert raw.puts == [("2000/1001.jpg", len(JPEG), "image/jpeg")]

    # 3. put の後に初めて downloaded_at が入る (= 順序が逆だと未 upload の行が hash に流れる)
    assert db.index_of("SET download_status=NULL") > db.index_of("INSERT INTO crawl_image")
    upd_sql, upd_params = db.find("SET download_status=NULL")
    assert "downloaded_at=NOW()" in upd_sql and upd_params == (1001,)


def test_upload_gets_synthetic_url_from_content_hash(be: CreateBackend) -> None:
    be.create_image(data=JPEG, url=None, site="create-acme", ext="jpg")
    _, params = be.db.find("INSERT INTO crawl_image")
    # url_sha256 が UNIQUE なので URL 無しでは入れられない → 内容ハッシュから合成する
    assert params[0] == f"create://create-acme/{hashlib.sha256(JPEG).hexdigest()}"


def test_real_url_is_stored_verbatim(be: CreateBackend) -> None:
    be.create_image(data=JPEG, url="https://example.com/a.jpg", site="s", ext="jpg")
    _, params = be.db.find("INSERT INTO crawl_image")
    # 実 URL をそのまま入れる = 既存クローラーが取った同じ URL と自然に dedup される
    assert params[0] == "https://example.com/a.jpg"


def test_page_url_is_stored_when_given(be: CreateBackend) -> None:
    be.create_image(data=JPEG, url="https://example.com/a.jpg", site="s", ext="jpg",
                    page_url="https://example.com/gallery/1")
    sql, params = be.db.find("INSERT INTO crawl_image")
    assert "page_url" in sql
    assert params[-1] == "https://example.com/gallery/1"


def test_page_url_defaults_to_null(be: CreateBackend) -> None:
    be.create_image(data=JPEG, url=None, site="s", ext="jpg")
    _, params = be.db.find("INSERT INTO crawl_image")
    # 未指定なら空文字でなく NULL を入れる (crawl_video.page_url と同じ扱い)
    assert params[-1] is None


def test_dedup_short_circuits_before_insert(be: CreateBackend) -> None:
    be.db.existing_image = 42
    r = be.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert (r.id, r.dedup, r.state) == (42, True, "existing")
    assert not any("INSERT INTO crawl_image" in s for s in be.db.sqls())
    assert be.raw.puts == []            # 既存なら MinIO も触らない


def test_dedup_query_covers_both_unique_keys(be: CreateBackend) -> None:
    be.create_image(data=JPEG, url="https://example.com/a.jpg", site="s", ext="jpg")
    sql, _ = be.db.find("SELECT id FROM crawl_image")
    assert "data_sha256" in sql and "url_sha256" in sql


def test_file_no_collision_retries_with_new_number(be: CreateBackend) -> None:
    # site_file_no UNIQUE との衝突 (内容重複ではない) → file_no を採番し直して再挑戦
    be.db.insert_image_errors = 1
    r = be.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert r.id == 1001 and r.dedup is False
    inserts = [p for s, p in be.db.log if s.startswith("INSERT INTO crawl_image")]
    assert len(inserts) == 2
    assert inserts[0][2] != inserts[1][2]          # file_no が前進している


def test_content_dup_found_after_1062_is_reported_as_dedup(be: CreateBackend) -> None:
    be.db.insert_image_errors = 1

    original = be.db.execute

    def execute(sql: str, params: tuple = ()) -> FakeCursor:
        # 1062 後の再 SELECT で「実は内容重複だった」ことが判明するケース
        if "SELECT id FROM crawl_image" in sql and be.db.insert_image_errors == 0:
            be.db.existing_image = 99
        return original(sql, params)

    be.db.execute = execute  # type: ignore[method-assign]
    r = be.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert (r.id, r.dedup) == (99, True)


def test_persistent_collision_gives_up(be: CreateBackend) -> None:
    be.db.insert_image_errors = 99
    with pytest.raises(CreateError) as ei:
        be.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert ei.value.reason == "insert_conflict"


def test_crawl_config_created_when_missing(be: CreateBackend) -> None:
    be.db.config_rows = []
    be.create_image(data=JPEG, url=None, site="create-new", ext="jpg")
    sql, params = be.db.find("INSERT INTO crawl_config")
    assert params == ("create-new",)
    assert "'image', 0," in sql            # enabled=0 = レガシー crawl.py に巡回させない


def test_minio_failure_rolls_back_the_row() -> None:
    b = CreateBackend(db=FakeDb(), raw=FakeStore(fail=True), paprika=FakeStore())
    with pytest.raises(CreateError) as ei:
        b.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert ei.value.reason == "storage_failed"
    # downloaded_at は入らない → orphan_reconcile が拾わない
    assert not any("SET download_status=NULL" in s for s in b.db.sqls())
    # 実体を置けなかった行は消す。 残すと data_sha256 UNIQUE で再投入が永久に dedup ヒットし、
    # 「取り込めたことになっているが embedding は永遠に来ない」行を指し続ける。
    sql, params = b.db.find("DELETE FROM crawl_image")
    assert params == (1001,)
    assert "downloaded_at IS NULL" in sql          # 他が触った行は消さない
    assert b.db.deleted_images == 1
    # DELETE が通ったなら ignore_reason の保険は打たない (二重処理しない)
    assert not any("create_upload_failed" in s for s in b.db.sqls())


def test_rollback_failure_falls_back_to_marking_the_row() -> None:
    """巻き戻しの DELETE 自体が失敗したら、 少なくとも運用で拾える印を残す。"""
    db = FakeDb()
    original = db.execute

    def execute(sql: str, params: tuple = ()) -> FakeCursor:
        if sql.startswith("DELETE FROM crawl_image"):
            original(sql, params)               # log には残す
            raise RuntimeError("connection lost")
        return original(sql, params)

    db.execute = execute  # type: ignore[method-assign]
    b = CreateBackend(db=db, raw=FakeStore(fail=True), paprika=FakeStore())
    with pytest.raises(CreateError):
        b.create_image(data=JPEG, url=None, site="s", ext="jpg")
    sql, params = db.find("ignore_reason='create_upload_failed'")
    assert params == (1001,)


def test_minio_failure_then_retry_can_succeed() -> None:
    """巻き戻しが効いていれば、 MinIO 復旧後に同じ画像を投入し直せる。"""
    store = FakeStore(fail=True)
    b = CreateBackend(db=FakeDb(), raw=store, paprika=FakeStore())
    with pytest.raises(CreateError):
        b.create_image(data=JPEG, url=None, site="s", ext="jpg")
    # 行が消えている前提なので dedup ヒットしない (FakeDb.existing_image は None のまま)
    store.fail = False
    r = b.create_image(data=JPEG, url=None, site="s", ext="jpg")
    assert r.dedup is False and r.state == "queued"
    assert store.puts == [("2000/1001.jpg", len(JPEG), "image/jpeg")]


# ---------------- 動画 ----------------


def test_video_put_before_insert(be: CreateBackend) -> None:
    r = be.create_video(data=MP4, url=None, site="create-acme", ext="mp4",
                        mime="video/mp4", page_url="https://example.com/p")
    assert (r.id, r.dedup, r.state) == (7001, False, "queued")

    key = be.paprika.puts[0][0]
    assert key.startswith("create/") and key.endswith(".mp4")

    sql, params = be.db.find("INSERT INTO crawl_video")
    assert "'pending'" in sql                  # video-dispatcher が拾う状態
    assert params[0] == "create-acme"
    assert params[2] == key                    # storage_url = 置いたキー
    assert params[3] == "https://example.com/p"
    assert params[6] == len(MP4)


def test_video_dedup_skips_upload(be: CreateBackend) -> None:
    be.db.existing_video = 5
    r = be.create_video(data=MP4, url="https://example.com/v.mp4", site="s", ext="mp4")
    assert (r.id, r.dedup) == (5, True)
    assert be.paprika.puts == []


def test_video_insert_race_cleans_up_orphan_object(be: CreateBackend) -> None:
    be.db.insert_video_dup = True
    be.db.existing_video = None

    original = be.db.execute
    seen = {"n": 0}

    def execute(sql: str, params: tuple = ()) -> FakeCursor:
        # 2 回目の SELECT (INSERT が 1062 になった後) で既存行が見える
        if "FROM crawl_video" in sql:
            seen["n"] += 1
            if seen["n"] >= 2:
                be.db.existing_video = 88
        return original(sql, params)

    be.db.execute = execute  # type: ignore[method-assign]
    r = be.create_video(data=MP4, url=None, site="s", ext="mp4")
    assert (r.id, r.dedup) == (88, True)
    # 置いたが誰も参照しない実体は回収する
    assert be.paprika.removed == [be.paprika.puts[0][0]]


def test_video_without_paprika_store_is_unavailable() -> None:
    b = CreateBackend(db=FakeDb(), raw=FakeStore(), paprika=None)
    with pytest.raises(CreateError) as ei:
        b.create_video(data=MP4, url=None, site="s", ext="mp4")
    assert ei.value.reason == "video_unavailable"


# ---------------- 接続を put をまたいで持ち越さない ----------------
#
# 2026-09-16: 1.5GiB の動画投入が item error `internal` で落ちた。 実体は
# `2006 MySQL server has gone away` で、 原因は「dedup SELECT → 1382 秒かかる MinIO put
# → INSERT」の間、 単一コネクションを握ったままにしていたこと。 MariaDB の wait_timeout
# (600s) を超えたので向こうから切られていた。 MinIO は無関係で、 600 秒以上かかる処理なら
# 何を挟んでも同じ。 put の最中に接続を持っていないことを不変条件として固定する。


def test_video_does_not_hold_the_db_connection_across_the_upload() -> None:
    db = FakeDb()
    held: list[bool] = []
    paprika = FakeStore(observe=lambda: held.append(db.connected))
    be = CreateBackend(db=db, raw=FakeStore(), paprika=paprika)

    be.create_video(data=MP4, url=None, site="create-acme", ext="mp4")

    assert held == [False]          # put の最中は接続を持っていない
    assert db.closes >= 2           # dedup 区間と INSERT 区間でそれぞれ閉じている


def test_image_does_not_hold_the_db_connection_across_the_upload() -> None:
    db = FakeDb()
    held: list[bool] = []
    raw = FakeStore(observe=lambda: held.append(db.connected))
    be = CreateBackend(db=db, raw=raw, paprika=FakeStore())

    be.create_image(data=JPEG, url=None, site="create-acme", ext="jpg")

    assert held == [False]


def test_video_insert_failure_reclaims_the_object_and_raises(be: CreateBackend) -> None:
    """重複以外の INSERT 失敗でも実体を回収する (回収漏れが 17 個 29GB 滞留していた)。"""
    be.db.insert_video_error = GoneAway()

    with pytest.raises(GoneAway):
        be.create_video(data=MP4, url=None, site="s", ext="mp4")

    assert be.paprika.puts != []
    assert be.paprika.removed == [be.paprika.puts[0][0]]


# ---------------- 動画 (非同期: spool → 'uploading' → 裏で put → 'pending') ----------------
#
# 2026-09-24: 1.2〜2GB の動画で 504 が 1 日 8 件。 .8 の MinIO へ同期 put すると 20 分を
# 超え、 前段 nginx の proxy_read_timeout (600s) を越えていた。 行は後から入るので拡張には
# 「失敗したのに登録済み」に見え、 再送で 2GB を二重に送っていた。


def _abe(tmp_path, **kw) -> CreateBackend:
    kw.setdefault("paprika", FakeStore())
    return CreateBackend(db=kw.pop("db", FakeDb()), raw=FakeStore(),
                         spool_dir=tmp_path / "spool", spool_min_free_bytes=0, **kw)


def _spool(b: CreateBackend, data: bytes = MP4):
    return b.spool(io.BytesIO(data), max_bytes=1 << 30)


def _drain(b: CreateBackend) -> None:
    b._uploads.join()
    b._backups.join()


def _age(path, seconds: float) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


def test_spool_hashes_and_keeps_head(tmp_path) -> None:
    b = _abe(tmp_path)
    sp = _spool(b)
    assert sp.size == len(MP4) and not sp.truncated
    assert sp.sha256 == hashlib.sha256(MP4).digest()
    assert sp.head == MP4[:64]
    assert sp.path.read_bytes() == MP4


def test_spool_stops_at_max_bytes(tmp_path) -> None:
    b = _abe(tmp_path)
    sp = b.spool(io.BytesIO(MP4), max_bytes=4)
    assert sp.truncated


def test_spool_refuses_when_disk_is_low(tmp_path) -> None:
    b = _abe(tmp_path)
    b.spool_min_free_bytes = 1 << 62
    with pytest.raises(CreateError) as ei:
        _spool(b)
    assert ei.value.reason == "spool_full"


def test_spooled_video_is_registered_as_uploading_then_put_then_pending(tmp_path) -> None:
    b = _abe(tmp_path)
    sp = _spool(b)
    r = b.create_video_spooled(sp, url=None, site="create-acme", ext="mp4",
                               mime="video/mp4", page_url="https://example.com/p")
    assert (r.id, r.dedup, r.state) == (7001, False, "uploading")

    _, params = b.db.find("INSERT INTO crawl_video")
    assert params[-1] == "uploading"           # 後段 (dispatcher / vfe) が拾わない値
    key = params[2]
    assert key == f"create/{sp.token}.mp4"     # spool 名と uuid を揃える (再起動後の突合)

    _drain(b)
    assert b.paprika.puts == [(key, len(MP4), "video/mp4")]
    assert b.db.video_status_updates == [(7001, "pending", None)]
    assert not sp.path.exists()                # put し終えた spool は消す
    assert b._inflight == {}


def test_spooled_video_does_not_hold_the_db_connection_during_put(tmp_path) -> None:
    db = FakeDb()
    held: list[bool] = []
    b = _abe(tmp_path, db=db, paprika=FakeStore(observe=lambda: held.append(db.connected)))
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert held == [False]


def test_spooled_video_put_failure_marks_failed_and_cleans_up(tmp_path) -> None:
    b = _abe(tmp_path, paprika=FakeStore(fail=True))
    sp = _spool(b)
    b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    _drain(b)
    (vid, status, error), = b.db.video_status_updates
    assert (vid, status) == (7001, "failed") and "MinIO down" in error
    assert b.paprika.removed == [f"create/{sp.token}.mp4"]
    assert not sp.path.exists()


def test_spool_is_kept_when_pending_update_keeps_failing(tmp_path, monkeypatch) -> None:
    """put 済みでも 'pending' に倒せなければ spool は残す (次回起動の recover が拾う)。"""
    monkeypatch.setattr("pipeline.api_public.backend.time.sleep", lambda _s: None)
    b = _abe(tmp_path)
    b.db.update_video_errors = 99
    sp = _spool(b)
    b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika.puts != []
    assert sp.path.exists()


def test_spooled_video_dedup_skips_insert_and_discards_spool(tmp_path) -> None:
    b = _abe(tmp_path)
    b.db.existing_video = 5
    sp = _spool(b)
    r = b.create_video_spooled(sp, url="https://example.com/v.mp4", site="s", ext="mp4")
    assert (r.id, r.dedup, r.state) == (5, True, "existing")
    assert not any("INSERT INTO crawl_video" in q for q in b.db.sqls())
    assert not sp.path.exists()
    assert b._upload_threads == []             # 裏に何も回していない


def test_spooled_video_insert_race_is_dedup(tmp_path) -> None:
    b = _abe(tmp_path)
    b.db.insert_video_dup = True
    original = b.db.execute
    seen = {"n": 0}

    def execute(sql: str, params: tuple = ()) -> FakeCursor:
        if "FROM crawl_video" in sql:
            seen["n"] += 1
            if seen["n"] >= 2:
                b.db.existing_video = 88
        return original(sql, params)

    b.db.execute = execute  # type: ignore[method-assign]
    sp = _spool(b)
    r = b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    assert (r.id, r.dedup) == (88, True)
    assert not sp.path.exists()


def test_spooled_video_insert_failure_discards_spool_and_raises(tmp_path) -> None:
    b = _abe(tmp_path)
    b.db.insert_video_error = GoneAway()
    sp = _spool(b)
    with pytest.raises(GoneAway):
        b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    assert not sp.path.exists()
    assert b._inflight == {}


def test_spooled_video_without_paprika_is_unavailable(tmp_path) -> None:
    b = _abe(tmp_path, paprika=None)
    sp = _spool(b)
    with pytest.raises(CreateError) as ei:
        b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    assert ei.value.reason == "video_unavailable"
    assert not sp.path.exists()


def test_recover_uploads_resumes_promotes_or_fails(tmp_path) -> None:
    b = _abe(tmp_path)
    b.spool_dir.mkdir(parents=True)
    (b.spool_dir / "aaa.part").write_bytes(MP4)          # spool が残っている → put し直す
    b.paprika.present = {"create/bbb.mp4"}               # 実体だけある → put は済んでいた
    b.db.uploading_rows = [
        (1, "create/aaa.mp4", "video/mp4"),
        (2, "create/bbb.mp4", None),
        (3, "create/ccc.mp4", None),                     # どちらも無い → 取り戻せない
    ]
    old = b.spool_dir / "zzz.part"                       # どの行にも無い古い spool
    old.write_bytes(b"x")
    _age(old, 2 * 86400)
    fresh = b.spool_dir / "yyy.part"                     # どの行にも無いが新しい → 残す
    fresh.write_bytes(b"x")

    out = b.recover_uploads()
    _drain(b)

    assert out == {"resumed": 1, "pending": 1, "failed": 1, "backup_resumed": 0,
                   "orphan_spool": 1}
    assert b.paprika.puts == [("create/aaa.mp4", len(MP4), "video/mp4")]
    assert sorted((v, s) for v, s, _ in b.db.video_status_updates) == [
        (1, "pending"), (2, "pending"), (3, "failed")]
    assert not old.exists() and fresh.exists()
    assert b._inflight == {}


def test_recover_uploads_skips_rows_this_process_is_uploading(tmp_path) -> None:
    b = _abe(tmp_path)
    b._inflight[9] = "live.part"
    b.spool_dir.mkdir(parents=True)
    live = b.spool_dir / "live.part"
    live.write_bytes(MP4)
    _age(live, 2 * 86400)                               # 古くても処理中なら孤児扱いしない
    b.db.uploading_rows = [(9, "create/live.mp4", None)]

    out = b.recover_uploads()

    assert out["resumed"] == 0 and out["orphan_spool"] == 0
    assert live.exists()
    assert b.paprika.puts == []


# ---------------- 動画の置き場: .48 (RAM) が一次、 .8 (HDD) がバックアップ ----------------


G = 1024 ** 3


@pytest.fixture
def ram(monkeypatch: pytest.MonkeyPatch) -> dict:
    """.48 の容量 (total, free) を差し替える。 None にすると metrics 読み失敗。"""
    cap: dict = {"v": (100 * G, 90 * G)}

    def fake_capacity(endpoint: str, timeout: float = 4.0):
        if cap["v"] is None:
            raise RuntimeError("metrics down")
        return cap["v"]

    monkeypatch.setattr("pipeline.api_public.backend._minio_capacity", fake_capacity)
    return cap


def _be48(tmp_path, **kw) -> CreateBackend:
    kw.setdefault("paprika_video", FakeStore())
    return _abe(tmp_path, backup_settle_s=0, **kw)


def _key(b: CreateBackend) -> str:
    return b.db.find("INSERT INTO crawl_video")[1][2]


def test_video_goes_to_ram_first_then_pending_then_hdd_backup(tmp_path, ram) -> None:
    b = _be48(tmp_path)
    order: list[str] = []
    b.paprika_video.observe = lambda: order.append(".48 put")
    b.paprika.observe = lambda: order.append(f".8 put (status={b.db.video_status_updates})")
    sp = _spool(b)
    b.create_video_spooled(sp, url=None, site="s", ext="mp4", mime="video/mp4")
    _drain(b)

    key = _key(b)
    assert b.paprika_video.puts == [(key, len(MP4), "video/mp4")]
    assert b.paprika.puts == [(key, len(MP4), "video/mp4")]
    # .48 に置いた直後に pending (= vfe が拾える)。 .8 のバックアップはその後。
    assert order == [".48 put", ".8 put (status=[(7001, 'pending', None)])"]
    assert b.paprika.removed == []             # .48 に実体がある = 未処理 → バックアップは残す
    assert not sp.path.exists()


def test_ram_nearly_full_goes_straight_to_hdd(tmp_path, ram) -> None:
    ram["v"] = (100 * G, 21 * G)               # 使用 79% + 動画 → 上限 79% を超える
    b = _be48(tmp_path, video_ram_max_pct=79.0)
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika_video.puts == []
    assert [k for k, *_ in b.paprika.puts] == [_key(b)]
    assert b.db.video_status_updates == [(7001, "pending", None)]


def test_ram_capacity_unreadable_goes_to_hdd(tmp_path, ram) -> None:
    ram["v"] = None
    b = _be48(tmp_path)
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika_video.puts == []
    assert len(b.paprika.puts) == 1


def test_ram_put_failure_falls_back_to_hdd(tmp_path, ram) -> None:
    b = _be48(tmp_path, paprika_video=FakeStore(fail=True))
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika_video.removed == [_key(b)]    # 途中の残骸を回収
    assert len(b.paprika.puts) == 1
    assert b.db.video_status_updates == [(7001, "pending", None)]


def test_both_stores_failing_marks_failed(tmp_path, ram) -> None:
    b = _be48(tmp_path, paprika=FakeStore(fail=True), paprika_video=FakeStore(fail=True))
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert [(v, s) for v, s, _ in b.db.video_status_updates] == [(7001, "failed")]


def test_hdd_backup_failure_is_not_fatal(tmp_path, ram) -> None:
    b = _be48(tmp_path, paprika=FakeStore(fail=True))
    sp = _spool(b)
    b.create_video_spooled(sp, url=None, site="s", ext="mp4")
    _drain(b)
    assert b.db.video_status_updates == [(7001, "pending", None)]
    assert not sp.path.exists()


def test_backup_finished_after_vfe_processed_is_reclaimed(tmp_path, ram) -> None:
    """vfe が先に処理して両方から消した後にバックアップが書かれた → 誰も消さないので自分で消す。"""
    b = _be48(tmp_path)

    def vfe_processes_during_backup() -> None:
        b.paprika_video.present.clear()        # vfe の _delete_minio_source
        b.db.video_download_status = "completed"

    b.paprika.observe = vfe_processes_during_backup
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika.removed == [_key(b)]
    assert b.paprika.present == set()


def test_backup_is_kept_when_ram_copy_vanished_unprocessed(tmp_path, ram) -> None:
    """.48 が CT 再起動等で飛んだ (行は未処理) → バックアップが要る場面なので残す。"""
    b = _be48(tmp_path)
    b.paprika.observe = lambda: b.paprika_video.present.clear()
    b.db.video_download_status = "pending"
    b.create_video_spooled(_spool(b), url=None, site="s", ext="mp4")
    _drain(b)
    assert b.paprika.removed == []
    assert b.paprika.present == {_key(b)}


def test_recover_promotes_rows_whose_object_is_only_on_ram(tmp_path, ram) -> None:
    b = _be48(tmp_path)
    b.paprika_video.present = {"create/ram.mp4"}
    b.db.uploading_rows = [(4, "create/ram.mp4", None)]
    out = b.recover_uploads()
    assert out["pending"] == 1
    assert b.db.video_status_updates == [(4, "pending", None)]


def test_backup_is_skipped_when_vfe_already_processed(tmp_path, ram) -> None:
    """バックアップ待ちの間に vfe が処理を終えていたら .8 には書かない。"""
    b = _be48(tmp_path)
    sp = _spool(b)
    b.db.video_download_status = "completed"       # .48 には実体が無い (vfe が消した)
    b._backup_one(7001, "create/x.mp4", sp.path, "video/mp4")
    assert b.paprika.puts == []
    assert not sp.path.exists()


def test_ram_put_does_not_wait_for_hdd_backup(tmp_path, ram) -> None:
    """.48 に置けた次の動画は、 前の動画の .8 バックアップを待たずに put される。"""
    import threading as _th

    release = _th.Event()
    b = _be48(tmp_path, upload_workers=1)
    b.paprika.observe = lambda: release.wait(5)    # .8 バックアップを塞いでおく
    b.create_video_spooled(_spool(b, MP4 + b"1"), url=None, site="s", ext="mp4")
    b.db.existing_video = None
    b.create_video_spooled(_spool(b, MP4 + b"2"), url=None, site="s", ext="mp4")
    b._uploads.join()                              # 一次側だけ待つ
    assert len(b.paprika_video.puts) == 2           # 2 本とも .48 に置けている
    release.set()
    _drain(b)


def test_recover_requeues_spools_left_waiting_for_backup(tmp_path, ram) -> None:
    """バックアップ待ちのまま再起動した spool はバックアップのキューへ戻す。"""
    b = _be48(tmp_path)
    b.spool_dir.mkdir(parents=True)
    (b.spool_dir / "wait.part").write_bytes(MP4)        # 行は pending、 .48 に実体あり
    (b.spool_dir / "done.part").write_bytes(MP4)        # 行は completed (vfe 処理済み)
    b.paprika_video.present = {"create/wait.mp4"}
    b.db.recent_create_rows = [
        (11, "create/wait.mp4", "video/mp4", "pending"),
        (12, "create/done.mp4", None, "completed"),
    ]
    b.db.video_download_status = "completed"            # done の再確認で読まれる値

    out = b.recover_uploads()
    _drain(b)

    assert out["backup_resumed"] == 2
    assert [k for k, *_ in b.paprika.puts] == ["create/wait.mp4"]   # done は .8 に書かない
    assert list(b.spool_dir.iterdir()) == []
