"""CreateBackend の投入契約テスト (= 既存パイプラインの入口と噛み合っているか)。

MariaDB / MinIO は fake。 検証したいのは **SQL の中身と手順の順序**:
  - crawl_image は 'processing' で入り、 MinIO put 成功後に初めて downloaded_at が入る
    (= 途中で落ちた行が orphan_reconcile → image-hash-extract に流れない)
  - raw MinIO のキーが image_hash_extract._fetch_raw_minio と同じ算出になっている
  - 一意制約 (data_sha256 / url_sha256 / site_file_no) の 3 種を撃ち分けている
"""

from __future__ import annotations

import hashlib

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
        self.pinged = 0
        self.deleted_images = 0

    def ping(self) -> None:
        self.pinged += 1

    def execute(self, sql: str, params: tuple = ()) -> FakeCursor:
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
        if "FROM crawl_video" in sql:
            return FakeCursor([(self.existing_video,)] if self.existing_video else [], 1, 0)
        if sql.startswith("INSERT INTO crawl_video"):
            if self.insert_video_dup:
                raise DupEntry()
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
    def __init__(self, *, fail: bool = False) -> None:
        self.puts: list[tuple[str, int, str]] = []
        self.removed: list[str] = []
        self.fail = fail
        self.calls: list[str] = []

    def put_bytes(self, key: str, data: bytes, content_type: str = "") -> None:
        self.calls.append(f"put:{key}")
        if self.fail:
            raise RuntimeError("MinIO down")
        self.puts.append((key, len(data), content_type))

    def remove(self, key: str) -> None:
        self.removed.append(key)


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
