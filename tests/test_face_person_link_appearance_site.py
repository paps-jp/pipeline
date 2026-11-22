"""face-person-link が人物の出現サイト (crawl_person_appearance) を正しい動画から引くことの回帰テスト。

守りたいこと:
  - 旧クローラ動画の顔 (movie_id だけを持つ) は crawl_movie のサイトで数え、動画として数える
  - 番号が同じ crawl_video があっても、そちらのサイトは付けない
    (crawl_movie と crawl_video は番号が重なる。 video_id で旧動画を引いていた頃、
    人物に紐付いた旧動画の顔 117,654 件がすべて無関係の動画のサイトで記録されていた。 2026-11-22)
  - 画像の顔と新しい動画の顔は今まで通り
  - どこにも結び付かない顔は何も書かない

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "face_person_link" / "link_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="face_person_link plugin が未配置")


@pytest.fixture(scope="module")
def m():
    pytest.importorskip("httpx")
    pytest.importorskip("numpy")
    spec = importlib.util.spec_from_file_location("face_person_link_appearance_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Cur:
    """SELECT は sqlite で答え、 appearance への UPSERT は引数を記録するだけの cursor。"""

    def __init__(self, conn, upserts):
        self._c = conn.cursor()
        self._upserts = upserts

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("INSERT INTO crawl_person_appearance"):
            self._upserts.append(tuple(params))
            return
        self._c.execute(sql.replace("%s", "?"), params)

    def fetchone(self):
        return self._c.fetchone()

    def close(self):
        self._c.close()


class _Db:
    def __init__(self, conn):
        self.conn = conn
        self.upserts: list[tuple] = []

    def ping(self, reconnect=True):
        return None

    def cursor(self):
        return _Cur(self.conn, self.upserts)


@pytest.fixture()
def db():
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE crawl_image (id INTEGER PRIMARY KEY, site TEXT);
        CREATE TABLE crawl_video (id INTEGER PRIMARY KEY, site TEXT);
        CREATE TABLE crawl_movie (id INTEGER PRIMARY KEY, site TEXT);
        CREATE TABLE crawl_face (id INTEGER PRIMARY KEY, image_id INTEGER,
                                 video_id INTEGER, movie_id INTEGER);
        INSERT INTO crawl_image VALUES (5, 'img.example');
        INSERT INTO crawl_video VALUES (777, 'new-video.example');
        INSERT INTO crawl_movie VALUES (777, 'old-movie.example');
        -- 1: 画像 / 2: 新しい動画 / 3: 旧動画 (番号 777 は crawl_video と重なる) / 4: どこにも無い
        INSERT INTO crawl_face VALUES (1, 5, NULL, NULL);
        INSERT INTO crawl_face VALUES (2, NULL, 777, NULL);
        INSERT INTO crawl_face VALUES (3, NULL, NULL, 777);
        INSERT INTO crawl_face VALUES (4, NULL, NULL, NULL);
        """
    )
    return _Db(c)


def test_old_movie_face_uses_crawl_movie_site(m, db):
    m._update_person_appearance({"db": db}, 42, 3)
    assert db.upserts == [(42, "old-movie.example", 0, 1)]


def test_image_and_new_video_faces_unchanged(m, db):
    m._update_person_appearance({"db": db}, 42, 1)
    m._update_person_appearance({"db": db}, 43, 2)
    assert db.upserts == [(42, "img.example", 1, 0), (43, "new-video.example", 0, 1)]


def test_unlinked_face_writes_nothing(m, db):
    m._update_person_appearance({"db": db}, 42, 4)
    assert db.upserts == []
