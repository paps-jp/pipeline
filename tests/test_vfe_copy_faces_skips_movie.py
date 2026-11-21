"""video-face-extract の重複動画への顔複写が、旧クローラ動画 (crawl_movie) の顔を拾わないことの回帰テスト。

守りたいこと:
  - 同じ中身の動画 (crawl_video_hash が一致) へ、元の動画の顔を複写する
  - movie_id が入っている顔 (旧クローラ動画 crawl_movie の顔) は、video_id が一致していても複写しない。
    crawl_movie と crawl_video は id がどちらも 1 から振られて重なるので、拾うと
    番号がたまたま同じ別の旧動画の顔が、無関係の新しい動画に付く (2026-07-09 に 21 件)
  - 複写した件数を返す

plugin は非公開リポジトリの入れ子 clone なので、未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "video_face_extract" / "extract_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="video_face_extract plugin が未配置")


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("vfe_copy_faces_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Cur:
    """mariadb の %s を sqlite の ? に読み替えるだけの薄い cursor。"""

    def __init__(self, conn):
        self._c = conn.cursor()

    def execute(self, sql, params=()):
        self._c.execute(sql.replace("%s", "?"), params)

    @property
    def rowcount(self):
        return self._c.rowcount

    def close(self):
        self._c.close()


class _Db:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return _Cur(self.conn)


_COLS = ("video_id, movie_id, file_name, minio_key, face_index, det_score, bbox_x1, bbox_y1, bbox_x2, bbox_y2, "
         "crop_x1, crop_y1, yaw_deg, pitch_deg, roll_deg, is_profile, kps_json, adaface_norm, adaface_ready")


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE crawl_face (id INTEGER PRIMARY KEY AUTOINCREMENT, {_COLS.replace(',', ' ,')} )")
    yield _Db(conn), conn
    conn.close()


def _face(conn, *, video_id, movie_id, key):
    conn.execute(
        f"INSERT INTO crawl_face ({_COLS}) VALUES (?, ?, 'f.jpg', ?, 0, 0.9, 0, 0, 10, 10, 0, 0, 0, 0, 0, 0, '[]', 20.0, 1)",
        (video_id, movie_id, key),
    )


def test_copies_crawl_video_faces_but_not_crawl_movie_faces(m, db):
    wrapped, conn = db
    # crawl_video 42 の本来の顔 2 件
    _face(conn, video_id=42, movie_id=None, key="crawl_face/video/42/p0_0_aa.jpg")
    _face(conn, video_id=42, movie_id=None, key="")  # アップロード失敗で key が空でも複写する
    # 番号が重なった旧動画 (crawl_movie 42) の顔。video_id にも 42 が残っている古い形
    _face(conn, video_id=42, movie_id=42, key="twivideo/100/37-1.png")
    # 旧動画の顔で video_id を空にした今の形
    _face(conn, video_id=None, movie_id=42, key="twivideo/100/37-2.png")

    copied = m._copy_faces_for_video(wrapped, src_video_id=42, dst_video_id=9000)

    assert copied == 2
    rows = conn.execute("SELECT minio_key, movie_id FROM crawl_face WHERE video_id=9000 ORDER BY id").fetchall()
    assert rows == [("crawl_face/video/42/p0_0_aa.jpg", None), ("", None)]


def test_nothing_to_copy_returns_zero(m, db):
    wrapped, conn = db
    _face(conn, video_id=7, movie_id=7, key="twihub/100/7-1.png")
    assert m._copy_faces_for_video(wrapped, src_video_id=7, dst_video_id=9001) == 0
    assert conn.execute("SELECT COUNT(*) FROM crawl_face WHERE video_id=9001").fetchone()[0] == 0
