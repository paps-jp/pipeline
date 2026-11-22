"""image-embed が旧クローラ動画の顔を動画の顔として振り分けることの回帰テスト。

守りたいこと:
  - movie_id だけを持つ顔 (旧クローラ動画 crawl_movie) は動画側に入る
    (見ていなかった頃はどちらにも入らず、 品質落ちと同じ adaface_ready=2 にされた。
    2026-06-30〜07-02 に 33,994 件。 norm は通過した顔と同じ分布だった)
  - 画像の顔・新しい動画の顔は今まで通り。 どこにも結び付かない顔はどちらにも入れない
  - 動画側の crop 取得は movie_id だけの顔も対象にする (where_col が video_id だけだと
    振り分けても SELECT で落ちて同じく =2 になる)
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "plugins" / "image_embed" / "embed_main.py"

if not _SRC.exists():
    pytest.skip(f"本番 plugin が無い環境 ({_SRC.name} は非公開リポジトリ側)",
                allow_module_level=True)


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("_embed_split_under_test", _SRC)
    m = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(m)
    return m


class _Cur:
    def __init__(self, conn):
        self._c = conn.cursor()

    def execute(self, sql, params=()):
        self._c.execute(sql.replace("%s", "?"), params)

    def fetchall(self):
        return self._c.fetchall()

    def close(self):
        self._c.close()


class _Db:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return _Cur(self.conn)


@pytest.fixture()
def db():
    c = sqlite3.connect(":memory:")
    c.executescript(
        """
        CREATE TABLE crawl_face (id INTEGER PRIMARY KEY, image_id INTEGER,
                                 video_id INTEGER, movie_id INTEGER);
        INSERT INTO crawl_face VALUES (1, 5, NULL, NULL);
        INSERT INTO crawl_face VALUES (2, NULL, 777, NULL);
        INSERT INTO crawl_face VALUES (3, NULL, NULL, 777);
        INSERT INTO crawl_face VALUES (4, NULL, NULL, NULL);
        """
    )
    return _Db(c)


def test_old_movie_face_goes_to_video_side(mod, db):
    image_fids, video_fids = mod._split_image_video(db, [1, 2, 3, 4])
    assert image_fids == [1]
    assert sorted(video_fids) == [2, 3]


def test_video_crop_select_includes_movie_only_faces(mod, db, monkeypatch):
    seen = {}

    def fake_core(state, db_, face_ids, *, where_col, media_type, label):
        seen["where_col"] = where_col
        return [], []

    monkeypatch.setattr(mod, "_embed_crop_batch_directly", fake_core)
    mod._embed_video_batch_directly({}, db, [3])
    rows = db.conn.execute(
        f"SELECT id FROM crawl_face WHERE {seen['where_col']} IS NOT NULL ORDER BY id").fetchall()
    assert [r[0] for r in rows] == [2, 3]
