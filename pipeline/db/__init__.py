"""Database 抽象化レイヤ。

`get_db(url)` を呼ぶと URL スキーマに応じた DB アダプタが返る。
"""

from __future__ import annotations

from pipeline.db.base import Database


def get_db(url: str) -> Database:
    """URL スキーマで DB アダプタを選択して開く。

    >>> db = get_db("sqlite:///./pipeline.db")
    >>> db.ensure_schema()
    """
    if url.startswith("sqlite:"):
        from pipeline.db.sqlite import SqliteDatabase

        return SqliteDatabase(url)
    if url.startswith(("postgresql:", "postgres:")):
        raise NotImplementedError("postgres adapter は F1 で実装予定")
    if url.startswith(("mysql:", "mariadb:")):
        from pipeline.db.mariadb import MariadbDatabase

        return MariadbDatabase(url)
    raise ValueError(f"unsupported DB URL scheme: {url!r}")


__all__ = ["Database", "get_db"]
