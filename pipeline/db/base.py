"""Database アダプタの抽象基底クラス。

各 DB 種別 (SQLite / PostgreSQL / MariaDB) は本クラスを継承して dialect 差を吸収。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Iterator


class Database(ABC):
    """全 DB アダプタ共通の interface."""

    url: str

    @abstractmethod
    def __init__(self, url: str) -> None: ...

    @abstractmethod
    def ensure_schema(self) -> None:
        """テーブル / インデックスを idempotent に作成。"""

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator["Connection"]:
        """transaction scope。with 抜けで自動 commit/rollback。"""

    @abstractmethod
    def close(self) -> None: ...


class Connection(ABC):
    """transaction 内で渡される connection wrapper."""

    @abstractmethod
    def execute(self, sql: str, params: dict[str, Any] | tuple = ()) -> "Cursor": ...

    @abstractmethod
    def executemany(self, sql: str, seq_params: list[tuple]) -> None: ...


class Cursor(ABC):
    """cursor の最小 interface (fetchall / fetchone / rowcount)."""

    @abstractmethod
    def fetchone(self) -> dict[str, Any] | None: ...

    @abstractmethod
    def fetchall(self) -> list[dict[str, Any]]: ...

    @property
    @abstractmethod
    def rowcount(self) -> int: ...
