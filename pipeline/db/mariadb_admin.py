"""外部 MariaDB (crawl_config 等) の汎用テーブル admin。

`pipeline/repositories/*` は制御プレーン自身の SQLite (`pipeline.db.base.Database`)
専用なので、 crawl_config のような外部 MariaDB のテーブルはここで別に扱う。

対象テーブル・列は `TABLE_REGISTRY` に**宣言したものだけ**を扱う。 クライアントから
任意のテーブル名/列名を受け取らない (本番の外部 DB への直接書き込みなので、
「汎用」を任意 SQL 露出の方向にはしない)。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    editable: bool = False
    kind: str = "str"   # "str" | "int"


@dataclass(frozen=True)
class TableSpec:
    name: str            # 実テーブル名。 レジストリ経由でしか解決されないので SQL に直接使える
    label: str            # UI 表示名
    pk: str
    columns: list[ColumnSpec] = field(default_factory=list)
    searchable: tuple[str, ...] = ()

    def column(self, name: str) -> ColumnSpec | None:
        return next((c for c in self.columns if c.name == name), None)

    def editable_names(self) -> set[str]:
        return {c.name for c in self.columns if c.editable}


TABLE_REGISTRY: dict[str, TableSpec] = {
    "crawl_config": TableSpec(
        name="crawl_config", label="クロール対象サイト", pk="id",
        columns=[
            ColumnSpec("id"),
            ColumnSpec("site"),
            ColumnSpec("url", editable=True),
            ColumnSpec("domain"),
            ColumnSpec("type"),
            # -1/0/1/50/99 が実在し、それぞれ別の意味を持つ (単純な bool ではない)。
            # 2026-08-25 実測: -1=個別理由付きで無効化、0=無効(資産ホスト等)、
            # 1=有効、50=優先度フラグ、99=恒久除外。
            ColumnSpec("enabled", editable=True, kind="int"),
            ColumnSpec("next_no"),
            ColumnSpec("max_pages_per_run", editable=True, kind="int"),
            ColumnSpec("memo", editable=True),
            ColumnSpec("locked_at"),
            ColumnSpec("worker_id"),
        ],
        searchable=("site", "domain", "memo", "url"),
    ),
    "host_import_queue": TableSpec(
        name="host_import_queue", label="取り込み候補キュー", pk="id",
        columns=[
            ColumnSpec("id"),
            ColumnSpec("source_host_id"),
            ColumnSpec("domain"),
            ColumnSpec("top_url"),
            ColumnSpec("status"),
            ColumnSpec("last_error_type"),
            ColumnSpec("last_error"),
            ColumnSpec("last_http_status"),
            ColumnSpec("next_retry_at"),
        ],
        searchable=("domain", "status", "last_error_type"),
        # 全列 editable=False (今回は可視化のみ: classify がなぜ skip したか追える)
    ),
}


class TableNotFoundError(KeyError):
    pass


class ColumnNotEditableError(ValueError):
    pass


def get_spec(table: str) -> TableSpec:
    spec = TABLE_REGISTRY.get(table)
    if spec is None:
        raise TableNotFoundError(table)
    return spec


def list_rows(db: Any, lock: threading.Lock, spec: TableSpec, *,
              q: str = "", enabled: int | None = None,
              limit: int = 50, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
    """検索 + ページングして (rows, total) を返す。 列名は spec からのみ組み立てる。"""
    col_names = [c.name for c in spec.columns]
    where_sql = ""
    params: list[Any] = []
    conditions: list[str] = []
    if q and spec.searchable:
        like = f"%{q}%"
        conditions.append(
            "(" + " OR ".join(f"{c} LIKE %s" for c in spec.searchable) + ")")
        params.extend([like] * len(spec.searchable))
    if enabled is not None and spec.column("enabled") is not None:
        conditions.append("enabled = %s")
        params.append(enabled)
    if conditions:
        where_sql = " WHERE " + " AND ".join(conditions)

    with lock:
        db.ping()
        count_cur = db.execute(
            f"SELECT COUNT(*) FROM {spec.name}{where_sql}", tuple(params))
        try:
            total = int(count_cur.fetchone()[0])
        finally:
            count_cur.close()

        cols_sql = ", ".join(col_names)
        cur = db.execute(
            f"SELECT {cols_sql} FROM {spec.name}{where_sql} "
            f"ORDER BY {spec.pk} DESC LIMIT %s OFFSET %s",
            tuple(params) + (int(limit), int(offset)),
        )
        try:
            rows = [dict(zip(col_names, r)) for r in cur.fetchall()]
        finally:
            cur.close()
    return rows, total


def update_row(db: Any, lock: threading.Lock, spec: TableSpec,
                pk_value: Any, fields: dict[str, Any]) -> dict[str, Any]:
    """editable な列だけを更新する。 editable でない列が来たら拒否。"""
    editable = spec.editable_names()
    unknown = set(fields) - editable
    if unknown:
        raise ColumnNotEditableError(
            f"編集不可な列: {sorted(unknown)} (許可: {sorted(editable)})")
    if not fields:
        raise ValueError("更新する列がありません")

    sets_sql = ", ".join(f"{k} = %s" for k in fields)
    params = tuple(fields.values()) + (pk_value,)

    col_names = [c.name for c in spec.columns]
    with lock:
        db.ping()
        cur = db.execute(
            f"UPDATE {spec.name} SET {sets_sql} WHERE {spec.pk} = %s", params)
        cur.close()

        cur = db.execute(
            f"SELECT {', '.join(col_names)} FROM {spec.name} WHERE {spec.pk} = %s",
            (pk_value,),
        )
        try:
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        raise KeyError(f"{spec.name}.{spec.pk}={pk_value} が見つかりません")
    return dict(zip(col_names, row))
