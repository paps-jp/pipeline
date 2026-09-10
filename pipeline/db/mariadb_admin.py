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
    creatable: bool = False   # 新規追加フォームに出すか (editable とは別軸: site/domain は
                               # 作成時にしか決められないが既存行の編集は許さない、等)
    kind: str = "str"   # "str" | "int"


@dataclass(frozen=True)
class TableSpec:
    name: str            # 実テーブル名。 レジストリ経由でしか解決されないので SQL に直接使える
    label: str            # UI 表示名
    pk: str
    columns: list[ColumnSpec] = field(default_factory=list)
    searchable: tuple[str, ...] = ()
    # ---- 新規追加 (create) ----
    create_required: tuple[str, ...] = ()             # 空/未指定を拒否する列
    create_defaults: dict[str, Any] = field(default_factory=dict)  # フォームに出さず固定値で埋める列
    # 新規追加時にもう1テーブルへ種を撒く (= crawl_config だけ enabled=1 にしても
    # crawl 表に開始行が無いと永久に何も起きない、という host_import_main.py と
    # 同じ罠を admin 画面からの手動追加でも踏まないため)。
    seed_table: str | None = None
    seed_columns: dict[str, str] | None = None         # {このテーブルの列: seed_table の列}
    # ---- 削除 (delete) ----
    # 本当に DELETE FROM してよいテーブルだけ deletable=True にする。 crawl_config の
    # ように他テーブル (crawl_image/crawl_video 等) から site を参照されうる行は
    # 孤児データを残すので対象外。 その代わり soft_delete_* で「削除」を固定値の
    # UPDATE に読み替える (crawl_config は enabled=-1 = 恒久除外/無効化。
    # 2026-09-10 の priority スキーム移行で 99→-1 に統合した。 詳細は
    # paprika_job_submit/submit_main.py の priority docstring を参照)。
    deletable: bool = False
    soft_delete_column: str | None = None
    soft_delete_value: Any = None
    # ---- 種URL 再投入 (reseed) ----
    # `crawl` 表内の「この crawl_config 行に紐づく種URL」の downloaded_at /
    # submitted_at を NULL に戻す UI アクションを許可する。 使い所は:
    #   - 種ジョブが完了マークだけ付いてリンクが取り込めていないサイト
    #     (links-pull の site_cache 未更新など [[new-sites-dilute-rotation-and-lose-links]])
    #   - priority tier に居るのに crawl 在庫ゼロで自動除外されているサイト
    # 対応する crawl 行が無ければ 1 行 INSERT IGNORE で種を撒き直す
    # (create_row の seed_table 経路と同じ形)。 副作用が「1 サイト分の再投入」で
    # 収まるよう、 リセット対象は必ず `crawl.url = <cc.url>` の 1 URL に限定する。
    reseedable: bool = False

    def column(self, name: str) -> ColumnSpec | None:
        return next((c for c in self.columns if c.name == name), None)

    def editable_names(self) -> set[str]:
        return {c.name for c in self.columns if c.editable}

    def creatable_names(self) -> set[str]:
        return {c.name for c in self.columns if c.creatable}


TABLE_REGISTRY: dict[str, TableSpec] = {
    "crawl_config": TableSpec(
        name="crawl_config", label="クロール対象サイト", pk="id",
        columns=[
            ColumnSpec("id"),
            ColumnSpec("site", creatable=True),
            ColumnSpec("url", editable=True, creatable=True),
            ColumnSpec("domain", creatable=True),
            ColumnSpec("type"),
            # 2026-09-10 priority スキーム: enabled は tri-state ではなく数値。
            # < 1 (-1, 0) = 無効。 >= 1 = 有効で、 値が大きいほど優先。
            #   -1 = 恒久除外/無効化 (以前の -1「個別理由」と 99「恒久除外」を統合)
            #    0 = 無効 (資産ホスト等、 legacy)
            #    1 = 通常
            #   >1 = 優先 tier (submit filler が enabled DESC で先頭注入)
            # 詳細は paprika_job_submit/submit_main.py の priority docstring。
            ColumnSpec("enabled", editable=True, creatable=True, kind="int"),
            ColumnSpec("next_no"),
            ColumnSpec("max_pages_per_run", editable=True, creatable=True, kind="int"),
            ColumnSpec("memo", editable=True, creatable=True),
            ColumnSpec("locked_at"),
            ColumnSpec("worker_id"),
        ],
        searchable=("site", "domain", "memo", "url"),
        create_required=("site", "url", "domain"),
        # next_no は crawl_host_import と同じ既定 (1 から採番)。 type は今のところ
        # 画像サイトしか手動追加の対象にしない。
        create_defaults={"type": "image", "next_no": 1, "enabled": 1},
        # crawl_config.enabled=1 だけでは巡回は始まらない (crawl 表の開始行が無いと
        # job-submit が投げる URL が無い、 plugins/crawl_host_import/host_import_main.py
        # の docstring 参照)。 admin から手動追加するときも同じ種まきをする。
        seed_table="crawl",
        seed_columns={"site": "site", "url": "url"},
        # 「削除」= enabled=-1 (恒久除外) への変更。 crawl_image 等が site を参照
        # したままになりうるので本当の DELETE はしない。 2026-09-10 の priority
        # スキーム移行で、 高い値 (>=1) は「優先 tier」の意味を持つようになった
        # ため、 除外は必ず負値側 (-1) を使う。 旧 99 は同 migration で -1 に統合。
        soft_delete_column="enabled",
        soft_delete_value=-1,
        # 種URL 再投入ボタンを有効化 (詳細は reseedable フィールドのコメント)。
        reseedable=True,
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
        # 他テーブルから参照されない使い捨てデータなので本当の DELETE で問題ない。
        deletable=True,
    ),
}


class TableNotFoundError(KeyError):
    pass


class ColumnNotEditableError(ValueError):
    pass


class RowCreationNotSupportedError(ValueError):
    pass


class MissingRequiredFieldError(ValueError):
    pass


class DuplicateRowError(ValueError):
    pass


class RowDeletionNotSupportedError(ValueError):
    pass


class RowReseedNotSupportedError(ValueError):
    pass


def get_spec(table: str) -> TableSpec:
    spec = TABLE_REGISTRY.get(table)
    if spec is None:
        raise TableNotFoundError(table)
    return spec


def _errno(e: Exception) -> int | None:
    """mariadb / pymysql の双方から MySQL errno を取り出す (api_public.backend._errno と同方針)。"""
    n = getattr(e, "errno", None)
    if isinstance(n, int):
        return n
    args = getattr(e, "args", ())
    if args and isinstance(args[0], int):
        return args[0]
    return None


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


def create_row(db: Any, lock: threading.Lock, spec: TableSpec,
                fields: dict[str, Any]) -> dict[str, Any]:
    """creatable な列 + create_defaults で 1 行 INSERT する。 seed_table 設定が
    あれば同じ tick で種まきも行う (crawl_config → crawl の罠を踏まないため)。
    """
    creatable = spec.creatable_names()
    if not creatable:
        raise RowCreationNotSupportedError(f"{spec.name} は新規追加に対応していません")
    unknown = set(fields) - creatable
    if unknown:
        raise ColumnNotEditableError(
            f"新規追加不可な列: {sorted(unknown)} (許可: {sorted(creatable)})")

    values: dict[str, Any] = dict(spec.create_defaults)
    values.update({k: v for k, v in fields.items() if v not in (None, "")})

    missing = [c for c in spec.create_required if not values.get(c)]
    if missing:
        raise MissingRequiredFieldError(f"必須項目が未入力: {missing}")

    col_names_in = list(values.keys())
    placeholders = ", ".join(["%s"] * len(col_names_in))
    all_col_names = [c.name for c in spec.columns]

    with lock:
        db.ping()
        try:
            cur = db.execute(
                f"INSERT INTO {spec.name} ({', '.join(col_names_in)}) VALUES ({placeholders})",
                tuple(values[c] for c in col_names_in),
            )
        except Exception as e:  # noqa: BLE001
            if _errno(e) == 1062:
                raise DuplicateRowError(f"{spec.name} に同じ内容が既に存在します") from e
            raise
        new_pk = cur.lastrowid
        cur.close()

        if spec.seed_table and spec.seed_columns:
            seed_cols = list(spec.seed_columns.values())
            seed_vals = tuple(values.get(src) for src in spec.seed_columns)
            scur = db.execute(
                f"INSERT IGNORE INTO {spec.seed_table} ({', '.join(seed_cols)}) "
                f"VALUES ({', '.join(['%s'] * len(seed_cols))})",
                seed_vals,
            )
            scur.close()

        cur = db.execute(
            f"SELECT {', '.join(all_col_names)} FROM {spec.name} WHERE {spec.pk} = %s",
            (new_pk,),
        )
        try:
            row = cur.fetchone()
        finally:
            cur.close()
    return dict(zip(all_col_names, row))


def delete_row(db: Any, lock: threading.Lock, spec: TableSpec,
                pk_value: Any) -> dict[str, Any] | None:
    """行を削除する。 soft_delete_column 設定があれば本当の DELETE はせず、
    その列を固定値に書き換える UPDATE として扱う (更新後の行を返す)。
    deletable でも soft_delete でもないテーブルは拒否する。
    """
    if spec.soft_delete_column is not None:
        return update_row(db, lock, spec, pk_value,
                          {spec.soft_delete_column: spec.soft_delete_value})
    if not spec.deletable:
        raise RowDeletionNotSupportedError(f"{spec.name} は削除に対応していません")

    with lock:
        db.ping()
        cur = db.execute(
            f"DELETE FROM {spec.name} WHERE {spec.pk} = %s", (pk_value,))
        rowcount = cur.rowcount
        cur.close()
    if not rowcount:
        raise KeyError(f"{spec.name}.{spec.pk}={pk_value} が見つかりません")
    return None


def reseed_row(db: Any, lock: threading.Lock, spec: TableSpec,
                pk_value: Any) -> dict[str, Any]:
    """種URL を「未投入」状態に戻して priority filler に拾い直させる。

    2026-09-11 追加。 crawl_config 行 1 件に対して:
      1. seed_table (通常 ``crawl``) の対応行 (site + url 一致) の
         ``downloaded_at`` / ``submitted_at`` を NULL に戻す
      2. 該当 seed 行が存在しなければ INSERT IGNORE で 1 行撒く
         (create_row の seed_table 経路と同じ形)

    使い所は TableSpec.reseedable のコメント参照。

    戻り値: ``{"reset": N, "inserted": M, "site": ..., "url": ...}``。
    UI が「N 件を再投入対象に戻しました」と表示するために使う。 通常は
    N=1 (種URL 1本), M=0 だが、 URL が edit されて crawl 側に無ければ
    N=0, M=1 になる (新規種撒き)。
    """
    if not spec.reseedable:
        raise RowReseedNotSupportedError(
            f"{spec.name} は種URL 再投入に対応していません")
    if not (spec.seed_table and spec.seed_columns):
        # reseedable なのに seed_table 未設定 = spec バグ。 明示的に落とす
        # (silent no-op にすると UI の成功メッセージだけ返って実際は何も
        # 起きない、 という debug 困難な状態になる)。
        raise RowReseedNotSupportedError(
            f"{spec.name}: reseedable=True だが seed_table/seed_columns 未設定")

    # 現行の crawl_config 行を読んで site/url を確定させる。 UI 側で持って
    # いる値を信じずに DB を権威にするのは、 タイムラグ (list → click) の
    # 間に他 operator が編集した場合の食い違いを避けるため。
    all_col_names = [c.name for c in spec.columns]
    with lock:
        db.ping()
        cur = db.execute(
            f"SELECT {', '.join(all_col_names)} FROM {spec.name} "
            f"WHERE {spec.pk} = %s",
            (pk_value,),
        )
        try:
            row = cur.fetchone()
        finally:
            cur.close()
        if row is None:
            raise KeyError(f"{spec.name}.{spec.pk}={pk_value} が見つかりません")
        row_dict = dict(zip(all_col_names, row))

        # seed_columns は ``{self_col: seed_col}``。 crawl_config → crawl は
        # ``{"site": "site", "url": "url"}``。 ここでは「seed_table 側の
        # site/url 両方が一致する行」を対象にする。
        seed_where_cols = list(spec.seed_columns.values())     # seed_table 側の列名
        self_source_cols = list(spec.seed_columns.keys())     # 自分側の列名
        seed_where_vals = tuple(row_dict.get(c) for c in self_source_cols)
        if any(v in (None, "") for v in seed_where_vals):
            raise ValueError(
                f"種URL 再投入に必要な列が空です: "
                f"{dict(zip(self_source_cols, seed_where_vals))}")

        seed_where_sql = " AND ".join(f"{c} = %s" for c in seed_where_cols)
        # crawl 表の全 URL を戻したいわけではなく「seed URL 1 本」を再投入対象に
        # したいだけ。 links-pull で拾ってきた 2 本目以降を巻き込まないよう
        # 種の url まで含めた完全一致で絞る。
        upd_cur = db.execute(
            f"UPDATE {spec.seed_table} "
            f"SET downloaded_at = NULL, submitted_at = NULL "
            f"WHERE {seed_where_sql}",
            seed_where_vals,
        )
        reset = int(upd_cur.rowcount or 0)
        upd_cur.close()

        inserted = 0
        if reset == 0:
            # 種 URL が seed_table にまだ無い (URL が edit された等)。
            # create_row と同じ形で撒き直す。
            ins_cur = db.execute(
                f"INSERT IGNORE INTO {spec.seed_table} "
                f"({', '.join(seed_where_cols)}) "
                f"VALUES ({', '.join(['%s'] * len(seed_where_cols))})",
                seed_where_vals,
            )
            inserted = int(ins_cur.rowcount or 0)
            ins_cur.close()

    return {
        "reset": reset,
        "inserted": inserted,
        **dict(zip(seed_where_cols, seed_where_vals)),
    }
