#!/usr/bin/env python3
"""crawl_image に face_count (その画像から検出された顔数) を追加する。

## なぜ必要か

「ドメインごとの取得顔数」を知りたいとき、`crawl_face` (1億行超・45GB、
site/domain 列を持たない) を `crawl_image` 経由で毎回 JOIN して集計するのは
本番 DB への負荷が大きすぎる。 `crawl_video.face_count` (動画側は既に検出時
書き込み済み) と同じ形で `crawl_image` にも持たせれば、
`SELECT site, SUM(face_count) FROM crawl_image GROUP BY site` だけで済む。

書き込みは `plugins/image_hash_extract/production_main.py` の
`_sync_image_face_count()` が検出/dup_hit の都度更新する。 既存行 (この列が
NULL のまま) は `scripts/backfill_image_face_count.py` で埋める。

## 既存行について

既存行は NULL のままで良い (`SUM()` は NULL を無視するので集計上は 0 と等価)。
過去分もドメイン集計に含めたい場合は `backfill_image_face_count.py` を実行する。

## 影響

ALTER は nullable + default 無しなので MariaDB 10.3+ で **INSTANT**
(`crawl_face` への crop_x1/crop_y1 追加 [[video-face-crop-origin-bug]] と同条件、
1.4 億行に対してもメタデータ更新のみ)。 レプリカ/バックアップ側にも同じ ALTER が必要。

## 使い方

    python migrate_add_image_face_count.py                # DRY-RUN (現状表示のみ)
    python migrate_add_image_face_count.py --apply         # ALTER 実行
    python migrate_add_image_face_count.py --apply --algorithm INPLACE   # INSTANT 不可な版用
"""
from __future__ import annotations

import argparse
import sys

TABLE = "crawl_image"
COLUMNS = (
    ("face_count", "INT DEFAULT NULL COMMENT "
                    "'検出された顔数 (crawl_face.image_id 一致件数)'"),
)


def load_env(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError as e:
        sys.exit(f"env 読めません: {path}: {e}")
    return out


def connect(env: dict[str, str]):
    cfg = dict(host=env.get("DB_HOST"), port=int(env.get("DB_PORT", 3306)),
               user=env.get("DB_USER"), password=env.get("DB_PASS"),
               database=env.get("DB_NAME"))
    if not all([cfg["host"], cfg["user"], cfg["password"], cfg["database"]]):
        sys.exit("DB_HOST/USER/PASS/NAME が env に足りません")
    try:
        import mariadb
        c = mariadb.connect(**cfg)
        c.autocommit = True
        return c
    except ImportError:
        import pymysql
        return pymysql.connect(connect_timeout=10, read_timeout=600, autocommit=True, **cfg)


def existing_columns(cur) -> set[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s", (TABLE,))
    return {str(r[0]).lower() for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default="/mnt/paps-ai/ai/.env")
    ap.add_argument("--apply", action="store_true", help="実際に ALTER を実行")
    ap.add_argument("--algorithm", default="INSTANT",
                    choices=["INSTANT", "INPLACE", "COPY"],
                    help="ALTER の ALGORITHM (既定 INSTANT)")
    ap.add_argument("--lock", default="NONE",
                    choices=["NONE", "SHARED", "EXCLUSIVE"],
                    help="ALTER の LOCK (既定 NONE。 crawl_image のように"
                        " indexed virtual column を持つテーブルは"
                        " INSTANT/LOCK=NONE 不可で SHARED が必要なことがある)")
    args = ap.parse_args()

    env = load_env(args.env_file)
    conn = connect(env)
    cur = conn.cursor()

    have = existing_columns(cur)
    todo = [(name, ddl) for name, ddl in COLUMNS if name not in have]

    print(f"table = {TABLE}")
    for name, _ in COLUMNS:
        print(f"  {name}: {'既に存在' if name in have else '未追加'}")
    if not todo:
        print("\n追加するものはありません (冪等)。")
        return 0

    adds = ", ".join(f"ADD COLUMN {name} {ddl}" for name, ddl in todo)
    sql = f"ALTER TABLE {TABLE} {adds}, ALGORITHM={args.algorithm}, LOCK={args.lock}"
    print(f"\n実行する SQL:\n  {sql}")

    if not args.apply:
        print("\nDRY-RUN でした。 実行するには --apply を付けてください。")
        return 0

    import time
    t0 = time.time()
    cur.execute(sql)
    print(f"\n完了 ({time.time() - t0:.2f}s)")

    have2 = existing_columns(cur)
    missing = [n for n, _ in COLUMNS if n not in have2]
    if missing:
        print(f"検証失敗: {missing} が見えません", file=sys.stderr)
        return 1
    print("検証 OK: 列が存在します。")
    print("\n次の手順:")
    print("  1. image_hash_extract/production_main.py を全 GPU ホストへ deploy")
    print("     → 以降の新規画像は face_count が埋まる")
    print("  2. backfill_image_face_count.py で既存行を埋める (任意、過去分も集計に含める場合)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
