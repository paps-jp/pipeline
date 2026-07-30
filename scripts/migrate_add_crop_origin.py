#!/usr/bin/env python3
"""crawl_face に crop_x1 / crop_y1 (crop 画像の原点) を追加する。

## なぜ必要か

kps_json は **元 frame 座標**なので、 MinIO 上の画像の座標系へ移すには「その画像の左上が
frame のどこか」を引く必要がある。 embed は長らく検出 bbox 原点を引いていたが、 動画顔の
minio_key が指す画像は bbox ぴったりの crop ではないため全動画顔の align がずれていた
([[video-face-crop-origin-bug]])。

movie2face の manifest には `crop_bbox` が最初から入っているのに保存していなかった。 この
列でそれを永続化し、 vfe (extract_main.py) が INSERT 時に書く。

## 既存行について

既存行は NULL のままで良い。 embed 側 (`_crop_origin`) が、 align のために既にデコード
済の画像の寸法から世代を判定して原点を復元する:

    margin crop (movie2face 0.4)  → (max(0,floor(x1-0.4bw)), max(0,floor(y1-0.4bh)))
    元画像/フレーム丸ごと         → (0, 0)

本番実測 (id 帯 5 点 x 40 件) で判定は全件一致し、 align の枠外黒余白は
81.9%/37.2%/99.5%/23.2%/24.2% → 3.9%/7.6%/0.0%/1.1%/10.0% に改善する。

## 影響

ALTER は nullable + default 無しなので MariaDB 10.3+ で **INSTANT** (テーブル 8,700 万行に
対してもメタデータ更新のみ)。 レプリカ/バックアップ側にも同じ ALTER が必要。

## 使い方

    python migrate_add_crop_origin.py                 # DRY-RUN (現状表示のみ)
    python migrate_add_crop_origin.py --apply         # ALTER 実行
    python migrate_add_crop_origin.py --apply --algorithm INPLACE   # INSTANT 不可な版用
"""
from __future__ import annotations

import argparse
import sys

TABLE = "crawl_face"
COLUMNS = (
    ("crop_x1", "FLOAT DEFAULT NULL COMMENT 'minio_key 画像の原点 X (movie2face crop_bbox)'"),
    ("crop_y1", "FLOAT DEFAULT NULL COMMENT 'minio_key 画像の原点 Y (movie2face crop_bbox)'"),
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
    sql = f"ALTER TABLE {TABLE} {adds}, ALGORITHM={args.algorithm}, LOCK=NONE"
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
    print("検証 OK: 両列が存在します。")
    print("\n次の手順:")
    print("  1. vfe (video_face_extract/extract_main.py) を全 GPU ホストへ deploy")
    print("     → 以降の新規動画顔は crop_x1/crop_y1 が埋まる")
    print("  2. image_embed/embed_main.py を deploy (既存 NULL 行は寸法から原点を復元)")
    print("  3. canary 1 ホストで runs の origin= 内訳と keep 率を確認")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
