#!/usr/bin/env python3
"""crawl_image.face_count の既存行 (NULL のまま) を一回きりで埋める。

## 背景

`migrate_add_image_face_count.py` で列を足しただけでは、既存の crawl_image
(1.4 億行) は face_count=NULL のまま (今後の新規行は
`image_hash_extract/production_main.py` の `_sync_image_face_count()` が
検出時に埋める)。 過去分もドメイン別集計 (`SUM(face_count) GROUP BY site`) に
含めたい場合はこのスクリプトで一度だけ埋める。

## やり方

crawl_image.id を範囲チャンクに分けて、 チャンクごとに

    UPDATE crawl_image ci
    JOIN (
      SELECT image_id, COUNT(*) AS cnt FROM crawl_face
      WHERE image_id BETWEEN :lo AND :hi GROUP BY image_id
    ) fc ON fc.image_id = ci.id
    SET ci.face_count = fc.cnt
    WHERE ci.id BETWEEN :lo AND :hi AND ci.face_count IS NULL

を実行する。 crawl_face.idx_image_id を使った range scan なので、 チャンクの
負荷は crawl_face 全件舐めではなく該当 image_id 帯だけに収まる。
crawl_face 行が無い image (顔が無い/未処理) は対象外のまま NULL — SUM() に
とっては 0 と等価なので問題ない。

チャンク間に --sleep-s を挟んで本番への継続負荷を抑える。 --start-id / --end-id
で範囲を絞れば分割実行・再開ができる (未指定なら crawl_image の MIN/MAX を使う)。

## 使い方

    python backfill_image_face_count.py                          # DRY-RUN
    python backfill_image_face_count.py --apply                  # 全件実行
    python backfill_image_face_count.py --apply --start-id 1 --end-id 50000000  # 範囲指定
    python backfill_image_face_count.py --apply --chunk-size 50000 --sleep-s 0.5
"""
from __future__ import annotations

import argparse
import sys
import time


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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default="/mnt/paps-ai/ai/.env")
    ap.add_argument("--apply", action="store_true", help="実際に UPDATE を実行")
    ap.add_argument("--chunk-size", type=int, default=100_000)
    ap.add_argument("--sleep-s", type=float, default=0.3)
    ap.add_argument("--start-id", type=int, default=None)
    ap.add_argument("--end-id", type=int, default=None)
    args = ap.parse_args()

    env = load_env(args.env_file)
    conn = connect(env)
    cur = conn.cursor()

    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name='crawl_image' "
                "AND column_name='face_count'")
    if not cur.fetchone():
        sys.exit("crawl_image.face_count が存在しません。"
                 " 先に migrate_add_image_face_count.py --apply を実行してください。")

    start_id = args.start_id
    end_id = args.end_id
    if start_id is None or end_id is None:
        cur.execute("SELECT MIN(id), MAX(id) FROM crawl_image")
        lo, hi = cur.fetchone()
        if lo is None:
            print("crawl_image が空です。")
            return 0
        start_id = start_id if start_id is not None else int(lo)
        end_id = end_id if end_id is not None else int(hi)

    total_chunks = max(1, (end_id - start_id) // args.chunk_size + 1)
    print(f"範囲: id {start_id} 〜 {end_id} ({total_chunks} チャンク, "
         f"chunk_size={args.chunk_size}, sleep={args.sleep_s}s)")
    if not args.apply:
        print("\nDRY-RUN でした。 実行するには --apply を付けてください。")
        return 0

    t0 = time.time()
    updated_total = 0
    lo = start_id
    chunk_no = 0
    while lo <= end_id:
        hi = min(lo + args.chunk_size - 1, end_id)
        chunk_no += 1
        ct0 = time.time()
        ucur = conn.cursor()
        try:
            ucur.execute(
                """
                UPDATE crawl_image ci
                JOIN (
                  SELECT image_id, COUNT(*) AS cnt FROM crawl_face
                  WHERE image_id BETWEEN %s AND %s GROUP BY image_id
                ) fc ON fc.image_id = ci.id
                SET ci.face_count = fc.cnt
                WHERE ci.id BETWEEN %s AND %s AND ci.face_count IS NULL
                """,
                (lo, hi, lo, hi),
            )
            n = ucur.rowcount
        finally:
            ucur.close()
        updated_total += n
        elapsed = time.time() - t0
        print(f"[{chunk_no}/{total_chunks}] id {lo}-{hi}: updated={n} "
             f"(chunk={time.time() - ct0:.2f}s, total={elapsed:.1f}s, "
             f"累計updated={updated_total})")
        lo = hi + 1
        if lo <= end_id and args.sleep_s > 0:
            time.sleep(args.sleep_s)

    print(f"\n完了: {updated_total} 行更新 ({time.time() - t0:.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
