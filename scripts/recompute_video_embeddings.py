#!/usr/bin/env python3
"""動画顔 embedding を全再計算する (crop 原点 bug 修正後の backfill ドライバ)。

## 背景

動画顔の embedding は、 MinIO 上の画像の原点を誤って引いていたため全て劣化していた
([[video-face-crop-origin-bug]])。 修正後に既存分を作り直すのが本スクリプト。

## 前提 (これを満たさないと再計算が「静かに無駄」になる)

1. `image_embed/embed_main.py` の `_crop_origin` 修正が全 GPU ホストに deploy 済
2. `embed_write/writer_main.py` の **in-place 上書き**が deploy 済
   `crawl_embedding_index` は PRIMARY KEY(face_id) なので、 旧 writer の `INSERT IGNORE`
   では index が古いベクトルを指したまま新しい値だけが shard を食う (しかも
   crawl_face.adaface_norm は更新されるので成功に見える)。
3. `scripts/migrate_add_crop_origin.py --apply` 済 (crop_x1/crop_y1)

`--check` で 1 と 2 の deploy 状況を確認できる。

## やること

id 昇順に crawl_face を歩き、 対象 face_id を `image-embed` のキューへ push するだけ。
`adaface_ready` は **触らない**:
  - リセットすると一時的に「未 embed」扱いになり、 orphan_reconcile とも二重に走る
  - 触らなくても embed → writer が in-place 上書きして adaface_ready=1 を再度書く
  - つまり検索が落ちる瞬間が無い

## clone の除外

`_copy_faces_for_video` は重複動画向けに crawl_face を行複製する。 clone は元と同じ
minio_key / kps / bbox なので再計算しても**同一ベクトル**になり、 index も持っていない
(PK が face_id なので元の行だけが index を持つ)。 既定で除外する (= 同じ minio_key の
最小 id だけを対象にする)。 実測で対象 6.10M → 約 4.25M に減る。

clone 自身を検索対象にしたい場合は `--copy-clone-index` を後から実行する (元の index 行を
clone の face_id へ複写する。 再計算が終わってから走らせること)。

## 流量制御

embed キューの pending を見て、 閾値を超えている間は push を止める (= 新規の取り込みを
止めない)。 `--queue-high` / `--queue-low` で調整。

## 中断・再開

毎バッチ後に state ファイルへ last_id を書く。 `--stop-file` を作れば安全に停止。

## 使い方

    python recompute_video_embeddings.py --check          # 前提の deploy 状況だけ確認
    python recompute_video_embeddings.py                  # DRY-RUN (件数を数えるだけ)
    python recompute_video_embeddings.py --apply
    python recompute_video_embeddings.py --apply --batch 2000 --sleep 1.0
    python recompute_video_embeddings.py --copy-clone-index --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

DEFAULT_STATE_DIR = "/var/lib/pipeline/recompute_video_emb"
DEFAULT_CONTROL = "http://127.0.0.1:8001"
WORKLOAD = "image-embed"


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


def connect(env: dict[str, str], read_timeout: int = 600):
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
        return pymysql.connect(connect_timeout=10, read_timeout=read_timeout,
                               autocommit=True, **cfg)


# ---------------- 前提チェック ----------------

def check_prereqs(cur, control: str) -> bool:
    ok = True
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name='crawl_face' "
                "AND column_name IN ('crop_x1','crop_y1')")
    cols = {str(r[0]).lower() for r in cur.fetchall()}
    if {"crop_x1", "crop_y1"} <= cols:
        print("  [OK]   crawl_face.crop_x1 / crop_y1 が存在")
    else:
        print("  [NG]   crop_x1/crop_y1 が無い → migrate_add_crop_origin.py --apply を先に")
        ok = False

    # embed 側の修正が動いているか: runs の output に origin= 内訳が出ているかで判定
    try:
        with urllib.request.urlopen(
                f"{control}/api/v1/workloads/{WORKLOAD}/runs?limit=40", timeout=15) as r:
            runs = json.loads(r.read())
        blob = json.dumps(runs)
        if '"origin"' in blob or "origin=" in blob:
            print("  [OK]   image-embed の runs に origin 判定が出ている (修正 deploy 済)")
        else:
            print("  [WARN] runs に origin が見えない。 embed_main.py の deploy と "
                  "worker 再起動を確認 (直近 run が無いだけの可能性もあり)")
    except Exception as e:
        print(f"  [WARN] runs 取得失敗 ({e}) — deploy は手動で確認すること")

    # writer の in-place が入っているか (ソースを直接見るのが確実)
    for p in ("/opt/pipeline/plugins/embed_write/writer_main.py",
              "/home/paps-ai/ai/pipeline/plugins/embed_write/writer_main.py"):
        if os.path.exists(p):
            with open(p, encoding="utf-8") as fh:
                src = fh.read()
            if "ON DUPLICATE KEY UPDATE" in src and "_open_shard_for_update" in src:
                print(f"  [OK]   writer in-place 上書きあり ({p})")
            else:
                print(f"  [NG]   writer が旧版 ({p}) → INSERT IGNORE のままなので "
                      f"再計算しても index が更新されない")
                ok = False
            break
    else:
        print("  [WARN] writer_main.py が見つからず確認できません (writer ホストで実行を)")
    return ok


# ---------------- キュー流量 ----------------

def queue_pending(control: str) -> int | None:
    try:
        with urllib.request.urlopen(
                f"{control}/api/v1/workloads/{WORKLOAD}/queue", timeout=10) as r:
            return int(json.loads(r.read()).get("by_state", {}).get("pending", 0))
    except Exception:
        return None


def push_batch(control: str, ids: list[int]) -> int:
    payload = {"items": [{"pk": str(i), "extra": {"recompute": 1}} for i in ids]}
    req = urllib.request.Request(
        f"{control}/api/v1/workloads/{WORKLOAD}/tasks/batch",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return int(json.loads(r.read()).get("inserted") or 0)


# ---------------- 対象選択 ----------------

SELECT_TARGETS = """
SELECT f.id FROM crawl_face f
WHERE f.video_id IS NOT NULL
  AND f.id > %s
  AND f.adaface_ready IN (1, 2)
  AND f.minio_key IS NOT NULL AND f.minio_key <> ''
  AND f.kps_json IS NOT NULL AND f.bbox_x1 IS NOT NULL
  {clone_filter}
ORDER BY f.id LIMIT %s
"""

# clone = 同じ minio_key を持つ行のうち id 最小でないもの。 idx_minio_key が効くので
# 2000 件バッチで実測 0.32s。
CLONE_FILTER = """
  AND NOT EXISTS (SELECT 1 FROM crawl_face g
                  WHERE g.minio_key = f.minio_key AND g.id < f.id)
"""


def read_state(path: str) -> int:
    try:
        with open(path, encoding="utf-8") as f:
            return int(json.load(f).get("last_id") or 0)
    except Exception:
        return 0


def write_state(path: str, last_id: int, pushed: int) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_id": last_id, "pushed": pushed, "ts": int(time.time())}, f)
    os.replace(tmp, path)


def run_recompute(args, conn) -> int:
    cur = conn.cursor()
    state_path = os.path.join(args.state_dir, "state.json")
    last_id = args.start_id if args.start_id is not None else read_state(state_path)
    sql = SELECT_TARGETS.format(
        clone_filter="" if args.include_clones else CLONE_FILTER)

    print(f"開始 last_id={last_id:,} batch={args.batch} "
          f"clone={'含む' if args.include_clones else '除外'} "
          f"{'APPLY' if args.apply else 'DRY-RUN'}")
    total_seen = total_pushed = 0
    t_start = time.time()
    while True:
        if os.path.exists(args.stop_file):
            print(f"STOP ファイル検出 ({args.stop_file}) — 停止します")
            break
        cur.execute(sql, (last_id, args.batch))
        ids = [int(r[0]) for r in cur.fetchall()]
        if not ids:
            print("対象が尽きました (完了)")
            break
        total_seen += len(ids)

        if args.apply:
            # キューが混んでいる間は待つ (= 新規取り込みを止めない)。
            # control plane が落ちていて pending 不明 (None) の時は「進む」ではなく「待つ」。
            # 進むと push が失敗するだけなので、 待って復帰を見るほうが安全。
            while True:
                pend = queue_pending(args.control_url)
                if pend is not None and pend <= args.queue_high:
                    break
                if pend is None:
                    print(f"  control plane 応答なし → {args.queue_wait}s 待機")
                else:
                    print(f"  queue pending={pend:,} > {args.queue_high:,} → "
                          f"{args.queue_wait}s 待機")
                time.sleep(args.queue_wait)
                if os.path.exists(args.stop_file):
                    break
            # push は retry する。 数十時間走るジョブが control plane の再起動 (数秒) で
            # 死ぬと、 その間ずっと再計算が止まったまま気付かない。
            n = None
            for attempt in range(args.push_retries):
                try:
                    n = push_batch(args.control_url, ids)
                    break
                except Exception as e:
                    wait = min(60.0, 2.0 * (attempt + 1))
                    print(f"  push 失敗 ({attempt + 1}/{args.push_retries}) "
                          f"last_id={last_id}: {str(e)[:120]} → {wait}s 後に retry",
                          file=sys.stderr)
                    time.sleep(wait)
                    if os.path.exists(args.stop_file):
                        break
            if n is None:
                print(f"push が {args.push_retries} 回失敗したので中断 "
                      f"(last_id={last_id} から再開可)", file=sys.stderr)
                return 1
            total_pushed += n
        last_id = ids[-1]
        if args.apply:
            write_state(state_path, last_id, total_pushed)
        rate = total_seen / max(0.001, time.time() - t_start)
        print(f"  last_id={last_id:,} seen={total_seen:,} pushed={total_pushed:,} "
              f"({rate:.0f}/s)")
        if args.limit and total_seen >= args.limit:
            print(f"--limit {args.limit:,} に到達 — 停止")
            break
        if args.sleep:
            time.sleep(args.sleep)
    print(f"\n対象 {total_seen:,} 件 / push {total_pushed:,} 件 "
          f"/ {time.time() - t_start:.0f}s")
    if not args.apply:
        print("DRY-RUN でした。 実行するには --apply を付けてください。")
    return 0


def run_copy_clone_index(args, conn) -> int:
    """clone の face_id へ、 元の行の index を複写する (再計算完了後に実行)。

    clone は元と同一のベクトルなので、 元の (shard_id, row_index) をそのまま指してよい。
    shard 側は 1 行を複数 face_id が共有する形になるが、 読み出しは row_index 指定なので
    問題ない (容量も増えない)。
    """
    cur = conn.cursor()
    print(f"clone index 複写 {'APPLY' if args.apply else 'DRY-RUN'}")
    sql = """
    SELECT f.id, i.shard_id, i.row_index, i.norm
    FROM crawl_face f
    JOIN crawl_face o ON o.minio_key = f.minio_key AND o.id < f.id
    JOIN crawl_embedding_index i ON i.face_id = o.id
    LEFT JOIN crawl_embedding_index mine ON mine.face_id = f.id
    WHERE f.video_id IS NOT NULL AND f.id > %s AND mine.face_id IS NULL
    ORDER BY f.id LIMIT %s
    """
    state_path = os.path.join(args.state_dir, "clone_state.json")
    last_id = args.start_id if args.start_id is not None else read_state(state_path)
    total = 0
    while True:
        if os.path.exists(args.stop_file):
            print("STOP ファイル検出 — 停止")
            break
        cur.execute(sql, (last_id, args.batch))
        rows = cur.fetchall()
        if not rows:
            print("対象が尽きました (完了)")
            break
        if args.apply:
            c2 = conn.cursor()
            c2.executemany(
                "INSERT INTO crawl_embedding_index (face_id, shard_id, row_index, norm) "
                "VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE shard_id=VALUES(shard_id), "
                "row_index=VALUES(row_index), norm=VALUES(norm)",
                [(int(r[0]), int(r[1]), int(r[2]), r[3]) for r in rows])
            c2.close()
        total += len(rows)
        last_id = int(rows[-1][0])
        if args.apply:
            write_state(state_path, last_id, total)
        print(f"  last_id={last_id:,} copied={total:,}")
        if args.limit and total >= args.limit:
            break
        if args.sleep:
            time.sleep(args.sleep)
    print(f"\n複写 {total:,} 件")
    if not args.apply:
        print("DRY-RUN でした。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default="/mnt/paps-ai/ai/.env")
    ap.add_argument("--control-url", default=DEFAULT_CONTROL)
    ap.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    ap.add_argument("--stop-file", default=os.path.join(DEFAULT_STATE_DIR, "STOP"))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--check", action="store_true", help="前提の deploy 状況だけ見る")
    ap.add_argument("--copy-clone-index", action="store_true",
                    help="clone の face_id へ元の index 行を複写 (再計算後に実行)")
    ap.add_argument("--include-clones", action="store_true",
                    help="clone も再計算対象にする (既定は除外)")
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=0, help="この件数で打ち切る (0=無制限)")
    ap.add_argument("--start-id", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.5, help="バッチ間の待ち (秒)")
    ap.add_argument("--queue-high", type=int, default=50_000,
                    help="embed キューの pending がこれを超えている間 push を止める")
    ap.add_argument("--queue-wait", type=float, default=30.0)
    ap.add_argument("--push-retries", type=int, default=20,
                    help="push 失敗時の retry 回数 (control plane 再起動を跨げるように)")
    args = ap.parse_args()

    env = load_env(args.env_file)
    conn = connect(env)
    cur = conn.cursor()

    if args.check:
        print("前提チェック:")
        return 0 if check_prereqs(cur, args.control_url) else 1

    print("前提チェック:")
    if not check_prereqs(cur, args.control_url):
        if args.apply:
            print("\n前提を満たしていません。 修正してから --apply してください。",
                  file=sys.stderr)
            return 1
        print("\n(DRY-RUN なので続行します)")

    if args.copy_clone_index:
        return run_copy_clone_index(args, conn)
    return run_recompute(args, conn)


if __name__ == "__main__":
    raise SystemExit(main())
