#!/usr/bin/env python3
"""crawl_face.minio_key を site 込みの旧体系から ID fan-out 体系へ移行する。

旧: crawl_face/{site}/{image_id}_{face_index}.jpg
新: face/{id を3桁ずつ区切った上位}/{face_id}.jpg
    例) face_id=80089450 -> face/800/894/80089450.jpg

site を含めない理由:
  - 同一バイナリが複数サイトに存在する (266 サイトの例あり)
  - 重複コピー (_copy_faces_for_image) が minio_key をそのまま複製するため
    「キーの site」と「行の site」が食い違う (毎日 約 5 万件)
  - site プレフィックスは最大 2.2% 偏在し、10 億件では 1 プレフィックスに
    2,185 万オブジェクトとなって listing が破綻する

--- 実行前提 (writer 切替が先) ---
本スクリプトを走らせる前に、書き込み側 (_copy_faces_for_image 等) を
新体系で物理コピーする実装へ切り替えておく必要がある。切り替え前だと:
  - 移行済み行 (face/…) を writer が複製すると 2 行が同一オブジェクトを
    共有し、片方の GC で他方が壊れる
  - スキャンが通過した id 領域に旧キーの新規行が湧き続けて 1 パスで終わらない
起動時に「直近 id 帯に crawl_face/ プレフィックスが残っていない」ことを
assert する (--skip-writer-check で override 可、非推奨)。

--- 安全設計 ---
バッチ単位で以下の順序を守る:
    (1) copy   旧キー -> 新キー   (全件)
    (2) UPDATE crawl_face.minio_key = 新キー  (成功したものだけ・単一トランザクション)
    (3) delete 旧キー             (更新できたものだけ)
この順なら「DB が指す先が存在しない」瞬間が発生しない。
  (1)-(2) 間で中断 -> 新キーのゴミが残るだけ (再実行で上書き、冪等)
  (2)-(3) 間で中断 -> 旧キーが残るだけ (容量のみ、参照されない)
UPDATE は明示的トランザクションで実行し、失敗時は全ロールバック + abort。
部分成功で「done=total なのに未移行行が残る」状態を作らない。

--- 中断・再開 ---
STOP ファイル (--stop-file) を作れば、実行中のバッチを終えて安全に停止。
毎バッチ後に state ファイル (--state-file) に last_id を書き出し、
次回起動時に --start-id 未指定なら自動再開する。

--- ログ・エラー出力 ---
missing/error/state/stop の既定パスは reboot で消えない永続ディレクトリ
(--state-dir, 既定 /var/lib/pipeline/migrate_face) 配下に置く。
削除失敗した旧キーも別ファイルに記録する (容量リーク追跡用)。

--- 使い方 ---
  python migrate_face_keys.py                    # DRY-RUN
  python migrate_face_keys.py --apply            # 実移行
  python migrate_face_keys.py --apply --limit 500
  python migrate_face_keys.py --apply --start-id 12345678   # 手動再開
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

TARGET_PREFIX = "crawl_face/"
EXCLUDE_PREFIX = "crawl_face/video/"
BUCKET = "crawl"
ENDPOINT = "10.10.50.16:9000"

DEFAULT_STATE_DIR = "/var/lib/pipeline/migrate_face"
WRITER_CHECK_RECENT_IDS = 10000  # 直近 N id を writer 切替チェックの標本にする


def new_key(face_id: int) -> str:
    """face_id -> face/{3桁ずつ}/{face_id}.jpg

    3 桁ずつ区切り、最後のかたまり以外をディレクトリにする。
    ファイル名はフル ID (末尾チャンクだけだと同名ディレクトリと衝突する)。
    ゼロ埋めしないので桁が増えれば階層が 1 つ深くなるだけで、
    どのディレクトリも最大 1,110 ファイルに収まる。
    """
    sid = str(int(face_id))
    chunks = [sid[i:i + 3] for i in range(0, len(sid), 3)]
    return "face/" + "".join(c + "/" for c in chunks[:-1]) + sid + ".jpg"


def load_env(path: str = "/mnt/paps-ai/ai/.env") -> dict:
    env = {}
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_state(state_file: str) -> dict:
    if not os.path.exists(state_file):
        return {}
    try:
        with open(state_file, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print("state 読み込み失敗 (無視して 0 から再開): %s" % e, flush=True)
        return {}


def _save_state(state_file: str, last_id: int, done: int, total: int) -> None:
    """last_id を state ファイルへ atomic に書く (crash 中の破損防止)."""
    payload = {"last_id": int(last_id), "done": int(done),
               "total": int(total), "ts": int(time.time())}
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    os.replace(tmp, state_file)


class DB:
    """mariadb 接続を再接続 retry で薄くラップする。

    数日走行中の wait_timeout / NW 瞬断で 1 発死しないよう、OperationalError
    を捉えて再接続する。SELECT/UPDATE の両方に必要。
    """

    def __init__(self, env: dict):
        self._env = env
        self._conn = None
        self._connect()

    def _connect(self) -> None:
        import mariadb
        self._conn = mariadb.connect(
            host=self._env["DB_HOST"], port=int(self._env["DB_PORT"]),
            user=self._env["DB_USER"], password=self._env["DB_PASS"],
            database=self._env["DB_NAME"])
        self._conn.autocommit = True

    @property
    def raw(self):
        return self._conn

    def cursor(self):
        return self._conn.cursor()

    def run(self, fn, retries: int = 5):
        """fn(conn) を再接続 retry つきで実行する。fn 内で状態を持たないこと。"""
        import mariadb
        delay = 1.0
        for i in range(retries):
            try:
                return fn(self._conn)
            except mariadb.OperationalError as e:
                print("  DB 切断検知 (%s) 再接続 %d/%d" % (str(e)[:80], i + 1, retries),
                      flush=True)
                try:
                    self._conn.close()
                except Exception:
                    pass
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                self._connect()
        raise RuntimeError("DB 再接続 %d 回失敗" % retries)


def _check_writer_switched(db: DB) -> tuple[bool, int]:
    """直近の書き込みが旧キーを吐いていないことを確認する。

    戻り値 (ok, sample_old_count)。ok=False なら writer が未切替。
    """
    def _q(conn):
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT minio_key FROM crawl_face ORDER BY id DESC LIMIT %s"
            ") t WHERE minio_key LIKE %s AND minio_key NOT LIKE %s",
            (WRITER_CHECK_RECENT_IDS, TARGET_PREFIX + "%", EXCLUDE_PREFIX + "%"))
        return c.fetchone()[0]
    n = db.run(_q)
    return (n == 0, int(n))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--start-id", type=int, default=-1,
                    help="未指定なら state ファイルから自動再開 (無ければ 0)")
    ap.add_argument("--state-dir", default=DEFAULT_STATE_DIR,
                    help="state / missing / error / stop の永続置き場")
    ap.add_argument("--state-file", default=None,
                    help="既定 <state-dir>/state.json")
    ap.add_argument("--stop-file", default=None,
                    help="既定 <state-dir>/STOP")
    ap.add_argument("--missing-file", default=None,
                    help="既定 <state-dir>/face_missing.txt  実体が無い face_id")
    ap.add_argument("--error-file", default=None,
                    help="既定 <state-dir>/face_error.txt  一時エラー (再試行対象)")
    ap.add_argument("--del-failed-file", default=None,
                    help="既定 <state-dir>/del_failed.txt  削除失敗した旧キー (容量リーク追跡)")
    ap.add_argument("--skip-writer-check", action="store_true",
                    help="writer 切替 assert を無効化 (非推奨・データ破壊リスクあり)")
    args = ap.parse_args()

    from minio import Minio
    from minio.commonconfig import CopySource
    from minio.deleteobjects import DeleteObject

    # 永続ディレクトリの用意 (/tmp は reboot で消えるので既定にしない)
    _ensure_dir(args.state_dir)
    state_file = args.state_file or os.path.join(args.state_dir, "state.json")
    stop_file = args.stop_file or os.path.join(args.state_dir, "STOP")
    missing_file = args.missing_file or os.path.join(args.state_dir, "face_missing.txt")
    error_file = args.error_file or os.path.join(args.state_dir, "face_error.txt")
    del_failed_file = args.del_failed_file or os.path.join(args.state_dir, "del_failed.txt")

    env = load_env()
    db = DB(env)
    m = Minio(ENDPOINT, access_key=env["MINIO_ACCESS_KEY"],
              secret_key=env["MINIO_SECRET_KEY"], secure=False)

    # writer 切替 assert (--apply かつ --skip-writer-check なし の時のみ強制)
    ok, sample = _check_writer_switched(db)
    print("writer 切替チェック: 直近 %d 件中 旧キー %d 件"
          % (WRITER_CHECK_RECENT_IDS, sample), flush=True)
    if not ok and args.apply and not args.skip_writer_check:
        print("ERROR: writer がまだ旧体系 (crawl_face/…) を書いている。", flush=True)
        print("       _copy_faces_for_image 側を新 face_id の物理コピーへ", flush=True)
        print("       切り替えてから再実行すること。", flush=True)
        print("       (どうしても override するなら --skip-writer-check)", flush=True)
        return 3

    # start-id の決定 (明示指定 > state ファイル > 0)
    state = _load_state(state_file)
    if args.start_id >= 0:
        last_id = args.start_id
        print("開始 id (--start-id 指定): %d" % last_id, flush=True)
    elif state.get("last_id"):
        last_id = int(state["last_id"])
        print("開始 id (state から再開): %d  (前回 %d/%d)"
              % (last_id, state.get("done", 0), state.get("total", 0)), flush=True)
    else:
        last_id = 0
        print("開始 id: 0 (state なし)", flush=True)

    def _count(conn):
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM crawl_face "
            "WHERE minio_key LIKE %s AND minio_key NOT LIKE %s AND id > %s",
            (TARGET_PREFIX + "%", EXCLUDE_PREFIX + "%", last_id))
        return c.fetchone()[0]
    total = db.run(_count)
    print("対象 %d 件 / モード %s / 並列 %d / state=%s"
          % (total, "実移行" if args.apply else "DRY-RUN", args.workers, state_file),
          flush=True)

    done = ok_copy = ok_upd = ok_del = 0
    missing = errors = del_failed_total = 0
    t0 = time.time()

    def do_copy(item):
        """戻り値 (face_id, 旧, 新, err, kind)
        kind: None=成功 / 'missing'=実体が無い / 'error'=一時的な失敗

        NoSuchKey (実体が無い) と、通信断・タイムアウト等の一時的失敗は
        必ず区別する。 後者を「実体なし」として行削除すると、
        **実在するデータを消してしまう**。
        """
        fid, old, nk = item
        try:
            m.copy_object(BUCKET, nk, CopySource(BUCKET, old))
            return (fid, old, nk, None, None)
        except Exception as e:
            code = getattr(e, "code", "") or ""
            msg = str(e)[:120]
            kind = "missing" if code in ("NoSuchKey", "NoSuchObject") else "error"
            return (fid, old, nk, msg, kind)

    while True:
        if os.path.exists(stop_file):
            print("STOP ファイル (%s) を検出。安全に停止する。" % stop_file, flush=True)
            break

        def _fetch(conn, lid=None):
            c = conn.cursor()
            c.execute(
                "SELECT id, minio_key FROM crawl_face "
                "WHERE minio_key LIKE %s AND minio_key NOT LIKE %s AND id > %s "
                "ORDER BY id LIMIT %s",
                (TARGET_PREFIX + "%", EXCLUDE_PREFIX + "%",
                 lid if lid is not None else last_id, args.batch))
            return c.fetchall()
        # last_id を closure で参照するために lambda 経由 (再接続 retry 対応)
        current_last_id = last_id
        rows = db.run(lambda conn: _fetch(conn, current_last_id))
        if not rows:
            break

        items = []
        for fid, old in rows:
            last_id = fid
            nk = new_key(fid)
            if old != nk:
                items.append((fid, old, nk))
        if not items:
            _save_state(state_file, last_id, done, total)
            continue

        if not args.apply:
            for fid, old, nk in items[:5]:
                print("  %s\n    -> %s" % (old, nk), flush=True)
            done += len(items)
            _save_state(state_file, last_id, done, total)
            if args.limit and done >= args.limit:
                break
            continue

        # (1) copy を並列で
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            res = list(pool.map(do_copy, items))
        copied = [(f, o, n) for f, o, n, e, k in res if e is None]
        miss_ids = [f for f, o, n, e, k in res if k == "missing"]
        err_ids = [(f, e) for f, o, n, e, k in res if k == "error"]
        if miss_ids:
            with open(missing_file, "a", encoding="utf-8") as fh:
                for f in miss_ids:
                    fh.write(str(f) + "\n")
            missing += len(miss_ids)
        if err_ids:
            with open(error_file, "a", encoding="utf-8") as fh:
                for f, e in err_ids:
                    fh.write("%s | %s\n" % (f, e.replace("\n", " ")))
            errors += len(err_ids)
            # バッチローカルにサンプル出力 (累積カウンタで抑制しない)
            print("  一時エラー %d 件 例) id=%s: %s"
                  % (len(err_ids), err_ids[0][0], err_ids[0][1]), flush=True)
        ok_copy += len(copied)

        # (2) UPDATE を明示トランザクションで (部分成功を作らない)
        #     autocommit=True のまま executemany が中断すると「途中まで
        #     commit 済み」の不整合が残るため、必ずロールバック可能にする。
        if copied:
            def _do_update(conn):
                conn.autocommit = False
                c2 = conn.cursor()
                try:
                    c2.executemany(
                        "UPDATE crawl_face SET minio_key=%s WHERE id=%s",
                        [(n, f) for f, o, n in copied])
                    conn.commit()
                finally:
                    c2.close()
                    conn.autocommit = True
            try:
                db.run(_do_update)
                ok_upd += len(copied)
            except Exception as e:
                try:
                    db.raw.rollback()
                except Exception:
                    pass
                try:
                    db.raw.autocommit = True
                except Exception:
                    pass
                # UPDATE 失敗は abort。旧キーは残っており、新キーは孤児として
                # 残るだけ (再実行で上書きされ整合)。人間の判断へ戻す。
                _save_state(state_file, last_id, done, total)
                print("UPDATE 失敗 — 安全のため abort。last_id=%d error=%s"
                      % (last_id, str(e)[:200]), flush=True)
                print("再実行: --start-id %d" % last_id, flush=True)
                return 2

        # (3) 旧キーを一括削除 (失敗しても DB は正しい、容量が残るだけ)
        #     remove_objects は失敗のみ yield するので個別に数える。
        #     数えないと容量リークが不可視化する (10 億件では致命)。
        if copied:
            copied_old = {o: f for f, o, n in copied}
            objs = [DeleteObject(o) for o in copied_old]
            del_failed_batch = 0
            try:
                for err in m.remove_objects(BUCKET, objs):
                    # err は DeleteError: object_name / code / message を持つ
                    del_failed_batch += 1
                    try:
                        obj_name = getattr(err, "object_name", "?")
                        code = getattr(err, "code", "?")
                        msg = getattr(err, "message", "")[:100]
                    except Exception:
                        obj_name, code, msg = "?", "?", str(err)[:100]
                    with open(del_failed_file, "a", encoding="utf-8") as fh:
                        fh.write("%s | %s | %s | %s\n"
                                 % (copied_old.get(obj_name, "?"), obj_name, code, msg))
            except Exception as e:
                # remove_objects 自体が例外を投げた (通信断など)
                # → copied 全件が削除不明として del_failed に記録
                print("  旧キー削除呼び出し失敗: %s" % str(e)[:150], flush=True)
                with open(del_failed_file, "a", encoding="utf-8") as fh:
                    for o, f in copied_old.items():
                        fh.write("%s | %s | CALL_FAILED | %s\n"
                                 % (f, o, str(e)[:100]))
                del_failed_batch = len(copied_old)
            ok_del += len(copied) - del_failed_batch
            del_failed_total += del_failed_batch
            if del_failed_batch:
                print("  旧キー削除 %d 件失敗 (del_failed.txt に記録)"
                      % del_failed_batch, flush=True)

        done += len(items)
        _save_state(state_file, last_id, done, total)
        el = time.time() - t0
        print("  %d/%d  copy=%d upd=%d del=%d 実体なし=%d 一時err=%d del失敗=%d"
              "  %.0f 件/秒  last_id=%d"
              % (done, total, ok_copy, ok_upd, ok_del, missing, errors,
                 del_failed_total, done / max(el, 0.01), last_id), flush=True)

        if args.limit and done >= args.limit:
            break

    el = time.time() - t0
    print("終了: 処理=%d copy=%d upd=%d del=%d 実体なし=%d 一時err=%d del失敗=%d"
          "  %.0f 秒  %.0f 件/秒"
          % (done, ok_copy, ok_upd, ok_del, missing, errors, del_failed_total,
             el, done / max(el, 0.01)))
    print("  state       : %s" % state_file)
    print("  実体なし    : %s" % missing_file)
    print("  一時エラー  : %s  (再試行対象。削除しないこと)" % error_file)
    print("  削除失敗    : %s  (容量リーク追跡用)" % del_failed_file)
    print("再開する場合: --start-id %d  (未指定でも state から自動再開)" % last_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
