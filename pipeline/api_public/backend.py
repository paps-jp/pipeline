"""外部投入のコア (= 既存パイプラインの入口に行を作るだけの薄い層)。

## なぜ新しい embedding 経路を作らないのか

画像は `crawl_image` に「DL 済 / 未 hash / 除外なし」の行が現れた時点で、 supervisor の
`_orphan_reconcile_run` (30s tick) が `image-hash-extract` キューへ push し、 そこから
detect → `crawl_face` → `image-embed` → `embed-write` → `face-person-link` まで既存の
フリートが全部やる。 動画も `crawl_video.download_status='pending'` を video-dispatcher が
拾って `video-face-extract` に渡し、 抽出結果は `crawl_image` に登録されて画像経路に合流する。

つまり外部投入がやるべきことは **MinIO に置く + 行を 1 本作る** だけで、 新テーブル /
新プラグイン / 新モデルは一切不要。 このモジュールはその 2 手順を、 既存 plugin
(`paprika_image_pull` / `paprika_video_pull`) と**同一の契約**で行う。

## 契約 (実コードから確認済)

画像: `paprika_image_pull.image_main` と同じ二段
  1. `INSERT crawl_image (..., download_status='processing')`  ← id 採番が MinIO キーに必要
  2. raw MinIO (.17 `raw`) へ `{dir_no}/{image_id}.jpg` を put
  3. `UPDATE download_status=NULL, downloaded_at=NOW()`
  `downloaded_at IS NULL` の間は orphan_reconcile の条件に合致しないので、 2 の途中で
  落ちても中途半端な行が hash キューへ流れることはない。

動画: `paprika_video_pull.bridge_main` と同じ形
  1. paprika MinIO へ `create/{uuid}.{ext}` を put   ← storage_url を先に決められる
  2. `INSERT crawl_video (crawl_id=0, ..., download_status='pending')`
  vfe が処理後に `_delete_minio_source` で実体を消すので、 置いた動画の後始末は不要。
  ※ URL 投入 (`create_video`) の形。 multipart 投入は下の非同期版を使う。

動画 (multipart, `create_video_spooled`): 順序が逆で、 put を裏に回す
  1. 受信した body を spool へ書き出す (sha256 も同時に取る)
  2. `INSERT crawl_video (..., download_status='uploading')` → video_id を 202 で返す
  3. 裏スレッドが paprika MinIO へ put → `UPDATE download_status='pending'`
     (失敗なら 'failed')。 'uploading' は後段の誰も拾わない値なので 2〜3 の間は不可視。

## 一意制約 (実 DDL から確認済)

  crawl_image.uidx_url_sha256   … url は実質必須かつ一意 → アップロードは合成 URL
                                   `create://{site}/{sha256}` を入れる
  crawl_image.uidx_data_sha256  … 内容 dedup が無料 → 冪等 API がタダで手に入る
  crawl_image.site_file_no      … crawl_config.next_no 採番。 衝突は file_no 再採番で retry
  crawl_video.uq_source_url_sha256 … 同上 (source_url が一意)
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import queue
import shutil
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from pipeline.storage import StorageRegistry

log = logging.getLogger("pipeline.api_public.backend")

# 動画の非同期投入 (`create_video_spooled`) 用。
#
# multipart で受けた動画は、 受信完了の時点で一旦ここへ書き出して 202 を返し、 paprika
# MinIO (.8, HDD) への put は裏のスレッドで行う。 .8 は実測 1〜2MB/s しか出ないので、
# 1.5GB 級を同期で put すると応答まで 20 分を超え、 前段 nginx の proxy_read_timeout
# (600s) で 504 になっていた (2026-09-24 に 1 日 8 件。 行は後で入るので拡張からは
# 「失敗したのに登録されている」に見え、 再送で 2GB を二重に送っていた)。
#
# **再起動をまたいで残る場所を指定すること**: 裏の put が終わる前に落ちた行は
# 次回起動時に spool のファイルから put し直す (`recover_uploads`)。 PrivateTmp な
# /tmp に置くと再起動で消え、 その行は failed に落ちる。
SPOOL_DIR = os.environ.get("PIPELINE_CREATE_SPOOL_DIR", "/var/tmp/pipeline-create-spool")
UPLOAD_WORKERS = int(os.environ.get("PIPELINE_CREATE_UPLOAD_WORKERS", "2"))
# spool 先の空きがこれを割ったら受け付けない (拡張の連投で nas の / を埋めないため)。
SPOOL_MIN_FREE_BYTES = int(os.environ.get("PIPELINE_CREATE_SPOOL_MIN_FREE_BYTES",
                                          str(20 * 1024 ** 3)))
_SPOOL_CHUNK = 8 * 1024 * 1024
# 孤児 spool (行を作る前に落ちた等) を掃除する経過時間。
_SPOOL_ORPHAN_AGE_S = 24 * 3600

# 裏 put 中の crawl_video.download_status。 video-dispatcher / vfe は 'pending' / 'queued'
# しか拾わず、 video-asset-gc / video-health もこの値には触らないので、 put が終わるまで
# 後段から見えない (= 実体の無い動画を vfe が掴みに行かない)。
VIDEO_UPLOADING = "uploading"

# raw MinIO のディレクトリ分割幅。 image-hash-extract の `raw_dir_base` (既定 1000) と
# **必ず一致させる**。 ズレると put したキーを hash 側が引けず origin fallback に落ちる。
DIR_BASE = int(os.environ.get("PIPELINE_CREATE_RAW_DIR_BASE", "1000"))

ER_DUP_ENTRY = 1062


class CreateError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


@dataclass
class CreateResult:
    id: int
    kind: str          # "image" | "video"
    dedup: bool        # True = 既に取り込み済み (何も投入していない)
    state: str         # "queued" | "existing" | "uploading"


@dataclass
class Spooled:
    """受信済みアップロードを spool に書き出したもの (`CreateBackend.spool`)。"""
    token: str         # spool ファイル名 = MinIO キーの uuid 部分 (再起動後の突合に使う)
    path: Path
    size: int
    sha256: bytes
    head: bytes        # 先頭 64 バイト (形式判別用)
    truncated: bool    # max_bytes を超えたので途中で読むのをやめた

    def discard(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _errno(e: Exception) -> int | None:
    """mariadb / pymysql の双方から MySQL errno を取り出す。"""
    n = getattr(e, "errno", None)
    if isinstance(n, int):
        return n
    args = getattr(e, "args", ())
    if args and isinstance(args[0], int):
        return args[0]
    return None


def raw_object_key(image_id: int, dir_base: int = DIR_BASE) -> str:
    """`image_hash_extract._fetch_raw_minio` と同一のキー算出。"""
    dir_no = ((image_id - 1) // dir_base + 1) * dir_base
    return f"{dir_no}/{image_id}.jpg"


def site_for(user_slug: str) -> str:
    """API キーの所有者 → 投入 site 名。

    `site='create'` 単一にせずテナント毎に分けるのは、 `crawl_image.site` に index があり
    フロー UI / GC / 削除系が site 単位で動くため。 外部投入分だけを数える・止める・消すが
    後から SQL 1 本でできる。 `crawl_video.site` が varchar(64) なので長さを切る。
    """
    safe = "".join(c if (c.isalnum() or c in "-_") else "-" for c in (user_slug or "anon"))
    return f"create-{safe}"[:64]


class CreateBackend:
    """MariaDB (.20) + raw MinIO (.17) + paprika MinIO への薄いアダプタ。

    `pipeline.storage.Db` は単一コネクションでスレッド安全でない。 control plane の sync
    エンドポイントは threadpool で並列に走るので、 DB 操作は全て `_db()` 区間 (= `_lock`)
    の下で直列化する。 MinIO put は数分かかりうるので **区間の外**で行う。

    **接続を MinIO put をまたいで持ち越さない** のがこのクラスの不変条件。 1 リクエストは
    「DB → put → DB」の形をしていて、 put の間 DB 接続は完全にアイドルになる。 MariaDB の
    `wait_timeout` (実測 600s) を超えると向こうから切られるので、 持ち越した接続で 2 回目の
    DB を叩くと `2006 MySQL server has gone away` になる。 API 的には item error
    `internal` になり、 置いた実体だけが MinIO に残る。 2026-09-16 に 1.5GiB の投入で再現
    (put に 1382s = 2.24MB/s しか出ない .8 の MinIO なら 660MB 程度から射程に入る)。
    `_db()` は区間を出るたびに接続を捨てるので、 次の区間は必ず張り直した接続で始まる。
    """

    def __init__(self, *, db: Any, raw: Any, paprika: Any = None,
                 dir_base: int = DIR_BASE, spool_dir: str | Path = SPOOL_DIR,
                 upload_workers: int = UPLOAD_WORKERS,
                 spool_min_free_bytes: int = SPOOL_MIN_FREE_BYTES) -> None:
        self.db = db
        self.raw = raw
        self.paprika = paprika
        self.dir_base = dir_base
        self._lock = threading.Lock()
        self.spool_dir = Path(spool_dir)
        self.spool_min_free_bytes = spool_min_free_bytes
        self._upload_workers = max(1, upload_workers)
        self._uploads: queue.Queue[tuple[int, str, Path, str]] = queue.Queue()
        self._upload_threads: list[threading.Thread] = []
        # 裏 put を持っている video_id → spool ファイル名。 recover_uploads が同じ行を
        # 二重に拾わない / 処理待ちの spool を孤児と誤認して消さないため。
        # 追加は INSERT と同じ _db() 区間 (= 同じ lock) で行う。
        self._inflight: dict[int, str] = {}

    @classmethod
    def from_env_file(cls, path: str | None) -> CreateBackend:
        reg = StorageRegistry.from_env_file(path)
        try:
            paprika = reg.minio("paprika")
        except RuntimeError:
            paprika = None      # PAPRIKA_MINIO_* 未設定 = 動画投入だけ無効
            log.warning("create: paprika MinIO 未設定 — 動画投入は 503 になります")
        return cls(db=reg.db(), raw=reg.minio("raw"), paprika=paprika)

    # ---------------- DB 小道具 (全て _db() 区間の中で呼ぶ) ----------------

    @contextmanager
    def _db(self) -> Iterator[None]:
        """DB を触る区間。 lock を取り、 区間を出るときに接続を捨てる。

        捨てるのは「長い MinIO put をまたいで接続を持ち越さない」ため (クラス docstring)。
        張り直しは実測 13ms で、 この API の流量 (1 リクエスト数件) なら無視できる。
        区間の中で例外が出ても閉じるので、 壊れた接続が次のリクエストに漏れない。
        """
        with self._lock:
            try:
                yield
            finally:
                self.db.close()

    def _q(self, sql: str, params: tuple = ()) -> list:
        cur = self.db.execute(sql, params)
        try:
            return cur.fetchall()
        finally:
            cur.close()

    def _x(self, sql: str, params: tuple = ()) -> Any:
        """INSERT/UPDATE を実行し (rowcount, lastrowid) を返す。"""
        cur = self.db.execute(sql, params)
        try:
            return cur.rowcount, cur.lastrowid
        finally:
            cur.close()

    # ---------------- crawl_config / file_no ----------------

    def _ensure_crawl_config(self, site: str) -> None:
        """投入 site の crawl_config 行を用意 (無いと file_no を採番できない)。

        `enabled=0` で作る = レガシー crawl.py のフリートがこの site を巡回対象に
        しないようにする (外部投入専用 site なので巡回する URL は無い)。
        動画は file_no を持たないのでこの採番は画像経路だけが使う。
        """
        rows = self._q("SELECT id FROM crawl_config WHERE site=%s", (site,))
        if rows:
            return
        try:
            self._x(
                "INSERT INTO crawl_config (site, next_no, type, enabled, memo) "
                "VALUES (%s, 1, 'image', 0, 'external create API')",
                (site,),
            )
        except Exception as e:
            if _errno(e) != ER_DUP_ENTRY:
                raise
        log.info("create: crawl_config 行を作成 site=%s", site)

    def _next_file_no(self, site: str) -> int:
        """`image_main._next_file_no` 移植。 UPDATE で inc → SELECT で新値 → -1 が採番値。"""
        rc, _ = self._x("UPDATE crawl_config SET next_no = next_no + 1 WHERE site=%s", (site,))
        if rc == 0:
            raise CreateError("no_crawl_config", f"crawl_config に site={site} が無い")
        rows = self._q("SELECT next_no FROM crawl_config WHERE site=%s", (site,))
        if not rows:
            raise CreateError("no_crawl_config", f"crawl_config に site={site} が無い")
        return int(rows[0][0]) - 1

    # ---------------- 画像 ----------------

    def _find_image(self, store_url: str, sha: bytes) -> int | None:
        rows = self._q(
            "SELECT id FROM crawl_image "
            "WHERE data_sha256=%s OR url_sha256 = UNHEX(SHA2(%s, 256)) LIMIT 1",
            (sha, store_url),
        )
        return int(rows[0][0]) if rows else None

    def _insert_image(self, store_url: str, site: str, file_no: int,
                      sha: bytes, ext: str | None,
                      page_url: str | None = None) -> int | None:
        """crawl_image INSERT。 1062 (= 重複) は None。 それ以外は送出。

        page_url は出所ページ URL の記録用 (crawl_video と同じ varchar(2048))。 画像経路の
        下流 (hash → face → embed) はこの列を見ないので、 純粋にプロベナンス保存のための列。
        """
        try:
            _, last = self._x(
                "INSERT INTO crawl_image "
                "(url, crawl_ids, site, file_no, ext, download_status, data_sha256, page_url) "
                "VALUES (%s, '0', %s, %s, %s, 'processing', %s, %s)",
                (store_url, site, file_no, (ext or "")[:6] or None, sha,
                 (page_url or "")[:2048] or None),
            )
            if not last:
                # lastrowid が取れないと MinIO キーを組めない (= 実体の置き場が決まらない)。
                raise CreateError("no_insert_id", "crawl_image の lastrowid が取得できない")
            return int(last)
        except Exception as e:
            if _errno(e) == ER_DUP_ENTRY:
                return None
            raise

    def create_image(self, *, data: bytes, url: str | None, site: str,
                     ext: str | None, page_url: str | None = None) -> CreateResult:
        sha = hashlib.sha256(data).digest()
        # アップロードには URL が無いが url_sha256 が UNIQUE なので合成する。 URL 投入時は
        # 実 URL をそのまま入れる → 既存クローラーが取った同じ URL と自然に dedup される。
        store_url = url or f"create://{site}/{sha.hex()}"

        image_id: int | None = None
        with self._db():
            existing = self._find_image(store_url, sha)
            if existing is not None:
                return CreateResult(existing, "image", dedup=True, state="existing")
            self._ensure_crawl_config(site)
            # site_file_no UNIQUE との衝突 (= 他プロセスと next_no が競合) は内容重複では
            # ないので、 file_no を採番し直して retry する。 内容重複なら再 SELECT で当たる。
            for _ in range(3):
                file_no = self._next_file_no(site)
                image_id = self._insert_image(store_url, site, file_no, sha, ext, page_url)
                if image_id is not None:
                    break
                existing = self._find_image(store_url, sha)
                if existing is not None:
                    return CreateResult(existing, "image", dedup=True, state="existing")
            if image_id is None:
                raise CreateError("insert_conflict", "crawl_image INSERT が 3 回衝突")

        key = raw_object_key(image_id, self.dir_base)
        try:
            self.raw.put_bytes(key, data, "image/jpeg")
        except Exception as e:
            # 実体を置けなかった行は **消す** (= リクエストを丸ごと巻き戻す)。
            # ignore_reason を立てて残すだけでは駄目: 行が残ると data_sha256 UNIQUE で
            # 再投入が永久に dedup ヒットし、 MinIO に実体が無い行を指し続けて
            # 「取り込めたことになっているが embedding は永遠に来ない」状態になる。
            #
            # 消して安全な根拠: この行は今このリクエストが INSERT したもので、
            # downloaded_at が NULL のまま = orphan_reconcile の条件に一度も合致しておらず、
            # hash キューに入っていない → crawl_face も参照も無い。 採番済 file_no を
            # 1 つ捨てるだけで済む。
            with self._db():
                try:
                    self._x("DELETE FROM crawl_image WHERE id=%s "
                            "AND download_status='processing' AND downloaded_at IS NULL",
                            (image_id,))
                except Exception:  # noqa: BLE001
                    # 消せなかった場合だけ、 少なくとも「投入失敗」と分かる印を残す
                    # (この行は再投入を dedup で塞ぐので、 運用で拾えるようにする)。
                    log.exception("create: 巻き戻し DELETE 失敗 image_id=%s", image_id)
                    try:
                        self._x("UPDATE crawl_image SET ignore_reason='create_upload_failed' "
                                "WHERE id=%s AND download_status='processing'", (image_id,))
                    except Exception:  # noqa: BLE001
                        log.exception("create: 失敗マークにも失敗 image_id=%s", image_id)
            raise CreateError("storage_failed", f"raw MinIO put 失敗: {e}"[:300]) from e

        with self._db():
            self._x("UPDATE crawl_image SET download_status=NULL, downloaded_at=NOW() "
                    "WHERE id=%s", (image_id,))
        log.info("create: image_id=%s site=%s key=%s bytes=%d", image_id, site, key, len(data))
        return CreateResult(image_id, "image", dedup=False, state="queued")

    def image_status(self, image_id: int) -> dict[str, Any] | None:
        with self._db():
            rows = self._q(
                "SELECT id, site, url, hash_id, download_status, downloaded_at, ignore_reason, "
                "       page_url "
                "FROM crawl_image WHERE id=%s", (image_id,))
            if not rows:
                return None
            r = rows[0]
            faces = self._faces_for(image_col="image_id", value=image_id)
        return {
            "image_id": int(r[0]),
            "site": r[1],
            "url": r[2],
            "hash_id": int(r[3]) if r[3] is not None else None,
            "download_status": r[4],
            "downloaded_at": str(r[5]) if r[5] else None,
            "ignore_reason": r[6],
            "page_url": r[7],
            "state": _image_state(r),
            "faces": faces,
        }

    def _faces_for(self, *, image_col: str, value: int) -> list[dict[str, Any]]:
        """crawl_face を image_id / video_id で引いて外部向けに整形 (要 _lock)。"""
        # image_col は呼出側が渡す固定リテラル ("image_id" / "video_id") のみ。 外部入力は
        # value 側だけで、 そちらはプレースホルダに載せている。
        rows = self._q(
            "SELECT id, adaface_ready, adaface_norm, det_score, "
            "       bbox_x1, bbox_y1, bbox_x2, bbox_y2, person_id, minio_key "
            f"FROM crawl_face WHERE {image_col}=%s ORDER BY id",
            (value,))
        return [
            {
                "face_id": int(f[0]),
                # adaface_ready: 0=未計算 / 1=計算済 / 2=対象外 (低品質 crop 等で落選)
                "adaface_ready": int(f[1] or 0),
                "embedded": int(f[1] or 0) == 1,
                "adaface_norm": f[2],
                "det_score": f[3],
                "bbox": [f[4], f[5], f[6], f[7]],
                "person_id": int(f[8]) if f[8] is not None else None,
                "crop_minio_key": f[9],
            }
            for f in rows
        ]

    # ---------------- 動画 ----------------

    def _find_video(self, source_url: str) -> int | None:
        rows = self._q(
            "SELECT id FROM crawl_video WHERE source_url_sha256 = UNHEX(SHA2(%s, 256)) LIMIT 1",
            (source_url,))
        return int(rows[0][0]) if rows else None

    def create_video(self, *, data: bytes, url: str | None, site: str,
                     ext: str | None, mime: str | None = None,
                     page_url: str | None = None) -> CreateResult:
        if self.paprika is None:
            raise CreateError("video_unavailable",
                              "PAPRIKA_MINIO_* 未設定のため動画投入は無効")
        sha = hashlib.sha256(data).digest()
        source_url = url or f"create://{site}/{sha.hex()}"
        ext = (ext or "mp4")[:8]

        with self._db():
            existing = self._find_video(source_url)
            if existing is not None:
                return CreateResult(existing, "video", dedup=True, state="existing")

        # storage_url は id に依存しないので put → INSERT の順にできる (画像より単純)。
        storage_key = f"create/{uuid.uuid4().hex}.{ext}"
        try:
            self.paprika.put_bytes(storage_key, data, mime or "video/mp4")
        except Exception as e:
            raise CreateError("storage_failed", f"paprika MinIO put 失敗: {e}"[:300]) from e

        video_id: int | None = None
        insert_error: Exception | None = None
        with self._db():
            try:
                _, last = self._x(
                    "INSERT INTO crawl_video "
                    "(crawl_id, site, source_url, storage_url, page_url, mime, ext, "
                    " size_bytes, download_status) "
                    "VALUES (0, %s, %s, %s, %s, %s, %s, %s, 'pending')",
                    (site, source_url, storage_key, (page_url or "")[:2048] or None,
                     (mime or "")[:64] or None, ext, len(data)),
                )
                video_id = int(last)
            except Exception as e:
                if _errno(e) != ER_DUP_ENTRY:
                    # 重複以外の失敗 (接続断 / ENOSPC など)。 ここで送出を遅らせるのは、
                    # **どの理由で入らなかった場合も置いた実体を回収する**ため。 行が無い
                    # オブジェクトは誰からも参照されず、 GC も持っていかない
                    # (2026-09-15 までに 17 個 29GB が滞留していた)。
                    insert_error = e
                else:
                    existing = self._find_video(source_url)
        if video_id is None:
            # 行が作れなかった → 置いた実体は誰も参照しないので回収する。
            try:
                self.paprika.remove(storage_key)
            except Exception:  # noqa: BLE001
                log.warning("create: 孤児 video オブジェクト削除失敗 key=%s", storage_key)
            if insert_error is not None:
                raise insert_error
            if existing is None:
                raise CreateError("insert_conflict", "crawl_video INSERT が衝突")
            return CreateResult(existing, "video", dedup=True, state="existing")

        log.info("create: video_id=%s site=%s key=%s bytes=%d",
                 video_id, site, storage_key, len(data))
        return CreateResult(video_id, "video", dedup=False, state="queued")

    # ---------------- 動画 (非同期: spool → 202 → 裏で put) ----------------

    def spool(self, src: BinaryIO, *, max_bytes: int) -> Spooled:
        """受信済みのアップロードを spool へ書き出しつつ sha256 を取る。

        2GB を bytes でメモリに載せない (同期版の `create_video` は載せていた)。
        `max_bytes` を超えた時点で読むのをやめ `truncated=True` で返す。
        """
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(self.spool_dir).free
        if free < self.spool_min_free_bytes:
            raise CreateError("spool_full",
                              f"spool 先の空き {free >> 30}GiB < "
                              f"{self.spool_min_free_bytes >> 30}GiB (裏の put 待ちが詰まっている)")
        token = uuid.uuid4().hex
        path = self.spool_dir / f"{token}.part"
        h = hashlib.sha256()
        head = b""
        size = 0
        truncated = False
        try:
            with open(path, "wb") as out:
                while True:
                    chunk = src.read(_SPOOL_CHUNK)
                    if not chunk:
                        break
                    if size + len(chunk) > max_bytes:
                        truncated = True
                        break
                    if len(head) < 64:
                        head += chunk[:64 - len(head)]
                    h.update(chunk)
                    out.write(chunk)
                    size += len(chunk)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            raise
        return Spooled(token, path, size, h.digest(), head, truncated)

    def create_video_spooled(self, sp: Spooled, *, url: str | None, site: str,
                             ext: str | None, mime: str | None = None,
                             page_url: str | None = None) -> CreateResult:
        """spool 済みの動画を 'uploading' で行登録し、 paprika MinIO への put を裏へ回す。

        **`sp` の所有権はここで引き取る** (dedup / 失敗なら消す、 受理なら裏の put が
        終わった後に消す)。 呼び出し側は以後 `sp` に触らない。

        行を put より**先に**作るのは同期版と逆。 202 で video_id を返すには行が要るため。
        'uploading' の間は後段から見えないので、 実体の無い行を vfe が掴むことはない。
        """
        handed_off = False
        try:
            if self.paprika is None:
                raise CreateError("video_unavailable",
                                  "PAPRIKA_MINIO_* 未設定のため動画投入は無効")
            source_url = url or f"create://{site}/{sp.sha256.hex()}"
            ext = (ext or "mp4")[:8]
            # uuid を spool 名と揃える = 再起動後に storage_url から spool を引ける。
            storage_key = f"create/{sp.token}.{ext}"
            content_type = mime or "video/mp4"

            with self._db():
                existing = self._find_video(source_url)
                if existing is not None:
                    return CreateResult(existing, "video", dedup=True, state="existing")
                try:
                    _, last = self._x(
                        "INSERT INTO crawl_video "
                        "(crawl_id, site, source_url, storage_url, page_url, mime, ext, "
                        " size_bytes, download_status) "
                        "VALUES (0, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (site, source_url, storage_key, (page_url or "")[:2048] or None,
                         (mime or "")[:64] or None, ext, sp.size, VIDEO_UPLOADING),
                    )
                except Exception as e:
                    if _errno(e) != ER_DUP_ENTRY:
                        raise
                    existing = self._find_video(source_url)
                    if existing is None:
                        raise CreateError("insert_conflict",
                                          "crawl_video INSERT が衝突") from e
                    return CreateResult(existing, "video", dedup=True, state="existing")
                video_id = int(last)
                self._inflight[video_id] = sp.path.name

            self._enqueue_upload(video_id, storage_key, sp.path, content_type)
            handed_off = True
            log.info("create: video_id=%s site=%s key=%s bytes=%d (受理、 put は裏で)",
                     video_id, site, storage_key, sp.size)
            return CreateResult(video_id, "video", dedup=False, state=VIDEO_UPLOADING)
        finally:
            if not handed_off:
                sp.discard()

    def _enqueue_upload(self, video_id: int, key: str, path: Path, content_type: str) -> None:
        self._ensure_upload_workers()
        self._uploads.put((video_id, key, path, content_type))

    def _ensure_upload_workers(self) -> None:
        # daemon にするのは、 ThreadPoolExecutor だと終了時に put の完了 (数十分) を
        # 待って systemd の stop timeout で SIGKILL されるため。 途中で死んだ put は
        # 次回起動の recover_uploads が spool から拾い直す。
        with self._lock:
            self._upload_threads = [t for t in self._upload_threads if t.is_alive()]
            while len(self._upload_threads) < self._upload_workers:
                t = threading.Thread(target=self._upload_loop, daemon=True,
                                     name=f"create-upload-{len(self._upload_threads)}")
                t.start()
                self._upload_threads.append(t)

    def _upload_loop(self) -> None:
        while True:
            job = self._uploads.get()
            try:
                self._upload_one(*job)
            except Exception:  # noqa: BLE001 (ワーカーを殺さない)
                log.exception("create: 裏 put の後処理で例外 video_id=%s", job[0])
            finally:
                self._uploads.task_done()

    def _upload_one(self, video_id: int, key: str, path: Path, content_type: str) -> None:
        t0 = time.monotonic()
        try:
            try:
                self.paprika.put_file(key, str(path), content_type)
            except Exception as e:  # noqa: BLE001
                log.warning("create: 裏 put 失敗 video_id=%s key=%s: %s", video_id, key, e)
                with contextlib.suppress(Exception):
                    self.paprika.remove(key)       # 途中まで書かれた実体があれば回収
                self._set_video_status(
                    video_id, "failed", f"create: paprika MinIO put 失敗: {e}"[:1000])
                path.unlink(missing_ok=True)
                return
            # 'pending' へ倒せなかった (DB 断など) ときは spool を残す: 行は 'uploading'
            # のままなので、 次回起動の recover_uploads が実体の存在を見て pending にする。
            if self._set_video_status(video_id, "pending", None):
                path.unlink(missing_ok=True)
            log.info("create: 裏 put 完了 video_id=%s key=%s %.0fs",
                     video_id, key, time.monotonic() - t0)
        finally:
            with self._lock:
                self._inflight.pop(video_id, None)

    def _set_video_status(self, video_id: int, status: str, error: str | None,
                          *, attempts: int = 3) -> bool:
        """'uploading' からの遷移。 put 後の数十分で DB が一時的に切れていても諦めない。"""
        for i in range(attempts):
            try:
                with self._db():
                    self._x("UPDATE crawl_video SET download_status=%s, error_message=%s "
                            "WHERE id=%s AND download_status=%s",
                            (status, error, video_id, VIDEO_UPLOADING))
                return True
            except Exception as e:  # noqa: BLE001
                log.warning("create: crawl_video %s → %s 失敗 (%d/%d): %s",
                            video_id, status, i + 1, attempts, e)
                time.sleep(2 * (i + 1))
        return False

    def recover_uploads(self) -> dict[str, int]:
        """前のプロセスが put し終える前に落ちた 'uploading' 行を片付ける。

        - spool が残っている → put し直す (put は同じキーへの上書きなので冪等)
        - spool は無いが実体が MinIO にある → put は完了していた (S3 の put は完了時に
          初めて見える) → 'pending' へ
        - どちらも無い → 取り戻せない → 'failed'
        あわせて、 どの行からも参照されない古い spool (行を作る前に落ちた等) を消す。
        """
        out = {"resumed": 0, "pending": 0, "failed": 0, "orphan_spool": 0}
        if self.paprika is None:
            return out
        with self._db():
            rows = self._q("SELECT id, storage_url, mime FROM crawl_video "
                           "WHERE download_status=%s", (VIDEO_UPLOADING,))
            rows = [r for r in rows if int(r[0]) not in self._inflight]
            for vid, key, _ in rows:
                self._inflight[int(vid)] = f"{Path(str(key or '')).stem}.part"
            referenced = set(self._inflight.values())
        for vid, key, mime in rows:
            vid = int(vid)
            path = self.spool_dir / f"{Path(str(key or '')).stem}.part"
            if key and path.exists():
                self._enqueue_upload(vid, key, path, mime or "video/mp4")
                out["resumed"] += 1
                continue
            try:
                present = bool(key) and self.paprika.exists(key)
            except Exception as e:  # noqa: BLE001 (判定不能 → 触らない。 次回起動で再判定)
                log.warning("create: recover 判定不能 video_id=%s: %s", vid, e)
                present = None
            if present is True:
                ok = self._set_video_status(vid, "pending", None)
                out["pending"] += int(ok)
            elif present is False:
                ok = self._set_video_status(
                    vid, "failed", "create: put 完了前に再起動し spool も失われた")
                out["failed"] += int(ok)
            with self._lock:
                self._inflight.pop(vid, None)
        if self.spool_dir.is_dir():
            now = time.time()
            for p in self.spool_dir.glob("*.part"):
                if p.name in referenced:
                    continue
                with contextlib.suppress(FileNotFoundError):
                    if now - p.stat().st_mtime > _SPOOL_ORPHAN_AGE_S:
                        p.unlink()
                        out["orphan_spool"] += 1
        if any(out.values()):
            log.info("create: recover_uploads %s", out)
        return out

    def video_status(self, video_id: int) -> dict[str, Any] | None:
        with self._db():
            rows = self._q(
                "SELECT id, site, source_url, download_status, face_count, "
                "       representative_image_ids, processed_at, error_message "
                "FROM crawl_video WHERE id=%s", (video_id,))
            if not rows:
                return None
            faces = self._faces_for(image_col="video_id", value=video_id)
        r = rows[0]
        reps: list[int] = []
        if r[5]:
            reps = [int(x) for x in str(r[5]).replace(" ", "").split(",") if x.isdigit()]
        return {
            "video_id": int(r[0]),
            "site": r[1],
            "source_url": r[2],
            "download_status": r[3],
            "face_count": int(r[4]) if r[4] is not None else None,
            "representative_image_ids": reps,
            "processed_at": str(r[6]) if r[6] else None,
            "error": r[7],
            "state": ("done" if r[6] else "failed" if r[3] == "failed"
                      else "uploading" if r[3] == VIDEO_UPLOADING else "processing"),
            "faces": faces,
        }


def _image_state(row: Any) -> str:
    """crawl_image の列から外部に見せる state を組み立てる (新テーブルを持たない代償)。"""
    hash_id, download_status, downloaded_at, ignore_reason = row[3], row[4], row[5], row[6]
    if ignore_reason:
        return "excluded"
    if download_status == "processing" or not downloaded_at:
        return "uploading"
    if hash_id is None:
        return "queued"          # orphan_reconcile が hash キューへ push 待ち / 処理中
    return "hashed"              # detect 済 → faces[] の embedded を見る
