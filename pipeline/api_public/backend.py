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

## 一意制約 (実 DDL から確認済)

  crawl_image.uidx_url_sha256   … url は実質必須かつ一意 → アップロードは合成 URL
                                   `create://{site}/{sha256}` を入れる
  crawl_image.uidx_data_sha256  … 内容 dedup が無料 → 冪等 API がタダで手に入る
  crawl_image.site_file_no      … crawl_config.next_no 採番。 衝突は file_no 再採番で retry
  crawl_video.uq_source_url_sha256 … 同上 (source_url が一意)
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import uuid
from dataclasses import dataclass
from typing import Any

from pipeline.storage import StorageRegistry

log = logging.getLogger("pipeline.api_public.backend")

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
    state: str         # "queued" | "existing"


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
    エンドポイントは threadpool で並列に走るので、 DB 操作は全て `_lock` 下で直列化する。
    MinIO put は数百 ms かかりうるので **lock の外**で行う (= DB を待たせない)。
    """

    def __init__(self, *, db: Any, raw: Any, paprika: Any = None,
                 dir_base: int = DIR_BASE) -> None:
        self.db = db
        self.raw = raw
        self.paprika = paprika
        self.dir_base = dir_base
        self._lock = threading.Lock()

    @classmethod
    def from_env_file(cls, path: str | None) -> CreateBackend:
        reg = StorageRegistry.from_env_file(path)
        try:
            paprika = reg.minio("paprika")
        except RuntimeError:
            paprika = None      # PAPRIKA_MINIO_* 未設定 = 動画投入だけ無効
            log.warning("create: paprika MinIO 未設定 — 動画投入は 503 になります")
        return cls(db=reg.db(), raw=reg.minio("raw"), paprika=paprika)

    # ---------------- DB 小道具 (全て _lock 下で呼ぶ) ----------------

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

    def _alive(self) -> None:
        """各リクエストの先頭で接続を確かめる (要 _lock)。

        `Db` は long-lived な単一コネクションなので、 MariaDB 側の wait_timeout で切られると
        以降のリクエストが全部落ち続ける (`Db.ping()` が失敗時に `_conn` を None に戻すので、
        ここを通せば次アクセスで再接続される)。 プラグインの `_ensure_db_alive` と同方針。
        """
        self.db.ping()

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
        with self._lock:
            self._alive()
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
            with self._lock:
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

        with self._lock:
            self._x("UPDATE crawl_image SET download_status=NULL, downloaded_at=NOW() "
                    "WHERE id=%s", (image_id,))
        log.info("create: image_id=%s site=%s key=%s bytes=%d", image_id, site, key, len(data))
        return CreateResult(image_id, "image", dedup=False, state="queued")

    def image_status(self, image_id: int) -> dict[str, Any] | None:
        with self._lock:
            self._alive()
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

        with self._lock:
            self._alive()
            existing = self._find_video(source_url)
            if existing is not None:
                return CreateResult(existing, "video", dedup=True, state="existing")

        # storage_url は id に依存しないので put → INSERT の順にできる (画像より単純)。
        storage_key = f"create/{uuid.uuid4().hex}.{ext}"
        try:
            self.paprika.put_bytes(storage_key, data, mime or "video/mp4")
        except Exception as e:
            raise CreateError("storage_failed", f"paprika MinIO put 失敗: {e}"[:300]) from e

        with self._lock:
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
                    raise
                video_id = None  # type: ignore[assignment]
                existing = self._find_video(source_url)
        if video_id is None:
            # 並列投入に負けた → 置いた実体は誰も参照しないので回収する。
            try:
                self.paprika.remove(storage_key)
            except Exception:  # noqa: BLE001
                log.warning("create: 孤児 video オブジェクト削除失敗 key=%s", storage_key)
            if existing is None:
                raise CreateError("insert_conflict", "crawl_video INSERT が衝突")
            return CreateResult(existing, "video", dedup=True, state="existing")

        log.info("create: video_id=%s site=%s key=%s bytes=%d",
                 video_id, site, storage_key, len(data))
        return CreateResult(video_id, "video", dedup=False, state="queued")

    def video_status(self, video_id: int) -> dict[str, Any] | None:
        with self._lock:
            self._alive()
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
            "state": "done" if r[6] else ("failed" if r[3] == "failed" else "processing"),
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
