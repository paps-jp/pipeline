"""storage_capacity: MinIO 各ストアの使用量を収集して MariaDB に持つ。

- server の `_storage_capacity_loop` が 60s 毎に `collect_and_upsert()` を呼ぶ。
- flow の tank ノードは `metric_sql` でこの表を引くだけ (= snapshot API は MinIO を
  一切叩かない)。 **この分離は必須**: 2026-07-20 に .47 が「TCP は受けるが HTTP 無応答」
  のハングを起こしており、 snapshot 内で同期呼び出しをしているとダッシュボードごと
  固まる ([[minio-47-ramdisk-hang]])。

収集は MinIO の **admin API** (`MinioAdmin.info()`) を使う。 認証なしの Prometheus
エンドポイント (`/minio/v2/metrics/*`) は 403 で使えなかった。 admin API は通常の
access/secret key で署名すれば通る。

tank は `pct_used` (0-100) を読み、 `capacity_warn: 100` と組で使う想定。 総容量が
変わっても YAML を直さずに済む。
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

_DEFAULT_DB_ENV = "/opt/pipeline/.env"

# 収集対象。 (name, endpoint を引く env key, 既定 endpoint, access/secret の env key)
# name は flow_layout.yaml の metric_sql が参照するキーなので変えないこと。
_TARGETS = [
    ("image-ram-47", "MINIO_HUB_ENDPOINT", "192.0.2.47:9000",
     "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"),
    ("video-ram-48", "PAPRIKA_MINIO_VIDEO_ENDPOINT", "192.0.2.48:9000",
     "PAPRIKA_MINIO_ACCESS_KEY", "PAPRIKA_MINIO_SECRET_KEY"),
    ("raw-17", "MINIO_RAW_ENDPOINT", "192.0.2.17:9000",
     "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"),
]

# 1 ストアあたりの上限。 ハング中のストアで収集全体が止まらないようにする
# (既定の minio-py は connect/read とも 300 秒)。
_CONNECT_TIMEOUT_S = 3.0
_READ_TIMEOUT_S = 10.0


def _load_env() -> dict[str, str]:
    path = Path(os.environ.get("PAPRIKA_FLOW_DB_ENV") or _DEFAULT_DB_ENV)
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _fetch_one(endpoint: str, access: str, secret: str) -> tuple[int, int, int, int]:
    """(total, used, free) bytes と オブジェクト数。 取れなければ例外。"""
    import urllib3
    from minio import MinioAdmin
    from minio.credentials import StaticProvider

    http = urllib3.PoolManager(
        num_pools=2, maxsize=4,
        timeout=urllib3.Timeout(connect=_CONNECT_TIMEOUT_S, read=_READ_TIMEOUT_S),
        retries=urllib3.Retry(total=None, connect=1, read=False, status=1, other=0,
                              backoff_factor=0.2),
    )
    admin = MinioAdmin(endpoint=endpoint, credentials=StaticProvider(access, secret),
                       secure=False, http_client=http)
    info = json.loads(admin.info())
    total = used = free = 0
    for server in info.get("servers") or []:
        for drive in server.get("drives") or []:
            total += int(drive.get("totalspace") or 0)
            used += int(drive.get("usedspace") or 0)
            free += int(drive.get("availspace") or 0)
    objects = int(((info.get("objects") or {}).get("count")) or 0)
    return total, used, free, objects


class StorageCapacityRepository:
    """収集は MariaDB へ直接書く (control plane の SQLite ではなく)。

    tank の `metric_sql` は MariaDB に対して実行される (`flow._exec_tanks`) ので、
    書き先も MariaDB でなければ読めない。
    """

    @staticmethod
    def collect_and_upsert() -> int:
        """全ターゲットを収集して upsert。 戻り値 = 成功した件数。

        1 つが落ちても他は続行する (ハング中のストアがあっても残りは更新される)。
        **失敗したストアは行を更新しない** = `updated_at` が古いまま残る。 tank 側は
        その stale を検知して警告表示する (黙って古い値を出すと今日のような
        「見えているのに死んでいる」 状態を再現してしまう)。
        """
        env = _load_env()
        if not all(env.get(k) for k in ("DB_HOST", "DB_USER", "DB_PASS", "DB_NAME")):
            log.debug("storage_capacity: db env missing; skip")
            return 0

        rows = []
        for name, ep_key, ep_default, ak_key, sk_key in _TARGETS:
            endpoint = (env.get(ep_key) or ep_default).strip()
            access, secret = env.get(ak_key), env.get(sk_key)
            if not (endpoint and access and secret):
                log.debug("storage_capacity: creds missing for %s; skip", name)
                continue
            try:
                total, used, free, objects = _fetch_one(endpoint, access, secret)
            except Exception as e:
                # ハング/停止は想定内。 stack trace は出さず 1 行で残す。
                log.warning("storage_capacity: %s (%s) 収集失敗: %s",
                            name, endpoint, str(e)[:120])
                continue
            if total <= 0:
                continue
            rows.append((name, endpoint, total, used, free,
                         int(round(used * 100.0 / total)), objects))

        if not rows:
            return 0

        try:
            import mariadb  # type: ignore
            conn = mariadb.connect(
                host=env["DB_HOST"], port=int(env.get("DB_PORT", "3306")),
                user=env["DB_USER"], password=env["DB_PASS"],
                database=env["DB_NAME"], connect_timeout=3,
            )
        except ImportError:
            import pymysql  # type: ignore
            conn = pymysql.connect(
                host=env["DB_HOST"], port=int(env.get("DB_PORT", "3306")),
                user=env["DB_USER"], password=env["DB_PASS"],
                database=env["DB_NAME"], connect_timeout=3, autocommit=True,
            )
        try:
            cur = conn.cursor()
            # updated_at は **DB の NOW(6)** で打つ。 Python 側の UTC を渡すと、
            # tank の鮮度判定 (`updated_at > NOW() - INTERVAL 5 MINUTE`) が
            # 常に false になる: この MariaDB は time_zone=SYSTEM = JST なので
            # NOW() は JST を返し、 UTC の値と比較すると 9 時間ズレて必ず stale 扱い
            # → タンクが常時 overflow 表示になる (2026-07-21 実装時に検出)。
            # 書きと読みを両方 DB 時刻に揃えれば TZ 設定にも Python 側の時計ズレにも
            # 影響されない。
            # 正味流量は「前回サンプルとの差 / 経過秒」。 ON DUPLICATE KEY UPDATE の
            # 代入は **左から順に評価される** ので、 net_* を先に計算してから
            # objects_count / used_bytes / updated_at を上書きする (順序を入れ替えると
            # 常に 0 になる)。
            #
            # 間隔が 1 秒未満 or 600 秒超のときは 0 にする: 前者は 0 除算、 後者は
            # 収集が止まっていた区間 (ストア停止/再起動) を巨大な流量として誤表示しないため。
            # 初回 (列追加直後 / 新ストア追加直後) は前値が 0 なので、 差分が
            # 「全件」 になって桁外れの流量を出す。 前値が 0 の間は 0 のままにする
            # (実測: 列追加直後に net_obj/min=478347 という値が出た)。
            _gap = "TIMESTAMPDIFF(SECOND, updated_at, NOW(6))"
            _ok = f"({_gap} BETWEEN 1 AND 600 AND used_bytes > 0)"
            for r in rows:
                cur.execute(
                    "INSERT INTO storage_capacity"
                    " (name, endpoint, total_bytes, used_bytes, free_bytes,"
                    "  pct_used, objects_count, updated_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(6))"
                    " ON DUPLICATE KEY UPDATE"
                    f"   net_objects_per_min=IF({_ok},"
                    f"     (VALUES(objects_count)-objects_count)*60.0/{_gap}, 0),"
                    f"   net_bytes_per_min=IF({_ok},"
                    f"     (VALUES(used_bytes)-used_bytes)*60.0/{_gap}, 0),"
                    "   endpoint=VALUES(endpoint), total_bytes=VALUES(total_bytes),"
                    "   used_bytes=VALUES(used_bytes), free_bytes=VALUES(free_bytes),"
                    "   pct_used=VALUES(pct_used), objects_count=VALUES(objects_count),"
                    "   updated_at=NOW(6)",
                    r,
                )
            conn.commit()
            cur.close()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return len(rows)
