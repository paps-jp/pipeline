"""型付きストレージラッパー (= Airflow Hook / Prefect Block / Dagster Resource 相当)。

- 接続ライフサイクル (lazy connect / reconnect) + retry + `.healthy()` を内蔵。
- creds は StorageRegistry が env から解決して注入 (プラグインはハードコードしない)。
- minio / mariadb ドライバは **メソッド内で lazy import** — 未インストール環境
  (control plane / test) でも本モジュール自体は import できる。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class HealthStatus:
    """1 接続の死活プローブ結果 (= ストレージ監視の単位)。"""
    name: str                    # "minio:raw" / "db:main"
    kind: str                    # "minio" | "db"
    ok: bool
    latency_ms: int
    endpoint: str | None = None
    error: str | None = None


def _retry(fn: Callable[..., Any], *args: Any, retries: int = 2, **kwargs: Any) -> Any:
    """transient エラーに指数バックオフで retry。最後の例外を再送。"""
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 (ドライバ例外は多岐 → 一律 retry)
            last = e
            if attempt < retries:
                time.sleep(0.3 * (attempt + 1))
    assert last is not None
    raise last


class MinioStore:
    """MinIO 接続の型付きラッパー。"""

    def __init__(self, name: str, *, endpoint: str | None, access_key: str | None,
                 secret_key: str | None, bucket: str, secure: bool = False,
                 retries: int = 2) -> None:
        self.name = name
        self.endpoint = endpoint
        self.bucket = bucket
        self._access = access_key
        self._secret = secret_key
        self._secure = secure
        self._retries = retries
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            from minio import Minio  # lazy
            self._client = Minio(self.endpoint, access_key=self._access,
                                 secret_key=self._secret, secure=self._secure)
        return self._client

    # ---- I/O (= 既存プラグインの put/get/exists を統一) ----
    def put_file(self, key: str, path: str, content_type: str = "application/octet-stream") -> Any:
        return _retry(self.client.fput_object, self.bucket, key, str(path),
                      content_type=content_type, retries=self._retries)

    def get_to_file(self, key: str, path: str) -> Any:
        return _retry(self.client.fget_object, self.bucket, key, str(path), retries=self._retries)

    def exists(self, key: str) -> bool:
        try:
            _retry(self.client.stat_object, self.bucket, key, retries=self._retries)
            return True
        except Exception:
            return False

    def remove(self, key: str) -> Any:
        return _retry(self.client.remove_object, self.bucket, key, retries=self._retries)

    # ---- 死活 ----
    def healthy(self) -> HealthStatus:
        """bucket 存在確認 = 軽量 reachability プローブ (Connection refused 等を即検出)。"""
        t0 = time.monotonic()
        try:
            _retry(self.client.bucket_exists, self.bucket, retries=0)
            return HealthStatus(self.name, "minio", True,
                                int((time.monotonic() - t0) * 1000), self.endpoint)
        except Exception as e:  # noqa: BLE001
            self._client = None  # 壊れた client を捨てて次回再生成
            return HealthStatus(self.name, "minio", False,
                                int((time.monotonic() - t0) * 1000), self.endpoint, str(e)[:200])


class Db:
    """MariaDB 接続の型付きラッパー (raw `mariadb` ドライバ = プラグイン準拠)。"""

    def __init__(self, name: str, *, host: str | None, port: int, user: str | None,
                 password: str | None, database: str | None) -> None:
        self.name = name
        self.host = host
        self._cfg = dict(host=host, port=int(port), user=user,
                         password=password, database=database)
        self._conn: Any = None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            import mariadb  # lazy
            self._conn = mariadb.connect(**self._cfg, reconnect=True)
            self._conn.autocommit = True
        return self._conn

    def ping(self) -> None:
        try:
            self.conn.ping()
        except Exception:
            self._conn = None  # 次アクセスで再接続

    def execute(self, sql: str, params: tuple | None = None) -> Any:
        cur = self.conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def query(self, sql: str, params: tuple | None = None) -> list:
        cur = self.execute(sql, params)
        return cur.fetchall()

    def healthy(self) -> HealthStatus:
        t0 = time.monotonic()
        try:
            self.ping()
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return HealthStatus(self.name, "db", True,
                                int((time.monotonic() - t0) * 1000), self.host)
        except Exception as e:  # noqa: BLE001
            self._conn = None
            return HealthStatus(self.name, "db", False,
                                int((time.monotonic() - t0) * 1000), self.host, str(e)[:200])
