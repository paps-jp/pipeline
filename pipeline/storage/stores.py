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


def _minio_http_live(endpoint: str, secure: bool, timeout: float = 3.0) -> tuple[bool, int, str | None]:
    """MinIO の `/minio/health/live`(無認証)を HTTP GET で叩く reachability プローブ。
    `minio` パッケージ非依存 = control plane / 監視系の venv でも動く。
    Connection refused / No route / タイムアウトを即 down として検出。
    """
    import time as _t
    import urllib.error
    import urllib.request
    scheme = "https" if secure else "http"
    url = f"{scheme}://{endpoint}/minio/health/live"
    t0 = _t.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=timeout) as r:
            code = getattr(r, "status", 200)
        return (200 <= code < 500), int((_t.monotonic() - t0) * 1000), None
    except urllib.error.HTTPError as e:
        # 4xx でも「サーバは応答している」= reachable 扱い
        return (e.code < 500), int((_t.monotonic() - t0) * 1000), None
    except Exception as e:  # noqa: BLE001
        return False, int((_t.monotonic() - t0) * 1000), str(e)[:200]


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

    def put_bytes(self, key: str, data: bytes,
                  content_type: str = "application/octet-stream") -> Any:
        """メモリ上の bytes を put (= tmp ファイルを経由しない外部投入経路用)。

        `fput_object` と違い retry 毎に BytesIO を作り直す (= 1 度読み切った
        stream を再利用すると 2 回目が 0 バイトで成功してしまう)。
        """
        from io import BytesIO

        def _put() -> Any:
            return self.client.put_object(self.bucket, key, BytesIO(data), len(data),
                                          content_type=content_type)

        return _retry(_put, retries=self._retries)

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
        """minio パッケージ有: bucket 存在確認(認証付き)。無(監視系 venv 等): HTTP
        `/minio/health/live` で reachability のみ。どちらも Connection refused 等を即検出。"""
        try:
            import minio  # noqa: F401
            has_minio = True
        except ImportError:
            has_minio = False
        if not has_minio:
            ok, ms, err = _minio_http_live(self.endpoint or "", self._secure)
            return HealthStatus(self.name, "minio", ok, ms, self.endpoint, err)
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
            # mariadb(C wrapper)優先、無ければ pure-python pymysql (flow.py と同方針)。
            try:
                import mariadb  # lazy
                self._conn = mariadb.connect(**self._cfg, reconnect=True)
                self._conn.autocommit = True
            except ImportError:
                import pymysql  # lazy
                self._conn = pymysql.connect(
                    host=self._cfg["host"], port=self._cfg["port"],
                    user=self._cfg["user"], password=self._cfg["password"],
                    database=self._cfg["database"],
                    connect_timeout=3, read_timeout=30, autocommit=True,
                )
        return self._conn

    def ping(self) -> None:
        try:
            try:
                self.conn.ping(reconnect=True)   # pymysql
            except TypeError:
                self.conn.ping()                 # mariadb (引数なし)
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
