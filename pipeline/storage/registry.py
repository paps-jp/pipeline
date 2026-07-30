"""接続レジストリ (= Airflow Connections / Prefect Blocks 相当)。

env(+kwargs override)から named connection を解決し、型付きラッパーを
lazy 生成・cache する。プラグインは **名前で参照するだけ**で creds を
ハードコードしない。`.health()` が全接続のプローブ結果を返す (= 監視の源)。

既知の接続 (env キーは現行プラグイン準拠):
  minio:raw   → MINIO_RAW_ENDPOINT / MINIO_RAW_BUCKET(既定 raw) / MINIO_ACCESS_KEY / MINIO_SECRET_KEY   (.17 PANTRY)
  minio:crawl → MINIO_ENDPOINT / MINIO_BUCKET(既定 crawl) / MINIO_ACCESS_KEY / MINIO_SECRET_KEY / MINIO_VERIFY_TLS  (.16 ATTIC)
  minio:paprika → PAPRIKA_MINIO_ENDPOINT / PAPRIKA_MINIO_BUCKET(既定 paprika) / PAPRIKA_MINIO_ACCESS_KEY / PAPRIKA_MINIO_SECRET_KEY
  db:main     → DB_HOST / DB_PORT(既定 3306) / DB_USER / DB_PASS / DB_NAME
"""

from __future__ import annotations

from typing import Any

from .env import load_env_file
from .stores import Db, HealthStatus, MinioStore


class StorageRegistry:
    def __init__(self, env: dict[str, str] | None = None,
                 overrides: dict[str, Any] | None = None) -> None:
        self._env: dict[str, Any] = dict(env or {})
        if overrides:
            self._env.update({k: v for k, v in overrides.items() if v is not None})
        self._cache: dict[tuple[str, str], Any] = {}

    @classmethod
    def from_env_file(cls, path: str | None,
                      overrides: dict[str, Any] | None = None) -> "StorageRegistry":
        return cls(env=load_env_file(path), overrides=overrides)

    def _e(self, *keys: str, default: Any = None) -> Any:
        for k in keys:
            v = self._env.get(k)
            if v not in (None, ""):
                return v
        return default

    # ---------------- MinIO ----------------
    def minio(self, name: str) -> MinioStore:
        cached = self._cache.get(("minio", name))
        if cached is not None:
            return cached
        if name == "raw":
            store = MinioStore(
                "minio:raw",
                endpoint=self._e("MINIO_RAW_ENDPOINT"),
                access_key=self._e("MINIO_ACCESS_KEY", "MINIO_ACCESS"),
                secret_key=self._e("MINIO_SECRET_KEY", "MINIO_SECRET"),
                bucket=self._e("MINIO_RAW_BUCKET", default="raw"),
                secure=False,
            )
        elif name == "crawl":
            store = MinioStore(
                "minio:crawl",
                endpoint=self._e("MINIO_ENDPOINT"),
                access_key=self._e("MINIO_ACCESS_KEY", "MINIO_ACCESS"),
                secret_key=self._e("MINIO_SECRET_KEY", "MINIO_SECRET"),
                bucket=self._e("MINIO_BUCKET", default="crawl"),
                secure=str(self._e("MINIO_VERIFY_TLS", default="0")).lower() in ("1", "true", "yes"),
            )
        elif name == "paprika":
            # video-face-extract が crawl_video.storage_url を fget する先 (= paprika hub)。
            # 外部投入した動画もこのバケットに置かないと vfe が拾えない。
            store = MinioStore(
                "minio:paprika",
                endpoint=self._e("PAPRIKA_MINIO_ENDPOINT"),
                access_key=self._e("PAPRIKA_MINIO_ACCESS_KEY"),
                secret_key=self._e("PAPRIKA_MINIO_SECRET_KEY"),
                bucket=self._e("PAPRIKA_MINIO_BUCKET", default="paprika"),
                secure=False,
            )
        else:
            raise KeyError(f"unknown minio connection: {name!r} (known: raw, crawl, paprika)")
        if not store.endpoint:
            raise RuntimeError(f"storage: minio:{name} endpoint 未設定 (env 不足)")
        self._cache[("minio", name)] = store
        return store

    # ---------------- DB ----------------
    def db(self, name: str = "main") -> Db:
        cached = self._cache.get(("db", name))
        if cached is not None:
            return cached
        d = Db(
            f"db:{name}",
            host=self._e("DB_HOST"),
            port=self._e("DB_PORT", default=3306),
            user=self._e("DB_USER"),
            password=self._e("DB_PASS"),
            database=self._e("DB_NAME"),
        )
        if not all([d._cfg["host"], d._cfg["user"], d._cfg["password"], d._cfg["database"]]):
            raise RuntimeError("storage: db creds 未設定 (DB_HOST/USER/PASS/NAME)")
        self._cache[("db", name)] = d
        return d

    # ---------------- 死活 (= 監視の源) ----------------
    def health(self, minio_names: tuple[str, ...] = ("raw", "crawl"),
               db_names: tuple[str, ...] = ("main",)) -> list[HealthStatus]:
        """登録接続を全プローブ。control plane / supervisor から定期呼び出しして
        MinIO/MariaDB の死活を一元的に検知 → 赤ボックス/通知に統合する用。"""
        out: list[HealthStatus] = []
        for n in minio_names:
            try:
                out.append(self.minio(n).healthy())
            except Exception as e:  # noqa: BLE001 (未設定接続はスキップ扱い)
                out.append(HealthStatus(f"minio:{n}", "minio", False, 0, None, str(e)[:200]))
        for n in db_names:
            try:
                out.append(self.db(n).healthy())
            except Exception as e:  # noqa: BLE001
                out.append(HealthStatus(f"db:{n}", "db", False, 0, None, str(e)[:200]))
        return out
