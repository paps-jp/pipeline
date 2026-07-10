"""pipeline.storage — ストレージ接続の SDK (レジストリ + 型付きラッパー + 死活)。

設計意図 (他 orchestrator の範例):
  - 中央レジストリ    ← Airflow Connections / Prefect Blocks
  - 型付きラッパー     ← Airflow Hook / Dagster Resource
  - `.healthy()` 死活  ← ストレージ監視の一元化 (MinIO/MariaDB のサイレント停止根絶)

現状は各プラグインが `setup()` 内で `_setup_minio()`/DB接続を env から自前構築し重複。
本 SDK に寄せると: 重複排除 + creds 単一ソース + テスト mock + **中央ヘルス監視**。

使い方 (プラグイン setup 内):
    from pipeline.storage import StorageRegistry
    reg = StorageRegistry.from_env_file(kwargs.get("db_env_file"), overrides=kwargs)
    state = {"reg": reg, "db": reg.db(), "raw": reg.minio("raw")}
    # process 内: state["raw"].put_file(key, path) / state["db"].query(sql)

監視側 (control plane / supervisor):
    for h in reg.health():
        if not h.ok: alert(h)   # h.name / h.endpoint / h.error
"""

from __future__ import annotations

from .env import load_env_file
from .registry import StorageRegistry
from .stores import Db, HealthStatus, MinioStore

__all__ = ["StorageRegistry", "MinioStore", "Db", "HealthStatus", "load_env_file"]
