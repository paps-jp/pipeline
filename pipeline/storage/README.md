# pipeline.storage — 移行ガイド(skeleton)

各プラグインの `setup()` に散在する MinIO/DB 構築を、中央レジストリ + 型付きラッパーに寄せる。
**新規開発から本 SDK を使い、既存プラグインは触るたびに置換**(破壊変更なし、段階移行)。

## Before(現行 `paprika_image_pull/image_main.py`)

```python
def _load_env_file(path): ...          # ← 全プラグインに重複
def _setup_raw_minio(env, kwargs):     # ← MinIO 構築を自前
    endpoint = env.get("MINIO_RAW_ENDPOINT") or kwargs.get("minio_raw_endpoint")
    ...
    return Minio(endpoint, access_key=..., secret_key=..., secure=False), bucket

def setup(**kwargs):
    env = _load_env_file(kwargs.get("db_env_file"))
    db_cfg = {"host": env.get("DB_HOST") or ..., "port": ..., ...}
    if not all(...): raise RuntimeError("DB creds required")
    import mariadb
    db = mariadb.connect(**db_cfg, reconnect=True); db.autocommit = True
    raw_minio, raw_bucket = _setup_raw_minio(env, kwargs)
    if raw_minio is None: raise RuntimeError("MINIO_RAW_ENDPOINT 未設定")
    return {"db": db, "raw_minio": raw_minio, "raw_bucket": raw_bucket, ...}

# process 内:
raw_minio.fput_object(raw_bucket, key, str(tmp_path), content_type="image/jpeg")
```

## After(SDK 利用)

```python
from pipeline.storage import StorageRegistry

def setup(**kwargs):
    reg = StorageRegistry.from_env_file(kwargs.get("db_env_file"), overrides=kwargs)
    return {
        "reg": reg,
        "db": reg.db(),          # creds 未設定なら即 RuntimeError(従来と同じ)
        "raw": reg.minio("raw"),
        ...
    }

# process 内:
state["raw"].put_file(key, tmp_path, content_type="image/jpeg")   # retry 内蔵
rows = state["db"].query("SELECT ... WHERE ...", (pk,))
```

- `_load_env_file` / `_setup_raw_minio` を**丸ごと削除**できる。
- creds 解決・retry・reconnect・死活はラッパー側に集約。
- 接続名(`raw`/`crawl`/`main`)は `registry.py` に定義済み。新エンドポイントは registry に1箇所追加。

## 監視への接続(今回の .16 サイレント停止対策)

```python
# control plane の周期ループ or supervisor watchdog:
reg = StorageRegistry.from_env_file(ENV_PATH)
for h in reg.health():            # [HealthStatus(name, kind, ok, latency_ms, endpoint, error), ...]
    if not h.ok:
        # → フロー左上の赤ボックス / 通知に流す(GPU・silent-hang と同じ経路)
        raise_storage_alert(h)
```

`reg.health()` が MinIO(bucket_exists プローブ)と DB(SELECT 1)の死活を返すので、
**「どこからも監視されていなかった MinIO」を1箇所で定期プローブ**できるようになる。

## 段階ロードマップ
1. **Phase 1(本 skeleton)**: レジストリ + `MinioStore`/`Db` + `.healthy()`。新規プラグインで採用。
2. **Phase 2**: executor が workload config の `resources:` 宣言を見て `ctx` に注入(Dagster Resource 流の DI、テスト mock 可能)。
3. **Phase 3**: `reg.health()` を control plane の `/api/v1/infra` + フロー赤ボックスに配線(ストレージ監視の一元化)。
