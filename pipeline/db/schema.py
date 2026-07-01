"""DDL 定義。

SQLite を基準として書く。PostgreSQL / MariaDB アダプタは
self.dialect_rewrite() で方言差を吸収する想定。
"""

from __future__ import annotations

# --- workloads (workload 定義のマスタ) ----------------------------------
WORKLOADS_DDL = """
CREATE TABLE IF NOT EXISTS workloads (
    slug                 TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    description          TEXT,
    enabled              INTEGER NOT NULL DEFAULT 0,
    queue_table          TEXT NOT NULL,

    executor_type        TEXT NOT NULL,
    executor_config      TEXT NOT NULL DEFAULT '{}',

    success_criteria     TEXT NOT NULL DEFAULT '{"type":"exit_code","expected":0}',

    priority             INTEGER NOT NULL DEFAULT 100,
    weight               REAL    NOT NULL DEFAULT 1.0,
    batch_size           INTEGER NOT NULL DEFAULT 10,
    lease_secs           INTEGER NOT NULL DEFAULT 300,
    max_attempts         INTEGER NOT NULL DEFAULT 5,

    resources            TEXT NOT NULL DEFAULT '{}',
    host_affinity        TEXT NOT NULL DEFAULT '[]',
    on_success           TEXT,
    on_failure           TEXT,

    observed_depth       INTEGER NOT NULL DEFAULT 0,
    observed_age_secs    INTEGER NOT NULL DEFAULT 0,
    observed_rate        REAL    NOT NULL DEFAULT 0,

    -- worker self-report で集めた peak VRAM (= install-multi-worker 自動 N 算定の元値)
    observed_vram_mb_peak     INTEGER,
    observed_vram_sample_count INTEGER NOT NULL DEFAULT 0,
    observed_vram_updated_at  TEXT,

    -- 業務 queue の格納先 backend (Phase 2-α 2026-06-29)。
    -- 'sqlite' (= 既定、 control plane と同居) / 'mariadb' (= secondary_db)。
    -- worker/control が workload 毎にこの値で QueueRepository の backend を切替。
    queue_backend        TEXT NOT NULL DEFAULT 'sqlite',

    created_by           TEXT,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_version       INTEGER NOT NULL DEFAULT 1
);
"""

WORKLOADS_ALTERS = [
    "ALTER TABLE workloads ADD COLUMN observed_vram_mb_peak INTEGER",
    "ALTER TABLE workloads ADD COLUMN observed_vram_sample_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE workloads ADD COLUMN observed_vram_updated_at TEXT",
    # supervisor (= 自動オーケストレーター) がこの workload に介入することを許可するか。
    # 1 = 任せる (= 既定)、 0 = オペレータが手で握る (= supervisor の action はスキップ)。
    "ALTER TABLE workloads ADD COLUMN supervisor_enabled INTEGER NOT NULL DEFAULT 1",
    # 同一 host 上で同時実行できる worker 数の上限。 NULL = 無制限 (= 既定)。
    # 用途: image-embed のように cuBLAS init で VRAM をでかく確保する plugin で、
    # 同じ GPU を多重起動して CUBLAS_STATUS_ALLOC_FAILED で setup 死亡を防ぐ。
    "ALTER TABLE workloads ADD COLUMN max_concurrent_per_host INTEGER",
    # fleet 全体 (= 全 host 合計) の同時実行 worker 上限。 NULL = 無制限。
    # 単一 writer 保証 (embed-write=1) / balancer 過剰配分の抑制に使う。
    "ALTER TABLE workloads ADD COLUMN max_concurrent_total INTEGER",
    # CPU/GPU 分類フラグ (2026-06-28)。 静的配置設計 + UI 表示用。
    # NULL/0 = CPU only (= web/IO bound)、 1 = GPU heavy (= ONNX/CUDA)。
    # operator が worker.workload_filter を配置するときの判断材料。
    "ALTER TABLE workloads ADD COLUMN requires_gpu INTEGER NOT NULL DEFAULT 0",
    # VRAM 観測の充実 (2026-06-28)。 peak は OOM 防止の絶対線、 avg/p95 は
    # 実態に近い配置設計用の数字。 worker daemon が周期的に POST し、 hub 側で
    # sliding window で集計。 NULL = 未計測。
    "ALTER TABLE workloads ADD COLUMN observed_vram_mb_avg INTEGER",
    "ALTER TABLE workloads ADD COLUMN observed_vram_mb_p95 INTEGER",
    # 業務 queue 移行 (Phase 2-α 2026-06-29)。 queue の格納先 backend。
    # 'sqlite' (= 既定) / 'mariadb' (= secondary_db)。 workload 毎に切替。
    "ALTER TABLE workloads ADD COLUMN queue_backend TEXT NOT NULL DEFAULT 'sqlite'",
]

# workers テーブルへの後付け列 (= 既存 DB を壊さず adopt)
# - workload_filter: 自動切替の SoT。 worker daemon が control plane を poll して
#   反映する。 NULL/空 = env (PIPELINE_WORKLOAD_FILTER) にフォールバック。
WORKERS_ALTERS = [
    "ALTER TABLE workers ADD COLUMN workload_filter TEXT",        # JSON list[str] or NULL
    "ALTER TABLE workers ADD COLUMN filter_updated_at TEXT",
    "ALTER TABLE workers ADD COLUMN filter_updated_by TEXT",      # supervisor / operator など
    # systemd の PIPELINE_WORKLOAD_FILTER env (= worker daemon が register 時に申告)。
    # NULL = env 未設定 (= 全 workload claim 可)。 LLM advisor が dual filter を作る
    # ときに「env 由来の slug を奪わない」 ための基盤。
    "ALTER TABLE workers ADD COLUMN env_filter TEXT",             # JSON list[str] or NULL
]


# --- workers (接続中の worker レジストリ) -------------------------------
WORKERS_DDL = """
CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,            -- e.g. "w_a1b2"
    host            TEXT NOT NULL,
    pid             INTEGER,
    tags            TEXT NOT NULL DEFAULT '[]',  -- JSON list[str]
    resources       TEXT NOT NULL DEFAULT '{}',  -- JSON
    state           TEXT NOT NULL DEFAULT 'connecting', -- connecting/active/draining/lost
    started_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    current_workload TEXT,
    current_phase   TEXT,
    rows_processed  INTEGER NOT NULL DEFAULT 0,
    errors_total    INTEGER NOT NULL DEFAULT 0
);
"""


# --- runs (個別 task の処理履歴) ----------------------------------------
RUNS_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    id             TEXT PRIMARY KEY,
    workload_slug  TEXT NOT NULL,
    pk             TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    attempt        INTEGER NOT NULL DEFAULT 1,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    success        INTEGER,
    exit_code      INTEGER,
    duration_ms    INTEGER,
    stdout         TEXT,
    stderr         TEXT,
    output_json    TEXT,
    error          TEXT
);
"""


# --- users (UI ログインユーザ) ------------------------------------------
USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
    slug             TEXT PRIMARY KEY,
    display_name     TEXT,
    email            TEXT,
    password_hash    TEXT NOT NULL,
    roles            TEXT NOT NULL DEFAULT '["viewer"]',
    enabled          INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_at    TEXT
);
"""


# --- sessions (HMAC セッションキャッシュ) -------------------------------
SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    user_slug       TEXT NOT NULL,
    issued_at       TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    last_seen_at    TEXT,
    ip_address      TEXT
);
"""


# --- api_keys (プログラマ用) --------------------------------------------
API_KEYS_DDL = """
CREATE TABLE IF NOT EXISTS api_keys (
    id              TEXT PRIMARY KEY,
    user_slug       TEXT NOT NULL,
    name            TEXT NOT NULL,
    key_hash        TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    expires_at      TEXT,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at    TEXT
);
"""


# --- plugins (Plugin Registry: content-addressable コードアーカイブ) ----
PLUGINS_DDL = """
CREATE TABLE IF NOT EXISTS plugins (
    id              TEXT PRIMARY KEY,        -- "{slug}@{version}" 例: "hash-detect@7f3a91"
    slug            TEXT NOT NULL,
    version         TEXT NOT NULL,           -- 内容由来の sha256 prefix (12 hex chars)
    source_type     TEXT NOT NULL,           -- "inline" | "zip" | "git"
    source_meta     TEXT NOT NULL DEFAULT '{}',  -- JSON: {"git_url": ..., "ref": ...} 等
    archive         BLOB NOT NULL,           -- deterministic tar.gz bytes
    archive_size    INTEGER NOT NULL,
    main_module     TEXT NOT NULL DEFAULT 'main',  -- importlib.import_module する名前
    requirements    TEXT,                    -- requirements.txt 内容 (UI 表示用 + worker pip install)
    notes           TEXT,
    created_by      TEXT,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS plugins_idx_slug ON plugins (slug, created_at DESC);
"""


# --- settings (システム設定の key-value 永続化、 LLM 接続情報等) -----------
SETTINGS_DDL = """
CREATE TABLE IF NOT EXISTS settings (
    key             TEXT PRIMARY KEY,
    value           TEXT,
    description     TEXT,
    is_secret       INTEGER NOT NULL DEFAULT 0,   -- 1 なら API 経由 GET で mask する
    updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by      TEXT
);
"""


# --- llm_calls (LLM advisor の入出力監査ログ) -----------------------------
LLM_CALLS_DDL = """
CREATE TABLE IF NOT EXISTS llm_calls (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    provider        TEXT NOT NULL,
    model           TEXT NOT NULL,
    prompt_tokens   INTEGER,
    completion_tokens INTEGER,
    total_tokens    INTEGER,
    duration_ms     INTEGER,
    success         INTEGER NOT NULL,
    error           TEXT,
    prompt_json     TEXT,           -- 送信した messages(問題追跡用、 size 大)
    response_text   TEXT,           -- 生 response
    actions_json    TEXT,           -- parse 後の action list
    actions_applied INTEGER,
    analysis        TEXT            -- LLM 分析テキスト (= UI 表示用)
);
CREATE INDEX IF NOT EXISTS llm_calls_idx_called ON llm_calls (called_at DESC);
"""


# --- audit_log -----------------------------------------------------------
AUDIT_LOG_DDL = """
CREATE TABLE IF NOT EXISTS audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor           TEXT NOT NULL,        -- user.slug or "system" or "api:<key_id>"
    action          TEXT NOT NULL,        -- "workload.create", "auth.login", ...
    target          TEXT,                 -- 対象 ID/slug
    before_json     TEXT,
    after_json      TEXT,
    ip_address      TEXT
);
"""


# --- service_logs (daemon stdout を集約: Python logging stream を HTTP で push) ---
SERVICE_LOGS_DDL = """
CREATE TABLE IF NOT EXISTS service_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,         -- ISO8601 with milliseconds
    host            TEXT NOT NULL,         -- hostname (ai-gpu1 等)
    service         TEXT NOT NULL,         -- "pipeline-worker-gpu" / "pipeline-oss" / "plugin:<slug>"
    worker_id       TEXT,                  -- daemon が決まっていれば
    level           TEXT NOT NULL,         -- DEBUG / INFO / WARNING / ERROR / CRITICAL
    logger          TEXT,                  -- python logger 名 (dispatch_main 等)
    message         TEXT NOT NULL,
    exc_info        TEXT                   -- traceback (任意)
);
CREATE INDEX IF NOT EXISTS service_logs_idx_ts ON service_logs (id DESC);
CREATE INDEX IF NOT EXISTS service_logs_idx_host ON service_logs (host, id DESC);
CREATE INDEX IF NOT EXISTS service_logs_idx_service ON service_logs (service, id DESC);
"""


# --- deploy_paths (配信パス: src→dst + setup_command + service_command) ----
DEPLOY_PATHS_DDL = """
CREATE TABLE IF NOT EXISTS deploy_paths (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    label             TEXT NOT NULL,                 -- 表示名 (例: "embed_writer plugin")
    src_path          TEXT NOT NULL,                 -- .7 上の絶対 path (ファイル or ディレクトリ)
    dst_path          TEXT NOT NULL,                 -- GPU 箱の絶対 path
    enabled           INTEGER NOT NULL DEFAULT 1,
    delete_mode       INTEGER NOT NULL DEFAULT 0,    -- 1 = rsync --delete
    setup_command     TEXT,                          -- 配信直後 dst で 1 回実行 (例: pip install)
    service_command   TEXT,                          -- service として常駐 (= systemd unit 自動生成)
    notes             TEXT,
    last_synced_at    TEXT,
    last_synced_ok    INTEGER,                       -- 1=success, 0=fail, NULL=未実行
    created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS deploy_paths_idx_enabled ON deploy_paths (enabled);
"""


# --- deploy_targets (配信先 GPU 箱の レジストリ) -----------------------
DEPLOY_TARGETS_DDL = """
CREATE TABLE IF NOT EXISTS deploy_targets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    label           TEXT NOT NULL,                 -- 表示名 (例: "ai-gpu1")
    host            TEXT NOT NULL,                 -- IP or DNS (例: "10.10.50.23")
    ssh_user        TEXT NOT NULL DEFAULT 'root',
    ssh_port        INTEGER NOT NULL DEFAULT 22,
    enabled         INTEGER NOT NULL DEFAULT 1,
    notes           TEXT,
    last_deploy_at  TEXT,
    last_deploy_ok  INTEGER,                       -- 1=success, 0=fail, NULL=未実行
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS deploy_targets_idx_enabled ON deploy_targets (enabled);
"""


# --- worker_admin_cmds (= daemon に投げる admin コマンド queue) ----------
WORKER_ADMIN_CMDS_DDL = """
CREATE TABLE IF NOT EXISTS worker_admin_cmds (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_host     TEXT NOT NULL,          -- 配信先 host (= deploy_targets.host) or "*" (= 全 host)
    cmd_type        TEXT NOT NULL,          -- "fetch_archive" / "exec_shell" / "install_systemd"
    cmd_payload     TEXT NOT NULL,          -- JSON: {url|script|unit_name|content|...}
    state           TEXT NOT NULL DEFAULT 'pending',  -- pending / claimed / done / failed
    claimed_by      TEXT,                   -- worker_id
    claimed_at      TEXT,
    completed_at    TEXT,
    exit_code       INTEGER,
    stdout          TEXT,
    stderr          TEXT,
    error           TEXT,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deadline_at     TEXT                    -- 期限 (= 過ぎたら無視)
);
CREATE INDEX IF NOT EXISTS worker_admin_cmds_idx_pending
    ON worker_admin_cmds (state, target_host, id);
"""


# --- worker_metrics (= GPU 温度等の時系列。 reaper で 24h 前を delete) ---
WORKER_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS worker_metrics (
    worker_id     TEXT NOT NULL,
    gpu_idx       INTEGER NOT NULL,
    ts            TEXT NOT NULL,
    temp_c        REAL,
    util_pct      REAL,
    mem_used_mb   INTEGER,
    mem_util_pct  REAL,
    mem_total_mb  INTEGER,
    power_w       REAL,
    sm_clock_mhz  INTEGER,
    mem_clock_mhz INTEGER,
    PRIMARY KEY (worker_id, gpu_idx, ts)
);
CREATE INDEX IF NOT EXISTS worker_metrics_idx_ts
    ON worker_metrics (ts);
"""

# 既存 DB に対する idempotent な ALTER (= 後から追加された列)
WORKER_METRICS_ALTERS = [
    "ALTER TABLE worker_metrics ADD COLUMN mem_util_pct  REAL",
    "ALTER TABLE worker_metrics ADD COLUMN mem_total_mb  INTEGER",
    "ALTER TABLE worker_metrics ADD COLUMN power_w       REAL",
    "ALTER TABLE worker_metrics ADD COLUMN sm_clock_mhz  INTEGER",
    "ALTER TABLE worker_metrics ADD COLUMN mem_clock_mhz INTEGER",
]


# --- plugin_runtime (= プラグインがライブ UI 用に置く一時データ) --------
# プラグインが自分の状態 (動画スクショ・進捗・最新顔写真 ID 等) を
# 一時的に置くキー値ストア。 control plane が iframe UI 用に配信する。
# reaper で TTL 経過分を delete。
PLUGIN_RUNTIME_STATE_DDL = """
CREATE TABLE IF NOT EXISTS plugin_runtime_state (
    slug         TEXT NOT NULL,
    key          TEXT NOT NULL,
    value_json   TEXT NOT NULL,
    updated_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (slug, key)
);
CREATE INDEX IF NOT EXISTS plugin_runtime_state_idx_updated
    ON plugin_runtime_state (updated_at);
"""

PLUGIN_RUNTIME_BLOB_DDL = """
CREATE TABLE IF NOT EXISTS plugin_runtime_blob (
    slug         TEXT NOT NULL,
    key          TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    data         BLOB NOT NULL,
    size_bytes   INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (slug, key)
);
CREATE INDEX IF NOT EXISTS plugin_runtime_blob_idx_updated
    ON plugin_runtime_blob (updated_at);
"""


# --- vram_observations (= per-workload raw sample for avg/p95 集計、 2026-06-28) ---
# 既存 record_vram_observation は peak のみ更新するが、 配置設計には avg/p95 が必要。
# raw sample を 1h 保持して周期 task で workloads.observed_vram_mb_avg/p95 を計算。
# 古い行は reaper で削除 (60min より古い)。
VRAM_OBSERVATIONS_DDL = """
CREATE TABLE IF NOT EXISTS vram_observations (
    slug        TEXT NOT NULL,
    worker_id   TEXT NOT NULL,
    ts          TEXT NOT NULL,
    used_mb     INTEGER NOT NULL,
    PRIMARY KEY (slug, worker_id, ts)
);
CREATE INDEX IF NOT EXISTS vram_observations_idx_slug_ts
    ON vram_observations (slug, ts);
CREATE INDEX IF NOT EXISTS vram_observations_idx_ts
    ON vram_observations (ts);
"""


ALL_DDL = [
    WORKLOADS_DDL,
    WORKERS_DDL,
    RUNS_DDL,
    USERS_DDL,
    SESSIONS_DDL,
    API_KEYS_DDL,
    PLUGINS_DDL,
    AUDIT_LOG_DDL,
    SERVICE_LOGS_DDL,
    DEPLOY_TARGETS_DDL,
    DEPLOY_PATHS_DDL,
    WORKER_ADMIN_CMDS_DDL,
    WORKER_METRICS_DDL,
    PLUGIN_RUNTIME_STATE_DDL,
    PLUGIN_RUNTIME_BLOB_DDL,
    SETTINGS_DDL,
    LLM_CALLS_DDL,
    VRAM_OBSERVATIONS_DDL,
]


def workload_queue_ddl(queue_table: str) -> str:
    """workload ごとに 1 つ作る <slug>_queue 表の DDL。"""
    # NOTE: テーブル名は呼出側でバリデーション (英数 + _) 済の想定
    return f"""
CREATE TABLE IF NOT EXISTS {queue_table} (
    pk          TEXT PRIMARY KEY,
    state       TEXT NOT NULL DEFAULT 'pending',
    claimed_at  TEXT,
    claimed_by  TEXT,
    attempt     INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    extra       TEXT NOT NULL DEFAULT '{{}}',
    enqueued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS {queue_table}_idx_claim
    ON {queue_table} (state, claimed_at, attempt, pk);
"""
