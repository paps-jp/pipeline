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

    created_by           TEXT,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_version       INTEGER NOT NULL DEFAULT 1
);
"""


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
    worker_id    TEXT NOT NULL,
    gpu_idx      INTEGER NOT NULL,
    ts           TEXT NOT NULL,
    temp_c       REAL,
    util_pct     REAL,
    mem_used_mb  INTEGER,
    PRIMARY KEY (worker_id, gpu_idx, ts)
);
CREATE INDEX IF NOT EXISTS worker_metrics_idx_ts
    ON worker_metrics (ts);
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
