/**
 * 簡易 API クライアント。fetch + JSON ラッパ。
 * 将来 openapi-typescript で型自動生成に置換予定。
 */

const BASE = ""; // 本番は同一オリジン、dev は Vite proxy 経由

async function request<T>(
  path: string,
  init?: RequestInit & { json?: unknown },
): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.json !== undefined) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(BASE + path, {
    ...init,
    headers,
    body: init?.json !== undefined ? JSON.stringify(init.json) : init?.body,
  });
  if (!res.ok) {
    const body = await res.text();
    let detail: unknown = body;
    try {
      detail = JSON.parse(body);
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, detail);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, body: unknown) {
    super(`API error ${status}`);
    this.status = status;
    this.body = body;
  }
}

// ----------------- 型 -----------------

export interface SystemStatus {
  version: string;
  mode: string;
  db_url: string;
  now: string;
}

export interface Workload {
  slug: string;
  name: string;
  description: string | null;
  enabled: boolean;
  // 既定 true。 false の時 supervisor の patch/filter 介入が skip される。
  supervisor_enabled: boolean;
  queue_table: string;
  executor_type: string;
  executor_config: Record<string, unknown>;
  success_criteria: Record<string, unknown>;
  priority: number;
  weight: number;
  batch_size: number;
  lease_secs: number;
  max_attempts: number;
  // 並列/常駐制御 (WorkloadBase)。 balancer/elastic が参照。
  max_concurrent_per_host: number | null;
  max_concurrent_total: number | null;
  min_workers: number;
  max_workers: number | null;
  requires_gpu: boolean;
  queue_backend: string;
  resources: Record<string, unknown>;
  host_affinity: unknown[];
  on_success: Record<string, unknown> | null;
  on_failure: Record<string, unknown> | null;
  observed_depth: number;
  observed_age_secs: number;
  observed_rate: number;
  observed_vram_mb_peak: number | null;
  observed_vram_sample_count: number;
  observed_vram_updated_at: string | null;
  created_by: string | null;
  created_at: string;
  updated_at: string;
  schema_version: number;
}

export interface WorkloadCreate {
  slug: string;
  name: string;
  description?: string | null;
  enabled?: boolean;
  executor_type: string;
  executor_config?: Record<string, unknown>;
  success_criteria?: Record<string, unknown>;
  priority?: number;
  weight?: number;
  batch_size?: number;
  lease_secs?: number;
  max_attempts?: number;
  resources?: Record<string, unknown>;
  host_affinity?: unknown[];
}

// ----------------- API 関数 -----------------

export interface RunRecord {
  id: string;
  workload_slug: string;
  pk: string;
  worker_id: string;
  attempt: number;
  started_at: string;
  finished_at: string | null;
  success: boolean | null;
  exit_code: number | null;
  duration_ms: number | null;
  stdout: string | null;
  stderr: string | null;
  output_json: Record<string, unknown> | null;
  error: string | null;
}

export interface QueueStats {
  by_state: Record<string, number>;
  total: number;
}

export interface WorkerInfo {
  id: string;
  host: string;
  pid: number | null;
  tags: string[];
  resources: Record<string, unknown>;
  state: string;
  started_at: string | null;
  last_seen_at: string | null;
  current_workload: string | null;
  current_phase: string | null;
  rows_processed: number;
  errors_total: number;
  // Track B (単一 workload 移行): 担当 workload の派生スカラ (read-only)。
  // workload_filter が要素1のときのみ slug、None/空/複数 (= 移行残) は null。
  workload: string | null;
  workload_filter: string[] | null;
  filter_updated_at: string | null;
  filter_updated_by: string | null;
  env_filter: string[] | null;
}

// ----------------- Plugin Registry -----------------

export interface PluginKwargField {
  key: string;
  type: "int" | "float" | "str" | "path" | "bool" | "enum" | "secret";
  default?: unknown;
  label?: string;
  help?: string;
  min?: number;
  max?: number;
  options?: unknown[];
  required?: boolean;
  /** UI のセクション名 (= supervisor のようにキーが数十個ある plugin を畳むため) */
  group?: string;
}

export interface PluginManifest {
  name?: string;
  description?: string;
  init_kwargs: PluginKwargField[];
  hidden_kwargs: string[];
  ui_panel?: boolean;
  ui_panel_mode?: "video" | "image";
}

export interface AvailablePlugin {
  slug: string;
  path: string;
  modules: string[];
  has_requirements: boolean;
  has_ui_panel?: boolean;
  manifest?: PluginManifest | null;
}

export interface FleetState {
  stopped: boolean;
  stopped_at: string | null;
  stopped_by: string | null;
  recorded_slugs: string[];
  enabled_now: string[];
  /** 止めると顔検索が死ぬ workload (faiss-api 等)。「検索以外全停止」の除外集合。 */
  search_slugs: string[];
}

export interface FleetActionResult {
  ok: boolean;
  stopped: boolean;
  changed: string[];
  failed: { slug: string; error: string }[];
  recorded_slugs: string[];
  /** keep_search で意図的に残した slug。 */
  kept_slugs: string[];
  message: string;
}

export const api = {
  status: () => request<SystemStatus>("/api/v1/status"),
  health: () => request<{ ok: boolean; version: string }>("/api/v1/health"),

  listWorkloads: () =>
    request<{ workloads: Workload[]; total: number }>("/api/v1/workloads"),
  getWorkload: (slug: string) => request<Workload>(`/api/v1/workloads/${slug}`),
  createWorkload: (payload: WorkloadCreate) =>
    request<Workload>("/api/v1/workloads", { method: "POST", json: payload }),
  updateWorkload: (slug: string, payload: Omit<WorkloadCreate, "slug">) =>
    request<Workload>(`/api/v1/workloads/${slug}`, { method: "PUT", json: payload }),
  setWorkloadEnabled: (slug: string, enabled: boolean) =>
    request<Workload>(`/api/v1/workloads/${slug}/enabled`, {
      method: "PATCH",
      json: { enabled },
    }),
  setSupervisorEnabled: (slug: string, enabled: boolean) =>
    request<Workload>(`/api/v1/workloads/${slug}/supervisor_enabled`, {
      method: "PATCH",
      json: { enabled },
    }),
  deleteWorkload: (slug: string) =>
    request<void>(`/api/v1/workloads/${slug}`, { method: "DELETE" }),

  // --- フリート全停止 / 再開 ---
  fleetState: () => request<FleetState>("/api/v1/fleet/state"),
  fleetStop: (by?: string, keepSearch = false) =>
    request<FleetActionResult>("/api/v1/fleet/stop", {
      method: "POST",
      json: { by, keep_search: keepSearch },
    }),
  fleetResume: (by?: string) =>
    request<FleetActionResult>("/api/v1/fleet/resume", { method: "POST", json: { by } }),

  enqueueTask: (slug: string, pk: string, extra: Record<string, unknown> = {}) =>
    request<{ inserted: number; duplicates: number }>(
      `/api/v1/workloads/${slug}/tasks`,
      { method: "POST", json: { pk, extra } },
    ),
  getQueueStats: (slug: string) =>
    request<QueueStats>(`/api/v1/workloads/${slug}/queue`),
  listRuns: (slug: string, limit = 50) =>
    request<{ runs: RunRecord[]; total: number }>(
      `/api/v1/workloads/${slug}/runs?limit=${limit}`,
    ),

  listAvailablePlugins: () =>
    request<{ root: string; plugins: AvailablePlugin[] }>(
      "/api/v1/plugins/available",
    ),

  listWorkersMetrics: (minutes = 30) =>
    request<{
      workers: Record<string, Record<string, Array<{
        ts: string;
        temp_c: number | null;
        util_pct: number | null;
        mem_used_mb: number | null;
        mem_util_pct: number | null;
        mem_total_mb: number | null;
        power_w: number | null;
        sm_clock_mhz: number | null;
        mem_clock_mhz: number | null;
      }>>>;
      since_minutes: number;
    }>(`/api/v1/workers/metrics?minutes=${minutes}`),

  listWorkers: () =>
    request<{ workers: WorkerInfo[]; total: number }>("/api/v1/workers"),
  setWorkerFilter: (workerId: string, workloads: string[] | null,
                    updatedBy?: string) =>
    request<WorkerInfo>(`/api/v1/workers/${workerId}/filter`, {
      method: "POST",
      json: { workloads, updated_by: updatedBy ?? "ui" },
    }),
  listRecentRuns: (limit = 200) =>
    request<{ runs: RunRecord[]; total: number }>(`/api/v1/runs?limit=${limit}`),

  // ---------------- settings ----------------
  listSettings: () => request<{ settings: SettingItem[] }>("/api/v1/settings"),
  setSetting: (key: string, value: string | null, updatedBy?: string) =>
    request<SettingItem>(`/api/v1/settings/${encodeURIComponent(key)}`, {
      method: "PATCH",
      json: { value, updated_by: updatedBy ?? "ui" },
    }),
  testLlm: (body: { endpoint?: string; api_key?: string; model?: string; timeout_s?: number }) =>
    request<{
      ok: boolean; status_code: number | null; latency_ms: number;
      model: string; response_excerpt: string | null; error: string | null;
    }>("/api/v1/settings/llm/test", { method: "POST", json: body }),

  listLlmCalls: (limit = 50) =>
    request<{ calls: LlmCallSummary[]; total: number }>(`/api/v1/llm_calls?limit=${limit}`),
  getLlmCall: (id: number) => request<LlmCallDetail>(`/api/v1/llm_calls/${id}`),

  dashboardOverview: () =>
    request<DashboardOverview>("/api/v1/dashboard/overview"),
  workloadsRunsSummary: () =>
    request<WorkloadRunsSummary[]>("/api/v1/dashboard/workloads-runs-summary"),

  listServiceLogs: (params: {
    limit?: number;
    since_id?: number;
    host?: string | null;
    service?: string | null;
    worker_id?: string | null;
    min_level?: "DEBUG" | "INFO" | "WARNING" | "ERROR" | "CRITICAL" | null;
  } = {}) => {
    const qs = new URLSearchParams();
    if (params.limit !== undefined) qs.set("limit", String(params.limit));
    if (params.since_id !== undefined && params.since_id !== null)
      qs.set("since_id", String(params.since_id));
    if (params.host) qs.set("host", params.host);
    if (params.service) qs.set("service", params.service);
    if (params.worker_id) qs.set("worker_id", params.worker_id);
    if (params.min_level) qs.set("min_level", params.min_level);
    const q = qs.toString();
    return request<{ records: ServiceLogRecord[]; total: number; max_id: number | null }>(
      `/api/v1/service-logs${q ? "?" + q : ""}`,
    );
  },

  setWorkloadHostAffinity: (slug: string, hosts: string[]) =>
    request<Workload>(`/api/v1/workloads/${slug}/host_affinity`, {
      method: "PATCH",
      json: { hosts },
    }),

  flowSnapshot: () => request<FlowSnapshot>("/api/v1/flow/snapshot"),
  saveFlowLayout: (positions: Array<{ id: string; x: number; y: number }>) =>
    request<{ updated: number; skipped: number }>("/api/v1/flow/layout", {
      method: "POST",
      json: { positions },
    }),
  flowRates: (sinceMin: number) =>
    request<{ since: string; count: number; series: FlowRateRow[] }>(
      `/api/v1/flow/rates?since_min=${sinceMin}`,
    ),
};

export interface FlowNode {
  id: string;
  kind: "workload" | "tank" | "external";
  x: number;
  y: number;
  label: string;
  icon?: string | null;
  workload_slug?: string | null;
  url?: string | null;
  state?: "running" | "idle" | "failed" | "backoff" | null;
  throughput_per_min?: number | null;
  last_run_at?: string | null;
  last_output?: Record<string, unknown> | null;
  adapt?: Record<string, unknown> | null;
  pending?: number | null;
  capacity_warn?: number | null;
  fill_ratio?: number | null;
  /** 件数以外の tank (= RAM ディスクの GB 等) の単位表記。値の後ろに付ける。 */
  unit?: string | null;
  error?: string | null;
  error_worker?: string | null;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  label?: string | null;
  metric_field?: string | null;
  dashed?: boolean;
  rate_per_min?: number | null;
}

// GET /api/v1/flow/rates の 1 行 (flow_rate_1m の long-format: 1 分バケット)。
export interface FlowRateRow {
  ts_min: string;
  slug: string;
  metric: string;
  value: number;
}

// ---------------- settings + llm ----------------

export interface SettingItem {
  key: string;
  value: string | null;
  value_masked?: string | null;
  description: string | null;
  is_secret: number;
  updated_at: string | null;
  updated_by?: string | null;
}

export interface LlmCallSummary {
  id: number;
  called_at: string;
  provider: string;
  model: string;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  total_tokens: number | null;
  duration_ms: number;
  success: number;
  error: string | null;
  actions_applied: number;
  analysis: string | null;
}

export interface LlmCallDetail extends LlmCallSummary {
  prompt_json?: unknown;
  response_text?: string;
  actions_json?: unknown;
}

export interface InfraAlert {
  name: string;
  kind: string;
  endpoint?: string | null;
  error?: string | null;
  severity?: string;
  detail?: string | null;
}

export interface FlowSnapshot {
  canvas: { width?: number; height?: number; background?: string };
  nodes: FlowNode[];
  edges: FlowEdge[];
  infra_alerts?: InfraAlert[];
}

export interface RunningRun {
  id: string;
  workload_slug: string;
  pk: string;
  worker_id: string;
  attempt: number;
  started_at: string;
}

export interface RecentFailure {
  id: string;
  workload_slug: string;
  pk: string;
  worker_id: string;
  started_at: string;
  reason: string | null;
}

export interface QueueDepth {
  workload_slug: string;
  by_state: Record<string, number>;
  total: number;
}

export interface DashboardOverview {
  running: RunningRun[];
  recent_failures: RecentFailure[];
  queue_depths: QueueDepth[];
}

export interface WorkloadRunsSummary {
  workload_slug: string;
  bits: number[];        // 1=success, 0=fail, -1=unknown (新しい順)
  success_rate: number | null;
}

export interface ServiceLogRecord {
  id: number;
  ts: string;
  host: string;
  service: string;
  worker_id: string | null;
  level: string;
  logger: string | null;
  message: string;
  exc_info: string | null;
}

export interface DeployRun {
  id: string;
  started_at: string;
  finished_at: string | null;
  duration_s: number | null;
  success: boolean | null;
  exit_code: number | null;
  log: string;
  hosts: string[];
  skip_restart: boolean;
  dry_run: boolean;
}

export interface DeployTarget {
  id: number;
  label: string;
  host: string;
  ssh_user: string;
  ssh_port: number;
  enabled: boolean;
  notes: string | null;
  last_deploy_at: string | null;
  last_deploy_ok: boolean | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface DeployTargetCreate {
  label: string;
  host: string;
  ssh_user?: string;
  ssh_port?: number;
  enabled?: boolean;
  notes?: string | null;
}

export const deployApi = {
  trigger: (opts: { hosts?: string[]; skip_restart?: boolean; dry_run?: boolean } = {}) =>
    request<DeployRun>("/api/v1/admin/deploy", { method: "POST", json: opts }),
  list: () => request<DeployRun[]>("/api/v1/admin/deploy"),
  get: (id: string) => request<DeployRun>(`/api/v1/admin/deploy/${id}`),

  listTargets: () => request<DeployTarget[]>("/api/v1/admin/deploy-targets"),
  createTarget: (body: DeployTargetCreate) =>
    request<DeployTarget>("/api/v1/admin/deploy-targets", { method: "POST", json: body }),
  updateTarget: (id: number, body: Partial<DeployTargetCreate>) =>
    request<DeployTarget>(`/api/v1/admin/deploy-targets/${id}`, { method: "PUT", json: body }),
  deleteTarget: (id: number) =>
    request<void>(`/api/v1/admin/deploy-targets/${id}`, { method: "DELETE" }),
  getPubkey: () => request<{ pubkey: string | null; source: string | null; hint?: string }>(
    "/api/v1/admin/deploy-targets/pubkey",
  ),

  listPaths: () => request<DeployPath[]>("/api/v1/admin/deploy-paths"),
  createPath: (body: DeployPathCreate) =>
    request<DeployPath>("/api/v1/admin/deploy-paths", { method: "POST", json: body }),
  updatePath: (id: number, body: Partial<DeployPathCreate>) =>
    request<DeployPath>(`/api/v1/admin/deploy-paths/${id}`, { method: "PUT", json: body }),
  deletePath: (id: number) =>
    request<void>(`/api/v1/admin/deploy-paths/${id}`, { method: "DELETE" }),
};

// ---------------- ホスト設定 (host_policy + agent desired) ----------------

/** GET /api/v1/host-policy の 1 行。 auto 面 (agent の nvidia-smi 検出) と
 *  manual 面 (operator の上書き) を merge した effective 込み。 */
export interface HostPolicy {
  host: string;
  // auto 面
  gpu_model: string | null;
  vram_total_mb: number | null;
  vram_free_mb: number | null;
  gpu_children_alive: number | null;
  last_seen_at: string | null;
  // manual 面
  tier: string | null;
  max_gpu_workers: number | null;
  vram_override_mb: number | null;
  labels: string[];
  notes: string | null;
  enabled: number;
  updated_at: string | null;
  updated_by: string | null;
  // merge 結果
  tier_effective: string | null;
  vram_effective_mb: number | null;
}

export interface HostPolicyUpdate {
  tier?: string | null;
  max_gpu_workers?: number | null;
  vram_override_mb?: number | null;
  labels?: string[];
  notes?: string | null;
  enabled?: boolean;
  updated_by?: string;
}

/** agent template / effective の 1 workload エントリ。 */
export interface AgentWorkloadEntry {
  count: number;
  gpu?: boolean;
  vram_mb?: number;
  cvd?: string | null;
  /** operator 固定。 true の間 supervisor の elastic は増減させない。 */
  pin?: boolean;
  /** effective 側のみ。 host_affinity 違反で planner が 0 に矯正した印。
   *  この印がある 0 は template から復活させてはいけない (絶対制約)。 */
  affinity_blocked?: boolean;
}

export interface AgentChildStatus {
  child_id: string;
  workload: string;
  gpu: boolean;
  alive: boolean;
}

/** GET /api/v1/agents の 1 行。 */
export interface AgentRecord {
  host: string;
  /** operator/elastic が置く目標値 (上限テンプレ)。 */
  template: { workloads: Record<string, AgentWorkloadEntry> } | null;
  /** planner が VRAM 実測で丸めた実配布値。 agent はこれを reconcile する。 */
  desired: { workloads: Record<string, AgentWorkloadEntry> } | null;
  last_children: AgentChildStatus[] | null;
  last_seen_at: string | null;
  last_vram_total_mb: number | null;
  last_vram_free_mb: number | null;
  last_gpu_model: string | null;
  updated_at: string | null;
  updated_by: string | null;
}

export const hostApi = {
  listPolicy: () => request<{ hosts: HostPolicy[] }>("/api/v1/host-policy"),
  updatePolicy: (host: string, body: HostPolicyUpdate) =>
    request<HostPolicy>(`/api/v1/host-policy/${encodeURIComponent(host)}`, {
      method: "PUT",
      json: { updated_by: "ui", ...body },
    }),

  listAgents: () => request<{ agents: AgentRecord[] }>("/api/v1/agents"),
  /** template を全置換する (= このホストの workload 別 目標台数)。
   *
   *  effective は「CPU slug は template 同値 / GPU slug は min(既存 effective, template)」で
   *  追随する (planner の VRAM 算定を消さないため)。 GPU 行は planner が VRAM 実測で
   *  戻すまで目標に届かないことがある。 */
  setAgentDesired: (host: string, workloads: Record<string, AgentWorkloadEntry>) =>
    request<{ host: string; ok: boolean }>(
      `/api/v1/agents/${encodeURIComponent(host)}/desired`,
      { method: "PUT", json: { workloads } },
    ),
  /** effective (agent が実際に reconcile する値) を直接書く。
   *
   *  通常は planner の領分だが、 planner が触れないホスト・slug で effective が
   *  目標を下回ったまま戻らなくなったときの operator 用の復旧レバー
   *  (2026-08-02: nas-c2 の CPU singleton が effective=0 で恒久停止)。 */
  setAgentEffective: (host: string, workloads: Record<string, AgentWorkloadEntry>) =>
    request<{ host: string; ok: boolean }>(
      `/api/v1/agents/${encodeURIComponent(host)}/effective`,
      { method: "PUT", json: { workloads } },
    ),
};

export interface DeployPath {
  id: number;
  label: string;
  src_path: string;
  dst_path: string;
  enabled: boolean;
  delete_mode: boolean;
  setup_command: string | null;
  service_command: string | null;
  notes: string | null;
  last_synced_at: string | null;
  last_synced_ok: boolean | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface DeployPathCreate {
  label: string;
  src_path: string;
  dst_path: string;
  enabled?: boolean;
  delete_mode?: boolean;
  setup_command?: string | null;
  service_command?: string | null;
  notes?: string | null;
}
