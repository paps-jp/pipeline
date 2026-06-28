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
  workload_filter: string[] | null;
  filter_updated_at: string | null;
  filter_updated_by: string | null;
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

  flowSnapshot: () => request<FlowSnapshot>("/api/v1/flow/snapshot"),
  saveFlowLayout: (positions: Array<{ id: string; x: number; y: number }>) =>
    request<{ updated: number; skipped: number }>("/api/v1/flow/layout", {
      method: "POST",
      json: { positions },
    }),
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
  error?: string | null;
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

export interface FlowSnapshot {
  canvas: { width?: number; height?: number; background?: string };
  nodes: FlowNode[];
  edges: FlowEdge[];
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
