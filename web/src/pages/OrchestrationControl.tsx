/**
 * Orchestration Control — 流量制御パネル。
 *
 * 各 workload に対し:
 *  - 担当 worker 数(= workload_filter で claim 可能な worker)
 *  - 並列バッチ規模(= batch_size)
 *  - 優先度(= priority)
 *  - lease 周期(= lease_secs)
 * を「弱(-)」「強(+)」 のボタンで増減できる。
 *
 * 上の値は実時間で確定。 supervisor (apply_mode=1) が同時に弄ってる場合に
 * 警告 banner を出す (= 操作の race を抑止)。
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  Code,
  Collapse,
  Group,
  Loader,
  Paper,
  Progress,
  Stack,
  Table,
  Text,
  Tooltip,
} from "@mantine/core";
import {
  IconAdjustments,
  IconBolt,
  IconChevronDown,
  IconChevronUp,
  IconMinus,
  IconPlus,
  IconRobot,
  IconRobotOff,
  IconStack2,
  IconUsers,
} from "@tabler/icons-react";

import {
  api,
  type RunRecord,
  type Workload,
  type WorkerInfo,
} from "@/api/client";

// ---------------- helpers ----------------

function parseHostFromWid(wid: string): string {
  if (!wid.startsWith("w_")) return wid;
  const parts = wid.slice(2).split("_");
  return parts.length >= 2 ? `${parts[0]}-${parts[1]}` : wid;
}

function buildWorkloadPutBody(w: Workload, patch: Partial<Workload>): Record<string, unknown> {
  // PUT は WorkloadCreate スキーマで受ける (= observed_* / created_at / queue_table 等 NG)
  const STRIP = new Set([
    "slug", "queue_table", "created_at", "updated_at",
    "observed_depth", "observed_age_secs", "observed_rate",
    "observed_vram_mb_peak", "observed_vram_sample_count",
    "observed_vram_updated_at", "created_by", "schema_version",
  ]);
  const body: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(w)) {
    if (!STRIP.has(k)) body[k] = v;
  }
  Object.assign(body, patch);
  return body;
}

interface WlMetrics {
  throughput_min: number;   // 直近 5min の successful runs / min
  backlog: number;          // pending + claimed
  active_workers: number;   // この workload を filter 上で claim 可能 + active な worker 数
}

function fmtNum(n: number, digits = 1): string {
  if (!isFinite(n)) return "—";
  if (Math.abs(n) >= 100) return Math.round(n).toString();
  return n.toFixed(digits);
}

// ---------------- supervisor warning ----------------

function useSupervisorApplyMode() {
  const q = useQuery({
    queryKey: ["supervisor-workload"],
    queryFn: () => api.getWorkload("pipeline-supervisor"),
    refetchInterval: 30_000,
    retry: false,
  });
  const ec = q.data?.executor_config as Record<string, unknown> | undefined;
  const ik = (ec?.init_kwargs ?? {}) as Record<string, unknown>;
  return {
    enabled: q.data?.enabled ?? false,
    applyMode: Boolean(Number(ik?.apply_mode ?? 0)),
    loading: q.isLoading,
  };
}

// ---------------- main page ----------------

export default function OrchestrationControl() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const wlsQ = useQuery({
    queryKey: ["workloads-for-orch"],
    queryFn: () => api.listWorkloads(),
    refetchInterval: 5_000,
  });
  const workersQ = useQuery({
    queryKey: ["workers-for-orch"],
    queryFn: () => api.listWorkers(),
    refetchInterval: 5_000,
  });
  const runsQ = useQuery({
    queryKey: ["runs-for-orch"],
    queryFn: () => api.listRecentRuns(500),
    refetchInterval: 5_000,
  });
  const sup = useSupervisorApplyMode();

  // workload ごとの metrics (= throughput / backlog / active workers)
  const metricsBySlug = useMemo(() => {
    const wls = wlsQ.data?.workloads ?? [];
    const workers = (workersQ.data?.workers ?? []).filter((w) => w.state === "active");
    const runs = runsQ.data?.runs ?? [];
    const now = Date.now();
    const cutoff = now - 5 * 60 * 1000;
    // throughput: 直近 5min の successful run 数 / 5
    const counts = new Map<string, number>();
    for (const r of runs) {
      if (!r.success) continue;
      const fin = r.finished_at ?? r.started_at;
      if (!fin) continue;
      const t = Date.parse(fin);
      if (isNaN(t) || t < cutoff) continue;
      counts.set(r.workload_slug, (counts.get(r.workload_slug) ?? 0) + 1);
    }
    const out = new Map<string, WlMetrics>();
    for (const w of wls) {
      // active workers = filter に含まれる(or filter=null=全受け) worker のうち、 host_affinity が
      // 空 or match する者
      const hostAffinity = (w.host_affinity ?? []) as string[];
      const matchHost = (worker: WorkerInfo) =>
        hostAffinity.length === 0 || hostAffinity.includes(parseHostFromWid(worker.id));
      const claimable = workers.filter((worker) => {
        if (!matchHost(worker)) return false;
        const filter = worker.workload_filter;
        if (filter === null) return true;
        return filter.includes(w.slug);
      }).length;
      out.set(w.slug, {
        throughput_min: (counts.get(w.slug) ?? 0) / 5,
        backlog: (w.observed_depth ?? 0),
        active_workers: claimable,
      });
    }
    return out;
  }, [wlsQ.data, workersQ.data, runsQ.data]);

  // ---------------- 個別 mutation (= 強/弱) ----------------

  const patchMut = useMutation({
    mutationFn: async (args: { w: Workload; patch: Partial<Workload> }) => {
      const body = buildWorkloadPutBody(args.w, args.patch);
      return api.updateWorkload(args.w.slug, body as never);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workloads-for-orch"] }),
  });

  // 担当 worker 数の +/-: filter=null の worker から「最初の 1 人を該当 slug 専用に絞る」 か、
  // 既に絞られてる worker の filter から該当 slug を抜く。 ホスト分散を保つため、
  // 現在 claim 担当少ない host から優先的に追加 / 多い host から優先削除。
  const filterMut = useMutation({
    mutationFn: async (args: { slug: string; delta: 1 | -1; workers: WorkerInfo[];
                                allSlugs: string[] }) => {
      const { slug, delta, workers, allSlugs } = args;
      // worker × host map
      const byHost = new Map<string, WorkerInfo[]>();
      for (const w of workers.filter((w) => w.state === "active")) {
        const h = parseHostFromWid(w.id);
        const arr = byHost.get(h) ?? [];
        arr.push(w);
        byHost.set(h, arr);
      }
      const inFilter = (w: WorkerInfo) =>
        w.workload_filter === null || w.workload_filter.includes(slug);
      // 担当しているの数 per host
      const countPerHost = new Map<string, number>();
      for (const [h, ws] of byHost.entries()) {
        countPerHost.set(h, ws.filter(inFilter).length);
      }
      if (delta === 1) {
        // 担当を 1 個増やす: 担当が一番少ない host の中で、 filter から slug を「外れてる」
        // worker を 1 つ拾い、 そのfilter に slug を足す。 候補なければ no-op。
        const hosts = Array.from(byHost.keys())
          .sort((a, b) => (countPerHost.get(a)! - countPerHost.get(b)!));
        for (const h of hosts) {
          const candidate = byHost.get(h)!.find(
            (w) => w.workload_filter !== null && !w.workload_filter.includes(slug),
          );
          if (!candidate) continue;
          const cur = candidate.workload_filter ?? [];
          const next = [...cur, slug].sort();
          await api.setWorkerFilter(candidate.id, next, "ui:flow-control");
          return;
        }
        throw new Error("追加対象なし: 全 worker が既に担当 or filter=null");
      } else {
        // 担当を 1 個減らす: 担当が一番多い host から、 filter=明示の slug 入り worker を 1 つ
        // 拾い、 slug を抜く。 結果 filter=[] なら null (= env fallback) に置換。
        const hosts = Array.from(byHost.keys())
          .sort((a, b) => (countPerHost.get(b)! - countPerHost.get(a)!));
        for (const h of hosts) {
          const candidate = byHost.get(h)!.find(
            (w) => w.workload_filter !== null && w.workload_filter.includes(slug),
          );
          if (!candidate) continue;
          const cur = candidate.workload_filter!;
          const next = cur.filter((s) => s !== slug);
          // null=全受 worker は触らない (= 強の対象でも弱の対象でもない)
          await api.setWorkerFilter(
            candidate.id, next.length === 0 ? allSlugs.filter((s) => s !== slug) : next,
            "ui:flow-control",
          );
          return;
        }
        // 該当 worker 無し → 全 worker が filter=null か明示 list に slug 含まず
        // null=全受 を 1 個減らす場合は「明示 filter で slug を除外」 する必要があり破壊的。
        // 安全側で no-op + メッセージ。
        throw new Error("削減対象なし: 明示 filter に該当 slug を持つ worker が無い");
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["workers-for-orch"] });
    },
  });

  if (wlsQ.isLoading || workersQ.isLoading) {
    return (
      <Box p="xl">
        <Loader />
        <Text size="sm" c="dimmed" mt="sm">
          {t("orchestration.loading", "読み込み中…")}
        </Text>
      </Box>
    );
  }

  const wls = (wlsQ.data?.workloads ?? [])
    .filter((w) => w.enabled && w.slug !== "pipeline-supervisor")
    .sort((a, b) => a.slug.localeCompare(b.slug));
  const allEnabledSlugs = wls.map((w) => w.slug);
  const workers = workersQ.data?.workers ?? [];

  // 強/弱 = priority ±10 (= デフォルトの 1 ボタン操作)
  const strong = (w: Workload, dir: 1 | -1) => {
    const target = Math.max(0, Math.min(200, w.priority + dir * 10));
    if (target === w.priority) return;
    patchMut.mutate({ w, patch: { priority: target } });
  };

  return (
    <Box p="md">
      <Stack gap="md">
        <Group justify="space-between">
          <Group gap="xs">
            <IconAdjustments size={24} />
            <Text size="xl" fw={700}>
              {t("orchestration.title", "流量制御")}
            </Text>
          </Group>
          <Text size="sm" c="dimmed">
            {t("orchestration.subtitle", "各 workload の流量を弱める/強めるための操作パネル。")}
          </Text>
        </Group>

        {sup.applyMode && (
          <Alert
            color="yellow"
            icon={<IconRobot size={18} />}
            title={t("orchestration.supervisor_warn_title", "supervisor が自動制御中")}
          >
            {t(
              "orchestration.supervisor_warn",
              "pipeline-supervisor が apply_mode=1 で動作中です。 手動で変更してもルール条件が成立すれば再度上書きされます。",
            )}
          </Alert>
        )}

        <Paper p="md" shadow="xs" radius="md">
          <Table verticalSpacing="sm" highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("orchestration.workload", "ワークロード")}</Table.Th>
                <Table.Th style={{ width: 130 }}>
                  {t("orchestration.throughput", "流量")}
                </Table.Th>
                <Table.Th style={{ width: 100 }}>
                  {t("orchestration.backlog", "バックログ")}
                </Table.Th>
                <Table.Th style={{ width: 80 }}>
                  {t("orchestration.workers", "ワーカー")}
                </Table.Th>
                <Table.Th style={{ width: 60 }}>
                  {t("orchestration.priority", "優先度")}
                </Table.Th>
                <Table.Th style={{ width: 60 }}>
                  {t("orchestration.batch", "バッチ")}
                </Table.Th>
                <Table.Th style={{ width: 200 }}>
                  {t("orchestration.actions", "操作")}
                </Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {wls.map((w) => (
                <WorkloadRow
                  key={w.slug}
                  w={w}
                  metrics={metricsBySlug.get(w.slug)}
                  workers={workers}
                  allSlugs={allEnabledSlugs}
                  onStrong={strong}
                  onPatch={(patch) => patchMut.mutate({ w, patch })}
                  onFilterChange={(delta) =>
                    filterMut.mutate({
                      slug: w.slug, delta, workers, allSlugs: allEnabledSlugs,
                    })
                  }
                  pending={patchMut.isPending || filterMut.isPending}
                />
              ))}
            </Table.Tbody>
          </Table>
          {(patchMut.error || filterMut.error) && (
            <Alert color="red" mt="sm">
              {(patchMut.error || filterMut.error)?.toString()}
            </Alert>
          )}
        </Paper>
      </Stack>
    </Box>
  );
}

// ---------------- 行 ----------------

function WorkloadRow({
  w, metrics, workers, allSlugs, onStrong, onPatch, onFilterChange, pending,
}: {
  w: Workload;
  metrics: WlMetrics | undefined;
  workers: WorkerInfo[];
  allSlugs: string[];
  onStrong: (w: Workload, dir: 1 | -1) => void;
  onPatch: (patch: Partial<Workload>) => void;
  onFilterChange: (delta: 1 | -1) => void;
  pending: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

  // 流量バー: 0..30 /min を 0..100% にマップ (簡易)
  const flowPct = Math.min(100, Math.round(((metrics?.throughput_min ?? 0) / 30) * 100));
  const flowColor =
    (metrics?.throughput_min ?? 0) < 0.1
      ? "gray"
      : (metrics?.throughput_min ?? 0) < 5 ? "yellow" : "green";

  return (
    <>
      <Table.Tr>
        <Table.Td>
          <Group gap={6} wrap="nowrap">
            <ActionIcon
              size="sm"
              variant="subtle"
              onClick={() => setExpanded((v) => !v)}
              aria-label="expand"
            >
              {expanded ? <IconChevronUp size={14} /> : <IconChevronDown size={14} />}
            </ActionIcon>
            {w.supervisor_enabled === false && (
              <Tooltip label="supervisor 自動制御 OFF (= 手動値が固定される)">
                <IconRobotOff size={14} color="var(--mantine-color-red-6)" />
              </Tooltip>
            )}
            <Text size="sm" fw={600}>
              {w.slug}
            </Text>
          </Group>
        </Table.Td>

        <Table.Td>
          <Stack gap={2}>
            <Text size="xs" c="dimmed">
              {fmtNum(metrics?.throughput_min ?? 0)} /min
            </Text>
            <Progress value={flowPct} color={flowColor} size="sm" />
          </Stack>
        </Table.Td>

        <Table.Td>
          <Badge color={(metrics?.backlog ?? 0) > 100 ? "orange" : "gray"} variant="light">
            {fmtNum(metrics?.backlog ?? 0, 0)}
          </Badge>
        </Table.Td>

        <Table.Td>
          <Group gap={4}>
            <IconUsers size={14} />
            <Text size="sm">{metrics?.active_workers ?? 0}</Text>
          </Group>
        </Table.Td>

        <Table.Td>
          <Badge variant="filled" color="blue" size="sm">
            {w.priority}
          </Badge>
        </Table.Td>

        <Table.Td>
          <Badge variant="light" size="sm">
            {w.batch_size}
          </Badge>
        </Table.Td>

        <Table.Td>
          <Group gap={4} wrap="nowrap">
            <Tooltip label="流量を弱める (priority −10)">
              <Button
                size="xs"
                variant="light"
                color="red"
                leftSection={<IconMinus size={12} />}
                onClick={() => onStrong(w, -1)}
                disabled={pending || w.priority <= 0}
              >
                弱
              </Button>
            </Tooltip>
            <Tooltip label="流量を強める (priority +10)">
              <Button
                size="xs"
                variant="light"
                color="green"
                leftSection={<IconBolt size={12} />}
                onClick={() => onStrong(w, 1)}
                disabled={pending || w.priority >= 200}
              >
                強
              </Button>
            </Tooltip>
          </Group>
        </Table.Td>
      </Table.Tr>
      <Table.Tr>
        <Table.Td colSpan={7} style={{ padding: 0, border: 0 }}>
          <Collapse in={expanded}>
            <FineControls
              w={w}
              workers={workers}
              allSlugs={allSlugs}
              onPatch={onPatch}
              onFilterChange={onFilterChange}
              pending={pending}
            />
          </Collapse>
        </Table.Td>
      </Table.Tr>
    </>
  );
}

// ---------------- 詳細つまみ (折りたたみ) ----------------

function FineControls({
  w, workers, allSlugs, onPatch, onFilterChange, pending,
}: {
  w: Workload;
  workers: WorkerInfo[];
  allSlugs: string[];
  onPatch: (patch: Partial<Workload>) => void;
  onFilterChange: (delta: 1 | -1) => void;
  pending: boolean;
}) {
  const { t } = useTranslation();
  void allSlugs;
  void workers;
  return (
    <Box p="sm" style={{ background: "var(--mantine-color-default-hover)" }}>
      <Group gap="lg" wrap="wrap">
        <Knob
          icon={<IconUsers size={14} />}
          label={t("orchestration.knob_workers", "担当 worker 数")}
          value={`${w.host_affinity.length === 0 ? "全 host" : w.host_affinity.join(",")}`}
          hint={t("orchestration.knob_workers_hint",
            "+/- で host 別に worker filter を増減します")}
          onDown={() => onFilterChange(-1)}
          onUp={() => onFilterChange(1)}
          disabled={pending}
        />
        <Knob
          icon={<IconStack2 size={14} />}
          label={t("orchestration.knob_batch", "並列バッチ規模")}
          value={String(w.batch_size)}
          hint={t("orchestration.knob_batch_hint",
            "1 claim で何件まとめて処理するか。 多いほど GPU batch 推論等が速くなる")}
          onDown={() => onPatch({ batch_size: Math.max(1, w.batch_size - 1) })}
          onUp={() => onPatch({ batch_size: Math.min(1024, w.batch_size + 1) })}
          disabled={pending}
        />
        <Knob
          icon={<IconBolt size={14} />}
          label={t("orchestration.knob_priority", "優先度")}
          value={String(w.priority)}
          hint={t("orchestration.knob_priority_hint",
            "高いほど他より先に claim される(= preempt 対象)。 0-200")}
          onDown={() => onPatch({ priority: Math.max(0, w.priority - 10) })}
          onUp={() => onPatch({ priority: Math.min(200, w.priority + 10) })}
          disabled={pending}
        />
        <Knob
          icon={<IconAdjustments size={14} />}
          label={t("orchestration.knob_lease", "lease 周期 (秒)")}
          value={String(w.lease_secs)}
          hint={t("orchestration.knob_lease_hint",
            "claim の有効期限。 短いほど取りこぼし回復が早く、 長いほど overhead 低い")}
          onDown={() => onPatch({ lease_secs: Math.max(15, w.lease_secs - 30) })}
          onUp={() => onPatch({ lease_secs: Math.min(86400, w.lease_secs + 30) })}
          disabled={pending}
        />
      </Group>
      <Group gap="xs" mt="sm">
        <Text size="xs" c="dimmed">
          {t("orchestration.host_affinity", "Host affinity")}:
        </Text>
        <Code style={{ fontSize: 11 }}>
          {w.host_affinity.length === 0
            ? t("orchestration.all_hosts", "全 host 対象")
            : w.host_affinity.join(", ")}
        </Code>
      </Group>
    </Box>
  );
}

function Knob({
  icon, label, value, hint, onDown, onUp, disabled,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint?: string;
  onDown: () => void;
  onUp: () => void;
  disabled: boolean;
}) {
  return (
    <Tooltip label={hint ?? ""} disabled={!hint} multiline w={260}>
      <Box style={{ minWidth: 160 }}>
        <Group gap={4} mb={2}>
          {icon}
          <Text size="xs" c="dimmed">
            {label}
          </Text>
        </Group>
        <Group gap={4}>
          <ActionIcon
            size="sm"
            variant="light"
            color="red"
            onClick={onDown}
            disabled={disabled}
          >
            <IconMinus size={12} />
          </ActionIcon>
          <Text fw={600} size="sm" style={{ minWidth: 60, textAlign: "center" }}>
            {value}
          </Text>
          <ActionIcon
            size="sm"
            variant="light"
            color="green"
            onClick={onUp}
            disabled={disabled}
          >
            <IconPlus size={12} />
          </ActionIcon>
        </Group>
      </Box>
    </Tooltip>
  );
}

// 未使用 import 警告抑止 (= 将来の history 表示で使う予定)
void (null as unknown as RunRecord);
