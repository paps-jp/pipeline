import {
  Badge,
  Box,
  Card,
  Code,
  Group,
  Indicator,
  Progress,
  SimpleGrid,
  Stack,
  Text,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import {
  IconActivity,
  IconAlertTriangle,
  IconCircleCheck,
  IconCpu,
  IconFlame,
  IconListNumbers,
  IconStack2,
} from "@tabler/icons-react";
import { useTranslation } from "react-i18next";

import { api } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { ErrorState } from "@/components/states";

interface GpuPoint {
  ts: string;
  temp_c: number | null;
  util_pct: number | null;
  mem_used_mb: number | null;
  mem_util_pct: number | null;
  mem_total_mb: number | null;
  power_w: number | null;
  sm_clock_mhz: number | null;
  mem_clock_mhz: number | null;
}

type MetricsResponse = {
  workers: Record<string, Record<string, GpuPoint[]>>;
  since_minutes: number;
};

// ---- 共通スタイル -----------------------------------------------------------

const ELLIPSIS: React.CSSProperties = {
  minWidth: 0,
  maxWidth: "100%",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  display: "inline-block",
};

// 実行中パネルに並べる最大行数 (=空きスペースに合わせ 5→20 へ拡張、 余りは "...他 N 件")
const RUNNING_PANEL_ROWS = 20;

// Light / Dark 両対応のヒーローパネル スタイル + 背景アイコン。
// Mantine v7 は `[data-mantine-color-scheme]` を <html> に付与するのでこのセレクタで分岐できる。
// 各 tone の light は色付き淡背景、 dark は同色の rgba 低透明 で控えめにする。
const DASHBOARD_STYLES = `
.paprika-hero {
  position: relative;
  overflow: hidden;
  transition: background 200ms ease, border-color 200ms ease;
}

/* ---- Light theme ---- */
.paprika-hero[data-tone="indigo"] {
  background: linear-gradient(135deg, var(--mantine-color-indigo-0) 0%, var(--mantine-color-blue-0) 100%);
  border-color: var(--mantine-color-indigo-2);
}
.paprika-hero[data-tone="red"] {
  background: linear-gradient(135deg, var(--mantine-color-red-0) 0%, var(--mantine-color-pink-0) 100%);
  border-color: var(--mantine-color-red-2);
}
.paprika-hero[data-tone="green"] {
  background: linear-gradient(135deg, var(--mantine-color-green-0) 0%, var(--mantine-color-teal-0) 100%);
  border-color: var(--mantine-color-green-2);
}
.paprika-hero[data-tone="violet"] {
  background: linear-gradient(135deg, var(--mantine-color-violet-0) 0%, var(--mantine-color-indigo-0) 100%);
  border-color: var(--mantine-color-violet-2);
}

/* ---- Dark theme: 暗背景の上に色を rgba 低透明で乗せる ---- */
[data-mantine-color-scheme="dark"] .paprika-hero[data-tone="indigo"] {
  background:
    radial-gradient(circle at top right, rgba(99, 102, 241, 0.22), transparent 60%),
    linear-gradient(135deg, rgba(99, 102, 241, 0.10) 0%, rgba(59, 130, 246, 0.04) 100%);
  border-color: rgba(99, 102, 241, 0.35);
}
[data-mantine-color-scheme="dark"] .paprika-hero[data-tone="red"] {
  background:
    radial-gradient(circle at top right, rgba(239, 68, 68, 0.24), transparent 60%),
    linear-gradient(135deg, rgba(239, 68, 68, 0.10) 0%, rgba(236, 72, 153, 0.05) 100%);
  border-color: rgba(239, 68, 68, 0.38);
}
[data-mantine-color-scheme="dark"] .paprika-hero[data-tone="green"] {
  background:
    radial-gradient(circle at top right, rgba(34, 197, 94, 0.20), transparent 60%),
    linear-gradient(135deg, rgba(34, 197, 94, 0.08) 0%, rgba(20, 184, 166, 0.04) 100%);
  border-color: rgba(34, 197, 94, 0.32);
}
[data-mantine-color-scheme="dark"] .paprika-hero[data-tone="violet"] {
  background:
    radial-gradient(circle at top right, rgba(139, 92, 246, 0.22), transparent 60%),
    linear-gradient(135deg, rgba(139, 92, 246, 0.10) 0%, rgba(99, 102, 241, 0.04) 100%);
  border-color: rgba(139, 92, 246, 0.35);
}

/* inactive (= 何も走ってない/0件) は両モードで素のカード */
.paprika-hero[data-tone="inactive"] {
  background: var(--mantine-color-body);
  border-color: var(--mantine-color-default-border);
}

/* 大型背景アイコン */
.paprika-bg-icon {
  position: absolute;
  right: -18px;
  bottom: -22px;
  opacity: 0.10;
  pointer-events: none;
}
.paprika-bg-icon[data-active="false"] { opacity: 0.05; }
[data-mantine-color-scheme="dark"] .paprika-bg-icon { opacity: 0.16; }
[data-mantine-color-scheme="dark"] .paprika-bg-icon[data-active="false"] { opacity: 0.08; }

/* 失敗理由 左ボーダー: dark 時に暗背景で消えないように調整 */
.paprika-fail-reason {
  padding-left: 6px;
  border-left: 2px solid var(--mantine-color-red-3);
}
[data-mantine-color-scheme="dark"] .paprika-fail-reason {
  border-left-color: var(--mantine-color-red-7);
}

/* ステータスドット (生 div の純色) を dark でも見えるように */
.paprika-status-dot {
  width: 8px;
  height: 8px;
  border-radius: 4px;
  display: inline-block;
  flex-shrink: 0;
}

/* 実行中 / 失敗 リストの行間 罫線 (= テーマ追従の薄ボーダー) */
.paprika-divided-rows > * + * {
  border-top: 1px solid var(--mantine-color-default-border);
  padding-top: 6px;
}
`;

// ---- スパークライン --------------------------------------------------------

function MetricSparkline({
  points,
  label,
  accessor,
  unit,
  fmt,
  scaleMin,
  scaleMax,
  colorOf,
  gradientId,
}: {
  points: GpuPoint[];
  label: string;
  accessor: (p: GpuPoint) => number | null;
  unit: string;
  fmt?: (v: number) => string;
  scaleMin?: number;
  scaleMax?: number;
  colorOf?: (last: number) => string;
  gradientId: string;
}) {
  if (points.length < 2) {
    return <Text size="xs" c="dimmed">(データ収集中…)</Text>;
  }
  const W = 280, H = 56, PAD = 4;
  const vals = points.map(accessor).filter((v): v is number => typeof v === "number");
  if (vals.length === 0) return <Text size="xs" c="dimmed">(no {label})</Text>;
  const minV = Math.min(...vals, scaleMin ?? Infinity);
  const maxV = Math.max(...vals, scaleMax ?? -Infinity);
  const range = Math.max(1, maxV - minV);
  const x = (i: number) => PAD + (i / (points.length - 1)) * (W - PAD * 2);
  const y = (v: number) => H - PAD - ((v - minV) / range) * (H - PAD * 2);

  let lineD = "";
  let lastValidIdx = -1;
  let firstValidIdx = -1;
  points.forEach((p, i) => {
    const v = accessor(p);
    if (typeof v !== "number") return;
    if (firstValidIdx < 0) firstValidIdx = i;
    lastValidIdx = i;
    lineD += `${lineD ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
  });
  const lastV = vals[vals.length - 1];
  const formatter = fmt ?? ((v) => v.toFixed(0));
  const color = colorOf ? colorOf(lastV) : "#6366f1";
  const areaD =
    lastValidIdx >= 0
      ? `${lineD} L${x(lastValidIdx).toFixed(1)},${(H - PAD).toFixed(1)} L${x(firstValidIdx).toFixed(1)},${(H - PAD).toFixed(1)} Z`
      : "";

  // "現在値 / 上限" 表示用の最大値: scale 上限を最優先 (温度=80℃, 使用率=100% 等)、
  // 上限定義の無いメトリクス (電力 W / VRAM GB) は観測ピークを使う。
  const maxRef = scaleMax ?? maxV;

  return (
    <Stack gap={2}>
      <Group justify="space-between" wrap="nowrap">
        <Text size="xs" c="dimmed" fw={500}>{label}</Text>
        <Text size="sm" fw={700} style={{ color, fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
          {formatter(lastV)}
          <Text span size="xs" c="dimmed" fw={500}> / {formatter(maxRef)}</Text>
          <Text span size="xs" c="dimmed" ml={2}>{unit.trim()}</Text>
        </Text>
      </Group>
      <svg width={W} height={H} style={{ display: "block", width: "100%" }} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.32" />
            <stop offset="100%" stopColor={color} stopOpacity="0" />
          </linearGradient>
        </defs>
        {areaD && <path d={areaD} fill={`url(#${gradientId})`} />}
        <path d={lineD} stroke={color} strokeWidth={1.6} fill="none" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </Stack>
  );
}

// ---- ヘルパ ---------------------------------------------------------------

// slug → name のマップ (= dashboard で「名前」表示用)。 React Query が同 queryKey を dedup するので
// 各 panel から呼んでも HTTP は 1 本。
function useWorkloadNameMap(): Map<string, string> {
  const q = useQuery({
    queryKey: ["workloads-name-map"],
    queryFn: api.listWorkloads,
    refetchInterval: 30_000,
    staleTime: 10_000,
  });
  return new Map((q.data?.workloads ?? []).map((w) => [w.slug, w.name]));
}

function relativeAge(iso: string): string {
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return "—";
  const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  return `${Math.floor(diff / 86400)}d`;
}

// ---- パネル: 実行中 -------------------------------------------------------

function RunningPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });
  const nameMap = useWorkloadNameMap();
  const count = q.data?.running.length ?? 0;
  const live = count > 0;

  return (
    <Card
      padding="lg"
      radius="lg"
      className="paprika-hero"
      data-tone={live ? "indigo" : "inactive"}
    >
      <IconActivity
        size={140}
        stroke={1.2}
        className="paprika-bg-icon"
        data-active={live}
        style={{ color: "var(--mantine-color-indigo-6)" }}
      />
      <Stack gap={6} style={{ position: "relative" }}>
        <Group justify="space-between" align="center">
          <Group gap={8} align="center">
            {live ? (
              <Indicator processing size={8} color="indigo" offset={0} position="middle-end">
                <span />
              </Indicator>
            ) : (
              <span className="paprika-status-dot" style={{ background: "var(--mantine-color-dimmed)" }} />
            )}
            <Text fw={700} size="sm" tt="uppercase" c="dimmed" style={{ letterSpacing: 0.6 }}>実行中</Text>
          </Group>
          <Text size="xs" c="dimmed">3s 更新</Text>
        </Group>
        <Group align="baseline" gap={6}>
          <Text fw={800} c={live ? "indigo" : "dimmed"} style={{ fontSize: 44, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>
            {q.isLoading ? "—" : count}
          </Text>
          <Text size="sm" c="dimmed" fw={500}>件</Text>
        </Group>
        {q.error && <ErrorState error={q.error} onRetry={() => q.refetch()} />}
        {!q.error && count === 0 && !q.isLoading && (
          <Text size="xs" c="dimmed">キューに pending が来ると ワーカーが拾います。</Text>
        )}
        {count > 0 && (
          <Stack gap={6} mt={4} className="paprika-divided-rows">
            {q.data!.running.slice(0, RUNNING_PANEL_ROWS).map((r) => (
              <Group key={r.id} gap="xs" wrap="nowrap" justify="space-between">
                <Group gap={6} wrap="nowrap" style={{ minWidth: 0, flex: 1 }}>
                  <Badge color="indigo" variant="light" size="sm" maw={150} style={{ flexShrink: 0 }} title={r.workload_slug}>
                    {nameMap.get(r.workload_slug) ?? r.workload_slug}
                  </Badge>
                  <Code style={{ fontSize: 11, ...ELLIPSIS }}>{r.pk}</Code>
                </Group>
                <Group gap={6} wrap="nowrap" style={{ flexShrink: 0 }}>
                  <Text size="xs" c="dimmed" truncate maw={110}>{r.worker_id}</Text>
                  <Badge size="sm" variant="default" style={{ fontVariantNumeric: "tabular-nums" }}>{relativeAge(r.started_at)}</Badge>
                </Group>
              </Group>
            ))}
            {count > RUNNING_PANEL_ROWS && (
              <Text size="xs" c="dimmed">…他 {count - RUNNING_PANEL_ROWS} 件</Text>
            )}
          </Stack>
        )}
      </Stack>
    </Card>
  );
}

// ---- パネル: 最近の失敗 ---------------------------------------------------

function RecentFailuresPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });
  const nameMap = useWorkloadNameMap();
  const count = q.data?.recent_failures.length ?? 0;
  const bad = count > 0;

  return (
    <Card
      padding="lg"
      radius="lg"
      className="paprika-hero"
      data-tone={bad ? "red" : "green"}
    >
      {bad ? (
        <IconAlertTriangle
          size={140}
          stroke={1.2}
          className="paprika-bg-icon"
          data-active={true}
          style={{ color: "var(--mantine-color-red-6)" }}
        />
      ) : (
        <IconCircleCheck
          size={140}
          stroke={1.2}
          className="paprika-bg-icon"
          data-active={true}
          style={{ color: "var(--mantine-color-green-6)" }}
        />
      )}
      <Stack gap={6} style={{ position: "relative" }}>
        <Group justify="space-between" align="center">
          <Group gap={8} align="center">
            <span
              className="paprika-status-dot"
              style={{
                background: bad
                  ? "var(--mantine-color-red-6)"
                  : "var(--mantine-color-green-6)",
              }}
            />
            <Text fw={700} size="sm" tt="uppercase" c="dimmed" style={{ letterSpacing: 0.6 }}>最近の失敗</Text>
          </Group>
          <Text size="xs" c="dimmed">直近 10 件</Text>
        </Group>
        <Group align="baseline" gap={6}>
          <Text fw={800} c={bad ? "red" : "green"} style={{ fontSize: 44, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>
            {q.isLoading ? "—" : count}
          </Text>
          <Text size="sm" c="dimmed" fw={500}>件</Text>
        </Group>
        {!bad && !q.isLoading && (
          <Text size="xs" c="dimmed">直近 run に failure 無し。順調です。</Text>
        )}
        {bad && (
          <Stack gap={8} mt={4} className="paprika-divided-rows">
            {q.data!.recent_failures.slice(0, 5).map((f) => (
              <Stack key={f.id} gap={2}>
                <Group gap={6} wrap="nowrap" justify="space-between">
                  <Group gap={6} wrap="nowrap" style={{ minWidth: 0, flex: 1 }}>
                    <Badge color="red" variant="light" size="sm" maw={150} style={{ flexShrink: 0 }} title={f.workload_slug}>
                      {nameMap.get(f.workload_slug) ?? f.workload_slug}
                    </Badge>
                    <Code style={{ fontSize: 11, ...ELLIPSIS }}>{f.pk}</Code>
                  </Group>
                  <Text size="xs" c="dimmed" style={{ flexShrink: 0, fontVariantNumeric: "tabular-nums" }}>{relativeAge(f.started_at)} 前</Text>
                </Group>
                {f.reason && (
                  <Text size="xs" c="red" lineClamp={1} className="paprika-fail-reason">{f.reason}</Text>
                )}
              </Stack>
            ))}
          </Stack>
        )}
      </Stack>
    </Card>
  );
}

// ---- パネル: キュー深さ ---------------------------------------------------

function QueueDepthsPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });
  const nameMap = useWorkloadNameMap();
  const depths = q.data?.queue_depths ?? [];
  const totalPending = depths.reduce((acc, d) => acc + (d.by_state.pending ?? 0), 0);
  const totalClaimed = depths.reduce((acc, d) => acc + (d.by_state.claimed ?? 0), 0);
  const live = totalPending > 0;

  return (
    <Card
      padding="lg"
      radius="lg"
      className="paprika-hero"
      data-tone={live ? "violet" : "inactive"}
    >
      <IconStack2
        size={140}
        stroke={1.2}
        className="paprika-bg-icon"
        data-active={live}
        style={{ color: "var(--mantine-color-violet-6)" }}
      />
      <Stack gap={6} style={{ position: "relative" }}>
        <Group justify="space-between" align="center">
          <Group gap={8} align="center">
            <span
              className="paprika-status-dot"
              style={{
                background: live
                  ? "var(--mantine-color-violet-6)"
                  : "var(--mantine-color-dimmed)",
              }}
            />
            <Text fw={700} size="sm" tt="uppercase" c="dimmed" style={{ letterSpacing: 0.6 }}>キュー深さ</Text>
          </Group>
          <Text size="xs" c="dimmed">{depths.length} workload</Text>
        </Group>
        <Group align="baseline" gap={10}>
          <Text fw={800} c={live ? "violet" : "dimmed"} style={{ fontSize: 44, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>
            {q.isLoading ? "—" : totalPending}
          </Text>
          <Text size="sm" c="dimmed" fw={500}>pending</Text>
          {totalClaimed > 0 && (
            <Text size="xs" c="yellow" fw={600} style={{ fontVariantNumeric: "tabular-nums" }}>
              +{totalClaimed} claimed
            </Text>
          )}
        </Group>
        {depths.length === 0 && !q.isLoading && (
          <Text size="xs" c="dimmed">enable な workload がありません。</Text>
        )}
        {depths.length > 0 && (
          <Stack gap={6} mt={4}>
            {depths.map((d) => {
              const pending = d.by_state.pending ?? 0;
              const claimed = d.by_state.claimed ?? 0;
              const failed = d.by_state.failed ?? 0;
              const total = pending + claimed + failed;
              const max = Math.max(1, total, 10);
              return (
                <Stack key={d.workload_slug} gap={2}>
                  <Group gap={6} justify="space-between" wrap="nowrap">
                    <Text size="xs" fw={600} truncate style={{ minWidth: 0, flex: 1 }} title={d.workload_slug}>{nameMap.get(d.workload_slug) ?? d.workload_slug}</Text>
                    <Group gap={4} style={{ flexShrink: 0 }}>
                      {pending > 0 && <Badge size="xs" color="indigo" variant="light">{pending}</Badge>}
                      {claimed > 0 && <Badge size="xs" color="yellow" variant="light">{claimed}</Badge>}
                      {failed > 0 && <Badge size="xs" color="red" variant="light">{failed}</Badge>}
                      {total === 0 && (
                        <Text size="xs" c="dimmed">空</Text>
                      )}
                    </Group>
                  </Group>
                  <Box style={{ display: "flex", height: 5, borderRadius: 3, overflow: "hidden", background: "var(--mantine-color-default-border)" }}>
                    {pending > 0 && <Box style={{ width: `${(pending / max) * 100}%`, background: "var(--mantine-color-indigo-5)" }} />}
                    {claimed > 0 && <Box style={{ width: `${(claimed / max) * 100}%`, background: "var(--mantine-color-yellow-5)" }} />}
                    {failed > 0 && <Box style={{ width: `${(failed / max) * 100}%`, background: "var(--mantine-color-red-5)" }} />}
                  </Box>
                </Stack>
              );
            })}
          </Stack>
        )}
      </Stack>
    </Card>
  );
}

// ---- GPU カード ----------------------------------------------------------

function GpuHostCard({
  baseHost,
  workerIds,
  gpus,
}: {
  baseHost: string;
  workerIds: string[];
  gpus: Record<string, GpuPoint[]>;
}) {
  const latestTemps: number[] = [];
  const latestUtils: number[] = [];
  for (const points of Object.values(gpus)) {
    const last = points[points.length - 1];
    if (last?.temp_c != null) latestTemps.push(last.temp_c);
    if (last?.util_pct != null) latestUtils.push(last.util_pct);
  }
  const hottest = latestTemps.length > 0 ? Math.max(...latestTemps) : null;
  const busiest = latestUtils.length > 0 ? Math.max(...latestUtils) : 0;
  const tempColor =
    hottest == null ? "gray" : hottest >= 75 ? "red" : hottest >= 65 ? "orange" : "teal";
  const busy = busiest >= 30;

  return (
    <Card padding="lg" radius="lg" withBorder>
      <Stack gap={4}>
        <Group justify="space-between" align="center" wrap="nowrap">
          <Group gap={8} align="center" wrap="nowrap" style={{ minWidth: 0 }}>
            <IconCpu size={16} stroke={1.6} color={`var(--mantine-color-${tempColor}-6)`} />
            <Text fw={700} size="sm" truncate>{baseHost}</Text>
          </Group>
          <Group gap={6} wrap="nowrap" style={{ flexShrink: 0 }}>
            {hottest != null && (
              <Badge size="sm" variant="light" color={tempColor} leftSection={<IconFlame size={11} stroke={1.8} />} style={{ fontVariantNumeric: "tabular-nums" }}>
                {hottest.toFixed(0)}℃
              </Badge>
            )}
            <Badge size="sm" variant="default">{workerIds.length} w</Badge>
          </Group>
        </Group>
        <Progress.Root size="xs" radius="xl" mt={2}>
          <Progress.Section value={busiest} color={busy ? "indigo" : "gray.4"} />
        </Progress.Root>
      </Stack>
      <Stack gap="md" mt="md">
        {Object.entries(gpus).map(([gpuIdx, points]) => (
          <Stack key={gpuIdx} gap={6}>
            <Group justify="space-between" align="center">
              <Badge size="xs" variant="dot" color="indigo">GPU {gpuIdx}</Badge>
              <Text size="10px" c="dimmed">{points.length} samples</Text>
            </Group>
            <MetricSparkline points={points} label="温度" unit="℃"
              gradientId={`g-${baseHost}-${gpuIdx}-temp`}
              accessor={(p) => p.temp_c}
              scaleMin={30} scaleMax={80}
              colorOf={(t) => (t >= 75 ? "#ef4444" : t >= 65 ? "#f59e0b" : "#10b981")} />
            <MetricSparkline points={points} label="電力" unit="W"
              gradientId={`g-${baseHost}-${gpuIdx}-pow`}
              accessor={(p) => {
                // hardware sensor / driver bug 除外: 物理上限 (= 民生最高 RTX4090=450W,
                // workstation A6000=300W cap) を逸脱した値は driver telemetry の嘘とみなし
                // chart から除外。 ai-gpu4 (RTX3080) で 432W 出続ける現象への暫定対処。
                return p.power_w !== null && p.power_w > 500 ? null : p.power_w;
              }}
              colorOf={() => "#f59e0b"} />
            <MetricSparkline points={points} label="GPU 使用率" unit="%"
              gradientId={`g-${baseHost}-${gpuIdx}-util`}
              accessor={(p) => p.util_pct}
              scaleMin={0} scaleMax={100}
              colorOf={() => "#6366f1"} />
            <MetricSparkline points={points} label="VRAM" unit=" GB"
              gradientId={`g-${baseHost}-${gpuIdx}-vram`}
              accessor={(p) => p.mem_used_mb}
              fmt={(v) => (v / 1024).toFixed(1)}
              colorOf={() => "#8b5cf6"} />
            <MetricSparkline points={points} label="Mem 帯域" unit="%"
              gradientId={`g-${baseHost}-${gpuIdx}-mbw`}
              accessor={(p) => p.mem_util_pct}
              scaleMin={0} scaleMax={100}
              colorOf={() => "#ec4899"} />
          </Stack>
        ))}
      </Stack>
    </Card>
  );
}

// ---- Dashboard ルート -----------------------------------------------------

export default function Dashboard() {
  const { t } = useTranslation();

  const metricsQ = useQuery({
    queryKey: ["workers-metrics"],
    queryFn: () => api.listWorkersMetrics(30),
    refetchInterval: 10_000,
  });

  const workersQ = useQuery({
    queryKey: ["workers-for-dashboard"],
    queryFn: () => api.listWorkers(),
    refetchInterval: 10_000,
  });

  const metrics = metricsQ.data as MetricsResponse | undefined;
  const workerHostMap = new Map(
    (workersQ.data?.workers ?? []).map((w) => [w.id, w.host]),
  );

  const hostGroups = (() => {
    if (!metrics) return [] as Array<[string, { workerIds: string[]; gpus: Record<string, GpuPoint[]> }]>;
    const byHost = new Map<string, { workerIds: string[]; gpus: Record<string, GpuPoint[]> }>();
    for (const [wid, gpus] of Object.entries(metrics.workers)) {
      const host = workerHostMap.get(wid);
      if (!host) continue;
      const baseHost = host.replace(/-\d+$/, "");
      if (!byHost.has(baseHost)) byHost.set(baseHost, { workerIds: [], gpus: {} });
      const entry = byHost.get(baseHost)!;
      entry.workerIds.push(wid);
      for (const [gpuIdx, points] of Object.entries(gpus)) {
        const cur = entry.gpus[gpuIdx];
        if (!cur || cur.length < points.length) entry.gpus[gpuIdx] = points;
      }
    }
    return Array.from(byHost.entries()).sort(([a], [b]) => a.localeCompare(b));
  })();

  return (
    <Stack gap="lg">
      <style>{DASHBOARD_STYLES}</style>
      <PageHeader title={t("dashboard.title")} />

      <SimpleGrid cols={{ base: 1, md: 3 }} spacing="md">
        <RunningPanel />
        <RecentFailuresPanel />
        <QueueDepthsPanel />
      </SimpleGrid>

      {hostGroups.length > 0 && (
        <Stack gap="sm">
          <Group justify="space-between" align="baseline">
            <Group gap={8} align="center">
              <IconListNumbers size={18} stroke={1.6} color="var(--mantine-color-indigo-6)" />
              <Text fw={700} size="md">
                GPU メトリクス
              </Text>
              <Text size="xs" c="dimmed">
                過去 {metrics?.since_minutes ?? 30} 分 · 10s 更新
              </Text>
            </Group>
            <Badge variant="light" color="indigo">{hostGroups.length} hosts</Badge>
          </Group>
          <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="md">
            {hostGroups.map(([baseHost, entry]) => (
              <GpuHostCard
                key={baseHost}
                baseHost={baseHost}
                workerIds={entry.workerIds}
                gpus={entry.gpus}
              />
            ))}
          </SimpleGrid>
        </Stack>
      )}
    </Stack>
  );
}
