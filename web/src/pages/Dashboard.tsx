import { Badge, Box, Card, Code, Group, SimpleGrid, Stack, Text } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { api } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState } from "@/components/states";

interface GpuPoint {
  ts: string;
  temp_c: number | null;
  util_pct: number | null;
  mem_used_mb: number | null;
}

type MetricsResponse = {
  workers: Record<string, Record<string, GpuPoint[]>>;
  since_minutes: number;
};

function TempSparkline({ points, label }: { points: GpuPoint[]; label: string }) {
  if (points.length < 2) {
    return <Text size="xs" c="dimmed">(データ収集中…)</Text>;
  }
  const W = 280, H = 60, PAD = 4;
  const temps = points.map((p) => p.temp_c).filter((t): t is number => typeof t === "number");
  if (temps.length === 0) return <Text size="xs" c="dimmed">(no temp)</Text>;
  const minT = Math.min(...temps, 30);
  const maxT = Math.max(...temps, 80);
  const range = Math.max(1, maxT - minT);
  const x = (i: number) => PAD + (i / (points.length - 1)) * (W - PAD * 2);
  const y = (t: number) => H - PAD - ((t - minT) / range) * (H - PAD * 2);
  const d = points
    .map((p, i) => {
      if (typeof p.temp_c !== "number") return "";
      return `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p.temp_c).toFixed(1)}`;
    })
    .filter(Boolean)
    .join(" ");
  const last = temps[temps.length - 1];
  const color = last >= 80 ? "#ef4444" : last >= 70 ? "#f59e0b" : "#10b981";
  return (
    <Stack gap={2}>
      <Group justify="space-between">
        <Text size="xs" c="dimmed">{label}</Text>
        <Text size="xs" fw={600} c={color}>{last.toFixed(0)}℃</Text>
      </Group>
      <svg width={W} height={H} style={{ display: "block" }}>
        <path d={d} stroke={color} strokeWidth={1.5} fill="none" />
      </svg>
      <Group justify="space-between">
        <Text size="xs" c="dimmed">{minT.toFixed(0)}℃</Text>
        <Text size="xs" c="dimmed">{maxT.toFixed(0)}℃</Text>
      </Group>
    </Stack>
  );
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

function RunningPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });

  return (
    <Card withBorder radius="md" padding="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>実行中</Text>
        <Badge color={q.data && q.data.running.length > 0 ? "blue" : "gray"} variant="light">
          {q.data?.running.length ?? "—"} 件
        </Badge>
      </Group>
      {q.error && <ErrorState error={q.error} onRetry={() => q.refetch()} />}
      {q.data && q.data.running.length === 0 && !q.isLoading && (
        <EmptyState
          title="今は何も走っていません"
          description="キューに pending タスクが来ると ワーカーが拾います。"
          minHeight={120}
        />
      )}
      {q.data && q.data.running.length > 0 && (
        <Stack gap={4}>
          {q.data.running.slice(0, 8).map((r) => (
            <Group key={r.id} gap="xs" wrap="nowrap" justify="space-between">
              <Group gap={6} wrap="nowrap" style={{ minWidth: 0 }}>
                <Badge color="blue" variant="dot" size="sm">{r.workload_slug}</Badge>
                <Code style={{ fontSize: 11 }}>{r.pk}</Code>
              </Group>
              <Group gap={6} wrap="nowrap">
                <Text size="xs" c="dimmed">{r.worker_id}</Text>
                <Badge size="sm" variant="default">{relativeAge(r.started_at)}</Badge>
              </Group>
            </Group>
          ))}
          {q.data.running.length > 8 && (
            <Text size="xs" c="dimmed">…他 {q.data.running.length - 8} 件</Text>
          )}
        </Stack>
      )}
    </Card>
  );
}

function RecentFailuresPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });

  return (
    <Card withBorder radius="md" padding="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>最近の失敗</Text>
        <Badge color={q.data && q.data.recent_failures.length > 0 ? "red" : "gray"} variant="light">
          {q.data?.recent_failures.length ?? "—"} 件
        </Badge>
      </Group>
      {q.data && q.data.recent_failures.length === 0 && !q.isLoading && (
        <EmptyState
          title="失敗はありません"
          description="直近 run に failure 無し。順調です。"
          minHeight={120}
        />
      )}
      {q.data && q.data.recent_failures.length > 0 && (
        <Stack gap={6}>
          {q.data.recent_failures.slice(0, 6).map((f) => (
            <Stack key={f.id} gap={2}>
              <Group gap={6} wrap="nowrap" justify="space-between">
                <Group gap={6} wrap="nowrap" style={{ minWidth: 0 }}>
                  <Badge color="red" variant="light" size="sm">{f.workload_slug}</Badge>
                  <Code style={{ fontSize: 11 }}>{f.pk}</Code>
                </Group>
                <Text size="xs" c="dimmed">{relativeAge(f.started_at)} 前</Text>
              </Group>
              {f.reason && (
                <Text size="xs" c="red.7" lineClamp={1}>{f.reason}</Text>
              )}
            </Stack>
          ))}
        </Stack>
      )}
    </Card>
  );
}

function QueueDepthsPanel() {
  const q = useQuery({
    queryKey: ["dashboard-overview"],
    queryFn: api.dashboardOverview,
    refetchInterval: 3_000,
  });

  return (
    <Card withBorder radius="md" padding="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>キュー深さ</Text>
        <Badge variant="light">{q.data?.queue_depths.length ?? "—"} workload</Badge>
      </Group>
      {q.data && q.data.queue_depths.length === 0 && !q.isLoading && (
        <EmptyState
          title="enable な workload がありません"
          description="ワークロードを enable にすると ここに pending/claimed が表示されます。"
          minHeight={120}
        />
      )}
      {q.data && q.data.queue_depths.length > 0 && (
        <Stack gap={6}>
          {q.data.queue_depths.map((d) => {
            const pending = d.by_state.pending ?? 0;
            const claimed = d.by_state.claimed ?? 0;
            const failed = d.by_state.failed ?? 0;
            const max = Math.max(1, pending + claimed + failed, 10);
            return (
              <Stack key={d.workload_slug} gap={2}>
                <Group gap={6} justify="space-between">
                  <Text size="xs" fw={600}>{d.workload_slug}</Text>
                  <Group gap={4}>
                    {pending > 0 && <Badge size="xs" color="blue" variant="light">pending {pending}</Badge>}
                    {claimed > 0 && <Badge size="xs" color="yellow" variant="light">claimed {claimed}</Badge>}
                    {failed > 0 && <Badge size="xs" color="red" variant="light">failed {failed}</Badge>}
                    {pending + claimed + failed === 0 && (
                      <Text size="xs" c="dimmed">空</Text>
                    )}
                  </Group>
                </Group>
                <Box style={{ display: "flex", height: 6, borderRadius: 3, overflow: "hidden", background: "var(--mantine-color-default-border)" }}>
                  {pending > 0 && <Box style={{ width: `${(pending / max) * 100}%`, background: "var(--mantine-color-blue-5)" }} />}
                  {claimed > 0 && <Box style={{ width: `${(claimed / max) * 100}%`, background: "var(--mantine-color-yellow-5)" }} />}
                  {failed > 0 && <Box style={{ width: `${(failed / max) * 100}%`, background: "var(--mantine-color-red-5)" }} />}
                </Box>
              </Stack>
            );
          })}
        </Stack>
      )}
    </Card>
  );
}

export default function Dashboard() {
  const { t } = useTranslation();
  const { data, error } = useQuery({
    queryKey: ["status"],
    queryFn: api.status,
    refetchInterval: 5_000,
  });

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

  return (
    <Stack gap="lg">
      <PageHeader title={t("dashboard.title")} />

      {error && <ErrorState error={error} onRetry={() => location.reload()} />}

      <SimpleGrid cols={{ base: 1, md: 3 }}>
        <RunningPanel />
        <RecentFailuresPanel />
        <QueueDepthsPanel />
      </SimpleGrid>

      {data && (
        <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }}>
          <Card withBorder radius="md" padding="md">
            <Text size="sm" c="dimmed">
              {t("dashboard.version")}
            </Text>
            <Text fw={700} size="xl" mt={4}>
              {data.version}
            </Text>
            <Badge mt="sm" variant="light" color={data.mode === "dev" ? "yellow" : "green"}>
              {data.mode}
            </Badge>
          </Card>

          <Card withBorder radius="md" padding="md">
            <Text size="sm" c="dimmed">
              {t("dashboard.db")}
            </Text>
            <Code mt={4} block>
              {data.db_url}
            </Code>
          </Card>

          <Card withBorder radius="md" padding="md">
            <Text size="sm" c="dimmed">
              {t("dashboard.time")}
            </Text>
            <Code mt={4} block>
              {data.now}
            </Code>
          </Card>
        </SimpleGrid>
      )}

      {metrics && Object.keys(metrics.workers).length > 0 && (
        <Stack gap="sm">
          <Group justify="space-between" align="baseline">
            <Text fw={600} size="md">GPU 温度 <Text span size="xs" c="dimmed">(過去 {metrics.since_minutes} 分)</Text></Text>
            <Text size="xs" c="dimmed">10s ごと更新</Text>
          </Group>
          <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }}>
            {Object.entries(metrics.workers)
              .filter(([workerId]) => workerHostMap.has(workerId))
              .map(([workerId, gpus]) => {
              const host = workerHostMap.get(workerId) ?? workerId;
              return (
                <Card key={workerId} withBorder radius="md" padding="md">
                  <Text size="sm" fw={600}>{host}</Text>
                  <Text size="xs" c="dimmed" mb="xs">
                    <Code>{workerId}</Code>
                  </Text>
                  <Stack gap="md">
                    {Object.entries(gpus).map(([gpuIdx, points]) => (
                      <TempSparkline
                        key={gpuIdx}
                        points={points}
                        label={`GPU ${gpuIdx}`}
                      />
                    ))}
                  </Stack>
                </Card>
              );
            })}
          </SimpleGrid>
        </Stack>
      )}
    </Stack>
  );
}
