/**
 * WorkloadControlPopover — Flow 図のノードクリックで開くモーダル本体。
 * /orchestration の行展開と同じつまみを 1 workload 分だけ縦に表示する。
 */

import { useMemo } from "react";
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  Group,
  Loader,
  Paper,
  Progress,
  Stack,
  Switch,
  Text,
  Tooltip,
} from "@mantine/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  IconAdjustments,
  IconBolt,
  IconMinus,
  IconPlus,
  IconRobot,
  IconRobotOff,
  IconStack2,
  IconUsers,
} from "@tabler/icons-react";

import { api, type WorkerInfo, type Workload } from "@/api/client";

function parseHostFromWid(wid: string): string {
  if (!wid.startsWith("w_")) return wid;
  const parts = wid.slice(2).split("_");
  return parts.length >= 2 ? `${parts[0]}-${parts[1]}` : wid;
}

const PUT_STRIP = new Set([
  "slug", "queue_table", "created_at", "updated_at",
  "observed_depth", "observed_age_secs", "observed_rate",
  "observed_vram_mb_peak", "observed_vram_sample_count",
  "observed_vram_updated_at", "created_by", "schema_version",
]);

function buildPut(w: Workload, patch: Partial<Workload>): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(w)) if (!PUT_STRIP.has(k)) body[k] = v;
  Object.assign(body, patch);
  return body;
}

export default function WorkloadControlPopover({ slug }: { slug: string }) {
  const qc = useQueryClient();
  const wlQ = useQuery({
    queryKey: ["wl", slug],
    queryFn: () => api.getWorkload(slug),
    refetchInterval: 3_000,
  });
  const workersQ = useQuery({
    queryKey: ["workers-for-popover"],
    queryFn: () => api.listWorkers(),
    refetchInterval: 5_000,
  });
  const runsQ = useQuery({
    queryKey: ["runs-for-popover"],
    queryFn: () => api.listRecentRuns(500),
    refetchInterval: 5_000,
  });
  const supQ = useQuery({
    queryKey: ["sup-for-popover"],
    queryFn: () => api.getWorkload("pipeline-supervisor"),
    refetchInterval: 30_000,
    retry: false,
  });

  const w = wlQ.data;
  const workers = workersQ.data?.workers ?? [];

  const metrics = useMemo(() => {
    if (!w) return null;
    const now = Date.now();
    const cutoff = now - 5 * 60 * 1000;
    let n = 0;
    for (const r of runsQ.data?.runs ?? []) {
      if (r.workload_slug !== w.slug) continue;
      if (!r.success) continue;
      const fin = r.finished_at ?? r.started_at;
      if (!fin) continue;
      const t = Date.parse(fin);
      if (isNaN(t) || t < cutoff) continue;
      n++;
    }
    const hostAffinity = (w.host_affinity ?? []) as string[];
    const claimable = workers.filter((worker) => {
      if (worker.state !== "active") return false;
      if (hostAffinity.length > 0 && !hostAffinity.includes(parseHostFromWid(worker.id))) return false;
      const f = worker.workload_filter;
      return f === null || f.includes(w.slug);
    }).length;
    return {
      throughput_min: n / 5,
      backlog: w.observed_depth ?? 0,
      active_workers: claimable,
    };
  }, [w, runsQ.data, workers]);

  const applyMode = (() => {
    const ec = supQ.data?.executor_config as Record<string, unknown> | undefined;
    const ik = (ec?.init_kwargs ?? {}) as Record<string, unknown>;
    return Boolean(Number(ik?.apply_mode ?? 0));
  })();

  const patchMut = useMutation({
    mutationFn: async (patch: Partial<Workload>) => {
      if (!w) throw new Error("no workload");
      return api.updateWorkload(w.slug, buildPut(w, patch) as never);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["wl", slug] });
      qc.invalidateQueries({ queryKey: ["workloads"] });
      qc.invalidateQueries({ queryKey: ["workloads-for-orch"] });
      qc.invalidateQueries({ queryKey: ["flow-snapshot"] });
    },
  });

  const supEnableMut = useMutation({
    mutationFn: async (enabled: boolean) => {
      if (!w) throw new Error("no workload");
      return api.setSupervisorEnabled(w.slug, enabled);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["wl", slug] });
      qc.invalidateQueries({ queryKey: ["workloads"] });
      qc.invalidateQueries({ queryKey: ["workloads-for-orch"] });
    },
  });

  const filterMut = useMutation({
    mutationFn: async (delta: 1 | -1) => {
      if (!w) throw new Error("no workload");
      const byHost = new Map<string, WorkerInfo[]>();
      for (const wi of workers.filter((x) => x.state === "active")) {
        const h = parseHostFromWid(wi.id);
        const arr = byHost.get(h) ?? [];
        arr.push(wi);
        byHost.set(h, arr);
      }
      const inFilter = (wi: WorkerInfo) =>
        wi.workload_filter === null || wi.workload_filter.includes(w.slug);
      const countPerHost = new Map<string, number>();
      for (const [h, ws] of byHost.entries()) {
        countPerHost.set(h, ws.filter(inFilter).length);
      }
      if (delta === 1) {
        const hosts = Array.from(byHost.keys()).sort(
          (a, b) => countPerHost.get(a)! - countPerHost.get(b)!,
        );
        for (const h of hosts) {
          const cand = byHost.get(h)!.find(
            (wi) => wi.workload_filter !== null && !wi.workload_filter.includes(w.slug),
          );
          if (!cand) continue;
          const next = [...(cand.workload_filter ?? []), w.slug].sort();
          await api.setWorkerFilter(cand.id, next, "ui:flow-popover");
          return;
        }
        throw new Error("追加対象なし: 全 worker が既に担当 or filter=null");
      } else {
        const hosts = Array.from(byHost.keys()).sort(
          (a, b) => countPerHost.get(b)! - countPerHost.get(a)!,
        );
        for (const h of hosts) {
          const cand = byHost.get(h)!.find(
            (wi) => wi.workload_filter !== null && wi.workload_filter.includes(w.slug),
          );
          if (!cand) continue;
          const next = cand.workload_filter!.filter((s) => s !== w.slug);
          // 空 list = filter 解除 (= env fallback)
          await api.setWorkerFilter(cand.id, next.length === 0 ? null : next, "ui:flow-popover");
          return;
        }
        throw new Error("削減対象なし");
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["workers-for-popover"] });
      qc.invalidateQueries({ queryKey: ["workers-with-filter"] });
    },
  });

  if (wlQ.isLoading || !w) {
    return (
      <Stack align="center" p="md">
        <Loader size="sm" />
        <Text size="xs" c="dimmed">読み込み中…</Text>
      </Stack>
    );
  }

  const flowPct = Math.min(100, Math.round(((metrics?.throughput_min ?? 0) / 30) * 100));
  const flowColor =
    (metrics?.throughput_min ?? 0) < 0.1 ? "gray"
      : (metrics?.throughput_min ?? 0) < 5 ? "yellow" : "green";
  const pending = patchMut.isPending || filterMut.isPending;

  const supervisorOptedOut = w.supervisor_enabled === false;

  return (
    <Stack gap="md">
      {applyMode && !supervisorOptedOut && (
        <Alert
          color="yellow"
          icon={<IconRobot size={16} />}
          title="supervisor が自動制御中"
          variant="light"
        >
          ルール条件が成立すれば再上書きされます。
        </Alert>
      )}

      <Paper p="xs" radius="sm" withBorder>
        <Group justify="space-between" wrap="nowrap">
          <Group gap={8} wrap="nowrap" style={{ minWidth: 0 }}>
            {supervisorOptedOut
              ? <IconRobotOff size={18} color="var(--mantine-color-red-6)" />
              : <IconRobot size={18} color="var(--mantine-color-green-6)" />}
            <Box style={{ minWidth: 0 }}>
              <Text size="sm" fw={600}>
                supervisor 自動制御
              </Text>
              <Text size="xs" c="dimmed">
                {supervisorOptedOut
                  ? "OFF: 手動操作だけが効きます"
                  : "ON: ルール条件成立で priority / filter が上書きされます"}
              </Text>
            </Box>
          </Group>
          <Switch
            size="md"
            checked={!supervisorOptedOut}
            onChange={(e) => supEnableMut.mutate(e.currentTarget.checked)}
            disabled={supEnableMut.isPending}
            aria-label="supervisor enabled"
          />
        </Group>
      </Paper>

      <Box>
        <Group justify="space-between" mb={4}>
          <Text size="xs" c="dimmed">流量</Text>
          <Text size="sm" fw={600}>
            {(metrics?.throughput_min ?? 0).toFixed(1)} /min
          </Text>
        </Group>
        <Progress value={flowPct} color={flowColor} size="md" />
        <Group justify="space-between" mt={6}>
          <Badge variant="light" color={(metrics?.backlog ?? 0) > 100 ? "orange" : "gray"}>
            backlog {metrics?.backlog ?? 0}
          </Badge>
          <Badge variant="light" leftSection={<IconUsers size={10} />}>
            {metrics?.active_workers ?? 0} workers
          </Badge>
        </Group>
      </Box>

      <Group gap="xs" grow>
        <Button
          variant="light"
          color="red"
          leftSection={<IconMinus size={14} />}
          onClick={() => patchMut.mutate({ priority: Math.max(0, w.priority - 10) })}
          disabled={pending || w.priority <= 0}
        >
          弱
        </Button>
        <Button
          variant="light"
          color="green"
          leftSection={<IconBolt size={14} />}
          onClick={() => patchMut.mutate({ priority: Math.min(200, w.priority + 10) })}
          disabled={pending || w.priority >= 200}
        >
          強
        </Button>
      </Group>

      <Stack gap="sm">
        <Knob
          icon={<IconUsers size={14} />}
          label="担当 worker 数"
          value={String(metrics?.active_workers ?? "—")}
          hint="+/- で host 別に worker filter を増減します"
          onDown={() => filterMut.mutate(-1)}
          onUp={() => filterMut.mutate(1)}
          disabled={pending}
        />
        <Knob
          icon={<IconStack2 size={14} />}
          label="並列バッチ規模 (batch_size)"
          value={String(w.batch_size)}
          hint="1 claim で何件まとめて処理するか。 多いほど GPU batch 推論等が速くなる"
          onDown={() => patchMut.mutate({ batch_size: Math.max(1, w.batch_size - 1) })}
          onUp={() => patchMut.mutate({ batch_size: Math.min(1024, w.batch_size + 1) })}
          disabled={pending}
        />
        <Knob
          icon={<IconBolt size={14} />}
          label="優先度 (priority)"
          value={String(w.priority)}
          hint="高いほど他より先に claim される。 0-200"
          onDown={() => patchMut.mutate({ priority: Math.max(0, w.priority - 10) })}
          onUp={() => patchMut.mutate({ priority: Math.min(200, w.priority + 10) })}
          disabled={pending}
        />
        <Knob
          icon={<IconAdjustments size={14} />}
          label="lease 周期 (秒)"
          value={String(w.lease_secs)}
          hint="claim の有効期限。 短いほど取りこぼし回復が早く、 長いほど overhead 低い"
          onDown={() => patchMut.mutate({ lease_secs: Math.max(15, w.lease_secs - 30) })}
          onUp={() => patchMut.mutate({ lease_secs: Math.min(86400, w.lease_secs + 30) })}
          disabled={pending}
        />
      </Stack>

      {(patchMut.error || filterMut.error) && (
        <Alert color="red" variant="light">
          {(patchMut.error || filterMut.error)?.toString()}
        </Alert>
      )}
    </Stack>
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
    <Group gap="xs" justify="space-between" wrap="nowrap">
      <Tooltip label={hint ?? ""} disabled={!hint} multiline w={280}>
        <Group gap={4} style={{ flex: 1, minWidth: 0 }}>
          {icon}
          <Text size="xs" c="dimmed" truncate>
            {label}
          </Text>
        </Group>
      </Tooltip>
      <Group gap={4} wrap="nowrap">
        <ActionIcon size="sm" variant="light" color="red" onClick={onDown} disabled={disabled}>
          <IconMinus size={12} />
        </ActionIcon>
        <Text fw={700} size="sm" style={{ minWidth: 50, textAlign: "center" }}>
          {value}
        </Text>
        <ActionIcon size="sm" variant="light" color="green" onClick={onUp} disabled={disabled}>
          <IconPlus size={12} />
        </ActionIcon>
      </Group>
    </Group>
  );
}
