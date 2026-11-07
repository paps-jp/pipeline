import {
  Alert,
  Badge,
  Box,
  Button,
  Checkbox,
  Code,
  Collapse,
  Group,
  NumberInput,
  Paper,
  Progress,
  ScrollArea,
  SegmentedControl,
  Select,
  Stack,
  Text,
  TextInput,
  UnstyledButton,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { IconAlertTriangle, IconChevronDown, IconChevronRight, IconRefresh } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  AgentRecord,
  AgentWorkloadEntry,
  HostPolicy,
  Workload,
  api,
  hostApi,
} from "@/api/client";
import { ErrorState } from "@/components/states";
import {
  EffGap,
  HostCapabilityPanel,
  countStuckGaps,
  effectiveGap,
  fmtRelative,
  vramEstimate,
} from "./HostConfig";

// ============================================================
// ホスト (100 台でも横に伸びない 2 ペイン)
//
// 1 つの (ホスト × ワークロード) 枠に対する操作を「自動 / 手動 / 停止」の 3 択に畳む。
// 内部表現は agent template の { count, pin } のまま (API・バックエンドは変えない):
//   自動 = pin 無し  … 台数は Supervisor の自動制御が決める (ここでは変えられない)
//   手動 = pin あり・count > 0 … 自動制御は触らない。 VRAM/上限の安全制限だけは優先される
//   停止 = pin あり・count = 0 … 0 のまま自動制御に復活させられない
// ============================================================

type Mode = "auto" | "manual" | "off";
type Entry = { count: number; pin: boolean };
type Filter = "all" | "warn" | "stale" | "manual";
type Status = "ok" | "warn" | "stale";

const STALE_S = 180;

function modeOf(e: Entry): Mode {
  if (!e.pin) return "auto";
  return e.count > 0 ? "manual" : "off";
}

function entryFor(mode: Mode, cur: Entry): Entry {
  if (mode === "auto") return { count: cur.count, pin: false };
  if (mode === "off") return { count: 0, pin: true };
  return { count: cur.count > 0 ? cur.count : 1, pin: true };
}

function isStale(agent: AgentRecord | undefined): boolean {
  if (!agent?.last_seen_at) return true;
  const d = new Date(agent.last_seen_at.replace(/(\.\d{3})\d+/, "$1")).getTime();
  return Number.isNaN(d) || (Date.now() - d) / 1000 > STALE_S;
}

/** 1 slug を差し替えた template 全体を作る (他の slug はそのまま)。 */
function withEntry(
  template: Record<string, AgentWorkloadEntry>,
  w: Workload | undefined,
  slug: string,
  e: Entry,
): Record<string, AgentWorkloadEntry> {
  const base = template[slug] ?? {};
  const next: AgentWorkloadEntry = {
    ...base,
    count: Math.max(0, Math.trunc(e.count)),
    gpu: base.gpu ?? Boolean(w?.requires_gpu),
    ...(w?.requires_gpu || base.gpu ? { vram_mb: base.vram_mb ?? vramEstimate(w) } : {}),
    pin: e.pin,
  };
  if (!next.pin) delete next.pin;
  return { ...template, [slug]: next };
}

function hostHasGpu(p: HostPolicy): boolean {
  return (p.vram_effective_mb ?? 0) > 0 || (p.tier_effective ?? "").startsWith("gpu-");
}

interface Summary {
  policy: HostPolicy;
  agent: AgentRecord | undefined;
  status: Status;
  manual: boolean;
  alive: number;
}

function summarize(policy: HostPolicy, agent: AgentRecord | undefined, workloads: Workload[]): Summary {
  const stale = isStale(agent);
  const template = agent?.template?.workloads ?? {};
  const effective = agent?.desired?.workloads ?? {};
  const bySlug = new Map(workloads.map((w) => [w.slug, w]));
  const bad = Object.keys(template).some((s) => {
    const g = effectiveGap(s, policy.host, template, effective, bySlug.get(s));
    return g === "stuck" || g === "vram";
  });
  return {
    policy,
    agent,
    status: stale ? "stale" : bad ? "warn" : "ok",
    manual: Object.values(template).some((e) => e.pin),
    alive: (agent?.last_children ?? []).filter((c) => c.alive).length,
  };
}

function StatusBadge({ s }: { s: Status }) {
  const { t } = useTranslation();
  if (s === "stale") return <Badge size="xs" color="red" variant="light">{t("hostfleet.st_stale", "応答なし")}</Badge>;
  if (s === "warn") return <Badge size="xs" color="yellow" variant="light">{t("hostfleet.st_warn", "要確認")}</Badge>;
  return null;
}

// ------------------------------------------------------------
// 右ペイン: 1 ホスト
// ------------------------------------------------------------
function HostDetail({
  sum,
  workloads,
}: {
  sum: Summary;
  workloads: Workload[];
}) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const { policy, agent } = sum;
  const template = agent?.template?.workloads ?? {};
  const effective = agent?.desired?.workloads ?? {};
  const children = agent?.last_children ?? [];
  const [draft, setDraft] = useState<Record<string, Entry>>({});
  const [capOpen, setCapOpen] = useState(false);

  const entryOf = (slug: string): Entry =>
    draft[slug] ?? { count: Number(template[slug]?.count ?? 0), pin: Boolean(template[slug]?.pin) };
  const hasGpu = hostHasGpu(policy);

  const rows = useMemo(() => {
    const known = new Set(workloads.map((w) => w.slug));
    const extra = Object.keys(template).filter((s) => !known.has(s));
    return [
      ...workloads.filter((w) => w.requires_gpu),
      ...workloads.filter((w) => !w.requires_gpu),
    ]
      .map((w) => ({ slug: w.slug, w: w as Workload | undefined }))
      .concat(extra.map((s) => ({ slug: s, w: undefined })));
  }, [workloads, template]);

  const isUnusable = (slug: string, w: Workload | undefined) => {
    const affinity = (w?.host_affinity ?? []) as string[];
    const allowed = affinity.length === 0 || affinity.includes(policy.host);
    const gpuBlocked = Boolean(w?.requires_gpu) && !hasGpu;
    return (!allowed || gpuBlocked) && Number(template[slug]?.count ?? 0) === 0 && !draft[slug];
  };
  const usableRows = rows.filter((r) => !isUnusable(r.slug, r.w));
  const unusableRows = rows.filter((r) => isUnusable(r.slug, r.w));
  const [showUnusable, setShowUnusable] = useState(false);

  const vramTotal = policy.vram_effective_mb ?? 0;
  const vramPlanned = workloads
    .filter((w) => w.requires_gpu)
    .reduce((sum2, w) => sum2 + Number(effective[w.slug]?.count ?? 0) * vramEstimate(w), 0);
  const vramPct = vramTotal > 0 ? Math.min(100, (vramPlanned / vramTotal) * 100) : 0;

  const save = useMutation({
    mutationFn: () => {
      let next = { ...template };
      for (const [slug, e] of Object.entries(draft)) {
        next = withEntry(next, workloads.find((x) => x.slug === slug), slug, e);
      }
      return hostApi.setAgentDesired(policy.host, next);
    },
    onSuccess: () => {
      setDraft({});
      qc.invalidateQueries({ queryKey: ["agents"] });
      notifications.show({
        color: "teal",
        message: t("hostfleet.saved", { host: policy.host, defaultValue: "{{host}} の設定を保存しました" }),
      });
    },
    onError: (e: unknown) => notifications.show({ color: "red", message: String(e) }),
  });

  // 実効が目標に追随していない行 (stuck) だけ目標まで戻す。 VRAM/許可外で丸めた行には触らない。
  const stuckCount = countStuckGaps(policy.host, agent, workloads);
  const resync = useMutation({
    mutationFn: () => {
      const bySlug = new Map(workloads.map((x) => [x.slug, x]));
      const next: Record<string, AgentWorkloadEntry> = {};
      for (const [slug, tSpec] of Object.entries(template)) {
        const base: AgentWorkloadEntry = { ...(effective[slug] ?? tSpec) };
        if (effectiveGap(slug, policy.host, template, effective, bySlug.get(slug)) === "stuck") {
          base.count = Number(tSpec.count ?? 0);
        }
        next[slug] = base;
      }
      return hostApi.setAgentEffective(policy.host, next);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["agents"] });
      notifications.show({ color: "teal", message: t("hostfleet.resynced", "実効を目標に戻しました") });
    },
    onError: (e: unknown) => notifications.show({ color: "red", message: String(e) }),
  });

  const setAffinity = useMutation({
    mutationFn: ({ slug, hosts }: { slug: string; hosts: string[] }) => api.setWorkloadHostAffinity(slug, hosts),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workloads-hostcfg"] }),
    onError: (e: unknown) => notifications.show({ color: "red", message: String(e) }),
  });

  const gapText = (gap: EffGap, target: number, eff: number): string | null => {
    if (gap === "vram")
      return t("hostfleet.cause_vram", {
        defaultValue: "原因: VRAM上限 (実測で {{eff}} 台に調整中。VRAM が空けば自動で {{target}} 台に戻ります)",
        eff, target,
      });
    if (gap === "stuck")
      return t("hostfleet.cause_stuck", {
        defaultValue: "原因: 実効 {{eff}} が目標 {{target}} に未追随 (自動では戻りません。「実効を目標に戻す」を押してください)",
        eff, target,
      });
    return null;
  };

  return (
    <Stack gap="sm">
      <Group gap={8} wrap="nowrap">
        <Text fw={600} size="lg">{policy.host}</Text>
        {policy.gpu_model && <Badge size="xs" variant="light">{policy.gpu_model}</Badge>}
        <StatusBadge s={sum.status} />
        {policy.enabled !== 1 && <Badge size="xs" color="gray">{t("hostfleet.off", "使用しない")}</Badge>}
      </Group>

      {vramTotal > 0 && (
        <Box>
          <Text size="xs" c="dimmed">
            VRAM {Math.round(vramPlanned)} / {vramTotal} MB {t("hostfleet.vram_used", "使用見込み")}
          </Text>
          <Progress value={vramPct} size="sm" color={vramPct > 90 ? "red" : vramPct > 70 ? "yellow" : "teal"} />
        </Box>
      )}
      {agent && (
        <Text size="xs" c="dimmed">
          {t("hostfleet.last_change", "最終変更")}: {agent.updated_by ?? "—"} ({fmtRelative(agent.updated_at)}) ・{" "}
          {t("hostfleet.last_seen", "最終応答")}: {fmtRelative(agent.last_seen_at)}
        </Text>
      )}

      {!agent && (
        <Alert color="orange" icon={<IconAlertTriangle size={16} />}>
          {t("hostfleet.no_agent", "このホストには pipeline-agent の記録がありません。agent が起動して sync するまで割り当ては配布されません。")}
        </Alert>
      )}
      {agent && sum.status === "stale" && (
        <Alert color="red" icon={<IconAlertTriangle size={16} />}>
          {t("hostfleet.stale", {
            defaultValue: "最終応答が {{ago}} です。設定しても反映されない可能性があります。",
            ago: fmtRelative(agent.last_seen_at),
          })}
        </Alert>
      )}
      {stuckCount > 0 && (
        <Alert color="red" icon={<IconAlertTriangle size={16} />}>
          <Group justify="space-between" wrap="nowrap">
            <Text size="sm">
              {t("hostfleet.stuck_alert", {
                defaultValue: "{{n}} 件で実効が目標に届いていません (VRAM でも許可外でもありません)。",
                n: stuckCount,
              })}
            </Text>
            <Button size="xs" color="red" variant="light" leftSection={<IconRefresh size={14} />}
                    loading={resync.isPending} onClick={() => resync.mutate()}>
              {t("hostfleet.resync", "実効を目標に戻す")}
            </Button>
          </Group>
        </Alert>
      )}

      <Text size="xs" c="dimmed">
        {t("hostfleet.legend", "自動 = Supervisor の自動制御が台数を決定 / 手動 = 台数を自分で決定 (自動制御の対象外。VRAM・上限の安全制限のみ優先) / 停止 = 0 台で固定")}
      </Text>

      <Box>
        {usableRows.map(({ slug, w }) => {
          const e = entryOf(slug);
          const mode = modeOf(e);
          const affinity = (w?.host_affinity ?? []) as string[];
          const allowed = affinity.length === 0 || affinity.includes(policy.host);
          const gpuBlocked = Boolean(w?.requires_gpu) && !hasGpu;
          const alive = children.filter((c) => c.workload === slug && c.alive).length;
          const target = Number(template[slug]?.count ?? 0);
          const eff = Number(effective[slug]?.count ?? 0);
          const gap = effectiveGap(slug, policy.host, template, effective, w);
          const unusable = !allowed || gpuBlocked;
          const cause = gapText(gap, target, eff);
          return (
            <Box key={slug} py="xs" style={{ borderTop: "0.5px solid var(--mantine-color-default-border)" }}>
              <Group justify="space-between" wrap="nowrap" align="center">
                <Box style={{ minWidth: 0, flex: 1 }}>
                  <Group gap={6} wrap="nowrap">
                    <Code>{slug}</Code>
                    {w?.requires_gpu && <Badge size="xs" color="violet" variant="light">GPU</Badge>}
                    {w && !w.enabled && <Badge size="xs" color="gray" variant="outline">{t("hostfleet.wl_off", "無効")}</Badge>}
                  </Group>
                  {w?.requires_gpu && (
                    <Text size="xs" c="dimmed">~{vramEstimate(w)} MB/{t("hostfleet.per_child", "台")}</Text>
                  )}
                </Box>
                {unusable && e.count === 0 ? (
                  <Group gap={6} wrap="nowrap">
                    <Badge size="sm" color="gray" variant="light">
                      {gpuBlocked ? t("hostfleet.no_gpu", "GPUなし") : t("hostfleet.not_allowed", "許可外")}
                    </Badge>
                    {!allowed && w && affinity.length > 0 && (
                      <Button size="compact-xs" variant="light" loading={setAffinity.isPending}
                              onClick={() => setAffinity.mutate({ slug, hosts: [...affinity, policy.host] })}>
                        {t("hostfleet.allow", "このホストを許可")}
                      </Button>
                    )}
                  </Group>
                ) : (
                  <Group gap="sm" wrap="nowrap">
                    <SegmentedControl
                      size="xs"
                      value={mode}
                      data={[
                        { value: "auto", label: t("hostfleet.mode_auto", "自動") },
                        { value: "manual", label: t("hostfleet.mode_manual", "手動") },
                        { value: "off", label: t("hostfleet.mode_off", "停止") },
                      ]}
                      onChange={(v) => setDraft((d) => ({ ...d, [slug]: entryFor(v as Mode, e) }))}
                    />
                    <NumberInput
                      size="xs"
                      w={72}
                      min={0}
                      max={64}
                      value={mode === "auto" ? target : e.count}
                      disabled={mode !== "manual"}
                      onChange={(v) => setDraft((d) => ({ ...d, [slug]: { count: Number(v) || 0, pin: true } }))}
                    />
                  </Group>
                )}
                <Box w={86} ta="right">
                  <Text size="sm" fw={500}>{t("hostfleet.alive", "稼働")} {alive}</Text>
                  {gap && (
                    <Badge size="xs" color={gap === "stuck" ? "red" : gap === "vram" ? "yellow" : "gray"} variant="light">
                      {gap === "vram" ? t("hostfleet.gap_vram", "VRAM上限")
                        : gap === "stuck" ? t("hostfleet.gap_stuck", "未追随")
                        : t("hostfleet.not_allowed", "許可外")}
                    </Badge>
                  )}
                </Box>
              </Group>
              {cause && <Text size="xs" c={gap === "stuck" ? "red" : "dimmed"} mt={2}>{cause}</Text>}
            </Box>
          );
        })}
      </Box>

      {unusableRows.length > 0 && (
        <Box>
          <UnstyledButton onClick={() => setShowUnusable((o) => !o)}>
            <Group gap={4}>
              {showUnusable ? <IconChevronDown size={14} /> : <IconChevronRight size={14} />}
              <Text size="sm" c="dimmed">
                {t("hostfleet.unusable", { defaultValue: "このホストでは使えないワークロード ({{n}})", n: unusableRows.length })}
              </Text>
            </Group>
          </UnstyledButton>
          <Collapse in={showUnusable}>
            <Stack gap={4} mt={6}>
              {unusableRows.map(({ slug, w }) => {
                const affinity = (w?.host_affinity ?? []) as string[];
                const allowed = affinity.length === 0 || affinity.includes(policy.host);
                return (
                  <Group key={slug} justify="space-between" wrap="nowrap">
                    <Group gap={6} wrap="nowrap">
                      <Code>{slug}</Code>
                      <Badge size="xs" color="gray" variant="light">
                        {allowed ? t("hostfleet.no_gpu", "GPUなし") : t("hostfleet.not_allowed", "許可外")}
                      </Badge>
                    </Group>
                    {!allowed && w && affinity.length > 0 && (
                      <Button size="compact-xs" variant="light" loading={setAffinity.isPending}
                              onClick={() => setAffinity.mutate({ slug, hosts: [...affinity, policy.host] })}>
                        {t("hostfleet.allow", "このホストを許可")}
                      </Button>
                    )}
                  </Group>
                );
              })}
            </Stack>
          </Collapse>
        </Box>
      )}

      <Group justify="space-between">
        <UnstyledButton onClick={() => setCapOpen((o) => !o)}>
          <Group gap={4}>
            {capOpen ? <IconChevronDown size={14} /> : <IconChevronRight size={14} />}
            <Text size="sm" c="dimmed">{t("hostfleet.capability", "ホスト能力・詳細設定")}</Text>
          </Group>
        </UnstyledButton>
        <Button size="xs" disabled={Object.keys(draft).length === 0} loading={save.isPending}
                onClick={() => save.mutate()}>
          {t("hostfleet.save", "保存")}
        </Button>
      </Group>
      <Collapse in={capOpen}>
        <HostCapabilityPanel policy={policy} />
      </Collapse>
    </Stack>
  );
}

// ------------------------------------------------------------
// 右ペイン: 複数ホストの一括操作
// ------------------------------------------------------------
function BulkPanel({
  hosts,
  workloads,
  onDone,
}: {
  hosts: Summary[];
  workloads: Workload[];
  onDone: () => void;
}) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [slug, setSlug] = useState<string | null>(workloads[0]?.slug ?? null);
  const [mode, setMode] = useState<Mode>("manual");
  const [count, setCount] = useState<number>(1);
  const w = workloads.find((x) => x.slug === slug);

  // 適用できないホスト (許可外 / GPU なし / agent 記録なし) は除いて数える
  const plan = hosts.map((h) => {
    const affinity = (w?.host_affinity ?? []) as string[];
    const allowed = affinity.length === 0 || affinity.includes(h.policy.host);
    const gpuBlocked = Boolean(w?.requires_gpu) && !hostHasGpu(h.policy);
    const ok = Boolean(h.agent) && allowed && !gpuBlocked;
    const total = h.policy.vram_effective_mb ?? 0;
    let over = false;
    if (ok && w?.requires_gpu && mode !== "off" && total > 0) {
      const tmpl = h.agent?.template?.workloads ?? {};
      const others = workloads
        .filter((x) => x.requires_gpu && x.slug !== w.slug)
        .reduce((s, x) => s + Number(tmpl[x.slug]?.count ?? 0) * vramEstimate(x), 0);
      over = others + (mode === "manual" ? count : 0) * vramEstimate(w) > total;
    }
    return { h, ok, over };
  });
  const targets = plan.filter((p) => p.ok);
  const skipped = plan.length - targets.length;
  const overN = targets.filter((p) => p.over).length;

  const apply = useMutation({
    mutationFn: async () => {
      if (!w) return 0;
      const e = entryFor(mode, { count: mode === "manual" ? count : 0, pin: true });
      const entry: Entry = mode === "auto" ? { count: 0, pin: false } : e;
      let done = 0;
      for (const p of targets) {
        const tmpl = p.h.agent?.template?.workloads ?? {};
        const cur = tmpl[w.slug];
        const next = withEntry(tmpl, w, w.slug, mode === "auto" ? { count: Number(cur?.count ?? 0), pin: false } : entry);
        await hostApi.setAgentDesired(p.h.policy.host, next);
        done += 1;
      }
      return done;
    },
    onSuccess: (n) => {
      qc.invalidateQueries({ queryKey: ["agents"] });
      notifications.show({
        color: "teal",
        message: t("hostfleet.bulk_done", { defaultValue: "{{n}} 台に適用しました", n }),
      });
      onDone();
    },
    onError: (e: unknown) => {
      qc.invalidateQueries({ queryKey: ["agents"] });
      notifications.show({ color: "red", message: String(e) });
    },
  });

  return (
    <Stack gap="sm">
      <Text fw={600} size="lg">
        {t("hostfleet.bulk_title", { defaultValue: "{{n}} 台を一括操作", n: hosts.length })}
      </Text>
      <Group gap="sm" align="flex-end">
        <Select
          size="xs"
          label={t("hostfleet.bulk_wl", "ワークロード")}
          w={240}
          searchable
          value={slug}
          onChange={setSlug}
          data={workloads.map((x) => ({ value: x.slug, label: x.slug }))}
        />
        <SegmentedControl
          size="xs"
          value={mode}
          data={[
            { value: "auto", label: t("hostfleet.mode_auto", "自動") },
            { value: "manual", label: t("hostfleet.mode_manual", "手動") },
            { value: "off", label: t("hostfleet.mode_off", "停止") },
          ]}
          onChange={(v) => setMode(v as Mode)}
        />
        <NumberInput size="xs" w={80} min={0} max={64} value={count} disabled={mode !== "manual"}
                     onChange={(v) => setCount(Number(v) || 0)} />
        <Text size="xs" c="dimmed" pb={6}>{t("hostfleet.per_host", "台 / ホスト")}</Text>
      </Group>
      <Text size="sm">
        {t("hostfleet.bulk_targets", { defaultValue: "適用対象 {{n}} 台", n: targets.length })}
        {skipped > 0 && (
          <Text span size="sm" c="dimmed">
            {" "}({t("hostfleet.bulk_skipped", { defaultValue: "{{n}} 台は許可外・GPUなし・agent記録なしのため対象外", n: skipped })})
          </Text>
        )}
      </Text>
      {overN > 0 ? (
        <Alert color="yellow" icon={<IconAlertTriangle size={16} />}>
          {t("hostfleet.bulk_over", {
            defaultValue: "{{n}} 台は VRAM 上限 (見込み) で希望台数に届かない可能性があります。実際の台数は planner が実測で調整します。",
            n: overN,
          })}
        </Alert>
      ) : (
        targets.length > 0 && (
          <Text size="sm" c="dimmed">{t("hostfleet.bulk_ok", "VRAM 見込み上の問題はありません。")}</Text>
        )
      )}
      <Text size="xs" c="dimmed">
        {t("hostfleet.bulk_note", "手動・停止にした枠は、Supervisor の自動制御の対象外になります。")}
      </Text>
      <Group>
        <Button size="xs" disabled={!w || targets.length === 0} loading={apply.isPending} onClick={() => apply.mutate()}>
          {t("hostfleet.bulk_apply", { defaultValue: "{{n}} 台に適用", n: targets.length })}
        </Button>
        <Button size="xs" variant="default" onClick={onDone}>{t("hostfleet.bulk_clear", "選択を解除")}</Button>
      </Group>
    </Stack>
  );
}

// ------------------------------------------------------------
// 画面本体
// ------------------------------------------------------------
export function HostFleet() {
  const { t } = useTranslation();
  const policyQ = useQuery({ queryKey: ["host-policy"], queryFn: () => hostApi.listPolicy(), refetchInterval: 15_000 });
  const agentsQ = useQuery({ queryKey: ["agents"], queryFn: () => hostApi.listAgents(), refetchInterval: 10_000 });
  const workloadsQ = useQuery({ queryKey: ["workloads-hostcfg"], queryFn: () => api.listWorkloads(), refetchInterval: 30_000 });

  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [selected, setSelected] = useState<string[]>([]);

  const workloads = workloadsQ.data?.workloads ?? [];
  const sums = useMemo(() => {
    const agentBy = new Map((agentsQ.data?.agents ?? []).map((a) => [a.host, a]));
    return (policyQ.data?.hosts ?? []).map((p) => summarize(p, agentBy.get(p.host), workloads));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [policyQ.data, agentsQ.data, workloadsQ.data]);

  if (policyQ.error) return <ErrorState error={policyQ.error} onRetry={() => policyQ.refetch()} />;

  const count = (f: Filter) =>
    f === "all" ? sums.length
      : f === "manual" ? sums.filter((s) => s.manual).length
      : sums.filter((s) => s.status === f).length;
  const visible = sums.filter((s) => {
    if (query && !s.policy.host.toLowerCase().includes(query.toLowerCase())) return false;
    if (filter === "manual") return s.manual;
    if (filter === "warn" || filter === "stale") return s.status === filter;
    return true;
  });
  const selSet = new Set(selected);
  const toggle = (h: string, on: boolean) =>
    setSelected((cur) => (on ? [...cur.filter((x) => x !== h), h] : cur.filter((x) => x !== h)));
  const selectedSums = sums.filter((s) => selSet.has(s.policy.host));

  const chips: { f: Filter; label: string }[] = [
    { f: "all", label: t("hostfleet.f_all", "すべて") },
    { f: "warn", label: t("hostfleet.f_warn", "要確認") },
    { f: "stale", label: t("hostfleet.f_stale", "応答なし") },
    { f: "manual", label: t("hostfleet.f_manual", "手動あり") },
  ];

  return (
    <Box style={{ display: "grid", gridTemplateColumns: "minmax(260px, 320px) minmax(0, 1fr)", gap: 16, alignItems: "start" }}>
      <Paper withBorder>
        <Stack gap={6} p="sm" pb={0}>
          <TextInput size="xs" placeholder={t("hostfleet.search", "ホスト名で検索")}
                     value={query} onChange={(e) => setQuery(e.currentTarget.value)} />
          <Group gap={6}>
            {chips.map((c) => (
              <Button key={c.f} size="compact-xs" radius="xl"
                      variant={filter === c.f ? "light" : "default"}
                      onClick={() => setFilter(c.f)}>
                {c.label} {count(c.f)}
              </Button>
            ))}
          </Group>
          <Group justify="space-between" pb={6}>
            <Checkbox size="xs" label={t("hostfleet.select_all", "表示中を全選択")}
                      checked={visible.length > 0 && visible.every((s) => selSet.has(s.policy.host))}
                      onChange={(e) =>
                        setSelected(e.currentTarget.checked ? visible.map((s) => s.policy.host) : [])}
            />
            <Text size="xs" c="dimmed">
              {t("hostfleet.counts", { defaultValue: "{{v}} 台表示 / {{s}} 台選択", v: visible.length, s: selected.length })}
            </Text>
          </Group>
        </Stack>
        <ScrollArea h={520} type="auto" style={{ borderTop: "0.5px solid var(--mantine-color-default-border)" }}>
          {visible.length === 0 && (
            <Text size="sm" c="dimmed" p="sm">{t("hostfleet.none", "該当するホストはありません。")}</Text>
          )}
          {visible.map((s) => {
            const on = selSet.has(s.policy.host);
            return (
              <Group key={s.policy.host} gap={8} wrap="nowrap" px="sm" py={6}
                     style={{
                       borderBottom: "0.5px solid var(--mantine-color-default-border)",
                       background: on ? "var(--mantine-primary-color-light)" : undefined,
                       cursor: "pointer",
                     }}
                     onClick={() => setSelected([s.policy.host])}>
                <Checkbox size="xs" checked={on}
                          onClick={(e) => e.stopPropagation()}
                          onChange={(e) => toggle(s.policy.host, e.currentTarget.checked)} />
                <Text size="sm" style={{ flex: 1, minWidth: 0 }} truncate>
                  {s.policy.host}
                  {s.policy.vram_effective_mb ? (
                    <Text span size="xs" c="dimmed"> {Math.round((s.policy.vram_effective_mb ?? 0) / 1000)}GB</Text>
                  ) : null}
                </Text>
                <StatusBadge s={s.status} />
                {s.manual && <Badge size="xs" variant="outline" color="gray">{t("hostfleet.mode_manual", "手動")}</Badge>}
                <Text size="xs" c="dimmed" w={50} ta="right">{t("hostfleet.alive", "稼働")} {s.alive}</Text>
              </Group>
            );
          })}
        </ScrollArea>
      </Paper>

      <Paper withBorder p="md">
        {selected.length === 0 && (
          <Text size="sm" c="dimmed">{t("hostfleet.pick", "左の一覧からホストを選択してください。複数選択すると一括操作になります。")}</Text>
        )}
        {selectedSums.length === 1 && (
          <HostDetail key={selectedSums[0].policy.host} sum={selectedSums[0]} workloads={workloads} />
        )}
        {selectedSums.length > 1 && (
          <BulkPanel hosts={selectedSums} workloads={workloads} onDone={() => setSelected([])} />
        )}
      </Paper>
    </Box>
  );
}
