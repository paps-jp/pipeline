import {
  Accordion,
  Alert,
  Badge,
  Box,
  Button,
  Code,
  Group,
  NumberInput,
  Paper,
  Progress,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  IconAlertTriangle,
  IconCpu,
  IconDeviceDesktopAnalytics,
  IconRefresh,
} from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  AgentRecord,
  AgentWorkloadEntry,
  HostPolicy,
  HostPolicyUpdate,
  Workload,
  api,
  hostApi,
} from "@/api/client";
import { ErrorState } from "@/components/states";

// ============================================================
// ホスト設定 (host_policy + agent desired template)
//
// 「このホストに何をどれだけ載せるか」を 1 画面で決める。 3 段の制約が直列なので
// 全部ここに出す (どれか 1 つでも欠けると台数を上げても子が動かない):
//   1. workload.host_affinity … このホストで claim して良いか (絶対制約)
//   2. agent template.count   … このホストの workload 別 目標台数
//   3. VRAM / host_policy     … planner が実測で丸める上限
// ============================================================

const TIER_OPTIONS = [
  { value: "", label: "自動判定 (VRAM から推定)" },
  { value: "gpu-16g-pro", label: "gpu-16g-pro (16GB 以上)" },
  { value: "gpu-10g-consumer", label: "gpu-10g-consumer (10GB 民生)" },
  { value: "cpu-only", label: "cpu-only (GPU 無し)" },
];

function fmtRelative(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return iso;
  const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

/** workload 1 子あたりの VRAM 見積り (観測 peak → 宣言値 → 既定 1500)。 */
function vramEstimate(w: Workload | undefined): number {
  if (!w) return 1500;
  const declared = Number((w.resources ?? {}).vram_mb ?? 0);
  return w.observed_vram_mb_peak || declared || 1500;
}

type DraftEntry = { count: number; pin: boolean };

// ------------------------------------------------------------
// 目標(template) と 実効(effective) の乖離
//
// agent が reconcile するのは effective だけなので、 目標を上げても effective が
// 追随しなければ子は 1 つも立たない。 「なぜ届いていないか」を出さないと、 画面上は
// ただの小さい数字で、 フリートが止まっていても気づけない (2026-08-02: nas-c2 の
// image-pull/job-submit が effective=0 のまま 1 時間停止し、 GPU 全台が遊休した)。
//   affinity … host_affinity 違反の矯正 (正常。 絶対制約なので目標が悪い)
//   vram     … planner が VRAM 実測で丸めた (正常。 GPU 行のみ起きうる)
//   stuck    … 上記いずれでもない = 誰も戻さない片道切符 (異常。 要 operator 介入)
// ------------------------------------------------------------
type EffGap = "affinity" | "vram" | "stuck" | null;

function effectiveGap(
  slug: string,
  host: string,
  template: Record<string, AgentWorkloadEntry>,
  effective: Record<string, AgentWorkloadEntry>,
  w: Workload | undefined,
): EffGap {
  const target = Number(template[slug]?.count ?? 0);
  const spec = effective[slug];
  if (Number(spec?.count ?? 0) >= target) return null;
  // 印 (planner が矯正済み) だけでなく host_affinity 自体も見る。 planner がまだ
  // 回っていない / このホストを skip する場合は印が付かないので、 印だけで判定すると
  // 「許可外なのに未追随」と誤診し、 operator に実効の引き上げを促してしまう
  // (= 許可外ホストで子が起動 → claim 拒否 → balancer swap の綱引き再発)。
  const affinity = (w?.host_affinity ?? []) as string[];
  if (spec?.affinity_blocked || (affinity.length > 0 && !affinity.includes(host))) {
    return "affinity";
  }
  return w?.requires_gpu ? "vram" : "stuck";
}

/** そのホストで operator 介入が要る (= stuck) 行数。 折り畳んだ状態でも出す。 */
function countStuckGaps(
  host: string,
  agent: AgentRecord | undefined,
  workloads: Workload[],
): number {
  const template = agent?.template?.workloads ?? {};
  const effective = agent?.desired?.workloads ?? {};
  const bySlug = new Map(workloads.map((w) => [w.slug, w]));
  return Object.keys(template).filter(
    (s) => effectiveGap(s, host, template, effective, bySlug.get(s)) === "stuck",
  ).length;
}

// ============================================================
// 1 ホスト分のカード
// ============================================================

function HostCard({
  policy,
  agent,
  workloads,
}: {
  policy: HostPolicy;
  agent: AgentRecord | undefined;
  workloads: Workload[];
}) {
  const { t } = useTranslation();
  const qc = useQueryClient();

  const template = agent?.template?.workloads ?? {};
  const effective = agent?.desired?.workloads ?? {};
  const children = agent?.last_children ?? [];

  // ---- 編集ドラフト (未保存の変更のみ保持。 未編集の行は template を素通し) ----
  const [draft, setDraft] = useState<Record<string, DraftEntry>>({});
  const [pol, setPol] = useState<HostPolicyUpdate | null>(null);

  const entryOf = (slug: string): DraftEntry =>
    draft[slug] ?? {
      count: Number(template[slug]?.count ?? 0),
      pin: Boolean(template[slug]?.pin),
    };
  const setEntry = (slug: string, patch: Partial<DraftEntry>) =>
    setDraft((d) => ({ ...d, [slug]: { ...entryOf(slug), ...patch } }));

  const polValue = <K extends keyof HostPolicyUpdate>(key: K, fallback: HostPolicyUpdate[K]) =>
    (pol && key in pol ? pol[key] : fallback) as HostPolicyUpdate[K];

  const aliveOf = (slug: string) =>
    children.filter((c) => c.workload === slug && c.alive).length;

  const saveDesired = useMutation({
    mutationFn: () => {
      const next: Record<string, AgentWorkloadEntry> = {};
      const slugs = new Set([...Object.keys(template), ...Object.keys(draft)]);
      for (const slug of slugs) {
        const e = entryOf(slug);
        const w = workloads.find((x) => x.slug === slug);
        const base = template[slug] ?? {};
        // 未登録 slug は workload 定義から gpu/vram_mb を補う (agent の spawn ゲート用)
        next[slug] = {
          ...base,
          count: Math.max(0, Math.trunc(e.count)),
          gpu: base.gpu ?? Boolean(w?.requires_gpu),
          ...(w?.requires_gpu || base.gpu
            ? { vram_mb: base.vram_mb ?? vramEstimate(w) }
            : {}),
          pin: e.pin,
        };
        if (!next[slug].pin) delete next[slug].pin;
      }
      return hostApi.setAgentDesired(policy.host, next);
    },
    onSuccess: () => {
      setDraft({});
      qc.invalidateQueries({ queryKey: ["agents"] });
      notifications.show({
        color: "teal",
        message: t("hostcfg.saved_desired", { host: policy.host,
          defaultValue: `${policy.host} の割り当てを保存しました` }),
      });
    },
    onError: (e: unknown) =>
      notifications.show({ color: "red", message: String(e) }),
  });

  const savePolicy = useMutation({
    mutationFn: () => hostApi.updatePolicy(policy.host, pol ?? {}),
    onSuccess: () => {
      setPol(null);
      qc.invalidateQueries({ queryKey: ["host-policy"] });
      notifications.show({
        color: "teal",
        message: t("hostcfg.saved_policy", { host: policy.host,
          defaultValue: `${policy.host} のホスト能力を保存しました` }),
      });
    },
    onError: (e: unknown) =>
      notifications.show({ color: "red", message: String(e) }),
  });

  // 実効が目標に追随していない行 (stuck) だけを目標まで引き上げる。
  // template 全体を effective に流し込むと planner の VRAM 算定まで消してしまい、
  // 物理的に入らない台数が配られる (2026-08-01 の 16GB カードに 22GB 分の plan)。
  const resyncEffective = useMutation({
    mutationFn: () => {
      const bySlug = new Map(workloads.map((x) => [x.slug, x]));
      const next: Record<string, AgentWorkloadEntry> = {};
      for (const [slug, tSpec] of Object.entries(template)) {
        // 土台は現 effective (planner の判断と affinity 矯正の印を保つ)
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
      notifications.show({
        color: "teal",
        message: t("hostcfg.resynced", { host: policy.host,
          defaultValue: `${policy.host} の実効を目標に戻しました` }),
      });
    },
    onError: (e: unknown) =>
      notifications.show({ color: "red", message: String(e) }),
  });

  const setAffinity = useMutation({
    mutationFn: ({ slug, hosts }: { slug: string; hosts: string[] }) =>
      api.setWorkloadHostAffinity(slug, hosts),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workloads-hostcfg"] }),
    onError: (e: unknown) =>
      notifications.show({ color: "red", message: String(e) }),
  });

  // ---- VRAM 予算の見込み (GPU 行の 目標台数 × 1 子あたり VRAM) ----
  const vramTotal = policy.vram_effective_mb ?? 0;
  const gpuRows = workloads.filter((w) => w.requires_gpu);
  const vramPlanned = gpuRows.reduce(
    (sum, w) => sum + entryOf(w.slug).count * vramEstimate(w), 0);
  const vramPct = vramTotal > 0 ? Math.min(100, (vramPlanned / vramTotal) * 100) : 0;

  // ---- GPU 非搭載ホストの判定 ----
  // GPU 無しホスト (= nas-c2 等の CPU 専用) に requires_gpu の目標台数を書いても、 agent の
  // VRAM 実測ゲート (nvidia-smi 不在 → spawn 見送り) で子は 1 つも立たない。 さらに planner は
  // vram_total=None のホストを skip するので effective も 0 に矯正されず、 「目標 3 / 実効 3 /
  // 稼働 0」 の残骸が画面に残り続ける。 そもそも書けないようにする。
  // VRAM 上書き or ティア手動指定があればそちらを尊重 (= agent 未報告の新規 GPU ホスト救済)。
  const hostHasGpu = vramTotal > 0 || (policy.tier_effective ?? "").startsWith("gpu-");

  const rows = useMemo(() => {
    const known = new Set(workloads.map((w) => w.slug));
    const extra = Object.keys(template).filter((s) => !known.has(s));
    return [
      ...workloads.filter((w) => w.requires_gpu),
      ...workloads.filter((w) => !w.requires_gpu),
    ].map((w) => ({ slug: w.slug, w }))
      .concat(extra.map((s) => ({ slug: s, w: undefined as unknown as Workload })));
  }, [workloads, template]);

  const desiredDirty = Object.keys(draft).length > 0;
  const stuckCount = countStuckGaps(policy.host, agent, workloads);
  const stale = !agent?.last_seen_at ||
    (Date.now() - new Date(agent.last_seen_at).getTime()) / 1000 > 180;

  return (
    <Stack gap="sm">
      {/* ---- ホスト能力 (host_policy) ---- */}
      <Paper withBorder p="sm">
        <Group justify="space-between" mb="xs">
          <Group gap={8}>
            <IconDeviceDesktopAnalytics size={16} />
            <Text fw={600} size="sm">
              {t("hostcfg.capability", "ホスト能力")}
            </Text>
            {policy.gpu_model && <Badge size="xs" variant="light">{policy.gpu_model}</Badge>}
          </Group>
          <Switch
            size="xs"
            label={t("hostcfg.host_enabled", "このホストを使う")}
            checked={Boolean(polValue("enabled", policy.enabled === 1))}
            onChange={(e) => setPol((p) => ({ ...p, enabled: e.currentTarget.checked }))}
          />
        </Group>
        <Group grow align="flex-end" wrap="wrap">
          <NumberInput
            size="xs"
            label={t("hostcfg.max_gpu_workers", "最大 GPU ワーカー数")}
            description={t("hostcfg.max_gpu_workers_desc", "空 = 制限なし")}
            min={0}
            max={64}
            value={polValue("max_gpu_workers", policy.max_gpu_workers) ?? ""}
            onChange={(v) =>
              setPol((p) => ({ ...p, max_gpu_workers: v === "" ? null : Number(v) }))}
          />
          <NumberInput
            size="xs"
            label={t("hostcfg.vram_override", "VRAM 上書き (MB)")}
            description={t("hostcfg.vram_override_desc", "空 = 実測値を使う")}
            min={0}
            max={200000}
            value={polValue("vram_override_mb", policy.vram_override_mb) ?? ""}
            onChange={(v) =>
              setPol((p) => ({ ...p, vram_override_mb: v === "" ? null : Number(v) }))}
          />
          <Select
            size="xs"
            label={t("hostcfg.tier", "ティア")}
            data={TIER_OPTIONS}
            value={polValue("tier", policy.tier) ?? ""}
            onChange={(v) => setPol((p) => ({ ...p, tier: v || null }))}
          />
          <TextInput
            size="xs"
            label={t("hostcfg.notes", "メモ")}
            value={polValue("notes", policy.notes) ?? ""}
            onChange={(e) => setPol((p) => ({ ...p, notes: e.currentTarget.value }))}
          />
        </Group>
        <Group justify="space-between" mt="xs">
          <Text size="xs" c="dimmed">
            {t("hostcfg.effective", "実効")}: {policy.tier_effective ?? "—"} /{" "}
            {policy.vram_effective_mb ?? "—"} MB
            {policy.updated_by && ` · ${t("hostcfg.updated_by", "更新")}: ${policy.updated_by}`}
          </Text>
          <Button size="xs" disabled={!pol} loading={savePolicy.isPending}
                  onClick={() => savePolicy.mutate()}>
            {t("hostcfg.save", "保存")}
          </Button>
        </Group>
      </Paper>

      {/* ---- ワークロード割り当て (agent template) ---- */}
      <Paper withBorder p="sm">
        <Group justify="space-between" mb="xs">
          <Group gap={8}>
            <IconCpu size={16} />
            <Text fw={600} size="sm">
              {t("hostcfg.allocation", "ワークロード割り当て")}
            </Text>
          </Group>
          <Group gap="xs">
            {vramTotal > 0 && (
              <Tooltip label={t("hostcfg.vram_plan_tip",
                "GPU 行の 目標台数 × 1 子あたり VRAM の合計。 実測 VRAM を超えると planner が自動で減らします")}>
                <Box w={190}>
                  <Text size="xs" c="dimmed">
                    {t("hostcfg.vram_plan", "VRAM 予約見込み")}:{" "}
                    {Math.round(vramPlanned)} / {vramTotal} MB
                  </Text>
                  <Progress value={vramPct} size="sm"
                            color={vramPct > 90 ? "red" : vramPct > 70 ? "yellow" : "teal"} />
                </Box>
              </Tooltip>
            )}
            {stuckCount > 0 && (
              <Tooltip label={t("hostcfg.resync_tip",
                "実効が目標に追随していない行を目標まで戻します。 planner が VRAM で減らした GPU 行と、 許可外で 0 にされた行は触りません")}>
                <Button size="xs" color="red" variant="light"
                        leftSection={<IconRefresh size={14} />}
                        loading={resyncEffective.isPending}
                        onClick={() => resyncEffective.mutate()}>
                  {t("hostcfg.resync", { defaultValue: "実効を目標に戻す ({{n}})", n: stuckCount })}
                </Button>
              </Tooltip>
            )}
            <Button size="xs" disabled={!desiredDirty} loading={saveDesired.isPending}
                    onClick={() => saveDesired.mutate()}>
              {t("hostcfg.save", "保存")}
            </Button>
          </Group>
        </Group>

        {stuckCount > 0 && (
          <Alert color="red" icon={<IconAlertTriangle size={16} />} mb="xs">
            {t("hostcfg.stuck_alert", {
              defaultValue:
                "{{n}} 件の workload で実効が目標に届いていません (VRAM でも許可外でもない)。 agent が見るのは実効だけなので、 この行の子は 1 つも動きません。",
              n: stuckCount,
            })}
          </Alert>
        )}

        {!agent && (
          <Alert color="orange" icon={<IconAlertTriangle size={16} />} mb="xs">
            {t("hostcfg.no_agent",
              "このホストには pipeline-agent の記録がありません。 agent が起動して sync するまで割り当ては配布されません。")}
          </Alert>
        )}
        {agent && stale && (
          <Alert color="orange" icon={<IconAlertTriangle size={16} />} mb="xs">
            {t("hostcfg.stale_agent", {
              defaultValue: "agent の最終報告が {{ago}} です。 設定しても反映されない可能性があります。",
              ago: fmtRelative(agent.last_seen_at),
            })}
          </Alert>
        )}

        <Table striped highlightOnHover withTableBorder>
          <Table.Thead>
            <Table.Tr>
              <Table.Th style={{ width: 90 }}>
                <Tooltip label={t("hostcfg.col_allow_tip",
                  "workload.host_affinity にこのホストが入っているか。 外れていると何台積んでも claim できません")}>
                  <span>{t("hostcfg.col_allow", "許可")}</span>
                </Tooltip>
              </Table.Th>
              <Table.Th>{t("hostcfg.col_workload", "ワークロード")}</Table.Th>
              <Table.Th style={{ width: 110 }}>
                <Tooltip label={t("hostcfg.col_target_tip", "このホストで動かしたい台数 (template)")}>
                  <span>{t("hostcfg.col_target", "目標")}</span>
                </Tooltip>
              </Table.Th>
              <Table.Th style={{ width: 70 }}>
                <Tooltip label={t("hostcfg.col_eff_tip", "planner が VRAM 実測で丸めた実配布値")}>
                  <span>{t("hostcfg.col_eff", "実効")}</span>
                </Tooltip>
              </Table.Th>
              <Table.Th style={{ width: 70 }}>{t("hostcfg.col_alive", "稼働")}</Table.Th>
              <Table.Th style={{ width: 90 }}>
                <Tooltip label={t("hostcfg.col_pin_tip",
                  "ON の間 supervisor の自動増減がこの行を触りません (手動値が次の tick で戻るのを防ぐ)")}>
                  <span>{t("hostcfg.col_pin", "固定")}</span>
                </Tooltip>
              </Table.Th>
              <Table.Th style={{ width: 150 }}>{t("hostcfg.col_note", "備考")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map(({ slug, w }) => {
              const e = entryOf(slug);
              const affinity = (w?.host_affinity ?? []) as string[];
              const unconstrained = affinity.length === 0;
              const allowed = unconstrained || affinity.includes(policy.host);
              const perHost = w?.max_concurrent_per_host ?? null;
              const overCap = perHost !== null && e.count > perHost;
              const alive = aliveOf(slug);
              const eff = Number(effective[slug]?.count ?? 0);
              const gap = effectiveGap(slug, policy.host, template, effective, w);
              // GPU workload × GPU 無しホスト = 何台書いても起動しない。 ただし既に残骸
              // (count>0) がある行は 0 に戻せないと困るので編集を許す。
              const gpuBlocked = Boolean(w?.requires_gpu) && !hostHasGpu;
              const dimmed = (!allowed || gpuBlocked) && e.count === 0;
              return (
                <Table.Tr key={slug} style={dimmed ? { opacity: 0.55 } : undefined}>
                  <Table.Td>
                    <Tooltip
                      label={
                        !w
                          ? t("hostcfg.unknown_workload", "workload 定義がありません (削除済み?)")
                          : unconstrained
                          ? t("hostcfg.affinity_unset", "host_affinity 未設定 = 全ホスト許可")
                          : t("hostcfg.affinity_toggle", "このホストを host_affinity に追加/削除")
                      }
                    >
                      <span>
                        <Switch
                          size="xs"
                          checked={allowed}
                          disabled={!w || unconstrained || setAffinity.isPending}
                          // host_affinity=[] は「全ホスト許可」= 有効。 触れないだけで無効ではないので
                          // disabled の灰色を打ち消して通常の ON 色で描く (灰色だと禁止に見える)
                          styles={
                            w && unconstrained
                              ? {
                                  track: {
                                    backgroundColor: "var(--mantine-primary-color-filled)",
                                    borderColor: "var(--mantine-primary-color-filled)",
                                    opacity: 1,
                                    cursor: "default",
                                  },
                                  thumb: {
                                    backgroundColor: "var(--mantine-color-white)",
                                    borderColor: "var(--mantine-color-white)",
                                  },
                                }
                              : undefined
                          }
                          onChange={(ev) => {
                            if (!w) return;
                            const on = ev.currentTarget.checked;
                            const next = on
                              ? [...affinity, policy.host]
                              : affinity.filter((h) => h !== policy.host);
                            if (!on && next.length === 0) {
                              notifications.show({
                                color: "red",
                                message: t("hostcfg.affinity_last_host",
                                  "最後の 1 ホストは外せません (空 = 全ホスト許可になり意図と逆になります)"),
                              });
                              return;
                            }
                            setAffinity.mutate({ slug, hosts: next });
                          }}
                        />
                      </span>
                    </Tooltip>
                  </Table.Td>
                  <Table.Td>
                    <Group gap={6} wrap="nowrap">
                      <Code>{slug}</Code>
                      {w?.requires_gpu && <Badge size="xs" color="violet" variant="light">GPU</Badge>}
                      {gpuBlocked && (
                        <Tooltip label={t("hostcfg.no_gpu_tip",
                          "このホストは GPU 非搭載 (VRAM 未報告) です。 GPU workload は agent の VRAM ゲートで spawn されません")}>
                          <Badge size="xs" color="gray" variant="outline">
                            {t("hostcfg.no_gpu", "GPU 無し")}
                          </Badge>
                        </Tooltip>
                      )}
                      {w && !w.enabled && (
                        <Badge size="xs" color="gray" variant="outline">
                          {t("hostcfg.disabled", "停止中")}
                        </Badge>
                      )}
                    </Group>
                  </Table.Td>
                  <Table.Td>
                    <NumberInput
                      size="xs"
                      min={0}
                      max={64}
                      value={e.count}
                      error={overCap || (gpuBlocked && e.count > 0)}
                      disabled={gpuBlocked && e.count === 0}
                      onChange={(v) => setEntry(slug, { count: Number(v) || 0 })}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Group gap={4} wrap="nowrap">
                      <Text size="sm"
                            fw={gap === "stuck" ? 700 : undefined}
                            c={gap === "stuck" ? "red" : gap ? "orange" : undefined}>
                        {eff}
                      </Text>
                      {gap === "affinity" && (
                        <Tooltip label={t("hostcfg.gap_affinity_tip",
                          "host_affinity 違反として planner が 0 に固定しています。 目標ではなく「許可」を直してください")}>
                          <Badge size="xs" color="gray" variant="outline">
                            {t("hostcfg.gap_affinity", "許可外")}
                          </Badge>
                        </Tooltip>
                      )}
                      {gap === "vram" && (
                        <Tooltip label={t("hostcfg.gap_vram_tip",
                          "planner が VRAM 実測で丸めています。 VRAM が空けば自動で目標まで戻ります")}>
                          <Badge size="xs" color="yellow" variant="light">VRAM</Badge>
                        </Tooltip>
                      )}
                      {gap === "stuck" && (
                        <Tooltip label={t("hostcfg.gap_stuck_tip",
                          "VRAM でも許可外でもないのに実効が目標に届いていません。 自動では戻らないので「実効を目標に戻す」を押してください")}>
                          <Badge size="xs" color="red">
                            {t("hostcfg.gap_stuck", "未追随")}
                          </Badge>
                        </Tooltip>
                      )}
                    </Group>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm" c={alive < eff ? "dimmed" : undefined}>{alive}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Switch
                      size="xs"
                      checked={e.pin}
                      onChange={(ev) => setEntry(slug, { pin: ev.currentTarget.checked })}
                    />
                  </Table.Td>
                  <Table.Td>
                    {!allowed && e.count > 0 && (
                      <Text size="xs" c="red">
                        {t("hostcfg.warn_affinity", "許可外 → 0 に矯正されます")}
                      </Text>
                    )}
                    {gpuBlocked && e.count > 0 && (
                      <Text size="xs" c="red">
                        {t("hostcfg.warn_no_gpu", "GPU 無し → 子は起動しません (0 にしてください)")}
                      </Text>
                    )}
                    {gap === "stuck" && (
                      <Text size="xs" c="red">
                        {t("hostcfg.warn_stuck", {
                          defaultValue: "実効 {{eff}} が目標 {{target}} に未追随 → 子は動きません",
                          eff, target: Number(template[slug]?.count ?? 0),
                        })}
                      </Text>
                    )}
                    {overCap && (
                      <Text size="xs" c="red">
                        {t("hostcfg.warn_cap", { defaultValue: "1 ホスト上限 {{cap}} 超過", cap: perHost })}
                      </Text>
                    )}
                    {w?.requires_gpu && e.count > 0 && (
                      <Text size="xs" c="dimmed">~{vramEstimate(w)} MB/台</Text>
                    )}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      </Paper>
    </Stack>
  );
}

// ============================================================
// セクション本体
// ============================================================

export function HostConfigSection() {
  const { t } = useTranslation();
  const policyQ = useQuery({
    queryKey: ["host-policy"],
    queryFn: () => hostApi.listPolicy(),
    refetchInterval: 15_000,
  });
  const agentsQ = useQuery({
    queryKey: ["agents"],
    queryFn: () => hostApi.listAgents(),
    refetchInterval: 10_000,
  });
  const workloadsQ = useQuery({
    queryKey: ["workloads-hostcfg"],
    queryFn: () => api.listWorkloads(),
    refetchInterval: 30_000,
  });

  const hosts = policyQ.data?.hosts ?? [];
  const agents = agentsQ.data?.agents ?? [];
  const workloads = workloadsQ.data?.workloads ?? [];
  const agentBy = new Map(agents.map((a) => [a.host, a]));

  if (policyQ.error) return <ErrorState error={policyQ.error} onRetry={() => policyQ.refetch()} />;

  return (
    <Box>
      <Group justify="space-between" mb="xs">
        <Title order={4}>
          {t("hostcfg.title", "ホスト設定")} ({hosts.length})
        </Title>
      </Group>
      <Text size="xs" c="dimmed" mb="sm">
        {t("hostcfg.help",
          "ホストごとの能力と workload 別 目標台数。 台数を上げても動かない時は ①許可 (host_affinity) ②VRAM 予約見込み ③1 ホスト上限 の順に確認してください。")}
      </Text>

      <Accordion variant="separated" multiple
                 defaultValue={hosts.map((h) => h.host)}>
        {hosts.map((h) => {
          const a = agentBy.get(h.host);
          const alive = (a?.last_children ?? []).filter((c) => c.alive);
          const gpuAlive = alive.filter((c) => c.gpu).length;
          const stuck = countStuckGaps(h.host, a, workloads);
          const freePct = h.vram_total_mb
            ? ((h.vram_total_mb - (h.vram_free_mb ?? 0)) / h.vram_total_mb) * 100
            : 0;
          return (
            <Accordion.Item key={h.host} value={h.host}>
              <Accordion.Control>
                <Group justify="space-between" pr="md" wrap="nowrap">
                  <Group gap={8} wrap="nowrap">
                    <Text fw={600}>{h.host}</Text>
                    {h.enabled !== 1 && (
                      <Badge size="xs" color="gray">{t("hostcfg.off", "無効")}</Badge>
                    )}
                    <Badge size="xs" variant="light" color="blue">
                      {t("hostcfg.children", { defaultValue: "子 {{n}}", n: alive.length })}
                    </Badge>
                    <Badge size="xs" variant="light" color="violet">
                      GPU {gpuAlive}
                      {h.max_gpu_workers !== null ? ` / ${h.max_gpu_workers}` : ""}
                    </Badge>
                    {stuck > 0 && (
                      <Badge size="xs" color="red" leftSection={<IconAlertTriangle size={10} />}>
                        {t("hostcfg.stuck_badge", { defaultValue: "未追随 {{n}}", n: stuck })}
                      </Badge>
                    )}
                  </Group>
                  <Group gap={8} wrap="nowrap">
                    <Box w={130}>
                      <Text size="xs" c="dimmed">
                        VRAM {h.vram_free_mb ?? "—"} / {h.vram_total_mb ?? "—"} MB free
                      </Text>
                      <Progress value={freePct} size="xs"
                                color={freePct > 90 ? "red" : freePct > 70 ? "yellow" : "teal"} />
                    </Box>
                    <Text size="xs" c="dimmed" w={70} ta="right">
                      {fmtRelative(h.last_seen_at)}
                    </Text>
                  </Group>
                </Group>
              </Accordion.Control>
              <Accordion.Panel>
                <HostCard policy={h} agent={a} workloads={workloads} />
              </Accordion.Panel>
            </Accordion.Item>
          );
        })}
      </Accordion>
    </Box>
  );
}
