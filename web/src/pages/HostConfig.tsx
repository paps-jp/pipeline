import {
  Badge,
  Button,
  Group,
  NumberInput,
  Paper,
  Select,
  Switch,
  Text,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { IconDeviceDesktopAnalytics } from "@tabler/icons-react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import {
  AgentRecord,
  AgentWorkloadEntry,
  HostPolicy,
  HostPolicyUpdate,
  Workload,
  hostApi,
} from "@/api/client";

// ============================================================
// ホスト設定の共通部品 (画面本体は HostFleet.tsx)
//   host_affinity / agent template.count / VRAM(host_policy) の 3 段制約を扱う。
// ============================================================

export const TIER_OPTIONS: { value: string; labelKey: string; labelJa: string }[] = [
  { value: "", labelKey: "hostcfg.tier_auto", labelJa: "自動判定 (VRAM から推定)" },
  { value: "gpu-16g-pro", labelKey: "hostcfg.tier_gpu16", labelJa: "gpu-16g-pro (16GB 以上)" },
  { value: "gpu-10g-consumer", labelKey: "hostcfg.tier_gpu10", labelJa: "gpu-10g-consumer (10GB 民生)" },
  { value: "cpu-only", labelKey: "hostcfg.tier_cpu", labelJa: "cpu-only (GPU 無し)" },
];

export function fmtRelative(iso: string | null): string {
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
export function vramEstimate(w: Workload | undefined): number {
  if (!w) return 1500;
  const declared = Number((w.resources ?? {}).vram_mb ?? 0);
  return w.observed_vram_mb_peak || declared || 1500;
}


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
export type EffGap = "affinity" | "vram" | "stuck" | null;

export function effectiveGap(
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
export function countStuckGaps(
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
// ホスト能力 (host_policy): 使う/使わない・VRAM 上書き・ティア・メモ
// 日常操作ではなく「このホストの前提」を直すときだけ開く。
// ============================================================

export function HostCapabilityPanel({ policy }: { policy: HostPolicy }) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [pol, setPol] = useState<HostPolicyUpdate | null>(null);

  const polValue = <K extends keyof HostPolicyUpdate>(key: K, fallback: HostPolicyUpdate[K]) =>
    (pol && key in pol ? pol[key] : fallback) as HostPolicyUpdate[K];

  const savePolicy = useMutation({
    mutationFn: () => hostApi.updatePolicy(policy.host, pol ?? {}),
    onSuccess: () => {
      setPol(null);
      qc.invalidateQueries({ queryKey: ["host-policy"] });
      notifications.show({
        color: "teal",
        message: t("hostcfg.saved_policy", {
          host: policy.host,
          defaultValue: "{{host}} のホスト能力を保存しました",
        }),
      });
    },
    onError: (e: unknown) =>
      notifications.show({ color: "red", message: String(e) }),
  });


  return (
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
          data={TIER_OPTIONS.map((o) => ({ value: o.value, label: t(o.labelKey, o.labelJa) }))}
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
  );
}
