import {
  Card,
  Group,
  Loader,
  SegmentedControl,
  SimpleGrid,
  Stack,
  Switch,
  Text,
  Title,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { api, type FlowRateRow } from "@/api/client";

// 各ワークフローノード(workload=slug)ごとのスループット時系列を small-multiples で
// 表示。データは既存 GET /api/v1/flow/rates(flow_rate_1m の 1 分バケット)。

const RANGES = [
  { label: "1h", value: "60" },
  { label: "6h", value: "360" },
  { label: "24h", value: "1440" },
];

function fmtNum(v: number): string {
  if (v >= 10000) return `${(v / 1000).toFixed(1)}k`;
  if (v >= 100) return v.toFixed(0);
  return String(Math.round(v * 10) / 10);
}

// ts_min を UTC epoch-分へ。flow_rate_1m は同じ分を tz なし("…15:05:00" = naive
// UTC)と +00:00 付きの 2 表現で持つため、naive を UTC とみなして揃え重複を畳む。
function tsToEpochMin(ts: string): number {
  const hasTz = /(?:Z|[+-]\d\d:?\d\d)$/.test(ts);
  const t = new Date(hasTz ? ts : `${ts}Z`).getTime();
  return Number.isNaN(t) ? NaN : Math.floor(t / 60000);
}

type Point = { m: number; value: number };
type NodeSeries = {
  slug: string;
  points: Point[];
  current: number;
  prev: number;
  delta: number;
  peak: number;
  metric: "items" | "runs";
};

// slug ごとに throughput(件/分)時系列を作る。宣言 metric があれば items_per_min を、
// 無ければ runs_per_min を採用(flow.py と同じ)。現在進行中の分と、集計ラグで末尾に
// 出る 0 バケットは落とす。
function perNodeSeries(series: FlowRateRow[]): NodeSeries[] {
  const bySlug = new Map<string, Map<number, { items: number; runs: number }>>();
  for (const r of series) {
    const m = tsToEpochMin(r.ts_min);
    if (Number.isNaN(m)) continue;
    let mm = bySlug.get(r.slug);
    if (!mm) {
      mm = new Map();
      bySlug.set(r.slug, mm);
    }
    let e = mm.get(m);
    if (!e) {
      e = { items: 0, runs: 0 };
      mm.set(m, e);
    }
    if (r.metric === "items_per_min") e.items = Math.max(e.items, r.value);
    else if (r.metric === "runs_per_min") e.runs = Math.max(e.runs, r.value);
  }
  const nowMin = Math.floor(Date.now() / 60000);
  const out: NodeSeries[] = [];
  for (const [slug, mm] of bySlug) {
    let declared = false;
    for (const e of mm.values()) if (e.items > 0) declared = true;
    const points: Point[] = [...mm.entries()]
      .filter(([m]) => m < nowMin)
      .sort((a, b) => a[0] - b[0])
      .map(([m, e]) => ({ m, value: e.items > 0 ? e.items : e.runs }));
    while (points.length > 1 && points[points.length - 1].value === 0) points.pop();
    if (points.length === 0) continue;
    const current = points[points.length - 1].value;
    const prev = points.length >= 2 ? points[points.length - 2].value : current;
    out.push({
      slug,
      points,
      current,
      prev,
      delta: current - prev,
      peak: Math.max(...points.map((p) => p.value)),
      metric: declared ? "items" : "runs",
    });
  }
  // 現在値の大きい順(=よく捌いているノードが上)。
  out.sort((a, b) => b.current - a.current || b.peak - a.peak);
  return out;
}

function Spark({
  points,
  peak,
  gradientId,
}: {
  points: Point[];
  peak: number;
  gradientId: string;
}) {
  if (points.length < 2) {
    return (
      <div style={{ height: 48, display: "flex", alignItems: "center" }}>
        <Text size="xs" c="dimmed">
          —
        </Text>
      </div>
    );
  }
  const W = 280;
  const H = 48;
  const PAD = 3;
  const maxV = Math.max(1, peak);
  const x = (i: number) => PAD + (i / (points.length - 1)) * (W - 2 * PAD);
  const y = (v: number) => H - PAD - (v / maxV) * (H - 2 * PAD);
  let line = "";
  points.forEach((p, i) => {
    line += `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.value).toFixed(1)}`;
  });
  const area = `${line} L${x(points.length - 1).toFixed(1)},${H - PAD} L${x(0).toFixed(1)},${H - PAD} Z`;
  const C = "var(--mantine-color-indigo-5)";
  return (
    <svg
      width="100%"
      height={H}
      viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      style={{ display: "block" }}
    >
      <defs>
        <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={C} stopOpacity="0.28" />
          <stop offset="100%" stopColor={C} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={area} fill={`url(#${gradientId})`} />
      <path
        d={line}
        stroke={C}
        strokeWidth={1.5}
        fill="none"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

// 前回バケット比の増減矢印。増=緑▲ / 減=赤▼ / 変化なし=灰→。
function DeltaArrow({ delta }: { delta: number }) {
  if (delta > 0)
    return (
      <Text span size="xs" c="teal.6" style={{ whiteSpace: "nowrap" }} title={`+${fmtNum(delta)}/分`}>
        ▲ {fmtNum(delta)}
      </Text>
    );
  if (delta < 0)
    return (
      <Text span size="xs" c="red.6" style={{ whiteSpace: "nowrap" }} title={`${fmtNum(delta)}/分`}>
        ▼ {fmtNum(-delta)}
      </Text>
    );
  return (
    <Text span size="xs" c="dimmed" title="変化なし">
      →
    </Text>
  );
}

// 重ね表示用の色パレット(ノード順に割り当て)。
const PALETTE = [
  "var(--mantine-color-indigo-5)",
  "var(--mantine-color-teal-5)",
  "var(--mantine-color-orange-5)",
  "var(--mantine-color-pink-5)",
  "var(--mantine-color-lime-6)",
  "var(--mantine-color-cyan-5)",
  "var(--mantine-color-grape-5)",
  "var(--mantine-color-red-5)",
  "var(--mantine-color-yellow-6)",
  "var(--mantine-color-blue-5)",
  "var(--mantine-color-green-6)",
  "var(--mantine-color-violet-5)",
];
const colorFor = (i: number) => PALETTE[i % PALETTE.length];

// 全ノードの時系列を 1 枚の SVG に重ねる。normalize=true は各線を自ノード peak で
// 0..1 に正規化(スケール差の大きいノードも形が見える)、false は全ノード共通の
// 絶対軸(絶対量の比較用)。
function OverlayChart({
  nodes,
  normalize,
}: {
  nodes: NodeSeries[];
  normalize: boolean;
}) {
  const allMins = nodes.flatMap((n) => n.points.map((p) => p.m));
  if (allMins.length === 0) return null;
  const minM = Math.min(...allMins);
  const maxM = Math.max(...allMins);
  const spanM = Math.max(1, maxM - minM);
  const globalPeak = Math.max(1, ...nodes.map((n) => n.peak));
  const W = 900;
  const H = 320;
  const PADL = 8;
  const PADR = 8;
  const PADT = 10;
  const PADB = 10;
  const x = (m: number) => PADL + ((m - minM) / spanM) * (W - PADL - PADR);
  const y = (v: number, peak: number) => {
    const maxV = normalize ? Math.max(1, peak) : globalPeak;
    return PADT + (1 - v / maxV) * (H - PADT - PADB);
  };
  return (
    <svg
      width="100%"
      viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      style={{ display: "block", height: 320 }}
    >
      {[0.25, 0.5, 0.75].map((f) => (
        <line
          key={f}
          x1={PADL}
          x2={W - PADR}
          y1={PADT + f * (H - PADT - PADB)}
          y2={PADT + f * (H - PADT - PADB)}
          stroke="var(--mantine-color-default-border)"
          strokeWidth={0.5}
          strokeDasharray="3 4"
        />
      ))}
      {nodes.map((n, i) => {
        if (n.points.length < 2) return null;
        let line = "";
        n.points.forEach((p, j) => {
          line += `${j ? "L" : "M"}${x(p.m).toFixed(1)},${y(p.value, n.peak).toFixed(1)}`;
        });
        return (
          <path
            key={n.slug}
            d={line}
            stroke={colorFor(i)}
            strokeWidth={1.5}
            fill="none"
            strokeLinecap="round"
            strokeLinejoin="round"
            vectorEffect="non-scaling-stroke"
          />
        );
      })}
    </svg>
  );
}

export default function Throughput() {
  const { t } = useTranslation();
  const [range, setRange] = useState("60");
  const [view, setView] = useState("grid");
  const [normalize, setNormalize] = useState(true);
  const ratesQ = useQuery({
    queryKey: ["flow-rates", range],
    queryFn: () => api.flowRates(Number(range)),
    refetchInterval: 10_000,
  });
  const wlQ = useQuery({
    queryKey: ["workloads-for-throughput"],
    queryFn: () => api.listWorkloads(),
    staleTime: 60_000,
  });
  const nameMap = new Map(
    (wlQ.data?.workloads ?? []).map((w) => [w.slug, w.name])
  );

  const nodes = ratesQ.data ? perNodeSeries(ratesQ.data.series) : [];
  const totalCurrent = nodes.reduce((s, n) => s + n.current, 0);

  return (
    <Stack gap="md">
      <Group justify="space-between" wrap="nowrap">
        <div>
          <Title order={2}>{t("throughput.title", "スループット")}</Title>
          <Text size="xs" c="dimmed">
            {t("throughput.subtitle", "ワークフローノードごとの処理レート (件/分)")}
            {nodes.length > 0 &&
              ` · ${nodes.length} ノード · 合計 ${fmtNum(totalCurrent)}/分`}
          </Text>
        </div>
        <Group gap="sm" wrap="nowrap">
          <SegmentedControl
            size="xs"
            data={[
              { label: t("throughput.view.grid", "並べる"), value: "grid" },
              { label: t("throughput.view.overlay", "重ねる"), value: "overlay" },
            ]}
            value={view}
            onChange={setView}
          />
          <SegmentedControl
            size="xs"
            data={RANGES}
            value={range}
            onChange={setRange}
          />
        </Group>
      </Group>

      {ratesQ.isLoading ? (
        <Group justify="center" p="xl">
          <Loader size="sm" />
        </Group>
      ) : nodes.length === 0 ? (
        <Text c="dimmed" size="sm">
          {t("throughput.collecting", "(データ収集中…)")}
        </Text>
      ) : view === "overlay" ? (
        <Stack gap="sm">
          <Group justify="flex-end">
            <Switch
              size="xs"
              checked={normalize}
              onChange={(e) => setNormalize(e.currentTarget.checked)}
              label={t("throughput.normalize", "各ノードを正規化 (形を揃える)")}
            />
          </Group>
          <Card withBorder radius="md" p="sm">
            <OverlayChart nodes={nodes} normalize={normalize} />
          </Card>
          <SimpleGrid cols={{ base: 2, xs: 3, md: 4, xl: 6 }} spacing="xs">
            {nodes.map((n, i) => (
              <Group key={n.slug} gap={6} wrap="nowrap">
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: 2,
                    background: colorFor(i),
                    flex: "0 0 auto",
                  }}
                />
                <Text size="xs" truncate title={n.slug} style={{ flex: 1 }}>
                  {nameMap.get(n.slug) ?? n.slug}
                </Text>
                <Text
                  size="xs"
                  fw={600}
                  style={{ fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}
                >
                  {fmtNum(n.current)}
                </Text>
                <DeltaArrow delta={n.delta} />
              </Group>
            ))}
          </SimpleGrid>
        </Stack>
      ) : (
        <SimpleGrid cols={{ base: 1, xs: 2, md: 3, xl: 4 }} spacing="sm">
          {nodes.map((n) => (
            <Card key={n.slug} withBorder radius="md" p="sm">
              <Group justify="space-between" wrap="nowrap" mb={2} gap={6}>
                <Text size="sm" fw={600} truncate title={n.slug}>
                  {nameMap.get(n.slug) ?? n.slug}
                </Text>
                <Group gap={6} wrap="nowrap">
                  <Text
                    size="sm"
                    fw={700}
                    c="indigo"
                    style={{
                      fontVariantNumeric: "tabular-nums",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {fmtNum(n.current)}
                    <Text span size="xs" c="dimmed">
                      {" "}
                      /分
                    </Text>
                  </Text>
                  <DeltaArrow delta={n.delta} />
                </Group>
              </Group>
              <Group gap={6} mb={4} wrap="nowrap">
                <Text size="10px" c="dimmed">
                  peak {fmtNum(n.peak)}
                </Text>
                <Text size="10px" c="dimmed">
                  · {n.metric === "items" ? "items" : "runs"}
                </Text>
              </Group>
              <Spark points={n.points} peak={n.peak} gradientId={`sp-${n.slug}`} />
            </Card>
          ))}
        </SimpleGrid>
      )}

      <Text size="xs" c="dimmed">
        {t(
          "throughput.note",
          "flow_rate_1m の 1 分バケット。各ノードは items_per_min(宣言 metric)優先 / runs_per_min フォールバック。▲/▼ は直前の 1 分バケット比の増減。現在分・末尾0除外、10秒更新。"
        )}
      </Text>
    </Stack>
  );
}
