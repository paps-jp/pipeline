import {
  Card,
  Group,
  Loader,
  SegmentedControl,
  SimpleGrid,
  Stack,
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
    out.push({
      slug,
      points,
      current: points[points.length - 1].value,
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

export default function Throughput() {
  const { t } = useTranslation();
  const [range, setRange] = useState("60");
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
        <SegmentedControl
          size="xs"
          data={RANGES}
          value={range}
          onChange={setRange}
        />
      </Group>

      {ratesQ.isLoading ? (
        <Group justify="center" p="xl">
          <Loader size="sm" />
        </Group>
      ) : nodes.length === 0 ? (
        <Text c="dimmed" size="sm">
          {t("throughput.collecting", "(データ収集中…)")}
        </Text>
      ) : (
        <SimpleGrid cols={{ base: 1, xs: 2, md: 3, xl: 4 }} spacing="sm">
          {nodes.map((n) => (
            <Card key={n.slug} withBorder radius="md" p="sm">
              <Group justify="space-between" wrap="nowrap" mb={2} gap={6}>
                <Text size="sm" fw={600} truncate title={n.slug}>
                  {nameMap.get(n.slug) ?? n.slug}
                </Text>
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
          "flow_rate_1m の 1 分バケット。各ノードは items_per_min(宣言 metric)優先 / runs_per_min フォールバック。現在分・末尾0除外、10秒更新。"
        )}
      </Text>
    </Stack>
  );
}
