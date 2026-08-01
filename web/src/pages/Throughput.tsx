import {
  Button,
  Card,
  Group,
  Loader,
  SegmentedControl,
  SimpleGrid,
  Stack,
  Switch,
  Text,
  Title,
  UnstyledButton,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { api, type FlowRateRow } from "@/api/client";

// 各ワークフローノード(workload=slug)ごとのスループット時系列を small-multiples /
// overlay で表示。データは既存 GET /api/v1/flow/rates(flow_rate_1m の 1 分バケット)。

// paprika-job-submit だけは items/runs でなく hub の投入レート(hub_started_per_min)を
// 採用する。flow.py と同じで、runs は adopted/timeout ノイズを含み投入数と乖離するため。
const PAPRIKA_SLUG = "paprika-job-submit";

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

function fmtClock(m: number): string {
  const d = new Date(m * 60000);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

// ts_min を UTC epoch-分へ。flow_rate_1m は同じ分を tz なし("…15:05:00" = naive
// UTC)と +00:00 付きの 2 表現で持つため、naive を UTC とみなして揃え重複を畳む。
function tsToEpochMin(ts: string): number {
  const hasTz = /(?:Z|[+-]\d\d:?\d\d)$/.test(ts);
  const t = new Date(hasTz ? ts : `${ts}Z`).getTime();
  return Number.isNaN(t) ? NaN : Math.floor(t / 60000);
}

type Point = { m: number; value: number };
type MetricKind = "items" | "runs" | "hub";
type NodeSeries = {
  slug: string;
  points: Point[];
  current: number;
  prev: number;
  delta: number;
  peak: number;
  metric: MetricKind;
};

const METRIC_LABEL: Record<MetricKind, string> = {
  items: "items",
  runs: "runs",
  hub: "投入",
};

// slug ごとに throughput(件/分)時系列を作る。paprika-job-submit は hub_started_per_min、
// それ以外は items_per_min(宣言 metric)優先 / runs_per_min フォールバック(flow.py と同じ)。
// 現在進行中の分と、集計ラグで末尾に出る 0 バケットは落とす。
function perNodeSeries(series: FlowRateRow[]): NodeSeries[] {
  const bySlug = new Map<
    string,
    Map<number, { items: number; runs: number; hub: number }>
  >();
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
      e = { items: 0, runs: 0, hub: 0 };
      mm.set(m, e);
    }
    if (r.metric === "items_per_min") e.items = Math.max(e.items, r.value);
    else if (r.metric === "runs_per_min") e.runs = Math.max(e.runs, r.value);
    else if (r.metric === "hub_started_per_min") e.hub = Math.max(e.hub, r.value);
  }
  const nowMin = Math.floor(Date.now() / 60000);
  const out: NodeSeries[] = [];
  for (const [slug, mm] of bySlug) {
    const isPaprika = slug === PAPRIKA_SLUG;
    let declared = false;
    for (const e of mm.values()) if (e.items > 0) declared = true;
    const metric: MetricKind = isPaprika ? "hub" : declared ? "items" : "runs";
    const valOf = (e: { items: number; runs: number; hub: number }) =>
      isPaprika ? e.hub : e.items > 0 ? e.items : e.runs;
    const points: Point[] = [...mm.entries()]
      .filter(([m]) => m < nowMin)
      .sort((a, b) => a[0] - b[0])
      .map(([m, e]) => ({ m, value: valOf(e) }));
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
      metric,
    });
  }
  // 現在値の大きい順(=よく捌いているノードが上)。
  out.sort((a, b) => b.current - a.current || b.peak - a.peak);
  return out;
}

// スパークラインの Y 目盛り位置(peak / 半分 / 0)。
const SPARK_TICKS = [1, 0.5, 0];

function Spark({
  points,
  peak,
  gradientId,
}: {
  points: Point[];
  peak: number;
  gradientId: string;
}) {
  const H = 84;
  if (points.length < 2) {
    return (
      <div style={{ height: H, display: "flex", alignItems: "center" }}>
        <Text size="xs" c="dimmed">
          —
        </Text>
      </div>
    );
  }
  const W = 280;
  const PADL = 34; // 左に目盛りラベルの余白
  const PADR = 4;
  const PADT = 6;
  const PADB = 6;
  const plotW = W - PADL - PADR;
  const plotH = H - PADT - PADB;
  const maxV = Math.max(1, peak);
  const x = (i: number) => PADL + (i / (points.length - 1)) * plotW;
  const y = (v: number) => PADT + (1 - v / maxV) * plotH;
  let line = "";
  points.forEach((p, i) => {
    line += `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.value).toFixed(1)}`;
  });
  const base = PADT + plotH;
  const area = `${line} L${x(points.length - 1).toFixed(1)},${base} L${x(0).toFixed(1)},${base} Z`;
  const C = "var(--mantine-color-indigo-5)";
  const pct = (v: number, span: number) => `${(v / span) * 100}%`;
  return (
    <div style={{ position: "relative", height: H }}>
      <svg
        width="100%"
        height="100%"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        style={{ display: "block", position: "absolute", inset: 0 }}
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={C} stopOpacity="0.28" />
            <stop offset="100%" stopColor={C} stopOpacity="0" />
          </linearGradient>
        </defs>
        {SPARK_TICKS.map((f) => {
          const yy = PADT + (1 - f) * plotH;
          return (
            <line
              key={f}
              x1={PADL}
              x2={W - PADR}
              y1={yy}
              y2={yy}
              stroke="var(--mantine-color-default-border)"
              strokeWidth={0.5}
              strokeDasharray={f === 0 ? undefined : "3 4"}
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
        <path d={area} fill={`url(#${gradientId})`} />
        <path
          d={line}
          stroke={C}
          strokeWidth={1.5}
          fill="none"
          strokeLinecap="round"
          strokeLinejoin="round"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
      {SPARK_TICKS.map((f) => {
        const yy = PADT + (1 - f) * plotH;
        return (
          <div
            key={f}
            style={{
              position: "absolute",
              left: 0,
              top: pct(yy, H),
              width: pct(PADL - 4, W),
              transform: "translateY(-50%)",
              textAlign: "right",
              fontSize: 9,
              lineHeight: 1,
              color: "var(--mantine-color-dimmed)",
              fontVariantNumeric: "tabular-nums",
              pointerEvents: "none",
            }}
          >
            {fmtNum(f * maxV)}
          </div>
        );
      })}
    </div>
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

// 重ね表示用の色パレット。slug アルファベット順で固定割り当て(選択状態が変わっても
// 各ノードの色がぶれないように)。paprika-job-submit だけは常にブランドの indigo。
const PALETTE = [
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
const PAPRIKA_COLOR = "var(--mantine-color-indigo-5)";

function buildColorMap(slugs: string[]): Map<string, string> {
  const sorted = [...new Set(slugs.filter((s) => s !== PAPRIKA_SLUG))].sort();
  const map = new Map<string, string>();
  map.set(PAPRIKA_SLUG, PAPRIKA_COLOR);
  sorted.forEach((s, i) => map.set(s, PALETTE[i % PALETTE.length]));
  return map;
}

const TICKS = [1, 0.75, 0.5, 0.25, 0];

// 選択ノードの時系列を 1 枚に重ねる。normalize=true は各線を自ノード peak で 0..1 に
// 正規化(スケール差の大きいノードも形が見える)、false は共通の絶対軸。線にカーソルを
// 重ねると最寄りの線をハイライトし、ノード名/値/時刻のツールチップと、その線の実数
// 目盛り(Y軸)を表示する。
function OverlayChart({
  nodes,
  normalize,
  colorFor,
  labelFor,
}: {
  nodes: NodeSeries[];
  normalize: boolean;
  colorFor: (slug: string) => string;
  labelFor: (slug: string) => string;
}) {
  const [hover, setHover] = useState<{ vx: number; vy: number } | null>(null);

  const allMins = nodes.flatMap((n) => n.points.map((p) => p.m));
  if (allMins.length === 0) return null;
  const minM = Math.min(...allMins);
  const maxM = Math.max(...allMins);
  const spanM = Math.max(1, maxM - minM);
  const globalPeak = Math.max(1, ...nodes.map((n) => n.peak));

  const W = 900;
  const H = 320;
  const PADL = 48;
  const PADR = 12;
  const PADT = 12;
  const PADB = 24;
  const plotW = W - PADL - PADR;
  const plotH = H - PADT - PADB;
  const x = (m: number) => PADL + ((m - minM) / spanM) * plotW;
  const y = (v: number, peak: number) => {
    const maxV = normalize ? Math.max(1, peak) : globalPeak;
    return PADT + (1 - v / maxV) * plotH;
  };

  const mins = [...new Set(allMins)].sort((a, b) => a - b);
  const snapMin = (vx: number): number => {
    const target = minM + ((vx - PADL) / plotW) * spanM;
    let best = mins[0];
    for (const m of mins)
      if (Math.abs(m - target) < Math.abs(best - target)) best = m;
    return best;
  };
  const valueAt = (n: NodeSeries, m: number): Point | null => {
    let best: Point | null = null;
    for (const p of n.points)
      if (!best || Math.abs(p.m - m) < Math.abs(best.m - m)) best = p;
    return best;
  };

  // ホバーから最寄りの線を選ぶ。
  let active: { node: NodeSeries; pt: Point; m: number } | null = null;
  if (hover && hover.vx >= PADL - 4 && hover.vx <= W - PADR + 4) {
    const m = snapMin(hover.vx);
    let bestDist = Infinity;
    for (const n of nodes) {
      const p = valueAt(n, m);
      if (!p) continue;
      const d = Math.abs(hover.vy - y(p.value, n.peak));
      if (d < bestDist) {
        bestDist = d;
        active = { node: n, pt: p, m };
      }
    }
    if (bestDist > plotH * 0.6) active = null;
  }

  // Y軸目盛りの基準ピーク。絶対軸ならグローバル、正規化ならホバー線 or 単独選択線の
  // 実ピーク。複数選択で正規化かつ非ホバー時のみ % 表示にする。
  const singleNode = nodes.length === 1 ? nodes[0] : null;
  const scaleNode = active?.node ?? (normalize ? singleNode : null);
  const showAbsolute = !normalize || scaleNode !== null;
  const scalePeak = !normalize ? globalPeak : scaleNode ? scaleNode.peak : 1;
  const tickColor = active ? colorFor(active.node.slug) : undefined;

  const onMove = (e: React.MouseEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return;
    setHover({
      vx: ((e.clientX - r.left) / r.width) * W,
      vy: ((e.clientY - r.top) / r.height) * H,
    });
  };

  const pct = (v: number, span: number) => `${(v / span) * 100}%`;

  return (
    <div
      style={{ position: "relative", width: "100%", height: H }}
      onMouseMove={onMove}
      onMouseLeave={() => setHover(null)}
    >
      <svg
        width="100%"
        height="100%"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        style={{ display: "block", position: "absolute", inset: 0 }}
      >
        {TICKS.map((f) => {
          const yy = PADT + (1 - f) * plotH;
          return (
            <line
              key={f}
              x1={PADL}
              x2={W - PADR}
              y1={yy}
              y2={yy}
              stroke="var(--mantine-color-default-border)"
              strokeWidth={0.5}
              strokeDasharray={f === 0 ? undefined : "3 4"}
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
        {active && (
          <line
            x1={x(active.m)}
            x2={x(active.m)}
            y1={PADT}
            y2={H - PADB}
            stroke="var(--mantine-color-dimmed)"
            strokeWidth={1}
            strokeDasharray="2 3"
            vectorEffect="non-scaling-stroke"
          />
        )}
        {nodes.map((n) => {
          if (n.points.length < 2) return null;
          const isActive = active?.node.slug === n.slug;
          let line = "";
          n.points.forEach((p, j) => {
            line += `${j ? "L" : "M"}${x(p.m).toFixed(1)},${y(p.value, n.peak).toFixed(1)}`;
          });
          return (
            <path
              key={n.slug}
              d={line}
              stroke={colorFor(n.slug)}
              strokeWidth={isActive ? 2.4 : 1.5}
              strokeOpacity={active && !isActive ? 0.22 : 0.95}
              fill="none"
              strokeLinecap="round"
              strokeLinejoin="round"
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
      </svg>

      {/* Y軸目盛り(HTML オーバーレイ: viewBox の非等倍スケールで文字が歪まないように) */}
      {TICKS.map((f) => {
        const yy = PADT + (1 - f) * plotH;
        const label = showAbsolute
          ? fmtNum(f * scalePeak)
          : `${Math.round(f * 100)}%`;
        return (
          <div
            key={f}
            style={{
              position: "absolute",
              left: 0,
              top: pct(yy, H),
              width: pct(PADL - 6, W),
              transform: "translateY(-50%)",
              textAlign: "right",
              fontSize: 10,
              lineHeight: 1,
              color: tickColor ?? "var(--mantine-color-dimmed)",
              fontVariantNumeric: "tabular-nums",
              pointerEvents: "none",
            }}
          >
            {label}
          </div>
        );
      })}

      {/* X軸(開始/終了時刻) */}
      <div
        style={{
          position: "absolute",
          left: pct(PADL, W),
          bottom: 2,
          fontSize: 10,
          color: "var(--mantine-color-dimmed)",
          pointerEvents: "none",
        }}
      >
        {fmtClock(minM)}
      </div>
      <div
        style={{
          position: "absolute",
          right: pct(PADR, W),
          bottom: 2,
          fontSize: 10,
          color: "var(--mantine-color-dimmed)",
          pointerEvents: "none",
        }}
      >
        {fmtClock(maxM)}
      </div>

      {/* ホバー中の点 + ツールチップ */}
      {active && (
        <>
          <div
            style={{
              position: "absolute",
              left: pct(x(active.m), W),
              top: pct(y(active.pt.value, active.node.peak), H),
              width: 9,
              height: 9,
              marginLeft: -4.5,
              marginTop: -4.5,
              borderRadius: "50%",
              background: colorFor(active.node.slug),
              border: "1.5px solid var(--mantine-color-body)",
              pointerEvents: "none",
            }}
          />
          <div
            style={{
              position: "absolute",
              left: pct(x(active.m), W),
              top: pct(y(active.pt.value, active.node.peak), H),
              transform:
                x(active.m) > W * 0.6
                  ? "translate(calc(-100% - 10px), -50%)"
                  : "translate(10px, -50%)",
              background: "var(--mantine-color-body)",
              border: "1px solid var(--mantine-color-default-border)",
              borderRadius: 6,
              padding: "4px 8px",
              boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
              pointerEvents: "none",
              whiteSpace: "nowrap",
              zIndex: 2,
            }}
          >
            <Group gap={6} wrap="nowrap" mb={1}>
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: 2,
                  background: colorFor(active.node.slug),
                  flex: "0 0 auto",
                }}
              />
              <Text size="xs" fw={600}>
                {labelFor(active.node.slug)}
              </Text>
            </Group>
            <Text size="xs" style={{ fontVariantNumeric: "tabular-nums" }}>
              <Text span fw={700}>
                {fmtNum(active.pt.value)}
              </Text>
              <Text span c="dimmed">
                {" "}
                /分 · {METRIC_LABEL[active.node.metric]} · {fmtClock(active.m)}
              </Text>
            </Text>
          </div>
        </>
      )}
    </div>
  );
}

export default function Throughput() {
  const { t } = useTranslation();
  const [range, setRange] = useState("60");
  const [view, setView] = useState("overlay");
  const [normalize, setNormalize] = useState(true);
  // null = 初期(Paprika のみ)。Set = ユーザが選び直した状態。
  const [selected, setSelected] = useState<Set<string> | null>(null);
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
  const labelFor = (slug: string) => nameMap.get(slug) ?? slug;

  const nodes = ratesQ.data ? perNodeSeries(ratesQ.data.series) : [];
  const totalCurrent = nodes.reduce((s, n) => s + n.current, 0);
  const colorMap = buildColorMap(nodes.map((n) => n.slug));
  const colorFor = (slug: string) => colorMap.get(slug) ?? PAPRIKA_COLOR;
  const paprika = nodes.find((n) => n.slug === PAPRIKA_SLUG);

  // 初期選択 = Paprika のみ(無ければ先頭ノード)。
  const defaultSel = new Set(
    paprika ? [PAPRIKA_SLUG] : nodes.slice(0, 1).map((n) => n.slug)
  );
  const effSel = selected ?? defaultSel;
  const overlayNodes = nodes.filter((n) => effSel.has(n.slug));

  const toggle = (slug: string) => {
    const next = new Set(effSel);
    if (next.has(slug)) next.delete(slug);
    else next.add(slug);
    setSelected(next);
  };

  return (
    <Stack gap="md">
      <Group justify="space-between" wrap="nowrap">
        <div>
          <Title order={2}>{t("throughput.title", "スループット")}</Title>
          <Text size="xs" c="dimmed">
            {t("throughput.subtitle", "ワークフローノードごとの処理レート (件/分)")}
            {paprika && (
              <>
                {" · "}
                <Text span c="indigo" fw={600}>
                  Paprika投入 {fmtNum(paprika.current)}/分
                </Text>
              </>
            )}
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
          <Group justify="space-between" wrap="nowrap">
            <Group gap={6} wrap="nowrap">
              <Button
                size="compact-xs"
                variant="subtle"
                onClick={() => setSelected(new Set(nodes.map((n) => n.slug)))}
              >
                {t("throughput.selectAll", "全表示")}
              </Button>
              <Button
                size="compact-xs"
                variant="subtle"
                color="gray"
                onClick={() => setSelected(defaultSel)}
                title="Paprika投入のみ"
              >
                {t("throughput.reset", "Paprikaのみ")}
              </Button>
            </Group>
            <Switch
              size="xs"
              checked={normalize}
              onChange={(e) => setNormalize(e.currentTarget.checked)}
              label={t("throughput.normalize", "各ノードを正規化 (形を揃える)")}
            />
          </Group>
          <Card withBorder radius="md" p="sm">
            {overlayNodes.length === 0 ? (
              <Text c="dimmed" size="sm" ta="center" py="xl">
                {t("throughput.pickNode", "凡例からノードを選んでください")}
              </Text>
            ) : (
              <OverlayChart
                nodes={overlayNodes}
                normalize={normalize}
                colorFor={colorFor}
                labelFor={labelFor}
              />
            )}
          </Card>
          <SimpleGrid cols={{ base: 2, xs: 3, md: 4, xl: 6 }} spacing="xs">
            {nodes.map((n) => {
              const on = effSel.has(n.slug);
              return (
                <UnstyledButton
                  key={n.slug}
                  onClick={() => toggle(n.slug)}
                  style={{ opacity: on ? 1 : 0.4 }}
                  title={on ? "クリックで非表示" : "クリックで表示"}
                >
                  <Group gap={6} wrap="nowrap">
                    <span
                      style={{
                        width: 10,
                        height: 10,
                        borderRadius: 2,
                        background: on ? colorFor(n.slug) : "transparent",
                        border: `1.5px solid ${colorFor(n.slug)}`,
                        flex: "0 0 auto",
                      }}
                    />
                    <Text size="xs" truncate title={n.slug} style={{ flex: 1 }}>
                      {labelFor(n.slug)}
                    </Text>
                    <Text
                      size="xs"
                      fw={600}
                      style={{
                        fontVariantNumeric: "tabular-nums",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {fmtNum(n.current)}
                    </Text>
                    <DeltaArrow delta={n.delta} />
                  </Group>
                </UnstyledButton>
              );
            })}
          </SimpleGrid>
        </Stack>
      ) : (
        <SimpleGrid cols={{ base: 1, xs: 2, md: 3, xl: 4 }} spacing="sm">
          {nodes.map((n) => (
            <Card key={n.slug} withBorder radius="md" p="sm">
              <Group justify="space-between" wrap="nowrap" mb={2} gap={6}>
                <Text size="sm" fw={600} truncate title={n.slug}>
                  {labelFor(n.slug)}
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
                  {METRIC_LABEL[n.metric]}
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
          "flow_rate_1m の 1 分バケット。paprika-job-submit は hub 投入レート(hub_started_per_min)、他は items_per_min 優先 / runs_per_min フォールバック。重ねる表示は線にカーソルを重ねるとノード名・値・目盛りを表示。▲/▼ は直前の 1 分バケット比。現在分・末尾0除外、10秒更新。"
        )}
      </Text>
    </Stack>
  );
}
