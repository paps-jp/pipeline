import {
  Accordion,
  ActionIcon,
  Badge,
  Group,
  List,
  Paper,
  ScrollArea,
  SegmentedControl,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  ThemeIcon,
  Title,
  Tooltip,
} from "@mantine/core";
import { useDebouncedValue } from "@mantine/hooks";
import {
  IconCheck,
  IconClock,
  IconHelpCircle,
  IconRefresh,
  IconSearch,
  IconX,
} from "@tabler/icons-react";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import type { TFunction } from "i18next";
import { Trans, useTranslation } from "react-i18next";

import {
  api,
  ApiError,
  pluginStateApi,
  siteYieldApi,
  SiteYieldRow,
  YieldPick,
  YieldStatRow,
} from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

// ============================================================
// サイト収率: 一般向けの成績表。 専門用語はページ末尾の「詳しい人向け」に閉じ込める。
//   lane ts = 新しい選び方 (成績で選ぶ) / rr = いつもの選び方 (順番に回る)
//   報酬     = 見に行ったページに新しい画像が 1 枚でもあれば「当たり」
// ============================================================

const SUBMIT_SLUG = "paprika-job-submit";
const MIN_JOBS_FOR_VERDICT = 50;   // これ未満では比べない
const PROVISIONAL_HOURS = 12;      // 切替からこの時間までは「参考値」扱い

/** 子ヘルパーへ渡す t()。 */
type TFn = TFunction;

const LANE_ORDER = ["ts", "rr", "pr", "vg", "na"];
const LANE_NAME_JA: Record<string, string> = {
  ts: "新しい選び方",
  rr: "いつもの選び方",
  pr: "優先指定のサイト",
  vg: "初めてのサイト",
  na: "切り替え前に出した分",
};
const LANE_COLOR: Record<string, string> = {
  ts: "var(--mantine-color-indigo-6)",
  rr: "var(--mantine-color-gray-5)",
  pr: "var(--mantine-color-orange-5)",
  vg: "var(--mantine-color-teal-5)",
  na: "var(--mantine-color-gray-3)",
};
const GRADE: Record<string, { labelJa: string; color: string; hintJa: string }> = {
  good: { labelJa: "よく取れる", color: "teal", hintJa: "当たり率 60% 以上" },
  ok: { labelJa: "ふつう", color: "blue", hintJa: "当たり率 30〜60%" },
  poor: { labelJa: "あまり取れない", color: "red", hintJa: "当たり率 30% 未満" },
  unknown: { labelJa: "まだわからない", color: "gray", hintJa: "見に行った回数が少ない" },
};
const RANGES: { value: string; labelKey: string; labelJa: string }[] = [
  { value: "6", labelKey: "siteYield.range.6h", labelJa: "6時間" },
  { value: "24", labelKey: "siteYield.range.24h", labelJa: "24時間" },
  { value: "168", labelKey: "siteYield.range.7d", labelJa: "7日" },
];
const ORDERS: { value: string; labelKey: string; labelJa: string }[] = [
  { value: "mean_asc", labelKey: "siteYield.order.mean_asc", labelJa: "当たり率が低い順" },
  { value: "mean_desc", labelKey: "siteYield.order.mean_desc", labelJa: "当たり率が高い順" },
  { value: "jobs_desc", labelKey: "siteYield.order.jobs_desc", labelJa: "よく見に行っている順" },
  { value: "images_desc", labelKey: "siteYield.order.images_desc", labelJa: "見つけた画像が多い順" },
  { value: "updated_desc", labelKey: "siteYield.order.updated_desc", labelJa: "最近見た順" },
  { value: "site", labelKey: "siteYield.order.site", labelJa: "名前順" },
];
const GRADE_FILTER: { value: string; labelKey: string; labelJa: string }[] = [
  { value: "all", labelKey: "siteYield.grade_filter.all", labelJa: "すべての成績" },
  { value: "good", labelKey: "siteYield.grade.good.label", labelJa: "よく取れる" },
  { value: "ok", labelKey: "siteYield.grade.ok.label", labelJa: "ふつう" },
  { value: "poor", labelKey: "siteYield.grade.poor.label", labelJa: "あまり取れない" },
  { value: "unknown", labelKey: "siteYield.grade.unknown.label", labelJa: "まだわからない" },
];

function selectData(
  t: TFn,
  opts: { value: string; labelKey: string; labelJa: string }[],
): { value: string; label: string }[] {
  return opts.map((o) => ({ value: o.value, label: t(o.labelKey, o.labelJa) }));
}

function gradeLabel(t: TFn, key: string): string {
  const g = GRADE[key] ?? GRADE.unknown;
  return t(`siteYield.grade.${GRADE[key] ? key : "unknown"}.label`, g.labelJa);
}

function gradeHint(t: TFn, key: string): string {
  const g = GRADE[key] ?? GRADE.unknown;
  return t(`siteYield.grade.${GRADE[key] ? key : "unknown"}.hint`, g.hintJa);
}

function fmtAge(t: TFn, s: number | null | undefined): string {
  if (s === null || s === undefined) return "—";
  if (s < 90) return t("siteYield.age_now", "たった今");
  if (s < 90 * 60) return t("siteYield.age_min", "{{n}}分前").replace("{{n}}", String(Math.round(s / 60)));
  if (s < 36 * 3600) return t("siteYield.age_hour", "{{n}}時間前").replace("{{n}}", String(Math.round(s / 3600)));
  return t("siteYield.age_day", "{{n}}日前").replace("{{n}}", String(Math.round(s / 86400)));
}

function fmtNum(v: number, digits = 2): string {
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

function pct(v: number): string {
  return Number.isFinite(v) ? `${Math.round(v * 100)}%` : "—";
}

function laneName(t: TFn, l: string): string {
  const ja = LANE_NAME_JA[l];
  return ja ? t(`siteYield.lane.${l}`, ja) : l;
}

function laneColor(l: string): string {
  return LANE_COLOR[l] ?? "var(--mantine-color-gray-4)";
}

function hoursSince(dbLocal: string | undefined): number | null {
  if (!dbLocal) return null;
  const d = new Date(dbLocal.replace(" ", "T"));
  if (Number.isNaN(d.getTime())) return null;
  return (Date.now() - d.getTime()) / 3600_000;
}

// ---------------- 折れ線 (lane 別) ----------------

interface LineSeries {
  key: string;
  label: string;
  color: string;
  values: Map<string, number>;
}

function LaneLineChart({
  xs,
  series,
  height = 190,
  fmt = (v: number) => v.toFixed(2),
}: {
  xs: string[];
  series: LineSeries[];
  height?: number;
  fmt?: (v: number) => string;
}) {
  const { t } = useTranslation();
  const W = 640;
  const H = height;
  const PADL = 44;
  const PADR = 10;
  const PADT = 8;
  const PADB = 20;
  const plotW = W - PADL - PADR;
  const plotH = H - PADT - PADB;
  let maxV = 0;
  series.forEach((s) => s.values.forEach((v) => { if (v > maxV) maxV = v; }));
  const yMax = maxV > 0 ? maxV * 1.1 : 1;
  const n = xs.length;
  const x = (i: number) => PADL + (n > 1 ? (i / (n - 1)) * plotW : plotW / 2);
  const y = (v: number) => PADT + (1 - v / yMax) * plotH;
  const multiDay = n > 1 && xs[0].slice(0, 10) !== xs[n - 1].slice(0, 10);
  const lab = (ts: string) => (multiDay ? ts.slice(5, 16) : ts.slice(11, 16));
  const paths = series.map((s) => {
    let d = "";
    let pen = false;
    xs.forEach((ts, i) => {
      const v = s.values.get(ts);
      if (v === undefined) { pen = false; return; }
      d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
      pen = true;
    });
    return { ...s, d };
  });
  if (n === 0) {
    return (
      <Text size="sm" c="dimmed" style={{ height }}>
        {t("siteYield.no_data", "まだデータがありません")}
      </Text>
    );
  }
  const xLabelIdx = n >= 3 ? [0, Math.floor((n - 1) / 2), n - 1] : n === 2 ? [0, 1] : [0];
  return (
    <Stack gap={4}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }}>
        {[0, 0.5, 1].map((f) => {
          const yy = PADT + (1 - f) * plotH;
          return (
            <g key={f}>
              <line
                x1={PADL}
                x2={W - PADR}
                y1={yy}
                y2={yy}
                stroke="var(--mantine-color-default-border)"
                strokeWidth={0.6}
                strokeDasharray={f === 0 ? undefined : "3 4"}
              />
              <text
                x={PADL - 6}
                y={yy + 3.5}
                fontSize={10}
                textAnchor="end"
                fill="var(--mantine-color-dimmed)"
              >
                {fmt(yMax * f)}
              </text>
            </g>
          );
        })}
        {xLabelIdx.map((i) => (
          <text
            key={i}
            x={x(i)}
            y={H - 6}
            fontSize={10}
            textAnchor={i === 0 ? "start" : i === n - 1 ? "end" : "middle"}
            fill="var(--mantine-color-dimmed)"
          >
            {lab(xs[i])}
          </text>
        ))}
        {paths.map((p) => (
          <path
            key={p.key}
            d={p.d}
            stroke={p.color}
            strokeWidth={2}
            fill="none"
            strokeLinejoin="round"
            strokeLinecap="round"
          />
        ))}
      </svg>
      <Group gap="md" wrap="wrap">
        {series.map((s) => {
          const last = [...xs].reverse().map((ts) => s.values.get(ts)).find((v) => v !== undefined);
          return (
            <Group key={s.key} gap={6} wrap="nowrap">
              <span
                style={{
                  display: "inline-block",
                  width: 12,
                  height: 12,
                  borderRadius: 3,
                  background: s.color,
                }}
              />
              <Text size="sm">{s.label}</Text>
              <Text size="sm" c="dimmed" style={{ fontFamily: "ui-monospace, monospace" }}>
                {last === undefined ? "—" : fmt(last)}
              </Text>
            </Group>
          );
        })}
      </Group>
    </Stack>
  );
}

// ---------------- 当たり率のバー ----------------

function HitRateBar({ row }: { row: SiteYieldRow }) {
  const { t } = useTranslation();
  const p = (v: number) => `${Math.max(0, Math.min(100, v * 100)).toFixed(1)}%`;
  const g = GRADE[row.grade] ?? GRADE.unknown;
  const color = `var(--mantine-color-${g.color}-6)`;
  return (
    <Tooltip
      label={t("siteYield.hit_rate_tip", {
        defaultValue:
          "当たり率 だいたい {{mean}} ({{lo}}〜{{hi}} の範囲) / 最近 {{n}} 回の記録から",
        mean: pct(row.mean),
        lo: pct(row.lo),
        hi: pct(row.hi),
        n: fmtNum(row.n, 0),
      })}
      withArrow
    >
      <Group gap={8} wrap="nowrap">
        <div style={{ position: "relative", height: 12, width: 120, background: "var(--mantine-color-default-border)", borderRadius: 3 }}>
          <div
            style={{
              position: "absolute",
              left: p(row.lo),
              width: `calc(${p(row.hi)} - ${p(row.lo)})`,
              top: 3,
              height: 6,
              background: color,
              opacity: 0.3,
              borderRadius: 2,
            }}
          />
          <div
            style={{
              position: "absolute",
              left: 0,
              width: p(row.mean),
              top: 0,
              height: 12,
              background: color,
              opacity: row.grade === "unknown" ? 0.35 : 0.85,
              borderRadius: 3,
            }}
          />
        </div>
        <Text size="sm" style={{ fontFamily: "ui-monospace, monospace", minWidth: 36 }}>
          {row.grade === "unknown" ? "—" : pct(row.mean)}
        </Text>
      </Group>
    </Tooltip>
  );
}

// ---------------- 集計 ----------------

interface LaneTotals {
  jobs: number;
  productive: number;
  new_images: number;
  dup_images: number;
  no_image_jobs: number;
}

function sumLanes(rows: YieldStatRow[]): Record<string, LaneTotals> {
  const out: Record<string, LaneTotals> = {};
  for (const r of rows) {
    const t = (out[r.lane] ??= { jobs: 0, productive: 0, new_images: 0, dup_images: 0, no_image_jobs: 0 });
    t.jobs += r.jobs;
    t.productive += r.productive;
    t.new_images += r.new_images;
    t.dup_images += r.dup_images;
    t.no_image_jobs += r.no_image_jobs;
  }
  return out;
}

const perJob = (t: LaneTotals | undefined) => (t && t.jobs > 0 ? t.new_images / t.jobs : NaN);
const hitRate = (t: LaneTotals | undefined) => (t && t.jobs > 0 ? t.productive / t.jobs : NaN);

type VerdictKind = "wait" | "good" | "same" | "bad";

function computeVerdict(ts: LaneTotals | undefined, rr: LaneTotals | undefined): {
  kind: VerdictKind; ratio: number;
} {
  if (!ts || !rr || ts.jobs < MIN_JOBS_FOR_VERDICT || rr.jobs < MIN_JOBS_FOR_VERDICT) {
    return { kind: "wait", ratio: NaN };
  }
  const a = perJob(ts);
  const b = perJob(rr);
  if (!(b > 0)) return { kind: "wait", ratio: NaN };
  const ratio = a / b;
  if (ratio >= 1.15) return { kind: "good", ratio };
  if (ratio <= 0.85) return { kind: "bad", ratio };
  return { kind: "same", ratio };
}

// ---------------- 判定カード ----------------

function VerdictCard({
  ts, rr, hoursLabel, sinceH, provisional,
}: {
  ts?: LaneTotals; rr?: LaneTotals; hoursLabel: string; sinceH: number | null; provisional: boolean;
}) {
  const { t } = useTranslation();
  const v = computeVerdict(ts, rr);
  const styles: Record<VerdictKind, { color: string; title: string; icon: JSX.Element }> = {
    wait: { color: "gray", title: t("siteYield.verdict.wait", "まだ集計中です"), icon: <IconClock size={28} /> },
    good: { color: "teal", title: t("siteYield.verdict.good", "新しい選び方は効いています"), icon: <IconCheck size={28} /> },
    same: { color: "yellow", title: t("siteYield.verdict.same", "今のところ差はありません"), icon: <IconHelpCircle size={28} /> },
    bad: { color: "red", title: t("siteYield.verdict.bad", "いつもの選び方のほうが良さそうです"), icon: <IconX size={28} /> },
  };
  const s = styles[v.kind];
  let body: string;
  if (v.kind === "wait") {
    body = t("siteYield.verdict.wait_body", {
      defaultValue:
        "比べるにはどちらも {{min}} ページ以上必要です (この{{range}}: 新しい選び方 {{ts}} ページ / いつもの選び方 {{rr}} ページ)。",
      min: MIN_JOBS_FOR_VERDICT,
      range: hoursLabel,
      ts: ts?.jobs ?? 0,
      rr: rr?.jobs ?? 0,
    });
  } else {
    body = t("siteYield.verdict.body", {
      defaultValue:
        "この{{range}}、1 ページ見に行くごとに見つかった新しい画像は、新しい選び方が {{tsPer}} 枚、いつもの選び方が {{rrPer}} 枚。新しい選び方は {{ratio}} 倍です。",
      range: hoursLabel,
      tsPer: fmtNum(perJob(ts), 1),
      rrPer: fmtNum(perJob(rr), 1),
      ratio: fmtNum(v.ratio, 2),
    });
  }
  return (
    <Paper withBorder p="lg" style={{ borderColor: `var(--mantine-color-${s.color}-5)`, borderWidth: 2 }}>
      <Group align="flex-start" wrap="nowrap" gap="md">
        <ThemeIcon size={48} radius="xl" color={s.color} variant="light">
          {s.icon}
        </ThemeIcon>
        <Stack gap={4} style={{ flex: 1 }}>
          <Text size="sm" c="dimmed">{t("siteYield.verdict_question", "新しい選び方は効いている?")}</Text>
          <Title order={2} c={`${s.color}.7`} style={{ letterSpacing: "-0.01em" }}>
            {s.title}
            {Number.isFinite(v.ratio) && (
              <Text span size="xl" c="dimmed" fw={500}> (×{fmtNum(v.ratio, 2)})</Text>
            )}
          </Title>
          <Text size="md">{body}</Text>
          {provisional && sinceH !== null && (
            <Text size="sm" c="orange.7">
              {t("siteYield.provisional", {
                defaultValue:
                  "切り替えてからまだ {{elapsed}} しか経っていません。最初のうちは「まだ見ていないサイトを一通り試す」動きが混ざるので、半日〜1 日たってからの数字を見てください。",
                elapsed:
                  sinceH < 1
                    ? t("siteYield.elapsed_min", "{{n}} 分").replace("{{n}}", String(Math.round(sinceH * 60)))
                    : t("siteYield.elapsed_hour", "{{n}} 時間").replace("{{n}}", String(Math.round(sinceH))),
              })}
            </Text>
          )}
        </Stack>
      </Group>
    </Paper>
  );
}

// ---------------- 大きな数字で比べる ----------------

function BigStat({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <Stack gap={0}>
      <Text size="xs" c="dimmed">{label}</Text>
      <Text size="xl" fw={700} c={color} style={{ fontFamily: "ui-monospace, monospace", lineHeight: 1.2 }}>
        {value}
      </Text>
      {sub && <Text size="xs" c="dimmed">{sub}</Text>}
    </Stack>
  );
}

function CompareCard({ ts, rr, hoursLabel }: { ts?: LaneTotals; rr?: LaneTotals; hoursLabel: string }) {
  const { t } = useTranslation();
  const cols: { key: string; totals?: LaneTotals; desc: string }[] = [
    { key: "ts", totals: ts, desc: t("siteYield.compare.ts_desc", "当たりが多いサイトを多めに見に行く") },
    { key: "rr", totals: rr, desc: t("siteYield.compare.rr_desc", "サイトを順番に回る (これまでのやり方)") },
  ];
  return (
    <Paper withBorder p="md">
      <Text size="sm" fw={600} mb="xs">
        {t("siteYield.compare.title", {
          defaultValue: "この{{range}}の成績を比べる",
          range: hoursLabel,
        })}
      </Text>
      <SimpleGrid cols={2}>
        {cols.map((c) => (
          <Paper key={c.key} p="sm" radius="md" style={{ background: "var(--mantine-color-default-hover)" }}>
            <Group gap={6} mb={6}>
              <span style={{ display: "inline-block", width: 12, height: 12, borderRadius: 3, background: laneColor(c.key) }} />
              <Text size="sm" fw={600}>{laneName(t, c.key)}</Text>
            </Group>
            <Text size="xs" c="dimmed" mb="xs">{c.desc}</Text>
            <Stack gap="sm">
              <BigStat
                label={t("siteYield.compare.per_page", "1 ページあたり見つかった新しい画像")}
                value={t("siteYield.images_n", { defaultValue: "{{n}} 枚", n: fmtNum(perJob(c.totals), 1) })}
              />
              <BigStat
                label={t("siteYield.compare.hit_rate", "当たり率 (新しい画像が 1 枚でもあったページ)")}
                value={pct(hitRate(c.totals))}
              />
              <BigStat
                label={t("siteYield.compare.pages", "見に行ったページ数")}
                value={`${c.totals?.jobs ?? 0}`}
                sub={t("siteYield.compare.found_images", {
                  defaultValue: "見つけた新しい画像 {{n}} 枚",
                  n: c.totals?.new_images ?? 0,
                })}
              />
            </Stack>
          </Paper>
        ))}
      </SimpleGrid>
    </Paper>
  );
}

// ---------------- 成績の内訳 ----------------

function GradeTiles({ grades, total }: { grades: Record<string, number> | undefined; total: number | undefined }) {
  const { t } = useTranslation();
  const keys = ["good", "ok", "poor", "unknown"];
  return (
    <Paper withBorder p="md">
      <Text size="sm" fw={600} mb="xs">
        {t("siteYield.grades_title", "サイトの成績の内訳")}
        {total !== undefined && (
          <Text span size="xs" c="dimmed">
            {" "}
            {t("siteYield.grades_total", { defaultValue: "— 全 {{n}} サイト", n: total })}
          </Text>
        )}
      </Text>
      <SimpleGrid cols={{ base: 2, md: 4 }}>
        {keys.map((k) => {
          const g = GRADE[k];
          const n = grades?.[k] ?? 0;
          return (
            <Paper key={k} p="sm" radius="md" style={{ background: "var(--mantine-color-default-hover)" }}>
              <Badge color={g.color} variant="light" size="sm" mb={4}>{gradeLabel(t, k)}</Badge>
              <Text size="xl" fw={700} style={{ fontFamily: "ui-monospace, monospace", lineHeight: 1.2 }}>
                {grades ? n : "—"}
              </Text>
              <Text size="xs" c="dimmed">{gradeHint(t, k)}</Text>
            </Paper>
          );
        })}
      </SimpleGrid>
    </Paper>
  );
}

function Explainer() {
  const { t } = useTranslation();
  return (
    <Paper withBorder p="md">
      <Text size="sm" fw={600} mb="xs">{t("siteYield.explain.title", "しくみ (釣りで例えると)")}</Text>
      <Text size="sm" mb="xs">
        {t(
          "siteYield.explain.lead",
          "よく釣れる釣り場には多く通い、釣れない釣り場にも「たまに」様子を見に行く、という選び方です。",
        )}
      </Text>
      <List size="sm" spacing={4}>
        <List.Item>
          <Trans
            i18nKey="siteYield.explain.item1"
            defaults="サイトを見に行くたびに、「新しい画像が 1 枚でもあったか」を <0>当たり / はずれ</0> として記録します。"
            components={[<b key="0" />]}
          />
        </List.Item>
        <List.Item>
          <Trans
            i18nKey="siteYield.explain.item2"
            defaults="当たりが多いサイトほど選ばれやすくなります。古い記録は 2 日で半分に薄まるので、<0>最近の調子</0>が反映されます。"
            components={[<b key="0" />]}
          />
        </List.Item>
        <List.Item>
          {t(
            "siteYield.explain.item3",
            "はずれ続きのサイトも、たまには選ばれます (サイトが復活していたら気づけるように)。",
          )}
        </List.Item>
        <List.Item>
          <Trans
            i18nKey="siteYield.explain.item4"
            defaults="本当に効いているか比べるため、今は半分のサイトを<0>いつもの選び方</0> (順番に回る) で見に行っています。"
            components={[<b key="0" />]}
          />
        </List.Item>
      </List>
    </Paper>
  );
}

// ---------------- ページ ----------------

export default function SiteYield() {
  const { t } = useTranslation();
  const [hours, setHours] = useState("24");
  const [order, setOrder] = useState<string>("mean_asc");
  const [gradeFilter, setGradeFilter] = useState<string>("all");
  const [q, setQ] = useState("");
  const [dq] = useDebouncedValue(q, 300);
  const range = RANGES.find((r) => r.value === hours);
  const hoursLabel = range
    ? t(range.labelKey, range.labelJa)
    : t("siteYield.range_hours", { defaultValue: "{{n}}時間", n: hours });

  // 半減期 / prior は plugin の init_kwargs が権威 (UI の表示計算も同じ値で揃える)
  const submitWlQ = useQuery({
    queryKey: ["workload", SUBMIT_SLUG],
    queryFn: () => api.getWorkload(SUBMIT_SLUG),
    refetchInterval: 60_000,
    retry: false,
  });
  const ik = (submitWlQ.data?.executor_config?.init_kwargs ?? {}) as Record<string, unknown>;
  const halfLife = Number(ik.yield_half_life_h ?? 48) || 48;
  const prior = Number(ik.yield_prior ?? 0.5) || 0.5;
  const tsShare = Number(ik.yield_ts_share ?? 0.5);
  const yieldEnabled = ik.yield_enabled === undefined ? true : Number(ik.yield_enabled) !== 0;

  const sitesQ = useQuery({
    queryKey: ["site-yield-sites", halfLife, prior, order, dq],
    queryFn: () => siteYieldApi.sites({ half_life_h: halfLife, prior, order, q: dq || undefined, limit: 400 }),
    refetchInterval: 30_000,
  });
  const statsQ = useQuery({
    queryKey: ["site-yield-stats", hours],
    queryFn: () => siteYieldApi.stats({ hours: Number(hours) }),
    refetchInterval: 30_000,
  });
  const picksQ = useQuery({
    queryKey: ["site-yield-picks"],
    queryFn: () =>
      pluginStateApi
        .get<YieldPick[]>(SUBMIT_SLUG, "yield_picks")
        .then((r) => (Array.isArray(r.value) ? r.value : []))
        .catch((e: unknown) => {
          if (e instanceof ApiError && e.status === 404) return [] as YieldPick[];
          throw e;
        }),
    refetchInterval: 15_000,
  });

  const statRows = statsQ.data?.rows ?? [];
  const lanes = useMemo(() => {
    const present = new Set(statRows.map((r) => r.lane));
    return LANE_ORDER.filter((l) => present.has(l)).concat(
      [...present].filter((l) => !LANE_ORDER.includes(l)),
    );
  }, [statRows]);
  const xs = useMemo(() => [...new Set(statRows.map((r) => r.ts))].sort(), [statRows]);
  const totals = useMemo(() => sumLanes(statRows), [statRows]);
  const sinceH = hoursSince(statsQ.data?.first_seen?.ts);
  const provisional = sinceH !== null && sinceH < PROVISIONAL_HOURS;

  const mkSeries = (ls: string[], f: (r: YieldStatRow) => number | undefined): LineSeries[] =>
    ls.map((lane) => ({
      key: lane,
      label: laneName(t, lane),
      color: laneColor(lane),
      values: new Map(
        statRows
          .filter((r) => r.lane === lane)
          .map((r) => [r.ts, f(r)] as [string, number | undefined])
          .filter((kv): kv is [string, number] => kv[1] !== undefined),
      ),
    }));
  const mainLanes = ["ts", "rr"].filter((l) => lanes.includes(l));
  const harvestMain = mkSeries(mainLanes, (r) => (r.jobs > 0 ? r.new_images / r.jobs : undefined));
  const harvestAll = mkSeries(lanes, (r) => (r.jobs > 0 ? r.new_images / r.jobs : undefined));
  const prodAll = mkSeries(lanes, (r) => (r.jobs > 0 ? (r.productive / r.jobs) * 100 : undefined));
  const jobsAll = mkSeries(lanes, (r) => r.jobs);

  const summary = sitesQ.data?.summary;
  const sitesAll = sitesQ.data?.sites ?? [];
  const sites = gradeFilter === "all" ? sitesAll : sitesAll.filter((r) => r.grade === gradeFilter);
  const picks = (picksQ.data ?? []).slice(0, 40);
  const bucketMax = Math.max(1, ...(summary?.buckets.map((b) => b.count) ?? [1]));

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("siteYield.title", "サイトの成績表")}
        subtitle={t(
          "siteYield.subtitle",
          "画像がよく見つかるサイトを多めに、見つからないサイトはたまにだけ見に行く仕組みが、ちゃんと効いているかを見るページ",
        )}
        right={
          <Group gap="xs" wrap="nowrap">
            {!yieldEnabled && (
              <Badge color="gray" variant="light" tt="none">
                {t("siteYield.disabled_badge", "新しい選び方: 停止中")}
              </Badge>
            )}
            <SegmentedControl size="xs" data={selectData(t, RANGES)} value={hours} onChange={setHours} />
            <Tooltip label={t("common.refresh", "更新")}>
              <ActionIcon
                variant="subtle"
                onClick={() => {
                  void sitesQ.refetch();
                  void statsQ.refetch();
                  void picksQ.refetch();
                }}
                aria-label="refresh"
              >
                <IconRefresh size={16} />
              </ActionIcon>
            </Tooltip>
          </Group>
        }
      />

      {statsQ.isError ? (
        <ErrorState error={statsQ.error} onRetry={() => statsQ.refetch()} />
      ) : statsQ.data && !statsQ.data.available ? (
        <Paper withBorder p="sm">
          <Text size="sm" c="orange">
            {t("siteYield.no_table", "集計表がまだ作られていません。仕組みが動き出すと自動で作られます。")}
          </Text>
          <Text size="xs" c="dimmed">{statsQ.data.error}</Text>
        </Paper>
      ) : (
        <VerdictCard ts={totals.ts} rr={totals.rr} hoursLabel={hoursLabel} sinceH={sinceH} provisional={provisional} />
      )}

      <SimpleGrid cols={{ base: 1, lg: 2 }}>
        <CompareCard ts={totals.ts} rr={totals.rr} hoursLabel={hoursLabel} />
        <Paper withBorder p="md">
          <Text size="sm" fw={600} mb={4}>
            {t("siteYield.trend_title", "1 ページあたり見つかった新しい画像 (時間の流れ)")}
          </Text>
          <Text size="xs" c="dimmed" mb="xs">
            {t("siteYield.trend_hint", "上にあるほど、同じ手間で多く見つけている")}
          </Text>
          <LaneLineChart
            xs={xs}
            series={harvestMain}
            fmt={(v) => t("siteYield.images_n", { defaultValue: "{{n}} 枚", n: v.toFixed(1) })}
          />
        </Paper>
      </SimpleGrid>

      <SimpleGrid cols={{ base: 1, lg: 2 }}>
        <GradeTiles grades={summary?.grades} total={summary?.total} />
        <Explainer />
      </SimpleGrid>

      {/* サイト一覧 */}
      <Paper withBorder p="md">
        <Group justify="space-between" mb="xs" wrap="wrap">
          <Text size="sm" fw={600}>
            {t("siteYield.sites_title", "サイトごとの成績")}
            {sitesQ.data && (
              <Text span size="xs" c="dimmed">
                {" "}
                {t("siteYield.sites_count", {
                  defaultValue: "— {{shown}} 件を表示 (全 {{total}} サイト、上位 400 件まで)",
                  shown: sites.length,
                  total: sitesQ.data.total,
                })}
              </Text>
            )}
          </Text>
          <Group gap="xs">
            <TextInput
              size="xs"
              placeholder={t("siteYield.search_placeholder", "サイト名で探す")}
              leftSection={<IconSearch size={14} />}
              value={q}
              onChange={(e) => setQ(e.currentTarget.value)}
              w={200}
            />
            <Select size="xs" data={selectData(t, GRADE_FILTER)} value={gradeFilter} onChange={(v) => v && setGradeFilter(v)} w={160} />
            <Select size="xs" data={selectData(t, ORDERS)} value={order} onChange={(v) => v && setOrder(v)} w={200} />
          </Group>
        </Group>
        {sitesQ.isLoading ? (
          <TableSkeleton rows={8} cols={6} />
        ) : sitesQ.isError ? (
          <ErrorState error={sitesQ.error} onRetry={() => sitesQ.refetch()} />
        ) : sitesQ.data && !sitesQ.data.available ? (
          <EmptyState title={t("siteYield.no_records", "成績の記録がまだありません")} description={sitesQ.data.error ?? ""} />
        ) : sites.length === 0 ? (
          <EmptyState title={t("siteYield.no_match", "該当するサイトがありません")} />
        ) : (
          <ScrollArea h={520}>
            <Table fz="sm" striped highlightOnHover stickyHeader>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("siteYield.col_site", "サイト")}</Table.Th>
                  <Table.Th>{t("siteYield.col_grade", "成績")}</Table.Th>
                  <Table.Th>{t("siteYield.col_hit_rate", "最近の当たり率")}</Table.Th>
                  <Table.Th ta="right">{t("siteYield.col_visits", "見に行った回数")}</Table.Th>
                  <Table.Th ta="right">{t("siteYield.col_new_images", "見つけた新しい画像")}</Table.Th>
                  <Table.Th ta="right">{t("siteYield.col_last_seen", "最後に見たのは")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {sites.map((r) => {
                  const g = GRADE[r.grade] ?? GRADE.unknown;
                  return (
                    <Table.Tr key={r.id}>
                      <Table.Td>
                        <Group gap={6} wrap="nowrap">
                          <Text size="sm" style={{ maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.memo ?? undefined}>
                            {r.site}
                          </Text>
                          {r.enabled > 1 && (
                            <Tooltip
                              label={t("siteYield.priority_tip", {
                                defaultValue: "優先指定 ({{n}})",
                                n: r.enabled,
                              })}
                            >
                              <Badge size="xs" variant="light" color="orange">
                                {t("siteYield.priority", "優先")}
                              </Badge>
                            </Tooltip>
                          )}
                        </Group>
                      </Table.Td>
                      <Table.Td><Badge color={g.color} variant="light" size="sm">{gradeLabel(t, r.grade)}</Badge></Table.Td>
                      <Table.Td><HitRateBar row={r} /></Table.Td>
                      <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{r.jobs}</Table.Td>
                      <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{r.new_images}</Table.Td>
                      <Table.Td ta="right">{fmtAge(t, r.age_s)}</Table.Td>
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          </ScrollArea>
        )}
      </Paper>

      {/* 詳しい人向け */}
      <Accordion variant="separated">
        <Accordion.Item value="details">
          <Accordion.Control>
            <Text size="sm" fw={600}>{t("siteYield.advanced", "詳しい人向け (仕組みの中身・生データ)")}</Text>
          </Accordion.Control>
          <Accordion.Panel>
            <Stack gap="md">
              <Paper withBorder p="sm">
                <Text size="sm" fw={600} mb={4}>{t("siteYield.glossary_title", "用語の対応")}</Text>
                <Text size="xs" c="dimmed">
                  {t("siteYield.glossary_body", {
                    defaultValue:
                      "Thompson Sampling (optimistic)。サイト = 腕、job 投入 = プレイ、報酬 = 新規画像 ≥1。事後分布 Beta(S + {{prior}}, F + {{prior}})、S/F は壁時計半減期 {{halfLife}}h で指数減衰。窓の {{share}}% を θ* = max(θ, 平均) の上位で埋める (lane=ts)、残りは従来ローテ (lane=rr)。pr = priority tier (enabled>1) 注入、vg = never-submitted 注入、na = crawl_submit_job に行が無い job (切替前)。「最近の当たり率」= 事後平均、バーの薄い帯 = 近似 80% 区間。成績: good ≥0.6 / ok ≥0.3 / poor <0.3、n<3 は unknown。",
                    prior,
                    halfLife,
                    share: Math.round(tsShare * 100),
                    interpolation: { escapeValue: false },
                  })}
                </Text>
              </Paper>

              <Paper withBorder p="sm">
                <Text size="sm" fw={600} mb={4}>
                  {t("siteYield.lane_table_title", {
                    defaultValue: "lane 別の集計 (この{{range}})",
                    range: hoursLabel,
                  })}
                </Text>
                <Table fz="xs" striped>
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>lane</Table.Th>
                      <Table.Th ta="right">jobs</Table.Th>
                      <Table.Th ta="right">productive</Table.Th>
                      <Table.Th ta="right">{t("siteYield.col_success_rate", "成功率")}</Table.Th>
                      <Table.Th ta="right">{t("siteYield.col_new_images_short", "新規画像")}</Table.Th>
                      <Table.Th ta="right">{t("siteYield.col_new_per_job", "新規/job")}</Table.Th>
                      <Table.Th ta="right">dup</Table.Th>
                      <Table.Th ta="right">{t("siteYield.col_zero_image_jobs", "画像ゼロ job")}</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {lanes.map((l) => {
                      const tt = totals[l];
                      return (
                        <Table.Tr key={l}>
                          <Table.Td>
                            <Group gap={6} wrap="nowrap">
                              <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: laneColor(l) }} />
                              <Text size="xs">{l} — {laneName(t, l)}</Text>
                            </Group>
                          </Table.Td>
                          <Table.Td ta="right">{tt.jobs}</Table.Td>
                          <Table.Td ta="right">{tt.productive}</Table.Td>
                          <Table.Td ta="right">{pct(hitRate(tt))}</Table.Td>
                          <Table.Td ta="right">{tt.new_images}</Table.Td>
                          <Table.Td ta="right">{fmtNum(perJob(tt))}</Table.Td>
                          <Table.Td ta="right">{tt.dup_images}</Table.Td>
                          <Table.Td ta="right">{tt.no_image_jobs}</Table.Td>
                        </Table.Tr>
                      );
                    })}
                  </Table.Tbody>
                </Table>
              </Paper>

              <SimpleGrid cols={{ base: 1, lg: 3 }}>
                <Paper withBorder p="sm">
                  <Text size="sm" fw={600} mb={4}>{t("siteYield.chart_new_per_job", "新規画像 / job (全 lane)")}</Text>
                  <LaneLineChart xs={xs} series={harvestAll} />
                </Paper>
                <Paper withBorder p="sm">
                  <Text size="sm" fw={600} mb={4}>{t("siteYield.chart_success_rate", "成功率 % (新規画像 ≥1 の job 割合)")}</Text>
                  <LaneLineChart xs={xs} series={prodAll} fmt={(v) => `${v.toFixed(0)}%`} />
                </Paper>
                <Paper withBorder p="sm">
                  <Text size="sm" fw={600} mb={4}>{t("siteYield.chart_jobs", "判定した job 数 / bucket")}</Text>
                  <LaneLineChart xs={xs} series={jobsAll} fmt={(v) => v.toFixed(0)} />
                </Paper>
              </SimpleGrid>

              <SimpleGrid cols={{ base: 1, lg: 2 }}>
                <Paper withBorder p="sm">
                  <Text size="sm" fw={600} mb={4}>{t("siteYield.chart_posterior", "事後平均の分布 (観測あり n≥1 のサイト)")}</Text>
                  {summary ? (
                    <Group gap={4} align="flex-end" style={{ height: 120 }} wrap="nowrap">
                      {summary.buckets.map((b) => (
                        <Tooltip key={b.lo} label={t("siteYield.bucket_tip", {
                          defaultValue: "{{lo}}–{{hi}}: {{n}} サイト",
                          lo: b.lo.toFixed(1),
                          hi: b.hi.toFixed(1),
                          n: b.count,
                        })} withArrow>
                          <Stack gap={2} align="center" style={{ flex: 1 }}>
                            <Text size="xs" c="dimmed">{b.count}</Text>
                            <div
                              style={{
                                width: "100%",
                                height: `${Math.max(2, (b.count / bucketMax) * 90)}px`,
                                background: b.lo < 0.3 ? "var(--mantine-color-red-6)" : b.lo < 0.6 ? "var(--mantine-color-blue-5)" : "var(--mantine-color-teal-6)",
                                borderRadius: 3,
                              }}
                            />
                            <Text size="xs" c="dimmed">{b.lo.toFixed(1)}</Text>
                          </Stack>
                        </Tooltip>
                      ))}
                    </Group>
                  ) : (
                    <Text size="xs" c="dimmed">—</Text>
                  )}
                </Paper>

                <Paper withBorder p="sm">
                  <Text size="sm" fw={600} mb={4}>{t("siteYield.picks_title", "直近の TS 選択 (filler が窓に入れたサイト)")}</Text>
                  {picksQ.isError ? (
                    <ErrorState error={picksQ.error} onRetry={() => picksQ.refetch()} />
                  ) : picks.length === 0 ? (
                    <Text size="xs" c="dimmed">{t("siteYield.no_picks", "まだ選択ログがありません")}</Text>
                  ) : (
                    <ScrollArea h={160}>
                      <Table fz="xs" striped highlightOnHover stickyHeader>
                        <Table.Thead>
                          <Table.Tr>
                            <Table.Th>{t("siteYield.col_time_utc", "時刻 (UTC)")}</Table.Th>
                            <Table.Th>{t("siteYield.col_site", "サイト")}</Table.Th>
                            <Table.Th ta="right">θ*</Table.Th>
                            <Table.Th ta="right">{t("siteYield.col_mean", "平均")}</Table.Th>
                            <Table.Th ta="right">S / F</Table.Th>
                            <Table.Th ta="right">jobs</Table.Th>
                          </Table.Tr>
                        </Table.Thead>
                        <Table.Tbody>
                          {picks.map((p, i) => (
                            <Table.Tr key={`${p.t}-${p.site}-${i}`}>
                              <Table.Td style={{ fontFamily: "ui-monospace, monospace" }}>{p.t.slice(11, 19)}</Table.Td>
                              <Table.Td>{p.site}</Table.Td>
                              <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{fmtNum(p.theta, 3)}</Table.Td>
                              <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{fmtNum(p.mean, 3)}</Table.Td>
                              <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{fmtNum(p.s, 1)} / {fmtNum(p.f, 1)}</Table.Td>
                              <Table.Td ta="right" style={{ fontFamily: "ui-monospace, monospace" }}>{p.jobs}</Table.Td>
                            </Table.Tr>
                          ))}
                        </Table.Tbody>
                      </Table>
                    </ScrollArea>
                  )}
                </Paper>
              </SimpleGrid>
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>
    </Stack>
  );
}
