/**
 * Flow Dashboard — プラント風 SCADA 図で全 workload + DB tank + 流量を 1 画面表示。
 *
 * バックエンド `/api/v1/flow/snapshot` から layout + 動的 metric を取得し、
 * React Flow + カスタム SVG ノードでレンダ。 3 秒ごとに自動更新。
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  useNodesState,
  useViewport,
  type Node,
  type Edge,
  type NodeChange,
  type EdgeTypes,
  NodeResizer,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { ActionIcon, Badge, Box, Group, Loader, Modal, Paper, Stack, Text, Tooltip, useMantineColorScheme } from "@mantine/core";
import { IconAdjustmentsHorizontal } from "@tabler/icons-react";
import { motion } from "framer-motion";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import {
  IconAlertTriangle,
  IconAlertTriangleFilled,
  IconArrowsSplit,
  IconBrain,
  IconClockExclamation,
  IconCpu,
  IconDatabase,
  IconDatabaseImport,
  IconLink,
  IconList,
  IconMoodSmile,
  IconPhoto,
  IconScan,
  IconSearch,
  IconSearchOff,
  IconSend,
  IconServer,
  IconServerOff,
  IconUsers,
  IconVideo,
  type Icon,
  type IconProps,
} from "@tabler/icons-react";

import { api, type FlowEdge, type FlowNode, type FlowRateRow, type InfraAlert } from "@/api/client";
import WorkloadControlPopover from "@/components/WorkloadControlPopover";
import { ParticleEdge } from "./FlowEdge";

// WorkloadNode のギアアイコンクリックを Flow ページ全体の modal state に
// 伝えるための context。 WorkloadNode は ReactFlow が描く NodeTypes 内で
// レンダされるので、 props で onClick を直接渡せない。
const FlowControlContext = createContext<{
  openControl: (slug: string) => void;
}>({ openControl: () => {} });

// yaml の icon: 名 → 実 React コンポーネントへの map。
// 未マッピング名は WorkloadNode 内で fallback (= IconScan 等) に。
const ICON_MAP: Record<string, Icon> = {
  search: IconSearch,
  send: IconSend,
  server: IconServer,
  photo: IconPhoto,
  video: IconVideo,
  scan: IconScan,
  brain: IconBrain,
  cpu: IconCpu,
  "arrows-split": IconArrowsSplit,
  faces: IconUsers,
  face: IconMoodSmile,
  link: IconLink,
  list: IconList,
  "database-import": IconDatabaseImport,
  database: IconDatabase,
};

function NodeIcon({ name, ...rest }: { name?: string | null | undefined } & Omit<IconProps, "name">) {
  if (!name) return null;
  const Comp = ICON_MAP[name];
  if (!Comp) return null;
  return <Comp {...rest} />;
}

const STATE_COLOR: Record<string, string> = {
  running: "#22c55e",
  idle: "#94a3b8",
  failed: "#ef4444",
  backoff: "#f59e0b",
};

// state → i18n key 名 (= flow.state_running 等)
const STATE_KEY: Record<string, string> = {
  running: "flow.state_running",
  idle: "flow.state_idle",
  failed: "flow.state_failed",
  backoff: "flow.state_backoff",
};

function fmtNum(v: number | null | undefined): string {
  if (v == null) return "—";
  if (v >= 10_000) return (v / 1_000).toFixed(1) + "k";
  if (Number.isInteger(v)) return String(v);
  return v.toFixed(1);
}

// 各ノードに出す「投入数」= 直近 RATE_WINDOW_MIN 分の実件数合計。flow snapshot は
// レート(件/分)しか持たないので、Throughput ページと同じ /flow/rates(flow_rate_1m の
// 1 分バケット)を足し上げて件数にする。paprika-job-submit は hub 投入レート
// (hub_started_per_min)、他は items_per_min 優先 / runs_per_min フォールバック。
const RATE_WINDOW_MIN = 60;
const PAPRIKA_SLUG = "paprika-job-submit";

// flow_rate_1m は同じ分を tz なし("…15:05:00" = naive UTC)と +00:00 付きの 2 表現で
// 持つため、naive を UTC とみなして epoch-分へ揃え、per-minute で max を取り重複を畳む。
function rateTsToEpochMin(ts: string): number {
  const hasTz = /(?:Z|[+-]\d\d:?\d\d)$/.test(ts);
  const t = new Date(hasTz ? ts : `${ts}Z`).getTime();
  return Number.isNaN(t) ? NaN : Math.floor(t / 60_000);
}

function windowTotalsBySlug(series: FlowRateRow[]): Map<string, number> {
  const bySlug = new Map<
    string,
    Map<number, { items: number; runs: number; hub: number }>
  >();
  for (const r of series) {
    const m = rateTsToEpochMin(r.ts_min);
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
  const nowMin = Math.floor(Date.now() / 60_000);
  const out = new Map<string, number>();
  for (const [slug, mm] of bySlug) {
    const isPaprika = slug === PAPRIKA_SLUG;
    let sum = 0;
    for (const [m, e] of mm) {
      if (m >= nowMin) continue; // 進行中の分は部分値なので除外
      sum += isPaprika ? e.hub : e.items > 0 ? e.items : e.runs;
    }
    out.set(slug, sum);
  }
  return out;
}

// node label を i18n キー (`flow.node.<id>`) で引く、 未定義なら yaml の
// `label` をそのまま表示 (= 後方互換)。
function useNodeLabel(data: { id?: string; label: string }): string {
  const { t } = useTranslation();
  if (!data.id) return data.label;
  return t(`flow.node.${data.id}`, { defaultValue: data.label });
}

function relTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return iso;
  const s = Math.floor((Date.now() - t) / 1000);
  if (s < 5) return "now";
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

// last_output.watermark (= image-pull 等の自己 tick カーソル位置) と現在時刻の差を
// 時間単位で返す。 バックログ処理中の workload はここが実時刻より大きく遅れる
// (= 「何時間前の hub 結果を処理しているか」)。 SQLite 由来の値は
// "2026-08-25 07:36:33.899000+00:00" のようにスペース区切りで来ることがあり、
// 一部ブラウザの Date parser がそのままでは解釈できないため "T" 区切りに正規化する。
function backlogAgeHours(watermark: unknown): number | null {
  if (typeof watermark !== "string" || !watermark) return null;
  const normalized = watermark.includes("T") ? watermark : watermark.replace(" ", "T");
  const t = new Date(normalized).getTime();
  if (!Number.isFinite(t)) return null;
  const h = (Date.now() - t) / 3_600_000;
  return h >= 0 ? h : null;
}

// ---------- カスタムノード ----------

// 接続されていない handle は完全に消す (= dot 残骸を出さない)。
// React Flow は edge の sourceHandle/targetHandle 解決時に DOM 上の Handle を
// 必要とするため、 要素自体は残しつつ visual を 0 にする。
const HANDLE_HIDDEN = {
  width: 1,
  height: 1,
  background: "transparent",
  border: "none",
  boxShadow: "none",
  opacity: 0,
  pointerEvents: "none" as const,
};

// 配管フランジ風 handle (= 使用中のみ visible)。
// 半円形にしてボックス側の弧は clip-path で隠す → 工場の配管「ハーフ
// フランジ」風 (= 外側だけ膨らんだ D 字)。
//   - side で 4 辺の向きを切替: 外側を丸く / 内側 (= ボックス中心側) は直線
//   - clipPath で ボックス内側に被る半分をマスク
// 色:
//   dark: 工業金属 (conic-gradient + 暗い穴 + 控えめ accent ring)
//   light: パステル円 + 白中心
function flangeStyle(
  active: "in" | "out",
  isLight: boolean,
  side: "top" | "right" | "bottom" | "left",
): React.CSSProperties {
  // 外側だけ丸める borderRadius と 内側を切る clipPath を side 別に決定
  const radius =
    side === "top"
      ? "999px 999px 0 0"
      : side === "right"
      ? "0 999px 999px 0"
      : side === "bottom"
      ? "0 0 999px 999px"
      : "999px 0 0 999px";
  const clip =
    side === "top"
      ? "inset(0 0 50% 0)"      // 上半分のみ visible (= ボックス上に膨らむ)
      : side === "right"
      ? "inset(0 0 0 50%)"      // 右半分のみ
      : side === "bottom"
      ? "inset(50% 0 0 0)"
      : "inset(0 50% 0 0)";

  // 配管 = 落ち着いた indigo 系。 accent は source/target で色味だけ変える
  const accent = active === "out" ? "#6366f1" : "#a78bfa";

  if (isLight) {
    return {
      width: 22,
      height: 22,
      borderRadius: radius,
      clipPath: clip,
      background:
        "radial-gradient(circle at 50% 50%, #ffffff 0%, #ffffff 35%, " +
        `${accent}22 38%, ${accent}55 60%, ${accent}aa 100%)`,
      border: "none",
      padding: 0,
      boxShadow: `0 1px 3px ${accent}44`,
    };
  }
  return {
    width: 22,
    height: 22,
    borderRadius: radius,
    clipPath: clip,
    background:
      "conic-gradient(from 135deg, #1f2937 0deg, #64748b 60deg, #cbd5e1 120deg, " +
      "#64748b 180deg, #1f2937 240deg, #475569 300deg, #1f2937 360deg)",
    border: "none",
    padding: 0,
    boxShadow: [
      `inset 0 0 0 5px #0b1220`,
      `inset 0 0 0 6px ${accent}33`,
      `0 1px 2px rgba(0,0,0,0.55)`,
    ].join(", "),
  };
}

// 4 辺に source/target 両方の Handle を置く。 各 Handle に id を振り、
// edge 側は sourceHandle / targetHandle で配線先を選ぶ (= 最短側を選択)。
// active が指定されると、 その集合に含まれる handle だけがフランジ風に visible、
// 他は完全に透明 (= 接続されてない辺に dot が残らない)。
// ボックスの既定サイズ (= 従来の見た目)。 yaml に w/h があればそちらが勝つ。
// height を undefined にしてある kind は「中身なり (auto)」 で、 ユーザが
// リサイズして初めて高さが固定される (= 既定で中身が切れるのを避けるため)。
const NODE_DEFAULT_W: Record<string, number> = { workload: 220, tank: 180, external: 180 };
const NODE_DEFAULT_H: Record<string, number | undefined> = {
  workload: undefined, tank: 110, external: undefined,
};
// これ以上小さくすると文字が潰れて読めなくなる下限。
const NODE_MIN: Record<string, { w: number; h: number }> = {
  workload: { w: 170, h: 96 },
  tank: { w: 130, h: 84 },
  external: { w: 130, h: 64 },
};

// 選択中の box の見た目 (= 「今これを掴んでいる」 が一目で分かる様に色を変える)。
const selectColor = (isLight: boolean) => (isLight ? "#0284c7" : "#38bdf8");
function selectionStyle(isLight: boolean, selected: boolean) {
  if (!selected) return {};
  const c = selectColor(isLight);
  return {
    border: `2px solid ${c}`,
    boxShadow: `0 0 0 4px ${isLight ? "rgba(2,132,199,0.30)" : "rgba(56,189,248,0.35)"}`,
  };
}

// リサイズ枠。 選択中の node にだけ出す (= 常時出すと 34 個 x 8 ハンドルで
// 配線が見えなくなる)。 掴む所は工業計器に合わせて小さい四角。
function BoxResizer({ kind, isLight, visible }: {
  kind: string; isLight: boolean; visible: boolean;
}) {
  const min = NODE_MIN[kind] ?? NODE_MIN.workload;
  const c = selectColor(isLight);
  return (
    <NodeResizer
      isVisible={visible}
      minWidth={min.w}
      minHeight={min.h}
      // 線は透明にして「掴める帯」 に徹させる (実体の枠線は選択色の border が担う)。
      // 太さは下の RESIZE_BAND_CSS で 10px にしてある。
      lineStyle={{ borderColor: "transparent" }}
      // 隅のつまみは「掴める所」 と一目で分かる大きさ・塗りにする (小さい白抜きだと
      // 背景に紛れて 「枠が出ていない」 と見えるため)。
      handleStyle={{
        width: 12, height: 12, borderRadius: 2,
        background: c,
        border: `2px solid ${isLight ? "#ffffff" : "#0f172a"}`,
      }}
    />
  );
}

// 辺を掴んでリサイズするための当たり判定。 React Flow の既定 (style.css) は
//   `.line.right { width: 1px; left: 100%; transform: translate(-50%,0) }`
// で、 **1px 幅で辺の上にまたがって**いる。 そのままだと
//   1. 細すぎて掴めない
//   2. カードが overflow:hidden なので外側半分が切り取られ、 実効 0.5px になる
// ため、 太さを 14px にした上で **transform を消して枠の内側に寄せる**。
// これで 14px 全部がカードの中に入り、 クリップされずに掴める (2026-09-01)。
// カーソル (ew-resize / ns-resize) は React Flow の既定 CSS がそのまま効く。
const RESIZE_BAND = 14;
const RESIZE_BAND_CSS = `
  /* カード内には液面(1) / hazard テープ(4,5) / 本文 Stack(6) が居るので、
     それより前に出さないと帯が本文に隠れて掴めない。 */
  .react-flow__resize-control { z-index: 10; }
  .react-flow__resize-control.line { border: none; }
  .react-flow__resize-control.line.left,
  .react-flow__resize-control.line.right { width: ${RESIZE_BAND}px; transform: none; }
  .react-flow__resize-control.line.left { left: 0; }
  .react-flow__resize-control.line.right { left: auto; right: 0; }
  .react-flow__resize-control.line.top,
  .react-flow__resize-control.line.bottom { height: ${RESIZE_BAND}px; transform: none; }
  .react-flow__resize-control.line.top { top: 0; }
  .react-flow__resize-control.line.bottom { top: auto; bottom: 0; }
  /* 掴める辺だと分かる様に、 マウスが乗ったら帯を光らせる。 */
  .react-flow__resize-control.line:hover { background: rgba(56,189,248,0.30); }
`;

function NodeHandles({ active }: { active?: ReadonlySet<string> }) {
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const sides = ["top", "right", "bottom", "left"] as const;
  return (
    <>
      {sides.map((p) => {
        const id = `s-${p}`;
        const isActive = active?.has(id) ?? false;
        return (
          <Handle
            key={id}
            id={id}
            type="source"
            position={Position[p[0].toUpperCase() + p.slice(1) as "Top" | "Right" | "Bottom" | "Left"]}
            style={isActive ? flangeStyle("out", isLight, p) : HANDLE_HIDDEN}
          />
        );
      })}
      {sides.map((p) => {
        const id = `t-${p}`;
        const isActive = active?.has(id) ?? false;
        return (
          <Handle
            key={id}
            id={id}
            type="target"
            position={Position[p[0].toUpperCase() + p.slice(1) as "Top" | "Right" | "Bottom" | "Left"]}
            style={isActive ? flangeStyle("in", isLight, p) : HANDLE_HIDDEN}
          />
        );
      })}
    </>
  );
}

function WorkloadNode({
  data,
  selected,
}: {
  data: FlowNode & {
    activeHandles?: string[];
    submitted_window?: number;
    submitted_window_min?: number;
  };
  selected?: boolean;
}) {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const ctrl = useContext(FlowControlContext);
  const isLight = colorScheme === "light";
  const color = STATE_COLOR[data.state ?? "idle"];
  const label = t(STATE_KEY[data.state ?? "idle"]);
  const nodeLabel = useNodeLabel(data);
  const adapt = (data.adapt ?? {}) as Record<string, number>;
  const backlogH = backlogAgeHours(data.last_output?.watermark);
  const active = useMemo(() => new Set(data.activeHandles ?? []), [data.activeHandles]);
  // 停滞しているか = **上流タンクが積み上がっているか** を背景色で示す (2026-08-31)。
  //   増加中 (trend > 0) → 薄ピンク (= 食う側が追いついていない = 停滞)
  //   減少中 (trend < 0) → 薄い緑  (= バックログを消化できている)
  //   横ばい / 履歴不足   → 中立 (= 従来の白/紺)
  // 判定値は server 側で「上流 tank の実 SQL count の時系列 (flow_rate_1m の
  // tank_level)」 から出している。 edge の IN/OUT は宣言 metric が欠けると隣の
  // workload の throughput を借りる借り物なので使わない。
  // deadband は絶対値 1 件/分 (= 水位が 30s cache 由来の階段なので、 端数の揺れで
  // 色がチラつかない程度)。
  const trend = data.backlog_trend_per_min;
  const TREND_EPS = 1.0;
  const bias =
    trend == null || Math.abs(trend) < TREND_EPS ? "flat" : trend > 0 ? "piling" : "draining";
  // theme-aware 色: dark = 工業 / light = やさしいパステル白系
  const cardBg = isLight
    ? bias === "piling"
      ? "linear-gradient(135deg, #fff5f7 0%, #ffe4e6 100%)"
      : bias === "draining"
      ? "linear-gradient(135deg, #f4fdf7 0%, #dcfce7 100%)"
      : "linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)"
    : bias === "piling"
      ? "linear-gradient(135deg, #3b2130 0%, #1b0f16 100%)"
      : bias === "draining"
      ? "linear-gradient(135deg, #17352a 0%, #0b1a14 100%)"
      : "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)";
  // 色の根拠 (どのタンクが 何件/分 動いたか) はネイティブ tooltip で出す。 node は
  // ドラッグ対象なので Mantine の Tooltip で包むと掴みにくくなるため title 属性。
  const biasTitle =
    trend == null
      ? t("flow.backlog_trend_none", "上流バックログ: 履歴待ち")
      : `${t("flow.backlog_trend", "上流バックログ")} ${trend > 0 ? "+" : ""}${fmtNum(trend)}/分` +
        ` (${data.backlog_trend_span_min ?? 0}分, ${(data.backlog_tanks ?? []).join(", ")})`;
  const cardText = isLight ? "#1f2937" : "#e2e8f0";
  const cardBorder = isLight ? `1.5px solid ${color}cc` : `2px solid ${color}`;
  // 旧版は running 時に `0 0 18px ${color}66` の強い neon glow が出て
  // 線がボケて目が疲れたので、 ぼかし量を ~1/3 + 不透明度も控えめに。
  // 線自体 (border) はクリアに見える程度のうっすら影だけ残す。
  const cardShadow =
    data.state === "running"
      ? isLight
        ? `0 2px 6px ${color}22`
        : `0 0 4px ${color}33`
      : isLight
      ? "0 1px 4px rgba(15,23,42,0.06)"
      : "none";
  return (
    <motion.div
      initial={{ scale: 0.95, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      title={biasTitle}
      style={{
        // 大きさは node 側 (= yaml の w/h か kind 既定) が決める。 中身はそれに従う。
        width: "100%",
        height: data.h != null ? "100%" : undefined,
        background: cardBg,
        border: cardBorder,
        borderRadius: isLight ? 4 : 2,      // 工業計器風: 角はキリッと
        padding: 10,
        color: cardText,
        boxShadow: cardShadow,
        fontSize: 12,
        position: "relative",
        overflow: "hidden",   // 縮めた時に中身が外へはみ出さない様に
        ...selectionStyle(isLight, !!selected),
      }}
    >
      <NodeHandles active={active} />
      <Group justify="space-between" gap={4} wrap="nowrap">
        <Group gap={6} wrap="nowrap" style={{ minWidth: 0, flex: 1 }}>
          <NodeIcon name={data.icon} size={28} color={color} stroke={1.8} />
          <Text size="sm" fw={700} truncate>
            {nodeLabel}
          </Text>
        </Group>
        <Group gap={2} wrap="nowrap">
          <Badge size="xs" color={data.state === "running" ? "teal" : data.state === "failed" ? "red" : data.state === "backoff" ? "orange" : "gray"}>
            {label}
          </Badge>
          {data.workload_slug && (
            <Tooltip label={t("flow.tune", "流量調整")}>
              <ActionIcon
                size="xs"
                variant="subtle"
                color="gray"
                onMouseDown={(e) => e.stopPropagation()}
                onClick={(e) => {
                  e.stopPropagation();
                  ctrl.openControl(data.workload_slug!);
                }}
                // 選択中はリサイズの帯 (z-index 10) が辺から 14px 内側まで
                // 覆うので、 歯車はそれより前に出しておかないと押せなくなる。
                style={{ cursor: "pointer", position: "relative", zIndex: 11 }}
                aria-label="tune"
              >
                <IconAdjustmentsHorizontal size={12} />
              </ActionIcon>
            </Tooltip>
          )}
        </Group>
      </Group>
      <Stack gap={2} mt={6}>
        <Group gap={4} wrap="nowrap">
          <Text size="xs" c="dimmed">{t("flow.throughput")}</Text>
          <Text size="xs" fw={600} style={{ fontFamily: "ui-monospace, monospace" }}>
            {fmtNum(data.throughput_per_min)}{t("flow.per_min")}
          </Text>
        </Group>
        {data.raw_throughput_per_min != null && (
          <Group gap={4} wrap="nowrap">
            <Tooltip label={t("flow.tip_raw_throughput")}>
              <Text size="xs" c="dimmed">{t("flow.raw_throughput")}</Text>
            </Tooltip>
            <Text size="xs" fw={600} c="dimmed" style={{ fontFamily: "ui-monospace, monospace" }}>
              {fmtNum(data.raw_throughput_per_min)}{t("flow.per_min")}
            </Text>
          </Group>
        )}
        {data.submitted_window_min !== undefined && (
          <Group gap={4} wrap="nowrap">
            <Text size="xs" c="dimmed">{t("flow.submitted", "投入")}</Text>
            <Text size="xs" fw={600} style={{ fontFamily: "ui-monospace, monospace" }}>
              {fmtNum(data.submitted_window)}
            </Text>
            <Text size="10px" c="dimmed">
              ({data.submitted_window_min}
              {t("flow.min_suffix", "分")})
            </Text>
          </Group>
        )}
        <Group gap={4} wrap="nowrap">
          <Text size="xs" c="dimmed">{t("flow.last_tick")}</Text>
          <Text size="xs" style={{ fontFamily: "ui-monospace, monospace" }}>
            {relTime(data.last_run_at)}
          </Text>
        </Group>
        {Object.keys(adapt).length > 0 && (
          <Group gap={6} wrap="wrap">
            {adapt.interval_s !== undefined && (
              <Tooltip label={t("flow.tip_interval")}>
                <Badge size="xs" variant="light" color="indigo">⏱ {adapt.interval_s}s</Badge>
              </Tooltip>
            )}
            {adapt.page_limit !== undefined && (
              <Tooltip label={t("flow.tip_page_limit")}>
                <Badge size="xs" variant="light" color="indigo">📄 {adapt.page_limit}</Badge>
              </Tooltip>
            )}
            {adapt.hard_cap_eff !== undefined && (
              <Tooltip label={t("flow.tip_hard_cap")}>
                <Badge size="xs" variant="light" color="indigo">🧵 {adapt.hard_cap_eff}</Badge>
              </Tooltip>
            )}
          </Group>
        )}
        {backlogH !== null && backlogH >= 0.05 && (
          <Group gap={6} wrap="wrap">
            <Tooltip label={t("flow.tip_backlog_age")}>
              <Badge
                size="xs"
                variant="light"
                color={backlogH >= 6 ? "red" : backlogH >= 2 ? "orange" : "gray"}
              >
                🕰 {backlogH < 1 ? `${Math.round(backlogH * 60)}m` : `${backlogH.toFixed(1)}h`}
                {t("flow.backlog_age_suffix", "遅れ")}
              </Badge>
            </Tooltip>
          </Group>
        )}
      </Stack>
      {data.state === "failed" && <ErrorOverlay />}
      {/* リサイズ枠は **カード内の最後の子** に置く (2026-09-01)。 絶対配置なので
          レイアウトには影響しないが、 先頭に置くと本文が上に重なって辺の帯を
          掴めない (= 掴んだつもりが node のドラッグ移動になる)。 */}
      <BoxResizer kind="workload" isLight={isLight} visible={!!selected} />
    </motion.div>
  );
}

// 水面の波。 SVG path を `<animateTransform translate>` で横スクロール。
//
// シームレスループの正確な要件:
//   - Q-T サイン波は「上 bump + 下 bump」で **200 viewBox unit が 1 完全 cycle**
//     (= 100 unit だけ動かすと up/down が反転して不連続になる)。
//   - したがって translate は -200 単位の倍数で 1 ループ完了させる必要がある。
//   - path は visible range (= viewBox 200..600 = 400 units) + 最大 translate
//     量 を超える長さでカバーする。 ここでは path を 0..1000 (= 10 cycles)
//     にして tx=-200 (or -300) 時の右端空白を防ぐ。
// 水面のリアリティ表現。 SVG の高さを大きく取り (= 30px)、 viewBox も小さく
// (= 0..40) して各要素が目で見えるサイズに。 5 要素 + 上を渡る specular sweep:
//   1. メイン波 (= 大振幅・濃色、 左へ流れる、 3.5s)
//   2. 反射波 (= 半周期ずらし・薄色、 右へ反対方向、 5.5s) → 2 波の交差で見える呼吸感
//   3. 太いハイライト (= 波頂を白く強調、 メインと同期)
//   4. 大きなキラキラ泡 (5 個、 上下バウンド + opacity 点滅、 メインと同期で流れる)
//   5. specular sweep (= 横切る白い光帯、 7s で右から左に通る = 反射光の表現)
// タブが hidden の時 (裏タブで YouTube 視聴中など) は TankWave を非描画にして
// SMIL アニメを完全停止する。 GPU プロセスは全タブ共有なので、 SMIL の paint
// 負荷が YouTube のハードウェアビデオデコード枠を奪う問題を回避。
// (CSS transform アニメは SVG <g> に対して Chromium で視覚適用されない既知制約が
//  あるため SMIL のまま。 代わりに tab visibility ゲートで実効負荷を落とす)
function useTabVisible(): boolean {
  const [visible, setVisible] = useState(() =>
    typeof document !== "undefined" ? document.visibilityState !== "hidden" : true
  );
  useEffect(() => {
    const on = () => setVisible(document.visibilityState !== "hidden");
    document.addEventListener("visibilitychange", on);
    return () => document.removeEventListener("visibilitychange", on);
  }, []);
  return visible;
}

function TankWave({ color, isLight }: { color: string; isLight: boolean }) {
  const tabVisible = useTabVisible();
  // viewBox 0..40 height で振幅 を大きく
  // main: baseline y=20, 振幅 = 16 (peak 4..36)
  const pathMain =
    "M0,20 Q50,4 100,20 T200,20 T300,20 T400,20 T500,20 T600,20 T700,20 T800,20 T900,20 T1000,20 L1000,40 L0,40 Z";
  // back: 半周期ずらし baseline y=26、 振幅 やや控えめ (peak 18..34)
  const pathBack =
    "M50,26 Q100,18 150,26 T250,26 T350,26 T450,26 T550,26 T650,26 T750,26 T850,26 T950,26 T1050,26 L1050,40 L50,40 Z";
  // highlight: メイン波の頂上ライン (= 同位相)
  const pathHL =
    "M0,20 Q50,4 100,20 T200,20 T300,20 T400,20 T500,20 T600,20 T700,20 T800,20 T900,20 T1000,20";
  const hlColor = isLight ? "#ffffff" : "#f0f9ff";
  return (
    <svg
      style={{
        position: "absolute",
        left: "-50%",
        right: "-50%",
        top: -14,         // = SVG height 30 - 水面交差 16 程度
        height: 30,
        width: "200%",
        pointerEvents: "none",
        zIndex: 1,
        overflow: "visible",
      }}
      viewBox="0 0 800 40"
      preserveAspectRatio="none"
    >
      {/* 2. 反射波 (= 薄め、 反対方向) */}
      <g>
        <path d={pathBack} fill={color} opacity={isLight ? 0.35 : 0.5} />
        {tabVisible && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="-200 0; 0 0"
            dur="5.5s"
            repeatCount="indefinite"
          />
        )}
      </g>
      {/* 1. メイン波 (= 濃色) */}
      <g>
        <path d={pathMain} fill={color} opacity={isLight ? 0.72 : 0.9} />
        {tabVisible && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="0 0; -200 0"
            dur="3.5s"
            repeatCount="indefinite"
          />
        )}
      </g>
      {/* 3. ハイライト (= 太めの白線で波頂を強調) */}
      <g>
        <path
          d={pathHL}
          fill="none"
          stroke={hlColor}
          strokeWidth={2.2}
          opacity={isLight ? 0.7 : 0.85}
          strokeLinecap="round"
        />
        {tabVisible && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="0 0; -200 0"
            dur="3.5s"
            repeatCount="indefinite"
          />
        )}
      </g>
      {/* 4. specular sweep (= 反射光が横切るバンド、 7s に 1 回斜めに走る) */}
      <defs>
        <linearGradient id="tankwave-sweep" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor={hlColor} stopOpacity="0" />
          <stop offset="40%" stopColor={hlColor} stopOpacity={isLight ? 0.5 : 0.7} />
          <stop offset="60%" stopColor={hlColor} stopOpacity={isLight ? 0.5 : 0.7} />
          <stop offset="100%" stopColor={hlColor} stopOpacity="0" />
        </linearGradient>
      </defs>
      <g>
        <rect
          x={-160}
          y={6}
          width={120}
          height={20}
          fill="url(#tankwave-sweep)"
          transform="skewX(-25)"
        />
        {tabVisible && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="800 0; -160 0"
            dur="7s"
            repeatCount="indefinite"
          />
        )}
      </g>
    </svg>
  );
}

// Overflow (= fill_ratio >= 1.0) 時の警告 overlay。
// 工事現場の警告テープ風: 4 辺独立した黄黒ハザード stripe が時計回りに流れる
// + 下部に max width の WARNING バー。
//
//   ┌──→──→──→──┐
//   ↑           ↓
//   ↑           ↓
//   └──←──←──←──┘
//
// 実装: framer-motion の repeat=loop は cycle 終端で snap して "ガクッ" するので
// CSS @keyframes ベースに切替。 keyframes は from / to が visual 等価なら seamless。
//
// 縞の visual period 計算 (= seamless loop の鍵):
//   45° gradient の stops 1 周期を `S` に設定すると、 gradient direction の
//   period = S。 X / Y 軸への投影 period = S * √2。
//   → keyframe の to で X (or Y) を `S * √2` 動かせば start と end が visual
//      同一になり snap が消える。
//   ところが S = 20 だと S * √2 = 28.2842712… = 無理数 → browser の
//      sub-pixel 丸めで loop end != loop start となり「ガクッ」が出ていた。
//   解決: stops を半分の `S = 10/√2 ≒ 7.0710678` にすると S * √2 = ちょうど
//      10px、 to で 20px (= 整数) 動かせば完全 seamless。 stripe の visual 太さは
//      45° 投影で 10px に保たれる (= 元の 14.14 視覚太さの半分だが、 line-of-sight
//      で見ると同等)。

const HAZARD_THICK = 8;
const HAZARD_HALF = 7.0710678;            // = 10 / √2
const HAZARD_FULL = 14.1421356;           // = 2 * HAZARD_HALF
const hazardStripe = (c: string) =>
  `repeating-linear-gradient(45deg, ` +
  `${c} 0 ${HAZARD_HALF}px, #0f172a ${HAZARD_HALF}px ${HAZARD_FULL}px)`;
const HAZARD_BG = hazardStripe("#fbbf24");      // 黄黒: tank overflow 警告
const HAZARD_RED_BG = hazardStripe("#ef4444");  // 赤黒: workload エラー
// X / Y 軸 1 周期 = HAZARD_FULL * √2 = ちょうど 20px (= 整数)。
const HAZARD_X_PERIOD = "20";

// label を省くと 4 辺のトラテープだけを描く (= バー無しの枠として使える)。
function HazardOverlay({ bg, barBg, barText, label, barBelow = false }: {
  bg: string; barBg?: string; barText?: string; label?: string; barBelow?: boolean;
}) {
  return (
    <>
      <style>{`
        @keyframes hazardFlowRight {
          from { background-position: 0 0; }
          to   { background-position: ${HAZARD_X_PERIOD}px 0; }
        }
        @keyframes hazardFlowLeft {
          from { background-position: 0 0; }
          to   { background-position: -${HAZARD_X_PERIOD}px 0; }
        }
        @keyframes hazardFlowDown {
          from { background-position: 0 0; }
          to   { background-position: 0 ${HAZARD_X_PERIOD}px; }
        }
        @keyframes hazardFlowUp {
          from { background-position: 0 0; }
          to   { background-position: 0 -${HAZARD_X_PERIOD}px; }
        }
      `}</style>
      {/* TOP: 左 → 右 */}
      <div
        style={{
          position: "absolute",
          top: 0, left: 0, right: 0, height: HAZARD_THICK,
          background: bg,
          animation: "hazardFlowRight 1.4s linear infinite",
          pointerEvents: "none", zIndex: 4,
        }}
      />
      {/* RIGHT: 上 → 下 */}
      <div
        style={{
          position: "absolute",
          top: 0, right: 0, bottom: 0, width: HAZARD_THICK,
          background: bg,
          animation: "hazardFlowDown 1.4s linear infinite",
          pointerEvents: "none", zIndex: 4,
        }}
      />
      {/* BOTTOM: 右 → 左 */}
      <div
        style={{
          position: "absolute",
          bottom: 0, left: 0, right: 0, height: HAZARD_THICK,
          background: bg,
          animation: "hazardFlowLeft 1.4s linear infinite",
          pointerEvents: "none", zIndex: 4,
        }}
      />
      {/* LEFT: 下 → 上 */}
      <div
        style={{
          position: "absolute",
          top: 0, left: 0, bottom: 0, width: HAZARD_THICK,
          background: bg,
          animation: "hazardFlowUp 1.4s linear infinite",
          pointerEvents: "none", zIndex: 4,
        }}
      />
      {/* ラベルバー。 barBelow=true は box の真下・box 全幅 (= ERROR 表示)、
          false は box 内下部に inset (= 既存の WARNING 表示)。
          label 省略時は描かない (= テープだけの枠)。 */}
      {label && (
        <div
          style={{
            position: "absolute",
            ...(barBelow
              ? { top: "100%", left: 0, right: 0 }
              : { left: HAZARD_THICK, right: HAZARD_THICK, bottom: HAZARD_THICK }),
            background: barBg,
            color: barText,
            padding: "3px 6px",
            fontSize: 14,
            fontFamily: "ui-monospace, monospace",
            fontWeight: 900,
            letterSpacing: "0.18em",
            textAlign: "center",
            zIndex: 5,
            pointerEvents: "none",
            whiteSpace: "nowrap",
            overflow: "hidden",
          }}
        >
          {label}
        </div>
      )}
    </>
  );
}

// 黄黒テープ: tank overflow (fill_ratio >= 1.0) 警告。
function OverflowOverlay() {
  return <HazardOverlay bg={HAZARD_BG} barBg="#fbbf24" barText="#0f172a" label="⚠ WARNING" barBelow />;
}

// 赤黒テープ: workload エラー (state === "failed") 表示。
function ErrorOverlay() {
  return <HazardOverlay bg={HAZARD_RED_BG} barBg="#ef4444" barText="#ffffff" label="✕ ERROR" barBelow />;
}

function TankNode({ data, selected }: { data: FlowNode & { activeHandles?: string[];
                                                          inflow_per_min?: number;
                                                          outflow_per_min?: number };
                                       selected?: boolean }) {
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const nodeLabel = useNodeLabel(data);
  const ratio = data.fill_ratio ?? 0;
  const overflow = ratio >= 1.0;
  // 工業計器パレットに寄せた落ち着いた色 (= 旧 #ef4444 は派手すぎ・赤は不使用)。
  // overflow: 警告色を hazard yellow (#fbbf24) で強調 (= hazard tape と一致)。
  // それ以下は ratio に応じて 青(冷静) → 黄(警告) を滑らかに補間する
  // (= 旧版は ratio>0.7 で段階切替の hard cutoff だったため、 急に色が変わって
  //   違和感があった。 連続補間で fill が増えるほど自然に色が "温まる" 見た目に)。
  const fillColor = (() => {
    if (overflow) return "#fbbf24";   // hazard yellow solid
    const blueRGB = isLight ? [96, 165, 250] : [59, 130, 246]; // #60a5fa / #3b82f6
    const hazardRGB = [251, 191, 36];                          // #fbbf24
    // warm 度: ratio 0.30 以下 = 0、 ratio 0.95 以上 = 1。 中間は smoothstep。
    const t = Math.min(1, Math.max(0, (ratio - 0.30) / 0.65));
    const e = t * t * (3 - 2 * t);                             // ease-in-out
    const lerp = (a: number, b: number) => Math.round(a + (b - a) * e);
    const r = lerp(blueRGB[0], hazardRGB[0]);
    const g = lerp(blueRGB[1], hazardRGB[1]);
    const b = lerp(blueRGB[2], hazardRGB[2]);
    // hex 形式で返す (= 後続の `${fillColor}88` 等の alpha suffix が壊れない様に)
    const hex = (n: number) => n.toString(16).padStart(2, "0");
    return `#${hex(r)}${hex(g)}${hex(b)}`;
  })();
  const active = useMemo(() => new Set(data.activeHandles ?? []), [data.activeHandles]);
  const cardBg = isLight
    ? "linear-gradient(180deg, #ffffff 0%, #f1f5f9 100%)"
    : "linear-gradient(180deg, #1e293b 0%, #0f172a 100%)";
  // border は warn/overflow でも slate ベースを保ち、 警告は液面色 + 上部 stripe
  // で表現 (= フチを真っ赤 / 真っ黄に塗り潰す ダサい表現を回避)。
  const cardBorder = isLight
    ? "1.5px solid #cbd5e1"
    : "2px solid #475569";
  const cardText = isLight ? "#1f2937" : "#e2e8f0";
  return (
    <motion.div
      initial={{ scale: 0.95, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      style={{
        width: "100%",
        height: "100%",
        background: cardBg,
        border: cardBorder,
        borderRadius: isLight ? 4 : 2,
        padding: 8,
        color: cardText,
        position: "relative",
        overflow: "hidden",
        boxShadow: isLight ? "0 1px 4px rgba(15,23,42,0.06)" : "none",
        ...selectionStyle(isLight, !!selected),
      }}
    >
      <NodeHandles active={active} />
      {/* タンクの液面 (= 水面に波アニメ。 borderTop は使わず TankWave で代用)。
          overflow 時は薄いグラデではなく hazard tape と同じ濃い黄 solid に統一
          (= 内外の警告色トーンを揃えて視覚的に一段強く)。
          非 overflow 時は alpha 強めにして「液体の色」 がはっきり見える様に
          (= 旧版 22-44 だと薄すぎてほぼ透明、 水中色が分からなかった)。 */}
      <motion.div
        animate={{ height: `${Math.min(100, ratio * 100)}%` }}
        transition={{ duration: 0.8, ease: "easeOut" }}
        style={{
          position: "absolute",
          left: 0,
          right: 0,
          bottom: 0,
          background: overflow
            ? fillColor   // = #fbbf24 solid (= hazard tape 黄と統一)
            : isLight
              ? `linear-gradient(180deg, ${fillColor}88 0%, ${fillColor}66 100%)`
              : `linear-gradient(180deg, ${fillColor}cc 0%, ${fillColor}99 100%)`,
          overflow: "visible",   // 波が縁を僅かに越えるため
        }}
      >
        {ratio > 0.01 && <TankWave color={fillColor} isLight={isLight} />}
      </motion.div>
      {overflow && <OverflowOverlay />}
      <Stack gap={2} style={{ position: "relative", zIndex: 6 }}>
        <Group gap={6} wrap="nowrap">
          <NodeIcon name={data.icon} size={22} color={isLight ? "#475569" : "#94a3b8"} stroke={1.8} />
          <Text size="xs" fw={700} truncate>
            {nodeLabel}
          </Text>
        </Group>
        <Text size="lg" fw={800} style={{ fontFamily: "ui-monospace, monospace", lineHeight: 1 }}>
          {fmtNum(data.pending)}{data.unit ? ` ${data.unit}` : ""}
        </Text>
        {data.capacity_warn != null && (
          <Text size="xs" c="dimmed">
            / {fmtNum(data.capacity_warn)}{data.unit ? ` ${data.unit}` : ""}
          </Text>
        )}
        {/* 1 分間の流入/流出量 (= edges の rate_per_min を集計したもの)。
            IN/OUT ラベルは黒(地味)、 数値は意味色 (緑=in, 橙=out)。
            /min 単位は OUT 側だけに付ける (= IN は同一行なので共通単位と分かる)。 */}
        <Group gap={8} wrap="nowrap" mt={2} style={{ lineHeight: 1 }}>
          <Tooltip label="1 分間の流入量">
            <Group gap={3} wrap="nowrap">
              <Text size="xs" fw={600} c={isLight ? "#475569" : "#cbd5e1"}
                    style={{ lineHeight: 1 }}>
                IN
              </Text>
              <Text size="xs" fw={700} c={isLight ? "#15803d" : "#86efac"}
                    style={{ lineHeight: 1 }}>
                {fmtNum(data.inflow_per_min ?? 0)}
              </Text>
            </Group>
          </Tooltip>
          <Tooltip label="1 分間の流出量">
            <Group gap={3} wrap="nowrap">
              <Text size="xs" fw={600} c={isLight ? "#475569" : "#cbd5e1"}
                    style={{ lineHeight: 1 }}>
                OUT
              </Text>
              <Text size="xs" fw={700} c={isLight ? "#b45309" : "#fcd34d"}
                    style={{ lineHeight: 1 }}>
                {fmtNum(data.outflow_per_min ?? 0)}
              </Text>
              <Text size="xs" c={isLight ? "#475569" : "#cbd5e1"}
                    style={{ lineHeight: 1 }}>
                /min
              </Text>
            </Group>
          </Tooltip>
        </Group>
        {data.error && (
          <Text size="xs" c="red.5" truncate>
            ! {data.error}
          </Text>
        )}
      </Stack>
      <BoxResizer kind="tank" isLight={isLight} visible={!!selected} />
    </motion.div>
  );
}

function ExternalNode({ data, selected }: { data: FlowNode & { activeHandles?: string[] };
                                           selected?: boolean }) {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const nodeLabel = useNodeLabel(data);
  const active = useMemo(() => new Set(data.activeHandles ?? []), [data.activeHandles]);
  return (
    <Paper
      p={10}
      style={{
        width: "100%",
        height: data.h != null ? "100%" : undefined,
        background: isLight
          ? "linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)"
          : "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)",
        border: isLight ? "1.5px dashed #94a3b8" : "2px dashed #64748b",
        borderRadius: isLight ? 4 : 2,
        color: isLight ? "#1f2937" : "#e2e8f0",
        position: "relative",
        overflow: "hidden",
        boxShadow: isLight ? "0 1px 4px rgba(15,23,42,0.06)" : "none",
        ...selectionStyle(isLight, !!selected),
      }}
    >
      <NodeHandles active={active} />
      <Stack gap={4}>
        <Text size="xs" c="dimmed">{t("flow.external")}</Text>
        <Group gap={6} wrap="nowrap">
          <NodeIcon name={data.icon} size={26} color={isLight ? "#0891b2" : "#22d3ee"} stroke={1.8} />
          <Text size="sm" fw={700}>
            {nodeLabel}
          </Text>
        </Group>
        {data.url && (
          <Text size="xs" c="dimmed" truncate>
            {data.url}
          </Text>
        )}
      </Stack>
      <BoxResizer kind="external" isLight={isLight} visible={!!selected} />
    </Paper>
  );
}

const NODE_TYPES = {
  workload: WorkloadNode as never,
  tank: TankNode as never,
  external: ExternalNode as never,
};

// ---------- Annotation layer ----------

type AnnoKind = 'text' | 'line' | 'rect' | 'erase';

interface Anno {
  id: string;
  kind: 'text' | 'line' | 'rect';
  color: string;
  fontSize: number;
  x: number;
  y: number;
  x2: number;
  y2: number;
  text: string;
}

const ANNO_LS_KEY = 'pipeline-flow-annos-v1';

function AnnotationCanvas({
  annos,
  activeTool,
  annoColor,
  annoFontSize,
  rfBoxRef,
  onAdd,
  onDelete,
  onTextPlace,
}: {
  annos: Anno[];
  activeTool: AnnoKind | null;
  annoColor: string;
  annoFontSize: number;
  rfBoxRef: { current: HTMLDivElement | null };
  onAdd: (a: Omit<Anno, 'id'>) => void;
  onDelete: (id: string) => void;
  /** テキストツールでクリックされた座標を Flow へ通知 (Box相対スクリーン座標 + Flow座標) */
  onTextPlace: (sx: number, sy: number, fx: number, fy: number) => void;
}) {
  const vp = useViewport();
  const [draw, setDraw] = useState<{ x1: number; y1: number; x2: number; y2: number } | null>(null);

  useEffect(() => { setDraw(null); }, [activeTool]);

  const toFlow = useCallback((clientX: number, clientY: number) => {
    const el = rfBoxRef.current;
    if (!el) return { x: 0, y: 0 };
    const r = el.getBoundingClientRect();
    return {
      x: (clientX - r.left - vp.x) / vp.zoom,
      y: (clientY - r.top - vp.y) / vp.zoom,
    };
  }, [rfBoxRef, vp]);

  const sw = Math.max(0.5, 2 / vp.zoom);
  const dashArr = `${Math.max(2, 4 / vp.zoom)}`;
  const isActive = !!activeTool;

  const handleSvgDown = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!activeTool) return;
    e.stopPropagation();
    const p = toFlow(e.clientX, e.clientY);
    if (activeTool === 'text') {
      const r = rfBoxRef.current?.getBoundingClientRect();
      const sx = e.clientX - (r?.left ?? 0);
      const sy = e.clientY - (r?.top ?? 0);
      onTextPlace(sx, sy, p.x, p.y);
    } else if (activeTool !== 'erase') {
      setDraw({ x1: p.x, y1: p.y, x2: p.x, y2: p.y });
    }
  };

  const handleSvgMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!draw) return;
    const p = toFlow(e.clientX, e.clientY);
    setDraw((d) => d ? { ...d, x2: p.x, y2: p.y } : null);
  };

  const handleSvgUp = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!draw || !activeTool || activeTool === 'text' || activeTool === 'erase') return;
    const p = toFlow(e.clientX, e.clientY);
    const d = { ...draw, x2: p.x, y2: p.y };
    const minSize = 3 / vp.zoom;
    if (Math.abs(d.x2 - d.x1) > minSize || Math.abs(d.y2 - d.y1) > minSize) {
      onAdd({ kind: activeTool as 'line' | 'rect', x: d.x1, y: d.y1, x2: d.x2, y2: d.y2, text: '', color: annoColor, fontSize: annoFontSize });
    }
    setDraw(null);
  };

  const handleAnnoClick = (id: string) => { if (activeTool === 'erase') onDelete(id); };

  return (
    <svg
      style={{
        position: 'absolute',
        top: 0, left: 0, width: '100%', height: '100%',
        zIndex: isActive ? 1000 : 1,
        pointerEvents: isActive ? 'all' : 'none',
        cursor: activeTool === 'erase' ? 'not-allowed' : isActive ? 'crosshair' : 'default',
        overflow: 'visible',
      }}
      onMouseDown={handleSvgDown}
      onMouseMove={handleSvgMove}
      onMouseUp={handleSvgUp}
    >
      <g transform={`translate(${vp.x},${vp.y}) scale(${vp.zoom})`}>
        {annos.map((a) => {
          const clickHandler = (e: React.MouseEvent) => {
            e.stopPropagation();
            handleAnnoClick(a.id);
          };
          if (a.kind === 'text') {
            return (
              <text
                key={a.id}
                x={a.x} y={a.y}
                fill={a.color}
                fontSize={a.fontSize / vp.zoom}
                style={{ userSelect: 'none', pointerEvents: 'all', cursor: activeTool === 'erase' ? 'not-allowed' : 'default' }}
                onClick={clickHandler}
              >
                {a.text}
              </text>
            );
          }
          if (a.kind === 'line') {
            return (
              <line
                key={a.id}
                x1={a.x} y1={a.y} x2={a.x2} y2={a.y2}
                stroke={a.color} strokeWidth={sw} strokeLinecap="round"
                style={{ pointerEvents: 'all', cursor: activeTool === 'erase' ? 'not-allowed' : 'default' }}
                onClick={clickHandler}
              />
            );
          }
          const rx = Math.min(a.x, a.x2), ry = Math.min(a.y, a.y2);
          return (
            <rect
              key={a.id}
              x={rx} y={ry} width={Math.abs(a.x2 - a.x)} height={Math.abs(a.y2 - a.y)}
              stroke={a.color} strokeWidth={sw} fill={`${a.color}22`}
              style={{ pointerEvents: 'all', cursor: activeTool === 'erase' ? 'not-allowed' : 'default' }}
              onClick={clickHandler}
            />
          );
        })}
        {/* Drawing preview */}
        {draw && activeTool === 'line' && (
          <line x1={draw.x1} y1={draw.y1} x2={draw.x2} y2={draw.y2}
            stroke={annoColor} strokeWidth={sw} strokeDasharray={dashArr}
            strokeLinecap="round" pointerEvents="none" />
        )}
        {draw && activeTool === 'rect' && (() => {
          const rx = Math.min(draw.x1, draw.x2), ry = Math.min(draw.y1, draw.y2);
          return (
            <rect x={rx} y={ry} width={Math.abs(draw.x2 - draw.x1)} height={Math.abs(draw.y2 - draw.y1)}
              stroke={annoColor} strokeWidth={sw} strokeDasharray={dashArr}
              fill={`${annoColor}11`} pointerEvents="none" />
          );
        })()}
      </g>
    </svg>
  );
}

function AnnotationToolbar({
  activeTool,
  setActiveTool,
  annoColor,
  setAnnoColor,
  annoFontSize,
  setAnnoFontSize,
  onClear,
  isLight,
}: {
  activeTool: AnnoKind | null;
  setActiveTool: (t: AnnoKind | null) => void;
  annoColor: string;
  setAnnoColor: (c: string) => void;
  annoFontSize: number;
  setAnnoFontSize: (n: number) => void;
  onClear: () => void;
  isLight: boolean;
}) {
  const bg = isLight ? '#ffffff' : '#1e293b';
  const border = `1px solid ${isLight ? '#e2e8f0' : '#334155'}`;
  const textC = isLight ? '#475569' : '#94a3b8';
  const divider = <div style={{ width: 1, height: 18, background: isLight ? '#e2e8f0' : '#334155', flexShrink: 0 }} />;

  const toolBtn = (tool: AnnoKind | null, label: string, tip: string) => (
    <Tooltip label={tip} key={String(tool)}>
      <button
        onClick={() => setActiveTool(activeTool === tool ? null : tool)}
        style={{
          background: activeTool === tool ? '#3b82f6' : 'transparent',
          color: activeTool === tool ? '#fff' : textC,
          border: 'none',
          borderRadius: 4,
          padding: '3px 8px',
          cursor: 'pointer',
          fontSize: 13,
          fontWeight: 700,
          lineHeight: 1,
          whiteSpace: 'nowrap',
        }}
      >
        {label}
      </button>
    </Tooltip>
  );

  return (
    <div
      style={{
        position: 'absolute',
        bottom: 15,
        left: 52,
        zIndex: 10,
        background: bg,
        border,
        borderRadius: 6,
        padding: '5px 8px',
        display: 'flex',
        alignItems: 'center',
        gap: 5,
        boxShadow: '0 2px 8px rgba(0,0,0,0.18)',
      }}
    >
      <Text size="xs" c="dimmed" fw={600} style={{ marginRight: 2, whiteSpace: 'nowrap' }}>加筆</Text>
      {divider}
      {toolBtn('text', 'T', 'テキスト追加')}
      {toolBtn('line', '╱', '線を引く')}
      {toolBtn('rect', '□', '四角を描く')}
      {toolBtn('erase', '✕', '加筆を消す（クリックで削除）')}
      {divider}
      <Tooltip label="色">
        <input
          type="color"
          value={annoColor}
          onChange={(e) => setAnnoColor(e.target.value)}
          style={{ width: 22, height: 22, border: 'none', background: 'none', cursor: 'pointer', padding: 0, flexShrink: 0 }}
        />
      </Tooltip>
      {activeTool === 'text' && (
        <select
          value={annoFontSize}
          onChange={(e) => setAnnoFontSize(Number(e.target.value))}
          style={{ fontSize: 11, background: bg, color: textC, border, borderRadius: 3, padding: '1px 3px' }}
        >
          {[10, 12, 14, 16, 20, 24, 32, 48].map((s) => (
            <option key={s} value={s}>{s}px</option>
          ))}
        </select>
      )}
      {divider}
      <Tooltip label="全消去">
        <button
          onClick={onClear}
          style={{ background: 'transparent', color: '#ef4444', border: 'none', cursor: 'pointer', fontSize: 12, padding: '2px 5px', borderRadius: 4, whiteSpace: 'nowrap' }}
        >
          全消去
        </button>
      </Tooltip>
    </div>
  );
}

const EDGE_TYPES: EdgeTypes = {
  particle: ParticleEdge,
};

// ---------- Annotation drag handles ----------
// 各アノテーションの左上に移動ハンドルを表示。useViewport() が必要なため
// ReactFlow の子要素としてレンダするが、HTML div で実装（SVG の pointer-events
// より CSS pointer-events が優先されるブラウザ挙動を回避するため）。

function DragHandles({
  annos,
  onMove,
}: {
  annos: Anno[];
  onMove: (id: string, x: number, y: number, x2: number, y2: number) => void;
}) {
  const vp = useViewport();
  // 最新の zoom を ref で保持 (useEffect の stale closure 対策)
  const vpRef = useRef(vp);
  useEffect(() => { vpRef.current = vp; });

  const [drag, setDrag] = useState<{
    id: string;
    startCx: number; startCy: number;
    origX: number; origY: number; origX2: number; origY2: number;
  } | null>(null);

  useEffect(() => {
    if (!drag) return;
    const handleMove = (e: MouseEvent) => {
      const zoom = vpRef.current.zoom;
      const dx = (e.clientX - drag.startCx) / zoom;
      const dy = (e.clientY - drag.startCy) / zoom;
      onMove(drag.id, drag.origX + dx, drag.origY + dy, drag.origX2 + dx, drag.origY2 + dy);
    };
    const handleUp = () => setDrag(null);
    window.addEventListener('mousemove', handleMove);
    window.addEventListener('mouseup', handleUp);
    return () => {
      window.removeEventListener('mousemove', handleMove);
      window.removeEventListener('mouseup', handleUp);
    };
  }, [drag, onMove]);

  const SZ = 12; // ハンドルサイズ (px)

  return (
    <>
      {annos.map((a) => {
        // スクリーン座標への変換 (ReactFlow container 相対)
        let hx: number, hy: number;
        if (a.kind === 'text') {
          // テキスト: ベースラインが (x,y)。フォントサイズ分上がハンドル位置
          hx = a.x * vp.zoom + vp.x - SZ / 2;
          hy = a.y * vp.zoom + vp.y - a.fontSize - SZ / 2;
        } else {
          // line/rect: 左上コーナー
          hx = Math.min(a.x, a.x2) * vp.zoom + vp.x - SZ / 2;
          hy = Math.min(a.y, a.y2) * vp.zoom + vp.y - SZ / 2;
        }
        return (
          <div
            key={a.id}
            title="ドラッグで移動"
            onMouseDown={(e) => {
              e.stopPropagation();
              e.preventDefault();
              setDrag({
                id: a.id,
                startCx: e.clientX, startCy: e.clientY,
                origX: a.x, origY: a.y,
                origX2: a.x2, origY2: a.y2,
              });
            }}
            style={{
              position: 'absolute',
              left: hx,
              top: hy,
              width: SZ,
              height: SZ,
              background: drag?.id === a.id ? '#2563eb' : '#3b82f6',
              border: '1.5px solid rgba(255,255,255,0.85)',
              borderRadius: 2,
              cursor: 'move',
              zIndex: drag?.id === a.id ? 1100 : 50,
              pointerEvents: 'all',
              userSelect: 'none',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: 7,
              color: 'rgba(255,255,255,0.9)',
              lineHeight: 1,
              boxShadow: '0 1px 3px rgba(0,0,0,0.4)',
            }}
          >
            ⠿
          </div>
        );
      })}
    </>
  );
}

// ---------- GPU 緊急エラー アラート ----------
// フロー左上に常時固定の赤ボックス。 workload node の最新 run エラー(flow.py が
// error/error_worker として載せる)を GPU 故障シグネチャで走査し、 該当があれば表示。
// pan/zoom に依らず常に見える位置 (= 見落とし防止)。 DB 追加負荷なし(snapshot 再利用)。
const GPU_ERR_RE =
  /cudaGetDeviceCount|Error 80\d|MPS .*(daemon|server)|GPU (is |has )?(lost|fallen)|fell off the bus|No devices? (were )?found|Unable to determine the device handle|CUBLAS|no CUDA-capable device|CUDA error|Xid/i;

// supervisor の hang watchdog(silent-hang / hang-run)が stuck worker を
// fail/restart した時のマーカー。flow.py が node.error に載せる → 赤ボックスに集約。
const HANG_ERR_RE = /supervisor:(silent-hang|hang-run)|silent[- ]?hang|hang-run/i;

function GpuAlertBox({
  nodes,
  infraAlerts = [],
}: {
  nodes: FlowNode[];
  infraAlerts?: InfraAlert[];
}) {
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const gpu = useMemo(
    () =>
      nodes.filter(
        (n) => n.kind === "workload" && !!n.error && GPU_ERR_RE.test(n.error),
      ),
    [nodes],
  );
  const hang = useMemo(
    () =>
      nodes.filter(
        (n) =>
          n.kind === "workload" && !!n.error && HANG_ERR_RE.test(n.error) &&
          !GPU_ERR_RE.test(n.error),
      ),
    [nodes],
  );
  const total = gpu.length + hang.length + infraAlerts.length;
  if (total === 0) return null;
  // 見た目は tank の ERROR ボックスに揃える (2026-08-31): 赤黒トラテープが
  // 四辺を流れる工業計器の体裁。 旧版の 「赤地 + 赤い光が脈打つ枠」 は
  // 他の警告表現 (hazard tape) と語彙が揃っておらず浮いていた。
  // テープの下に文字が潜らない様、 中身は HAZARD_THICK ぶん内側に置く。
  return (
    <div
      style={{
        position: "absolute",
        top: 12,
        left: 12,
        zIndex: 20,
        maxWidth: 400,
        // light は canvas が #f6f7fb なので、 地を暗いままにすると 1 枚だけ
        // 夜のパネルが浮く。 node と同じ配色に揃える (トラテープ自体は
        // tank の ERROR と同じく両テーマ共通 = 赤黒のままで良い)。
        background: isLight ? "#ffffff" : "#0f172a",
        borderRadius: isLight ? 4 : 2,   // 工業計器風: 角はキリッと (node と同じ)
        color: isLight ? "#1f2937" : "#ffffff",
        padding: HAZARD_THICK + 4,
        overflow: "hidden",       // テープが角で溢れない様に
        boxShadow: isLight
          ? "0 2px 10px rgba(15,23,42,0.18)"
          : "0 4px 18px rgba(0,0,0,0.55)",
      }}
    >
      <HazardOverlay bg={HAZARD_RED_BG} />
      <div
        style={{
          position: "relative",
          zIndex: 6,              // テープ (zIndex 4) より前に出す
          background: "#ef4444",
          color: "#ffffff",
          padding: "3px 6px",
          fontSize: 14,
          fontFamily: "ui-monospace, monospace",
          fontWeight: 900,
          letterSpacing: "0.18em",   // = tank の `✕ ERROR` バーと同じ字送り
          textAlign: "center",
          whiteSpace: "nowrap",
        }}
      >
        ✕ ALERT ({total})
      </div>
      <div style={{
        position: "relative",
        zIndex: 6,
        marginTop: 6,
        display: "flex",
        flexDirection: "column",
        gap: 6,
      }}>
        {infraAlerts.map((a) => {
          const full = a.kind === "thinpool" || a.kind === "storage_full";
          // 逼迫系だけ warn/crit で三角の塗りを分ける。 他は kind そのものを表す絵柄に。
          const Ico = full
            ? a.severity === "warn"
              ? IconAlertTriangle
              : IconAlertTriangleFilled
            : a.kind === "faiss"
              ? IconSearchOff
              : a.kind === "gpu"
                ? IconCpu
                : IconServerOff;
          const head = full
            ? `ストレージ逼迫 · ${a.name}`
            : a.kind === "faiss"
              ? `顔検索 (FAISS) · ${a.name}`
              : a.kind === "gpu"
                ? `GPU センサー無応答 · ${a.name}`
                : `ストレージ停止 · ${a.name}`;
          return (
            <div key={`infra-${a.name}`} style={{ fontSize: 12, lineHeight: 1.35 }}>
              <div style={{ fontWeight: 700, display: "flex", alignItems: "center", gap: 5 }}>
                <Ico size={14} style={{ flexShrink: 0 }} />
                <span>
                  {head}
                  {!full && a.endpoint ? ` (${a.endpoint})` : ""}
                </span>
              </div>
              <div
                style={{
                  opacity: 0.85,
                  fontFamily: "ui-monospace, monospace",
                  fontSize: 11,
                  wordBreak: "break-word",
                }}
              >
                {String(a.detail ?? a.error ?? "到達できません").slice(0, 160)}
              </div>
            </div>
          );
        })}
        {gpu.map((a) => (
          <div key={a.id} style={{ fontSize: 12, lineHeight: 1.35 }}>
            <div style={{ fontWeight: 700, display: "flex", alignItems: "center", gap: 5 }}>
              <IconCpu size={14} style={{ flexShrink: 0 }} />
              <span>
                GPU · {a.label}
                {a.error_worker ? ` · ${a.error_worker}` : ""}
              </span>
            </div>
            <div
              style={{
                opacity: 0.85,
                fontFamily: "ui-monospace, monospace",
                fontSize: 11,
                wordBreak: "break-word",
              }}
            >
              {String(a.error).slice(0, 160)}
            </div>
          </div>
        ))}
        {hang.map((a) => (
          <div key={`hang-${a.id}`} style={{ fontSize: 12, lineHeight: 1.35 }}>
            <div style={{ fontWeight: 700, display: "flex", alignItems: "center", gap: 5 }}>
              <IconClockExclamation size={14} style={{ flexShrink: 0 }} />
              <span>
                ハング検知/復旧 · {a.label}
                {a.error_worker ? ` · ${a.error_worker}` : ""}
              </span>
            </div>
            <div
              style={{
                opacity: 0.85,
                fontFamily: "ui-monospace, monospace",
                fontSize: 11,
                wordBreak: "break-word",
              }}
            >
              {String(a.error).slice(0, 160)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------- ページ ----------

export default function Flow() {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const qc = useQueryClient();
  const [controlSlug, setControlSlug] = useState<string | null>(null);
  const rfBoxRef = useRef<HTMLDivElement>(null);
  const [annos, setAnnos] = useState<Anno[]>(() => {
    try { return JSON.parse(localStorage.getItem(ANNO_LS_KEY) || '[]') as Anno[]; }
    catch { return []; }
  });
  const [activeTool, setActiveTool] = useState<AnnoKind | null>(null);
  const [annoColor, setAnnoColor] = useState('#ef4444');
  const [annoFontSize, setAnnoFontSize] = useState(14);
  // テキスト入力: Box直下でレンダ（ReactFlow内に置くとイベント干渉するため）
  const [textInput, setTextInput] = useState<{ sx: number; sy: number; fx: number; fy: number } | null>(null);
  const [textVal, setTextVal] = useState('');
  const textInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { localStorage.setItem(ANNO_LS_KEY, JSON.stringify(annos)); }, [annos]);

  const addAnno = useCallback((a: Omit<Anno, 'id'>) => {
    setAnnos((prev) => [...prev, { ...a, id: `${Date.now()}-${Math.random().toString(36).slice(2)}` }]);
  }, []);

  const deleteAnno = useCallback((id: string) => {
    setAnnos((prev) => prev.filter((a) => a.id !== id));
  }, []);

  const moveAnno = useCallback((id: string, x: number, y: number, x2: number, y2: number) => {
    setAnnos((prev) => prev.map((a) => a.id === id ? { ...a, x, y, x2, y2 } : a));
  }, []);

  const commitTextInput = useCallback(() => {
    if (textInput && textVal.trim()) {
      addAnno({ kind: 'text', x: textInput.fx, y: textInput.fy, x2: textInput.fx, y2: textInput.fy, text: textVal.trim(), color: annoColor, fontSize: annoFontSize });
    }
    setTextInput(null);
    setTextVal('');
  }, [textInput, textVal, annoColor, annoFontSize, addAnno]);

  const handleTextPlace = useCallback((sx: number, sy: number, fx: number, fy: number) => {
    setTextInput({ sx, sy, fx, fy });
    setTextVal('');
    // フォーカスは次レンダ後に当てる
    setTimeout(() => textInputRef.current?.focus(), 0);
  }, []);

  // ツール切替時にテキスト入力をキャンセル
  useEffect(() => { setTextInput(null); setTextVal(''); }, [activeTool]);
  const ctrlValue = useMemo(
    () => ({ openControl: (slug: string) => setControlSlug(slug) }),
    [],
  );
  const snapQ = useQuery({
    queryKey: ["flow-snapshot"],
    queryFn: () => api.flowSnapshot(),
    refetchInterval: 3_000,
    refetchOnWindowFocus: true,
  });
  // 各ノードの「投入数」(直近 RATE_WINDOW_MIN 分の実件数合計) 用。snapshot と別系統の
  // 1 分バケットなので更新頻度は控えめ (件数は分解能が粗く 3 秒更新は不要)。
  const ratesQ = useQuery({
    queryKey: ["flow-rates-window", RATE_WINDOW_MIN],
    queryFn: () => api.flowRates(RATE_WINDOW_MIN),
    refetchInterval: 15_000,
  });
  const submittedBySlug = useMemo(
    () => (ratesQ.data ? windowTotalsBySlug(ratesQ.data.series) : new Map<string, number>()),
    [ratesQ.data],
  );

  // ドラッグで動かしたローカル座標を保持。 サーバ snapshot は metric を上書きするが
  // 座標は最新のローカルを優先 (= drag 中に snapshot 来ても飛ばない)。
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  // debounce save は 800ms 後に走るので、 その時点の位置を読むための ref
  // (= リサイズだけした node は dirtyPositions に居らず x/y を持たないため)。
  const nodesRef = useRef<Node[]>([]);
  const dirtyPositions = useRef<Map<string, { x: number; y: number }>>(new Map());
  useEffect(() => { nodesRef.current = nodes; }, [nodes]);
  // リサイズ結果も位置と同じ扱い (= 3 秒ごとの snapshot rebuild で巻き戻さない)。
  const dirtySizes = useRef<Map<string, { w: number; h: number }>>(new Map());

  // snapshot が来たら nodes を rebuild (= metric 部だけ反映、 位置は dirty 優先)。
  // setNodes の functional form で現 nodes を読み、 activeHandles (= edges
  // useEffect が以前計算したフランジ表示状態) を新 node data に引き継ぐ。 これを
  // しないと 3 秒ごとの snapshot で activeHandles=undefined になって、 既に edges 配列
  // が前回と同 key で memo されてれば edges useEffect が「変化なし」 判定で再計算しないため、
  // フランジが消えたまま戻らない (= 「一定期間後にフランジが消える」 バグ)。
  useEffect(() => {
    const snap = snapQ.data;
    if (!snap) return;
    const flowByNode = new Map<string, { in: number; out: number }>();
    for (const e of snap.edges) {
      const r = e.rate_per_min ?? 0;
      const ti = flowByNode.get(e.target) ?? { in: 0, out: 0 };
      ti.in += r;
      flowByNode.set(e.target, ti);
      const so = flowByNode.get(e.source) ?? { in: 0, out: 0 };
      so.out += r;
      flowByNode.set(e.source, so);
    }
    setNodes((curr) => {
      const prevActiveHandles = new Map<string, string[]>();
      // 選択状態も引き継ぐ (2026-09-01)。 ここで作り直す node は selected を
      // 持たないので、 引き継がないと **3 秒ごとに選択が解除される**。 クリックして
      // からハンドルへマウスを運ぶ間に消えるため、 リサイズが事実上できなかった。
      const prevSelected = new Set<string>();
      for (const n of curr) {
        const ah = (n.data as Record<string, unknown> | undefined)?.activeHandles;
        if (Array.isArray(ah)) prevActiveHandles.set(n.id, ah as string[]);
        if (n.selected) prevSelected.add(n.id);
      }
      return snap.nodes.map((n: FlowNode) => {
        const dirty = dirtyPositions.current.get(n.id);
        const flow = flowByNode.get(n.id);
        const base: Record<string, unknown> = flow
          ? { ...n, inflow_per_min: flow.in, outflow_per_min: flow.out }
          : { ...n };
        const ah = prevActiveHandles.get(n.id);
        if (ah) base.activeHandles = ah;
        if (n.kind === "workload") {
          base.submitted_window = submittedBySlug.get(n.workload_slug || n.id) ?? 0;
          base.submitted_window_min = RATE_WINDOW_MIN;
        }
        // 大きさ: ローカルのリサイズ中 > yaml の w/h > kind ごとの既定。
        // height 既定が undefined の kind は「中身なり」 のまま (= 従来の見た目)。
        const size = dirtySizes.current.get(n.id);
        const w = size?.w ?? n.w ?? NODE_DEFAULT_W[n.kind] ?? NODE_DEFAULT_W.workload;
        const h = size?.h ?? n.h ?? NODE_DEFAULT_H[n.kind];
        if (h != null) base.h = h;
        return {
          id: n.id,
          type: n.kind,
          position: dirty ?? { x: n.x, y: n.y },
          data: base,
          draggable: true,
          selected: prevSelected.has(n.id),
          width: w,
          height: h,
        };
      });
    });
  }, [snapQ.data, submittedBySlug, setNodes]);

  const edges: Edge[] = useMemo(() => {
    const snap = snapQ.data;
    if (!snap) return [];
    // node 中心座標 + サイズ map (= handle 自動選択用)
    // kind ごとの実描画 width/height は WorkloadNode=220x150 / TankNode=180x110 /
    // ExternalNode≒180x90。 ここで小さくズレても dominant-axis 判定はぶれない。
    // フォールバック値のみ。 実際は node の width/height (= yaml の w/h か
    // React Flow の実測) を優先する (= リサイズした box でも配線がズレない)。
    const SIZE: Record<string, { w: number; h: number }> = {
      workload: { w: 220, h: 150 },
      tank: { w: 180, h: 110 },
      external: { w: 180, h: 90 },
    };
    // node 中心 + bbox (= obstacle 判定用)
    type BBox = { cx: number; cy: number; x: number; y: number; w: number; h: number };
    const posMap = new Map<string, BBox>();
    for (const n of nodes) {
      const sz = SIZE[(n.type as string) ?? "workload"] ?? SIZE.workload;
      const w = n.width ?? n.measured?.width ?? sz.w;
      const h = n.height ?? n.measured?.height ?? sz.h;
      posMap.set(n.id, {
        cx: n.position.x + w / 2,
        cy: n.position.y + h / 2,
        x: n.position.x,
        y: n.position.y,
        w,
        h,
      });
    }
    // ----- obstacle-aware handle 自動選択 -----
    // 16 通り (= source 4 辺 × target 4 辺) を試し、 経路 bbox 内に他 node が
    // 入る組合せを obstacle hit としてカウント。 hit 数→ 経路長 の lex 順で
    // 最小を選ぶ。 結果として box の下をくぐらない自然な配線になる (= 完全な
    // 障害物迂回 router ではないが、 ほとんどの典型 case で改善)。
    const sideOffsets: Record<string, { dx: number; dy: number }> = {
      top: { dx: 0, dy: -1 },
      right: { dx: 1, dy: 0 },
      bottom: { dx: 0, dy: 1 },
      left: { dx: -1, dy: 0 },
    };
    const allNodes = Array.from(posMap.entries());      // [id, bbox][]
    const OFF = 24;   // handle exit offset (smoothstep offset と同じ)
    const PADDING = 4;  // bbox 判定の余白 (= 縁すれすれを obstacle にしない)
    function rectOverlap(
      ax1: number, ay1: number, ax2: number, ay2: number,
      bx1: number, by1: number, bx2: number, by2: number,
    ): boolean {
      return ax1 < bx2 && ax2 > bx1 && ay1 < by2 && ay2 > by1;
    }
    function pickHandles(srcId: string, tgtId: string) {
      const s = posMap.get(srcId);
      const t = posMap.get(tgtId);
      if (!s || !t) return { sourceHandle: "s-right", targetHandle: "t-left" };
      const others = allNodes.filter(([nid]) => nid !== srcId && nid !== tgtId);
      type Cand = { src: string; tgt: string; obs: number; len: number };
      let best: Cand | null = null;
      const sides = ["top", "right", "bottom", "left"] as const;
      for (const ss of sides) {
        const so = sideOffsets[ss];
        // source の 4 辺中点 + 24px offset の exit 点
        const sExitX = s.cx + so.dx * (s.w / 2 + OFF);
        const sExitY = s.cy + so.dy * (s.h / 2 + OFF);
        for (const ts of sides) {
          const to = sideOffsets[ts];
          const tExitX = t.cx + to.dx * (t.w / 2 + OFF);
          const tExitY = t.cy + to.dy * (t.h / 2 + OFF);
          // 経路 bbox: source / target exit を含む最小矩形
          const minX = Math.min(sExitX, tExitX) + PADDING;
          const maxX = Math.max(sExitX, tExitX) - PADDING;
          const minY = Math.min(sExitY, tExitY) + PADDING;
          const maxY = Math.max(sExitY, tExitY) - PADDING;
          let obs = 0;
          for (const [, ob] of others) {
            if (rectOverlap(minX, minY, maxX, maxY,
                ob.x, ob.y, ob.x + ob.w, ob.y + ob.h)) {
              obs++;
            }
          }
          const len = Math.abs(sExitX - tExitX) + Math.abs(sExitY - tExitY);
          if (
            !best ||
            obs < best.obs ||
            (obs === best.obs && len < best.len)
          ) {
            best = { src: `s-${ss}`, tgt: `t-${ts}`, obs, len };
          }
        }
      }
      return { sourceHandle: best!.src, targetHandle: best!.tgt };
    }
    // 1 度 build → 2 pass で lane offset を割当て (= 同じ handle から複数
    // 出る場合に並走させる)。 同 handle に対する edge を順番に並べて中央寄せ。
    const built = snap.edges.map((e: FlowEdge) => {
      const rate = e.rate_per_min ?? 0;
      const labelText = e.label
        ? rate > 0
          ? `${e.label}: ${fmtNum(rate)}`
          : e.label
        : null;
      const handles = pickHandles(e.source, e.target);
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: handles.sourceHandle,
        targetHandle: handles.targetHandle,
        type: "particle",
        data: {
          rate,
          dashed: !!e.dashed,
          label: labelText,
          sourceLane: 0,
          targetLane: 0,
        },
      } as Edge;
    });
    // 同 handle に集まる edge を集計 → lane index を中央寄せで配る
    // (= N 本なら -N/2 .. +N/2-1 のオフセット index に)。
    // edge は安定順 (= snap.edges の到着順) で並ぶので毎 tick 同じ並び。
    const groupBy = (key: (e: typeof built[number]) => string) => {
      const m = new Map<string, typeof built>();
      for (const e of built) {
        const k = key(e);
        if (!m.has(k)) m.set(k, []);
        m.get(k)!.push(e);
      }
      return m;
    };
    const sourceGroups = groupBy((e) => `${e.source}:${e.sourceHandle}`);
    const targetGroups = groupBy((e) => `${e.target}:${e.targetHandle}`);
    // lane は「相手側ノードの位置順」に並べてから割り当てる。 こうすると
    // 上の出力→上のノード / 下の出力→下のノード となり、 同じ handle から出る
    // (入る) 配管どうしが交差しない。 旧実装は edge 到着順で割当てていたため、
    // 上の出力が下のタンクに繋がると下の出力線と交差していた。 3 本以上でも
    // 位置ソートなので単調 (= 交差ゼロ)。
    //   水平 handle (Left/Right) → lane は Y 方向 → 相手の cy 昇順
    //   垂直 handle (Top/Bottom) → lane は X 方向 → 相手の cx 昇順
    const laneAxis = (handle?: string | null): "y" | "x" =>
      handle && (handle.includes("left") || handle.includes("right")) ? "y" : "x";
    const otherPos = (id: string, axis: "y" | "x"): number => {
      const p = posMap.get(id);
      if (!p) return 0;
      return axis === "y" ? p.cy : p.cx;
    };
    for (const [, list] of sourceGroups) {
      const axis = laneAxis(list[0].sourceHandle as string | undefined);
      list.sort((a, b) => otherPos(a.target, axis) - otherPos(b.target, axis));
      list.forEach((e, i) => {
        ((e.data as Record<string, unknown>).sourceLane as number) =
          i - (list.length - 1) / 2;
      });
    }
    for (const [, list] of targetGroups) {
      const axis = laneAxis(list[0].targetHandle as string | undefined);
      list.sort((a, b) => otherPos(a.source, axis) - otherPos(b.source, axis));
      list.forEach((e, i) => {
        ((e.data as Record<string, unknown>).targetLane as number) =
          i - (list.length - 1) / 2;
      });
    }
    // 双方向ペア (A->B と B->A) は 1 本のパイプに統合 (data.bidirectional)、
    // 粒子を両方向に流して双方向を表現する。 2 本に分けて lane オフセットすると
    // source/target の handle 向きが違う集合点でねじれるため、 1 本化が安定。
    const _drop = new Set<string>();
    for (const e of built) {
      if (_drop.has(e.id)) continue;
      const rev = built.find(
        (x) => x.source === e.target && x.target === e.source && x.id !== e.id,
      );
      if (rev) {
        (e.data as Record<string, unknown>).bidirectional = true;
        _drop.add(rev.id);
      }
    }
    return built.filter((e) => !_drop.has(e.id));
  }, [snapQ.data, nodes]);

  // edges から activeHandlesMap (= node id → 使用 handle id set) を組み、
  // nodes.data.activeHandles に反映 (= フランジ表示する handle を限定)。
  // 同じ map なら setNodes をスキップ (= edges→nodes→edges のループ防止)。
  const prevActiveKeyRef = useRef("");
  useEffect(() => {
    const map = new Map<string, Set<string>>();
    for (const e of edges) {
      if (e.sourceHandle) {
        let s = map.get(e.source);
        if (!s) { s = new Set(); map.set(e.source, s); }
        s.add(e.sourceHandle);
      }
      if (e.targetHandle) {
        let s = map.get(e.target);
        if (!s) { s = new Set(); map.set(e.target, s); }
        s.add(e.targetHandle);
      }
    }
    // 同一性 key (= ソート済 string) を作って 前回と比較
    const key = Array.from(map.entries())
      .map(([nid, s]) => `${nid}:${Array.from(s).sort().join(",")}`)
      .sort()
      .join("|");
    if (key === prevActiveKeyRef.current) return;
    prevActiveKeyRef.current = key;
    setNodes((curr) =>
      curr.map((n) => ({
        ...n,
        data: {
          ...(n.data as Record<string, unknown>),
          activeHandles: Array.from(map.get(n.id) ?? []),
        },
      })),
    );
  }, [edges, setNodes]);

  // ---------- Phase 3: ドラッグで位置を YAML に PATCH ----------
  const saveLayoutMut = useMutation({
    mutationFn: (
      positions: Array<{ id: string; x: number; y: number; w?: number; h?: number }>,
    ) => api.saveFlowLayout(positions),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["flow-snapshot"] });
    },
  });

  // debounce save (= 1 ドラッグ後 800ms 静止で送信)
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const scheduleSave = useCallback(() => {
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      // リサイズだけした node は dirtyPositions に居ないので、 両方の id を集める。
      // 位置は現在値が要るので nodes から引く (= x/y は必須フィールド)。
      const ids = new Set([
        ...dirtyPositions.current.keys(),
        ...dirtySizes.current.keys(),
      ]);
      const posOf = new Map(nodesRef.current.map((n) => [n.id, n.position]));
      const payload = Array.from(ids).flatMap((id) => {
        const p = dirtyPositions.current.get(id) ?? posOf.get(id);
        if (!p) return [];
        const sz = dirtySizes.current.get(id);
        return [{
          id,
          x: Math.round(p.x),
          y: Math.round(p.y),
          ...(sz ? { w: Math.round(sz.w), h: Math.round(sz.h) } : {}),
        }];
      });
      if (payload.length > 0) {
        saveLayoutMut.mutate(payload);
      }
    }, 800);
  }, [saveLayoutMut]);

  // node change handler: position 変化を dirty に記録 → debounce save
  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      onNodesChange(changes);
      for (const c of changes) {
        if (c.type === "position" && c.position) {
          dirtyPositions.current.set(c.id, c.position);
        }
        // dimensions は「初回計測」 でも飛んでくる。 NodeResizer 由来のものだけ
        // 拾いたいので resizing フラグの有無で区別する (計測由来には付かない)。
        if (c.type === "dimensions" && c.dimensions && c.resizing !== undefined) {
          dirtySizes.current.set(c.id, { w: c.dimensions.width, h: c.dimensions.height });
        }
      }
      // drag / resize 終了の signal で save
      if (changes.some((c) =>
        (c.type === "position" && c.dragging === false) ||
        (c.type === "dimensions" && c.resizing === false))) {
        scheduleSave();
      }
    },
    [onNodesChange, scheduleSave],
  );

  if (snapQ.isLoading) {
    return (
      <Box p="xl">
        <Loader />
        <Text size="sm" c="dimmed" mt="sm">{t("flow.loading")}</Text>
      </Box>
    );
  }
  if (snapQ.error) {
    return (
      <Box p="xl">
        <Text c="red">{t("flow.fetch_failed", { error: String(snapQ.error) })}</Text>
      </Box>
    );
  }

  const isLight = colorScheme === "light";
  // canvas 背景: dark = yaml の暗色、 light = やさしいクリーム白
  const bg = isLight ? "#f6f7fb" : (snapQ.data?.canvas.background || "#0f1120");
  const bgDotColor = isLight ? "#dde3ee" : "#1e293b";

  return (
    <FlowControlContext.Provider value={ctrlValue}>
    <Box ref={rfBoxRef} style={{ height: "calc(100vh - 80px)", background: bg, borderRadius: 8, overflow: "hidden", position: "relative" }}>
      <style>{RESIZE_BAND_CSS}</style>
      <GpuAlertBox nodes={snapQ.data?.nodes ?? []} infraAlerts={snapQ.data?.infra_alerts ?? []} />
      <AnnotationToolbar
        activeTool={activeTool}
        setActiveTool={setActiveTool}
        annoColor={annoColor}
        setAnnoColor={setAnnoColor}
        annoFontSize={annoFontSize}
        setAnnoFontSize={setAnnoFontSize}
        onClear={() => { if (window.confirm('加筆を全て消去しますか?')) setAnnos([]); }}
        isLight={isLight}
      />
      <Modal
        opened={controlSlug !== null}
        onClose={() => setControlSlug(null)}
        title={controlSlug ? t("flow.modal_title", "{{slug}} を調整", { slug: controlSlug }) : ""}
        size="md"
        centered
      >
        {controlSlug && <WorkloadControlPopover slug={controlSlug} />}
      </Modal>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        onNodesChange={handleNodesChange}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        proOptions={{ hideAttribution: true }}
        nodesConnectable={false}
        nodesDraggable
        // box をクリックで選択 → リサイズ枠が出る。 選択できないと NodeResizer の
        // 表示条件が作れないので false から変更した (2026-08-31)。
        elementsSelectable
        // 選択中に Backspace を押しても node を消さない (= 表示専用の画面)。
        deleteKeyCode={null}
        style={{ background: bg }}
      >
        <Background gap={20} size={1} color={bgDotColor} />
        <Controls showInteractive={false} style={{ background: isLight ? "#ffffff" : "#1e293b" }} />
        <AnnotationCanvas
          annos={annos}
          activeTool={activeTool}
          annoColor={annoColor}
          annoFontSize={annoFontSize}
          rfBoxRef={rfBoxRef}
          onAdd={addAnno}
          onDelete={deleteAnno}
          onTextPlace={handleTextPlace}
        />
        <DragHandles annos={annos} onMove={moveAnno} />
        <MiniMap
          nodeColor={(n) => {
            const d = n.data as unknown as FlowNode;
            if (d.kind === "tank") return isLight ? "#60a5fa" : "#3b82f6";
            if (d.kind === "external") return isLight ? "#94a3b8" : "#64748b";
            return STATE_COLOR[d.state ?? "idle"];
          }}
          maskColor={isLight ? "rgba(246,247,251,0.6)" : "rgba(15,17,32,0.6)"}
          style={{ background: isLight ? "#ffffff" : "#0f172a" }}
        />
      </ReactFlow>
      {/* テキスト入力: ReactFlow外のBox直下でレンダ（ReactFlow内ではイベント干渉するため） */}
      {textInput && (
        <input
          ref={textInputRef}
          value={textVal}
          onChange={(e) => setTextVal(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') { e.preventDefault(); commitTextInput(); }
            if (e.key === 'Escape') { setTextInput(null); setTextVal(''); }
          }}
          onBlur={commitTextInput}
          style={{
            position: 'absolute',
            left: textInput.sx,
            top: textInput.sy,
            zIndex: 1100,
            background: 'rgba(15,23,42,0.88)',
            color: annoColor,
            border: `1.5px solid ${annoColor}`,
            borderRadius: 3,
            padding: '2px 8px',
            fontSize: annoFontSize,
            outline: 'none',
            minWidth: 140,
            fontFamily: 'inherit',
            pointerEvents: 'all',
          }}
          placeholder="Enter で確定 / Esc でキャンセル"
        />
      )}
    </Box>
    </FlowControlContext.Provider>
  );
}
