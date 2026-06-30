/**
 * Flow Dashboard — プラント風 SCADA 図で全 workload + DB tank + 流量を 1 画面表示。
 *
 * バックエンド `/api/v1/flow/snapshot` から layout + 動的 metric を取得し、
 * React Flow + カスタム SVG ノードでレンダ。 3 秒ごとに自動更新。
 */

import { createContext, Fragment, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  useNodesState,
  type Node,
  type Edge,
  type NodeChange,
  type EdgeTypes,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { ActionIcon, Badge, Box, Group, Loader, Modal, Paper, Stack, Text, Tooltip, useMantineColorScheme } from "@mantine/core";
import { IconAdjustmentsHorizontal } from "@tabler/icons-react";
import { motion } from "framer-motion";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import {
  IconArrowsSplit,
  IconBrain,
  IconCpu,
  IconDatabase,
  IconDatabaseImport,
  IconLink,
  IconList,
  IconMoodSmile,
  IconPhoto,
  IconScan,
  IconSearch,
  IconSend,
  IconServer,
  IconUsers,
  IconVideo,
  type Icon,
  type IconProps,
} from "@tabler/icons-react";

import { api, type FlowEdge, type FlowNode } from "@/api/client";
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

function WorkloadNode({ data }: { data: FlowNode & { activeHandles?: string[] } }) {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const ctrl = useContext(FlowControlContext);
  const isLight = colorScheme === "light";
  const color = STATE_COLOR[data.state ?? "idle"];
  const label = t(STATE_KEY[data.state ?? "idle"]);
  const nodeLabel = useNodeLabel(data);
  const adapt = (data.adapt ?? {}) as Record<string, number>;
  const active = useMemo(() => new Set(data.activeHandles ?? []), [data.activeHandles]);
  // theme-aware 色: dark = 工業 / light = やさしいパステル白系
  const cardBg = isLight
    ? "linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)"
    : "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)";
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
      style={{
        width: 220,
        background: cardBg,
        border: cardBorder,
        borderRadius: isLight ? 4 : 2,      // 工業計器風: 角はキリッと
        padding: 10,
        color: cardText,
        boxShadow: cardShadow,
        fontSize: 12,
        position: "relative",
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
                style={{ cursor: "pointer" }}
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
      </Stack>
      {data.state === "failed" && <ErrorOverlay />}
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
function TankWave({ color, isLight }: { color: string; isLight: boolean }) {
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
        <animateTransform
          attributeName="transform"
          type="translate"
          values="-200 0; 0 0"
          dur="5.5s"
          repeatCount="indefinite"
        />
      </g>
      {/* 1. メイン波 (= 濃色) */}
      <g>
        <path d={pathMain} fill={color} opacity={isLight ? 0.72 : 0.9} />
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 0; -200 0"
          dur="3.5s"
          repeatCount="indefinite"
        />
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
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 0; -200 0"
          dur="3.5s"
          repeatCount="indefinite"
        />
      </g>
      {/* 4. キラキラ泡 (= 大きめの白点、 各点が上下バウンド + 透明度脈動) */}
      <g>
        {[
          { cx: 60,  cy: 14, r: 1.8, dur: 2.4 },
          { cx: 200, cy: 16, r: 1.5, dur: 2.9 },
          { cx: 360, cy: 12, r: 2.0, dur: 3.1 },
          { cx: 510, cy: 16, r: 1.5, dur: 2.6 },
          { cx: 680, cy: 14, r: 1.8, dur: 2.3 },
        ].map((b, i) => (
          <circle key={i} cx={b.cx} cy={b.cy} r={b.r} fill={hlColor}>
            <animate
              attributeName="cy"
              values={`${b.cy};${b.cy - 3};${b.cy}`}
              dur={`${b.dur}s`}
              repeatCount="indefinite"
            />
            <animate
              attributeName="opacity"
              values="0.15;0.95;0.15"
              dur={`${b.dur}s`}
              repeatCount="indefinite"
            />
          </circle>
        ))}
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 0; -200 0"
          dur="3.5s"
          repeatCount="indefinite"
        />
      </g>
      {/* 5. specular sweep (= 反射光が横切るバンド、 7s に 1 回斜めに走る) */}
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
        <animateTransform
          attributeName="transform"
          type="translate"
          values="800 0; -160 0"
          dur="7s"
          repeatCount="indefinite"
        />
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

function HazardOverlay({ bg, barBg, barText, label, barBelow = false }: {
  bg: string; barBg: string; barText: string; label: string; barBelow?: boolean;
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
          false は box 内下部に inset (= 既存の WARNING 表示)。 */}
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
    </>
  );
}

// 黄黒テープ: tank overflow (fill_ratio >= 1.0) 警告。
function OverflowOverlay() {
  return <HazardOverlay bg={HAZARD_BG} barBg="#fbbf24" barText="#0f172a" label="⚠ WARNING" />;
}

// 赤黒テープ: workload エラー (state === "failed") 表示。
function ErrorOverlay() {
  return <HazardOverlay bg={HAZARD_RED_BG} barBg="#ef4444" barText="#ffffff" label="✕ ERROR" barBelow />;
}

function TankNode({ data }: { data: FlowNode & { activeHandles?: string[];
                                                    inflow_per_min?: number;
                                                    outflow_per_min?: number } }) {
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
        width: 180,
        height: 110,
        background: cardBg,
        border: cardBorder,
        borderRadius: isLight ? 4 : 2,
        padding: 8,
        color: cardText,
        position: "relative",
        overflow: "hidden",
        boxShadow: isLight ? "0 1px 4px rgba(15,23,42,0.06)" : "none",
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
          {fmtNum(data.pending)}
        </Text>
        {data.capacity_warn != null && (
          <Text size="xs" c="dimmed">/ {fmtNum(data.capacity_warn)}</Text>
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
    </motion.div>
  );
}

function ExternalNode({ data }: { data: FlowNode & { activeHandles?: string[] } }) {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const nodeLabel = useNodeLabel(data);
  const active = useMemo(() => new Set(data.activeHandles ?? []), [data.activeHandles]);
  return (
    <Paper
      p={10}
      style={{
        width: 180,
        background: isLight
          ? "linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)"
          : "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)",
        border: isLight ? "1.5px dashed #94a3b8" : "2px dashed #64748b",
        borderRadius: isLight ? 4 : 2,
        color: isLight ? "#1f2937" : "#e2e8f0",
        position: "relative",
        boxShadow: isLight ? "0 1px 4px rgba(15,23,42,0.06)" : "none",
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
    </Paper>
  );
}

const NODE_TYPES = {
  workload: WorkloadNode as never,
  tank: TankNode as never,
  external: ExternalNode as never,
};

const EDGE_TYPES: EdgeTypes = {
  particle: ParticleEdge,
};

// ---------- worker × workload マトリクス ----------
// worker daemon は workload_filter (= 自動切替の SoT) に従って claim する。
// 各 host の各 worker の filter を表で見せ、 セルクリックで toggle 可能。

function parseHostFromWid(wid: string): string {
  // _host_stats と同規約: w_ai_gpu1_3_c0da → ai-gpu1
  if (!wid.startsWith("w_")) return wid;
  const parts = wid.slice(2).split("_");
  if (parts.length >= 2) return `${parts[0]}-${parts[1]}`;
  return wid;
}

function parseInstFromHost(host: string): number {
  // "ai-gpu1-6" → 6、 サフィックス無しは 0
  const m = host.match(/-(\d+)$/);
  return m ? parseInt(m[1], 10) : 0;
}

function WorkerMatrixPanel({ isLight }: { isLight: boolean }) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const workersQ = useQuery({
    queryKey: ["workers-with-filter"],
    queryFn: () => api.listWorkers(),
    refetchInterval: open ? 5_000 : 30_000,
  });
  const wlsQ = useQuery({
    queryKey: ["workloads-for-matrix"],
    queryFn: () => api.listWorkloads(),
    refetchInterval: 60_000,
  });

  const data = useMemo(() => {
    const ws = (workersQ.data?.workers ?? []).filter((w) => w.state === "active");
    const wls = (wlsQ.data?.workloads ?? []).filter((w) => w.enabled).map((w) => w.slug);
    // host → 各 worker (instance 順)
    const byHost = new Map<string, typeof ws>();
    for (const w of ws) {
      const host = parseHostFromWid(w.id);
      const arr = byHost.get(host) ?? [];
      arr.push(w);
      byHost.set(host, arr);
    }
    for (const arr of byHost.values()) {
      arr.sort((a, b) => parseInstFromHost(a.host) - parseInstFromHost(b.host));
    }
    return {
      hosts: Array.from(byHost.keys()).sort(),
      byHost,
      workloads: wls,
    };
  }, [workersQ.data, wlsQ.data]);

  const toggleMut = useMutation({
    mutationFn: async (args: { workerId: string; cur: string[] | null;
                                slug: string; allSlugs: string[] }) => {
      // cur=null (= 全 workload 対象) のセルをクリック → そのセルだけ「外した」 list を作る
      // cur=list → セルが含まれるか? toggle
      let next: string[] | null;
      if (args.cur === null) {
        next = args.allSlugs.filter((s) => s !== args.slug).sort();
      } else if (args.cur.includes(args.slug)) {
        const r = args.cur.filter((s) => s !== args.slug).sort();
        next = r;    // 空 list は server 側で「解除 == env fallback」 になる
      } else {
        next = [...args.cur, args.slug].sort();
      }
      return api.setWorkerFilter(args.workerId, next, "matrix-ui");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workers-with-filter"] }),
  });

  const cellBg = (active: boolean, allActive: boolean) =>
    allActive ? "#94a3b8" : active ? "#22c55e" : "transparent";
  const cellColor = (active: boolean, allActive: boolean) =>
    allActive || active ? "#ffffff" : isLight ? "#64748b" : "#94a3b8";

  return (
    <Paper
      shadow="md"
      radius="md"
      style={{
        position: "absolute",
        top: 12,
        right: 12,
        zIndex: 10,
        background: isLight ? "#ffffff" : "#1e293b",
        border: `1px solid ${isLight ? "#e2e8f0" : "#334155"}`,
        maxHeight: "calc(100vh - 120px)",
        overflow: "auto",
        padding: 8,
      }}
    >
      <Group justify="space-between" wrap="nowrap" gap={8} mb={open ? 8 : 0}>
        <Text size="xs" fw={700} c={isLight ? "#0f172a" : "#e2e8f0"}>
          {t("flow.matrix.title", "Worker × Workload")}
        </Text>
        <Text
          size="xs"
          c={isLight ? "#64748b" : "#94a3b8"}
          style={{ cursor: "pointer", userSelect: "none" }}
          onClick={() => setOpen(!open)}
        >
          {open ? "▲" : "▼"}
        </Text>
      </Group>
      {open && data.hosts.length > 0 && (
        <Box style={{ overflowX: "auto" }}>
          <table style={{ borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                <th style={{ textAlign: "left", padding: "2px 6px",
                              color: isLight ? "#475569" : "#cbd5e1" }}>
                  host / @inst
                </th>
                {data.workloads.map((slug) => (
                  <th
                    key={slug}
                    style={{
                      padding: "2px 4px",
                      writingMode: "vertical-rl",
                      transform: "rotate(180deg)",
                      height: 80,
                      color: isLight ? "#475569" : "#cbd5e1",
                      fontWeight: 500,
                    }}
                    title={slug}
                  >
                    {slug}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.hosts.map((host) => (
                <Fragment key={host}>
                  <tr key={`${host}-head`}>
                    <td
                      colSpan={data.workloads.length + 1}
                      style={{
                        padding: "6px 6px 2px",
                        fontWeight: 700,
                        color: isLight ? "#0f172a" : "#f1f5f9",
                        borderTop: `1px solid ${isLight ? "#e2e8f0" : "#334155"}`,
                      }}
                    >
                      {host}
                    </td>
                  </tr>
                  {(data.byHost.get(host) ?? []).map((w) => {
                    const inst = parseInstFromHost(w.host);
                    const filter = w.workload_filter;
                    const allActive = filter === null;
                    return (
                      <tr key={w.id}>
                        <td
                          style={{
                            padding: "1px 6px",
                            color: isLight ? "#334155" : "#cbd5e1",
                            whiteSpace: "nowrap",
                          }}
                          title={`${w.id}\nupdated by: ${w.filter_updated_by ?? "—"}\nat: ${w.filter_updated_at ?? "—"}`}
                        >
                          @{inst}
                          {allActive && (
                            <Text component="span" size="9px" c="dimmed" ml={4}>
                              (all)
                            </Text>
                          )}
                        </td>
                        {data.workloads.map((slug) => {
                          const active = allActive || (filter?.includes(slug) ?? false);
                          return (
                            <td
                              key={slug}
                              onClick={() =>
                                toggleMut.mutate({
                                  workerId: w.id,
                                  cur: filter,
                                  slug,
                                  allSlugs: data.workloads,
                                })
                              }
                              style={{
                                padding: 0,
                                textAlign: "center",
                                cursor: "pointer",
                                background: cellBg(active, allActive),
                                color: cellColor(active, allActive),
                                border: `1px solid ${isLight ? "#e2e8f0" : "#334155"}`,
                                minWidth: 16,
                                height: 16,
                                lineHeight: "16px",
                                fontSize: 10,
                                fontWeight: 700,
                              }}
                              title={`${w.id} → ${slug}: ${active ? "claim 可" : "filter 外"} (クリックで toggle)`}
                            >
                              {active ? "●" : ""}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </Fragment>
              ))}
            </tbody>
          </table>
          <Text size="9px" c="dimmed" mt={6}>
            ● = この worker が claim 可能 / 灰 = filter=null (env fallback 含む全受) / 緑 = 明示 list 内
          </Text>
        </Box>
      )}
    </Paper>
  );
}

// ---------- ページ ----------

export default function Flow() {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  const qc = useQueryClient();
  const [controlSlug, setControlSlug] = useState<string | null>(null);
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

  // ドラッグで動かしたローカル座標を保持。 サーバ snapshot は metric を上書きするが
  // 座標は最新のローカルを優先 (= drag 中に snapshot 来ても飛ばない)。
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const dirtyPositions = useRef<Map<string, { x: number; y: number }>>(new Map());

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
      for (const n of curr) {
        const ah = (n.data as Record<string, unknown> | undefined)?.activeHandles;
        if (Array.isArray(ah)) prevActiveHandles.set(n.id, ah as string[]);
      }
      return snap.nodes.map((n: FlowNode) => {
        const dirty = dirtyPositions.current.get(n.id);
        const flow = flowByNode.get(n.id);
        const base: Record<string, unknown> = flow
          ? { ...n, inflow_per_min: flow.in, outflow_per_min: flow.out }
          : { ...n };
        const ah = prevActiveHandles.get(n.id);
        if (ah) base.activeHandles = ah;
        return {
          id: n.id,
          type: n.kind,
          position: dirty ?? { x: n.x, y: n.y },
          data: base,
          draggable: true,
        };
      });
    });
  }, [snapQ.data, setNodes]);

  const edges: Edge[] = useMemo(() => {
    const snap = snapQ.data;
    if (!snap) return [];
    // node 中心座標 + サイズ map (= handle 自動選択用)
    // kind ごとの実描画 width/height は WorkloadNode=220x130 / TankNode=180x110 /
    // ExternalNode≒180x90。 ここで小さくズレても dominant-axis 判定はぶれない。
    const SIZE: Record<string, { w: number; h: number }> = {
      workload: { w: 220, h: 130 },
      tank: { w: 180, h: 110 },
      external: { w: 180, h: 90 },
    };
    // node 中心 + bbox (= obstacle 判定用)
    type BBox = { cx: number; cy: number; x: number; y: number; w: number; h: number };
    const posMap = new Map<string, BBox>();
    for (const n of nodes) {
      const sz = SIZE[(n.type as string) ?? "workload"] ?? SIZE.workload;
      posMap.set(n.id, {
        cx: n.position.x + sz.w / 2,
        cy: n.position.y + sz.h / 2,
        x: n.position.x,
        y: n.position.y,
        w: sz.w,
        h: sz.h,
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
    for (const [, list] of sourceGroups) {
      list.forEach((e, i) => {
        ((e.data as Record<string, unknown>).sourceLane as number) =
          i - (list.length - 1) / 2;
      });
    }
    for (const [, list] of targetGroups) {
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
    mutationFn: (positions: Array<{ id: string; x: number; y: number }>) =>
      api.saveFlowLayout(positions),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["flow-snapshot"] });
    },
  });

  // debounce save (= 1 ドラッグ後 800ms 静止で送信)
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const scheduleSave = useCallback(() => {
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      const payload = Array.from(dirtyPositions.current.entries()).map(([id, p]) => ({
        id,
        x: Math.round(p.x),
        y: Math.round(p.y),
      }));
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
      }
      // dragging=false の change が来たら save (drag 終了の signal)
      if (changes.some((c) => c.type === "position" && c.dragging === false)) {
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
    <Box style={{ height: "calc(100vh - 80px)", background: bg, borderRadius: 8, overflow: "hidden", position: "relative" }}>
      <WorkerMatrixPanel isLight={isLight} />
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
        elementsSelectable={false}
        style={{ background: bg }}
      >
        <Background gap={20} size={1} color={bgDotColor} />
        <Controls showInteractive={false} style={{ background: isLight ? "#ffffff" : "#1e293b" }} />
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
    </Box>
    </FlowControlContext.Provider>
  );
}
