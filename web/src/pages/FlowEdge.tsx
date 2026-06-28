/**
 * 流量比例の粒子アニメ edge。 React Flow の getSmoothStepPath (= 直交配管)
 * + SVG animateMotion で粒子を流す。 工場の配管アニメ風。
 *
 * 粒子数 = 流量の log scale (1..8)、
 * アニメ duration = 流量逆比例 (= rate 大 → 速い)。
 * bezier から smoothstep に変えた理由: source/target の handle が反対側に
 * ある場合 (= ノードが間に挟まる) bezier だと box を貫通するが、 smoothstep
 * は L 字 or ⊃ 字に曲がるため box との重なりが大幅に減る。
 */

import {
  BaseEdge,
  EdgeLabelRenderer,
  Position,
  getSmoothStepPath,
  type EdgeProps,
} from "@xyflow/react";
import { useMantineColorScheme } from "@mantine/core";

type EdgeData = {
  rate?: number | null;
  dashed?: boolean;
  label?: string | null;
  sourceLane?: number;
  targetLane?: number;
};

const LANE_GAP = 16;   // 同じ handle に集まる配管間のピクセル間隔

export function ParticleEdge(props: EdgeProps) {
  const {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    data,
    style,
    markerEnd,
  } = props;
  const d0 = (data ?? {}) as EdgeData;
  // 同 handle に複数 edge がある場合の lane オフセット:
  //   水平 handle (= Left/Right) → Y 方向にずらす
  //   垂直 handle (= Top/Bottom) → X 方向にずらす
  const sLane = (d0.sourceLane ?? 0) * LANE_GAP;
  const tLane = (d0.targetLane ?? 0) * LANE_GAP;
  const srcHoriz =
    sourcePosition === Position.Left || sourcePosition === Position.Right;
  const tgtHoriz =
    targetPosition === Position.Left || targetPosition === Position.Right;
  const sx = srcHoriz ? sourceX : sourceX + sLane;
  const sy = srcHoriz ? sourceY + sLane : sourceY;
  const tx = tgtHoriz ? targetX : targetX + tLane;
  const ty = tgtHoriz ? targetY + tLane : targetY;
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX: sx,
    sourceY: sy,
    sourcePosition,
    targetX: tx,
    targetY: ty,
    targetPosition,
    borderRadius: 12,    // 角丸 → 滑らか
    offset: 24,           // handle から最初の曲げまでの余白 (= node の縁から離す)
  });
  const d = d0;
  const rate = Number(d.rate ?? 0);
  const isActive = rate > 0;
  const particleCount = isActive ? Math.min(8, Math.max(2, Math.ceil(Math.log10(rate + 1) * 2 + 1))) : 0;
  // 速度: rate=1→6s / rate=10→4s / rate=100→2s / rate=1000→1s 程度
  const duration = isActive ? Math.max(0.8, 6 - Math.log10(rate + 1) * 1.4) : 0;

  // 配管風 3 層 stroke:
  //   dark: 工業金属 (dark shell + 落ち着き indigo fluid)
  //   light: やさしい配管 (灰白 shell + 淡 indigo fluid)
  // 旧版は緑 (= #22c55e) だったが目が疲れるとの指摘で 落ち着いた indigo 系へ。
  const { colorScheme } = useMantineColorScheme();
  const isLight = colorScheme === "light";
  const fluidColor = isActive
    ? (isLight ? "#818cf8" : "#6366f1")    // light=soft indigo / dark=indigo
    : (isLight ? "#cbd5e1" : "#64748b");
  const shellColor = isLight ? "#e2e8f0" : "#1f2937";
  const shadowColor = isLight ? "#cbd5e1" : "#0b1220";
  // 配管直径 (log scale)。 base 太め + 流量で更に太る。 視覚的に "管" として
  // 認識できる下限 = 10px、 上限は ノードに対し主張しすぎない 22px に。
  const baseWidth = Math.min(22, 10 + Math.log10(rate + 1) * 3);
  const shellWidth = baseWidth;
  const fluidWidth = Math.max(2, baseWidth - 6);
  const highlightWidth = Math.max(0.8, fluidWidth / 4);

  // rate=0 (= 停止中) では「死んでる」 視覚表現にする (= 全体 opacity を下げ、
  // 上面ハイライト(白点線)は描画しない)。 これで動いてないのに「流れて見える」
  // 錯覚を解消する。
  const inactiveDim = isActive ? 1 : 0.4;
  return (
    <>
      {/* 1. shadow — 配管の下にうっすら */}
      <BaseEdge
        id={`${id}-shadow`}
        path={edgePath}
        style={{
          ...style,
          stroke: shadowColor,
          strokeWidth: shellWidth + 4,
          strokeLinecap: "round",
          strokeLinejoin: "round",
          opacity: (isLight ? 0.5 : 0.45) * inactiveDim,
          fill: "none",
        }}
      />
      {/* 2. shell — 配管の金属外殻 */}
      <BaseEdge
        id={`${id}-shell`}
        path={edgePath}
        style={{
          ...style,
          stroke: shellColor,
          strokeWidth: shellWidth,
          strokeLinecap: "round",
          strokeLinejoin: "round",
          opacity: 0.95 * inactiveDim,
          fill: "none",
        }}
      />
      {/* 3. fluid — 内側を流れる流体色 */}
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          ...style,
          stroke: fluidColor,
          strokeWidth: fluidWidth,
          strokeLinecap: "round",
          strokeLinejoin: "round",
          strokeDasharray: d.dashed ? "6 4" : undefined,
          opacity: (isActive ? 0.95 : 0.5) * inactiveDim,
          fill: "none",
          // glow を控えめに (= 旧 `0 0 4px aa` を半分以下に)
          filter: isActive ? `drop-shadow(0 0 1.5px ${fluidColor}55)` : undefined,
        }}
      />
      {/* 4. highlight — 細い上面ハイライト (= 金属配管のテカリ。 light は控えめ)。
          rate=0 では描画しない: 白い点線が「動いてないのに流れて見える」 錯覚を生むため。 */}
      {isActive && (
        <BaseEdge
          id={`${id}-hl`}
          path={edgePath}
          style={{
            ...style,
            stroke: "#ffffff",
            strokeWidth: highlightWidth,
            strokeLinecap: "round",
            strokeLinejoin: "round",
            strokeDasharray: "1 6",
            opacity: isLight ? 0.6 : 0.35,
            fill: "none",
          }}
        />
      )}
      {isActive &&
        Array.from({ length: particleCount }).map((_, i) => (
          <circle
            key={i}
            r={Math.max(2, fluidWidth / 2.2)}
            fill={isLight ? "#ffffff" : "#ecfeff"}
            opacity={isLight ? 0.85 : 0.95}
            style={{ filter: `drop-shadow(0 0 4px ${fluidColor}${isLight ? "88" : "cc"})` }}
          >
            <animateMotion
              dur={`${duration}s`}
              repeatCount="indefinite"
              begin={`${(duration / particleCount) * i}s`}
              path={edgePath}
              rotate="auto"
            />
          </circle>
        ))}
      {d.label && (() => {
        // edge の中央セグメントの向きに応じて perpendicular にずらす:
        //  - 水平 (= L↔R handle) → ラベルを線の 上 (-100% Y) に
        //  - 垂直 (= T↔B handle) → ラベルを線の 右 (+8px X) に
        // どちらでも label が線に重ならない。
        const isHorizontal =
          (sourcePosition === Position.Left || sourcePosition === Position.Right) &&
          (targetPosition === Position.Left || targetPosition === Position.Right);
        const offsetTransform = isHorizontal
          ? "translate(-50%, calc(-100% - 4px))"
          : "translate(8px, -50%)";
        return (
          <EdgeLabelRenderer>
            <div
              style={{
                position: "absolute",
                transform: `${offsetTransform} translate(${labelX}px, ${labelY}px)`,
                background: isLight ? "#ffffff" : "#0f172a",
                color: isLight ? "#1f2937" : "#e2e8f0",
                padding: "2px 6px",
                borderRadius: 6,
                fontSize: 11,
                fontWeight: 600,
                border: `1px solid ${fluidColor}${isLight ? "77" : "55"}`,
                boxShadow: isLight ? "0 1px 3px rgba(15,23,42,0.08)" : "none",
                pointerEvents: "none",
              }}
              className="nodrag nopan"
            >
              {d.label}
            </div>
          </EdgeLabelRenderer>
        );
      })()}
    </>
  );
}
