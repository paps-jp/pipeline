import { Box, Group, Text, Tooltip } from "@mantine/core";

/**
 * 直近 N 件の run 成否を縦棒で並べる。
 * bits は新しい順 (左 = 新しい)。
 *   1 = success (teal)
 *   0 = failure (red)
 *  -1 = unknown / running (gray)
 */
export function RunsSparkline({
  bits,
  rate,
  height = 18,
  barWidth = 4,
  gap = 1,
}: {
  bits: number[];
  rate: number | null;
  height?: number;
  barWidth?: number;
  gap?: number;
}) {
  if (bits.length === 0) {
    return <Text size="xs" c="dimmed">—</Text>;
  }
  // bits[0] = 一番新しい → 右端に置きたいので reverse
  const reversed = [...bits].reverse();
  const rateText = rate === null ? "—" : `${Math.round(rate * 100)}%`;
  const rateColor =
    rate === null ? "dimmed" : rate >= 0.9 ? "teal.6" : rate >= 0.6 ? "yellow.7" : "red.6";

  return (
    <Tooltip
      label={`直近 ${bits.length} 件 成功率 ${rateText}`}
      withArrow
      position="top"
    >
      <Group gap={6} wrap="nowrap">
        <Box style={{ display: "flex", alignItems: "flex-end", gap }} h={height}>
          {reversed.map((b, i) => (
            <Box
              key={i}
              w={barWidth}
              h={b === -1 ? height * 0.4 : height}
              style={{
                background:
                  b === 1
                    ? "var(--mantine-color-teal-5)"
                    : b === 0
                    ? "var(--mantine-color-red-5)"
                    : "var(--mantine-color-gray-4)",
                borderRadius: 1,
              }}
            />
          ))}
        </Box>
        <Text size="xs" fw={600} c={rateColor} style={{ minWidth: 36, textAlign: "right" }}>
          {rateText}
        </Text>
      </Group>
    </Tooltip>
  );
}
