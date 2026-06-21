import { Group, Stack, Text, Title } from "@mantine/core";
import type { ReactNode } from "react";

/**
 * 全ページ共通の見出しブロック。
 *
 * - `title` は order=2 で統一 (= Dashboard / Workloads / Workers / ServiceLog / Deploy 同一)
 * - `subtitle` (任意) で 1 行説明 c="dimmed"
 * - `right` (任意) で右肩に actions / status badge
 * - 下に mb="md" の余白 → 各 page で Stack gap="lg" と組み合わせて 整った縦リズム
 */
export function PageHeader({
  title,
  subtitle,
  right,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  right?: ReactNode;
}) {
  return (
    <Group justify="space-between" align="flex-end" wrap="nowrap" mb="md">
      <Stack gap={2} style={{ minWidth: 0 }}>
        <Title order={2} style={{ letterSpacing: "-0.01em" }}>
          {title}
        </Title>
        {subtitle && (
          <Text size="sm" c="dimmed">
            {subtitle}
          </Text>
        )}
      </Stack>
      {right && (
        <Group gap="xs" wrap="nowrap">
          {right}
        </Group>
      )}
    </Group>
  );
}
