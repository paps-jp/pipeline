import { Box, Button, Center, Stack, Text } from "@mantine/core";
import type { Icon } from "@tabler/icons-react";
import { IconInbox } from "@tabler/icons-react";
import type { ReactNode } from "react";

/**
 * 共通 Empty 状態。
 *
 *   <EmptyState
 *      icon={IconUserOff}
 *      title="ワーカーが居ません"
 *      description="bootstrap スクリプトで GPU 箱を追加してください。"
 *      action={{ label: "Deploy ページへ", onClick: () => navigate('/deploy') }}
 *   />
 */
export function EmptyState({
  icon: IconComp = IconInbox,
  title,
  description,
  action,
  minHeight = 220,
}: {
  icon?: Icon;
  title: ReactNode;
  description?: ReactNode;
  action?: { label: string; onClick: () => void } | ReactNode;
  minHeight?: number;
}) {
  const actionNode =
    action && typeof action === "object" && "label" in action ? (
      <Button variant="light" onClick={(action as { onClick: () => void }).onClick}>
        {(action as { label: string }).label}
      </Button>
    ) : (
      (action as ReactNode)
    );

  return (
    <Center mih={minHeight} p="lg">
      <Stack align="center" gap="xs" maw={420} ta="center">
        <Box
          style={{
            width: 56,
            height: 56,
            borderRadius: "50%",
            display: "grid",
            placeItems: "center",
            background:
              "color-mix(in srgb, var(--mantine-color-indigo-5) 10%, transparent)",
            color: "var(--mantine-color-indigo-5)",
          }}
        >
          <IconComp size={28} stroke={1.5} />
        </Box>
        <Text fw={600} size="md" mt={4}>
          {title}
        </Text>
        {description && (
          <Text size="sm" c="dimmed">
            {description}
          </Text>
        )}
        {actionNode && <Box mt={6}>{actionNode}</Box>}
      </Stack>
    </Center>
  );
}
