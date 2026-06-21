import { Alert, Button, Code, Collapse, Stack } from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { IconAlertTriangle, IconRefresh } from "@tabler/icons-react";

/**
 * 共通 Error 状態。
 *
 *   <ErrorState error={query.error} onRetry={() => query.refetch()} />
 */
export function ErrorState({
  error,
  onRetry,
  title = "通信エラー",
}: {
  error: unknown;
  onRetry?: () => void;
  title?: string;
}) {
  const message = (error instanceof Error ? error.message : String(error)).slice(0, 200);
  const detail = error instanceof Error ? (error.stack ?? "") : JSON.stringify(error, null, 2);
  const [opened, { toggle }] = useDisclosure(false);

  return (
    <Alert
      color="red"
      variant="light"
      icon={<IconAlertTriangle size={18} />}
      title={title}
      radius="md"
    >
      <Stack gap="xs">
        <div>{message}</div>
        <Stack gap={6} align="flex-start">
          {onRetry && (
            <Button
              size="xs"
              variant="light"
              color="red"
              leftSection={<IconRefresh size={14} />}
              onClick={onRetry}
            >
              再試行
            </Button>
          )}
          {detail && (
            <Button size="xs" variant="subtle" color="red" onClick={toggle}>
              {opened ? "詳細を隠す" : "詳細"}
            </Button>
          )}
        </Stack>
        <Collapse in={opened}>
          <Code block style={{ maxHeight: 200, overflow: "auto", fontSize: 11 }}>
            {detail}
          </Code>
        </Collapse>
      </Stack>
    </Alert>
  );
}
