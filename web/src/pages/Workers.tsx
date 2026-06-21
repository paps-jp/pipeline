import { Badge, Code, Loader, Stack, Table, Text, Tooltip } from "@mantine/core";
import { IconUsersGroup } from "@tabler/icons-react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { WorkerInfo, api } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function fmtRelative(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return iso;
  const now = Date.now();
  const diff = (now - d.getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function StateBadge({ state }: { state: string }) {
  const color =
    state === "active" ? "green" :
    state === "draining" ? "yellow" :
    state === "lost" ? "red" :
    state === "connecting" ? "blue" : "gray";
  return <Badge color={color} variant="light">{state}</Badge>;
}

export default function Workers() {
  const { t } = useTranslation();

  const q = useQuery({
    queryKey: ["workers"],
    queryFn: () => api.listWorkers(),
    refetchInterval: 3_000,
  });

  const workers: WorkerInfo[] = q.data?.workers ?? [];

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("workers.title")}
        right={
          <>
            {q.isLoading && <Loader size="sm" />}
            <Badge color="green" variant="light">
              active: {workers.filter((w) => w.state === "active").length}
            </Badge>
            <Badge color="gray" variant="light">total: {workers.length}</Badge>
          </>
        }
      />

      {q.error && <ErrorState error={q.error} onRetry={() => q.refetch()} />}

      {q.isLoading && workers.length === 0 && <TableSkeleton rows={4} cols={8} />}

      {workers.length === 0 && !q.isLoading && !q.error && (
        <EmptyState
          icon={IconUsersGroup}
          title={t("workers.empty")}
          description="bootstrap スクリプトで GPU 箱を追加してください (Deploy → 配信先ホスト)。"
        />
      )}

      {workers.length > 0 && (
        <Table striped highlightOnHover withTableBorder>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("workers.id")}</Table.Th>
              <Table.Th>{t("workers.host")}</Table.Th>
              <Table.Th>{t("workers.state")}</Table.Th>
              <Table.Th>{t("workers.current_workload")}</Table.Th>
              <Table.Th>{t("workers.processed")}</Table.Th>
              <Table.Th>{t("workers.errors")}</Table.Th>
              <Table.Th>{t("workers.last_seen")}</Table.Th>
              <Table.Th>{t("workers.started_at")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {workers.map((w) => (
              <Table.Tr key={w.id}>
                <Table.Td><Code>{w.id}</Code></Table.Td>
                <Table.Td>{w.host}</Table.Td>
                <Table.Td><StateBadge state={w.state} /></Table.Td>
                <Table.Td>
                  {w.current_workload ? (
                    <Code>{w.current_workload}</Code>
                  ) : (
                    <Text size="sm" c="dimmed">—</Text>
                  )}
                </Table.Td>
                <Table.Td>{w.rows_processed}</Table.Td>
                <Table.Td>{w.errors_total > 0 ? <Text c="red">{w.errors_total}</Text> : w.errors_total}</Table.Td>
                <Table.Td>
                  <Tooltip label={fmtTime(w.last_seen_at)}>
                    <Text size="sm">{fmtRelative(w.last_seen_at)}</Text>
                  </Tooltip>
                </Table.Td>
                <Table.Td>
                  <Tooltip label={fmtTime(w.started_at)}>
                    <Text size="sm">{fmtRelative(w.started_at)}</Text>
                  </Tooltip>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
