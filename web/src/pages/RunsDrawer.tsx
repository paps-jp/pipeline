import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Code,
  Collapse,
  Drawer,
  Group,
  Loader,
  ScrollArea,
  Stack,
  Table,
  Text,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { IconChevronDown, IconChevronRight, IconRefresh } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { RunRecord, Workload, api } from "@/api/client";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

interface Props {
  opened: boolean;
  onClose: () => void;
  workload: Workload | null;
}

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function fmtMs(ms: number | null): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

function ResultBadge({ run, t }: { run: RunRecord; t: (k: string) => string }) {
  if (run.success === true) {
    return <Badge color="teal" variant="light">{t("workloads.runs.success")}</Badge>;
  }
  if (run.success === false) {
    return <Badge color="red" variant="light">{t("workloads.runs.failure")}</Badge>;
  }
  return <Badge color="gray" variant="light">—</Badge>;
}

/** stderr / error から最初の意味ある「原因」一行を抽出 (Python traceback の最終行など)。 */
function shortReason(run: RunRecord): string | null {
  const candidates = [run.error, run.stderr].filter(Boolean) as string[];
  for (const text of candidates) {
    const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
    // Python の "Error: …" / "Exception: …" を末尾から探す
    for (let i = lines.length - 1; i >= 0; i--) {
      const m = lines[i].match(/^([A-Z][A-Za-z0-9_.]*(?:Error|Exception|Failed|Failure)):?\s*(.*)$/);
      if (m) return m[2] ? `${m[1]}: ${m[2]}` : m[1];
    }
    if (lines.length > 0) return lines[lines.length - 1].slice(0, 200);
  }
  return null;
}

function RunRow({
  run,
  t,
  slug,
}: {
  run: RunRecord;
  t: (k: string) => string;
  slug: string;
}) {
  const [opened, setOpened] = useState(false);
  const qc = useQueryClient();
  const isFailed = run.success === false;
  const hasDetail = !!(run.stdout || run.stderr || run.error);

  const rerunMut = useMutation({
    mutationFn: () => api.enqueueTask(slug, run.pk, {}),
    onSuccess: (r) => {
      notifications.show({
        color: r.inserted > 0 ? "teal" : "yellow",
        title: r.inserted > 0 ? "再投入しました" : "既にキューに有り",
        message: `pk=${run.pk}`,
      });
      qc.invalidateQueries({ queryKey: ["queue", slug] });
      qc.invalidateQueries({ queryKey: ["runs", slug] });
    },
    onError: (e: Error) =>
      notifications.show({ color: "red", title: "再投入失敗", message: e.message }),
  });

  const reason = shortReason(run);

  return (
    <>
      <Table.Tr
        style={{ cursor: hasDetail ? "pointer" : "default" }}
        onClick={() => hasDetail && setOpened((v) => !v)}
      >
        <Table.Td style={{ width: 24 }}>
          {hasDetail ? (
            opened ? <IconChevronDown size={14} /> : <IconChevronRight size={14} />
          ) : null}
        </Table.Td>
        <Table.Td><ResultBadge run={run} t={t} /></Table.Td>
        <Table.Td><Code>{run.pk}</Code></Table.Td>
        <Table.Td>{run.attempt}</Table.Td>
        <Table.Td>{run.exit_code ?? "—"}</Table.Td>
        <Table.Td>{fmtMs(run.duration_ms)}</Table.Td>
        <Table.Td>
          <Tooltip label={run.started_at}>
            <Text size="xs">{fmtTime(run.started_at)}</Text>
          </Tooltip>
        </Table.Td>
        <Table.Td style={{ maxWidth: 280 }}>
          {isFailed && reason ? (
            <Text size="xs" c="red.7" truncate>{reason}</Text>
          ) : (
            <Text size="xs" c="dimmed" truncate>
              {(run.stdout || "—").split(/\r?\n/)[0]?.slice(0, 80) || "—"}
            </Text>
          )}
        </Table.Td>
        <Table.Td onClick={(e) => e.stopPropagation()} style={{ width: 60 }}>
          <Tooltip label="同じ pk を再投入">
            <ActionIcon
              size="sm"
              variant="subtle"
              color="indigo"
              loading={rerunMut.isPending}
              onClick={() => rerunMut.mutate()}
            >
              <IconRefresh size={14} />
            </ActionIcon>
          </Tooltip>
        </Table.Td>
      </Table.Tr>
      {hasDetail && (
        <Table.Tr style={{ background: "transparent" }}>
          <Table.Td colSpan={9} style={{ padding: 0, border: 0 }}>
            <Collapse in={opened}>
              <Box
                p="sm"
                style={{
                  background: "color-mix(in srgb, var(--mantine-color-default-border) 25%, transparent)",
                  borderLeft: "3px solid var(--mantine-color-indigo-5)",
                }}
              >
                <Stack gap="xs">
                  {run.error && (
                    <Section label="error">{run.error}</Section>
                  )}
                  {run.stderr && (
                    <Section label="stderr">{run.stderr}</Section>
                  )}
                  {run.stdout && (
                    <Section label="stdout">{run.stdout}</Section>
                  )}
                  {run.output_json && Object.keys(run.output_json).length > 0 && (
                    <Section label="output_json">
                      {JSON.stringify(run.output_json, null, 2)}
                    </Section>
                  )}
                  <Group gap="xs">
                    <Button
                      size="xs"
                      variant="light"
                      leftSection={<IconRefresh size={14} />}
                      onClick={() => rerunMut.mutate()}
                      loading={rerunMut.isPending}
                    >
                      この pk を再投入
                    </Button>
                    <Text size="xs" c="dimmed">
                      run_id: <Code>{run.id}</Code> · worker: <Code>{run.worker_id || "—"}</Code>
                    </Text>
                  </Group>
                </Stack>
              </Box>
            </Collapse>
          </Table.Td>
        </Table.Tr>
      )}
    </>
  );
}

function Section({ label, children }: { label: string; children: string }) {
  return (
    <Stack gap={2}>
      <Text size="xs" fw={600} c="dimmed" tt="uppercase">{label}</Text>
      <Code
        block
        style={{
          maxHeight: 220,
          overflow: "auto",
          fontSize: 11,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {children}
      </Code>
    </Stack>
  );
}

export default function RunsDrawer({ opened, onClose, workload }: Props) {
  const { t } = useTranslation();
  const slug = workload?.slug ?? "";

  const queueQ = useQuery({
    queryKey: ["queue", slug],
    queryFn: () => api.getQueueStats(slug),
    enabled: opened && !!slug,
    refetchInterval: opened ? 3_000 : false,
  });

  const runsQ = useQuery({
    queryKey: ["runs", slug],
    queryFn: () => api.listRuns(slug, 50),
    enabled: opened && !!slug,
    refetchInterval: opened ? 3_000 : false,
  });

  if (!workload) return null;

  return (
    <Drawer
      opened={opened}
      onClose={onClose}
      title={
        <Stack gap={2}>
          <Text fw={600}>{t("workloads.runs.title", { slug: workload.slug })}</Text>
          <Text size="xs" c="dimmed">
            {workload.name}
          </Text>
        </Stack>
      }
      position="right"
      size="xl"
      padding="md"
    >
      <Stack>
        <Group gap="xs" wrap="wrap">
          <Text fw={600} size="sm">{t("workloads.runs.queue_state")}:</Text>
          {queueQ.isLoading && <Loader size="xs" />}
          {queueQ.data && Object.keys(queueQ.data.by_state).length === 0 && (
            <Badge variant="default">{t("workloads.runs.queue_empty")}</Badge>
          )}
          {queueQ.data &&
            Object.entries(queueQ.data.by_state).map(([state, n]) => (
              <Badge
                key={state}
                variant="light"
                color={state === "pending" ? "blue" : state === "claimed" ? "yellow" : state === "failed" ? "red" : "gray"}
              >
                {state}: {n}
              </Badge>
            ))}
        </Group>

        {runsQ.isLoading && <TableSkeleton rows={6} cols={7} />}
        {runsQ.error && <ErrorState error={runsQ.error} onRetry={() => runsQ.refetch()} />}

        {runsQ.data && runsQ.data.runs.length === 0 && !runsQ.isLoading && (
          <EmptyState
            title={t("workloads.runs.empty")}
            description="まだ実行履歴がありません。タスクを投入すると ここに表示されます。"
          />
        )}

        {runsQ.data && runsQ.data.runs.length > 0 && (
          <ScrollArea h={520}>
            <Table verticalSpacing="xs" striped withTableBorder highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th></Table.Th>
                  <Table.Th>{t("workloads.runs.result")}</Table.Th>
                  <Table.Th>{t("workloads.runs.pk")}</Table.Th>
                  <Table.Th>{t("workloads.runs.attempt")}</Table.Th>
                  <Table.Th>{t("workloads.runs.exit")}</Table.Th>
                  <Table.Th>{t("workloads.runs.duration")}</Table.Th>
                  <Table.Th>{t("workloads.runs.started_at")}</Table.Th>
                  <Table.Th>{t("workloads.runs.output")}</Table.Th>
                  <Table.Th></Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {runsQ.data.runs.map((r) => (
                  <RunRow key={r.id} run={r} t={t} slug={slug} />
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea>
        )}
      </Stack>
    </Drawer>
  );
}
