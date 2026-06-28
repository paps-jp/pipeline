import {
  ActionIcon,
  Badge,
  Button,
  Code,
  Group,
  Stack,
  Switch,
  Table,
  Text,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { modals } from "@mantine/modals";
import { notifications } from "@mantine/notifications";
import { IconHistory, IconPencil, IconPlus, IconSend, IconTrash } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { api, Workload } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { RunsSparkline } from "@/components/RunsSparkline";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import RunsDrawer from "./RunsDrawer";
import TaskEnqueueModal from "./TaskEnqueueModal";
import WorkloadFormModal from "./WorkloadFormModal";

export default function Workloads() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams<{ slug?: string }>();
  const urlSlug = params.slug ?? null;
  const urlIsRuns = location.pathname.endsWith("/runs");

  const [modalOpened, modalCtl] = useDisclosure(false);
  const [enqueueOpened, enqueueCtl] = useDisclosure(false);
  const [drawerOpened, drawerCtl] = useDisclosure(false);
  const [editing, setEditing] = useState<Workload | undefined>(undefined);
  const [targetWorkload, setTargetWorkload] = useState<Workload | null>(null);

  const openCreate = () => {
    setEditing(undefined);
    modalCtl.open();
  };

  const openEdit = (w: Workload) => {
    navigate(`/workloads/${w.slug}`);
    setEditing(w);
    modalCtl.open();
  };

  const openEnqueue = (w: Workload) => {
    setTargetWorkload(w);
    enqueueCtl.open();
  };

  const openRuns = (w: Workload) => {
    navigate(`/workloads/${w.slug}/runs`);
    setTargetWorkload(w);
    drawerCtl.open();
  };

  const closeEdit = () => {
    modalCtl.close();
    if (urlSlug) navigate("/workloads", { replace: true });
  };
  const closeRuns = () => {
    drawerCtl.close();
    if (urlSlug) navigate("/workloads", { replace: true });
  };

  const { data, isLoading, error } = useQuery({
    queryKey: ["workloads"],
    queryFn: api.listWorkloads,
    refetchInterval: 10_000,
  });

  const summaryQ = useQuery({
    queryKey: ["workloads-runs-summary"],
    queryFn: api.workloadsRunsSummary,
    refetchInterval: 10_000,
  });
  const summaryMap = new Map((summaryQ.data ?? []).map((s) => [s.workload_slug, s]));

  // URL → drawer 自動オープン (= deep link & ブラウザ戻る進む対応)
  useEffect(() => {
    if (!urlSlug || !data) return;
    const w = data.workloads.find((x) => x.slug === urlSlug);
    if (!w) return;
    if (urlIsRuns) {
      setTargetWorkload(w);
      drawerCtl.open();
      modalCtl.close();
    } else {
      setEditing(w);
      modalCtl.open();
      drawerCtl.close();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlSlug, urlIsRuns, data]);

  const toggleMut = useMutation({
    mutationFn: ({ slug, enabled }: { slug: string; enabled: boolean }) =>
      api.setWorkloadEnabled(slug, enabled),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workloads"] }),
    onError: (e: Error) =>
      notifications.show({ color: "red", title: "Toggle failed", message: e.message }),
  });

  const deleteMut = useMutation({
    mutationFn: (slug: string) => api.deleteWorkload(slug),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workloads"] }),
    onError: (e: Error) =>
      notifications.show({ color: "red", title: "Delete failed", message: e.message }),
  });

  const askDelete = (w: Workload) => {
    modals.openConfirmModal({
      title: t("workloads.delete_confirm_title", "ワークロードを削除"),
      children: (
        <Text size="sm">
          <Code>{w.slug}</Code> を削除します。この操作は元に戻せません。
        </Text>
      ),
      labels: { confirm: "削除", cancel: "キャンセル" },
      confirmProps: { color: "red" },
      onConfirm: () => deleteMut.mutate(w.slug),
    });
  };

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("workloads.title")}
        right={
          <Button leftSection={<IconPlus size={16} />} onClick={openCreate}>
            {t("workloads.new")}
          </Button>
        }
      />

      <WorkloadFormModal opened={modalOpened} onClose={closeEdit} editing={editing} />
      <TaskEnqueueModal
        opened={enqueueOpened}
        onClose={enqueueCtl.close}
        workload={targetWorkload}
      />
      <RunsDrawer
        opened={drawerOpened}
        onClose={closeRuns}
        workload={targetWorkload}
      />

      {isLoading && <TableSkeleton rows={4} cols={9} />}
      {error && <ErrorState error={error} onRetry={() => qc.invalidateQueries({ queryKey: ["workloads"] })} />}

      {data && data.workloads.length === 0 && (
        <EmptyState
          title={t("workloads.empty")}
          description="「新規作成」 ボタンから ワークロードを追加してください。"
          action={{ label: t("workloads.new"), onClick: openCreate }}
        />
      )}

      {data && data.workloads.length > 0 && (
        <Table striped highlightOnHover withTableBorder>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("workloads.slug")}</Table.Th>
              <Table.Th>{t("workloads.name")}</Table.Th>
              <Table.Th>{t("workloads.executor")}</Table.Th>
              <Table.Th>直近 20</Table.Th>
              <Table.Th>{t("workloads.enabled")}</Table.Th>
              <Table.Th>{t("workloads.priority")}</Table.Th>
              <Table.Th>{t("workloads.weight")}</Table.Th>
              <Table.Th>{t("workloads.batch")}</Table.Th>
              <Table.Th>VRAM (declared / observed)</Table.Th>
              <Table.Th aria-label="actions" />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.workloads.map((w) => {
              const sum = summaryMap.get(w.slug);
              return (
              <Table.Tr key={w.slug}>
                <Table.Td>
                  <Code>{w.slug}</Code>
                </Table.Td>
                <Table.Td>{w.name}</Table.Td>
                <Table.Td>
                  <Badge variant="light" color="indigo">
                    {w.executor_type}
                  </Badge>
                </Table.Td>
                <Table.Td>
                  {sum ? (
                    <RunsSparkline bits={sum.bits} rate={sum.success_rate} />
                  ) : (
                    <Text size="xs" c="dimmed">—</Text>
                  )}
                </Table.Td>
                <Table.Td>
                  <Switch
                    checked={w.enabled}
                    onChange={(e) =>
                      toggleMut.mutate({ slug: w.slug, enabled: e.currentTarget.checked })
                    }
                    aria-label="toggle enabled"
                  />
                </Table.Td>
                <Table.Td>{w.priority}</Table.Td>
                <Table.Td>{w.weight.toFixed(2)}</Table.Td>
                <Table.Td>{w.batch_size}</Table.Td>
                <Table.Td>
                  {(() => {
                    const declared = (w.resources as Record<string, unknown> | undefined)?.vram_mb;
                    const declaredNum = typeof declared === "number" ? declared : Number(declared);
                    const observed = w.observed_vram_mb_peak;
                    const samples = w.observed_vram_sample_count ?? 0;
                    const fmt = (mb: number | null | undefined) =>
                      mb && mb > 0 ? `${(mb / 1024).toFixed(2)} GB` : "—";
                    // 観測値が宣言値を超えてたら警告色 (= OOM リスク)、 観測 << 宣言 なら情報色 (= 過大宣言)
                    const drift =
                      observed && declaredNum
                        ? observed / declaredNum
                        : null;
                    const color =
                      drift === null ? "dimmed" : drift > 1.0 ? "red" : drift < 0.6 ? "blue" : "dimmed";
                    return (
                      <Stack gap={0}>
                        <Text size="xs" c="dimmed">
                          decl {fmt(Number.isFinite(declaredNum) ? declaredNum : null)}
                        </Text>
                        <Text size="xs" c={color} fw={observed ? 600 : 400}>
                          obs {fmt(observed)}
                          {samples > 0 ? ` (n=${samples})` : ""}
                        </Text>
                      </Stack>
                    );
                  })()}
                </Table.Td>
                <Table.Td>
                  <Group gap={4} wrap="nowrap" justify="flex-end">
                    <ActionIcon
                      variant="subtle"
                      color="blue"
                      onClick={() => openEnqueue(w)}
                      aria-label="enqueue task"
                      title={t("workloads.enqueue.tooltip")}
                    >
                      <IconSend size={16} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      onClick={() => openRuns(w)}
                      aria-label="view runs"
                      title={t("workloads.runs.tooltip")}
                    >
                      <IconHistory size={16} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      onClick={() => openEdit(w)}
                      aria-label="edit workload"
                      title={t("workloads.edit.tooltip")}
                    >
                      <IconPencil size={16} />
                    </ActionIcon>
                    <ActionIcon
                      color="red"
                      variant="subtle"
                      onClick={() => askDelete(w)}
                      aria-label="delete workload"
                    >
                      <IconTrash size={16} />
                    </ActionIcon>
                  </Group>
                </Table.Td>
              </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
