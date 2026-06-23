import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Code,
  Group,
  Indicator,
  Loader,
  Modal,
  NumberInput,
  Stack,
  Switch,
  Table,
  Tabs,
  Text,
  TextInput,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { modals } from "@mantine/modals";
import { IconUsersGroup } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { TFunction } from "i18next";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import {
  DeployTarget,
  DeployTargetCreate,
  WorkerInfo,
  api,
  deployApi,
} from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

// ============================================================
// 共通 helpers
// ============================================================

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
  const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function ctrlBaseUrl(): string {
  if (typeof window !== "undefined") {
    return `${window.location.protocol}//${window.location.host}`;
  }
  return "";
}

function confirmDeleteWorker(label: string, onConfirm: () => void, t: TFunction) {
  modals.openConfirmModal({
    title: t("workers.registry.confirm_delete_title"),
    children: <Text size="sm">{t("workers.registry.confirm_delete_message", { label })}</Text>,
    labels: {
      confirm: t("workers.registry.delete"),
      cancel: t("workers.registry.cancel"),
    },
    confirmProps: { color: "red" },
    onConfirm,
  });
}

// ============================================================
// 登録ワーカー: 編集フォーム (= 旧 TargetEditor)
// ============================================================

function WorkerEditor({
  initial,
  onSave,
  onCancel,
  saving,
}: {
  initial?: DeployTarget;
  onSave: (body: DeployTargetCreate) => void;
  onCancel: () => void;
  saving: boolean;
}) {
  const { t } = useTranslation();
  const [label, setLabel] = useState(initial?.label ?? "");
  const [host, setHost] = useState(initial?.host ?? "");
  const [sshUser, setSshUser] = useState(initial?.ssh_user ?? "root");
  const [sshPort, setSshPort] = useState<number | string>(initial?.ssh_port ?? 22);
  const [enabled, setEnabled] = useState(initial?.enabled ?? true);
  const [notes, setNotes] = useState(initial?.notes ?? "");

  return (
    <Stack gap="xs">
      <TextInput label={t("workers.registry.label")} required value={label} onChange={(e) => setLabel(e.currentTarget.value)} placeholder="ai-gpu1" />
      <TextInput label="host (IP / DNS)" required value={host} onChange={(e) => setHost(e.currentTarget.value)} placeholder="10.10.50.23" />
      <Group grow>
        <TextInput label="SSH user" value={sshUser} onChange={(e) => setSshUser(e.currentTarget.value)} />
        <NumberInput label="SSH port" value={sshPort} onChange={setSshPort} min={1} max={65535} />
      </Group>
      <Textarea label={t("workers.registry.notes")} value={notes ?? ""} onChange={(e) => setNotes(e.currentTarget.value)} autosize minRows={2} maxRows={4} />
      <Switch label={t("workers.registry.enabled")} checked={enabled} onChange={(e) => setEnabled(e.currentTarget.checked)} />
      <Group justify="flex-end">
        <Button variant="default" onClick={onCancel}>{t("workers.registry.cancel")}</Button>
        <Button
          loading={saving}
          disabled={!label || !host}
          onClick={() =>
            onSave({
              label, host,
              ssh_user: sshUser, ssh_port: Number(sshPort),
              enabled, notes: notes || null,
            })
          }
        >
          {t("workers.registry.save")}
        </Button>
      </Group>
    </Stack>
  );
}

// ============================================================
// 登録ワーカー: 一覧 + 追加/編集/削除 + bootstrap modal + 公開鍵
// ============================================================

function WorkerRegistrySection() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ["deploy-targets"],
    queryFn: () => deployApi.listTargets(),
    refetchInterval: 10_000,
  });
  const pubkey = useQuery({
    queryKey: ["deploy-pubkey"],
    queryFn: () => deployApi.getPubkey(),
  });
  const [editorOpened, editorCtl] = useDisclosure(false);
  const [addModalOpened, addModalCtl] = useDisclosure(false);
  const [editing, setEditing] = useState<DeployTarget | undefined>(undefined);
  const [pubkeyOpened, pubkeyCtl] = useDisclosure(false);
  const [bootstrapCopied, setBootstrapCopied] = useState(false);

  const create = useMutation({
    mutationFn: (body: DeployTargetCreate) => deployApi.createTarget(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["deploy-targets"] });
      editorCtl.close();
    },
  });
  const update = useMutation({
    mutationFn: ({ id, body }: { id: number; body: Partial<DeployTargetCreate> }) =>
      deployApi.updateTarget(id, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["deploy-targets"] });
      editorCtl.close();
    },
  });
  const del = useMutation({
    mutationFn: (id: number) => deployApi.deleteTarget(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["deploy-targets"] }),
  });
  const toggleEnabled = useMutation({
    mutationFn: ({ id, enabled }: { id: number; enabled: boolean }) =>
      deployApi.updateTarget(id, { enabled }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["deploy-targets"] }),
  });

  const rows = list.data ?? [];

  return (
    <Box>
      <Group justify="space-between" mb="xs">
        <Title order={4}>{t("workers.section_registry")} ({rows.length})</Title>
        <Group gap="xs">
          <Button size="xs" variant="default" onClick={pubkeyCtl.open}>
            {t("workers.registry.pubkey")}
          </Button>
          <Button size="xs" onClick={addModalCtl.open}>
            {t("workers.registry.add")}
          </Button>
        </Group>
      </Group>

      <Table withTableBorder withColumnBorders striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 70 }}>{t("workloads.enabled")}</Table.Th>
            <Table.Th>{t("workers.registry.col_label")}</Table.Th>
            <Table.Th>{t("workers.registry.col_host_port")}</Table.Th>
            <Table.Th>{t("workers.registry.col_user")}</Table.Th>
            <Table.Th>{t("workers.registry.col_last")}</Table.Th>
            <Table.Th>{t("workers.registry.col_notes")}</Table.Th>
            <Table.Th style={{ width: 110 }}>{t("workers.registry.col_action")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {rows.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={7}>
                <Text c="dimmed" ta="center">{t("workers.registry.empty")}</Text>
              </Table.Td>
            </Table.Tr>
          )}
          {rows.map((row) => (
            <Table.Tr key={row.id}>
              <Table.Td>
                <Switch
                  checked={row.enabled}
                  onChange={(e) => toggleEnabled.mutate({ id: row.id, enabled: e.currentTarget.checked })}
                  size="xs"
                />
              </Table.Td>
              <Table.Td>{row.label}</Table.Td>
              <Table.Td><Code>{row.host}:{row.ssh_port}</Code></Table.Td>
              <Table.Td>{row.ssh_user}</Table.Td>
              <Table.Td>
                {row.last_deploy_at ? (
                  <Group gap={4}>
                    <Text size="xs">{fmtTime(row.last_deploy_at)}</Text>
                    {row.last_deploy_ok === true && <Badge size="xs" color="teal">{t("workers.registry.last_ok")}</Badge>}
                    {row.last_deploy_ok === false && <Badge size="xs" color="red">{t("workers.registry.last_ng")}</Badge>}
                  </Group>
                ) : (
                  <Text size="xs" c="dimmed">{t("workers.registry.last_not_run")}</Text>
                )}
              </Table.Td>
              <Table.Td>
                <Text size="xs" c="dimmed">{row.notes ?? ""}</Text>
              </Table.Td>
              <Table.Td>
                <Group gap={4}>
                  <Button
                    size="xs"
                    variant="light"
                    onClick={() => { setEditing(row); editorCtl.open(); }}
                  >
                    {t("workers.registry.edit")}
                  </Button>
                  <Tooltip label={t("workers.registry.delete")}>
                    <ActionIcon
                      size="sm" color="red" variant="subtle"
                      onClick={() => confirmDeleteWorker(row.label, () => del.mutate(row.id), t)}
                    >×</ActionIcon>
                  </Tooltip>
                </Group>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>

      <Modal
        opened={editorOpened}
        onClose={editorCtl.close}
        title={t("workers.registry.edit_title", { label: editing?.label ?? "" })}
        size="md"
      >
        <WorkerEditor
          initial={editing}
          saving={update.isPending}
          onCancel={editorCtl.close}
          onSave={(body) => { if (editing) update.mutate({ id: editing.id, body }); }}
        />
      </Modal>

      <Modal
        opened={addModalOpened}
        onClose={addModalCtl.close}
        title={t("workers.registry.add_title")}
        size="lg"
      >
        <Tabs defaultValue="bootstrap">
          <Tabs.List>
            <Tabs.Tab value="bootstrap">{t("workers.registry.tab_bootstrap")}</Tabs.Tab>
            <Tabs.Tab value="manual">{t("workers.registry.tab_manual")}</Tabs.Tab>
          </Tabs.List>
          <Tabs.Panel value="bootstrap" pt="md">
            <Stack gap="xs">
              <Text size="sm">{t("workers.registry.bootstrap_help")}</Text>
              <Group gap="xs" align="flex-start">
                <Code style={{ flex: 1, padding: 10, fontSize: 12, wordBreak: "break-all" }}>
                  {`curl -sSL ${ctrlBaseUrl()}/bootstrap.sh | sudo bash`}
                </Code>
                <Button
                  size="xs"
                  variant={bootstrapCopied ? "filled" : "light"}
                  color={bootstrapCopied ? "teal" : undefined}
                  onClick={() => {
                    navigator.clipboard.writeText(`curl -sSL ${ctrlBaseUrl()}/bootstrap.sh | sudo bash`);
                    setBootstrapCopied(true);
                    setTimeout(() => setBootstrapCopied(false), 1500);
                  }}
                >
                  {bootstrapCopied ? t("workers.registry.bootstrap_copied") : t("workers.registry.bootstrap_copy")}
                </Button>
              </Group>
              <Text size="xs" c="dimmed">{t("workers.registry.bootstrap_what")}</Text>
              <Text size="xs" c="dimmed">{t("workers.registry.bootstrap_prereq", { url: ctrlBaseUrl() })}</Text>
            </Stack>
          </Tabs.Panel>
          <Tabs.Panel value="manual" pt="md">
            <Text size="sm" c="dimmed" mb="xs">{t("workers.registry.manual_help")}</Text>
            <WorkerEditor
              saving={create.isPending}
              onCancel={addModalCtl.close}
              onSave={(body) => {
                create.mutate(body, { onSuccess: () => addModalCtl.close() });
              }}
            />
          </Tabs.Panel>
        </Tabs>
      </Modal>

      <Modal
        opened={pubkeyOpened}
        onClose={pubkeyCtl.close}
        title={t("workers.registry.pubkey_title")}
        size="lg"
      >
        <Stack gap="xs">
          <Text size="sm">{t("workers.registry.pubkey_help", { path: "/root/.ssh/authorized_keys" })}</Text>
          {pubkey.data?.pubkey ? (
            <>
              <Code block style={{ wordBreak: "break-all", whiteSpace: "pre-wrap" }}>
                {pubkey.data.pubkey}
              </Code>
              <Text size="xs" c="dimmed">source: {pubkey.data.source}</Text>
              <Box mt="md">
                <Text size="xs" fw={500} mb={4}>{t("workers.registry.pubkey_how_label")}</Text>
                <Code block>{`echo '${pubkey.data.pubkey}' | ssh root@<host> 'mkdir -p /root/.ssh && cat >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys'`}</Code>
              </Box>
            </>
          ) : (
            <Text c="red">{pubkey.data?.hint ?? t("workers.registry.pubkey_error")}</Text>
          )}
        </Stack>
      </Modal>
    </Box>
  );
}

// ============================================================
// 稼働状態: 既存の runtime workers 表
// ============================================================

function StateBadge({ state }: { state: string }) {
  const color =
    state === "active" ? "green" :
    state === "draining" ? "yellow" :
    state === "lost" ? "red" :
    state === "connecting" ? "blue" : "gray";
  // active は脈打つ green dot (= ダッシュボード「実行中」と同じ Indicator processing) + ラベル。
  // 他状態は静的な色付き Badge。
  if (state === "active") {
    return (
      <Group gap={8} wrap="nowrap" align="center">
        <Indicator processing size={10} color="green" offset={0} position="middle-center">
          <span style={{ display: "inline-block", width: 1, height: 12 }} />
        </Indicator>
        <Text size="xs" fw={600} c="green.7" style={{ letterSpacing: 0.3 }}>active</Text>
      </Group>
    );
  }
  return <Badge color={color} variant="light">{state}</Badge>;
}

function RuntimeSection() {
  const { t } = useTranslation();
  const q = useQuery({
    queryKey: ["workers"],
    queryFn: () => api.listWorkers(),
    refetchInterval: 3_000,
  });
  // workload slug → friendly name のマップ (= dashboard と同じ趣旨)
  const workloadsQ = useQuery({
    queryKey: ["workloads-name-map"],
    queryFn: () => api.listWorkloads(),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
  const workloadNameMap = new Map(
    (workloadsQ.data?.workloads ?? []).map((w) => [w.slug, w.name]),
  );
  const rawWorkers: WorkerInfo[] = q.data?.workers ?? [];
  // 並び替え: active を先、 lost は末尾。 ヘッダ件数は「現在生きてる」 数を出す
  const activeCount = rawWorkers.filter((w) => w.state === "active").length;
  const lostCount = rawWorkers.filter((w) => w.state === "lost").length;
  const stateOrder: Record<string, number> = {
    active: 0, connecting: 1, draining: 2, lost: 3,
  };
  const workers = [...rawWorkers].sort((a, b) => {
    const da = stateOrder[a.state] ?? 9;
    const db_ = stateOrder[b.state] ?? 9;
    if (da !== db_) return da - db_;
    return (b.last_seen_at ?? "").localeCompare(a.last_seen_at ?? "");
  });

  return (
    <Box>
      <Group justify="space-between" mb="xs">
        <Title order={4}>
          {t("workers.section_runtime")} ({activeCount})
        </Title>
        <Group gap="xs">
          {q.isLoading && <Loader size="xs" />}
          <Badge color="green" variant="light">active: {activeCount}</Badge>
          {lostCount > 0 && (
            <Badge color="gray" variant="light">lost: {lostCount}</Badge>
          )}
        </Group>
      </Group>

      {q.error && <ErrorState error={q.error} onRetry={() => q.refetch()} />}
      {q.isLoading && workers.length === 0 && <TableSkeleton rows={3} cols={8} />}

      {workers.length === 0 && !q.isLoading && !q.error && (
        <EmptyState
          icon={IconUsersGroup}
          title={t("workers.empty")}
          description="登録ワーカーで pipeline-worker.service が稼働すると ここに heartbeat が表示されます。"
          minHeight={140}
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
              <Table.Tr key={w.id} style={w.state === "lost" ? { opacity: 0.5 } : undefined}>
                <Table.Td><Code>{w.id}</Code></Table.Td>
                <Table.Td>{w.host}</Table.Td>
                <Table.Td><StateBadge state={w.state} /></Table.Td>
                <Table.Td>
                  {w.current_workload ? (
                    <Tooltip label={w.current_workload}>
                      <Badge color="indigo" variant="light" size="sm">
                        {workloadNameMap.get(w.current_workload) ?? w.current_workload}
                      </Badge>
                    </Tooltip>
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
    </Box>
  );
}

// ============================================================
// page entry
// ============================================================

export default function Workers() {
  const { t } = useTranslation();
  return (
    <Stack gap="lg">
      <PageHeader title={t("workers.title")} subtitle={t("workers.subtitle")} />
      <WorkerRegistrySection />
      <RuntimeSection />
    </Stack>
  );
}
