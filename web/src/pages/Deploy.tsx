import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Checkbox,
  Code,
  Group,
  Loader,
  Modal,
  NumberInput,
  ScrollArea,
  Stack,
  Switch,
  Table,
  Tabs,
  Text,
  TextInput,
  Textarea,
  Tooltip,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { modals } from "@mantine/modals";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { TFunction } from "i18next";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { DeployPath, DeployPathCreate, DeployRun, DeployTarget, DeployTargetCreate, deployApi } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";

function confirmDelete(label: string, onConfirm: () => void, t: TFunction) {
  modals.openConfirmModal({
    title: t("deploy.confirm_delete_title"),
    children: <Text size="sm">{t("deploy.confirm_delete_message", { label })}</Text>,
    labels: {
      confirm: t("deploy.confirm_delete_confirm"),
      cancel: t("deploy.confirm_delete_cancel"),
    },
    confirmProps: { color: "red" },
    onConfirm,
  });
}

const REFRESH_MS = 2000;

function fmtTime(iso: string | null): string {
  if (!iso) return "          ";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 19);
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function statusBadge(r: DeployRun) {
  if (r.finished_at === null) {
    return <Badge color="blue" leftSection={<Loader size={10} color="white" />}>running</Badge>;
  }
  if (r.success === true) return <Badge color="teal">success</Badge>;
  if (r.success === false) return <Badge color="red">fail (exit={r.exit_code})</Badge>;
  return <Badge color="gray">{String(r.success)}</Badge>;
}

function TargetEditor({
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
  const [label, setLabel] = useState(initial?.label ?? "");
  const [host, setHost] = useState(initial?.host ?? "");
  const [sshUser, setSshUser] = useState(initial?.ssh_user ?? "root");
  const [sshPort, setSshPort] = useState<number | string>(initial?.ssh_port ?? 22);
  const [enabled, setEnabled] = useState(initial?.enabled ?? true);
  const [notes, setNotes] = useState(initial?.notes ?? "");
  const { t } = useTranslation();

  return (
    <Stack gap="xs">
      <TextInput label={t("deploy.targets.label")} required value={label} onChange={(e) => setLabel(e.currentTarget.value)} placeholder="ai-gpu1" />
      <TextInput label="host (IP / DNS)" required value={host} onChange={(e) => setHost(e.currentTarget.value)} placeholder="10.10.50.23" />
      <Group grow>
        <TextInput label="SSH user" value={sshUser} onChange={(e) => setSshUser(e.currentTarget.value)} />
        <NumberInput label="SSH port" value={sshPort} onChange={setSshPort} min={1} max={65535} />
      </Group>
      <Textarea label={t("deploy.targets.notes")} value={notes ?? ""} onChange={(e) => setNotes(e.currentTarget.value)} autosize minRows={2} maxRows={4} />
      <Switch label={t("deploy.targets.enabled")} checked={enabled} onChange={(e) => setEnabled(e.currentTarget.checked)} />
      <Group justify="flex-end">
        <Button variant="default" onClick={onCancel}>{t("deploy.targets.cancel")}</Button>
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
          {t("deploy.targets.save")}
        </Button>
      </Group>
    </Stack>
  );
}

function ctrlBaseUrl(): string {
  if (typeof window !== "undefined") {
    return `${window.location.protocol}//${window.location.host}`;
  }
  return "";
}

function TargetsSection() {
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

  const targets = list.data ?? [];

  return (
    <Box>
      <Group justify="space-between" mb="xs">
        <Text fw={500}>{t("deploy.targets.section")} ({targets.length})</Text>
        <Group gap="xs">
          <Button size="xs" variant="default" onClick={pubkeyCtl.open}>
            {t("deploy.targets.pubkey")}
          </Button>
          <Button size="xs" onClick={addModalCtl.open}>
            {t("deploy.targets.add")}
          </Button>
        </Group>
      </Group>

      <Table withTableBorder withColumnBorders striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 70 }}>{t("workloads.enabled")}</Table.Th>
            <Table.Th>{t("deploy.targets.col_label")}</Table.Th>
            <Table.Th>{t("deploy.targets.col_host_port")}</Table.Th>
            <Table.Th>{t("deploy.targets.col_user")}</Table.Th>
            <Table.Th>{t("deploy.targets.col_last")}</Table.Th>
            <Table.Th>{t("deploy.targets.col_notes")}</Table.Th>
            <Table.Th style={{ width: 110 }}>{t("deploy.targets.col_action")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {targets.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={7}>
                <Text c="dimmed" ta="center">{t("deploy.targets.empty")}</Text>
              </Table.Td>
            </Table.Tr>
          )}
          {targets.map((row) => (
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
                    {row.last_deploy_ok === true && <Badge size="xs" color="teal">{t("deploy.targets.last_ok")}</Badge>}
                    {row.last_deploy_ok === false && <Badge size="xs" color="red">{t("deploy.targets.last_ng")}</Badge>}
                  </Group>
                ) : (
                  <Text size="xs" c="dimmed">{t("deploy.targets.last_not_run")}</Text>
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
                    onClick={() => {
                      setEditing(row);
                      editorCtl.open();
                    }}
                  >
                    {t("deploy.targets.edit")}
                  </Button>
                  <Tooltip label={t("deploy.targets.delete")}>
                    <ActionIcon
                      size="sm"
                      color="red"
                      variant="subtle"
                      onClick={() => confirmDelete(row.label, () => del.mutate(row.id), t)}
                    >
                      ×
                    </ActionIcon>
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
        title={t("deploy.targets.edit_title", { label: editing?.label ?? "" })}
        size="md"
      >
        <TargetEditor
          initial={editing}
          saving={update.isPending}
          onCancel={editorCtl.close}
          onSave={(body) => {
            if (editing) update.mutate({ id: editing.id, body });
          }}
        />
      </Modal>

      <Modal
        opened={addModalOpened}
        onClose={addModalCtl.close}
        title={t("deploy.targets.add_title")}
        size="lg"
      >
        <Tabs defaultValue="bootstrap">
          <Tabs.List>
            <Tabs.Tab value="bootstrap">{t("deploy.targets.tab_bootstrap")}</Tabs.Tab>
            <Tabs.Tab value="manual">{t("deploy.targets.tab_manual")}</Tabs.Tab>
          </Tabs.List>
          <Tabs.Panel value="bootstrap" pt="md">
            <Stack gap="xs">
              <Text size="sm">{t("deploy.targets.bootstrap_help")}</Text>
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
                  {bootstrapCopied ? t("deploy.targets.bootstrap_copied") : t("deploy.targets.bootstrap_copy")}
                </Button>
              </Group>
              <Text size="xs" c="dimmed">{t("deploy.targets.bootstrap_what")}</Text>
              <Text size="xs" c="dimmed">{t("deploy.targets.bootstrap_prereq", { url: ctrlBaseUrl() })}</Text>
            </Stack>
          </Tabs.Panel>
          <Tabs.Panel value="manual" pt="md">
            <Text size="sm" c="dimmed" mb="xs">{t("deploy.targets.manual_help")}</Text>
            <TargetEditor
              saving={create.isPending}
              onCancel={addModalCtl.close}
              onSave={(body) => {
                create.mutate(body, {
                  onSuccess: () => addModalCtl.close(),
                });
              }}
            />
          </Tabs.Panel>
        </Tabs>
      </Modal>

      <Modal
        opened={pubkeyOpened}
        onClose={pubkeyCtl.close}
        title={t("deploy.targets.pubkey_title")}
        size="lg"
      >
        <Stack gap="xs">
          <Text size="sm">{t("deploy.targets.pubkey_help", { path: "/root/.ssh/authorized_keys" })}</Text>
          {pubkey.data?.pubkey ? (
            <>
              <Code block style={{ wordBreak: "break-all", whiteSpace: "pre-wrap" }}>
                {pubkey.data.pubkey}
              </Code>
              <Text size="xs" c="dimmed">source: {pubkey.data.source}</Text>
              <Box mt="md">
                <Text size="xs" fw={500} mb={4}>{t("deploy.targets.pubkey_how_label")}</Text>
                <Code block>{`echo '${pubkey.data.pubkey}' | ssh root@<host> 'mkdir -p /root/.ssh && cat >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys'`}</Code>
              </Box>
            </>
          ) : (
            <Text c="red">{pubkey.data?.hint ?? t("deploy.targets.pubkey_error")}</Text>
          )}
        </Stack>
      </Modal>
    </Box>
  );
}

function PathEditor({
  initial,
  onSave,
  onCancel,
  saving,
}: {
  initial?: DeployPath;
  onSave: (body: DeployPathCreate) => void;
  onCancel: () => void;
  saving: boolean;
}) {
  const [label, setLabel] = useState(initial?.label ?? "");
  const [src, setSrc] = useState(initial?.src_path ?? "");
  const [dst, setDst] = useState(initial?.dst_path ?? "");
  const [setupCmd, setSetupCmd] = useState(initial?.setup_command ?? "");
  const [serviceCmd, setServiceCmd] = useState(initial?.service_command ?? "");
  const [enabled, setEnabled] = useState(initial?.enabled ?? true);
  const [deleteMode, setDeleteMode] = useState(initial?.delete_mode ?? false);
  const [notes, setNotes] = useState(initial?.notes ?? "");
  const { t } = useTranslation();

  return (
    <Stack gap="xs">
      <TextInput label={t("deploy.paths.label")} required value={label} onChange={(e) => setLabel(e.currentTarget.value)} placeholder="embed_writer plugin" />
      <TextInput
        label={t("deploy.paths.src")}
        required
        value={src}
        onChange={(e) => setSrc(e.currentTarget.value)}
        placeholder="/opt/pipeline/plugins/embed_writer"
      />
      <TextInput
        label={t("deploy.paths.dst")}
        required
        value={dst}
        onChange={(e) => setDst(e.currentTarget.value)}
        placeholder="/opt/pipeline/plugins/embed_writer"
      />
      <Textarea
        label={t("deploy.paths.setup_cmd")}
        value={setupCmd ?? ""}
        onChange={(e) => setSetupCmd(e.currentTarget.value)}
        autosize
        minRows={2}
        maxRows={6}
        placeholder="pip install -r requirements.txt"
        description={t("deploy.paths.setup_cmd_desc")}
      />
      <Textarea
        label={t("deploy.paths.service_cmd")}
        value={serviceCmd ?? ""}
        onChange={(e) => setServiceCmd(e.currentTarget.value)}
        autosize
        minRows={1}
        maxRows={3}
        placeholder="/usr/bin/python3 main.py --port 8080"
        description={t("deploy.paths.service_cmd_desc")}
      />
      <Group>
        <Switch label={t("deploy.paths.enabled")} checked={enabled} onChange={(e) => setEnabled(e.currentTarget.checked)} />
        <Switch label={t("deploy.paths.delete_mode")} checked={deleteMode} onChange={(e) => setDeleteMode(e.currentTarget.checked)} />
      </Group>
      <Textarea label={t("deploy.paths.notes")} value={notes ?? ""} onChange={(e) => setNotes(e.currentTarget.value)} autosize minRows={1} maxRows={3} />
      <Group justify="flex-end">
        <Button variant="default" onClick={onCancel}>{t("deploy.paths.cancel")}</Button>
        <Button
          loading={saving}
          disabled={!label || !src || !dst}
          onClick={() =>
            onSave({
              label, src_path: src, dst_path: dst,
              enabled, delete_mode: deleteMode,
              setup_command: setupCmd || null,
              service_command: serviceCmd || null,
              notes: notes || null,
            })
          }
        >
          {t("deploy.paths.save")}
        </Button>
      </Group>
    </Stack>
  );
}

function PathsSection() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ["deploy-paths"],
    queryFn: () => deployApi.listPaths(),
    refetchInterval: 10_000,
  });
  const [editorOpened, editorCtl] = useDisclosure(false);
  const [editing, setEditing] = useState<DeployPath | undefined>(undefined);

  const create = useMutation({
    mutationFn: (body: DeployPathCreate) => deployApi.createPath(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["deploy-paths"] });
      editorCtl.close();
    },
  });
  const update = useMutation({
    mutationFn: ({ id, body }: { id: number; body: Partial<DeployPathCreate> }) =>
      deployApi.updatePath(id, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["deploy-paths"] });
      editorCtl.close();
    },
  });
  const del = useMutation({
    mutationFn: (id: number) => deployApi.deletePath(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["deploy-paths"] }),
  });
  const toggle = useMutation({
    mutationFn: ({ id, enabled }: { id: number; enabled: boolean }) =>
      deployApi.updatePath(id, { enabled }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["deploy-paths"] }),
  });

  const paths = list.data ?? [];

  return (
    <Box>
      <Group justify="space-between" mb="xs">
        <Text fw={500}>{t("deploy.paths.section")} ({paths.length})</Text>
        <Button
          size="xs"
          onClick={() => { setEditing(undefined); editorCtl.open(); }}
        >
          {t("deploy.paths.add")}
        </Button>
      </Group>

      <Table withTableBorder withColumnBorders striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 60 }}>{t("deploy.paths.col_enabled")}</Table.Th>
            <Table.Th>{t("deploy.paths.col_label")}</Table.Th>
            <Table.Th>{t("deploy.paths.col_src_dst")}</Table.Th>
            <Table.Th>setup / service</Table.Th>
            <Table.Th>{t("deploy.paths.col_last")}</Table.Th>
            <Table.Th style={{ width: 110 }}>{t("deploy.paths.col_action")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {paths.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={6}>
                <Text c="dimmed" ta="center">{t("deploy.paths.empty")}</Text>
              </Table.Td>
            </Table.Tr>
          )}
          {paths.map((p) => (
            <Table.Tr key={p.id}>
              <Table.Td>
                <Switch
                  checked={p.enabled}
                  onChange={(e) => toggle.mutate({ id: p.id, enabled: e.currentTarget.checked })}
                  size="xs"
                />
              </Table.Td>
              <Table.Td>{p.label}{p.delete_mode && <Badge size="xs" ml={4} color="orange">--delete</Badge>}</Table.Td>
              <Table.Td>
                <Code style={{ fontSize: 11 }}>{p.src_path}</Code>
                <Text size="xs" c="dimmed">→ <Code style={{ fontSize: 11 }}>{p.dst_path}</Code></Text>
              </Table.Td>
              <Table.Td>
                {p.setup_command && (
                  <Text size="xs" c="dimmed" lineClamp={2} title={p.setup_command}>
                    setup: <Code style={{ fontSize: 10 }}>{p.setup_command.split("\n")[0].slice(0, 50)}</Code>
                  </Text>
                )}
                {p.service_command && (
                  <Text size="xs" c="dimmed" lineClamp={2} title={p.service_command}>
                    svc: <Code style={{ fontSize: 10 }}>{p.service_command.slice(0, 50)}</Code>
                  </Text>
                )}
              </Table.Td>
              <Table.Td>
                {p.last_synced_at ? (
                  <Group gap={4}>
                    <Text size="xs">{p.last_synced_at.slice(11, 19)}</Text>
                    {p.last_synced_ok === true && <Badge size="xs" color="teal">{t("deploy.targets.last_ok")}</Badge>}
                    {p.last_synced_ok === false && <Badge size="xs" color="red">{t("deploy.targets.last_ng")}</Badge>}
                  </Group>
                ) : <Text size="xs" c="dimmed">{t("deploy.paths.last_not_deployed")}</Text>}
              </Table.Td>
              <Table.Td>
                <Group gap={4}>
                  <Button size="xs" variant="light" onClick={() => { setEditing(p); editorCtl.open(); }}>
                    {t("deploy.paths.edit")}
                  </Button>
                  <ActionIcon
                    size="sm" color="red" variant="subtle"
                    onClick={() => confirmDelete(p.label, () => del.mutate(p.id), t)}
                  >×</ActionIcon>
                </Group>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>

      <Modal
        opened={editorOpened}
        onClose={editorCtl.close}
        title={editing ? t("deploy.paths.edit_title", { label: editing.label }) : t("deploy.paths.add_title")}
        size="lg"
      >
        <PathEditor
          initial={editing}
          saving={create.isPending || update.isPending}
          onCancel={editorCtl.close}
          onSave={(body) => {
            if (editing) update.mutate({ id: editing.id, body });
            else create.mutate(body);
          }}
        />
      </Modal>
    </Box>
  );
}

export default function Deploy() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [skipRestart, setSkipRestart] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [openedRun, setOpenedRun] = useState<DeployRun | null>(null);
  const [logModalOpened, logModalCtl] = useDisclosure(false);

  const list = useQuery({
    queryKey: ["deploys"],
    queryFn: () => deployApi.list(),
    refetchInterval: REFRESH_MS,
  });

  const detail = useQuery({
    queryKey: ["deploy-detail", openedRun?.id],
    queryFn: () => deployApi.get(openedRun!.id),
    enabled: openedRun !== null && logModalOpened,
    // running 中は 500ms 間隔で polling (= リアルタイム性 up)、 完了後は止まる
    refetchInterval: (q) => (q.state.data?.finished_at ? false : 500),
  });
  const logViewportRef = useRef<HTMLDivElement | null>(null);
  // log が更新されたら末尾までスクロール (= リアルタイムフィード追従)
  // 2 段 RAF で layout 計算 → paint 後にスクロール (= 1 段だと <pre> render 前で scrollHeight が古い)
  useEffect(() => {
    const v = logViewportRef.current;
    if (!v) return;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (logViewportRef.current) {
          logViewportRef.current.scrollTo({ top: logViewportRef.current.scrollHeight });
        }
      });
    });
  }, [openedRun?.log, openedRun?.finished_at, logModalOpened]);

  const trigger = useMutation({
    mutationFn: () =>
      deployApi.trigger({
        // hosts 省略 → DB の enabled=1 を使う
        skip_restart: skipRestart,
        dry_run: dryRun,
      }),
    onSuccess: (r) => {
      qc.invalidateQueries({ queryKey: ["deploys"] });
      setOpenedRun(r);
      logModalCtl.open();
    },
  });

  const runs = list.data ?? [];
  const runningCount = runs.filter((r) => r.finished_at === null).length;

  useEffect(() => {
    if (detail.data) setOpenedRun(detail.data);
  }, [detail.data]);

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("deploy.title")}
        subtitle={t("deploy.subtitle", { url: ctrlBaseUrl() })}
      />

      <Box style={{
        padding: 12,
        borderRadius: 8,
        background: "color-mix(in srgb, var(--mantine-color-indigo-6) 6%, transparent)",
        border: "1px solid color-mix(in srgb, var(--mantine-color-indigo-6) 18%, transparent)",
      }}>
        <Text size="sm" c="dimmed" component="div">
          <strong>① {t("deploy.intro_hosts")}</strong> — {t("deploy.intro_hosts_desc")} <br/>
          <strong>② {t("deploy.intro_paths")}</strong> — {t("deploy.intro_paths_desc")} <br/>
          <strong>③ {t("deploy.intro_button")}</strong> — {t("deploy.intro_button_desc")}
        </Text>
      </Box>

      <TargetsSection />

      <PathsSection />

      <Box style={{ border: "1px solid var(--mantine-color-default-border)", padding: 12, borderRadius: 8 }}>
        <Stack gap="xs">
          <Text fw={500}>{t("deploy.exec_section")}</Text>
          <Group>
            <Checkbox
              label={t("deploy.exec_skip_restart")}
              checked={skipRestart}
              onChange={(e) => setSkipRestart(e.currentTarget.checked)}
              size="xs"
            />
            <Checkbox
              label={t("deploy.exec_dry_run")}
              checked={dryRun}
              onChange={(e) => setDryRun(e.currentTarget.checked)}
              size="xs"
            />
          </Group>
          <Group>
            <Button
              onClick={() => trigger.mutate()}
              loading={trigger.isPending}
              disabled={runningCount > 0}
            >
              {runningCount > 0 ? t("deploy.exec_running") : t("deploy.exec_button")}
            </Button>
            {trigger.error instanceof Error && (
              <Text size="xs" c="red">{trigger.error.message}</Text>
            )}
          </Group>
        </Stack>
      </Box>

      <Box>
        <Group justify="space-between" mb="xs">
          <Text fw={500}>{t("deploy.history_section")}</Text>
          {list.isFetching && <Loader size="xs" />}
        </Group>
        <Table withTableBorder withColumnBorders striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>id</Table.Th>
              <Table.Th>started</Table.Th>
              <Table.Th>duration</Table.Th>
              <Table.Th>status</Table.Th>
              <Table.Th>hosts</Table.Th>
              <Table.Th>flags</Table.Th>
              <Table.Th>action</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {runs.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={7}>
                  <Text c="dimmed" ta="center">{t("deploy.history_empty")}</Text>
                </Table.Td>
              </Table.Tr>
            )}
            {runs.map((r) => (
              <Table.Tr key={r.id}>
                <Table.Td><Code>{r.id}</Code></Table.Td>
                <Table.Td>{fmtTime(r.started_at)}</Table.Td>
                <Table.Td>{r.duration_s != null ? `${r.duration_s}s` : "—"}</Table.Td>
                <Table.Td>{statusBadge(r)}</Table.Td>
                <Table.Td>
                  <Text size="xs">{r.hosts.join(", ")}</Text>
                </Table.Td>
                <Table.Td>
                  {r.dry_run && <Badge size="xs" color="yellow" mr={4}>dry</Badge>}
                  {r.skip_restart && <Badge size="xs" color="gray">no-restart</Badge>}
                </Table.Td>
                <Table.Td>
                  <Button
                    size="xs"
                    variant="light"
                    onClick={() => {
                      setOpenedRun(r);
                      logModalCtl.open();
                    }}
                  >
                    log
                  </Button>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Box>

      <Modal
        opened={logModalOpened}
        onClose={logModalCtl.close}
        title={openedRun ? `${openedRun.id} — ${openedRun.finished_at ? "finished" : "running"}` : ""}
        size="xl"
      >
        {openedRun && (
          <Stack gap="xs">
            <Group>
              {statusBadge(openedRun)}
              {openedRun.duration_s != null && <Text size="xs">{openedRun.duration_s}s</Text>}
              <Text size="xs" c="dimmed">hosts: {openedRun.hosts.join(", ")}</Text>
            </Group>
            <Box
              style={{
                background: "#1a1b1e",
                color: "#e9ecef",
                fontFamily: "ui-monospace, Menlo, Consolas, monospace",
                fontSize: 12,
                borderRadius: 6,
                padding: 8,
                border: "1px solid #2c2e33",
              }}
            >
              <ScrollArea
                h="calc(100vh - 320px)"
                viewportRef={logViewportRef}
                type="always"
                scrollbarSize={10}
                offsetScrollbars
              >
                <pre style={{ margin: 0, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                  {openedRun.log || "(empty)"}
                </pre>
              </ScrollArea>
            </Box>
          </Stack>
        )}
      </Modal>
    </Stack>
  );
}
