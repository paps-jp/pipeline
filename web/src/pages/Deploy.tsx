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
import { useEffect, useRef, useState } from "react";

import { DeployPath, DeployPathCreate, DeployRun, DeployTarget, DeployTargetCreate, deployApi } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";

function confirmDelete(label: string, onConfirm: () => void) {
  modals.openConfirmModal({
    title: "削除しますか?",
    children: <Text size="sm">「{label}」 を削除します。元に戻せません。</Text>,
    labels: { confirm: "削除", cancel: "キャンセル" },
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

  return (
    <Stack gap="xs">
      <TextInput label="ラベル" required value={label} onChange={(e) => setLabel(e.currentTarget.value)} placeholder="ai-gpu1" />
      <TextInput label="host (IP / DNS)" required value={host} onChange={(e) => setHost(e.currentTarget.value)} placeholder="10.10.50.23" />
      <Group grow>
        <TextInput label="SSH user" value={sshUser} onChange={(e) => setSshUser(e.currentTarget.value)} />
        <NumberInput label="SSH port" value={sshPort} onChange={setSshPort} min={1} max={65535} />
      </Group>
      <Textarea label="メモ" value={notes ?? ""} onChange={(e) => setNotes(e.currentTarget.value)} autosize minRows={2} maxRows={4} />
      <Switch label="enabled (= deploy 対象)" checked={enabled} onChange={(e) => setEnabled(e.currentTarget.checked)} />
      <Group justify="flex-end">
        <Button variant="default" onClick={onCancel}>キャンセル</Button>
        <Button
          color="violet"
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
          保存
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
  const [addModalOpened, addModalCtl] = useDisclosure(false);  // 新規追加 modal (= bootstrap or 手動)
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
        <Text fw={500}>配信先ホスト ({targets.length})</Text>
        <Group gap="xs">
          <Button size="xs" variant="default" onClick={pubkeyCtl.open}>
            公開鍵を表示
          </Button>
          <Button
            size="xs"
            color="violet"
            onClick={addModalCtl.open}
          >
            + 配信先を追加
          </Button>
        </Group>
      </Group>

      <Table withTableBorder withColumnBorders striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 70 }}>enabled</Table.Th>
            <Table.Th>ラベル</Table.Th>
            <Table.Th>host:port</Table.Th>
            <Table.Th>user</Table.Th>
            <Table.Th>最終 deploy</Table.Th>
            <Table.Th>メモ</Table.Th>
            <Table.Th style={{ width: 110 }}>action</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {targets.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={7}>
                <Text c="dimmed" ta="center">配信先未登録。 「+ 配信先を追加」 で登録してください</Text>
              </Table.Td>
            </Table.Tr>
          )}
          {targets.map((t) => (
            <Table.Tr key={t.id}>
              <Table.Td>
                <Switch
                  checked={t.enabled}
                  onChange={(e) => toggleEnabled.mutate({ id: t.id, enabled: e.currentTarget.checked })}
                  size="xs"
                />
              </Table.Td>
              <Table.Td>{t.label}</Table.Td>
              <Table.Td><Code>{t.host}:{t.ssh_port}</Code></Table.Td>
              <Table.Td>{t.ssh_user}</Table.Td>
              <Table.Td>
                {t.last_deploy_at ? (
                  <Group gap={4}>
                    <Text size="xs">{fmtTime(t.last_deploy_at)}</Text>
                    {t.last_deploy_ok === true && <Badge size="xs" color="teal">ok</Badge>}
                    {t.last_deploy_ok === false && <Badge size="xs" color="red">ng</Badge>}
                  </Group>
                ) : (
                  <Text size="xs" c="dimmed">未実行</Text>
                )}
              </Table.Td>
              <Table.Td>
                <Text size="xs" c="dimmed">{t.notes ?? ""}</Text>
              </Table.Td>
              <Table.Td>
                <Group gap={4}>
                  <Button
                    size="xs"
                    variant="light"
                    onClick={() => {
                      setEditing(t);
                      editorCtl.open();
                    }}
                  >
                    編集
                  </Button>
                  <Tooltip label="削除">
                    <ActionIcon
                      size="sm"
                      color="red"
                      variant="subtle"
                      onClick={() => confirmDelete(t.label, () => del.mutate(t.id))}
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

      {/* 編集 modal (= 既存 host の編集) */}
      <Modal
        opened={editorOpened}
        onClose={editorCtl.close}
        title={`配信先を編集 (${editing?.label ?? ""})`}
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

      {/* 新規追加 modal (= bootstrap or 手動入力) */}
      <Modal
        opened={addModalOpened}
        onClose={addModalCtl.close}
        title="新規ホストを追加"
        size="lg"
      >
        <Tabs defaultValue="bootstrap">
          <Tabs.List>
            <Tabs.Tab value="bootstrap">自動 (推奨: bootstrap)</Tabs.Tab>
            <Tabs.Tab value="manual">手動入力</Tabs.Tab>
          </Tabs.List>
          <Tabs.Panel value="bootstrap" pt="md">
            <Stack gap="xs">
              <Text size="sm">
                新規ホストの <Code>root</Code> シェルで <strong>下記の 1 行</strong> を実行してください。
                自動で install + 起動 + 配信先一覧に追加されます (= 既存ホストで再実行しても冪等)。
              </Text>
              <Group gap="xs" align="flex-start">
                <Code style={{ flex: 1, padding: 10, fontSize: 12, wordBreak: "break-all" }}>
                  {`curl -sSL ${ctrlBaseUrl()}/bootstrap.sh | sudo bash`}
                </Code>
                <Button
                  size="xs"
                  variant={bootstrapCopied ? "filled" : "light"}
                  color={bootstrapCopied ? "teal" : "violet"}
                  onClick={() => {
                    navigator.clipboard.writeText(`curl -sSL ${ctrlBaseUrl()}/bootstrap.sh | sudo bash`);
                    setBootstrapCopied(true);
                    setTimeout(() => setBootstrapCopied(false), 1500);
                  }}
                >
                  {bootstrapCopied ? "✓ コピー済" : "📋 コピー"}
                </Button>
              </Group>
              <Text size="xs" c="dimmed">
                実行内容: apt deps install / venv 構築 / pipeline source 取得 / systemd unit 配置 + start /
                公開鍵登録 / Pipeline に「私入った」 と join 通知。
              </Text>
              <Text size="xs" c="dimmed">
                前提: 対象ホストが OS + nvidia driver + CUDA + (NFS マウント等) 設定済の Linux マシンで、 Pipeline ({ctrlBaseUrl()}) に HTTP/SSH で疎通すること。
              </Text>
            </Stack>
          </Tabs.Panel>
          <Tabs.Panel value="manual" pt="md">
            <Text size="sm" c="dimmed" mb="xs">
              手動で IP/ポート/ユーザを登録します (= 既に service が手動 install 済 や 別環境からの登録時)。
            </Text>
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
        title="配信元 SSH 公開鍵 (Pipeline)"
        size="lg"
      >
        <Stack gap="xs">
          <Text size="sm">
            新しい配信先ホストを追加したら、 その host の <Code>/root/.ssh/authorized_keys</Code> にこの公開鍵を追加してください:
          </Text>
          {pubkey.data?.pubkey ? (
            <>
              <Code block style={{ wordBreak: "break-all", whiteSpace: "pre-wrap" }}>
                {pubkey.data.pubkey}
              </Code>
              <Text size="xs" c="dimmed">source: {pubkey.data.source}</Text>
              <Box mt="md">
                <Text size="xs" fw={500} mb={4}>追加方法 (ホストで 1 回実行):</Text>
                <Code block>{`echo '${pubkey.data.pubkey}' | ssh root@<host> 'mkdir -p /root/.ssh && cat >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys'`}</Code>
              </Box>
            </>
          ) : (
            <Text c="red">{pubkey.data?.hint ?? "公開鍵取得失敗"}</Text>
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

  return (
    <Stack gap="xs">
      <TextInput label="ラベル" required value={label} onChange={(e) => setLabel(e.currentTarget.value)} placeholder="embed_writer plugin" />
      <TextInput
        label="配信元 (Pipeline 上の絶対 path)"
        required
        value={src}
        onChange={(e) => setSrc(e.currentTarget.value)}
        placeholder="/opt/pipeline/plugins/embed_writer"
      />
      <TextInput
        label="配信先 (ホストの絶対 path)"
        required
        value={dst}
        onChange={(e) => setDst(e.currentTarget.value)}
        placeholder="/opt/pipeline/plugins/embed_writer"
      />
      <Textarea
        label="配信後実行 (= 配信直後 dst で 1 回、 例: pip install)"
        value={setupCmd ?? ""}
        onChange={(e) => setSetupCmd(e.currentTarget.value)}
        autosize
        minRows={2}
        maxRows={6}
        placeholder="pip install -r requirements.txt"
        description="複数行可。 dst ディレクトリで上から順次実行。 空なら skip"
      />
      <Textarea
        label="実行 (= service として常駐、 systemd unit 自動生成)"
        value={serviceCmd ?? ""}
        onChange={(e) => setServiceCmd(e.currentTarget.value)}
        autosize
        minRows={1}
        maxRows={3}
        placeholder="/usr/bin/python3 main.py --port 8080"
        description="ExecStart になる 1 行コマンド。 空なら systemd unit 生成しない"
      />
      <Group>
        <Switch label="enabled (= deploy 対象)" checked={enabled} onChange={(e) => setEnabled(e.currentTarget.checked)} />
        <Switch label="--delete (= dst の余分を削除)" checked={deleteMode} onChange={(e) => setDeleteMode(e.currentTarget.checked)} />
      </Group>
      <Textarea label="メモ" value={notes ?? ""} onChange={(e) => setNotes(e.currentTarget.value)} autosize minRows={1} maxRows={3} />
      <Group justify="flex-end">
        <Button variant="default" onClick={onCancel}>キャンセル</Button>
        <Button
          color="violet"
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
          保存
        </Button>
      </Group>
    </Stack>
  );
}

function PathsSection() {
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
        <Text fw={500}>配信パス ({paths.length})</Text>
        <Button
          size="xs"
          color="violet"
          onClick={() => { setEditing(undefined); editorCtl.open(); }}
        >
          + 配信パスを追加
        </Button>
      </Group>

      <Table withTableBorder withColumnBorders striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 60 }}>enabled</Table.Th>
            <Table.Th>ラベル</Table.Th>
            <Table.Th>配信元 → 配信先</Table.Th>
            <Table.Th>setup / service</Table.Th>
            <Table.Th>最終</Table.Th>
            <Table.Th style={{ width: 110 }}>action</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {paths.length === 0 && (
            <Table.Tr>
              <Table.Td colSpan={6}>
                <Text c="dimmed" ta="center">配信パス未登録。 「+ 配信パスを追加」 で登録してください</Text>
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
                    {p.last_synced_ok === true && <Badge size="xs" color="teal">ok</Badge>}
                    {p.last_synced_ok === false && <Badge size="xs" color="red">ng</Badge>}
                  </Group>
                ) : <Text size="xs" c="dimmed">未配信</Text>}
              </Table.Td>
              <Table.Td>
                <Group gap={4}>
                  <Button size="xs" variant="light" onClick={() => { setEditing(p); editorCtl.open(); }}>
                    編集
                  </Button>
                  <ActionIcon
                    size="sm" color="red" variant="subtle"
                    onClick={() => confirmDelete(p.label, () => del.mutate(p.id))}
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
        title={editing ? `配信パスを編集 (${editing.label})` : "配信パスを追加"}
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

// BootstrapSection は TargetsSection の「+ 配信先を追加」 modal 内に統合済 (= 上部から削除)

export default function Deploy() {
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
        title="Deploy"
        subtitle={`Pipeline (${ctrlBaseUrl()}) から登録ホストへ ファイル/コードを配信`}
      />

      <Box style={{
        padding: 12,
        borderRadius: 8,
        background: "color-mix(in srgb, var(--mantine-color-indigo-6) 6%, transparent)",
        border: "1px solid color-mix(in srgb, var(--mantine-color-indigo-6) 18%, transparent)",
      }}>
        <Text size="sm" c="dimmed" component="div">
          <strong>① 配信先ホスト</strong> = どこへ送るか (= IP/ポート/SSH ユーザ)。 <br/>
          <strong>② 配信パス</strong> = 何を どこへ (= 配信元path / 配信先path / 配信後実行コマンド / service として常駐コマンド)。 <br/>
          <strong>③ Deploy now</strong> ボタン = 上の 2 つを掛け合わせて 一括配信 + service restart。
        </Text>
      </Box>

      <TargetsSection />

      <PathsSection />

      <Box style={{ border: "1px solid var(--mantine-color-default-border)", padding: 12, borderRadius: 8 }}>
        <Stack gap="xs">
          <Text fw={500}>Deploy 実行</Text>
          <Group>
            <Checkbox
              label="skip restart (rsync のみ)"
              checked={skipRestart}
              onChange={(e) => setSkipRestart(e.currentTarget.checked)}
              size="xs"
            />
            <Checkbox
              label="dry run (rsync --dry-run)"
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
              color="violet"
            >
              {runningCount > 0 ? "deploy 走行中…" : "Deploy now"}
            </Button>
            {trigger.error instanceof Error && (
              <Text size="xs" c="red">{trigger.error.message}</Text>
            )}
          </Group>
        </Stack>
      </Box>

      <Box>
        <Group justify="space-between" mb="xs">
          <Text fw={500}>直近 deploy 履歴</Text>
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
                  <Text c="dimmed" ta="center">まだ deploy 履歴がありません</Text>
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
