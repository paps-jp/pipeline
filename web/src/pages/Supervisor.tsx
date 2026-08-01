/**
 * Supervisor — pipeline-supervisor プラグインの全設定 (init_kwargs) をサブシステム別に編集する。
 *
 * 設定は workload の executor_config.init_kwargs に入っており、 従来は Workloads の
 * 編集モーダルからしか触れず、 かつ plugin.yaml に宣言済みのキーしか出せなかった
 * (2026-08-01 時点で 131 キー中 49 キーのみ)。 ここでは manifest の group ごとに畳んで
 * 全項目を出し、 変更点だけを差分表示してから保存する。
 */

import { useEffect, useMemo, useState } from "react";
import {
  Accordion,
  Alert,
  Badge,
  Button,
  Code,
  Group,
  Loader,
  NumberInput,
  Paper,
  ScrollArea,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  IconAlertTriangle,
  IconCheck,
  IconRotate,
  IconSearch,
  IconSettingsAutomation,
} from "@tabler/icons-react";

import {
  api,
  type AvailablePlugin,
  type PluginKwargField,
  type Workload,
} from "@/api/client";

const SLUG = "pipeline-supervisor";
const PLUGIN_SLUG = "pipeline_supervisor";
const UNGROUPED = "その他";

/** apply_mode 系 (= 実適用 / dry-run の切替) は誤操作の影響が大きいので強調する */
function isApplySwitch(key: string): boolean {
  return key === "apply_mode" || key.endsWith("_apply_mode");
}

/** 0/1 で表現された bool (このプラグインの既存慣習) を Switch で出すか判定 */
function isBoolLike(f: PluginKwargField): boolean {
  return (
    f.type === "bool" ||
    (f.type === "int" &&
      (f.key.endsWith("_enabled") || isApplySwitch(f.key) || f.key.endsWith("_kill_switch")) &&
      (f.min ?? 0) === 0 &&
      (f.max ?? 1) === 1)
  );
}

function sameValue(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a == null && b == null;
  return String(a) === String(b);
}

export default function SupervisorPage() {
  const qc = useQueryClient();
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [search, setSearch] = useState("");

  const workloadQ = useQuery({
    queryKey: ["workload", SLUG],
    queryFn: () => api.getWorkload(SLUG),
  });
  const pluginsQ = useQuery({
    queryKey: ["plugins-available"],
    queryFn: () => api.listAvailablePlugins(),
  });

  const workload: Workload | undefined = workloadQ.data;
  const plugin: AvailablePlugin | undefined = pluginsQ.data?.plugins.find(
    (p) => p.slug === PLUGIN_SLUG,
  );
  const fields: PluginKwargField[] = plugin?.manifest?.init_kwargs ?? [];

  /** control plane に保存されている現在値 (未設定なら manifest の default) */
  const saved = useMemo(() => {
    const cfg = (workload?.executor_config ?? {}) as Record<string, unknown>;
    const kw = (cfg.init_kwargs ?? {}) as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const f of fields) {
      out[f.key] = f.key in kw ? kw[f.key] : (f.default ?? null);
    }
    return out;
  }, [workload, fields]);

  useEffect(() => {
    if (fields.length > 0) setValues(saved);
  }, [saved, fields.length]);

  const changed = useMemo(
    () => fields.filter((f) => !sameValue(values[f.key], saved[f.key])),
    [fields, values, saved],
  );

  const save = useMutation({
    mutationFn: async () => {
      if (!workload) throw new Error("workload 未取得");
      const cfg = {
        ...((workload.executor_config ?? {}) as Record<string, unknown>),
      };
      cfg.init_kwargs = {
        ...((cfg.init_kwargs ?? {}) as Record<string, unknown>),
        ...values,
      };
      return api.updateWorkload(SLUG, {
        name: workload.name,
        description: workload.description,
        enabled: workload.enabled,
        executor_type: workload.executor_type,
        executor_config: cfg,
        success_criteria: workload.success_criteria,
        priority: workload.priority,
        weight: workload.weight,
        batch_size: workload.batch_size,
        lease_secs: workload.lease_secs,
        max_attempts: workload.max_attempts,
        resources: workload.resources,
        host_affinity: workload.host_affinity,
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["workload", SLUG] }),
  });

  const groups = useMemo(() => {
    const q = search.trim().toLowerCase();
    const map = new Map<string, PluginKwargField[]>();
    for (const f of fields) {
      if (
        q &&
        !f.key.toLowerCase().includes(q) &&
        !(f.label ?? "").toLowerCase().includes(q) &&
        !(f.help ?? "").toLowerCase().includes(q)
      ) {
        continue;
      }
      const g = f.group || UNGROUPED;
      if (!map.has(g)) map.set(g, []);
      map.get(g)!.push(f);
    }
    return [...map.entries()];
  }, [fields, search]);

  const renderField = (f: PluginKwargField) => {
    const v = values[f.key];
    const dirty = !sameValue(v, saved[f.key]);
    const label = (
      <Group gap={6} wrap="nowrap">
        <Text size="sm" fw={dirty ? 700 : 400}>
          {f.label || f.key}
        </Text>
        {dirty && (
          <Badge size="xs" color="yellow">
            変更
          </Badge>
        )}
        {isApplySwitch(f.key) && (
          <Badge size="xs" color="red" variant="light">
            実適用
          </Badge>
        )}
      </Group>
    );
    const description = (
      <Text span size="xs" c="dimmed">
        <Code fz="xs">{f.key}</Code>
        {f.help ? ` — ${f.help}` : ""}
      </Text>
    );
    const set = (val: unknown) => setValues((p) => ({ ...p, [f.key]: val }));

    if (isBoolLike(f)) {
      const on = f.type === "bool" ? Boolean(v) : Number(v) === 1;
      return (
        <Switch
          key={f.key}
          label={label}
          description={description}
          checked={on}
          color={isApplySwitch(f.key) ? "red" : undefined}
          onChange={(e) =>
            set(f.type === "bool" ? e.currentTarget.checked : e.currentTarget.checked ? 1 : 0)
          }
        />
      );
    }
    if (f.type === "enum" && f.options) {
      return (
        <Select
          key={f.key}
          label={label}
          description={description}
          value={v == null ? null : String(v)}
          data={f.options.map((o) => String(o))}
          onChange={(val) => set(val)}
        />
      );
    }
    if (f.type === "int" || f.type === "float") {
      return (
        <NumberInput
          key={f.key}
          label={label}
          description={description}
          value={typeof v === "number" ? v : v == null || v === "" ? "" : Number(v)}
          min={f.min}
          max={f.max}
          allowDecimal={f.type === "float"}
          decimalScale={f.type === "float" ? 4 : 0}
          onChange={(val) => set(val === "" ? null : Number(val))}
        />
      );
    }
    return (
      <TextInput
        key={f.key}
        label={label}
        description={description}
        value={v == null ? "" : String(v)}
        onChange={(e) => set(e.currentTarget.value)}
      />
    );
  };

  if (workloadQ.isLoading || pluginsQ.isLoading) return <Loader />;
  if (workloadQ.isError) {
    return (
      <Alert color="red" title="supervisor workload を取得できません">
        {String(workloadQ.error)}
      </Alert>
    );
  }
  if (!plugin?.manifest) {
    return (
      <Alert color="orange" title="plugin manifest が読めません">
        control plane の <Code>PIPELINE_PLUGIN_ROOT</Code> 配下に{" "}
        <Code>{PLUGIN_SLUG}/plugin.yaml</Code> が必要です。
      </Alert>
    );
  }

  const applyOn = Number(values["apply_mode"]) === 1;

  return (
    <Stack>
      <Group justify="space-between" align="flex-end">
        <div>
          <Title order={3}>
            <Group gap={8}>
              <IconSettingsAutomation size={22} />
              Supervisor 設定
            </Group>
          </Title>
          <Text size="sm" c="dimmed">
            {fields.length} 項目 / {groups.length} グループ
          </Text>
        </div>
        <Group>
          <Button
            variant="default"
            leftSection={<IconRotate size={16} />}
            disabled={changed.length === 0}
            onClick={() => setValues(saved)}
          >
            変更を破棄
          </Button>
          <Button
            leftSection={<IconCheck size={16} />}
            loading={save.isPending}
            disabled={changed.length === 0}
            onClick={() => save.mutate()}
          >
            保存 ({changed.length})
          </Button>
        </Group>
      </Group>

      {!applyOn && (
        <Alert color="blue" title="dry-run 中" icon={<IconAlertTriangle size={16} />}>
          <Code>apply_mode = 0</Code> のため、 supervisor は判断をログに出すだけで実際の変更を
          行いません。 各サブシステムの <Code>*_apply_mode</Code> も同様です。
        </Alert>
      )}

      {save.isError && (
        <Alert color="red" title="保存に失敗しました">
          {String(save.error)}
        </Alert>
      )}
      {save.isSuccess && changed.length === 0 && (
        <Alert color="green" title="保存しました">
          次の tick (既定 30 秒) から反映されます。 worker の再起動は不要です。
        </Alert>
      )}

      {changed.length > 0 && (
        <Paper withBorder p="sm">
          <Text size="sm" fw={600} mb={4}>
            未保存の変更 {changed.length} 件
          </Text>
          <ScrollArea.Autosize mah={200}>
            <Table fz="xs" withRowBorders={false}>
              <Table.Tbody>
                {changed.map((f) => (
                  <Table.Tr key={f.key}>
                    <Table.Td>
                      <Code fz="xs">{f.key}</Code>
                    </Table.Td>
                    <Table.Td c="dimmed">{String(saved[f.key] ?? "")}</Table.Td>
                    <Table.Td>→</Table.Td>
                    <Table.Td fw={600}>{String(values[f.key] ?? "")}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea.Autosize>
        </Paper>
      )}

      <TextInput
        placeholder="キー / ラベル / 説明で絞り込み"
        leftSection={<IconSearch size={16} />}
        value={search}
        onChange={(e) => setSearch(e.currentTarget.value)}
      />

      {groups.length === 0 ? (
        <Text c="dimmed" size="sm">
          該当する設定はありません。
        </Text>
      ) : (
        <Accordion multiple defaultValue={groups.map(([g]) => g)} variant="separated">
          {groups.map(([g, fs]) => {
            const nDirty = fs.filter((f) => !sameValue(values[f.key], saved[f.key])).length;
            return (
              <Accordion.Item key={g} value={g}>
                <Accordion.Control>
                  <Group gap={8}>
                    <Text fw={600}>{g}</Text>
                    <Badge size="sm" variant="light">
                      {fs.length}
                    </Badge>
                    {nDirty > 0 && (
                      <Tooltip label="未保存の変更">
                        <Badge size="sm" color="yellow">
                          {nDirty}
                        </Badge>
                      </Tooltip>
                    )}
                  </Group>
                </Accordion.Control>
                <Accordion.Panel>
                  <Stack gap="sm">{fs.map(renderField)}</Stack>
                </Accordion.Panel>
              </Accordion.Item>
            );
          })}
        </Accordion>
      )}
    </Stack>
  );
}
