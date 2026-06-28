import {
  Accordion,
  Button,
  Checkbox,
  Drawer,
  Group,
  JsonInput,
  NumberInput,
  PasswordInput,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { useForm } from "@mantine/form";
import { notifications } from "@mantine/notifications";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  ApiError,
  AvailablePlugin,
  PluginKwargField,
  Workload,
  WorkloadCreate,
  api,
} from "@/api/client";

// 実装済 executor type のみ (= 他は未実装 + UI も削除)
type ExecutorType = "shell" | "python_module";

const SLUG_RE = /^[a-z0-9][a-z0-9_-]{0,62}$/;

const EXECUTOR_PRESETS: Record<ExecutorType, Record<string, unknown>> = {
  shell: {
    command: ["echo", "task={task.pk}"],
    cwd: null,
    env: {},
    timeout_secs: 60,
  },
  python_module: {
    module: "my_pkg.my_worker",
    callable: "process",
    init_kwargs: {},
  },
};

interface FormValues {
  slug: string;
  name: string;
  description: string;
  executor_type: ExecutorType;
  executor_config: string;
  priority: number;
  weight: number;
  batch_size: number;
  lease_secs: number;
  max_attempts: number;
  // 1 worker instance あたり想定 VRAM (MB)。
  // install-multi-worker.sh の --auto-from-workloads がここを読んで N を算出する。
  // 0 / 未指定 = GPU 不要 (CPU only / dispatcher 系)。
  vram_mb: number;
}

interface Props {
  opened: boolean;
  onClose: () => void;
  /** 編集モード: 既存 workload を渡す。create モードは undefined */
  editing?: Workload;
}

function valuesForWorkload(w: Workload): FormValues {
  return {
    slug: w.slug,
    name: w.name,
    description: w.description ?? "",
    executor_type: w.executor_type as ExecutorType,
    executor_config: JSON.stringify(w.executor_config, null, 2),
    priority: w.priority,
    weight: w.weight,
    batch_size: w.batch_size,
    lease_secs: w.lease_secs,
    max_attempts: w.max_attempts,
    vram_mb: Number((w.resources as Record<string, unknown> | undefined)?.vram_mb ?? 0),
  };
}

const DEFAULT_VALUES: FormValues = {
  slug: "",
  name: "",
  description: "",
  executor_type: "shell",
  executor_config: JSON.stringify(EXECUTOR_PRESETS.shell, null, 2),
  priority: 50,
  weight: 1,
  batch_size: 16,
  lease_secs: 120,
  max_attempts: 3,
  vram_mb: 0,
};

export default function WorkloadFormModal({ opened, onClose, editing }: Props) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const isEdit = !!editing;
  // python_module の dropdown 選択: ローカル plugins/ から source_path + module
  const [sourcePath, setSourcePath] = useState<string | null>(null);
  const [pluginModule, setPluginModule] = useState<string | null>(null);
  // dynamic form: init_kwargs を manifest の type 別 input で編集
  const [initKwargs, setInitKwargs] = useState<Record<string, unknown>>({});

  const availablePluginsQ = useQuery({
    queryKey: ["plugins-available"],
    queryFn: () => api.listAvailablePlugins(),
    enabled: opened,
  });
  const availablePlugins: AvailablePlugin[] = availablePluginsQ.data?.plugins ?? [];
  const selectedPlugin = useMemo(() => {
    if (!sourcePath) return null;
    // 1) path 完全一致 (= control plane と同じパスを指してる場合)
    let m = availablePlugins.find((p) => p.path === sourcePath);
    if (m) return m;
    // 2) ディレクトリ名 (basename) で fallback match
    //    control plane = /home/paps-ai/...、GPU worker = /opt/pipeline/... のように
    //    パス prefix が違っても、 末端のディレクトリ名 (= plugin slug) が一致すれば同じ plugin
    const baseName = sourcePath.replace(/\/+$/, "").split("/").pop();
    if (!baseName) return null;
    return availablePlugins.find((p) => p.slug === baseName) ?? null;
  }, [availablePlugins, sourcePath]);

  // plugin 切替時に init_kwargs を manifest.default で seed (= 既存値があれば優先)
  useEffect(() => {
    if (!selectedPlugin?.manifest) return;
    setInitKwargs((prev) => {
      const seeded: Record<string, unknown> = { ...prev };
      for (const f of selectedPlugin.manifest!.init_kwargs) {
        if (!(f.key in seeded) && f.default !== undefined && f.default !== null) {
          seeded[f.key] = f.default;
        }
      }
      return seeded;
    });
  }, [selectedPlugin]);

  const setKwarg = (key: string, value: unknown) =>
    setInitKwargs((prev) => ({ ...prev, [key]: value }));

  const renderKwargField = (f: PluginKwargField) => {
    const v = initKwargs[f.key];
    const common = {
      label: f.label || f.key,
      description: f.help,
      required: f.required,
    };
    switch (f.type) {
      case "int":
      case "float":
        return (
          <NumberInput
            key={f.key}
            {...common}
            value={typeof v === "number" ? v : undefined}
            onChange={(val) => setKwarg(f.key, val === "" ? null : Number(val))}
            min={f.min}
            max={f.max}
            decimalScale={f.type === "float" ? 4 : 0}
            allowDecimal={f.type === "float"}
          />
        );
      case "bool":
        return (
          <Checkbox
            key={f.key}
            {...common}
            checked={!!v}
            onChange={(e) => setKwarg(f.key, e.currentTarget.checked)}
          />
        );
      case "enum":
        return (
          <Select
            key={f.key}
            {...common}
            value={v == null ? null : String(v)}
            onChange={(val) => setKwarg(f.key, val)}
            data={(f.options ?? []).map((o) => ({ value: String(o), label: String(o) }))}
          />
        );
      case "secret":
        return (
          <PasswordInput
            key={f.key}
            {...common}
            value={typeof v === "string" ? v : ""}
            onChange={(e) => setKwarg(f.key, e.currentTarget.value)}
          />
        );
      default:
        return (
          <TextInput
            key={f.key}
            {...common}
            value={typeof v === "string" ? v : ""}
            onChange={(e) => setKwarg(f.key, e.currentTarget.value)}
            placeholder={f.type === "path" ? "/path/to/..." : undefined}
          />
        );
    }
  };

  const form = useForm<FormValues>({
    initialValues: DEFAULT_VALUES,
    validate: {
      slug: (v) => (SLUG_RE.test(v) ? null : t("workloads.create.slug_invalid")),
      name: (v) => (v.trim().length > 0 ? null : "required"),
      executor_config: (v) => {
        try {
          JSON.parse(v);
          return null;
        } catch {
          return t("workloads.create.invalid_json");
        }
      },
    },
  });

  // モーダルを開きなおすたびに values をリセット (edit 中なら現在値、create なら default)
  useEffect(() => {
    if (!opened) return;
    form.setValues(editing ? valuesForWorkload(editing) : DEFAULT_VALUES);
    form.resetDirty();
    if (editing && editing.executor_type === "python_module") {
      const cfg = editing.executor_config as Record<string, unknown>;
      setSourcePath(typeof cfg.source_path === "string" ? cfg.source_path : null);
      setPluginModule(typeof cfg.module === "string" ? cfg.module : null);
      setInitKwargs(
        cfg.init_kwargs && typeof cfg.init_kwargs === "object"
          ? { ...(cfg.init_kwargs as Record<string, unknown>) }
          : {},
      );
    } else {
      setSourcePath(null);
      setPluginModule(null);
      setInitKwargs({});
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opened, editing]);

  // executor 種別を変えたらプリセットを差し替え (ユーザー編集を上書きするので
  // confirm はせず明示の onChange ハンドラに限定)。
  const onExecutorChange = (value: string | null) => {
    if (!value) return;
    const ex = value as ExecutorType;
    form.setFieldValue("executor_type", ex);
    form.setFieldValue("executor_config", JSON.stringify(EXECUTOR_PRESETS[ex], null, 2));
  };

  const onSuccess = (w: Workload, action: "created" | "updated") => {
    const titleKey = action === "created" ? "workloads.create.created" : "workloads.edit.updated";
    notifications.show({
      color: "green",
      title: t(titleKey, { slug: w.slug }),
      message: "",
    });
    qc.invalidateQueries({ queryKey: ["workloads"] });
    onClose();
  };

  const onErr = (e: Error) => {
    const body = e instanceof ApiError ? JSON.stringify(e.body) : e.message;
    notifications.show({ color: "red", title: t("workloads.create.failed"), message: body });
  };

  const createMut = useMutation({
    mutationFn: (payload: WorkloadCreate) => api.createWorkload(payload),
    onSuccess: (w) => onSuccess(w, "created"),
    onError: onErr,
  });

  const updateMut = useMutation({
    mutationFn: ({ slug, payload }: { slug: string; payload: Omit<WorkloadCreate, "slug"> }) =>
      api.updateWorkload(slug, payload),
    onSuccess: (w) => onSuccess(w, "updated"),
    onError: onErr,
  });

  // python_module 用: dropdown 選択 + dynamic form の init_kwargs を merge して executor_config を build
  const buildPythonModuleConfig = (currentExecutorConfig: string): Record<string, unknown> => {
    if (!sourcePath) throw new Error(t("workloads.create.plugin_required") ?? "プラグインを選択してください");
    if (!pluginModule) throw new Error(t("workloads.create.plugin_module_required") ?? "モジュールを選択してください");
    const base = JSON.parse(currentExecutorConfig || "{}") as Record<string, unknown>;
    // manifest あり時は init_kwargs を dynamic form から組立 (= 既存 cfg.init_kwargs を上書き)
    const cfg: Record<string, unknown> = {
      ...base,
      source_path: sourcePath,
      module: pluginModule,
    };
    if (selectedPlugin?.manifest) {
      cfg.init_kwargs = { ...initKwargs };
    }
    return cfg;
  };

  const submit = async (values: FormValues) => {
    let executorConfig: Record<string, unknown>;
    if (values.executor_type === "python_module") {
      try {
        executorConfig = buildPythonModuleConfig(values.executor_config);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        notifications.show({ color: "red", title: t("workloads.create.failed"), message: msg });
        return;
      }
    } else {
      executorConfig = JSON.parse(values.executor_config);
    }
    // resources は既存値を保持しつつ vram_mb だけ更新する (= 他フィールドを消さない)
    const baseResources: Record<string, unknown> = {
      ...((editing?.resources as Record<string, unknown> | undefined) ?? {}),
    };
    if (values.vram_mb && values.vram_mb > 0) {
      baseResources.vram_mb = values.vram_mb;
    } else {
      delete baseResources.vram_mb;
    }
    const body = {
      name: values.name,
      description: values.description.trim() || null,
      executor_type: values.executor_type,
      executor_config: executorConfig,
      priority: values.priority,
      weight: values.weight,
      batch_size: values.batch_size,
      lease_secs: values.lease_secs,
      max_attempts: values.max_attempts,
      resources: baseResources,
    };
    if (isEdit) {
      updateMut.mutate({ slug: editing!.slug, payload: body });
    } else {
      createMut.mutate({ slug: values.slug, ...body });
    }
  };

  const pending = createMut.isPending || updateMut.isPending;

  const executorOptions: Array<{ value: ExecutorType; label: string }> = [
    { value: "shell", label: t("workloads.executor_types.shell") },
    { value: "python_module", label: t("workloads.executor_types.python_module") },
  ];

  return (
    <Drawer
      opened={opened}
      onClose={onClose}
      title={
        <Text fw={600}>
          {isEdit ? t("workloads.edit.title", { slug: editing!.slug }) : t("workloads.create.title")}
        </Text>
      }
      position="right"
      size="lg"
      padding="md"
    >
      <form onSubmit={form.onSubmit(submit)}>
        <Stack>
          <Text size="sm" fw={500}>
            {t("workloads.create.basic")}
          </Text>
          <TextInput
            label={t("workloads.slug")}
            placeholder="my-workload"
            description={t("workloads.create.slug_help")}
            required
            disabled={isEdit}
            {...form.getInputProps("slug")}
          />
          <TextInput
            label={t("workloads.name")}
            description={t("workloads.create.name_help")}
            required
            {...form.getInputProps("name")}
          />
          <Textarea
            label={t("workloads.create.description")}
            autosize
            minRows={2}
            maxRows={4}
            {...form.getInputProps("description")}
          />

          <Select
            label={t("workloads.create.executor")}
            data={executorOptions}
            value={form.values.executor_type}
            onChange={onExecutorChange}
            allowDeselect={false}
          />
          {form.values.executor_type === "python_module" ? (
            <Stack gap="xs">
              <Select
                label={t("workloads.create.plugin")}
                description={
                  availablePluginsQ.isLoading
                    ? t("workloads.create.plugin_loading")
                    : availablePlugins.length === 0
                    ? t("workloads.create.plugin_empty")
                    : t("workloads.create.plugin_help", {
                        root: availablePluginsQ.data?.root ?? "",
                        count: availablePlugins.length,
                      })
                }
                data={availablePlugins.map((p) => ({
                  value: p.path,
                  label: `${p.slug}${p.has_requirements ? " (= requirements.txt あり)" : ""}`,
                }))}
                // workload に保存されてる source_path が control plane の path と
                // 違っていても (e.g. worker side /opt/pipeline/... vs control side
                // /home/paps-ai/...) slug 突合した plugin の path を表示する。
                value={selectedPlugin?.path ?? sourcePath}
                onChange={(v) => {
                  setSourcePath(v);
                  setPluginModule(null);
                }}
                placeholder={t("workloads.create.plugin_placeholder")}
                searchable
                nothingFoundMessage={t("workloads.create.plugin_empty")}
              />
              <Select
                label={t("workloads.create.plugin_module")}
                description={t("workloads.create.plugin_module_help")}
                data={
                  selectedPlugin?.modules.map((m) => ({
                    value: m,
                    label: `${m}.py`,
                  })) ?? []
                }
                value={pluginModule}
                onChange={setPluginModule}
                placeholder={t("workloads.create.plugin_module_placeholder")}
                disabled={!selectedPlugin}
              />
              {selectedPlugin?.manifest && selectedPlugin.manifest.init_kwargs.length > 0 ? (
                <>
                  {selectedPlugin.manifest.description && (
                    <Text size="xs" c="dimmed" style={{ whiteSpace: "pre-wrap" }}>
                      {selectedPlugin.manifest.description}
                    </Text>
                  )}
                  <Text size="sm" fw={500}>プラグイン設定</Text>
                  {selectedPlugin.manifest.init_kwargs.map(renderKwargField)}
                  <Accordion variant="separated">
                    <Accordion.Item value="raw">
                      <Accordion.Control>
                        <Text size="xs">詳細 (= raw JSON / hidden_kwargs を直接編集)</Text>
                      </Accordion.Control>
                      <Accordion.Panel>
                        <JsonInput
                          label={t("workloads.create.executor_config")}
                          autosize
                          minRows={4}
                          formatOnBlur
                          validationError={t("workloads.create.invalid_json")}
                          {...form.getInputProps("executor_config")}
                        />
                      </Accordion.Panel>
                    </Accordion.Item>
                  </Accordion>
                </>
              ) : (
                <JsonInput
                  label={t("workloads.create.executor_config")}
                  description={t("workloads.create.executor_config_python_help")}
                  autosize
                  minRows={4}
                  formatOnBlur
                  validationError={t("workloads.create.invalid_json")}
                  {...form.getInputProps("executor_config")}
                />
              )}
            </Stack>
          ) : (
            <JsonInput
              label={t("workloads.create.executor_config")}
              autosize
              minRows={6}
              formatOnBlur
              validationError={t("workloads.create.invalid_json")}
              {...form.getInputProps("executor_config")}
            />
          )}

          <Accordion variant="separated">
            <Accordion.Item value="advanced">
              <Accordion.Control>{t("workloads.create.advanced")}</Accordion.Control>
              <Accordion.Panel>
                <Stack>
                  <NumberInput
                    label={t("workloads.priority")}
                    description={t("workloads.create.priority_help")}
                    min={0}
                    max={100}
                    {...form.getInputProps("priority")}
                  />
                  <NumberInput
                    label={t("workloads.weight")}
                    description={t("workloads.create.weight_help")}
                    min={0}
                    step={0.1}
                    decimalScale={2}
                    {...form.getInputProps("weight")}
                  />
                  <NumberInput
                    label={t("workloads.batch")}
                    description={t("workloads.create.batch_help")}
                    min={1}
                    {...form.getInputProps("batch_size")}
                  />
                  <NumberInput
                    label={t("workloads.create.lease")}
                    description={t("workloads.create.lease_help")}
                    min={1}
                    {...form.getInputProps("lease_secs")}
                  />
                  <NumberInput
                    label={t("workloads.create.max_attempts")}
                    min={1}
                    {...form.getInputProps("max_attempts")}
                  />
                  <NumberInput
                    label={t("workloads.create.vram_mb", "想定 VRAM (MB / worker)")}
                    description={t(
                      "workloads.create.vram_mb_help",
                      "1 worker instance あたり消費する VRAM の目安。 install-multi-worker.sh の --auto-from-workloads がここを読んで GPU ホスト 1 台あたりのインスタンス数を算出する。 0 = GPU 不要 (CPU only / dispatcher 系)",
                    )}
                    min={0}
                    step={256}
                    {...form.getInputProps("vram_mb")}
                  />
                </Stack>
              </Accordion.Panel>
            </Accordion.Item>
          </Accordion>

          <Group justify="flex-end" mt="sm">
            <Button variant="default" onClick={onClose} disabled={pending}>
              {t("workloads.create.cancel")}
            </Button>
            <Button type="submit" loading={pending}>
              {isEdit ? t("workloads.edit.submit") : t("workloads.create.submit")}
            </Button>
          </Group>
        </Stack>
      </form>
    </Drawer>
  );
}
