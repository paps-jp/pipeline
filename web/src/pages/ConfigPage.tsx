import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Code,
  Group,
  Paper,
  Stack,
  Text,
  Textarea,
} from "@mantine/core";
import { modals } from "@mantine/modals";
import { notifications } from "@mantine/notifications";
import { IconAlertTriangle } from "@tabler/icons-react";
import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ApiError, ConfigChange, ConfigPlan, configApi } from "@/api/client";

// 構成 (config as code): YAML を読み込む/貼る → 差分を確認 → 反映。
// 管理対象は静的な設定だけ (workload 定義・許可リスト・配信先/配信パス・ホスト静的ポリシー)。
// 自動制御が動かす項目 (enabled / priority / weight / 台数) は対象外なので、反映しても戻されない。

const OP_COLOR: Record<string, string> = {
  create: "green",
  update: "yellow",
  delete: "red",
  extra: "gray",
  skipped: "orange",
  unchanged: "gray",
};

function fmt(v: unknown): string {
  const s = JSON.stringify(v);
  return s === undefined ? "—" : s.length > 90 ? s.slice(0, 87) + "…" : s;
}

function errorLines(e: unknown): { lines: string[]; plan?: ConfigPlan } {
  if (e instanceof ApiError) {
    const d = (e.body as { detail?: unknown })?.detail as
      | { errors?: string[]; changes?: ConfigChange[] }
      | string
      | undefined;
    if (d && typeof d === "object" && Array.isArray(d.errors)) return { lines: d.errors };
    if (d && typeof d === "object" && Array.isArray(d.changes)) {
      return {
        lines: ["反映が途中で失敗しました。適用済みと未適用は下の一覧を確認してください。"],
        plan: d as unknown as ConfigPlan,
      };
    }
    return { lines: [typeof d === "string" ? d : `HTTP ${e.status}`] };
  }
  return { lines: [String(e)] };
}

export default function ConfigPage() {
  const { t } = useTranslation();
  const [yaml, setYaml] = useState("");
  const [prune, setPrune] = useState(false);
  const [plan, setPlan] = useState<ConfigPlan | null>(null);
  const [errors, setErrors] = useState<string[]>([]);

  const load = useMutation({
    mutationFn: () => configApi.exportYaml(),
    onSuccess: (text) => {
      setYaml(text);
      setPlan(null);
      setErrors([]);
    },
    onError: (e) => setErrors(errorLines(e).lines),
  });
  const doPlan = useMutation({
    mutationFn: () => configApi.plan(yaml, prune),
    onSuccess: (p) => {
      setPlan(p);
      setErrors([]);
    },
    onError: (e) => {
      setPlan(null);
      setErrors(errorLines(e).lines);
    },
  });
  const doApply = useMutation({
    mutationFn: () => configApi.apply(yaml, prune),
    onSuccess: (p) => {
      setPlan(p);
      setErrors([]);
      notifications.show({ color: "teal", message: t("config.applied", "構成を反映しました") });
    },
    onError: (e) => {
      const r = errorLines(e);
      setErrors(r.lines);
      if (r.plan) setPlan(r.plan);
    },
  });

  const confirmApply = () => {
    const s = plan?.summary ?? {};
    modals.openConfirmModal({
      title: t("config.confirm_title", "構成を反映しますか?"),
      children: (
        <Text size="sm">
          {t("config.confirm_body", {
            defaultValue: "作成 {{c}} / 更新 {{u}} / 削除 {{d}} を反映します。",
            c: s.create ?? 0,
            u: s.update ?? 0,
            d: s.delete ?? 0,
          })}
        </Text>
      ),
      labels: { confirm: t("config.apply", "反映"), cancel: t("common.cancel", "キャンセル") },
      confirmProps: { color: (s.delete ?? 0) > 0 ? "red" : undefined },
      onConfirm: () => doApply.mutate(),
    });
  };

  const shown = (plan?.changes ?? []).filter((c) => c.op !== "unchanged");

  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t(
          "config.intro",
          "ワークロード定義・許可リスト・配信先と配信パス・ホストの静的設定を YAML で管理します。差分を確認してから反映します。enabled / priority / weight / 台数は自動制御が動かすため、対象外です。",
        )}
      </Text>

      <Textarea
        label={t("config.yaml", "構成 (YAML)")}
        placeholder={t("config.placeholder", "「現在の構成を読み込む」を押すと、今の設定が YAML で入ります。")}
        value={yaml}
        onChange={(e) => {
          setYaml(e.currentTarget.value);
          setPlan(null);
        }}
        autosize
        minRows={12}
        maxRows={30}
        styles={{ input: { fontFamily: "ui-monospace, monospace", fontSize: 12 } }}
      />

      <Group>
        <Button variant="default" loading={load.isPending} onClick={() => load.mutate()}>
          {t("config.load", "現在の構成を読み込む")}
        </Button>
        <Button disabled={!yaml.trim()} loading={doPlan.isPending} onClick={() => doPlan.mutate()}>
          {t("config.plan", "差分を確認")}
        </Button>
        <Button
          color="indigo"
          variant="light"
          disabled={!plan || !plan.has_changes}
          loading={doApply.isPending}
          onClick={confirmApply}
        >
          {t("config.apply", "反映")}
        </Button>
        <Checkbox
          size="xs"
          checked={prune}
          onChange={(e) => {
            setPrune(e.currentTarget.checked);
            setPlan(null);
          }}
          label={t(
            "config.prune",
            "ファイルに無いワークロード・配信先を削除する (有効なワークロードは削除しません)",
          )}
        />
      </Group>

      {errors.length > 0 && (
        <Alert color="red" icon={<IconAlertTriangle size={16} />}>
          {errors.map((m, i) => (
            <Text key={i} size="sm">
              {m}
            </Text>
          ))}
        </Alert>
      )}

      {plan && (
        <Paper withBorder p="sm">
          <Group gap="xs" mb="xs">
            {Object.entries(plan.summary)
              .filter(([, n]) => n > 0)
              .map(([k, n]) => (
                <Badge key={k} color={OP_COLOR[k] ?? "gray"} variant="light">
                  {k} {n}
                </Badge>
              ))}
            {!plan.has_changes && (
              <Text size="sm" c="dimmed">
                {t("config.no_diff", "差分はありません。")}
              </Text>
            )}
          </Group>
          <Stack gap={6}>
            {shown.map((c) => (
              <div key={`${c.section}:${c.key}`}>
                <Group gap={6}>
                  <Badge size="sm" color={OP_COLOR[c.op] ?? "gray"}>
                    {c.op}
                  </Badge>
                  <Text size="xs" c="dimmed">
                    {c.section}
                  </Text>
                  <Code>{c.key}</Code>
                  {c.applied && (
                    <Badge size="xs" color="teal" variant="light">
                      {t("config.done", "反映済み")}
                    </Badge>
                  )}
                  {c.error && (
                    <Badge size="xs" color="red">
                      {c.error}
                    </Badge>
                  )}
                </Group>
                {Object.entries(c.diff).map(([k, d]) => (
                  <Text key={k} size="xs" ml="xl" ff="monospace">
                    {k}: {c.op === "create" ? fmt(d.after) : `${fmt(d.before)} → ${fmt(d.after)}`}
                  </Text>
                ))}
                {c.note && (
                  <Text size="xs" c="dimmed" ml="xl">
                    {c.note}
                  </Text>
                )}
              </div>
            ))}
          </Stack>
        </Paper>
      )}
    </Stack>
  );
}
