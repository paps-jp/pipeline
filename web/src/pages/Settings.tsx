/**
 * Settings — システム設定。 LLM advisor の接続情報 + コール履歴。
 * llm.api_key は masked 表示。 入力時のみ送信。
 */

import { useMemo, useState } from "react";
import {
  Alert,
  Badge,
  Box,
  Button,
  Code,
  Group,
  Loader,
  Modal,
  NumberInput,
  Paper,
  PasswordInput,
  Select,
  Stack,
  Switch,
  Table,
  Tabs,
  Text,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  IconBrain,
  IconCheck,
  IconList,
  IconPlugConnected,
  IconRobot,
  IconX,
} from "@tabler/icons-react";

import { api, type LlmCallDetail, type SettingItem } from "@/api/client";

function findSetting(items: SettingItem[], key: string): SettingItem | undefined {
  return items.find((s) => s.key === key);
}

function settingValue(item?: SettingItem): string {
  if (!item) return "";
  if (item.is_secret) return ""; // 生値は返ってこない
  return item.value ?? "";
}

export default function Settings() {
  return (
    <Box p="md">
      <Tabs defaultValue="llm">
        <Tabs.List>
          <Tabs.Tab value="llm" leftSection={<IconBrain size={16} />}>
            LLM advisor
          </Tabs.Tab>
          <Tabs.Tab value="calls" leftSection={<IconList size={16} />}>
            呼び出し履歴
          </Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel value="llm" pt="md">
          <LlmConfigSection />
        </Tabs.Panel>
        <Tabs.Panel value="calls" pt="md">
          <LlmCallsSection />
        </Tabs.Panel>
      </Tabs>
    </Box>
  );
}

// ---------------- LLM config ----------------

function LlmConfigSection() {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ["settings"],
    queryFn: () => api.listSettings(),
    refetchInterval: 60_000,
  });
  const setMut = useMutation({
    mutationFn: ({ key, value }: { key: string; value: string | null }) =>
      api.setSetting(key, value, "settings-ui"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["settings"] }),
  });
  const testMut = useMutation({
    mutationFn: (body: Parameters<typeof api.testLlm>[0]) => api.testLlm(body),
  });

  const items = q.data?.settings ?? [];
  const enabled = settingValue(findSetting(items, "llm.enabled")) === "1";
  const applyMode = settingValue(findSetting(items, "llm.apply_mode")) === "1";
  const provider = settingValue(findSetting(items, "llm.provider")) || "deepseek";
  const endpoint = settingValue(findSetting(items, "llm.endpoint"));
  const model = settingValue(findSetting(items, "llm.model"));
  const intervalMin = parseInt(settingValue(findSetting(items, "llm.interval_min")) || "15");
  const maxActions = parseInt(settingValue(findSetting(items, "llm.max_actions_per_cycle")) || "5");
  const confidence = parseFloat(settingValue(findSetting(items, "llm.confidence_threshold")) || "0.7");
  const timeoutS = parseInt(settingValue(findSetting(items, "llm.timeout_s")) || "60");
  const apiKeyItem = findSetting(items, "llm.api_key");
  const apiKeyMasked = apiKeyItem?.value_masked ?? null;
  const [newApiKey, setNewApiKey] = useState("");
  const [showSaved, setShowSaved] = useState(false);

  const onSetKV = (key: string, value: string | null) => {
    setMut.mutate({ key, value });
  };

  const onSaveApiKey = () => {
    if (!newApiKey) return;
    setMut.mutate(
      { key: "llm.api_key", value: newApiKey },
      {
        onSuccess: () => {
          setNewApiKey("");
          setShowSaved(true);
          setTimeout(() => setShowSaved(false), 3000);
        },
      },
    );
  };

  const onTest = () => {
    testMut.mutate({});
  };

  if (q.isLoading) return <Loader />;

  return (
    <Stack gap="md">
      <Title order={3}>LLM advisor 設定</Title>
      <Text size="sm" c="dimmed">
        supervisor が定期的に LLM にパイプライン状態を投げて、 最適化提案を受け取り適用します。
      </Text>

      <Paper p="md" withBorder>
        <Group justify="space-between">
          <Group gap="xs">
            <IconRobot size={20} color={enabled ? "var(--mantine-color-green-6)" : undefined} />
            <Box>
              <Text fw={600}>LLM advisor 有効化</Text>
              <Text size="xs" c="dimmed">
                {enabled ? "ON: 定期コール中" : "OFF: コールしない"}
              </Text>
            </Box>
          </Group>
          <Switch
            size="lg"
            checked={enabled}
            onChange={(e) => onSetKV("llm.enabled", e.currentTarget.checked ? "1" : "0")}
          />
        </Group>
      </Paper>

      <Paper p="md" withBorder>
        <Group justify="space-between">
          <Group gap="xs">
            <IconCheck size={20} color={applyMode ? "var(--mantine-color-green-6)" : undefined} />
            <Box>
              <Text fw={600}>apply_mode (= 提案を実適用)</Text>
              <Text size="xs" c="dimmed">
                {applyMode ? "ON: 自動で priority/filter を上書き" : "OFF (dry-run): 提案だけログに記録"}
              </Text>
            </Box>
          </Group>
          <Switch
            size="lg"
            checked={applyMode}
            onChange={(e) => onSetKV("llm.apply_mode", e.currentTarget.checked ? "1" : "0")}
          />
        </Group>
      </Paper>

      <Paper p="md" withBorder>
        <Stack gap="sm">
          <Title order={5}>接続</Title>
          <Select
            label="プロバイダ"
            value={provider}
            data={[
              { value: "deepseek", label: "DeepSeek" },
              { value: "openai", label: "OpenAI" },
              { value: "anthropic", label: "Anthropic" },
              { value: "custom", label: "Custom (OpenAI 互換)" },
            ]}
            onChange={(v) => v && onSetKV("llm.provider", v)}
          />
          <TextInput
            label="エンドポイント (chat completions)"
            placeholder="https://api.deepseek.com/v1/chat/completions"
            value={endpoint}
            onChange={(e) => onSetKV("llm.endpoint", e.currentTarget.value)}
          />
          <TextInput
            label="モデル名"
            placeholder="deepseek-chat"
            value={model}
            onChange={(e) => onSetKV("llm.model", e.currentTarget.value)}
          />
          <Box>
            <Text size="sm" fw={500} mb={4}>
              API キー
              {apiKeyMasked && (
                <Code ml={6} fz={11}>
                  現在: {apiKeyMasked}
                </Code>
              )}
            </Text>
            <Group gap="xs" wrap="nowrap">
              <PasswordInput
                placeholder="sk-..."
                value={newApiKey}
                onChange={(e) => setNewApiKey(e.currentTarget.value)}
                style={{ flex: 1 }}
              />
              <Button onClick={onSaveApiKey} disabled={!newApiKey} loading={setMut.isPending}>
                保存
              </Button>
            </Group>
            {showSaved && (
              <Text size="xs" c="green" mt={4}>
                ✓ API キー保存しました(マスク表示に変わります)
              </Text>
            )}
          </Box>
          <Group justify="space-between" mt="xs">
            <Button
              variant="light"
              leftSection={<IconPlugConnected size={16} />}
              onClick={onTest}
              loading={testMut.isPending}
            >
              接続テスト(保存済 API キーで実行)
            </Button>
          </Group>
          {testMut.data && (
            <Alert
              color={testMut.data.ok ? "green" : "red"}
              icon={testMut.data.ok ? <IconCheck size={16} /> : <IconX size={16} />}
            >
              <Stack gap={4}>
                <Text size="sm" fw={600}>
                  {testMut.data.ok ? "接続成功" : "接続失敗"}
                </Text>
                <Text size="xs">
                  status={testMut.data.status_code ?? "-"} | latency={testMut.data.latency_ms}ms |
                  model={testMut.data.model}
                </Text>
                {testMut.data.response_excerpt && (
                  <Code fz={11}>response: {testMut.data.response_excerpt}</Code>
                )}
                {testMut.data.error && (
                  <Text size="xs" c="red.6">
                    {testMut.data.error}
                  </Text>
                )}
              </Stack>
            </Alert>
          )}
        </Stack>
      </Paper>

      <Paper p="md" withBorder>
        <Stack gap="sm">
          <Title order={5}>挙動</Title>
          <Group grow>
            <NumberInput
              label="コール間隔(分)"
              value={intervalMin}
              min={1}
              max={120}
              onChange={(v) => onSetKV("llm.interval_min", String(v ?? 15))}
            />
            <NumberInput
              label="1 サイクル最大 action 数"
              value={maxActions}
              min={1}
              max={20}
              onChange={(v) => onSetKV("llm.max_actions_per_cycle", String(v ?? 5))}
            />
          </Group>
          <Group grow>
            <NumberInput
              label="confidence 閾値 (apply_mode 時)"
              value={confidence}
              min={0}
              max={1}
              step={0.05}
              onChange={(v) => onSetKV("llm.confidence_threshold", String(v ?? 0.7))}
            />
            <NumberInput
              label="HTTP timeout (秒)"
              value={timeoutS}
              min={5}
              max={300}
              onChange={(v) => onSetKV("llm.timeout_s", String(v ?? 60))}
            />
          </Group>
        </Stack>
      </Paper>
    </Stack>
  );
}

// ---------------- LLM call history ----------------

function LlmCallsSection() {
  const q = useQuery({
    queryKey: ["llm-calls"],
    queryFn: () => api.listLlmCalls(50),
    refetchInterval: 10_000,
  });
  const [openId, setOpenId] = useState<number | null>(null);

  if (q.isLoading) return <Loader />;
  const calls = q.data?.calls ?? [];

  return (
    <Stack gap="md">
      <Title order={3}>LLM 呼び出し履歴</Title>
      <Text size="sm" c="dimmed">直近 50 件。 クリックで prompt / response 詳細表示</Text>

      <Paper withBorder>
        <Table verticalSpacing="sm" highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>id</Table.Th>
              <Table.Th>時刻</Table.Th>
              <Table.Th>model</Table.Th>
              <Table.Th>状態</Table.Th>
              <Table.Th>適用</Table.Th>
              <Table.Th>tokens</Table.Th>
              <Table.Th>分析</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {calls.map((c) => (
              <Table.Tr
                key={c.id}
                style={{ cursor: "pointer" }}
                onClick={() => setOpenId(c.id)}
              >
                <Table.Td>{c.id}</Table.Td>
                <Table.Td>{c.called_at.slice(11, 19)}</Table.Td>
                <Table.Td>
                  <Code fz={11}>{c.model}</Code>
                </Table.Td>
                <Table.Td>
                  {c.success ? (
                    <Badge color="green" variant="light" size="sm">
                      OK ({c.duration_ms}ms)
                    </Badge>
                  ) : (
                    <Tooltip label={c.error ?? ""}>
                      <Badge color="red" variant="light" size="sm">
                        FAIL
                      </Badge>
                    </Tooltip>
                  )}
                </Table.Td>
                <Table.Td>
                  <Badge color={c.actions_applied > 0 ? "blue" : "gray"} variant="light" size="sm">
                    {c.actions_applied}
                  </Badge>
                </Table.Td>
                <Table.Td>
                  <Text fz={11}>{c.total_tokens ?? "-"}</Text>
                </Table.Td>
                <Table.Td>
                  <Text fz={11} truncate style={{ maxWidth: 400 }}>
                    {c.analysis ?? "—"}
                  </Text>
                </Table.Td>
              </Table.Tr>
            ))}
            {calls.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={7}>
                  <Text c="dimmed" ta="center" size="sm">
                    まだ呼び出しがありません
                  </Text>
                </Table.Td>
              </Table.Tr>
            )}
          </Table.Tbody>
        </Table>
      </Paper>

      <CallDetailModal callId={openId} onClose={() => setOpenId(null)} />
    </Stack>
  );
}

function CallDetailModal({ callId, onClose }: { callId: number | null; onClose: () => void }) {
  const q = useQuery({
    queryKey: ["llm-call", callId],
    queryFn: () => api.getLlmCall(callId!),
    enabled: callId !== null,
  });
  return (
    <Modal opened={callId !== null} onClose={onClose} size="xl" title={`LLM Call #${callId}`}>
      {q.isLoading && <Loader />}
      {q.data && <CallDetail call={q.data} />}
    </Modal>
  );
}

function CallDetail({ call }: { call: LlmCallDetail }) {
  const promptPretty = useMemo(() => {
    try {
      return JSON.stringify(call.prompt_json, null, 2);
    } catch {
      return "";
    }
  }, [call.prompt_json]);
  const actionsPretty = useMemo(() => {
    try {
      return JSON.stringify(call.actions_json, null, 2);
    } catch {
      return "";
    }
  }, [call.actions_json]);
  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        called_at: {call.called_at}  model: {call.model}  duration: {call.duration_ms}ms
      </Text>
      {call.analysis && (
        <Box>
          <Text size="sm" fw={600}>分析</Text>
          <Code block fz={12}>
            {call.analysis}
          </Code>
        </Box>
      )}
      {actionsPretty && (
        <Box>
          <Text size="sm" fw={600}>提案 actions ({call.actions_applied} 適用)</Text>
          <Code block fz={11} style={{ maxHeight: 300, overflow: "auto" }}>
            {actionsPretty}
          </Code>
        </Box>
      )}
      {call.response_text && (
        <Box>
          <Text size="sm" fw={600}>raw response</Text>
          <Code block fz={11} style={{ maxHeight: 300, overflow: "auto" }}>
            {call.response_text}
          </Code>
        </Box>
      )}
      {promptPretty && (
        <Box>
          <Text size="sm" fw={600}>送信した prompt (snapshot 含む)</Text>
          <Code block fz={11} style={{ maxHeight: 400, overflow: "auto" }}>
            {promptPretty}
          </Code>
        </Box>
      )}
    </Stack>
  );
}
