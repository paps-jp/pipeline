import {
  Badge,
  Box,
  Button,
  Group,
  ScrollArea,
  Stack,
  Tabs,
  Text,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ServiceLogRecord, api, deployApi } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";

const LOG_REFRESH_MS = 2000;

function fmtTime(iso: string | null): string {
  if (!iso) return "        ";
  const d = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
  if (Number.isNaN(d.getTime())) return iso.slice(11, 19);
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mi}:${ss}`;
}

function levelColor(level: string): string {
  if (level === "ERROR" || level === "CRITICAL") return "#ff6b6b";
  if (level === "WARN" || level === "WARNING") return "#fab005";
  if (level === "DEBUG") return "#868e96";
  return "#a5d8ff";
}

/** paprika 風 淡い点滅 (動作中 indicator) */
function LiveBadge({ paused }: { paused: boolean }) {
  if (paused) {
    return <Badge color="gray" variant="dot" size="md">一時停止</Badge>;
  }
  return (
    <Badge
      color="teal"
      variant="dot"
      size="md"
      style={{
        animation: "pulse-soft 1.6s ease-in-out infinite",
      }}
    >
      動作中
    </Badge>
  );
}

const MAX_DISPLAY_LINES = 1000;

function LogPanel({
  host,
  service,
  emptyMessage,
}: {
  host?: string;
  service?: string;
  emptyMessage: string;
}) {
  const [paused, setPaused] = useState(false);
  const viewportRef = useRef<HTMLDivElement | null>(null);

  const q = useQuery({
    queryKey: ["service-logs", host, service],
    queryFn: () =>
      api.listServiceLogs({
        limit: MAX_DISPLAY_LINES,
        host: host ?? null,
        service: service ?? null,
      }),
    refetchInterval: paused ? false : LOG_REFRESH_MS,
  });

  // 1000 行を超えたら新しい方だけ表示 (= 古い行をリセット)
  const records: ServiceLogRecord[] = useMemo(() => {
    const recs = q.data?.records ?? [];
    return recs.length > MAX_DISPLAY_LINES ? recs.slice(-MAX_DISPLAY_LINES) : recs;
  }, [q.data]);

  // 自動スクロール: ResizeObserver で content の height 変化を検知 → 即 bottom
  // (= mount 後の 1000 行 render が非同期に完了するため、 setTimeout では間に合わない)
  // - paused 中は scroll しない
  // - ユーザが手動で上にスクロールしてる時は ResizeObserver でも追従 (= 仕様)。
  //   厳密には scroll near-bottom 判定で disable できるが、 「常に最新を見たい」 要求が
  //   先のため auto follow を default に。
  useEffect(() => {
    if (paused) return;
    const v = viewportRef.current;
    if (!v) return;
    const scroll = () => {
      v.scrollTop = v.scrollHeight;
    };
    // 初回 即 scroll
    scroll();
    // content 変化 (= records 追加 / タブ切替直後の render 完了) を observe
    const ro = new ResizeObserver(scroll);
    // viewport の直接 child = content wrapper
    const content = v.firstElementChild;
    if (content) ro.observe(content);
    return () => ro.disconnect();
  }, [paused, host, service]);

  const errorCount = useMemo(
    () => records.filter((r) => r.level === "ERROR" || r.level === "CRITICAL").length,
    [records],
  );

  return (
    <Stack gap="xs">
      <Group justify="space-between">
        <Group gap="xs">
          <LiveBadge paused={paused} />
          {errorCount > 0 && (
            <Badge color="red" variant="filled" size="sm">
              ERROR {errorCount}
            </Badge>
          )}
        </Group>
        <Group gap="xs">
          <Text size="xs" c="dimmed">
            {records.length} 行 {records.length >= MAX_DISPLAY_LINES ? `(上限 ${MAX_DISPLAY_LINES})` : ""}
          </Text>
          <Button size="xs" variant="light" onClick={() => setPaused((p) => !p)}>
            {paused ? "再開" : "一時停止"}
          </Button>
        </Group>
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
          h="calc(100vh - 260px)"
          viewportRef={viewportRef}
          type="always"
          scrollbarSize={10}
          offsetScrollbars
        >
          {records.length === 0 && (
            <Text size="sm" c="dimmed" ta="center" py="xl">
              {emptyMessage}
            </Text>
          )}
          {records.map((r) => {
            const isError = r.level === "ERROR" || r.level === "CRITICAL";
            const hasExc = !!r.exc_info;
            return (
              <div
                key={r.id}
                style={{
                  borderBottom: "1px solid #25262b",
                  paddingBottom: 2,
                  marginBottom: 2,
                  background: isError ? "#2a1a1d" : "transparent",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    gap: 8,
                    lineHeight: 1.55,
                    whiteSpace: isError ? "pre-wrap" : "nowrap",
                    overflow: "hidden",
                    alignItems: "baseline",
                    wordBreak: isError ? "break-all" : "normal",
                  }}
                >
                  <span style={{ color: "#909296", flex: "0 0 64px" }}>{fmtTime(r.ts)}</span>
                  <span style={{ color: levelColor(r.level), flex: "0 0 60px", fontWeight: isError ? 600 : 400 }}>
                    [{r.level}]
                  </span>
                  <span
                    style={{
                      color: "#e9ecef",
                      flex: "1 1 auto",
                      minWidth: 0,
                      overflow: isError ? "visible" : "hidden",
                      textOverflow: isError ? "clip" : "ellipsis",
                    }}
                    title={isError ? undefined : `${r.logger ?? ""}: ${r.message}`}
                  >
                    {r.message}
                  </span>
                </div>
                {isError && hasExc && (
                  <pre
                    style={{
                      color: "#ff6b6b",
                      margin: "2px 0 4px 132px",
                      padding: 6,
                      background: "#1a1010",
                      borderLeft: "2px solid #ff6b6b",
                      borderRadius: 2,
                      fontSize: 11,
                      lineHeight: 1.45,
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-all",
                      overflowX: "hidden",
                      fontFamily: "ui-monospace, Menlo, Consolas, monospace",
                    }}
                  >
                    {r.exc_info}
                  </pre>
                )}
                {!isError && hasExc && (
                  <pre
                    style={{
                      color: "#fab005",
                      margin: "2px 0 4px 132px",
                      padding: 6,
                      background: "#1f1a0d",
                      borderRadius: 2,
                      fontSize: 11,
                      lineHeight: 1.45,
                      maxHeight: 200,
                      overflowX: "hidden",
                      overflowY: "auto",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-all",
                      fontFamily: "ui-monospace, Menlo, Consolas, monospace",
                    }}
                  >
                    {r.exc_info}
                  </pre>
                )}
              </div>
            );
          })}
        </ScrollArea>
      </Box>
    </Stack>
  );
}

export default function ServiceLog() {
  const { t } = useTranslation();

  // 配信先 (= ホスト) を取って動的にタブを生成
  const targets = useQuery({
    queryKey: ["deploy-targets-for-logs"],
    queryFn: () => deployApi.listTargets(),
    refetchInterval: 30_000,
  });
  const enabledTargets = useMemo(
    () => (targets.data ?? []).filter((t) => t.enabled),
    [targets.data],
  );

  return (
    <Stack gap="lg">
      <style>{`
        @keyframes pulse-soft {
          0%, 100% { opacity: 1; }
          50%      { opacity: 0.55; }
        }
      `}</style>

      <PageHeader title={t("logs.title")} />

      <Tabs defaultValue="pipeline" variant="outline">
        <Tabs.List>
          <Tabs.Tab value="pipeline">Pipeline (制御)</Tabs.Tab>
          {enabledTargets.map((t) => (
            <Tabs.Tab key={t.id} value={`host-${t.host}`}>
              {t.label}
            </Tabs.Tab>
          ))}
        </Tabs.List>

        <Tabs.Panel value="pipeline" pt="md" keepMounted={false}>
          <LogPanel
            service="pipeline-oss-control"
            emptyMessage="Pipeline (制御平面) のログはまだありません。 control plane が起動して何か動作するとここに流れます。"
          />
        </Tabs.Panel>

        {enabledTargets.map((t) => (
          <Tabs.Panel key={t.id} value={`host-${t.host}`} pt="md" keepMounted={false}>
            <LogPanel
              host={t.label}
              emptyMessage={`${t.label} (${t.host}) のログはまだありません。 service が起動して何か出力するとここに流れます。`}
            />
          </Tabs.Panel>
        ))}
      </Tabs>
    </Stack>
  );
}
