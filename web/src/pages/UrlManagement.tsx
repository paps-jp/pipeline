import {
  ActionIcon,
  Badge,
  Button,
  Group,
  Loader,
  NumberInput,
  Paper,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { useDebouncedValue } from "@mantine/hooks";
import { modals } from "@mantine/modals";
import { notifications } from "@mantine/notifications";
import { IconPlus, IconRefresh, IconSearch, IconTrash } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { mariadbTablesApi, UrlSiteRow, urlManagementApi } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

// ============================================================
// URL 管理: 単純な URL 追加 UI + 現在クロール中のサイト一覧
// ============================================================

const REFRESH_MS = 15_000;   // クロール中フラグは paprika_job_q 起点なので数秒〜数十秒で変わる

function AddUrlForm({ onAdded }: { onAdded: () => void }) {
  const { t } = useTranslation();
  const [url, setUrl] = useState("");
  const [enabled, setEnabled] = useState<number | "">(50);

  const registerMutation = useMutation({
    mutationFn: () =>
      urlManagementApi.register({
        url: url.trim(),
        enabled: enabled === "" ? undefined : Number(enabled),
      }),
    onSuccess: (data) => {
      notifications.show({
        color: "teal",
        message: t("urlManagement.added", {
          defaultValue: "追加しました: {{site}} (enabled={{en}})",
          site: data.site, en: data.enabled,
        }),
      });
      setUrl("");
      onAdded();
    },
    onError: (e: unknown) => {
      notifications.show({ color: "red", message: String(e) });
    },
  });

  const canSubmit = url.trim().length > 3 && !registerMutation.isPending;

  return (
    <Paper withBorder p="sm">
      <Stack gap="xs">
        <Text size="sm" fw={500}>
          {t("urlManagement.add_title", "新しいサイトを追加")}
        </Text>
        <Text size="xs" c="dimmed">
          {t("urlManagement.add_hint",
            "URL を貼るだけで site/domain は自動で埋めます (ホスト名から抽出)。 既定は enabled=50 (priority tier)。")}
        </Text>
        <Group align="flex-end" gap="xs" wrap="nowrap">
          <TextInput
            style={{ flex: 1 }}
            placeholder="https://example.com/"
            value={url}
            onChange={(e) => setUrl(e.currentTarget.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && canSubmit) registerMutation.mutate();
            }}
          />
          <NumberInput
            w={110}
            label="enabled"
            value={enabled}
            onChange={(v) => setEnabled(v === "" ? "" : Number(v))}
            min={1}
            max={100}
          />
          <Button
            leftSection={<IconPlus size={14} />}
            disabled={!canSubmit}
            loading={registerMutation.isPending}
            onClick={() => registerMutation.mutate()}
          >
            {t("urlManagement.add_button", "追加")}
          </Button>
        </Group>
      </Stack>
    </Paper>
  );
}

function SitesTable({ sites }: { sites: UrlSiteRow[] }) {
  const { t } = useTranslation();
  const qc = useQueryClient();

  const reseedMutation = useMutation({
    mutationFn: (pk: number) => mariadbTablesApi.reseedRow("crawl_config", pk),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ["url-mgmt-sites"] });
      const n = (data.reset ?? 0) + (data.inserted ?? 0);
      notifications.show({
        color: "teal",
        message: t("urlManagement.reseeded", {
          defaultValue: "種URL を再投入対象に戻しました ({{n}} 件)",
          n,
        }),
      });
    },
    onError: (e: unknown) => notifications.show({ color: "red", message: String(e) }),
  });

  const disableMutation = useMutation({
    mutationFn: (pk: number) => mariadbTablesApi.deleteRow("crawl_config", pk),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["url-mgmt-sites"] });
      notifications.show({
        color: "teal",
        message: t("urlManagement.disabled", "無効化しました (enabled=-1)"),
      });
    },
    onError: (e: unknown) => notifications.show({ color: "red", message: String(e) }),
  });

  const askReseed = (row: UrlSiteRow) => {
    modals.openConfirmModal({
      title: t("urlManagement.reseed_title", "種URL を再投入しますか"),
      children: (
        <Stack gap={4}>
          <Text size="sm">
            <b>{row.site}</b>{" "}
            {t("urlManagement.reseed_body",
              "の種URL を「未投入」状態に戻します。数分以内に priority filler が拾い直します。")}
          </Text>
          <Text size="xs" c="dimmed">{row.url}</Text>
        </Stack>
      ),
      labels: { confirm: t("urlManagement.reseed_confirm", "再投入"),
                cancel: t("urlManagement.cancel", "キャンセル") },
      confirmProps: { color: "blue" },
      onConfirm: () => reseedMutation.mutate(row.id),
    });
  };

  const askDisable = (row: UrlSiteRow) => {
    modals.openConfirmModal({
      title: t("urlManagement.disable_title", "無効化しますか"),
      children: (
        <Text size="sm">
          <b>{row.site}</b>{" "}
          {t("urlManagement.disable_body",
            "の enabled を -1 (恒久除外) に変更します。行自体は残り、後から priority tier に戻せます。")}
        </Text>
      ),
      labels: { confirm: t("urlManagement.disable_confirm", "無効化"),
                cancel: t("urlManagement.cancel", "キャンセル") },
      confirmProps: { color: "red" },
      onConfirm: () => disableMutation.mutate(row.id),
    });
  };

  return (
    <Paper withBorder style={{ overflowX: "auto" }}>
      <Table striped highlightOnHover withTableBorder stickyHeader>
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ width: 40 }}></Table.Th>
            <Table.Th>{t("urlManagement.col_site", "サイト")}</Table.Th>
            <Table.Th style={{ width: 90 }}>{t("urlManagement.col_priority", "優先度")}</Table.Th>
            <Table.Th style={{ width: 120 }}>{t("urlManagement.col_queued", "キュー")}</Table.Th>
            <Table.Th>URL</Table.Th>
            <Table.Th style={{ width: 90 }}>{t("urlManagement.col_actions", "操作")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {sites.map((s) => (
            <Table.Tr key={s.id}>
              <Table.Td>
                {s.is_currently_crawling ? (
                  <Tooltip label={t("urlManagement.crawling_now", "現在クロール中")}>
                    <Badge color="green" variant="filled" size="xs" circle />
                  </Tooltip>
                ) : (
                  <Tooltip label={t("urlManagement.idle", "キューなし")}>
                    <Badge color="gray" variant="light" size="xs" circle />
                  </Tooltip>
                )}
              </Table.Td>
              <Table.Td>
                <Text size="sm" fw={500}>{s.site}</Text>
                {s.memo && <Text size="xs" c="dimmed">{s.memo}</Text>}
              </Table.Td>
              <Table.Td>
                {s.is_priority ? (
                  <Badge color="blue" variant="filled" size="sm">
                    {t("urlManagement.priority", "優先")} {s.enabled}
                  </Badge>
                ) : (
                  <Badge color="gray" variant="light" size="sm">{s.enabled}</Badge>
                )}
              </Table.Td>
              <Table.Td>
                <Group gap={4} wrap="nowrap">
                  <Text size="sm" fw={500}>{s.queued}</Text>
                  <Text size="xs" c="dimmed">
                    ({t("urlManagement.unclaimed_short", "未POST")} {s.queued_unclaimed})
                  </Text>
                </Group>
              </Table.Td>
              <Table.Td>
                <Text size="xs" c="dimmed" style={{ wordBreak: "break-all" }}>
                  {s.url}
                </Text>
              </Table.Td>
              <Table.Td>
                <Group gap={4} wrap="nowrap">
                  <Tooltip label={t("urlManagement.reseed_tooltip", "種URL 再投入")}>
                    <ActionIcon
                      size="sm" color="blue" variant="subtle"
                      loading={reseedMutation.isPending && reseedMutation.variables === s.id}
                      onClick={() => askReseed(s)}
                    >
                      <IconRefresh size={14} />
                    </ActionIcon>
                  </Tooltip>
                  <Tooltip label={t("urlManagement.disable_confirm", "無効化")}>
                    <ActionIcon
                      size="sm" color="red" variant="subtle"
                      onClick={() => askDisable(s)}
                    >
                      <IconTrash size={14} />
                    </ActionIcon>
                  </Tooltip>
                </Group>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Paper>
  );
}

export default function UrlManagement() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [q, setQ] = useState("");
  const [debouncedQ] = useDebouncedValue(q, 300);
  const [onlyActive, setOnlyActive] = useState(false);

  const sitesQ = useQuery({
    queryKey: ["url-mgmt-sites", debouncedQ, onlyActive],
    queryFn: () => urlManagementApi.listSites({
      q: debouncedQ || undefined,
      only_active: onlyActive || undefined,
    }),
    refetchInterval: REFRESH_MS,
    placeholderData: (prev) => prev,
  });

  const sites = sitesQ.data?.sites ?? [];
  const total = sitesQ.data?.total ?? 0;
  const priorityCount = sitesQ.data?.priority_count ?? 0;
  const crawlingCount = sitesQ.data?.crawling_count ?? 0;

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("urlManagement.title", "URL管理")}
        subtitle={t("urlManagement.subtitle",
          "クロール対象サイトの追加と、現在クロール中の状況の一覧")}
      />

      <AddUrlForm onAdded={() => qc.invalidateQueries({ queryKey: ["url-mgmt-sites"] })} />

      <Group gap="md" align="flex-end">
        <TextInput
          w={280}
          leftSection={<IconSearch size={14} />}
          placeholder={t("urlManagement.search_placeholder", "サイト / ドメイン / URL / memo")}
          value={q}
          onChange={(e) => setQ(e.currentTarget.value)}
        />
        <Switch
          label={t("urlManagement.only_active", "クロール中のみ")}
          checked={onlyActive}
          onChange={(e) => setOnlyActive(e.currentTarget.checked)}
        />
        <Text size="xs" c="dimmed">
          {t("urlManagement.summary", {
            defaultValue: "全 {{n}} 件 / 優先 {{p}} / クロール中 {{c}}",
            n: total, p: priorityCount, c: crawlingCount,
          })}
        </Text>
        {sitesQ.isFetching && <Loader size="xs" />}
      </Group>

      {sitesQ.error && <ErrorState error={sitesQ.error} onRetry={() => sitesQ.refetch()} />}

      {!sitesQ.error && sitesQ.isLoading && (
        <Paper withBorder>
          <TableSkeleton rows={8} cols={6} />
        </Paper>
      )}

      {!sitesQ.error && !sitesQ.isLoading && sites.length === 0 && (
        <EmptyState title={t("urlManagement.no_sites", "該当するサイトがありません")} />
      )}

      {!sitesQ.error && sites.length > 0 && (
        <SitesTable sites={sites} />
      )}
    </Stack>
  );
}
