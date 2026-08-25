import {
  ActionIcon,
  Button,
  Code,
  Group,
  Loader,
  Modal,
  NumberInput,
  Pagination,
  Paper,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { useDebouncedValue, useDisclosure } from "@mantine/hooks";
import { modals } from "@mantine/modals";
import { notifications } from "@mantine/notifications";
import { IconPlus, IconSearch, IconTrash } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { MariadbTableMeta, mariadbTablesApi } from "@/api/client";
import { PageHeader } from "@/components/PageHeader";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";

// ============================================================
// mariadb-tables: 外部 MariaDB (crawl_config 等) の汎用テーブル admin
//
// pipeline/db/mariadb_admin.py の TABLE_REGISTRY に登録されたテーブルだけを
// 一覧・編集できる。 新しいテーブルはバックエンドのレジストリに 1 エントリ
// 足すだけでこの画面から扱えるようになる (フロント側の変更は不要)。
// ============================================================

const PAGE_SIZE = 50;

/** 1 セル分の編集可能フィールド。 onBlur で確定するまでローカルに持つ。 */
function EditableCell({
  value,
  kind,
  onCommit,
}: {
  value: unknown;
  kind: "str" | "int";
  onCommit: (next: unknown) => void;
}) {
  const [draft, setDraft] = useState<string>(value === null || value === undefined ? "" : String(value));

  const commit = () => {
    if (kind === "int") {
      const n = draft.trim() === "" ? null : Number(draft);
      if (draft.trim() !== "" && Number.isNaN(n)) return;
      if (n !== value) onCommit(n);
    } else {
      const v = draft === "" ? null : draft;
      if (v !== value) onCommit(v);
    }
  };

  return (
    <TextInput
      size="xs"
      value={draft}
      onChange={(e) => setDraft(e.currentTarget.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") e.currentTarget.blur();
      }}
    />
  );
}

/** 新規追加フォーム。 creatable な列だけを出す (id/next_no 等は defaults 側で埋まる)。 */
function CreateRowModal({
  meta,
  opened,
  onClose,
  onCreated,
}: {
  meta: MariadbTableMeta;
  opened: boolean;
  onClose: () => void;
  onCreated: () => void;
}) {
  const { t } = useTranslation();
  const creatableColumns = meta.columns.filter((c) => c.creatable);
  const [draft, setDraft] = useState<Record<string, string>>({});

  const createMutation = useMutation({
    mutationFn: () => {
      const fields: Record<string, unknown> = {};
      for (const c of creatableColumns) {
        const v = draft[c.name];
        if (v === undefined || v === "") continue;
        fields[c.name] = c.kind === "int" ? Number(v) : v;
      }
      return mariadbTablesApi.createRow(meta.name, fields);
    },
    onSuccess: () => {
      setDraft({});
      onCreated();
      onClose();
      notifications.show({
        color: "teal",
        message: t("mariadbTables.created", "新規追加しました"),
      });
    },
    onError: (e: unknown) => {
      notifications.show({ color: "red", message: String(e) });
    },
  });

  const missingRequired = meta.create_required.filter((name) => !draft[name]?.trim());

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={t("mariadbTables.create_title", { defaultValue: "{{label}} を新規追加", label: meta.label })}
    >
      <Stack gap="sm">
        {creatableColumns.map((c) => {
          const required = meta.create_required.includes(c.name);
          const label = required ? `${c.name} *` : c.name;
          return c.kind === "int" ? (
            <NumberInput
              key={c.name}
              label={label}
              value={draft[c.name] ?? ""}
              onChange={(v) => setDraft((d) => ({ ...d, [c.name]: v === "" ? "" : String(v) }))}
            />
          ) : (
            <TextInput
              key={c.name}
              label={label}
              value={draft[c.name] ?? ""}
              onChange={(e) => {
                // e.currentTarget はハンドラの同期実行が終わると null にリセットされる
                // (React の仕様)。 functional updater 内で遅延参照すると paste 等で
                // null 参照になりうるので、 同期的に読み取ってから渡す。
                const value = e.currentTarget.value;
                setDraft((d) => ({ ...d, [c.name]: value }));
              }}
            />
          );
        })}
        <Group justify="flex-end" mt="xs">
          <Button variant="subtle" onClick={onClose}>
            {t("mariadbTables.cancel", "キャンセル")}
          </Button>
          <Button
            disabled={missingRequired.length > 0}
            loading={createMutation.isPending}
            onClick={() => createMutation.mutate()}
          >
            {t("mariadbTables.create_submit", "追加")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}

function TableView({ meta }: { meta: MariadbTableMeta }) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [q, setQ] = useState("");
  const [debouncedQ] = useDebouncedValue(q, 300);
  const [enabledFilter, setEnabledFilter] = useState<number | "">("");
  const [page, setPage] = useState(1);
  const [createOpened, { open: openCreate, close: closeCreate }] = useDisclosure(false);

  const hasEnabledColumn = meta.columns.some((c) => c.name === "enabled");
  const isCreatable = meta.columns.some((c) => c.creatable);
  const isDeletable = meta.deletable || Boolean(meta.soft_delete_column);

  const rowsQ = useQuery({
    queryKey: ["mariadb-rows", meta.name, debouncedQ, enabledFilter, page],
    queryFn: () =>
      mariadbTablesApi.listRows(meta.name, {
        q: debouncedQ || undefined,
        enabled: enabledFilter === "" ? undefined : Number(enabledFilter),
        limit: PAGE_SIZE,
        offset: (page - 1) * PAGE_SIZE,
      }),
    placeholderData: (prev) => prev,
  });

  const updateMutation = useMutation({
    mutationFn: ({ pk, fields }: { pk: number; fields: Record<string, unknown> }) =>
      mariadbTablesApi.updateRow(meta.name, pk, fields),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["mariadb-rows", meta.name] });
    },
    onError: (e: unknown) => {
      notifications.show({
        color: "red",
        message: t("mariadbTables.save_failed", { defaultValue: "保存に失敗しました: {{msg}}", msg: String(e) }),
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (pk: number) => mariadbTablesApi.deleteRow(meta.name, pk),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["mariadb-rows", meta.name] });
      notifications.show({
        color: "teal",
        message: meta.deletable
          ? t("mariadbTables.deleted", "削除しました")
          : t("mariadbTables.disabled", "無効化しました"),
      });
    },
    onError: (e: unknown) => {
      notifications.show({ color: "red", message: String(e) });
    },
  });

  const askDelete = (pk: number, row: Record<string, unknown>) => {
    const identifier = String(row.site ?? row.domain ?? row[meta.pk] ?? pk);
    modals.openConfirmModal({
      title: meta.deletable
        ? t("mariadbTables.delete_title", "削除しますか")
        : t("mariadbTables.disable_title", "無効化しますか"),
      children: (
        <Text size="sm">
          {meta.deletable ? (
            <>
              <Code>{identifier}</Code>{" "}
              {t("mariadbTables.delete_body", "を完全に削除します。この操作は元に戻せません。")}
            </>
          ) : (
            <>
              <Code>{identifier}</Code> の <Code>{meta.soft_delete_column}</Code> を{" "}
              <Code>{String(meta.soft_delete_value)}</Code>{" "}
              {t("mariadbTables.disable_body",
                "に変更します(行自体は残り、後から編集で元に戻せます)。")}
            </>
          )}
        </Text>
      ),
      labels: {
        confirm: meta.deletable ? t("mariadbTables.delete_confirm", "削除") : t("mariadbTables.disable_confirm", "無効化"),
        cancel: t("mariadbTables.cancel", "キャンセル"),
      },
      confirmProps: { color: "red" },
      onConfirm: () => deleteMutation.mutate(pk),
    });
  };

  const rows = rowsQ.data?.rows ?? [];
  const total = rowsQ.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <Stack gap="sm">
      <Group>
        {meta.searchable.length > 0 && (
          <TextInput
            placeholder={t("mariadbTables.search_placeholder", {
              defaultValue: "検索 ({{cols}})",
              cols: meta.searchable.join(", "),
            })}
            leftSection={<IconSearch size={14} />}
            value={q}
            onChange={(e) => {
              setQ(e.currentTarget.value);
              setPage(1);
            }}
            w={320}
          />
        )}
        {hasEnabledColumn && (
          <NumberInput
            placeholder={t("mariadbTables.enabled_filter", "enabled で絞り込み")}
            value={enabledFilter}
            onChange={(v) => {
              setEnabledFilter(v === "" ? "" : Number(v));
              setPage(1);
            }}
            w={180}
            clampBehavior="strict"
          />
        )}
        <Text size="xs" c="dimmed">
          {t("mariadbTables.total", { defaultValue: "{{n}} 件", n: total })}
        </Text>
        {rowsQ.isFetching && <Loader size="xs" />}
        {isCreatable && (
          <Button size="xs" leftSection={<IconPlus size={14} />} ml="auto" onClick={openCreate}>
            {t("mariadbTables.create_button", "新規追加")}
          </Button>
        )}
      </Group>

      <CreateRowModal
        meta={meta}
        opened={createOpened}
        onClose={closeCreate}
        onCreated={() => qc.invalidateQueries({ queryKey: ["mariadb-rows", meta.name] })}
      />

      {rowsQ.error && <ErrorState error={rowsQ.error} onRetry={() => rowsQ.refetch()} />}

      {!rowsQ.error && rows.length === 0 && !rowsQ.isLoading && (
        <EmptyState title={t("mariadbTables.no_rows", "該当する行がありません")} />
      )}

      {!rowsQ.error && (rows.length > 0 || rowsQ.isLoading) && (
        <Paper withBorder style={{ overflowX: "auto" }}>
          {rowsQ.isLoading ? (
            <TableSkeleton rows={8} cols={meta.columns.length + (isDeletable ? 1 : 0)} />
          ) : (
            <Table striped highlightOnHover withTableBorder stickyHeader>
              <Table.Thead>
                <Table.Tr>
                  {meta.columns.map((c) => (
                    <Table.Th key={c.name}>{c.name}</Table.Th>
                  ))}
                  {isDeletable && <Table.Th style={{ width: 60 }} />}
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {rows.map((row) => {
                  const pk = Number(row[meta.pk]);
                  return (
                    <Table.Tr key={pk}>
                      {meta.columns.map((c) => (
                        <Table.Td key={c.name} style={{ minWidth: c.editable ? 160 : undefined }}>
                          {c.editable ? (
                            <EditableCell
                              value={row[c.name]}
                              kind={c.kind}
                              onCommit={(next) =>
                                updateMutation.mutate({ pk, fields: { [c.name]: next } })
                              }
                            />
                          ) : (
                            <Text size="xs" c="dimmed" style={{ whiteSpace: "nowrap" }}>
                              {row[c.name] === null || row[c.name] === undefined
                                ? "—"
                                : String(row[c.name])}
                            </Text>
                          )}
                        </Table.Td>
                      ))}
                      {isDeletable && (
                        <Table.Td>
                          <Tooltip
                            label={meta.deletable
                              ? t("mariadbTables.delete_confirm", "削除")
                              : t("mariadbTables.disable_confirm", "無効化")}
                          >
                            <ActionIcon
                              size="sm"
                              color="red"
                              variant="subtle"
                              onClick={() => askDelete(pk, row)}
                            >
                              <IconTrash size={14} />
                            </ActionIcon>
                          </Tooltip>
                        </Table.Td>
                      )}
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          )}
        </Paper>
      )}

      {totalPages > 1 && (
        <Group justify="center">
          <Pagination total={totalPages} value={page} onChange={setPage} size="sm" />
        </Group>
      )}
    </Stack>
  );
}

export default function MariadbTables() {
  const { t } = useTranslation();
  const tablesQ = useQuery({
    queryKey: ["mariadb-tables"],
    queryFn: () => mariadbTablesApi.listTables(),
  });
  const tables = useMemo(() => tablesQ.data?.tables ?? [], [tablesQ.data]);
  const [selected, setSelected] = useState<string | null>(null);

  const active = tables.find((tbl) => tbl.name === (selected ?? tables[0]?.name));

  return (
    <Stack gap="lg">
      <PageHeader
        title={t("mariadbTables.title", "クロール設定")}
        subtitle={t(
          "mariadbTables.subtitle",
          "外部 DB 上のクロール関連テーブルを直接閲覧・編集する (crawl_config 等)",
        )}
      />

      {tablesQ.error && <ErrorState error={tablesQ.error} onRetry={() => tablesQ.refetch()} />}

      {!tablesQ.error && tablesQ.isLoading && <Loader size="sm" />}

      {!tablesQ.error && tables.length > 0 && (
        <Stack gap="md">
          <Select
            label={t("mariadbTables.table_select", "テーブル")}
            data={tables.map((tbl) => ({ value: tbl.name, label: `${tbl.label} (${tbl.name})` }))}
            value={active?.name ?? null}
            onChange={setSelected}
            w={360}
            allowDeselect={false}
          />
          {active && <TableView key={active.name} meta={active} />}
        </Stack>
      )}
    </Stack>
  );
}
