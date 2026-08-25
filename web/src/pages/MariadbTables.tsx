import {
  Group,
  Loader,
  NumberInput,
  Pagination,
  Paper,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { useDebouncedValue } from "@mantine/hooks";
import { IconSearch } from "@tabler/icons-react";
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

function TableView({ meta }: { meta: MariadbTableMeta }) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [q, setQ] = useState("");
  const [debouncedQ] = useDebouncedValue(q, 300);
  const [enabledFilter, setEnabledFilter] = useState<number | "">("");
  const [page, setPage] = useState(1);

  const hasEnabledColumn = meta.columns.some((c) => c.name === "enabled");

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
      // eslint-disable-next-line no-console
      console.error(e);
      window.alert(t("mariadbTables.save_failed", { defaultValue: "保存に失敗しました: {{msg}}", msg: String(e) }));
    },
  });

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
      </Group>

      {rowsQ.error && <ErrorState error={rowsQ.error} onRetry={() => rowsQ.refetch()} />}

      {!rowsQ.error && rows.length === 0 && !rowsQ.isLoading && (
        <EmptyState title={t("mariadbTables.no_rows", "該当する行がありません")} />
      )}

      {!rowsQ.error && (rows.length > 0 || rowsQ.isLoading) && (
        <Paper withBorder style={{ overflowX: "auto" }}>
          {rowsQ.isLoading ? (
            <TableSkeleton rows={8} cols={meta.columns.length} />
          ) : (
            <Table striped highlightOnHover withTableBorder stickyHeader>
              <Table.Thead>
                <Table.Tr>
                  {meta.columns.map((c) => (
                    <Table.Th key={c.name}>{c.name}</Table.Th>
                  ))}
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
