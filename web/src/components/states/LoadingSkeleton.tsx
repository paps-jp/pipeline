import { Skeleton, Stack, Table } from "@mantine/core";

/** N 行 × M 列 のテーブル skeleton (header + 行)。 */
export function TableSkeleton({ rows = 5, cols = 4 }: { rows?: number; cols?: number }) {
  return (
    <Table>
      <Table.Thead>
        <Table.Tr>
          {Array.from({ length: cols }).map((_, i) => (
            <Table.Th key={i}>
              <Skeleton height={12} width="60%" radius="sm" />
            </Table.Th>
          ))}
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {Array.from({ length: rows }).map((_, r) => (
          <Table.Tr key={r}>
            {Array.from({ length: cols }).map((_, c) => (
              <Table.Td key={c}>
                <Skeleton height={10} radius="sm" />
              </Table.Td>
            ))}
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
}

/** N 個並ぶ card 風 skeleton。 */
export function CardSkeletonGrid({ count = 3, height = 120 }: { count?: number; height?: number }) {
  return (
    <Stack gap="md">
      {Array.from({ length: count }).map((_, i) => (
        <Skeleton key={i} height={height} radius="md" />
      ))}
    </Stack>
  );
}
