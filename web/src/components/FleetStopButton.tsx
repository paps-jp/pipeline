/**
 * ヘッダの「全停止 / 再開」ボタン。
 *
 * 停止: いま enabled=1 の workload を記録してから全部止める。
 * 再開: その記録にあるものだけを戻す (= もともと止めていたものは動かさない)。
 *
 * 停止も再開も影響範囲が大きいので、 実行前に対象を出して確認を取る。
 */
import { Badge, Button, Group, List, Modal, Text, Tooltip } from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { IconPlayerPlay, IconPlayerStop } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import { api } from "@/api/client";

export function FleetStopButton() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [opened, { open, close }] = useDisclosure(false);

  const { data } = useQuery({
    queryKey: ["fleet-state"],
    queryFn: () => api.fleetState(),
    refetchInterval: 5000,
  });

  const stopped = data?.stopped ?? false;
  const targets = stopped ? (data?.recorded_slugs ?? []) : (data?.enabled_now ?? []);

  const mutation = useMutation({
    mutationFn: () => (stopped ? api.fleetResume() : api.fleetStop()),
    onSuccess: (res) => {
      close();
      notifications.show({
        color: res.failed.length ? "orange" : stopped ? "teal" : "red",
        title: stopped
          ? t("fleet.resumed", "再開しました")
          : t("fleet.stopped", "全停止しました"),
        message:
          res.message +
          (res.failed.length
            ? ` / ${t("fleet.failed_n", "失敗")} ${res.failed.length}`
            : ""),
      });
      void qc.invalidateQueries({ queryKey: ["fleet-state"] });
      void qc.invalidateQueries({ queryKey: ["workloads"] });
    },
    onError: (e: unknown) => {
      notifications.show({
        color: "red",
        title: t("fleet.error", "実行できませんでした"),
        message: e instanceof Error ? e.message : String(e),
      });
    },
  });

  return (
    <>
      <Tooltip
        label={
          stopped
            ? t("fleet.tip_resume", "停止前に動いていた workload を再開します")
            : t("fleet.tip_stop", "稼働中の workload を記録してから全部止めます")
        }
      >
        <Button
          size="xs"
          variant={stopped ? "filled" : "light"}
          color={stopped ? "teal" : "red"}
          leftSection={
            stopped ? <IconPlayerPlay size={16} /> : <IconPlayerStop size={16} />
          }
          onClick={open}
        >
          {stopped ? t("fleet.resume", "再開") : t("fleet.stop", "全停止")}
          {stopped && targets.length > 0 && (
            <Badge ml="xs" size="sm" variant="white" color="teal">
              {targets.length}
            </Badge>
          )}
        </Button>
      </Tooltip>

      <Modal
        opened={opened}
        onClose={close}
        title={
          stopped
            ? t("fleet.modal_resume", "パイプラインを再開しますか")
            : t("fleet.modal_stop", "パイプラインを全停止しますか")
        }
        centered
      >
        <Text size="sm" mb="sm">
          {stopped
            ? t(
                "fleet.desc_resume",
                "停止時に記録した以下の workload だけを再開します。停止前から止めていたものは動きません。",
              )
            : t(
                "fleet.desc_stop",
                "現在稼働中の以下の workload を記録してから停止します。記録は再開時に使います。",
              )}
        </Text>

        {targets.length === 0 ? (
          <Text size="sm" c="dimmed">
            {t("fleet.none", "対象がありません")}
          </Text>
        ) : (
          <List size="sm" spacing={2} mb="sm" style={{ maxHeight: 260, overflowY: "auto" }}>
            {targets.map((s) => (
              <List.Item key={s}>{s}</List.Item>
            ))}
          </List>
        )}

        {!stopped && (
          <Text size="xs" c="dimmed" mb="sm">
            {t(
              "fleet.note_supervisor",
              "supervisor を最初に止めます (他 workload を復活させないため)。実行中のタスクは完了まで走ります。",
            )}
          </Text>
        )}

        <Group justify="flex-end" mt="md">
          <Button variant="subtle" onClick={close} disabled={mutation.isPending}>
            {t("common.cancel", "キャンセル")}
          </Button>
          <Button
            color={stopped ? "teal" : "red"}
            loading={mutation.isPending}
            onClick={() => mutation.mutate()}
          >
            {stopped ? t("fleet.resume", "再開") : t("fleet.stop", "全停止")}
          </Button>
        </Group>
      </Modal>
    </>
  );
}
