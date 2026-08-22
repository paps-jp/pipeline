/**
 * ヘッダの「全停止 / 検索以外全停止 / 再開」ボタン。
 *
 * 停止: いま enabled=1 の workload を記録してから止める。
 * 再開: その記録にあるものだけを戻す (= もともと止めていたものは動かさない)。
 *
 * 「検索以外全停止」は faiss-api (= resident_service で動く顔検索の本体) を対象から
 * 外す。 全停止すると検索まで落ちるため、 パイプラインだけ黙らせたい場面 —— FAISS
 * 不調で face-person-link を止めたい時など —— で検索を巻き添えにしないための導線。
 * 除外する slug はサーバが state.search_slugs で配る (UI 側にハードコードしない)。
 *
 * 停止も再開も影響範囲が大きいので、 実行前に対象を出して確認を取る。
 */
import { Badge, Button, Group, List, Modal, Text, Tooltip } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { IconPlayerPlay, IconPlayerStop, IconSearch } from "@tabler/icons-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { api } from "@/api/client";

/** モーダルを開いた理由。 null = 閉じている。 */
type Mode = "all" | "except_search" | "resume";

export function FleetStopButton() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const [mode, setMode] = useState<Mode | null>(null);
  const close = () => setMode(null);

  const { data } = useQuery({
    queryKey: ["fleet-state"],
    queryFn: () => api.fleetState(),
    refetchInterval: 5000,
  });

  const stopped = data?.stopped ?? false;
  const enabledNow = useMemo(() => data?.enabled_now ?? [], [data]);
  const searchSlugs = useMemo(() => data?.search_slugs ?? [], [data]);

  /** 「検索以外全停止」で残る (= いま動いている検索系) slug。 */
  const kept = useMemo(
    () => enabledNow.filter((s) => searchSlugs.includes(s)),
    [enabledNow, searchSlugs],
  );

  const targets = stopped
    ? (data?.recorded_slugs ?? [])
    : mode === "except_search"
      ? enabledNow.filter((s) => !searchSlugs.includes(s))
      : enabledNow;

  const mutation = useMutation({
    mutationFn: () =>
      stopped
        ? api.fleetResume()
        : api.fleetStop(undefined, mode === "except_search"),
    onSuccess: (res) => {
      close();
      notifications.show({
        color: res.failed.length ? "orange" : stopped ? "teal" : "red",
        title: stopped
          ? t("fleet.resumed", "再開しました")
          : mode === "except_search"
            ? t("fleet.stopped_except_search", "検索以外を停止しました")
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
      <Group gap={6} wrap="nowrap">
        {stopped ? (
          <Tooltip
            label={t("fleet.tip_resume", "停止前に動いていた workload を再開します")}
          >
            <Button
              size="xs"
              variant="filled"
              color="teal"
              leftSection={<IconPlayerPlay size={16} />}
              onClick={() => setMode("resume")}
            >
              {t("fleet.resume", "再開")}
              {targets.length > 0 && (
                <Badge ml="xs" size="sm" variant="white" color="teal">
                  {targets.length}
                </Badge>
              )}
            </Button>
          </Tooltip>
        ) : (
          <>
            <Tooltip
              label={t(
                "fleet.tip_stop_except_search",
                "顔検索 (faiss-api) は動かしたまま、 他の workload を止めます",
              )}
            >
              <Button
                size="xs"
                variant="light"
                color="orange"
                leftSection={<IconSearch size={16} />}
                onClick={() => setMode("except_search")}
              >
                {t("fleet.stop_except_search", "検索以外全停止")}
              </Button>
            </Tooltip>
            <Tooltip
              label={t(
                "fleet.tip_stop",
                "稼働中の workload を記録してから全部止めます (顔検索も落ちます)",
              )}
            >
              <Button
                size="xs"
                variant="light"
                color="red"
                leftSection={<IconPlayerStop size={16} />}
                onClick={() => setMode("all")}
              >
                {t("fleet.stop", "全停止")}
              </Button>
            </Tooltip>
          </>
        )}
      </Group>

      <Modal
        opened={mode !== null}
        onClose={close}
        title={
          stopped
            ? t("fleet.modal_resume", "パイプラインを再開しますか")
            : mode === "except_search"
              ? t("fleet.modal_stop_except_search", "顔検索を残して停止しますか")
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
            : mode === "except_search"
              ? t(
                  "fleet.desc_stop_except_search",
                  "顔検索を残したまま、以下の workload を記録してから停止します。記録は再開時に使います。",
                )
              : t(
                  "fleet.desc_stop",
                  "現在稼働中の以下の workload を記録してから停止します。記録は再開時に使います。",
                )}
        </Text>

        {!stopped && mode === "except_search" && (
          <Text size="sm" mb="sm" c="teal.4">
            {kept.length > 0
              ? t("fleet.keep_running", "稼働のまま残す") + `: ${kept.join(", ")}`
              : t(
                  "fleet.keep_none",
                  "検索系 workload はいま稼働していません (残すものがありません)",
                )}
          </Text>
        )}

        {!stopped && mode === "all" && kept.length > 0 && (
          <Text size="sm" mb="sm" c="red.4">
            {t("fleet.warn_search_dies", "顔検索も止まります") + `: ${kept.join(", ")}`}
          </Text>
        )}

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
            color={stopped ? "teal" : mode === "except_search" ? "orange" : "red"}
            loading={mutation.isPending}
            disabled={!stopped && targets.length === 0}
            onClick={() => mutation.mutate()}
          >
            {stopped
              ? t("fleet.resume", "再開")
              : mode === "except_search"
                ? t("fleet.stop_except_search", "検索以外全停止")
                : t("fleet.stop", "全停止")}
          </Button>
        </Group>
      </Modal>
    </>
  );
}
