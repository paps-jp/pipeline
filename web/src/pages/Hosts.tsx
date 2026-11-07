import { Stack } from "@mantine/core";
import { useTranslation } from "react-i18next";

import { PageHeader } from "@/components/PageHeader";
import { HostFleet } from "./HostFleet";
import { RuntimeSection, WorkerRegistrySection } from "./Workers";
import { TabbedPage } from "@/components/TabbedPage";

// ホスト: 設定 (台数・許可・能力) / 稼働ワーカー / 登録 (物理ホストの label・IP・SSH・公開鍵) の3タブ。
// 日常操作は「設定」。登録はサーバーを足す・直すときだけ使う。
export default function Hosts() {
  const { t } = useTranslation();
  return (
    <Stack gap="lg">
      <PageHeader
        title={t("hosts.title", "ホスト")}
        subtitle={t("hosts.subtitle", "ホストごとの台数・許可の設定と、ホストの登録")}
      />
      <TabbedPage
        defaultTab="config"
        tabs={[
          { value: "config", label: t("hosts.tab_config", "ホスト設定"), content: <HostFleet /> },
          { value: "workers", label: t("hosts.tab_workers", "稼働ワーカー"), content: <RuntimeSection /> },
          { value: "registry", label: t("hosts.tab_registry", "ホスト登録"), content: <WorkerRegistrySection /> },
        ]}
      />
    </Stack>
  );
}
