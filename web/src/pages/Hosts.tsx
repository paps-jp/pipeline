import { Stack } from "@mantine/core";
import { useTranslation } from "react-i18next";

import { PageHeader } from "@/components/PageHeader";
import { HostConfigSection } from "./HostConfig";
import { RuntimeSection, WorkerRegistrySection } from "./Workers";
import { TabbedPage } from "@/components/TabbedPage";

// ホスト管理: ホストごとの能力/割り当て設定 + 物理ホストの登録簿 (label / IP / SSH / 公開鍵)。
// ワーカー稼働状態は /workers に分離。
export default function Hosts() {
  const { t } = useTranslation();
  return (
    <Stack gap="lg">
      <PageHeader
        title={t("hosts.title", "ホスト")}
        subtitle={t("hosts.subtitle", "ホストごとの能力・ワーカー割り当てと登録の管理")}
      />
      <TabbedPage
        defaultTab="config"
        tabs={[
          {
            value: "config",
            label: "設定・登録",
            content: (
              <Stack gap="lg">
                <HostConfigSection />
                <WorkerRegistrySection />
              </Stack>
            ),
          },
          { value: "workers", label: "稼働ワーカー", content: <RuntimeSection /> },
        ]}
      />
    </Stack>
  );
}
