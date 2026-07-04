import { Stack } from "@mantine/core";
import { useTranslation } from "react-i18next";

import { PageHeader } from "@/components/PageHeader";
import { WorkerRegistrySection } from "./Workers";

// ホスト管理: ワーカーを動かす物理ホストの登録簿 (label / IP / SSH / 公開鍵)。
// ワーカー稼働状態は /workers に分離。
export default function Hosts() {
  const { t } = useTranslation();
  return (
    <Stack gap="lg">
      <PageHeader
        title={t("hosts.title", "ホスト")}
        subtitle={t("hosts.subtitle", "ワーカーを動かす物理ホストの登録・管理")}
      />
      <WorkerRegistrySection />
    </Stack>
  );
}
