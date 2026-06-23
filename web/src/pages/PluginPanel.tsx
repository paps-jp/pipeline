import { Text } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { api } from "@/api/client";

/**
 * プラグイン提供の panel.html を iframe で表示する汎用ページ。
 * plugin.yaml に `ui_panel: true` が宣言されたプラグインのみここに来る。
 * manifest の `ui_panel_mode` を iframe URL の `?mode=` で渡す。
 */
export default function PluginPanel() {
  const { t } = useTranslation();
  const { slug } = useParams<{ slug: string }>();
  const safeSlug = slug ?? "";

  const pluginsQ = useQuery({
    queryKey: ["available-plugins-sidebar"],
    queryFn: () => api.listAvailablePlugins(),
    staleTime: 30_000,
  });
  const mode =
    pluginsQ.data?.plugins?.find((p) => p.slug === safeSlug)?.manifest
      ?.ui_panel_mode ?? "video";
  // panel.html を編集して再配信した時に、 ブラウザの iframe キャッシュが古い HTML を
  // 握り続ける事故が出る。 サーバ側で no-cache を返すようにしたが、 既キャッシュ済みの
  // iframe 内容には効かないので、 ページ navigation 毎に新 src を生成して強制 fetch。
  const cacheBust = useMemo(() => Date.now(), [safeSlug]);
  const src = `/api/v1/plugins/${encodeURIComponent(safeSlug)}/web/panel.html?mode=${encodeURIComponent(mode)}&_=${cacheBust}`;

  if (!safeSlug) {
    return (
      <Text c="red">{t("plugin_panel.invalid_slug", "プラグイン slug が不明")}</Text>
    );
  }

  return (
    <iframe
      src={src}
      title={`${safeSlug} panel`}
      style={{
        display: "block",
        width: "calc(100% + var(--mantine-spacing-md, 16px) * 2)",
        height: "calc(100vh - 88px)",
        border: 0,
        background: "transparent",
        margin: "calc(var(--mantine-spacing-md, 16px) * -1)",
      }}
    />
  );
}
