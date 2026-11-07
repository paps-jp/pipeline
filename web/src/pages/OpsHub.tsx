import { useLocation } from "react-router-dom";

import { TabbedPage } from "@/components/TabbedPage";
import ConfigPage from "./ConfigPage";
import Deploy from "./Deploy";
import ServiceLog from "./ServiceLog";
import Settings from "./Settings";

// 運用: ログ / デプロイ / 構成 (config as code) / LLM advisor (旧「設定」)
const PATH_TAB: Record<string, string> = {
  "/logs": "logs",
  "/deploy": "deploy",
  "/config": "config",
  "/settings": "llm",
};

export default function OpsHub() {
  const { pathname } = useLocation();
  return (
    <TabbedPage
      defaultTab="logs"
      forced={PATH_TAB[pathname]}
      tabs={[
        { value: "logs", label: "ログ", content: <ServiceLog /> },
        { value: "deploy", label: "デプロイ", content: <Deploy /> },
        { value: "config", label: "構成", content: <ConfigPage /> },
        { value: "llm", label: "LLM advisor", content: <Settings /> },
      ]}
    />
  );
}
