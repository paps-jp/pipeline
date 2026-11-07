import { useLocation } from "react-router-dom";

import { TabbedPage } from "@/components/TabbedPage";
import Deploy from "./Deploy";
import ServiceLog from "./ServiceLog";
import Settings from "./Settings";

// 運用: ログ / デプロイ / LLM advisor (旧「設定」)
const PATH_TAB: Record<string, string> = {
  "/logs": "logs",
  "/deploy": "deploy",
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
        { value: "llm", label: "LLM advisor", content: <Settings /> },
      ]}
    />
  );
}
