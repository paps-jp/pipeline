import { useLocation } from "react-router-dom";

import { TabbedPage } from "@/components/TabbedPage";
import Dashboard from "./Dashboard";
import Flow from "./Flow";
import Throughput from "./Throughput";

// 状況: フロー図 / スループット / 概要 (旧 Dashboard)
const PATH_TAB: Record<string, string> = { "/flow": "flow", "/throughput": "throughput" };

export default function StatusHub() {
  const { pathname } = useLocation();
  return (
    <TabbedPage
      defaultTab="flow"
      forced={PATH_TAB[pathname]}
      tabs={[
        { value: "flow", label: "フロー図", content: <Flow /> },
        { value: "throughput", label: "スループット", content: <Throughput /> },
        { value: "overview", label: "概要", content: <Dashboard /> },
      ]}
    />
  );
}
