import { useLocation, useParams } from "react-router-dom";

import { TabbedPage } from "@/components/TabbedPage";
import Supervisor from "./Supervisor";
import Workloads from "./Workloads";

// ワークロード: 一覧 / 自動配分 (旧 Supervisor)
export default function WorkloadsHub() {
  const { pathname } = useLocation();
  const { slug } = useParams();
  const forced = slug ? "list" : pathname === "/supervisor" ? "supervisor" : undefined;
  return (
    <TabbedPage
      defaultTab="list"
      forced={forced}
      tabs={[
        { value: "list", label: "ワークロード", content: <Workloads /> },
        { value: "supervisor", label: "自動配分 (Supervisor)", content: <Supervisor /> },
      ]}
    />
  );
}
