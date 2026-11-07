import { useLocation } from "react-router-dom";

import { TabbedPage } from "@/components/TabbedPage";
import MariadbTables from "./MariadbTables";
import SiteYield from "./SiteYield";
import UrlManagement from "./UrlManagement";

// クロール: URL管理 / サイト収率 / クロール設定
const PATH_TAB: Record<string, string> = {
  "/url-management": "urls",
  "/site-yield": "yield",
  "/crawl-tables": "tables",
};

export default function CrawlHub() {
  const { pathname } = useLocation();
  return (
    <TabbedPage
      defaultTab="urls"
      forced={PATH_TAB[pathname]}
      tabs={[
        { value: "urls", label: "URL管理", content: <UrlManagement /> },
        { value: "yield", label: "サイト収率", content: <SiteYield /> },
        { value: "tables", label: "クロール設定", content: <MariadbTables /> },
      ]}
    />
  );
}
