import { Tabs } from "@mantine/core";
import type { ReactNode } from "react";
import { useSearchParams } from "react-router-dom";

export interface TabDef {
  value: string;
  label: string;
  content: ReactNode;
}

// 複数の画面を 1 つのサイドバー項目にまとめるための共通タブ枠。
// 選択中のタブは ?tab= に持つので、リンク共有・リロードで保たれる。
// forced を渡すと URL パス由来 (旧 /throughput 等) の指定が優先される。
export function TabbedPage({
  tabs,
  defaultTab,
  forced,
}: {
  tabs: TabDef[];
  defaultTab?: string;
  forced?: string;
}) {
  const [params, setParams] = useSearchParams();
  const wanted = forced ?? params.get("tab") ?? defaultTab ?? tabs[0].value;
  const active = tabs.some((t) => t.value === wanted) ? wanted : tabs[0].value;

  return (
    <Tabs
      value={active}
      onChange={(v) => {
        if (!v) return;
        const next = new URLSearchParams(params);
        next.set("tab", v);
        setParams(next, { replace: true });
      }}
      keepMounted={false}
    >
      <Tabs.List mb="md">
        {tabs.map((t) => (
          <Tabs.Tab key={t.value} value={t.value}>
            {t.label}
          </Tabs.Tab>
        ))}
      </Tabs.List>
      {tabs.map((t) => (
        <Tabs.Panel key={t.value} value={t.value}>
          {t.content}
        </Tabs.Panel>
      ))}
    </Tabs>
  );
}
