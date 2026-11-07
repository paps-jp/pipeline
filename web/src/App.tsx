import {
  ActionIcon,
  AppShell,
  Burger,
  Group,
  NavLink,
  Text,
  Title,
  Tooltip,
  useMantineColorScheme,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconBook2,
  IconLanguage,
  IconLink,
  IconList,
  IconMoon,
  IconPuzzle,
  IconScript,
  IconServer,
  IconSitemap,
  IconSun,
} from "@tabler/icons-react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, NavLink as RouterNavLink, Navigate, Route, Routes } from "react-router-dom";

import { api } from "@/api/client";
import { FleetStopButton } from "@/components/FleetStopButton";
import { PageTransition } from "@/components/PageTransition";
import CrawlHub from "./pages/CrawlHub";
import Hosts from "./pages/Hosts";
import OpsHub from "./pages/OpsHub";
import PluginPanel from "./pages/PluginPanel";
import StatusHub from "./pages/StatusHub";
import WorkloadsHub from "./pages/WorkloadsHub";

function HeaderStatus() {
  const { t } = useTranslation();
  const statusQ = useQuery({
    queryKey: ["status"],
    queryFn: () => api.status(),
    refetchInterval: 5_000,
  });
  const lastUpdate = statusQ.data?.now
    ? new Date(statusQ.data.now).toLocaleString()
    : "—";

  // テーマ対応: Mantine の標準色トークンを使うことで light/dark で自動反転
  // (= 旧 hard-coded white は light テーマで白背景に白文字になり見えなかった)。
  return (
    <Group gap="lg" wrap="nowrap" style={{ overflowX: "auto" }}>
      <Group gap={6} wrap="nowrap">
        <Text size="xs" c="dimmed">
          {t("header.last_update")}
        </Text>
        <Text size="xs" style={{ fontFamily: "ui-monospace, monospace" }}>
          {lastUpdate}
        </Text>
      </Group>
    </Group>
  );
}

export default function App() {
  const [opened, { toggle }] = useDisclosure();
  const { t, i18n } = useTranslation();
  const { colorScheme, toggleColorScheme } = useMantineColorScheme();

  const pluginsQ = useQuery({
    queryKey: ["available-plugins-sidebar"],
    queryFn: () => api.listAvailablePlugins(),
    refetchInterval: 60_000,
  });
  // workload の name を引きたい (= plugin.yaml の name は slug 同形のため見栄えが悪い)。
  // plugin slug は underscore、 workload slug は dash なので _ → - で対応付けする。
  const workloadsQ = useQuery({
    queryKey: ["workloads-for-sidebar"],
    queryFn: () => api.listWorkloads(),
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
  const workloadNameMap = new Map(
    (workloadsQ.data?.workloads ?? []).map((w) => [w.slug, w.name])
  );
  const uiPlugins =
    pluginsQ.data?.plugins?.filter((p) => p.has_ui_panel) ?? [];

  const toggleLang = () => {
    void i18n.changeLanguage(i18n.language.startsWith("ja") ? "en" : "ja");
  };

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{ width: 220, breakpoint: "sm", collapsed: { mobile: !opened } }}
      padding="md"
    >
      <AppShell.Header
        data-pipeline-header
        style={{
          background:
            colorScheme === "dark"
              ? "linear-gradient(180deg, rgba(15,17,32,0.92) 0%, rgba(15,17,32,0.76) 100%)"
              : "linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(255,255,255,0.76) 100%)",
          borderBottom:
            colorScheme === "dark"
              ? "1px solid rgba(110,125,255,0.18)"
              : "1px solid rgba(110,125,255,0.22)",
          color: colorScheme === "dark" ? "#e6e8f5" : "#1f2233",
        }}
      >
        <Group h="100%" px="md" justify="space-between" wrap="nowrap">
          <Group gap="md" wrap="nowrap">
            <Burger
              opened={opened}
              onClick={toggle}
              hiddenFrom="sm"
              size="sm"
            />
            <RouterLink
              to="/"
 aria-label="Home"
              style={{ display: "inline-flex", lineHeight: 0 }}
            >
              <img
                src="/logo.png"
                alt="pipeline-oss logo"
                height={36}
                style={{ display: "block", filter: "drop-shadow(0 2px 4px rgba(110,125,255,0.25))" }}
              />
            </RouterLink>
            <Title
              order={3}
              style={{
                fontWeight: 800,
                letterSpacing: "-0.01em",
                margin: 0,
                background:
                  "linear-gradient(135deg, var(--mantine-color-indigo-5) 0%, var(--mantine-color-indigo-7) 100%)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
                backgroundClip: "text",
              }}
            >
              {t("app.title")}
            </Title>
            <HeaderStatus />
          </Group>
          <Group gap="xs">
            <FleetStopButton />
            <Tooltip label={t("header.manual", "マニュアル (新しいタブで開く)")}>
              <ActionIcon
                variant="subtle"
                color="indigo"
                component="a"
                href="https://paps-jp.github.io/pipeline/"
                target="_blank"
                rel="noopener noreferrer"
                aria-label="Manual"
              >
                <IconBook2 size={18} />
              </ActionIcon>
            </Tooltip>
            <Tooltip label={t("header.toggle_lang")}>
              <ActionIcon
                variant="subtle"
                color="indigo"
                onClick={toggleLang}
                aria-label="Toggle language"
              >
                <IconLanguage size={18} />
              </ActionIcon>
            </Tooltip>
            <Tooltip label={t("header.toggle_theme")}>
              <ActionIcon
                variant="subtle"
                color="indigo"
                onClick={toggleColorScheme}
                aria-label="Toggle theme"
              >
                {colorScheme === "dark" ? <IconSun size={18} /> : <IconMoon size={18} />}
              </ActionIcon>
            </Tooltip>
          </Group>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar p="xs" style={{ overflowY: "auto" }}>
        <NavLink
          label={t("nav.status", "状況")}
          leftSection={<IconSitemap size={18} />}
          component={RouterNavLink}
          to="/status"
        />
        <NavLink
          label={t("nav.workloads", "ワークロード")}
          leftSection={<IconList size={18} />}
          component={RouterNavLink}
          to="/workloads"
        />
        <NavLink
          label={t("nav.hosts", "ホスト")}
          leftSection={<IconServer size={18} />}
          component={RouterNavLink}
          to="/hosts"
        />
        <NavLink
          label={t("nav.crawl", "クロール")}
          leftSection={<IconLink size={18} />}
          component={RouterNavLink}
          to="/crawl"
        />
        <NavLink
          label={t("nav.ops", "運用")}
          leftSection={<IconScript size={18} />}
          component={RouterNavLink}
          to="/ops"
        />
        {uiPlugins.length > 0 && (
          <Text size="xs" c="dimmed" px="sm" mt="md" mb={4}>
            {t("nav.plugins", "プラグイン")}
          </Text>
        )}
        {uiPlugins.map((p) => {
          const workloadSlug = p.slug.replace(/_/g, "-");
          const label =
            workloadNameMap.get(workloadSlug) ?? p.manifest?.name ?? p.slug;
          return (
          <NavLink
            key={p.slug}
            label={label}
            title={p.slug}
            leftSection={<IconPuzzle size={18} />}
            component={RouterNavLink}
            to={`/plugins/${p.slug}`}
          />
          );
        })}
      </AppShell.Navbar>

      <AppShell.Main>
        <Routes>
          <Route element={<PageTransition />}>
            <Route path="/" element={<Navigate to="/status" replace />} />
            <Route path="/status" element={<StatusHub />} />
            <Route path="/flow" element={<StatusHub />} />
            <Route path="/throughput" element={<StatusHub />} />
            <Route path="/workloads" element={<WorkloadsHub />} />
            <Route path="/workloads/:slug" element={<WorkloadsHub />} />
            <Route path="/workloads/:slug/runs" element={<WorkloadsHub />} />
            <Route path="/supervisor" element={<WorkloadsHub />} />
            <Route path="/hosts" element={<Hosts />} />
            <Route path="/workers" element={<Navigate to="/hosts?tab=workers" replace />} />
            <Route path="/crawl" element={<CrawlHub />} />
            <Route path="/url-management" element={<CrawlHub />} />
            <Route path="/site-yield" element={<CrawlHub />} />
            <Route path="/crawl-tables" element={<CrawlHub />} />
            <Route path="/ops" element={<OpsHub />} />
            <Route path="/logs" element={<OpsHub />} />
            <Route path="/deploy" element={<OpsHub />} />
            <Route path="/config" element={<OpsHub />} />
            <Route path="/settings" element={<OpsHub />} />
            <Route path="/plugins/:slug" element={<PluginPanel />} />
          </Route>
        </Routes>
      </AppShell.Main>
    </AppShell>
  );
}
