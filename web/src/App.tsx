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
  IconDashboard,
  IconLanguage,
  IconList,
  IconMoon,
  IconPuzzle,
  IconRocket,
  IconScript,
  IconSun,
  IconUsersGroup,
} from "@tabler/icons-react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { NavLink as RouterNavLink, Route, Routes } from "react-router-dom";

import { api } from "@/api/client";
import { PageTransition } from "@/components/PageTransition";
import Dashboard from "./pages/Dashboard";
import Deploy from "./pages/Deploy";
import PluginPanel from "./pages/PluginPanel";
import ServiceLog from "./pages/ServiceLog";
import Workers from "./pages/Workers";
import Workloads from "./pages/Workloads";

const HEADER_LABEL_COLOR = "rgba(255,255,255,0.65)";
const HEADER_VALUE_COLOR = "rgba(255,255,255,0.95)";

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

  return (
    <Group gap="lg" wrap="nowrap" style={{ overflowX: "auto" }}>
      <Group gap={6} wrap="nowrap">
        <Text size="xs" style={{ color: HEADER_LABEL_COLOR }}>
          {t("header.control_plane")}
        </Text>
        <Text size="xs" fw={700} style={{ color: HEADER_VALUE_COLOR }}>
          {statusQ.data?.mode ?? "—"}
        </Text>
      </Group>
      <Group gap={6} wrap="nowrap">
        <Text size="xs" style={{ color: HEADER_LABEL_COLOR }}>
          {t("header.last_update")}
        </Text>
        <Text size="xs" style={{ color: HEADER_VALUE_COLOR, fontFamily: "ui-monospace, monospace" }}>
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

      <AppShell.Navbar p="xs">
        <NavLink
          label={t("nav.dashboard")}
          leftSection={<IconDashboard size={18} />}
          component={RouterNavLink}
          to="/"
          end
        />
        <NavLink
          label={t("nav.workloads")}
          leftSection={<IconList size={18} />}
          component={RouterNavLink}
          to="/workloads"
        />
        <NavLink
          label={t("nav.workers", "ワーカー")}
          leftSection={<IconUsersGroup size={18} />}
          component={RouterNavLink}
          to="/workers"
        />
        <NavLink
          label={t("nav.logs")}
          leftSection={<IconScript size={18} />}
          component={RouterNavLink}
          to="/logs"
        />
        <NavLink
          label={t("nav.deploy")}
          leftSection={<IconRocket size={18} />}
          component={RouterNavLink}
          to="/deploy"
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
            <Route path="/" element={<Dashboard />} />
            <Route path="/workloads" element={<Workloads />} />
            <Route path="/workloads/:slug" element={<Workloads />} />
            <Route path="/workloads/:slug/runs" element={<Workloads />} />
            <Route path="/workers" element={<Workers />} />
            <Route path="/logs" element={<ServiceLog />} />
            <Route path="/deploy" element={<Deploy />} />
            <Route path="/plugins/:slug" element={<PluginPanel />} />
          </Route>
        </Routes>
      </AppShell.Main>
    </AppShell>
  );
}
