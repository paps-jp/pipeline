import { Button, Center, Stack, Text, Title } from "@mantine/core";
import { Component, type ErrorInfo, type ReactNode } from "react";

import { ErrorState } from "@/components/states";

/**
 * トップレベルの安全網。 このバウンダリが無いと未捕捉例外で React ツリーが
 * まるごとアンマウントされ、 画面が白紙になる (2026-08-26: crawl-tables の
 * 新規追加フォームで再現報告があった。 原因未特定だが、 原因が何であれ
 * 白紙より「エラー内容が見えて reload できる」方が調査・復旧しやすい)。
 */
export class ErrorBoundary extends Component<
  { children: ReactNode },
  { error: unknown }
> {
  state: { error: unknown } = { error: null };

  static getDerivedStateFromError(error: unknown) {
    return { error };
  }

  componentDidCatch(error: unknown, info: ErrorInfo) {
    // eslint-disable-next-line no-console
    console.error("ErrorBoundary caught:", error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <Center mih="100vh" p="xl">
          <Stack gap="md" maw={640}>
            <Title order={3}>画面の描画中にエラーが発生しました</Title>
            <Text size="sm" c="dimmed">
              直前の操作 (フォーム入力・貼り付け等) が原因の可能性があります。
              下のエラー内容を控えたうえで再読み込みしてください。
            </Text>
            <ErrorState error={this.state.error} />
            <Button onClick={() => window.location.reload()}>再読み込み</Button>
          </Stack>
        </Center>
      );
    }
    return this.props.children;
  }
}
