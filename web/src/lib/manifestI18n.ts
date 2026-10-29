/**
 * plugin.yaml 由来テキストの言語選択。
 *
 * manifest は plugin 側が権威なので、 UI の i18n.ts には取り込まず
 * yaml の `label_en` / `help_en` / `group_en` / `description_en` を使う。
 * 未翻訳の plugin は日本語のままフォールバックする (= 表示は壊れない)。
 */
import type { PluginKwargField, PluginManifest } from "@/api/client";
import i18n from "@/i18n";

function preferEn(): boolean {
  return (i18n.language || "").toLowerCase().startsWith("en");
}

function pick(en: string | undefined, ja: string | undefined): string | undefined {
  return (preferEn() && en) || ja || undefined;
}

export function fieldLabel(f: PluginKwargField): string {
  return pick(f.label_en, f.label) ?? f.key;
}

export function fieldHelp(f: PluginKwargField): string | undefined {
  return pick(f.help_en, f.help);
}

export function fieldGroup(f: PluginKwargField): string | undefined {
  return pick(f.group_en, f.group);
}

export function manifestDescription(m: PluginManifest | null | undefined): string | undefined {
  return m ? pick(m.description_en, m.description) : undefined;
}
