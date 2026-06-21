import { createTheme, MantineColorsTuple } from "@mantine/core";

/** モダンな indigo accent (= Linear / Vercel 系)。 */
const indigo: MantineColorsTuple = [
  "#eef0ff",
  "#dde1ff",
  "#b6bfff",
  "#8e9bff",
  "#6e7dff",
  "#5868ff",
  "#4c5dff",
  "#3e4eee",
  "#3645d4",
  "#2a39bb",
];

export const theme = createTheme({
  primaryColor: "indigo",
  primaryShade: { light: 6, dark: 5 },
  colors: { indigo },
  fontFamily:
    'Inter, -apple-system, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif',
  fontFamilyMonospace:
    '"JetBrains Mono", ui-monospace, Menlo, Consolas, "Courier New", monospace',
  headings: {
    fontFamily:
      'Inter, -apple-system, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif',
    fontWeight: "700",
    sizes: {
      h1: { fontSize: "1.9rem", lineHeight: "1.2" },
      h2: { fontSize: "1.45rem", lineHeight: "1.25" },
      h3: { fontSize: "1.15rem", lineHeight: "1.3" },
      h4: { fontSize: "1.0rem", lineHeight: "1.35" },
    },
  },
  defaultRadius: "md",
  cursorType: "pointer",
  components: {
    Card: {
      defaultProps: {
        withBorder: true,
        radius: "md",
      },
      styles: {
        root: {
          transition: "border-color 120ms ease, box-shadow 120ms ease, transform 120ms ease",
        },
      },
    },
    Button: {
      defaultProps: { radius: "md" },
    },
    NavLink: {
      styles: {
        root: {
          borderRadius: 8,
          transition: "background 120ms ease",
        },
      },
    },
    Code: {
      styles: {
        root: { fontSize: "0.82em" },
      },
    },
  },
});
