import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Vite 設定
//   - dev: http://localhost:5173 で起動、/api を FastAPI (8000) に proxy
//   - build: ../pipeline/web/static/ に出力 (FastAPI が serve)
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "../pipeline/web/static",
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": process.env.VITE_API_TARGET || "http://localhost:8000",
      "/docs": process.env.VITE_API_TARGET || "http://localhost:8000",
      "/openapi.json": process.env.VITE_API_TARGET || "http://localhost:8000",
    },
  },
});
