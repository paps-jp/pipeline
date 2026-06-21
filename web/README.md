# Pipeline Web UI

React + TypeScript + Vite + Mantine UI による管理画面。

## 開発時

```bash
# 1. 依存インストール (初回のみ)
cd web
npm install

# 2. FastAPI backend を別ターミナルで起動
cd ..
.venv/Scripts/pipeline run --dev    # http://localhost:8000

# 3. Vite dev server 起動
cd web
npm run dev                          # http://localhost:5173
```

`http://localhost:5173` で UI が見えます。`/api` パスは自動で 8000 に proxy。

## 本番ビルド

```bash
cd web
npm run build
# 出力: ../pipeline/web/static/
# FastAPI が起動時に検出 → / でその index.html を返す
```

## 構成

```
web/
├── package.json
├── vite.config.ts         dev proxy + build output to ../pipeline/web/static
├── tsconfig.json
├── index.html
├── src/
│   ├── main.tsx           App エントリ (Mantine + React Query + Router + i18n)
│   ├── App.tsx            AppShell + nav + routes
│   ├── i18n.ts            ja / en の翻訳
│   ├── api/
│   │   └── client.ts      typed fetch wrapper
│   └── pages/
│       ├── Dashboard.tsx  ステータス表示
│       └── Workloads.tsx  ワークロード一覧 + enable toggle + delete
└── .gitignore
```

## スタック

- Vite + React 18 + TypeScript
- Mantine v7 (UI component library)
- TanStack Query v5 (API state)
- React Router v6
- i18next + react-i18next (ja/en)
- Tabler Icons
