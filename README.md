# Pipeline

> 空きGPUをバッチで埋め尽くす軽量 batch fleet — MIG が使えない GPU (RTX 4090 / A6000 / L4 等)
> でも、 1 枚に複数 workload を同居させて、 単一ジョブでは 40-60% アイドルになる GPU を取り戻す。
> 1-10 GPU の AI チーム向け。

📖 **ドキュメント**: <https://paps-jp.github.io/pipeline/> ([セットアップ](https://paps-jp.github.io/pipeline/setup.html) / [マニュアル](https://paps-jp.github.io/pipeline/manual.html))
🧪 **配布元**: [ぱっぷすラボ](https://paps-jp.github.io/)

## 何ができる

- ループ処理 (1 行 → 1 タスク) を fleet で並列実行
- ワークロード定義は **GUI から登録** (executor 型 = shell / http / sql / python_module / container)
- 入力ソースは DB queue / SQL select / file glob / HTTP poll の中から選択
- スケジューラ (priority + weight) は組込み Optimizer が自動チューニング
- ライブログ / メトリクス / 再起動ボタン / マニュアル全部 web UI で完結
- 単一バイナリ的に動く (SQLite + 単一プロセス mode あり)
- スケールアウト時は worker を追加 → 自動 join

## こんな人向け

- 「CLI ツールを fleet で回したい」開発者
- Ray は大袈裟 / Airflow は重い、と感じている人
- バッチを cron 数本で運用してたが管理が辛い小〜中規模チーム
- **Python は書けないが SQL は読める** くらいの社内オペレータ

## 状態

Pre-alpha — paps-jp の face_search で dogfooding 中。OSS としての切り出しと
リファクタは進行中 (このリポジトリ = その作業中の monorepo)。

## クイックスタート (TBD)

```bash
pip install pipeline   # 未公開
pipeline run --dev     # SQLite + 単一プロセス、http://localhost:8000
```

## リンク

- 📖 ドキュメント: <https://paps-jp.github.io/pipeline/>
- 🧪 ぱっぷすラボ: <https://paps-jp.github.io/>
- 💻 リポジトリ: <https://github.com/paps-jp/pipeline>

## ライセンス

[MIT](./LICENSE)

## スタック

- Python 3.12+
- FastAPI + Pydantic v2 (REST API)
- React + TypeScript + Vite (Web UI)
- SQLite / PostgreSQL / MariaDB (Tier 1)
- 認証: ローカル users + scrypt + API key
- i18n: 日本語 / 英語 (react-i18next)
