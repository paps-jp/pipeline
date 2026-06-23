# rss-news-summarize

RSS / Atom の 1 エントリ = 1 タスクで 本文を 取得し、 Claude haiku で 数行に 要約する。

[`rss-news-dispatcher`](../rss_news_dispatcher/) と セットで 使う:

```
[YAML feeds list]              [pipeline-oss API]
       │                         POST /api/v1/workloads/.../tasks/batch
       ▼                                ▲
rss-news-dispatcher  ──────────────────┘
  (self-loop, 30 min)
                                pk = entry URL
                                extra = {feed_name, title, summary_html, published}
                                        │
                                        ▼
                              rss-news-summarize
                                  (1 task = 1 entry)
                                        │
                                        ▼  Claude haiku
                                        ▼  + plugin SQLite に 永続化
                                runs.output_json
```

## なぜ 2 プラグインに 分けるか

- **dispatcher** は ネットワーク I/O bound、 1 ワーカー専有で OK
- **summarize** は LLM I/O bound、 ワーカー N 台で 並列に スケール
- pipeline-oss の キュー が この 2 段を きれいに 繋ぐ (= self-loop)

## セットアップ

```bash
# 1) 制御プレーンの venv に 依存追加
pip install anthropic feedparser PyYAML trafilatura beautifulsoup4

# 2) ANTHROPIC_API_KEY を service 環境変数 で 与える
sudo systemctl edit pipeline-oss
#   [Service]
#   Environment="ANTHROPIC_API_KEY=sk-ant-..."

# 3) Web UI から 2 つ workload 作成:
#    a) rss-news-dispatcher  (defaults で OK)
#    b) rss-news-summarize   (model = claude-haiku-4-5-20251001)
```

## フィード一覧の 編集

[`../rss_news_dispatcher/feeds.example.yaml`](../rss_news_dispatcher/feeds.example.yaml)
を コピーして 自分の フィードに 差し替えれば OK。 YAML / 1 行 1 URL の TXT 両対応。

## 出力

- **runs.output_json** に 1 件ずつ `{url, feed, title, model, summary, latency_ms, input_tokens, output_tokens}` が 入る → Web UI の Runs Drawer で 直接 読める
- **`summaries.sqlite`** に 永続化 (再要約 スキップに 使われる + 別ツールから grep / 集計可能)
- 既に DB に 同じ URL が あれば `cached: true` で 即 return (= LLM 呼び出し スキップ)

## チューニング

| 項目 | 効果 |
|---|---|
| `model` を `claude-sonnet-4-6` に | 品質向上 / コスト 3-5x |
| `summary_sentences` を 5 に | 1 記事を 詳しく |
| `fetch_full_article=false` | RSS summary だけで 要約 (速いが 浅い) |
| `max_input_chars` を 落とす | 長文記事の トークン爆発 抑制 |

## トラブルシュート

- **403 / 429** が 頻発 → `User-Agent` の 変更 or 同一ホスト並列度を 下げる (dispatcher の interval_s を 増やす)
- **本文が "no article text"** → JS rendering 必須サイト。 `fetch_full_article=false` で RSS summary に フォールバック
- **同じ記事を 何度も 要約してる** → `store_db_path` が 揮発領域 (/tmp 等) を 見てないか 確認
