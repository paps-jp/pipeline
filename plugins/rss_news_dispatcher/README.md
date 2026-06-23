# rss-news-dispatcher

登録した RSS / Atom フィードを 定期 fetch し、 まだ処理していない 新エントリだけを
[`rss-news-summarize`](../rss_news_summarize/) の queue に batch enqueue する。

self-loop 方式 (= 1 tick 完了 → sleep(interval_s) → 次 tick を pipeline-oss API へ
self-enqueue) で 1 ワーカーを 専有する。

## dedup の 仕組み

URL 単位で per-plugin SQLite (`state_db_path`) に 永続化。 同じ URL は 二度 enqueue
されない (= 同じ記事を 二度要約しない)。 dispatcher を 再起動しても 既見 履歴は
残るので 重複 enqueue は 起きない。

## フィード一覧ファイル

YAML (推奨):

```yaml
feeds:
  - name: Publickey
    url: https://www.publickey1.jp/atom.xml
  - name: Hacker News
    url: https://hnrss.org/frontpage
```

TXT (1 行 1 URL、 `#` 行頭 コメント可):

```text
# JP テック
https://www.publickey1.jp/atom.xml
https://gigazine.net/news/rss_2.0/

# EN テック
https://hnrss.org/frontpage
```

[`feeds.example.yaml`](feeds.example.yaml) を コピーして 編集するのが 早い。

## 観測

- **runs.output_json** に 毎 tick `{tick, feeds_total, entries_seen, entries_new, enqueued, errors, dispatch_secs}` が 入る → Web UI の Runs Drawer で 直接 読める
- フィード単位の 失敗 (404 / timeout) は `errors[]` に エントリ毎 サマリ。 落ちた 1 フィードで tick 全体は 落ちない

## ペア

→ 続きの 1 タスク = 1 エントリで Claude 要約する [`rss-news-summarize`](../rss_news_summarize/) を 参照。
