# echo_sample

pipeline-oss プラグインの最小サンプル。

> ドキュメント: <https://paps-jp.github.io/pipeline/>

## 何をするか

- task の `pk` と `extra` を print + `runs.output_json` に echo
- 任意で 1 タスク毎に `sleep_secs` 秒 sleep
- 任意で 「`pk` がこの部分文字列を含むなら失敗」 でリトライ / dead-letter テスト

## 使い方

1. Web UI (= `http://<control>:8001`) を開く
2. **ワークロード** タブ → **新規作成**
3. 実行モード: **python_module**
4. プラグイン: `echo-sample` を選択
5. モジュール: `main` を選択
6. 設定:
   - `prefix`: お好み (デフォルト `[echo]`)
   - `sleep_secs`: 0 = 即返却 / 1.0 = 1 秒スリープ
   - `fail_pk_substr`: 空文字 = 全成功 / `"FAIL"` = pk に `FAIL` を含むタスクのみ失敗
7. 作成 → タスク投入 (任意の pk を 1 件 enqueue)
8. **runs** drawer で結果を確認

## ファイル構成

| ファイル | 役割 |
|---|---|
| `plugin.yaml` | マニフェスト (description + init_kwargs + hidden_kwargs) — Web UI のフォームはこれを元に生成される |
| `main.py` | `setup()` / `process()` / `cleanup()` を export |
| `requirements.txt` | pip 依存 (空でも OK) |

新しいプラグインを作るときは このディレクトリをコピーして 中身を書き換えるのが早い。
