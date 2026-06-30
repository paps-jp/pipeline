# Pipeline プロジェクト — Claude 向けメモ

## デプロイ構成

### プラグインルート (全ホスト統一)

すべてのホストで **`/opt/pipeline/plugins`** をプラグインルートとして使用する。

| ホスト | プラグインルート | 備考 |
|--------|----------------|------|
| nas (10.10.50.7) | `/opt/pipeline/plugins` | control plane。ソースは `/home/paps-ai/ai/pipeline/plugins` (git管理、変更はこちらで行う) |
| ai-gpu1 (10.10.50.23) | `/opt/pipeline/plugins` | GPU worker |
| ai-gpu4 (10.10.50.29) | `/opt/pipeline/plugins` | GPU worker |
| ai-gpu5 (10.10.50.30) | `/opt/pipeline/plugins` | GPU worker |

- nas の `pipeline-oss.service` は `PIPELINE_PLUGIN_ROOT=/opt/pipeline/plugins` を設定済み (`/etc/systemd/system/pipeline-oss.service`)
- ソース置き場 (`/home/paps-ai/ai/pipeline/plugins`) は開発専用。nasへの反映は rsync で行う:
  ```
  rsync -a --delete /home/paps-ai/ai/pipeline/plugins/ /opt/pipeline/plugins/
  ```

### デプロイフロー

1. ローカル (`C:\Users\kazuna\pipeline`) で実装
2. `scp` または `rsync` で nas の **ソース** (`/home/paps-ai/ai/pipeline/plugins/<slug>/`) へ
3. nas では `rsync -a --delete` で `/opt/pipeline/plugins/` へ同期 (または scp 直接)
4. GPU ホスト (ai-gpu1/4/5) は `/opt/pipeline/plugins/<slug>/` へ直接 scp
5. `__pycache__/*.pyc` を削除してから worker 再起動

```bash
# 全ホストへ一斉デプロイ例
for host in nas ai-gpu1 ai-gpu4 ai-gpu5; do
  scp plugins/foo/bar.py "$host:/opt/pipeline/plugins/foo/"
done
# nas は追加でソースも更新
scp plugins/foo/bar.py nas:/home/paps-ai/ai/pipeline/plugins/foo/
```

### SSH 接続

```bash
# ~/.ssh/config にエイリアス設定済み (鍵: ~/.ssh/id_paprika)
ssh nas        # root@10.10.50.7
ssh ai-gpu1    # root@10.10.50.23
ssh ai-gpu4    # root@10.10.50.29
ssh ai-gpu5    # root@10.10.50.30
```

### Worker 再起動

```bash
# nas
ssh nas "systemctl restart pipeline-worker-c2.service"
# GPU ホスト (CPU + GPU worker まとめて)
ssh ai-gpu1 "systemctl restart 'pipeline-worker-cpu@*' 'pipeline-worker-gpu@*'"
```

## コミット・作業ルール

- `Co-Authored-By:` は付けない
- 実装 / コミット / デプロイ は分けて行う
- deploy-to-gpu.sh は rsync 失敗を [OK] と誤報告するので、デプロイ後は md5 で各ホストを突合検証すること
