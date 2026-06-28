# pipeline-supervisor

パイプライン全体を俯瞰して自律的に調整する **オーケストレーター プラグイン**。
pipeline-oss の他 workload (= 各 plugin) と同じ self-loop モデルで動き、
30 秒毎に下記を観測して必要なら設定を書き換える。

## 観測対象

| API | 用途 |
|---|---|
| `GET /api/v1/workers/metrics` | GPU host 別 util / power / VRAM |
| `GET /api/v1/workloads` | enabled な workload 一覧 + 現行 priority/lease |
| `GET /api/v1/workloads/<slug>/queue` | pending / claimed 件数 |
| `GET /api/v1/runs` | 直近 success 数 + output_json (= dup ratio 等) |

## アクション

`orchestration_rules.yaml` に書かれた if-then ルールを評価し、 既定では
**dry-run** (= service_logs に予定だけ出す)。 `apply_mode=1` で
`PUT /api/v1/workloads/<slug>` を実行して priority / lease_secs /
host_affinity / enabled を書き換える。

## ルール書式

```yaml
rules:
  - id: starved-priority-bump
    when:
      workload.throughput_min:    { lt: 0.5 }
      workload.backlog:           { gt: 100 }
      streak: 3                              # 3 tick 連続成立
    action:
      patch_workload:
        priority:    "+20"                   # 現在値 + 20 (= 上限 max_priority)
    cooldown_ticks: 5                        # 適用後 5 tick は同 rule 無視
```

| 演算 | 例 | 意味 |
|---|---|---|
| `lt` `gt` `eq` `gte` `lte` | `{ lt: 0.5 }` | 比較 |
| `+N` | `priority: "+20"` | 現在値 + N |
| `-N` `*N` `/N` | `lease_secs: "*0.7"` | 各演算 |
| `N` | `priority: 100` | 絶対値 |

## 評価対象の field

```python
# host (= GPU host)
id, util_avg, util_max, power_avg, mem_avg, sample_n

# workload
slug, enabled, priority, lease_secs, host_affinity,
backlog, pending, claimed,
throughput_min, drain_eta_min, dup_ratio, succ_n
```

## 1 インスタンス保証

`host_affinity` に **特定の worker** (例: `["ai-gpu1-1"]`) を 1 つだけ指定する。
複数 worker で動くと PUT 競合 → 状態が暴れるので必須。 既定は空 = 警告ログ
だけ出して dry-run 動作させて確認後、 UI / API で固定する想定。

## init_kwargs

| key | default | 説明 |
|---|---|---|
| `control_url` | `http://127.0.0.1:8001` | pipeline-oss 制御プレーン URL |
| `rules_path` | "" (= プラグイン同梱) | ルール yaml 絶対パス。 編集→次 tick 反映 |
| `interval_s` | 30 | 評価間隔 |
| `metrics_minutes` | 10 | GPU メトリクス window |
| `throughput_window_min` | 10 | スループット計算 window |
| `apply_mode` | 0 | 1=PUT 実行 / 0=dry-run (log のみ) |
| `max_priority` | 200 | priority bump 上限 (= 暴走防止) |

## デバッグ

dry-run 中の output_json から「何が判定された / 何を変えようとした」が見える:

```bash
curl 'http://127.0.0.1:8001/api/v1/runs?limit=10' | \
  jq '.runs[] | select(.workload_slug=="pipeline-supervisor") | .output_json'
```

```json
{
  "tick": 42, "host_count": 3, "wl_count": 9,
  "rules_loaded": 4, "actions": 2, "apply_mode": 0,
  "results": [
    {"rule": "starved-priority-bump", "dry_run": true,
     "slug": "paprika-image-pull", "changes": {"priority": [100, 120]}}
  ]
}
```

## 注意

- **公開設定の取扱**: rules yaml で外部 host を渡せる (= `host` field) ので
  信頼できる operator の手元でのみ運用する。
- **暴走防止**: `max_priority`、 cooldown_ticks、 streak で抑え込み済。
  それでも疑わしいときは workload `pipeline-supervisor` を
  `PATCH .../enabled false` で即時停止できる。
