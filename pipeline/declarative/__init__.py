"""宣言的構成管理 (config as code)。

ワークロード定義・許可リスト・配信先/配信パス・ホスト静的ポリシーを 1 つの YAML に
書き、 `plan` で DB との差分を確認してから `apply` で反映する。

**宣言の対象は「静的な設定」だけ**。 supervisor / watchdog が実行時に動かす項目
(workload.enabled / priority / weight、 host_policy.enabled、 agent の台数) は
対象外にしてあり、 apply が自動制御と喧嘩しない。
"""
