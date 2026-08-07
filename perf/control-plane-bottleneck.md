# control plane ボトルネック調査と対処 (2026-08-07)

計測ツール: `scripts/bench_control_plane.py`
調査時のコミット: `3eca861`

## 結論 (先に 3 行)

1. **ボトルネックは DB エンジンでも SQLite の直列化でもなく、「1 task あたりに発行される
   HTTP request と DB transaction の本数」**。 1 task = 3 request × 各 2〜12 transaction。
2. worker を増やすとスループットは **下がる** (1 台 107 → 16 台 62 tasks/s)。
   増えるのは latency だけ。 fleet は 2〜4 worker で飽和していた。
3. 対処後の実測: batch 実行の workload で **62 → 332 tasks/s (5.3 倍)**、
   逐次実行で **62 → 83 tasks/s (1.3 倍)**、 idle 時の DB 占有は **31% → 5%**。

計測環境は 4 core のサンドボックス。 **絶対値は本番と一致しない** (本番 control plane の
core 数・ディスク・LAN RTT で上下する)。 一方で以下の比率は構造由来なので、
本番でも同じ向きに出るはず。

---

## 1. 計測: worker 台数を振ったスケール曲線 (対処前)

12 workload・各 8000 task を積んだ状態で、 `service.py` と同じ順序で API を叩く
worker を N 台並べた。

| workers | tasks/s | tx 保持合計 / wall |
|--------:|--------:|-------------------:|
| 1  | **107.4** | 10.3 % |
| 4  | 73.7 | 72.5 % |
| 16 | **62.4** | 70.9 % |

- worker 1 → 16 で **throughput が 42% 減**。 典型的な飽和形。
- `tx 保持合計 / wall` は 「SQLite の単一接続を誰かが握っていた時間の割合」。
  worker 4 台で 72% に達し、 以降は頭打ち。 現行の `SqliteDatabase` は接続 1 本を
  `RLock` で直列化しているので **100% が構造上の天井**。

## 2. request / transaction の増幅 (= 真因)

`bench_control_plane.py sqlcount` で、API 1 リクエストが張る transaction 数を数えた
(workload 12 件、batch 10 件):

| endpoint | 対処前 (SQLite) | 対処前 (secondary_db=本番) | 対処後 (SQLite / secondary) |
|---|---:|---:|---:|
| `GET  /workers/{id}/workloads` | 4 | 3 | 5 / 5 |
| `POST /workers/{id}/claim` (空振り) | 3 (うち **UPDATE 1**) | 4 | 3 / 4 (**UPDATE 無し**) |
| `POST /workers/{id}/complete` (pks=10) | **12** | **22** | **3 / 4** |
| `GET  /workers/{id}/higher-pending` | **14** | **15** | **3 / 4** |
| `PUT  /workers/{id}/heartbeat` | 2 | 2 | 2 / 2 |

内訳で効いていたもの:

- **`workers.py:_qrepo()` が呼ばれるたびに `WorkloadRepository.list_all()` を実行**。
  しかも `complete()` は `for pk in body.pks:` の **ループの中で `_qrepo(request)` を作って**
  いたので、pk 10 件の complete で workloads 全表スキャン + pydantic parse が **10 回** 走る。
  これが 12 tx → 22 tx の差。
- **`higher-pending` は workload 数だけ `count_by_state` を撃つ N+1**。 workload 12 件で 14 tx。
  batch 完了ごとに呼ばれる hot path。
- **`_get_worker_or_404` が全 endpoint の先頭で `SELECT * FROM workers WHERE id=:id`**。
  20 秒の計測で 3,920 回発行されていた (= request 数とほぼ同数)。 **未対処**。
- worker 側は 1 task につき 3 リクエスト (`runs/start` → `complete` → `runs/{id}/finish`) を
  **逐次 await** で投げていた (`_execute_one`)。 `_execute_batch` も start/finish は
  task 数だけ往復していた。

## 3. idle でも DB を食う

task を 1 件も積まずに 16 worker を回した (`load --workers 16 --idle`、
worker の `DEFAULT_IDLE_SLEEP_S=1.0` を再現):

| | 空振り claim | tx 保持合計 / wall |
|---|---:|---:|
| 対処前 | 2421 回 / 21s (= 115 回/秒) | **31.1 %** |
| 対処後 | **0 回** | **5.3 %** |

原因は空振り claim が (a) 3 transaction 張り、(b) 候補が 0 件でも
`UPDATE <queue> SET state='claimed' ...` という **書き込みを必ず発行する** こと。
worker は offer された workload を順に claim するので、
16 worker × 12 workload = 1 cycle あたり 192 回の空振りになっていた。

> **訂正**: 本調査の初版はここを 72% と記載していた。 計測ハーネスの idle 時待ちが
> 0.02 秒で、 実際の worker の 1.0 秒より遥かに短かったための過大評価。
> 実際の worker 挙動に合わせて再計測した値が上表の 31.1%。

## 4. 「request を畳んだら」の理論上限

`--fused` は、1 batch 分の run 記録 + complete を **1 request / 1 transaction** で行う
仮想 endpoint (= 本番 API には無い) で同じ負荷をかけるモード。 DB もロック設計も現行のまま。

| workers | 対処前 | fused (理論上限) |
|--------:|-----:|------:|
| 1  | 107.4 | 1075.7 |
| 16 | 62.4 |  948.8 |

**SQLite も RLock も、まだ 15 倍先まで余力がある**ことがここで分かった。
ただし実装では Dashboard の 「実行中」 表示 (= `runs.finished_at IS NULL`) を保つため
run 開始の記録を残しており、 後述の通り実効値はこれより小さい。

## 5. 効かなかった対策 (試して外したもの)

仮説検証のため 3 つの改修を prototype (monkeypatch) して同条件で測った。
いずれも **throughput はほぼ動かなかった**:

| prototype | 16 worker tasks/s (baseline 63.9) |
|---|---:|
| P1: SQLite を thread-local 接続 + `busy_timeout` にして RLock 直列を廃止 | 58.6 |
| P2: `workloads` の read を TTL cache 化 | 66.2 |
| P3: 空振り claim の write を `SELECT 1` で短絡 | 63.4 |
| P1+P2+P3 | 63.2 |

- **P1 は逆効果だった**。 直列 RLock が消えても、今度は SQLite ファイルレベルの
  write lock と commit が 40 スレッド分競合し、transaction の合計保持時間が
  12s → 69s に膨らんだだけだった。 「接続を増やす」だけでは解決しない。
- P2 / P3 は DB 仕事量を減らすが、request 数が減らないので上限が動かない。

この結果が第 1 節の結論を裏付けている: 直すべきは DB 層ではなく **API の粒度**。

---

## 6. 実装した対処と実測効果

### 入れたもの

1. **`complete` の bulk 化** — `_qrepo()` 生成をループ外に出し、
   `QueueRepository.complete_many()` で `DELETE ... WHERE pk IN (...)` を 1 transaction に。
   (12 tx → 3 tx、 本番構成で 22 tx → 4 tx)
2. **空振り claim から書き込みを除去** — `QueueRepository.claim` の先頭で候補の有無だけ読み、
   0 件なら `UPDATE` を発行せず返す。 MariaDB 経路は元々書かないので短絡を通さない。
3. **`claimable` ヒント** — `GET /workers/{id}/workloads` が
   「claim 候補がある slug」 を同梱し、 worker はそれ以外への claim を省く。
   判定は backend ごと transaction 1 本の EXISTS。
   **offer 一覧からは外さない**: worker は offer されなくなった slug の executor を破棄するので、
   queue が一瞬空になっただけで model load 数十秒の executor が捨てられ rebuild churn になる。
4. **`runs/start-batch` + `batch-result`** — batch 分の run 開始をまとめ、
   「run 完了 + queue complete/fail」 を 1 リクエスト / 4 transaction 以下に畳む。
   `_execute_batch` は 2N+1 往復 → 2 往復、 `_execute_one` は 3 往復 → 2 往復。
5. **`higher-pending` の N+1 解消** — workload ごとの `count_by_state` を
   `claimable_tables(include_expired=False, fail_open=False)` に置換 (14 tx → 3 tx)。

### 互換性

- 新 endpoint は追加のみ。 既存の `complete` / `fail` / `runs/start` / `runs/{id}/finish` は
  そのまま残してある。
- 旧 worker daemon は `claimable` を読まないので従来通り全 slug に claim する
  (= 空振りは減らないが正しく動く)。
- 新 worker daemon は `batch-result` / `start-batch` が 404 なら旧経路へ degrade するので、
  **control plane と worker のどちらを先に更新しても動く**。
- 意味論は従来と同じ at-least-once。 `batch-result` の再送は 2 回目の DELETE が 0 行になるだけ。
- Dashboard の 「実行中」 表示は `runs/start-batch` で run 行を先に作るので維持される。

### 実測 (12 workload、 各 8000 task、 同一環境)

| workers | 対処前 | 対処後 (逐次 executor) | 対処後 (batch executor) |
|--------:|------:|----------------------:|------------------------:|
| 1  | 107.4 | 142.5 (1.33x) | **591.5 (5.5x)** |
| 4  |  73.7 |  95.5 (1.30x) | **413.6 (5.6x)** |
| 16 |  62.4 |  82.6 (1.32x) | **332.2 (5.3x)** |

idle 時 (16 worker、 task 0 件): tx 保持 **31.1% → 5.3%**、 空振り claim **115 回/秒 → 0**。

- `process_batch` を持つ plugin (= GPU 推論系の高スループット workload) が **5 倍**。
  ここが fleet のスループットを決めているので、効果はここに集中する。
- `process_batch` を持たない逐次 plugin は **1.3 倍**。 1 task ごとに
  `runs/start` を残している (= Dashboard の実行中表示を保つ) ため 2 往復が下限。
  第 4 節の 15 倍はこの start を捨てた場合の理論値で、 実装では採っていない。

### まだ残っている改善余地

- `_get_worker_or_404` / `_wlrepo().list_all()` が全 request の先頭で走る。
  短 TTL の in-process cache が次の一手 (= 第 5 節 P2。 単体では上限を動かさないが、
  workload 数が増えるほど効く)。
- SQLite の単一接続 RLock。 上を全部やった後で、 read は thread-local・write は
  単一 writer に寄せる形なら効くはず。 **単純な接続分散は P1 の通り逆効果**。
- 逐次 plugin の `runs/start` 往復。 結果報告を batch 末尾まで遅延させれば 1 往復にできるが、
  Dashboard の実行中表示が batch 単位に粗くなるトレードオフがある。

## 再現手順

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"

# 対処前後の比較 (--legacy = 旧 worker の呼び出しパターン)
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --legacy
.venv/bin/python scripts/bench_control_plane.py load --workers 16
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --batch-executor

# idle polling の費用
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --idle --legacy
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --idle

# 1 リクエストあたりの SQL / transaction 本数
.venv/bin/python scripts/bench_control_plane.py sqlcount
```

## 付記: 調査中に見つかった別件

`pytest` が 3 件失敗する (本調査の変更前から、 変更後も同じ 3 件)。 ボトルネックとは
無関係だが記録しておく。

- `tests/test_api_workers.py::test_register_and_get`
- `tests/test_executors_python_module.py::test_missing_module_raises_config_error`
- `tests/test_executors_python_module_source_path.py::test_source_path_missing_module_raises`
