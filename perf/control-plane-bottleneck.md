# control plane ボトルネック調査 (2026-08-07)

計測ツール: `scripts/bench_control_plane.py`
対象コミット: `3eca861`

## 結論 (先に 3 行)

1. **ボトルネックは DB エンジンでも SQLite の直列化でもなく、「1 task あたりに発行される
   HTTP request と DB transaction の本数」**。 1 task = 3 request × 各 2〜12 transaction。
2. これを「1 batch = 1 request / 1 transaction」に畳んだ実験では、同じ SQLite・同じ
   単一接続・同じ RLock のまま **62 → 949 tasks/s (15.2 倍)** になった。
3. worker を増やすとスループットは **下がる** (1 台 93 → 16 台 62 tasks/s)。
   増えるのは latency だけ (p50 3.3ms → 74ms)。 fleet は 2〜4 worker で飽和している。

計測環境は 4 core のサンドボックス。 **絶対値は本番と一致しない** (本番の
control plane の core 数・ディスク・LAN RTT で上下する)。 一方で以下の比率
(worker 数を増やすと落ちる / request を畳むと 15 倍 / idle でも同じだけ食う) は
構造由来なので、本番でも同じ向きに出るはず。

---

## 1. 計測: worker 台数を振ったスケール曲線

12 workload・各 6000 task を積んだ状態で、 `service.py` と同じ順序で API を叩く
worker を N 台並べた (`bench_control_plane.py load --workers N --dur 12`)。

| workers | tasks/s | POST complete p50 | p95 | tx held 合計 / wall |
|--------:|--------:|------------------:|----:|--------------------:|
| 1  | **93.0** | 3.0 ms | 3.6 ms | 8.9 % |
| 2  | 82.3 | 7.2 ms | 9.1 ms | 59.8 % |
| 4  | 68.9 | 16.8 ms | 28.2 ms | 72.5 % |
| 8  | 67.4 | 36.0 ms | 66.8 ms | 73.4 % |
| 16 | **62.4** | 74.3 ms | 152.3 ms | 72.3 % |

- worker 1 → 16 で **throughput は 33% 減、latency は 22 倍**。 典型的な飽和形。
- `tx held 合計 / wall` は「SQLite の単一接続を誰かが握っていた時間の割合」。
  worker 2 台で既に 60% を超え、4 台以降は 72〜73% で頭打ち。
  現行の `SqliteDatabase` は接続 1 本を `RLock` で直列化しているので **100% が構造上の天井**。

## 2. request / transaction の増幅 (= 真因)

`bench_control_plane.py sqlcount` で、API 1 リクエストが張る transaction 数を数えた
(workload 12 件、batch 10 件、`_execute_batch` 相当の一括 complete):

| endpoint | tx (SQLite のみ) | tx (secondary_db 有効 = 本番) |
|---|---:|---:|
| `GET  /workers/{id}/workloads` | 4 | 3 |
| `POST /workers/{id}/claim` (空振り) | 3 | 4 |
| `POST /workers/{id}/complete` (pks=10) | **12** | **22** |
| `GET  /workers/{id}/higher-pending` | **14** | **15** |
| `PUT  /workers/{id}/heartbeat` | 2 | 2 |

(`GET /workloads` の 4 と 3 の差は secondary_db とは無関係で、`_recent_run_counts` の
10 秒キャッシュが冷えているかどうかの差。)

そして worker 側 (`service.py`) は 1 task につき 3 リクエスト
(`runs/start` → `complete` → `runs/{id}/finish`) を **逐次 await** で投げる
(`_execute_one`)。 `_execute_batch` でも complete だけが一括で、
`runs/start` と `runs/finish` は task 数だけ往復する。

内訳で効いているもの:

- **`_get_worker_or_404` が全 endpoint の先頭で `SELECT * FROM workers WHERE id=:id`**。
  20 秒の計測で 3,920 回発行されていた。 request 数とほぼ同数。
- **`workers.py:_qrepo()` が呼ばれるたびに `WorkloadRepository.list_all()` を実行**
  (`workers.py:481`)。 secondary_db (= 本番の MariaDB queue) が有効なときだけ通る経路。
  しかも `complete()` は `for pk in body.pks:` の **ループの中で `_qrepo(request)` を作る**
  (`workers.py:1078-1079`) ので、pk 10 件の complete で workloads 全表スキャン + pydantic
  parse が **10 回**走る。 これが 12 tx → 22 tx の差。
- **`higher-pending` は workload 数だけ `count_by_state` を撃つ N+1** (`workers.py:987-1005`)。
  workload 12 件で 14 tx。 これが batch 完了ごとに呼ばれる。
- **`GET /workloads` は request ごとに `workers` 全行スキャン** (`_host_vram_budget`) +
  `workloads` 全行 + `max_concurrent_*` が設定された workload の数だけ追加 COUNT。

## 3. idle でも同じだけ食う

task を 1 件も積まずに 16 worker を回した (`load --workers 16 --idle`):

```
tasks completed : 0  (0.0 tasks/s)
empty claims    : 2673      (= 178 回/秒)
tx held total   : 10.88s / 15.1s wall = 72.1%
```

**何も処理していないのに、満負荷時と同じ 72% の直列区間を消費している。**
原因は空振り claim が (a) 3 transaction 張り、(b) 候補が 0 件でも
`UPDATE <queue> SET state='claimed' ...` という **書き込みを必ず発行する**こと
(`queue.py:222-238`)。 worker は offer された workload を順に claim するので、
16 worker × 12 workload = 1 cycle あたり 192 回の空振り claim になる。

## 4. 「request を畳んだら」の実測 (headroom)

`--fused` は、1 batch 分の run 記録 + complete を **1 request / 1 transaction** で
行う仮想 endpoint を生やして同じ負荷をかけるモード。 DB もロック設計も現行のまま。

| workers | 現行 | fused | 倍率 |
|--------:|-----:|------:|-----:|
| 1  | 93.0 | 1075.7 | 11.6x |
| 4  | 68.9 | 1054.0 | 15.3x |
| 16 | 62.4 |  948.8 | **15.2x** |

fused 時の新しいボトルネックは `POST claim` (p50 132ms / 累計 159s) に移る。
つまり **SQLite も RLock も、まだ 15 倍先まで余力がある**。

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
- P2 / P3 は DB 仕事量を確かに減らすが、request 数が減らないので上限が動かない。
  **ただし本番 (= secondary_db 有効、workload 数がもっと多い) では P2 の効きは
  この計測より大きいはず** — complete あたり 10 回の全表スキャンが消えるため。

この結果が第 1 節の結論を裏付けている: 直すべきは DB 層ではなく **API の粒度**。

---

## 推奨する対処 (効果が大きい順)

1. **1 task 3 往復をやめる。**
   `runs/start` + `complete` + `runs/finish` を「batch 単位で 1 リクエスト」に畳む
   endpoint を足し、`service.py` の `_execute_one` / `_execute_batch` をそちらへ寄せる。
   計測上ここだけで一桁変わる。 互換のため既存 endpoint は残してよい。
2. **`complete` のループ内 `_qrepo(request)` を外に出す** (`workers.py:1078`)。
   1 行の移動で、本番構成の complete が 22 tx → 12 tx になる。
   同じ transaction 内で `DELETE ... WHERE pk IN (...)` に畳めばさらに 3 tx。
3. **空振り claim を無料にする。** `queue.py:claim` の先頭で候補有無を読み取り、
   0 件なら write を発行せず返す。 加えて worker 側に「空振りした workload は
   次サイクルまで skip」のバックオフを入れると 1 cycle 192 回の空振りが激減する。
4. **`workloads` / `workers` の read を in-process cache に。**
   `list_all()` と `_get_worker_or_404` は事実上すべての request の先頭で走っている。
   書き込み API で invalidate する短 TTL cache で十分。
5. **`higher-pending` の N+1 を潰す。** workload ごとの `count_by_state` ではなく、
   pending 件数を 1 クエリ (もしくは既存の集計テーブル) から取る。
6. **SQLite の単一接続 RLock は「4 以降」で扱う。** 上を直した後、
   read 用に thread-local 接続、write は単一 writer に寄せる形なら効くはず。
   単純な接続分散は P1 の通り逆効果になる。

## 再現手順

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"

# スケール曲線
for n in 1 2 4 8 16; do
  .venv/bin/python scripts/bench_control_plane.py load \
      --workers $n --workloads 12 --tasks 6000 --dur 12 --port $((9100+n))
done

# idle polling の費用
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --idle

# request を畳んだ場合の上限
.venv/bin/python scripts/bench_control_plane.py load --workers 16 --fused

# 1 リクエストあたりの SQL / transaction 本数
.venv/bin/python scripts/bench_control_plane.py sqlcount
```

## 付記: 調査中に見つかった別件

`pytest` が 3 件失敗する (本調査の変更前から)。 ボトルネックとは無関係だが記録しておく。

- `tests/test_api_workers.py::test_register_and_get`
- `tests/test_executors_python_module.py::test_missing_module_raises_config_error`
- `tests/test_executors_python_module_source_path.py::test_source_path_missing_module_raises`
