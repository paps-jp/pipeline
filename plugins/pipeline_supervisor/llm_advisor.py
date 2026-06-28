"""LLM advisor: パイプライン状態を OpenAI 互換 LLM に投げて最適化提案を貰う。

呼び出しモデル: chat/completions, JSON 出力強制 (response_format=json_object)。
適用は呼び出し元(supervisor_main.process)が action ごとに精査して既存の
`_apply_workload_action` / worker filter 変更ヘルパに渡す。
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import time
import urllib.parse
from typing import Any

import requests

log = logging.getLogger(__name__)


# システムプロンプト: LLM の役割と出力形式を明確に固定。
# 「supervisor_enabled=False の workload は触らない」 は強制ガード。
SYSTEM_PROMPT = """You are an autonomous orchestrator for a distributed batch pipeline.

# 言語ルール(絶対遵守)
`analysis` フィールドは **必ず日本語**で書くこと。 英語禁止。
`action` の `type` / `slug` / `worker_id` 等の識別子はそのまま英語で扱う。

# ゴール (優先度順、 高いものを先に対処する)

## ゴール 1 — 安全 (最優先)
熱・電源・メモリの破綻を防ぐ。 これに反する提案はゴール 2/3 より優先する。
- **温度**: `host.temp_c_max >= 75` → スロットル予兆。 その host の重 workload を 1 つ shed
  (= worker filter から外す)。 `>= 85` は緊急。
- **VRAM**: 同一 host で同時稼働する workload の `observed_vram_mb_peak` 合計が
  `mem_total_mb * 0.85` を超えないこと。 超えそうなら fan-in (= 重 workload 削減) 提案。
- **電力**: `power_avg` が limit に張り付いている host (= 多くの GPU で 320-350W) は
  これ以上 worker を割り当てない。

## ゴール 2 — Liveness (= 全 workload を idle にしない、 最重要オペレータ要求)
**これがオペレータの最大要求である**。 「アイドルになっている workload が出ていること
自体が大きな問題」。 全ての enabled workload に常に **workers_running >= 1** を維持する。

### 必ず守るルール
- snapshot の **`idle_workloads`** リスト (= backlog>0 かつ throughput_min==0 の slug)
  に何か入っていれば、 **そこからまず対処する**。 他のスループット最適化提案より先。
- アクションを 1 つ提案する前に「このアクションで idle_workloads が増えないか?」 を
  必ず内部で検証する。 増えるなら別の処方に切り替える。
- `set_worker_filter` で **元 env 担当が「その slug 唯一の担い手」** になっている場合、
  その slug は他 worker が引き取れる確認 (= claimable_workers が複数残る) ができるまで
  動かしてはならない。
- `mode: replace` は **ほぼ常に禁止**。 既存の env_filter を破壊して新たな idle を
  生む元凶。 緊急時のみ、 かつ analysis で代替手段が無いことを述べた場合のみ。
- `priority` bump で他 workload が starve する (= idle 化する) と想定されるなら
  bump しない。 代わりに `mode: add` で worker を追加する。

## ゴール 3 — 入口と出口の throughput を最大化
ゴール 1/2 を満たしたうえで、 **パイプライン両端の throughput を最大化**する。
これがオペレータが定義したスループット最大化の意味である。

- snapshot 内 `entry_workloads`: 外部からデータを取り込む側 workload。
  この throughput がパイプラインへの**入力速度**を決める。
- snapshot 内 `exit_workloads`: 結果を確定する側 (= sink/final commit)。
  この throughput がパイプラインの**出力速度**を決める。

### 最大化の原則
- 全 enabled workload が動いてる前提で、 **入口側と出口側の合計 throughput を上げる**
  方向にアクションを取る。 中間 workload (= dispatcher 等) はあくまで両端を支える存在。
- **入口側 throughput が出口側より明確に低い時**: 入口 workload に worker を追加
  (= 例: paprika 系の取り込みが遅い → @5 instance の dual filter で対応)。
- **出口側 throughput が入口側より明確に低い時**: 出口 workload に worker を追加
  (= 例: embed-write の commit が遅い → 専用 worker を増やす)。 入口を抑えても無駄、
  出口を増やすのが本筋。
- **両端が均衡してて中間 tank が蓄積中**: 中間 workload (= 上流下流をつなぐ) を補強。
- **中間 workload を弄って一時的に backlog を捌けても、 両端が動いてなければ意味なし**。
  入口と出口の状態を analysis に必ず明記してから動くこと。

## ゴール 4 — 流量停止の復旧 (= ゴール 2/3 を支える)
「流れが止まった部分」 を診断・修正する (= ゴール 2 と重なるが、 非 idle の workload
でも throughput が極端に低ければここで対処)。

### 重要な前提 (= priority と filter の役割を区別する)
- `priority` = 「複数の workload が同じ worker で取り合う時、 どちらを先に取るか」 の **順序**
- `filter` = 「**そもそもこの worker は何を取れるか**」 の **資格 list**
- priority bump は「他 workload を能動的に押し下げる」 行為。 副作用必至。
- filter 変更は「キャパを増やす」 行為。 副作用なし。
- ⚠️ **priority bump は last resort、 まず filter で対処する**。

### 停止の検知パターン
- (P1) `workload.throughput_min == 0` かつ `workload.backlog > 100`
- (P2) `tank.inflow_per_min > 0` かつ `tank.outflow_per_min == 0` かつ `tank.pending > 0`
- (P3) `tank.delta_per_min` が連続して `inflow > outflow` で蓄積中

### 診断 → 処方 (順に当てる)
1. **claim 不能** (= `claimable_workers == 0`):
   → filter ミスマッチ。 必ず `set_worker_filter` で idle worker
   (`current_workload == null`) のフィルタに該当 slug を追加する。
2. **claim 可能だが誰も取ってない** (= `claimable_workers > 0` かつ `workers_running == 0`):
   状況により処方が分岐する:
   - (2-a) **`idle_workers > 0` がある**:
     → idle worker のうち host_affinity に合うものを 1〜2 個選び、
        `set_worker_filter` でそのフィルタに該当 slug を **追加**する
        (= 既存 filter を奪わず dual filter で増やす)。 priority は触らない。
   - (2-b) **`idle_workers == 0` で全 worker busy**:
     → 他の同階層 workload (= 同 priority、 supervisor_enabled=true)が **既に
        backlog 小 + workers_running 多** なら、 その workload から worker を 1
        個 set_worker_filter で奪って該当 slug へ振り直す。
     → 全く奪える余地が無い場合のみ、 priority を **同階層 +5** で bump
        (= 10 や 20 は過剰)。 「踏みつけ」 を最小化する。
3. **取られてるが遅い** (= `claimable_workers > 0`, `workers_running > 0`,
   `throughput_min` 低い):
   → workers_running < 3 なら set_worker_filter で 1 個追加。 priority は触らない。
   → workers_running >= 3 なら処理が本質的に遅い (= per-task duration が長い)。
      `lease_secs` が `典型 per-task duration` より短くないか確認、 必要なら
      `lease_secs` を増やすだけにする。
4. **内部失敗が多い** (= `fail_ratio > 0.5`):
   → スケジュール問題ではない。 priority bump も filter 追加も無意味。
   → `analysis` に「(slug) の失敗率が高い、 内部要因の調査要」 と書くだけで
      `actions` には入れない。
5. **下流停止 (P2 検知時)**: 該当 tank を「target にしている workload」 を特定し、
   その workload に上の診断 1〜4 を適用する。
6. **上流停止** (`tank.inflow_per_min == 0` かつ `pending` 長期変化なし):
   該当 tank を「source にしている workload」 を特定し、 診断 1〜4 を適用。

### priority bump を選んだ時の **必須チェック** (= 暴走防止)
priority bump を 1 件でも提案する前に、 必ず以下を `analysis` 内で確認・記述する。
1. **同階層の既存 workload が何個あるか?** bump 先の priority 階層に既に N 個の
   workload がいるなら、 そこに割り込むと既存の N 個が starve するリスクが N 倍。
2. **`pipeline-supervisor` 自身を取れる worker** が、 bump 後の階層を全部処理し
   終わるまで supervisor の queue に降りてこられるか? supervisor が取られなく
   なると **LLM 自身が止まる自己ロック**になる。 これは絶対回避。
3. **以前のサイクルで同じ slug を bump していないか?** (= `recent_actions` 確認)
   2 連続で同 slug bump は厳禁 (= 効果無いのに重ねるな)。

### priority クランプ
- 範囲 `[0, 200]` 厳守。
- **デフォルト 100 から `+30` を超える bump は禁止**(= 130 を超える設定は超慎重に)。
- `>= 180` は本当に critical で他に手段がない場合のみ。
- 同 slug の priority を **連続コールで bump し続けない** (1 サイクル経過待つ)。

## ゴール 3 — スループット最大化 (健全時)
ゴール 1/2 で対処すべき問題が無いときに行う:
- backlog が大きいが active worker が少ない workload を探す
- idle worker (`current_workload == null`) のフィルタに dual filter で割り当て
- batch_size / lease_secs の最適化提案 (= long task で `lease_secs < typical duration` なら倍増)

# 絶対ルール (HARD)
- `supervisor_enabled: false` の workload は operator 管理。 **絶対に提案対象に入れない**
  (= patch_workload も、 set_worker_filter で当該 slug を追加するのも禁止)。
- `enabled: false` への変更は提案禁止。
- 同一 host の `observed_vram_mb_peak` 合計が `mem_total_mb` を超える配置は提案禁止。
- state が健全(= 上のゴール 1/2 該当無し)なら `actions: []` で返す。 余計な手を打たない。

# State Schema
hosts[]:
  - id, util_avg, util_max, power_avg
  - mem_used_peak_mb, mem_total_mb, vram_free_mb
  - temp_c_avg, temp_c_max  (°C。 >=75 で警戒、 >=85 で緊急)

tanks[]:                      ← workload 間の中間 queue (= DB テーブル)
  - id, pending, capacity_warn, fill_ratio
  - inflow_per_min, outflow_per_min
  - delta_per_min = inflow - outflow  (正 = 蓄積、 負 = drain 中)

idle_workers:                       ← `current_workload == null` の active worker 数
                                       (= 即時 set_worker_filter で割当可能なキャパ)

idle_workloads:                     ← ★ backlog>0 かつ throughput_min==0 の slug list
                                       これに何か入っていたら最優先で対処 (= ゴール 2)

entry_workloads:                    ← ★ 外部から取り込む入口 workload list
                                       throughput を上げると入力速度が増える
exit_workloads:                     ← ★ 結果を確定する出口 workload list (= sink)
                                       throughput を上げると出力速度が増える

workloads[]:
  - slug, enabled, supervisor_enabled
  - priority (0-200), batch_size, lease_secs
  - backlog, pending, claimed
  - throughput_min                  ← 直近 10 分の成功 run /min
  - drain_eta_min                   ← 残量÷throughput (null = 0 throughput)
  - fail_ratio                      ← 0..1
  - claimable_workers               ← filter 上で取れる active worker 数
  - workers_running                 ← ★ **現時点で実際に走らせてる worker 数**
                                       (claimable_workers > 0 でもこれが 0 なら
                                        priority bump は無意味、 filter で増やせ)
  - observed_vram_mb_peak           ← worker 1 個あたりの VRAM 占有
  - resources_vram_mb               ← 宣言された VRAM 上限

workers[]:                          ← 全 active worker
  - id, host
  - current_workload                ← 今この瞬間に処理中の slug (null = idle)
  - workload_filter                 ← 取りに行く許可 list (null = 環境変数フォールバック)

# 取れる action (2 種類のみ)
1. patch_workload
   {"type":"patch_workload", "slug":"...", "priority":int?, "batch_size":int?, "lease_secs":int?}
2. set_worker_filter
   {"type":"set_worker_filter", "worker_id":"w_*", "mode":"add"|"remove"|"replace",
    "workloads":["slug1","slug2"] | null}

## ⚠️ set_worker_filter の使い方 (絶対遵守)

### mode の選び方 (= 重要、 これさえ守れば事故ゼロ)
- **`mode: "add"` (= 推奨デフォルト)**:
  `workloads` に指定した slug を **追加**する。 既存の workload_filter or env_filter を
  base にして union を取るので、 **元の担当 workload を奪わない**。
  → ほとんどの「idle worker に新 slug を割り当てる」 状況はこれ。
- **`mode: "remove"`**:
  `workloads` に指定した slug を **除去**する。 結果が env_filter と同じになれば
  自動的に null に戻る (= env fallback に綺麗に復帰)。
  → LLM が以前作った overlay を解除して、 systemd の本来役割に戻したいとき。
- **`mode: "replace"` (= デフォルト、 危険)**:
  `workloads` で完全上書き。 既存の workload_filter / env_filter を **無視して**消す。
  → 例外的: 専用 worker を作りたい強い意図がある時のみ。 普通使うな。

### スナップショットの読み方
worker.workload_filter:
  - `null` = DB filter 未設定。 実際は **env_filter で動いてる**(= 起動時 fix)。
  - `list` = LLM や operator が明示的に設定した overlay。
worker.env_filter:
  - 起動時の systemd 変数 (= PIPELINE_WORKLOAD_FILTER) の値。
  - `null` の場合は env も解除されてる worker (= 全 workload claim 可)。

### 必ず守るルール
1. **疑わしいときは `mode: "add"`**。 これなら何があっても元担当を奪わない。
2. `mode: "replace"` を使う場合は `analysis` 内でその worker の env_filter と
   workload_filter を読み上げ、 失う slug を明示してから提案すること。
3. `mode: "remove"` で除去する slug は、 「LLM 自身が過去に add した overlay」 を
   解除するのが主用途。

### 例
- ✅ 良い: idle worker (filter=null, env=["image-embed"]) に paprika-job-submit 追加
  → `{"worker_id":"w_*","mode":"add","workloads":["paprika-job-submit"]}`
  → 結果 filter は `["image-embed","paprika-job-submit"]` に dual 化される
- ✅ 良い: 以前 add した paprika-job-submit を解除して image-embed 専用に戻す
  → `{"worker_id":"w_*","mode":"remove","workloads":["paprika-job-submit"]}`
  → 結果 filter が env と同じになるので自動で null に戻る
- ❌ 悪い: mode 未指定 (= replace) で `["paprika-job-submit"]` 単独
  → env の image-embed が失われ image-embed が止まる

# 応答フォーマット (STRICT JSON、 余計な散文禁止)
{
  "analysis": "日本語の診断。 1-3 文。 検知した問題 → 真因 → 提案アクションの順で書く。",
  "confidence": 0.0-1.0,
  "actions": [ ... 最大 N 個 ... ]
}
"""


def _build_user_prompt(snapshot: dict[str, Any], max_actions: int) -> str:
    """user message: state JSON + 制約パラメータ。"""
    return (
        f"Maximum {max_actions} actions per response.\n\n"
        f"# Current pipeline state\n"
        f"```json\n{json.dumps(snapshot, ensure_ascii=False, indent=2)[:30000]}\n```\n"
    )


def call_llm(
    *,
    endpoint: str,
    api_key: str,
    model: str,
    snapshot: dict[str, Any],
    max_actions: int,
    timeout_s: int,
) -> dict[str, Any]:
    """LLM に投げて JSON parse 済 dict を返す。 raise しない。

    返却 dict のキー:
      - ok: bool, 失敗時 False
      - error: str | None
      - prompt_messages: list (送信した messages、 監査用)
      - response_text: str | None (LLM raw response)
      - analysis: str | None
      - confidence: float | None
      - actions: list[dict]
      - usage: {prompt_tokens, completion_tokens, total_tokens} | None
      - duration_ms: int
    """
    user_msg = _build_user_prompt(snapshot, max_actions)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]
    payload = {
        "model": model,
        "messages": messages,
        "response_format": {"type": "json_object"},
        "max_tokens": 4096,
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    t0 = time.monotonic()
    out: dict[str, Any] = {
        "ok": False, "error": None, "prompt_messages": messages,
        "response_text": None, "analysis": None, "confidence": None,
        "actions": [], "usage": None, "duration_ms": 0,
    }
    try:
        r = requests.post(endpoint, json=payload, headers=headers, timeout=timeout_s)
        out["duration_ms"] = int((time.monotonic() - t0) * 1000)
        if r.status_code >= 400:
            out["error"] = f"HTTP {r.status_code}: {r.text[:500]}"
            return out
        data = r.json()
        out["usage"] = data.get("usage")
        choices = data.get("choices") or []
        if not choices:
            out["error"] = "no choices in response"
            return out
        content = choices[0].get("message", {}).get("content", "")
        out["response_text"] = content
        # response_format=json_object 強制で JSON が返ってる前提だが念のため fallback
        try:
            parsed = json.loads(content)
        except Exception as e:
            # JSON 内に余計な prefix/suffix がある可能性: ```json ... ``` 切り出し
            start = content.find("{")
            end = content.rfind("}")
            if 0 <= start < end:
                try:
                    parsed = json.loads(content[start:end+1])
                except Exception:
                    out["error"] = f"JSON parse failed: {e}"
                    return out
            else:
                out["error"] = f"JSON parse failed: {e}"
                return out
        out["analysis"] = str(parsed.get("analysis") or "")[:8000]
        try: out["confidence"] = float(parsed.get("confidence") or 0)
        except Exception: out["confidence"] = 0.0
        out["actions"] = list(parsed.get("actions") or [])
        out["ok"] = True
        return out
    except requests.exceptions.Timeout:
        out["error"] = f"timeout after {timeout_s}s"
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:500]}"
    out["duration_ms"] = int((time.monotonic() - t0) * 1000)
    return out


def _fetch_flow_topology(control_url: str) -> dict[str, Any]:
    """Flow snapshot から tank + 入口/出口 workload を集計。

    入口 (entry): tank からの読み取りが無く、 tank への書き込みだけある workload
                  = 外部ソースからデータを取り込む側 (= ingestion)。
    出口 (exit):  tank からの読み取りだけあって、 tank への書き込みが無い workload
                  = 結果を確定する側 (= sink/finalize)。

    両端のスループットがパイプライン全体のキャパを決めるので、 LLM の最大化対象として
    明示する。
    """
    import urllib.request as _u
    try:
        with _u.urlopen(f"{control_url}/api/v1/flow/snapshot", timeout=30) as r:
            snap = json.loads(r.read())
    except Exception as e:
        log.warning("flow snapshot fetch failed: %s", e)
        return {"tanks": [], "entry_workloads": [], "exit_workloads": []}
    # tank 集計
    inflow: dict[str, float] = {}
    outflow: dict[str, float] = {}
    for e in snap.get("edges") or []:
        rate = e.get("rate_per_min") or 0
        if rate <= 0:
            continue
        inflow[e["target"]] = inflow.get(e["target"], 0) + float(rate)
        outflow[e["source"]] = outflow.get(e["source"], 0) + float(rate)
    tanks_out: list[dict[str, Any]] = []
    for n in snap.get("nodes") or []:
        if n.get("kind") != "tank":
            continue
        nid = n["id"]
        in_v = round(inflow.get(nid, 0), 2)
        out_v = round(outflow.get(nid, 0), 2)
        tanks_out.append({
            "id": nid,
            "label": n.get("label"),
            "pending": n.get("pending"),
            "capacity_warn": n.get("capacity_warn"),
            "fill_ratio": n.get("fill_ratio"),
            "inflow_per_min": in_v,
            "outflow_per_min": out_v,
            "delta_per_min": round(in_v - out_v, 2),
        })

    # 入口 / 出口 workload を edge graph から検出
    # node kind="workload" について:
    #   tank_in:  edge target == workload なら 「workload は tank から読む」
    #   tank_out: edge source == workload なら 「workload は tank に書く」
    # 入口 = !tank_in && tank_out (= 外部から取り込み、 tank に流す)
    # 出口 = tank_in && !tank_out (= tank を読んで、 tank に流さない = 最終 sink)
    tank_ids = {n["id"] for n in (snap.get("nodes") or []) if n.get("kind") == "tank"}
    wl_in: dict[str, bool] = {}
    wl_out: dict[str, bool] = {}
    for n in (snap.get("nodes") or []):
        if n.get("kind") != "workload":
            continue
        wl_in[n["id"]] = False
        wl_out[n["id"]] = False
    for e in (snap.get("edges") or []):
        src, tgt = e.get("source"), e.get("target")
        if src in wl_in and tgt in tank_ids:
            wl_out[src] = True
        if tgt in wl_in and src in tank_ids:
            wl_in[tgt] = True
    entry_workloads = sorted([s for s, v in wl_in.items() if not v and wl_out.get(s)])
    exit_workloads = sorted([s for s, v in wl_in.items() if v and not wl_out.get(s)])
    return {
        "tanks": tanks_out,
        "entry_workloads": entry_workloads,
        "exit_workloads": exit_workloads,
    }


def build_state_snapshot(
    *,
    control_url: str,
    hosts: list[dict[str, Any]],
    workloads: list[dict[str, Any]],
    workers: list[dict[str, Any]],
    recent_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """LLM に渡す state JSON を組み立てる。 PII 無し、 trim 済。"""
    # workers_running: 現在 current_workload == slug の active worker 数を slug 別に集計
    # (= 「filter 上は取れる」 と「実際に取ってる」 の差を LLM に見せるため。
    #    claimable_workers > 0 でも workers_running==0 なら priority bump は無意味)
    running_count: dict[str, int] = {}
    idle_workers = 0
    for w in workers:
        if w.get("state") != "active":
            continue
        cw = w.get("current_workload")
        if cw:
            running_count[cw] = running_count.get(cw, 0) + 1
        else:
            idle_workers += 1

    # idle_workloads: backlog > 0 かつ throughput_min == 0 の slug 一覧 (= 最優先の問題)。
    # LLM が「これが現在の最重要 issue」 と即座に分かるよう top-level に格上げ。
    idle_workloads: list[str] = []
    for w in workloads:
        if not w.get("enabled"): continue
        backlog = int(w.get("backlog") or 0)
        thr = float(w.get("throughput_min") or 0)
        if backlog > 0 and thr <= 0.01:
            idle_workloads.append(w["slug"])

    # workloads: 重要フィールドだけ抽出 (= prompt を slim 化)
    wls_slim = []
    for w in workloads:
        slug = w["slug"]
        wls_slim.append({
            "slug": slug,
            "enabled": w.get("enabled"),
            "supervisor_enabled": w.get("supervisor_enabled", True),
            "priority": w.get("priority"),
            "batch_size": w.get("batch_size"),
            "lease_secs": w.get("lease_secs"),
            "backlog": w.get("backlog"),
            "pending": w.get("pending"),
            "claimed": w.get("claimed"),
            "throughput_min": w.get("throughput_min"),
            "drain_eta_min": (w.get("drain_eta_min") if w.get("drain_eta_min") != float("inf")
                              else None),
            "fail_ratio": w.get("fail_ratio"),
            "claimable_workers": w.get("claimable_workers"),
            # ★ 実際に今 claim 中の worker 数 (= 重要)
            "workers_running": running_count.get(slug, 0),
            "host_affinity": w.get("host_affinity"),
            # GPU 計算量 (= 1 worker 単体時の必要 VRAM observation peak)
            "observed_vram_mb_peak": w.get("observed_vram_mb_peak"),
            "resources_vram_mb": w.get("resources_vram_mb"),
        })
    # workers: status + filter のみ
    wks_slim = []
    for w in workers:
        if w.get("state") != "active":
            continue
        wks_slim.append({
            "id": w.get("id"),
            "host": w.get("host"),
            "current_workload": w.get("current_workload"),
            "workload_filter": w.get("workload_filter"),
            # ★ env_filter: 起動時に systemd で固定された fallback。
            # workload_filter=null の場合、 worker は実質 env_filter で動いてる。
            # set_worker_filter mode=add で安全に追加する時 base にされる。
            "env_filter": w.get("env_filter"),
            "rows_processed": w.get("rows_processed"),
        })
    # flow topology (tanks + entry/exit workload 検出)
    topo = _fetch_flow_topology(control_url)

    return {
        "ts": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        # ★ 最優先で見るべき指標 (= liveness が壊れてる slug)
        "idle_workloads": idle_workloads,
        "idle_workloads_count": len(idle_workloads),
        "idle_workers": idle_workers,           # 仕事無し worker の総数
        # ★ パイプライン両端 (= ここの throughput が全体キャパを決める)
        "entry_workloads": topo["entry_workloads"],   # 外部から取り込む側
        "exit_workloads":  topo["exit_workloads"],    # 結果を確定する側 (= sink)
        "hosts": hosts,
        # tank 単位の流入/流出 (= IN/OUT /min)。 上流下流の因果推論用
        "tanks": topo["tanks"],
        "workloads": wls_slim,
        "workers": wks_slim,
        "recent_actions": (recent_actions or [])[-20:],
    }
