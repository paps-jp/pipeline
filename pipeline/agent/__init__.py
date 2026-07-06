"""pipeline-agent: 1 host = 1 プロセスで worker 子プロセスを spawn/監督する (P1 最小 PoC)。

設計: docs/PIPELINE_AGENT.md
- P1: 既存 `pipeline worker` を subprocess として spawn/監督するだけ (worker 無改修)。
  子の workload は PIPELINE_WORKLOAD_FILTER env で指定、 claim/heartbeat は子が直叩き。
- P2 以降: sync packet (1 agent=1 packet/10s) + pipe IPC でデータプレーン集約。
"""
