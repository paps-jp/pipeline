#!/bin/bash
# Multi-instance worker installer with configurable VRAM-based N calculation.
#
# Usage:
#   bash install-multi-worker.sh [OPTIONS]
#
# Options (each overrides the corresponding env var):
#   --per-vram-mb N             1 worker instance あたり VRAM 想定 (default 4096)
#   --ratio R                   VRAM 利用率 0.0-1.0 (default 0.8)
#   --max-instances N           インスタンス数上限 (default 4)
#   --instances N               強制 N (auto 計算を skip)
#   --auto-from-workloads URL   control plane (例: http://10.10.50.7:8001) から
#                               enabled workload の VRAM 要件を取得し per-vram-mb として使う。
#                               worker self-report された observed_vram_mb_peak が有れば
#                               max(declared, ceil(observed * 1.3)) を採用 (= 自動学習)。
#                               宣言値が無い場合は observed 単独。 両方無ければ default。
#   --observed-safety R         observed_vram_mb_peak の安全マージン倍率 (default 1.3)
#   --quiet                     INFO ログ抑制
#   -h, --help                  この help を表示
#
# Env (lowest priority, overridden by args):
#   PIPELINE_WORKER_PER_VRAM_MB=4096
#   PIPELINE_WORKER_VRAM_RATIO=0.8
#   PIPELINE_WORKER_MAX_INSTANCES=4
#   PIPELINE_WORKER_INSTANCES=N
#   PIPELINE_CONTROL_URL=http://10.10.50.7:8001
#
# 動作:
#   1. (--auto-from-workloads URL の場合) control plane から workload の最大 vram_mb 取得
#   2. nvidia-smi で VRAM 総量取得
#   3. N = min( floor(VRAM_MB * ratio / per_vram_mb), max_instances )  ※下限 1
#   4. 旧 pipeline-worker-gpu.service を stop + disable
#   5. /etc/systemd/system/pipeline-worker-gpu@.service を必要 (= 事前に scp 配置)
#   6. systemctl enable --now pipeline-worker-gpu@{1..N}

set -euo pipefail

# ---------- defaults (env) ----------
PER_VRAM_MB="${PIPELINE_WORKER_PER_VRAM_MB:-4096}"
VRAM_RATIO="${PIPELINE_WORKER_VRAM_RATIO:-0.8}"
MAX_INSTANCES="${PIPELINE_WORKER_MAX_INSTANCES:-4}"
FORCED_N="${PIPELINE_WORKER_INSTANCES:-}"
CONTROL_URL="${PIPELINE_CONTROL_URL:-}"
AUTO_FROM=""
OBSERVED_SAFETY="${PIPELINE_WORKER_OBSERVED_SAFETY:-1.3}"
QUIET=0

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --per-vram-mb) PER_VRAM_MB="$2"; shift 2 ;;
    --ratio) VRAM_RATIO="$2"; shift 2 ;;
    --max-instances) MAX_INSTANCES="$2"; shift 2 ;;
    --instances) FORCED_N="$2"; shift 2 ;;
    --auto-from-workloads) AUTO_FROM="$2"; shift 2 ;;
    --observed-safety) OBSERVED_SAFETY="$2"; shift 2 ;;
    --quiet) QUIET=1; shift ;;
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { [ "$QUIET" = "1" ] || echo "[install] $*"; }
warn() { echo "[install] WARN: $*" >&2; }
err() { echo "[install] ERROR: $*" >&2; }

# ---------- 1. (option) auto fetch per_vram_mb from workloads ----------
# observed_vram_mb_peak (= worker self-report で集めた値) を最優先、 無ければ
# resources.vram_mb (= operator 手動宣言)、 それも無ければ env/default に fallback。
# enabled workload 全体から最大値を取り、 1 worker あたり必要 VRAM とみなす。
if [ -n "$AUTO_FROM" ] && [ -z "$FORCED_N" ]; then
  log "querying enabled workloads from $AUTO_FROM (safety=$OBSERVED_SAFETY)"
  fetched=$(python3 <<PYEOF
import json, math, urllib.request, sys
try:
    r = urllib.request.urlopen("$AUTO_FROM/api/v1/workloads", timeout=5)
    d = json.loads(r.read())
    safety = float("$OBSERVED_SAFETY")
    vs = []
    sources = []
    for w in d.get("workloads", []):
        if not w.get("enabled"):
            continue
        rsrc = w.get("resources") or {}
        declared = int(rsrc.get("vram_mb") or 0)
        observed = int(w.get("observed_vram_mb_peak") or 0)
        observed_safe = math.ceil(observed * safety) if observed > 0 else 0
        chosen = max(declared, observed_safe)
        if chosen > 0:
            vs.append(chosen)
            origin = "observed*safety" if observed_safe >= declared and observed_safe > 0 else "declared"
            sources.append(f"  - {w.get('slug')}: declared={declared} observed={observed} (safe={observed_safe}) → {chosen} [{origin}]")
    if vs:
        print(max(vs))
        sys.stderr.write("\n".join(sources) + "\n")
    else:
        print(0)
except Exception as e:
    sys.stderr.write(f"auto-from-workloads fetch failed: {e}\n")
    print(0)
PYEOF
)
  if [ -n "$fetched" ] && [ "$fetched" -gt 0 ]; then
    PER_VRAM_MB="$fetched"
    log "auto: per_vram_mb=$PER_VRAM_MB (max of declared / observed*safety from enabled workloads)"
  else
    log "auto: no vram_mb (declared or observed) in workloads; falling back to per_vram_mb=$PER_VRAM_MB"
  fi
fi

# ---------- 2. N の算出 ----------
if [ -n "$FORCED_N" ]; then
  N="$FORCED_N"
  log "N forced: $N"
else
  VRAM_MB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | tr -d ' ' || true)
  if [ -z "$VRAM_MB" ] || [ "$VRAM_MB" -lt 1 ]; then
    warn "cannot read VRAM via nvidia-smi, defaulting to N=1"
    N=1
  else
    N=$(python3 -c "import math; print(max(1, min($MAX_INSTANCES, int(math.floor(int('$VRAM_MB') * $VRAM_RATIO / $PER_VRAM_MB)))))")
    log "VRAM_total=${VRAM_MB}MB  ratio=$VRAM_RATIO  per=${PER_VRAM_MB}MB  cap=$MAX_INSTANCES  → N=$N"
  fi
fi

# ---------- 3. 旧 unit を完全削除 ----------
# 注意: systemctl disable だけでは Restart=always のループを切れない (= 死んでも再起動する)。
# unit ファイル自体を削除 + daemon-reload で systemd に「もう知らない」状態にする。
if [ -f /etc/systemd/system/pipeline-worker-gpu.service ]; then
  log "stopping + removing old single-instance unit (Restart=always loop breaker)..."
  systemctl stop    pipeline-worker-gpu.service 2>/dev/null || true
  systemctl disable pipeline-worker-gpu.service 2>/dev/null || true
  rm -f /etc/systemd/system/pipeline-worker-gpu.service
  systemctl daemon-reload
fi

# ---------- 4. template unit が存在するか確認 ----------
TEMPLATE_PATH="/etc/systemd/system/pipeline-worker-gpu@.service"
if [ ! -f "$TEMPLATE_PATH" ]; then
  err "$TEMPLATE_PATH not found. Copy it first:"
  err "  scp scripts/pipeline-worker-gpu@.service root@<host>:$TEMPLATE_PATH"
  exit 1
fi

# ---------- 5. 既存の不要 instance を停止 ----------
log "checking existing instances..."
for unit in $(systemctl list-units --type=service --no-legend 'pipeline-worker-gpu@*.service' 2>/dev/null | awk '{print $1}'); do
  inst_id=$(echo "$unit" | sed 's/pipeline-worker-gpu@//; s/\.service//')
  if [ -n "$inst_id" ] && [ "$inst_id" -gt "$N" ] 2>/dev/null; then
    log "  stopping $unit (out of range)"
    systemctl stop "$unit" 2>/dev/null || true
    systemctl disable "$unit" 2>/dev/null || true
  fi
done

# ---------- 6. N 個 enable + start ----------
systemctl daemon-reload
log "enabling $N instance(s)..."
for i in $(seq 1 "$N"); do
  systemctl enable --quiet "pipeline-worker-gpu@${i}.service"
done

log "starting / restarting $N instance(s)..."
for i in $(seq 1 "$N"); do
  systemctl restart "pipeline-worker-gpu@${i}.service"
done

sleep 3
echo
echo "[install] status:"
for i in $(seq 1 "$N"); do
  state=$(systemctl is-active "pipeline-worker-gpu@${i}.service" 2>/dev/null || echo "unknown")
  echo "  pipeline-worker-gpu@${i}: $state"
done
echo
echo "[install] done. N=$N instance(s) running on $(hostname)"
