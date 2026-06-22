#!/bin/bash
# Pipeline-OSS deploy: .7 (SoT) → 配信先 GPU 箱への配信 + service 管理
#
# 使い方:
#   sudo bash /opt/pipeline/scripts/deploy-to-gpu.sh
#
# 環境変数:
#   GPU_HOSTS="10.10.50.23 10.10.50.29"  配信先 host (default: deploy_targets.enabled=1)
#   PATHS_JSON='[{src,dst,setup_command,...}]'  配信パス (default: deploy_paths.enabled=1)
#   CTRL_URL=http://10.10.50.7:8001  control plane API
#   SKIP_RESTART=1  service restart せず (= rsync + setup_command のみ)
#   DRY_RUN=1       rsync --dry-run
#
# 仕組み:
#   1. control plane API から hosts と paths を取得
#   2. 各 host × 各 path で並列に:
#      - rsync (src → dst)
#      - setup_command があれば dst で実行
#      - service_command があれば systemd unit 自動生成 + restart
#   3. pipeline-worker-gpu.service の restart (= 既存 daemon の入れ替え用、 SKIP_RESTART=0 時)

set -euo pipefail

CTRL_URL="${CTRL_URL:-http://10.10.50.7:8001}"
SKIP_RESTART="${SKIP_RESTART:-0}"
DRY_RUN="${DRY_RUN:-0}"

ts() { date +'%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*"; }

# GPU_HOSTS と PATHS_JSON は env で渡される (= control plane の admin API が セット)
# 単体実行用に env が無ければ DB から直接読む
if [ -z "${GPU_HOSTS:-}" ] || [ -z "${PATHS_JSON:-}" ]; then
  TMP_QUERY=$(mktemp)
  /opt/pipeline/.venv/bin/python3 -c "
import sqlite3, json
db = sqlite3.connect('/opt/pipeline/data/pipeline.db')
c = db.cursor()
c.execute(\"SELECT host FROM deploy_targets WHERE enabled=1\")
hosts = ' '.join(r[0] for r in c.fetchall())
c.execute(\"SELECT id, label, src_path, dst_path, enabled, delete_mode, setup_command, service_command FROM deploy_paths WHERE enabled=1\")
paths = []
for r in c.fetchall():
  paths.append({'id': r[0], 'label': r[1], 'src_path': r[2], 'dst_path': r[3],
                'enabled': bool(r[4]), 'delete_mode': bool(r[5]),
                'setup_command': r[6], 'service_command': r[7]})
print(f'GPU_HOSTS={hosts}')
print(f'PATHS_JSON={json.dumps(paths)}')
" > "$TMP_QUERY"
  source "$TMP_QUERY"
  rm "$TMP_QUERY"
fi

if [ -z "$GPU_HOSTS" ]; then
  log "ERROR: no enabled deploy_targets"
  exit 1
fi

# paths が空配列なら何もしない (= warning だけ)
N_PATHS=$(echo "$PATHS_JSON" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
if [ "$N_PATHS" = "0" ]; then
  log "WARN: no enabled deploy_paths"
fi

log "=== deploy start (hosts=[$GPU_HOSTS], paths=$N_PATHS) ==="

RSYNC_BASE=(-az)
if [ "$DRY_RUN" = "1" ]; then
  RSYNC_BASE+=("--dry-run")
fi

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# 各 host × 各 path で並列実行
report_ok() { log "  [OK]   $*"; }
report_fail() { log "  [FAIL] $*"; }

# Python で paths を 1 行ずつ TSV 化 → bash で読む
PATHS_TSV="$TMPDIR/paths.tsv"
echo "$PATHS_JSON" | python3 -c "
import sys, json
for p in json.load(sys.stdin):
    # TSV: id, label, src, dst, delete_mode, setup_command(b64), service_command(b64)
    import base64
    sc = base64.b64encode((p.get('setup_command') or '').encode()).decode()
    svc = base64.b64encode((p.get('service_command') or '').encode()).decode()
    print(f\"{p['id']}\t{p['label']}\t{p['src_path']}\t{p['dst_path']}\t{1 if p['delete_mode'] else 0}\t{sc}\t{svc}\")
" > "$PATHS_TSV"

# 1 host × 1 path を処理する関数
deploy_one() {
  local host="$1" id="$2" label="$3" src="$4" dst="$5" delmode="$6" setup_b64="$7" svc_b64="$8"
  local rsync_flags=("${RSYNC_BASE[@]}")
  [ "$delmode" = "1" ] && rsync_flags+=("--delete")

  # src が file or dir で rsync の trailing slash 扱いが違う
  local src_arg dst_arg
  if [ -d "$src" ]; then
    src_arg="$src/"
    dst_arg="$dst/"
  else
    src_arg="$src"
    dst_arg="$dst"
  fi

  local out="$TMPDIR/p${id}-${host}.log"
  {
    echo "--- rsync $src_arg → root@$host:$dst_arg ---"
    rsync "${rsync_flags[@]}" \
      -e "ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5" \
      "$src_arg" "root@$host:$dst_arg" 2>&1

    # setup_command
    local setup
    setup=$(echo "$setup_b64" | base64 -d)
    if [ -n "$setup" ] && [ "$DRY_RUN" != "1" ]; then
      echo "--- setup_command on root@$host:$dst ---"
      ssh -o ConnectTimeout=5 root@$host "cd '$dst' && $setup" 2>&1
    fi

    # service_command → systemd unit 自動生成 + restart
    local svc
    svc=$(echo "$svc_b64" | base64 -d)
    if [ -n "$svc" ] && [ "$DRY_RUN" != "1" ]; then
      local unit="pipeline-deploy-${label// /_}.service"
      echo "--- service_command: install $unit + restart ---"
      ssh -o ConnectTimeout=5 root@$host "cat > /etc/systemd/system/$unit <<EOF
[Unit]
Description=Pipeline deploy: $label
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$dst
ExecStart=$svc
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --quiet $unit
systemctl restart $unit
sleep 1
systemctl is-active $unit" 2>&1
    fi
  } > "$out" 2>&1
  local rc=$?

  # 結果を control plane に通知 (= last_synced)
  if [ "$rc" -eq 0 ]; then
    report_ok "p${id} $label → $host"
  else
    report_fail "p${id} $label → $host (see $out)"
    tail -5 "$out" | sed 's/^/      /'
  fi
  return $rc
}

# Step 1: rsync + setup + service (全 host × 全 path 並列)
log "Step 1: rsync + setup + service per (host, path)"

PIDS=()
while IFS=$'\t' read -r id label src dst delmode setup_b64 svc_b64; do
  for host in $GPU_HOSTS; do
    deploy_one "$host" "$id" "$label" "$src" "$dst" "$delmode" "$setup_b64" "$svc_b64" &
    PIDS+=($!)
  done
done < "$PATHS_TSV"

# wait all
for pid in "${PIDS[@]:-}"; do
  wait "$pid" || true
done

# Step 2: pipeline-worker-gpu.service の restart (= 既存 daemon を更新)
if [ "$SKIP_RESTART" = "1" ] || [ "$DRY_RUN" = "1" ]; then
  log "Step 2: SKIP_RESTART=$SKIP_RESTART, DRY_RUN=$DRY_RUN → skip pipeline-worker-gpu restart"
else
  log "Step 2: restart pipeline-worker-gpu instances (= template + 単発の両方対応)"
  for host in $GPU_HOSTS; do
    out="$TMPDIR/restart-$host.log"
    (
      ssh -o ConnectTimeout=5 root@$host bash <<'EOSSH' > "$out" 2>&1
# template instance (= pipeline-worker-gpu@N.service) を列挙、 無ければ単発 .service へ fallback
units=$(systemctl list-units --type=service --all --no-legend 'pipeline-worker-gpu@*' 2>/dev/null \
        | awk '{print $1}' | grep -v '^$' | sort -u)
if [ -z "$units" ]; then
  units="pipeline-worker-gpu.service"
fi
ok=0; total=0
for u in $units; do
  total=$((total+1))
  if systemctl restart "$u" 2>&1; then
    ok=$((ok+1))
  fi
done
sleep 2
for u in $units; do
  echo "  $u: $(systemctl is-active "$u" 2>&1)"
done
echo "restarted: $ok/$total"
EOSSH
    ) &
  done
  wait
  for host in $GPU_HOSTS; do
    out="$TMPDIR/restart-$host.log"
    summary=$(grep '^restarted:' "$out" 2>/dev/null || echo "?")
    log "  $host: $summary"
  done
fi

log "=== deploy done ==="
