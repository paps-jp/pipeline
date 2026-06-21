#!/bin/bash
# Pipeline-OSS Bootstrap: 新規 GPU 箱に worker daemon を 1 行で組込む
#
# 使い方 (新規 GPU 箱で root として):
#   curl -sSL http://10.10.50.7:8001/bootstrap.sh | sudo bash
#
# 環境変数:
#   CTRL_URL                control plane URL (default: http://10.10.50.7:8001)
#   INSTALL_DIR             pipeline 配置先 (default: /opt/pipeline)
#   SERVICE_USER            daemon 実行ユーザ (default: www、 既存 face_search 流用)
#   SKIP_VENV               既存 venv を流用 (default: 1 = /home/www/face_search/bin/pipeline を使う)
#   FORCE                   既存 install を上書き (default: 0)
#
# 冪等性: 既に install 済の host で再実行しても壊れない。

set -euo pipefail

CTRL_URL="${CTRL_URL:-http://10.10.50.7:8001}"
INSTALL_DIR="${INSTALL_DIR:-/opt/pipeline}"
SERVICE_USER="${SERVICE_USER:-www}"
SKIP_VENV="${SKIP_VENV:-1}"
FORCE="${FORCE:-0}"

log() { echo "[bootstrap $(date +%H:%M:%S)] $*"; }

# --- 0. root チェック ---
if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: must run as root (use sudo)" >&2
  exit 1
fi

log "=== Pipeline-OSS bootstrap ==="
log "  control plane: $CTRL_URL"
log "  install dir  : $INSTALL_DIR"
log "  service user : $SERVICE_USER"

# --- 1. apt 依存 ---
# libmariadb-dev は plugin (hash_detect / embed) が使う mariadb-connector-python の
# wheel build に必須 (= 無いと pip install mariadb がソースビルド失敗する)
log "Step 1: apt deps (rsync, python3-venv, python3-dev, libmariadb-dev, curl)"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq rsync python3-venv python3-pip python3-dev libmariadb-dev gcc curl >/dev/null

# --- 2. service user 作成 (なければ) ---
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  log "Step 2: create user $SERVICE_USER"
  useradd -r -m -d "/home/$SERVICE_USER" -s /bin/bash "$SERVICE_USER"
else
  log "Step 2: user $SERVICE_USER already exists (skip)"
fi

# --- 3. control plane の公開鍵を /root/.ssh に登録 (= ssh fallback 用) ---
log "Step 3: register control plane public key"
PUBKEY=$(curl -fsS "$CTRL_URL/api/v1/admin/deploy-targets/pubkey" 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('pubkey') or '')" 2>/dev/null || echo "")
if [ -n "$PUBKEY" ]; then
  mkdir -p /root/.ssh && chmod 700 /root/.ssh
  touch /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys
  if grep -qF "$PUBKEY" /root/.ssh/authorized_keys; then
    log "  pubkey already registered"
  else
    echo "$PUBKEY" >> /root/.ssh/authorized_keys
    log "  pubkey added to /root/.ssh/authorized_keys"
  fi
else
  log "  WARN: could not fetch pubkey from $CTRL_URL"
fi

# --- 4. pipeline source を fetch + 展開 ---
log "Step 4: fetch pipeline source from $CTRL_URL"
mkdir -p "$INSTALL_DIR"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
curl -fsSL "$CTRL_URL/api/v1/admin/bootstrap/source.tar.gz" -o "$TMP/src.tar.gz"
tar xzf "$TMP/src.tar.gz" -C "$INSTALL_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
log "  extracted to $INSTALL_DIR"

# --- 5. venv の判定 + 構築 ---
if [ "$SKIP_VENV" = "1" ] && [ -x "/home/www/face_search/bin/pipeline" ]; then
  # 既存 face_search venv を流用 (= 旧フリート互換)
  PYTHON_BIN="/home/www/face_search/bin/python3"
  PIPELINE_BIN="/home/www/face_search/bin/pipeline"
  log "Step 5: using existing venv /home/www/face_search (SKIP_VENV=1)"
else
  # 新規 venv 構築 + pyproject.toml から editable install (= pipeline package 自身 + 全依存)
  log "Step 5: create venv at $INSTALL_DIR/.venv + pip install -e .[mysql]"
  python3 -m venv "$INSTALL_DIR/.venv"
  "$INSTALL_DIR/.venv/bin/pip" install -q -U pip
  # numpy は plugin (insightface 等) で使う、 pipeline pkg 自体の dep ではない
  "$INSTALL_DIR/.venv/bin/pip" install -q -e "${INSTALL_DIR}[mysql]" numpy
  # pip install -e は entry-points 経由で pipeline コマンドを venv/bin に置く
  PYTHON_BIN="$INSTALL_DIR/.venv/bin/python"
  PIPELINE_BIN="$INSTALL_DIR/.venv/bin/pipeline"
  if [ ! -x "$PIPELINE_BIN" ]; then
    log "  ERROR: $PIPELINE_BIN が作られなかった (= pip install -e .[mysql] が失敗)"
    "$INSTALL_DIR/.venv/bin/pip" install -e "${INSTALL_DIR}[mysql]"  # verbose 再実行で原因表示
    exit 1
  fi
  log "  installed: $($PIPELINE_BIN --version 2>&1 | head -1)"
fi

# --- 6. systemd unit 配置 ---
log "Step 6: install systemd unit (pipeline-worker-gpu.service)"
UNIT=/etc/systemd/system/pipeline-worker-gpu.service
if [ -f "$UNIT" ] && [ "$FORCE" != "1" ]; then
  log "  unit already exists (use FORCE=1 to overwrite)"
else
  cat > "$UNIT" <<EOF
[Unit]
Description=Pipeline worker daemon (bootstrap)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=/home/$SERVICE_USER
Environment=PIPELINE_PLUGIN_CACHE_DIR=/tmp/pipeline-c3-cache
Environment=CUDA_VISIBLE_DEVICES=0
ExecStart=$PIPELINE_BIN worker --control-url $CTRL_URL --skip-pip-install --log-level INFO
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  log "  installed"
fi

# --- 7. service 起動 ---
log "Step 7: enable + start pipeline-worker-gpu.service"
systemctl enable --quiet pipeline-worker-gpu.service
systemctl restart pipeline-worker-gpu.service
sleep 3
STATE=$(systemctl is-active pipeline-worker-gpu.service)
log "  service state: $STATE"
if [ "$STATE" != "active" ]; then
  log "  ERROR: service not active; check 'journalctl -u pipeline-worker-gpu.service -n 30'"
  exit 1
fi

# --- 8. control plane に join 通知 (= 同 host 既存なら skip) ---
log "Step 8: join control plane as deploy_target"
HN=$(hostname)
IP=$(hostname -I | awk '{print $1}')

# 既存 check
ALREADY=$(curl -fsS "$CTRL_URL/api/v1/admin/deploy-targets" 2>/dev/null \
  | python3 -c "import sys,json;targets=json.load(sys.stdin);print('1' if any(t['host']=='$IP' for t in targets) else '0')" 2>/dev/null || echo "0")
if [ "$ALREADY" = "1" ]; then
  log "  already registered: $IP (skip)"
else
  JOIN_BODY="{\"label\":\"$HN\",\"host\":\"$IP\",\"ssh_user\":\"root\",\"ssh_port\":22,\"enabled\":true,\"notes\":\"bootstrapped $(date '+%Y-%m-%d %H:%M:%S')\"}"
  if curl -fsS -X POST "$CTRL_URL/api/v1/admin/deploy-targets" \
     -H 'Content-Type: application/json' -d "$JOIN_BODY" >"$TMP/join.json" 2>"$TMP/join.err"; then
    NEW_ID=$(python3 -c "import sys,json;print(json.load(open('$TMP/join.json')).get('id',''))")
    log "  joined: id=$NEW_ID label=$HN host=$IP"
  else
    log "  WARN: join failed: $(cat $TMP/join.err 2>/dev/null | head -1)"
  fi
fi

log "=== Bootstrap done. Worker registered to $CTRL_URL ==="
log "確認: http://10.10.50.7:8001/logs で「$HN」 タブを開いて daemon log を確認"
