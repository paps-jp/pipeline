#!/bin/bash
# pipeline-pve-wrap — proxmox-manager 用の forced-command ラッパ。
#
# 目的:
#   proxmox-manager の thin_trim / svc_restart は LXC の `pct exec` と systemctl を
#   使うため PVE ノードの root SSH を要する。 だが control plane (nas) にハイパー
#   バイザの無制限 root を渡すのは blast radius が大きすぎる。 この forced-command で
#   **許可した 3 系統の操作だけ**に絞り、 それ以外は実行せず拒否・記録する。
#
# 設置 (全 PVE ノード共通。 /etc/pve は pmxcfs 共有だが /usr/local/bin は各ノード):
#   install -m 0755 pipeline-pve-wrap.sh /usr/local/bin/pipeline-pve-wrap
#
# 鍵の登録 (1 ノードで実施すれば pmxcfs 経由で全ノードへ伝播):
#   /etc/pve/priv/authorized_keys に 1 行:
#     command="/usr/local/bin/pipeline-pve-wrap",no-port-forwarding,\
#     no-agent-forwarding,no-X11-forwarding,no-pty ssh-ed25519 AAAA... pipeline@nas
#
# 許可する操作:
#   pct list                      → VMID の一覧 (1 行 1 個)
#   pct exec <vmid> -- fstrim -v /  → 該当 CT の rootfs を trim
#   systemctl restart <service>   → ALLOWED_SERVICES のものだけ
#
# 拒否したコマンドは syslog (authpriv.warning) に残る。 eval も sh -c も使わないので、
# 引数にシェルメタ文字を混ぜても別コマンドにはならない。

set -euo pipefail

ALLOWED_SERVICES="pvestatd pvedaemon pveproxy pve-cluster corosync"
TAG="pipeline-pve-wrap"

cmd="${SSH_ORIGINAL_COMMAND:-}"

deny() {
	logger -p authpriv.warning -t "$TAG" "DENIED from ${SSH_CLIENT%% *}: $cmd"
	echo "pipeline-pve-wrap: command not permitted" >&2
	exit 42
}

allow() {
	logger -p authpriv.info -t "$TAG" "ALLOW from ${SSH_CLIENT%% *}: $cmd"
}

# --- pct list ---
if [[ "$cmd" == "pct list" ]]; then
	allow
	exec pct list
fi

# --- pct exec <vmid> -- fstrim -v / ---
if [[ "$cmd" =~ ^pct\ exec\ ([0-9]{1,10})\ --\ fstrim\ -v\ /$ ]]; then
	allow
	exec pct exec "${BASH_REMATCH[1]}" -- fstrim -v /
fi

# --- systemctl restart <service> ---
if [[ "$cmd" =~ ^systemctl\ restart\ ([A-Za-z0-9@._-]{1,64})$ ]]; then
	svc="${BASH_REMATCH[1]}"
	for a in $ALLOWED_SERVICES; do
		if [[ "$svc" == "$a" ]]; then
			allow
			exec systemctl restart "$svc"
		fi
	done
	deny
fi

deny
