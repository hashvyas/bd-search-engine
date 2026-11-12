#!/usr/bin/env bash
# Install ZeroTier if it is missing, then join ZT_NETWORK_ID from config/cluster.env. Safe to re-run.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
[[ "${ZT_NETWORK_ID:-}" =~ ^[0-9a-fA-F]{16}$ ]] || die "set ZT_NETWORK_ID (16 hex chars) in config/cluster.env"

if ! command -v zerotier-cli >/dev/null; then
  # The official installer adds ZeroTier's GPG-signed apt repository, so apt verifies the package.
  log "installing ZeroTier"
  curl -fsSL https://install.zerotier.com -o /tmp/zt-install.sh
  sudo bash /tmp/zt-install.sh
fi

if ! sudo zerotier-cli info >/dev/null 2>&1; then
  # WSL without systemd: run the service as a background daemon.
  sudo systemctl enable --now zerotier-one 2>/dev/null || sudo zerotier-one -d
  sleep 3
fi
log "ZeroTier node: $(sudo zerotier-cli info)"

if sudo zerotier-cli listnetworks | grep -q "$ZT_NETWORK_ID"; then
  log "already joined $ZT_NETWORK_ID"
else
  sudo zerotier-cli join "$ZT_NETWORK_ID"
fi

log "Approve this node at https://my.zerotier.com/network/$ZT_NETWORK_ID (Members, tick 'Auth'). Waiting up to 5 min..."
for _ in $(seq 1 60); do
  line="$(sudo zerotier-cli listnetworks | grep "$ZT_NETWORK_ID" || true)"
  if echo "$line" | grep -q " OK "; then
    log "network OK: $line"
    log "Put this node's ZeroTier IP (last column) into NODEx_IP in config/cluster.env on all nodes."
    exit 0
  fi
  sleep 5
done
die "not authorized yet: $(sudo zerotier-cli listnetworks | grep "$ZT_NETWORK_ID" || echo 'not listed')"
