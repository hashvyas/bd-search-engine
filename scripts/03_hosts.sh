#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"
load_env
for i in 1 2 3 4; do
  v="NODE${i}_IP"
  [[ "${!v:-}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "$v is not set to an IPv4 address in config/cluster.env"
done

new="$(mktemp)"
# BSD sed (macOS) has no \b; it spells word boundaries [[:<:]] and [[:>:]]
if [ "$(os_name)" = Darwin ]; then wb='[[:<:]]'; we='[[:>:]]'; else wb='\b'; we='\b'; fi
sed -e '/^# BEGIN bd-search-engine$/,/^# END bd-search-engine$/d' \
    -e "s/^\(127\.[0-9.]*[[:space:]].*${wb}node[1-4]${we}.*\)\$/# \1  # disabled by bd-search-engine/" /etc/hosts > "$new"
{
  echo "# BEGIN bd-search-engine"
  echo "$NODE1_IP node1"
  echo "$NODE2_IP node2"
  echo "$NODE3_IP node3"
  echo "$NODE4_IP node4"
  echo "# END bd-search-engine"
} >> "$new"
if cmp -s "$new" /etc/hosts; then log "/etc/hosts already up to date"
else sudo cp /etc/hosts /etc/hosts.bak-bdse && sudo cp "$new" /etc/hosts && log "updated /etc/hosts (backup: /etc/hosts.bak-bdse)"; fi
rm -f "$new"
if [ "$(os_name)" = Darwin ]; then sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder 2>/dev/null || true; fi

for i in 1 2 3 4; do log "node$i -> $(host_ip "node$i")"; done

if grep -qi microsoft /proc/version 2>/dev/null \
   && ! grep -Eqi '^[[:space:]]*generateHosts[[:space:]]*=[[:space:]]*false' /etc/wsl.conf 2>/dev/null; then
  log "WARNING: WSL will overwrite /etc/hosts on restart. Add 'generateHosts = false' under [network] in /etc/wsl.conf."
fi
