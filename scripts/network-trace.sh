#!/usr/bin/env bash
# Run on the Linux test node, using the selected pod sandbox PID.
set -euo pipefail
sandbox_pid=${1:?Usage: sudo scripts/network-trace.sh SANDBOX_PID OUTPUT_DIR [CAPTURE_SECONDS]}
output=${2:?Provide an output directory}
seconds=${3:-10}
[[ $sandbox_pid =~ ^[0-9]+$ && $seconds =~ ^[0-9]+$ ]] || { echo 'PID and capture duration must be numeric.' >&2; exit 2; }
(( seconds >= 1 && seconds <= 60 )) || { echo 'Capture duration must be 1–60 seconds.' >&2; exit 2; }
(( EUID == 0 )) || { echo 'Run on the Linux test node as root.' >&2; exit 2; }
for tool in nsenter ip tcpdump timeout; do command -v "$tool" >/dev/null; done
[[ -e /proc/$sandbox_pid/ns/net ]] || { echo 'Sandbox PID does not exist.' >&2; exit 2; }
mkdir -p "$output"
readlink "/proc/$sandbox_pid/ns/net" > "$output/network-namespace.txt"
nsenter -t "$sandbox_pid" -n ip -details address > "$output/pod-addresses.txt"
nsenter -t "$sandbox_pid" -n ip route show table all > "$output/pod-routes.txt"
nsenter -t "$sandbox_pid" -n ip -6 route show table all > "$output/pod-routes-ipv6.txt"
ip -details link > "$output/node-links.txt"
ip route show table all > "$output/node-routes.txt"
if [[ -d /etc/cni/net.d ]]; then find /etc/cni/net.d -maxdepth 1 -type f -printf '%f\n' > "$output/cni-config-names.txt"; fi
# Generate HTTP/DNS traffic from a second terminal during this capture.
set +e
nsenter -t "$sandbox_pid" -n timeout --signal=INT "$seconds" tcpdump -U -ni any -w "$output/pod-traffic.pcap" 'port 53 or tcp port 80'
result=$?
set -e
[[ $result == 0 || $result == 124 ]] || exit "$result"
echo "Namespace, interfaces, routes and packet capture saved in $output"
