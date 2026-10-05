#!/usr/bin/env bash
# Runs only against an explicitly supplied local kind/MicroK8s context.
set -euo pipefail
context=${1:?Usage: scripts/network-integration.sh LOCAL_CONTEXT [--network-policy]}
case "$context" in kind-*|microk8s) ;; *) echo 'Use an explicit kind-* or microk8s test context.' >&2; exit 2;; esac
k() { kubectl --context "$context" "$@"; }
cd "$(dirname "$0")/.."
for tool in kubectl curl python3; do command -v "$tool" >/dev/null; done
k apply -f config/manager/network.yaml
k apply -f examples/network/workload.yaml
k -n sentinel-system rollout status deployment/sentinel-network-monitor --timeout=120s
k -n sentinel-network-demo rollout status deployment/web --timeout=120s
k -n sentinel-system port-forward deployment/sentinel-network-monitor 18090:8090 >/tmp/sentinel-network-portforward.log 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" 2>/dev/null || true
  k -n sentinel-network-demo delete networkpolicy block-web --ignore-not-found >/dev/null 2>&1 || true
  k apply -f examples/network/workload.yaml >/dev/null
}
trap cleanup EXIT
wait_reason() {
 local expected=$1
 for ((i=0;i<45;i++)); do
  if curl -fsS --max-time 2 http://127.0.0.1:18090/network/status | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d["services"].get("sentinel-network-demo/web",{}); sys.exit(0 if not d.get("scanError") and r.get("reason")==sys.argv[1] else 1)' "$expected" 2>/dev/null; then
   echo "PASS: $expected"; return
  fi
  sleep 2
 done
 echo "Timed out waiting for $expected" >&2
 curl -s http://127.0.0.1:18090/network/status >&2
 return 1
}
wait_reason Available
k -n sentinel-network-demo patch service web --type=merge -p '{"spec":{"selector":{"app":"missing"}}}'
wait_reason NoReadyEndpoints
k apply -f examples/network/workload.yaml
wait_reason Available
# Wrong targetPort still yields endpoints but traffic cannot reach the listener.
k -n sentinel-network-demo patch service web --type=json -p '[{"op":"replace","path":"/spec/ports/0/targetPort","value":81}]'
wait_reason ConnectionFailed
k apply -f examples/network/workload.yaml
wait_reason Available
k -n sentinel-network-demo scale deployment/web --replicas=0
wait_reason NoReadyEndpoints
k -n sentinel-network-demo scale deployment/web --replicas=2
wait_reason Available
if [[ ${2:-} == --network-policy ]]; then
 k apply -f examples/network/deny-ingress.yaml
 wait_reason ConnectionFailed
 k -n sentinel-network-demo delete networkpolicy block-web
 wait_reason Available
fi
curl -fsS http://127.0.0.1:18090/network/status | python3 -m json.tool
