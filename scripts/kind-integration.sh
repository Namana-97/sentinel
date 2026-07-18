#!/usr/bin/env bash
set -euo pipefail

cluster="${KIND_CLUSTER:-sentinel-test}"
image="${IMAGE:-sentinel:integration}"
created=0
port_forward_pid=""
tmp_dir="$(mktemp -d)"
if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --wait 90s
  created=1
fi
cleanup() {
	if [[ -n "$port_forward_pid" ]]; then kill "$port_forward_pid" >/dev/null 2>&1 || true; fi
  kubectl delete namespace sentinel-demo --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [[ "$created" == 1 ]]; then kind delete cluster --name "$cluster"; fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

docker build -t "$image" .
kind load docker-image "$image" --name "$cluster"
kubectl apply -k config
kubectl -n sentinel-system set image deployment/sentinel-operator manager="$image"
kubectl -n sentinel-system set image daemonset/sentinel-node-monitor monitor="$image"
kubectl -n sentinel-system rollout status deployment/sentinel-operator --timeout=120s
kubectl -n sentinel-system rollout status daemonset/sentinel-node-monitor --timeout=120s

kubectl create namespace sentinel-demo
kubectl label namespace sentinel-demo sentinel.io/enabled=true
kubectl apply -f config/samples/sentinel_v1_memorypolicy.yaml
kubectl -n sentinel-demo apply -f - <<'YAML'
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: block-memory-demo}
spec:
  minAvailable: 1
  selector: {matchLabels: {app: memory-demo}}
YAML
kubectl -n sentinel-demo apply -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: memory-demo
  labels: {app: memory-demo}
spec:
  restartPolicy: Never
  containers:
    - name: pressure
      image: python:3.13-alpine
      command: [python, -c]
      args:
        - |
          import signal,time
          signal.signal(signal.SIGTERM, signal.SIG_IGN)
          blocks=[]
          while True:
            blocks.append(bytearray(1024*1024))
            time.sleep(.03)
      resources:
        requests: {memory: 32Mi}
        limits: {memory: 128Mi}
YAML
kubectl -n sentinel-demo wait --for=condition=Ready pod/memory-demo --timeout=90s

kubectl -n sentinel-system port-forward service/sentinel-api 18090:8090 >"$tmp_dir/port-forward.log" 2>&1 &
port_forward_pid=$!
for _ in {1..60}; do
  if curl -fsS http://127.0.0.1:18090/policies >"$tmp_dir/policies.json"; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:18090/policies >/dev/null

# A finite cgroup reading must be visible through REST before the pod disappears.
for _ in {1..60}; do
  curl -fsS http://127.0.0.1:18090/pods/status >"$tmp_dir/snapshots.json"
  if python3 - "$tmp_dir/snapshots.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))
assert any(x.get("name")=="memory-demo" and x.get("usageBytes",0)>0 and x.get("limitBytes",0)>0 and x.get("collectedAt") for x in items)
PY
  then break; fi
  sleep 1
done
python3 - "$tmp_dir/snapshots.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))
assert any(x.get("name")=="memory-demo" and "web-memory-safety" in x.get("policies",[]) for x in items), items
PY

# Keep the PDB closed until the first eviction is rejected.
for _ in {1..90}; do
  kubectl -n sentinel-system logs daemonset/sentinel-node-monitor --tail=500 >"$tmp_dir/monitor-before-retry.log"
  if grep -q '"stage":"eviction_rejected"' "$tmp_dir/monitor-before-retry.log"; then break; fi
  sleep 1
done
grep -q '"stage":"eviction_rejected"' "$tmp_dir/monitor-before-retry.log"

curl -fsS http://127.0.0.1:18090/policies/web-memory-safety >"$tmp_dir/policy.json"
python3 - "$tmp_dir/policy.json" <<'PY'
import json,sys
status=json.load(open(sys.argv[1])).get("status",{})
assert status.get("breachCount")==1, status
assert status.get("lastBreachTime"), status
PY

kubectl -n sentinel-demo patch pdb block-memory-demo --type=merge -p '{"spec":{"minAvailable":0}}'
kubectl -n sentinel-demo wait --for=delete pod/memory-demo --timeout=180s
kubectl -n sentinel-system logs daemonset/sentinel-node-monitor --tail=1000 >"$tmp_dir/monitor.log"

# Validate ordered stages for one intervention ID and prove eviction retry did not repeat the breach or signal.
python3 - "$tmp_dir/monitor.log" <<'PY'
import json,sys
events=[]
for line in open(sys.argv[1]):
    try: event=json.loads(line)
    except json.JSONDecodeError: continue
    if event.get("pod") == "memory-demo" and event.get("intervention_id"):
        events.append(event)
assert events, "no structured intervention events"
identifier=events[0]["intervention_id"]
stages=[e.get("stage") for e in events if e.get("intervention_id")==identifier]
required=["breach_detected","sigterm_sent","grace_completed","memory_rechecked","eviction_attempted","eviction_rejected","eviction_retry","eviction_accepted"]
positions=[stages.index(stage) for stage in required]
assert positions == sorted(positions), stages
assert stages.count("breach_detected")==1, stages
assert stages.count("sigterm_sent")==1, stages
assert stages.count("eviction_attempted")>=2, stages
PY

curl -fsS http://127.0.0.1:18090/policies/web-memory-safety >"$tmp_dir/policy-final.json"
python3 - "$tmp_dir/policy-final.json" <<'PY'
import json,sys
status=json.load(open(sys.argv[1])).get("status",{})
assert status.get("breachCount")==1, status
assert status.get("lastBreachTime"), status
PY
