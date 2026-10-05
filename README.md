# Sentinel

Sentinel is a Kubernetes operator that intervenes before a limited container reaches the Linux OOM killer. It combines controller-runtime reconciliation, client-go shared informers and workqueues, a bounded Go worker pool, direct cgroup v1/v2 reads, graceful Linux signalling, and Kubernetes `policy/v1` eviction.

This is a portfolio-grade systems project, not an in-process metrics demo: memory values come from the node's kernel interfaces, policies live in the Kubernetes API, PodDisruptionBudgets are honored by the Eviction subresource, and every long-running component has a cancellation path.

## Quick demo with Kind

```bash
kind create cluster --name sentinel
docker build -t sentinel:dev .
kind load docker-image sentinel:dev --name sentinel
kubectl apply -k config
kubectl -n sentinel-system set image deployment/sentinel-operator manager=sentinel:dev
kubectl -n sentinel-system set image daemonset/sentinel-node-monitor monitor=sentinel:dev
kubectl -n sentinel-system rollout status deployment/sentinel-operator
kubectl -n sentinel-system rollout status daemonset/sentinel-node-monitor

kubectl create namespace demo
kubectl label namespace demo sentinel.io/enabled=true
kubectl apply -f config/samples/sentinel_v1_memorypolicy.yaml
kubectl describe memorypolicy web-memory-safety
```

Port-forward the Kubernetes-backed REST API:

```bash
kubectl -n sentinel-system port-forward service/sentinel-api 8090:8090
curl -s http://localhost:8090/policies
curl -s http://localhost:8090/pods/status
```

`/pods/status` is backed by exact node cgroup observations, not estimated Kubernetes metrics. A response entry looks like:

```json
{"namespace":"demo","name":"memory-demo","uid":"...","node":"sentinel-control-plane","usageBytes":112197632,"limitBytes":134217728,"usagePercent":83,"policies":["web-memory-safety"],"collectedAt":"2026-07-18T12:00:00Z"}
```

Each DaemonSet pod owns one bounded ConfigMap containing at most 256 observations for its node, with at most eight applicable policy names per entry. Monitors replace entries by pod UID and remove entries on observed deletion. The API omits observations older than 15 seconds, so a stopped or disconnected monitor cannot present old data as live.

Create a policy through REST:

```bash
curl -sS -X POST http://localhost:8090/policies \
  -H 'Content-Type: application/json' \
  -d '{"name":"api-memory","spec":{"namespaceSelector":{"matchLabels":{"sentinel.io/enabled":"true"}},"labelSelector":{"matchLabels":{"app":"api"}},"thresholdPercent":85,"gracePeriodSeconds":15}}'
```

## Intervention semantics

At or above the threshold, a node worker fetches the current policy, records and logs the breach, sends SIGTERM to processes in the pod cgroup, waits the configured grace period, and re-reads memory. If pressure remains, it creates an Eviction object. PDB rejection is an expected retryable outcome. A bounded, mutex-protected per-pod intervention record keeps that retry in the eviction-only phase, preventing duplicate breach counts and signals while still rechecking memory and current policy state. Unlimited cgroups are not evaluated.

Kubernetes has no generic API for signalling all processes in a container cgroup, so node-local DaemonSet monitoring is required. Eviction is preferred over direct Pod deletion because the `policy/v1` subresource preserves admission controls and PodDisruptionBudget semantics. The Kubernetes API also remains Sentinel's only persistence layer: policies, status, and bounded node snapshots require no separate database.

The node agent is security-sensitive because Kubernetes exposes no generic container signal API. See [the architecture and security model](docs/architecture.md), [the breach sequence](docs/sequence.md), and [development and deployment instructions](docs/development.md).

## Repository map

- `api/v1`, `controllers`: CRD contract and operator lifecycle
- `internal/monitor`, `cgroup`, `workerpool`, `eviction`, `policy`: node data plane
- `web`: standard-library JSON API and middleware
- `config`: CRD, least-privilege RBAC, Deployment, DaemonSet, Service, and sample
- `tests`, `scripts`: opt-in real Kind breach flow

## Quality gates

```bash
make fmt
make vet
make lint
make test-race
make docker-build
```

CI runs all of these code checks and builds the final distroless image. The runtime contains only the statically linked Sentinel binary and distroless certificates/system data.

The worker pool bounds both queued work and active goroutines. Every completed job emits a typed result containing its key, outcome, attempt count, retry flag, and timing. A dedicated buffered fan-in path aggregates worker results; the monitor consumes it to complete, forget, or rate-limit the Kubernetes workqueue key.

## Complete Kind demonstration

The opt-in integration test builds and deploys Sentinel, observes a live REST snapshot, blocks the first eviction with a PDB, opens the PDB, and validates the ordered structured stages under one intervention ID:

```bash
make test-integration
```

Expected stages are `breach_detected`, `sigterm_sent`, `grace_completed`, `memory_rechecked`, `eviction_attempted`, `eviction_rejected`, `eviction_retry`, and `eviction_accepted`. The test also verifies exactly one breach count and signal, plus `lastBreachTime`, through the REST API. Docker, Kind, kubectl, curl, and Python 3 are required; the test remains opt-in and never targets a production kubeconfig intentionally.

## Non-goals and operational limits

Sentinel does not replace correct resource sizing or application-level load shedding. A rapid allocation can still race the scan interval and reach OOM first. SIGTERM is delivered at pod cgroup scope; applications must handle it, and cluster administrators must approve the node-trusted DaemonSet. Eviction behavior is intentionally subject to admission controls and PDB availability.

## Phase 2 — networking and service recovery

An optional `--mode=network-monitor` checks labeled HTTP Services from a normal
pod network, tracks ready EndpointSlices, DNS and HTTP availability, and logs
outage/recovery transitions. Deploy it with `config/manager/network.yaml`.
See [the Phase 2 guide](docs/networking-phase2.md) for failure scenarios,
local-cluster validation and Linux packet-path tracing.
