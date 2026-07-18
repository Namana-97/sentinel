# Developer guide

Prerequisites are Go 1.24+, Docker, `kubectl`, Kustomize support in kubectl, and optionally Kind.

```bash
make fmt vet test-race
make docker-build IMAGE=sentinel:dev
kind create cluster --name sentinel
kind load docker-image sentinel:dev --name sentinel
kubectl apply -k config
kubectl -n sentinel-system set image deployment/sentinel-operator manager=sentinel:dev
kubectl -n sentinel-system set image daemonset/sentinel-node-monitor monitor=sentinel:dev
```

For MicroK8s, import the image into containerd or publish it to a registry, replace the image in `config/kustomization.yaml`, then run `microk8s kubectl apply -k config`.

`go test ./... -race` covers concurrency-sensitive units. The Kind test creates a real limited-memory pod and causes an actual eviction:

```bash
make test-integration
```

The integration test creates or reuses `sentinel-test`; a cluster it creates is deleted afterward. Do not run it against a production kubeconfig.

The script port-forwards the REST API, requires a fresh cgroup snapshot, verifies breach status, intentionally rejects the first eviction with a PodDisruptionBudget, then permits retry. Structured `intervention_id` and `stage` fields prove that SIGTERM precedes eviction and that the eviction-only retry does not duplicate signalling or breach accounting.

## Controller lifecycle

1. controller-runtime starts its cache and elects one active operator replica.
2. a `MemoryPolicy` event enqueues its object key.
3. reconciliation validates selectors, counts matching cached running pods, refreshes the protected registry, and patches the `Ready` condition.
4. deletion removes the registry entry. Node monitor dynamic informers independently remove their cached copy.
5. transient client failures are returned to controller-runtime for rate-limited retry.
