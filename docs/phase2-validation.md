# Phase 2 validation

Validated on 2026-10-05 with Go 1.24.0:

- `go test ./... -race`: passed across the existing and new packages.
- `go test ./internal/network -race -count=1`: passed, including the newly added timeout/configuration cases.
- `go vet ./...`: passed.
- Go formatting checked; new/modified Go source formatted with gofmt.
- New Kubernetes fixtures parsed as YAML.
- `bash -n` passed for both new scripts.

The local-cluster integration script and packet collector were not executed:
this implementation workspace has no Docker, kubectl or Kubernetes runtime.
They require validation on your disposable local cluster before describing
Phase 2 as demonstrated end to end. YAML parsing is not server-side API validation.

Phase 2 is an optional mode of the existing binary, with its own least-privilege
ServiceAccount and Deployment. Existing memory policies and eviction behavior
are retained. Networking observations are sampled and independent of eviction
IDs; they do not guarantee zero downtime or automatically identify the cause
of a network timeout.
