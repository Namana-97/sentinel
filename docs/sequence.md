# Breach sequence

```mermaid
sequenceDiagram
  participant I as Shared informers
  participant Q as Workqueue
  participant W as Bounded worker
  participant C as Linux cgroup
  participant K as Kubernetes API
  I->>Q: enqueue local pod key
  Q->>W: dispatch with timeout
  W->>C: read usage and finite limit
  W->>K: GET current MemoryPolicy
  W->>K: update status subresource
  W->>W: create bounded intervention state + ID
  W->>K: publish node snapshot ConfigMap
  W->>W: stage=breach_detected
  W->>C: SIGTERM cgroup processes
  W->>W: stage=sigterm_sent
  W->>W: wait grace period (cancellable)
  W->>W: stage=grace_completed
  W->>C: re-read exact memory values
  W->>W: stage=memory_rechecked
  alt recovered
    W->>Q: forget key
  else still above threshold
    W->>K: create policy/v1 Eviction
    K->>K: enforce PodDisruptionBudget
    alt accepted
      K-->>W: 200
    else PDB rejects
      K-->>W: 429
      W->>W: retain eviction-only state
      W->>Q: rate-limited retry
    end
  end
```

The latest policy is fetched after detecting a breach so a stale informer observation cannot apply superseded thresholds. Status updates use optimistic conflict retries. On a PDB retry, current memory and policy are evaluated again, but breach accounting, SIGTERM, and the grace wait are not repeated.
