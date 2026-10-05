# Sentinel Phase 2: service availability during recovery

Phase 1 acts on memory pressure. Phase 2 observes what a client can reach while
pods disappear and replacements become ready. A separate, unprivileged Go
`network-monitor` mode uses the same binary. It runs inside an ordinary pod
network, with read-only API permissions for Services and EndpointSlices.

## Run it

Build and load the updated image (use the same image tag as the manifests):

```sh
make docker-build IMAGE=sentinel:latest
kind load docker-image sentinel:latest --name sentinel
# For MicroK8s, export/import the image into its containerd or use your registry.
```
Install Phase 1 with `kubectl apply -k config`. Phase 2 is opt-in:

```sh
kubectl apply -f config/manager/network.yaml
kubectl apply -f examples/network/workload.yaml
kubectl -n sentinel-system rollout status deployment/sentinel-network-monitor
kubectl -n sentinel-system port-forward deployment/sentinel-network-monitor 18090:8090
# In another terminal:
curl -s http://localhost:18090/network/status
kubectl -n sentinel-system logs deployment/sentinel-network-monitor -f
```

On any existing HTTP Service, add label `sentinel.io/network-check: "true"`.
Select a named port using annotation `sentinel.io/network-port: http` and
optionally a GET path using `sentinel.io/network-path: /health`. One-port
Services select that port automatically. GET checks should use a read-only path.
The default interval is 5 seconds and timeout is 3 seconds per Service.
Configure `--cluster-domain` if the cluster does not use `cluster.local`.

The monitor queries ready, non-terminating endpoints for the selected port,
resolves the Service's DNS name, then sends an HTTP GET through the Service.
It logs availability transitions and retains the latest observations at
`GET /network/status`. Reason values include `Available`, `NoReadyEndpoints`,
`DNSFailed`, `ConnectionFailed`, `HTTPFailed`, `EndpointLookupFailed`,
`InvalidConfiguration`, and `UnsupportedService`.

`unavailableSince` and `lastRecoverySeconds` measure sampled service outages,
including outages which happen during eviction. They are not attributed to a
specific eviction ID. A rollout with sufficient spare capacity may have no
observable outage. The sampling interval can miss brief failures. The snapshot
is in memory and resets when this monitor restarts; `checkedAt`, `observedAt`
and `scanError` let clients recognize stale data or API-access failures.
`/healthz` means the process is serving, not that every monitored app is healthy.

## Failure and recovery demo

With the updated image loaded in a local cluster:

```sh
scripts/network-integration.sh kind-sentinel
# On MicroK8s, or a kind cluster with a policy-enforcing CNI:
scripts/network-integration.sh microk8s --network-policy
```

The script checks a healthy baseline, wrong selector, wrong target port,
zero ready replicas and recovery. The optional scenario blocks ingress with a
NetworkPolicy. It restores the demo workload afterwards and leaves the demo
namespace and network monitor available for inspection. It does not uninstall
the operator. Only use the named disposable test cluster.

For a DNS failure demo, temporarily set `--cluster-domain=invalid.example` in
this network Deployment's args, observe `DNSFailed`, and restore the default.
This tests failure classification; it does not disrupt cluster-wide DNS.

For the eviction connection, apply `examples/network/memorypolicy.yaml` and use
the memory-growth workload technique from Phase 1 on pods matching this demo's
selector. Observe Phase 1 intervention logs, EndpointSlice changes and network
status together. The nginx fixture is a traffic baseline; it does not itself
allocate memory to force a breach. Never infer eviction happened from network
failure alone.

## Trace the actual packet path

Use a disposable Linux cluster. The networking monitor itself does not need
host networking, host PID access, or packet-capture privileges. Debugging tools
are separate from the deployed monitor.

Start a debug container sharing the network monitor pod's network namespace:

```sh
kubectl -n sentinel-system get pods -l app=sentinel-network-monitor
kubectl -n sentinel-system debug -it POD_NAME --image=nicolaka/netshoot --target=monitor --profile=netadmin
# Inside the debug container:
ip -brief address
ip route
cat /etc/resolv.conf
dig web.sentinel-network-demo.svc.cluster.local
curl -v --connect-timeout 3 http://web.sentinel-network-demo.svc.cluster.local/
tcpdump -ni any 'port 53 or tcp port 80'
```

In another terminal inspect the control-plane routing inputs:

```sh
kubectl -n sentinel-network-demo get pods -o wide
kubectl -n sentinel-network-demo get service web -o yaml
kubectl -n sentinel-network-demo get endpointslice -l kubernetes.io/service-name=web -o yaml
kubectl -n sentinel-network-demo get networkpolicy
```

Compare a direct pod-IP request with a Service-name request. Direct pod access
working while Service access fails narrows the investigation toward Service
routing or DNS; it is not a complete diagnosis by itself.

On the Linux node hosting the probe pod, find the pod sandbox with `sudo crictl
pods`, inspect it with `sudo crictl inspectp SANDBOX_ID` and obtain its process
PID from the runtime output. Runtime layouts differ. Then inspect rather than
modify its network namespace:

```sh
sudo nsenter -t SANDBOX_PID -n ip address
sudo nsenter -t SANDBOX_PID -n ip route
sudo nsenter -t SANDBOX_PID -n cat /sys/class/net/eth0/iflink
ip -o link
sudo tcpdump -ni any 'tcp port 80'
```

To collect the evidence in one folder, run on the test node while generating
requests from a second terminal:

```sh
sudo scripts/network-trace.sh SANDBOX_PID /tmp/sentinel-network-evidence 10
```

The collector records namespace identity, pod/node interfaces and routes, CNI
config filenames, and a bounded DNS/HTTP packet capture. It does not copy CNI
credentials or alter routes.

The `iflink` index can identify the host-side veth peer on a veth-based CNI.
Record the actual interface and route names; do not assume every CNI uses a
bridge or the same overlay. Inspect `/etc/cni/net.d` on the node. On an iptables
kube-proxy cluster, inspect `sudo iptables-save -t nat`; for IPVS inspect
`sudo ipvsadm -Ln`. An eBPF Service implementation may use neither path: use
that CNI's tools. For kind, the node itself is a Docker container, so node
commands must run in that container, not blindly on the laptop host.

Save the packet capture and an explanation of what you observed: DNS query,
Service destination, selected pod IP, request/response, and what changed during
failure and recovery. That is the evidence for networking internals experience.

## Limits and interpretation

- HTTP over TCP only. No TLS, authentication, UDP, ExternalName or headless checks.
- Checks run from one pod's perspective, not every node or real external client.
- Endpoint availability describes selected endpoint readiness, not pod history.
- A timeout is reported as `ConnectionFailed`. It does not prove a NetworkPolicy,
  CNI or kube-proxy fault. Verify policies, routes and captures before assigning cause.
- NetworkPolicy scenarios require enforcement by the installed CNI. Default kind
  networking alone does not provide that enforcement.
- Scans are sequential and bounded per Service. Large numbers of Services increase
  the effective scan interval; this is a focused demo, not a fleet monitoring system.
- Direct SIGTERM in Phase 1 happens before PDB admission. PDB enforcement on the
  later eviction does not protect against that earlier signalling. This existing
  limitation matters when interpreting traffic availability and should be considered
  before production use.

## Verify and remove

```sh
go test ./... -race
go vet ./...
scripts/network-integration.sh kind-sentinel
kubectl delete -f examples/network/workload.yaml
kubectl delete -f config/manager/network.yaml
```

Unit tests cover stage classification, endpoint filtering, outage timing and
removal of deleted Services. The local-cluster script exercises actual routing;
fake-client unit tests cannot establish that CNI, DNS or Service routing works.

References:
- https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/
- https://kubernetes.io/docs/concepts/services-networking/network-policies/
- https://kubernetes.io/docs/tasks/debug/debug-application/debug-service/
