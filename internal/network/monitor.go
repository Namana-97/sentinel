// Package network observes Service availability from an ordinary pod network.
package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const Selector = "sentinel.io/network-check=true"

type Result struct {
	Namespace           string     `json:"namespace"`
	Service             string     `json:"service"`
	ObservedAt          time.Time  `json:"observedAt"`
	ReadyEndpoints      int        `json:"readyEndpoints"`
	DNSAddresses        []string   `json:"dnsAddresses,omitempty"`
	HTTPStatus          int        `json:"httpStatus,omitempty"`
	Reason              string     `json:"reason"`
	Message             string     `json:"message,omitempty"`
	Available           bool       `json:"available"`
	UnavailableSince    *time.Time `json:"unavailableSince,omitempty"`
	LastRecoverySeconds float64    `json:"lastRecoverySeconds,omitempty"`
}

type Monitor struct {
	Client        kubernetes.Interface
	Interval      time.Duration
	Timeout       time.Duration
	ClusterDomain string
	Log           *slog.Logger
	Lookup        func(context.Context, string) ([]string, error)
	HTTP          *http.Client
	mu            sync.RWMutex
	results       map[string]Result
	lastScan      time.Time
	scanError     string
}

func New(client kubernetes.Interface, log *slog.Logger, interval, timeout time.Duration, domain string) (*Monitor, error) {
	if interval <= 0 || timeout <= 0 || domain == "" {
		return nil, fmt.Errorf("network interval, timeout and cluster domain must be configured")
	}
	return &Monitor{Client: client, Log: log, Interval: interval, Timeout: timeout, ClusterDomain: domain,
		Lookup: net.DefaultResolver.LookupHost, HTTP: &http.Client{Timeout: timeout, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, results: make(map[string]Result)}, nil
}

// Run bounds each scan and serializes probes: no unbounded goroutines or overlapping scans.
func (m *Monitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		m.Scan(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (m *Monitor) Scan(ctx context.Context) {
	listCtx, cancel := context.WithTimeout(ctx, m.Timeout)
	services, err := m.Client.CoreV1().Services("").List(listCtx, metav1.ListOptions{LabelSelector: Selector})
	cancel()
	if err != nil {
		m.mu.Lock()
		m.scanError = err.Error()
		m.mu.Unlock()
		m.Log.Error("network scan failed", "error", err)
		return
	}
	next := make(map[string]Result)
	for _, svc := range services.Items {
		if ctx.Err() != nil {
			return
		}
		checkCtx, stop := context.WithTimeout(ctx, m.Timeout)
		result := m.Check(checkCtx, svc)
		stop()
		key := svc.Namespace + "/" + svc.Name
		m.mu.RLock()
		previous, exists := m.results[key]
		m.mu.RUnlock()
		result = transition(previous, result)
		if !exists || previous.Reason != result.Reason || previous.Available != result.Available {
			m.Log.Info("service availability changed", "namespace", svc.Namespace, "service", svc.Name, "available", result.Available, "reason", result.Reason, "message", result.Message, "last_recovery_seconds", result.LastRecoverySeconds)
		}
		next[key] = result
	}
	m.mu.Lock()
	m.results = next
	m.lastScan = time.Now().UTC()
	m.scanError = ""
	m.mu.Unlock()
}

func transition(previous, current Result) Result {
	current.LastRecoverySeconds = previous.LastRecoverySeconds
	if !current.Available {
		if previous.UnavailableSince != nil {
			current.UnavailableSince = previous.UnavailableSince
		} else {
			t := current.ObservedAt
			current.UnavailableSince = &t
		}
	} else if previous.UnavailableSince != nil {
		current.LastRecoverySeconds = current.ObservedAt.Sub(*previous.UnavailableSince).Seconds()
	}
	return current
}

// Check observes endpoints, DNS and HTTP independently enough to locate the failed stage.
// It does not claim that a timeout proves a NetworkPolicy or CNI failure.
func (m *Monitor) Check(ctx context.Context, svc corev1.Service) Result {
	r := Result{Namespace: svc.Namespace, Service: svc.Name, ObservedAt: time.Now().UTC()}
	fail := func(reason string, err error) Result {
		r.Reason = reason
		if err != nil {
			r.Message = err.Error()
		}
		return r
	}
	if svc.Spec.Type == corev1.ServiceTypeExternalName || svc.Spec.ClusterIP == corev1.ClusterIPNone || svc.Spec.ClusterIP == "" {
		return fail("UnsupportedService", fmt.Errorf("only Services with a ClusterIP are supported"))
	}
	portName := svc.Annotations["sentinel.io/network-port"]
	var port *corev1.ServicePort
	for i := range svc.Spec.Ports {
		p := &svc.Spec.Ports[i]
		if p.Protocol == corev1.ProtocolTCP && ((portName != "" && p.Name == portName) || (portName == "" && len(svc.Spec.Ports) == 1)) {
			port = p
			break
		}
	}
	if port == nil {
		return fail("InvalidConfiguration", fmt.Errorf("select a TCP Service port with sentinel.io/network-port; one-port Services select automatically"))
	}
	path := svc.Annotations["sentinel.io/network-path"]
	if path == "" {
		path = "/"
	}
	if path[0] != '/' {
		return fail("InvalidConfiguration", fmt.Errorf("network-path must start with /"))
	}
	slices, err := m.Client.DiscoveryV1().EndpointSlices(svc.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=" + svc.Name})
	if err != nil {
		return fail("EndpointLookupFailed", err)
	}
	for _, slice := range slices.Items {
		matches := false
		for _, p := range slice.Ports {
			if p.Port != nil && (p.Protocol == nil || *p.Protocol == corev1.ProtocolTCP) && ((p.Name == nil && port.Name == "") || (p.Name != nil && *p.Name == port.Name)) {
				matches = true
			}
		}
		if !matches {
			continue
		}
		for _, ep := range slice.Endpoints {
			if (ep.Conditions.Ready == nil || *ep.Conditions.Ready) && (ep.Conditions.Terminating == nil || !*ep.Conditions.Terminating) {
				r.ReadyEndpoints++
			}
		}
	}
	host := svc.Name + "." + svc.Namespace + ".svc." + m.ClusterDomain
	addresses, err := m.Lookup(ctx, host)
	r.DNSAddresses = addresses
	if r.ReadyEndpoints == 0 {
		return fail("NoReadyEndpoints", fmt.Errorf("check pod readiness, Service selector and EndpointSlice ports"))
	}
	if err != nil {
		return fail("DNSFailed", err)
	}
	if len(addresses) == 0 {
		return fail("DNSFailed", fmt.Errorf("DNS returned no addresses"))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, strconv.Itoa(int(port.Port)))+path, nil)
	if err != nil {
		return fail("InvalidConfiguration", err)
	}
	response, err := m.HTTP.Do(req)
	if err != nil {
		return fail("ConnectionFailed", err)
	}
	response.Body.Close()
	r.HTTPStatus = response.StatusCode
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fail("HTTPFailed", fmt.Errorf("HTTP status %d", response.StatusCode))
	}
	r.Available = true
	r.Reason = "Available"
	return r
}

// ServeHTTP exposes the last snapshot, including scan errors and time for freshness checks.
func (m *Monitor) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		CheckedAt time.Time         `json:"checkedAt"`
		ScanError string            `json:"scanError,omitempty"`
		Services  map[string]Result `json:"services"`
	}{m.lastScan, m.scanError, m.results})
}
