package network

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestCheckStages(t *testing.T) {
	for _, tc := range []struct {
		name            string
		ready           bool
		dnsError        bool
		httpStatus      int
		connectionError bool
		want            string
	}{
		{"healthy", true, false, 200, false, "Available"},
		{"selector or readiness failure", false, false, 200, false, "NoReadyEndpoints"},
		{"DNS failure", true, true, 200, false, "DNSFailed"},
		{"application error", true, false, 503, false, "HTTPFailed"},
		{"blocked connection", true, false, 0, true, "ConnectionFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "demo"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10", Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP}}}}
			name := "http"
			port := int32(8080)
			slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "app-1", Namespace: "demo", Labels: map[string]string{"kubernetes.io/service-name": "app"}}, Ports: []discoveryv1.EndpointPort{{Name: &name, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.244.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: &tc.ready}}}}
			m, _ := New(fake.NewSimpleClientset(slice), slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second, time.Second, "cluster.local")
			m.Lookup = func(ctx context.Context, host string) ([]string, error) {
				if host != "app.demo.svc.cluster.local" {
					t.Fatal(host)
				}
				if tc.dnsError {
					return nil, fmt.Errorf("DNS unavailable")
				}
				return []string{"10.96.0.10"}, nil
			}
			m.HTTP = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != "http://app.demo.svc.cluster.local:80/" {
					t.Fatal(req.URL)
				}
				if tc.connectionError {
					return nil, fmt.Errorf("timeout")
				}
				return &http.Response{StatusCode: tc.httpStatus, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
			})}
			if got := m.Check(context.Background(), svc); got.Reason != tc.want {
				t.Fatalf("got %+v; want %s", got, tc.want)
			}
		})
	}
}
func TestRecoveryDuration(t *testing.T) {
	start := time.Unix(100, 0)
	a := transition(Result{}, Result{ObservedAt: start, Reason: "NoReadyEndpoints"})
	b := transition(a, Result{ObservedAt: start.Add(3 * time.Second), Reason: "ConnectionFailed"})
	c := transition(b, Result{ObservedAt: start.Add(8 * time.Second), Available: true})
	if c.UnavailableSince != nil || c.LastRecoverySeconds != 8 {
		t.Fatalf("bad recovery: %+v", c)
	}
}
func TestNoReadyTerminatingOrWrongPortEndpoints(t *testing.T) {
	for _, kind := range []string{"terminating", "wrong-port"} {
		t.Run(kind, func(t *testing.T) {
			yes := true
			name := "http"
			if kind == "wrong-port" {
				name = "other"
			}
			port := int32(80)
			conditions := discoveryv1.EndpointConditions{Ready: &yes}
			if kind == "terminating" {
				conditions.Terminating = &yes
			}
			slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "demo", Labels: map[string]string{"kubernetes.io/service-name": "app"}}, Ports: []discoveryv1.EndpointPort{{Name: &name, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Conditions: conditions}}}
			m, _ := New(fake.NewSimpleClientset(slice), slog.Default(), time.Second, time.Second, "cluster.local")
			m.Lookup = func(context.Context, string) ([]string, error) { return []string{"10.96.0.10"}, nil }
			svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "demo"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10", Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP}}}}
			if r := m.Check(context.Background(), svc); r.ReadyEndpoints != 0 || r.Reason != "NoReadyEndpoints" {
				t.Fatal(r)
			}
		})
	}
}
func TestScanRemovesDeletedServices(t *testing.T) {
	client := fake.NewSimpleClientset([]runtime.Object{}...)
	m, _ := New(client, slog.Default(), time.Second, time.Second, "cluster.local")
	m.results["demo/deleted"] = Result{Service: "deleted"}
	m.Scan(context.Background())
	if len(m.results) != 0 {
		t.Fatal("stale deleted service")
	}
}

func TestHTTPTimeoutAndCancellation(t *testing.T) {
	svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "demo"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10", Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP}}}}
	name := "http"
	port := int32(80)
	ready := true
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "demo", Labels: map[string]string{"kubernetes.io/service-name": "app"}}, Ports: []discoveryv1.EndpointPort{{Name: &name, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}
	m, _ := New(fake.NewSimpleClientset(slice), slog.Default(), time.Second, time.Second, "cluster.local")
	m.Lookup = func(context.Context, string) ([]string, error) { return []string{"10.96.0.10"}, nil }
	m.HTTP = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if r := m.Check(ctx, svc); r.Reason != "ConnectionFailed" {
		t.Fatal(r)
	}
}

func TestUnsupportedAndAmbiguousServices(t *testing.T) {
	m, _ := New(fake.NewSimpleClientset(), slog.Default(), time.Second, time.Second, "cluster.local")
	for _, tc := range []struct {
		spec   corev1.ServiceSpec
		reason string
	}{
		{corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: "example.com"}, "UnsupportedService"},
		{corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone}, "UnsupportedService"},
		{corev1.ServiceSpec{ClusterIP: "10.96.0.10", Ports: []corev1.ServicePort{{Name: "a", Port: 80, Protocol: corev1.ProtocolTCP}, {Name: "b", Port: 81, Protocol: corev1.ProtocolTCP}}}, "InvalidConfiguration"},
	} {
		if r := m.Check(context.Background(), corev1.Service{Spec: tc.spec}); r.Reason != tc.reason {
			t.Fatal(r)
		}
	}
}
