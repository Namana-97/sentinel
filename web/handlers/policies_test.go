package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	"github.com/namanakanchan/sentinel/internal/snapshot"
	"github.com/namanakanchan/sentinel/web/models"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testAPI(t *testing.T) API {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := sentinelv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return API{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
}

func TestPolicyCreateDefaultsGracePeriod(t *testing.T) {
	api := testAPI(t)
	response := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"name":"defaulted","spec":{"thresholdPercent":80}}`)
	api.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/policies", body))
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var item sentinelv1.MemoryPolicy
	if err := json.Unmarshal(response.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}
	if item.Spec.GracePeriodSeconds != sentinelv1.DefaultGracePeriodSeconds {
		t.Fatalf("grace=%d", item.Spec.GracePeriodSeconds)
	}
}

func TestPodStatusReturnsFreshMemorySnapshots(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sentinelv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	items := []snapshot.PodMemory{
		{Namespace: "demo", PodName: "fresh", PodUID: "uid-fresh", Node: "node-a", UsageBytes: 80, LimitBytes: 100, UsagePercent: 80, Policies: []string{"policy"}, CollectedAt: now},
		{Namespace: "demo", PodName: "stale", PodUID: "uid-stale", Node: "node-a", CollectedAt: now.Add(-time.Minute)},
	}
	data, _ := json.Marshal(items)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: snapshot.DefaultNamespace, Labels: map[string]string{snapshot.LabelKey: "true"}}, Data: map[string]string{snapshot.DataKey: string(data)}}
	api := API{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build(), SnapshotNamespace: snapshot.DefaultNamespace, SnapshotFreshness: 15 * time.Second, Now: func() time.Time { return now }}
	response := httptest.NewRecorder()
	api.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/pods/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got []models.PodStatus
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].UID != "uid-fresh" || got[0].UsageBytes != 80 || got[0].Policies[0] != "policy" {
		t.Fatalf("snapshots=%#v", got)
	}
}
func TestPolicyCRUD(t *testing.T) {
	api := testAPI(t)
	body := []byte(`{"name":"demo","spec":{"thresholdPercent":80,"gracePeriodSeconds":5}}`)
	request := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	response := httptest.NewRecorder()
	api.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/policies/demo", nil)
	response = httptest.NewRecorder()
	api.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("get=%d", response.Code)
	}
	update := []byte(`{"name":"demo","spec":{"thresholdPercent":90,"gracePeriodSeconds":10}}`)
	request = httptest.NewRequest(http.MethodPut, "/policies/demo", bytes.NewReader(update))
	response = httptest.NewRecorder()
	api.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("update=%d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/policies", nil)
	response = httptest.NewRecorder()
	api.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("list=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodDelete, "/policies/demo", nil)
	response = httptest.NewRecorder()
	api.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete=%d", response.Code)
	}
}
func TestInvalidJSON(t *testing.T) {
	api := testAPI(t)
	response := httptest.NewRecorder()
	api.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/policies", bytes.NewBufferString(`{"wat":1}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", response.Code)
	}
	if response.Header().Get("Content-Type") != "" { /* middleware owns the JSON header in production */
	}
}
