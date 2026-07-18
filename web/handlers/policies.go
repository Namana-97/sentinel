// Package handlers implements the Sentinel JSON API over Kubernetes resources.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	"github.com/namanakanchan/sentinel/internal/snapshot"
	"github.com/namanakanchan/sentinel/web/models"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// API translates every REST operation directly to the Kubernetes client.
type API struct {
	Client            client.Client
	SnapshotNamespace string
	SnapshotFreshness time.Duration
	Now               func() time.Time
}

// Routes returns the complete API mux.
func (a API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /policies", a.create)
	mux.HandleFunc("GET /policies", a.list)
	mux.HandleFunc("GET /policies/{name}", a.get)
	mux.HandleFunc("PUT /policies/{name}", a.update)
	mux.HandleFunc("DELETE /policies/{name}", a.delete)
	mux.HandleFunc("GET /pods/status", a.podStatus)
	return mux
}

func (a API) create(w http.ResponseWriter, r *http.Request) {
	var request models.PolicyRequest
	if !decode(w, r, &request) {
		return
	}
	if request.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	item := &sentinelv1.MemoryPolicy{TypeMeta: metav1.TypeMeta{APIVersion: sentinelv1.GroupVersion.String(), Kind: "MemoryPolicy"}, ObjectMeta: metav1.ObjectMeta{Name: request.Name}, Spec: request.Spec.MemoryPolicySpec()}
	if err := a.Client.Create(r.Context(), item); err != nil {
		handleKubeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, item)
}
func (a API) list(w http.ResponseWriter, r *http.Request) {
	var list sentinelv1.MemoryPolicyList
	if err := a.Client.List(r.Context(), &list); err != nil {
		handleKubeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &list)
}
func (a API) get(w http.ResponseWriter, r *http.Request) {
	item, ok := a.find(w, r.Context(), r.PathValue("name"))
	if ok {
		writeJSON(w, http.StatusOK, item)
	}
}
func (a API) update(w http.ResponseWriter, r *http.Request) {
	current, ok := a.find(w, r.Context(), r.PathValue("name"))
	if !ok {
		return
	}
	var request models.PolicyRequest
	if !decode(w, r, &request) {
		return
	}
	if request.Name != "" && request.Name != current.Name {
		writeError(w, http.StatusBadRequest, "body name must match path")
		return
	}
	current.Spec = request.Spec.MemoryPolicySpec()
	if err := a.Client.Update(r.Context(), current); err != nil {
		handleKubeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, current)
}
func (a API) delete(w http.ResponseWriter, r *http.Request) {
	current, ok := a.find(w, r.Context(), r.PathValue("name"))
	if !ok {
		return
	}
	if err := a.Client.Delete(r.Context(), current); err != nil {
		handleKubeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (a API) find(w http.ResponseWriter, ctx context.Context, name string) (*sentinelv1.MemoryPolicy, bool) {
	item := &sentinelv1.MemoryPolicy{}
	if err := a.Client.Get(ctx, types.NamespacedName{Name: name}, item); err != nil {
		handleKubeError(w, err)
		return nil, false
	}
	return item, true
}

func (a API) podStatus(w http.ResponseWriter, r *http.Request) {
	namespace := a.SnapshotNamespace
	if namespace == "" {
		namespace = snapshot.DefaultNamespace
	}
	freshness := a.SnapshotFreshness
	if freshness <= 0 {
		freshness = snapshot.DefaultFreshness
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	var configMaps corev1.ConfigMapList
	if err := a.Client.List(r.Context(), &configMaps, client.InNamespace(namespace), client.MatchingLabels{snapshot.LabelKey: "true"}); err != nil {
		handleKubeError(w, err)
		return
	}
	result := make([]models.PodStatus, 0)
	cutoff := now().Add(-freshness)
	for i := range configMaps.Items {
		items, err := snapshot.Decode(&configMaps.Items[i])
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, item := range snapshot.Fresh(items, cutoff) {
			result = append(result, models.PodStatus{Namespace: item.Namespace, Name: item.PodName, UID: item.PodUID, Node: item.Node, UsageBytes: item.UsageBytes, LimitBytes: item.LimitBytes, UsagePercent: item.UsagePercent, Policies: item.Policies, CollectedAt: item.CollectedAt})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace == result[j].Namespace {
			return result[i].Name < result[j].Name
		}
		return result[i].Namespace < result[j].Namespace
	})
	writeJSON(w, http.StatusOK, result)
}

func decode(w http.ResponseWriter, r *http.Request, target interface{}) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON object")
		return false
	}
	return true
}
func handleKubeError(w http.ResponseWriter, err error) {
	switch {
	case apierrors.IsNotFound(err):
		writeError(w, http.StatusNotFound, "resource not found")
	case apierrors.IsAlreadyExists(err):
		writeError(w, http.StatusConflict, "resource already exists")
	case apierrors.IsConflict(err):
		writeError(w, http.StatusConflict, "resource version conflict")
	case apierrors.IsInvalid(err):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case apierrors.IsForbidden(err):
		writeError(w, http.StatusForbidden, "operation forbidden")
	default:
		writeError(w, http.StatusInternalServerError, "Kubernetes API request failed")
	}
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, models.Error{Error: message})
}
func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
