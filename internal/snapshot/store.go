// Package snapshot persists bounded, node-owned pod memory observations in Kubernetes ConfigMaps.
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	// DefaultNamespace contains node snapshot ConfigMaps in the standard installation.
	DefaultNamespace = "sentinel-system"
	// DataKey is the ConfigMap key containing the JSON snapshot array.
	DataKey = "snapshots.json"
	// LabelKey identifies ConfigMaps owned by Sentinel snapshot publishers.
	LabelKey = "sentinel.io/memory-snapshots"
	// DefaultMaxEntries bounds observations retained by each node monitor.
	DefaultMaxEntries = 256
	// DefaultFreshness is the maximum age returned by the REST API.
	DefaultFreshness = 15 * time.Second
)

// PodMemory is one exact, finite cgroup memory observation.
type PodMemory struct {
	Namespace    string    `json:"namespace"`
	PodName      string    `json:"podName"`
	PodUID       string    `json:"podUID"`
	Node         string    `json:"node"`
	UsageBytes   uint64    `json:"usageBytes"`
	LimitBytes   uint64    `json:"limitBytes"`
	UsagePercent uint64    `json:"usagePercent"`
	Policies     []string  `json:"policies"`
	CollectedAt  time.Time `json:"collectedAt"`
}

// Store owns one bounded ConfigMap for a node. Only that node's monitor writes it.
type Store struct {
	Client     kubernetes.Interface
	Namespace  string
	NodeName   string
	MaxEntries int
	Freshness  time.Duration
	Now        func() time.Time
	mu         sync.Mutex
}

func (s *Store) defaults() {
	if s.Namespace == "" {
		s.Namespace = DefaultNamespace
	}
	if s.MaxEntries <= 0 {
		s.MaxEntries = DefaultMaxEntries
	}
	if s.Freshness <= 0 {
		s.Freshness = DefaultFreshness
	}
	if s.Now == nil {
		s.Now = time.Now
	}
}

// ConfigMapName returns a deterministic DNS-safe name without exposing long node names.
func ConfigMapName(node string) string {
	clean := strings.ToLower(node)
	clean = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, clean)
	clean = strings.Trim(clean, "-")
	if len(clean) > 35 {
		clean = clean[:35]
	}
	sum := sha256.Sum256([]byte(node))
	return "sentinel-memory-" + clean + "-" + hex.EncodeToString(sum[:4])
}

// Publish inserts or replaces a pod observation, drops stale entries, and enforces the bound.
func (s *Store) Publish(ctx context.Context, value PodMemory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaults()
	if len(value.Policies) > 8 {
		value.Policies = append([]string(nil), value.Policies[:8]...)
	}
	return s.mutate(ctx, true, func(items []PodMemory) []PodMemory {
		items = fresh(items, s.Now().Add(-s.Freshness))
		out := items[:0]
		for _, item := range items {
			if item.PodUID != value.PodUID {
				out = append(out, item)
			}
		}
		out = append(out, value)
		sort.Slice(out, func(i, j int) bool { return out[i].CollectedAt.After(out[j].CollectedAt) })
		if len(out) > s.MaxEntries {
			out = out[:s.MaxEntries]
		}
		return out
	})
}

// Delete removes an observation immediately when a pod deletion event is observed.
func (s *Store) Delete(ctx context.Context, namespace, name, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaults()
	return s.mutate(ctx, false, func(items []PodMemory) []PodMemory {
		out := items[:0]
		for _, item := range items {
			matches := uid != "" && item.PodUID == uid
			if uid == "" {
				matches = item.Namespace == namespace && item.PodName == name
			}
			if !matches {
				out = append(out, item)
			}
		}
		return fresh(out, s.Now().Add(-s.Freshness))
	})
}

func (s *Store) mutate(ctx context.Context, create bool, change func([]PodMemory) []PodMemory) error {
	name := ConfigMapName(s.NodeName)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if !create {
				return nil
			}
			items := change(nil)
			data, marshalErr := json.Marshal(items)
			if marshalErr != nil {
				return marshalErr
			}
			_, createErr := s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace, Labels: map[string]string{LabelKey: "true"}},
				Data:       map[string]string{DataKey: string(data)},
			}, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		items, err := Decode(cm)
		if err != nil {
			return err
		}
		data, err := json.Marshal(change(items))
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data[DataKey] = string(data)
		_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

// Decode parses observations from a snapshot ConfigMap.
func Decode(cm *corev1.ConfigMap) ([]PodMemory, error) {
	if cm.Data == nil || cm.Data[DataKey] == "" {
		return nil, nil
	}
	var items []PodMemory
	if err := json.Unmarshal([]byte(cm.Data[DataKey]), &items); err != nil {
		return nil, fmt.Errorf("decode snapshot ConfigMap %s/%s: %w", cm.Namespace, cm.Name, err)
	}
	return items, nil
}

// Fresh returns observations newer than the cutoff.
func Fresh(items []PodMemory, cutoff time.Time) []PodMemory { return fresh(items, cutoff) }

func fresh(items []PodMemory, cutoff time.Time) []PodMemory {
	out := make([]PodMemory, 0, len(items))
	for _, item := range items {
		if !item.CollectedAt.Before(cutoff) {
			out = append(out, item)
		}
	}
	return out
}
