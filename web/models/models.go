// Package models defines the stable Sentinel REST representations.
package models

import (
	"time"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyRequest is accepted by create and replace operations.
type PolicyRequest struct {
	Name string            `json:"name"`
	Spec PolicySpecRequest `json:"spec"`
}

// PolicySpecRequest uses a pointer for grace period so omission can apply the CRD default.
type PolicySpecRequest struct {
	NamespaceSelector  metav1.LabelSelector `json:"namespaceSelector,omitempty"`
	LabelSelector      metav1.LabelSelector `json:"labelSelector,omitempty"`
	ThresholdPercent   int32                `json:"thresholdPercent"`
	GracePeriodSeconds *int32               `json:"gracePeriodSeconds,omitempty"`
}

// MemoryPolicySpec returns a fully defaulted Kubernetes policy spec.
func (r PolicySpecRequest) MemoryPolicySpec() sentinelv1.MemoryPolicySpec {
	grace := sentinelv1.DefaultGracePeriodSeconds
	if r.GracePeriodSeconds != nil {
		grace = *r.GracePeriodSeconds
	}
	return sentinelv1.MemoryPolicySpec{NamespaceSelector: r.NamespaceSelector, LabelSelector: r.LabelSelector, ThresholdPercent: r.ThresholdPercent, GracePeriodSeconds: grace}
}

// PodStatus reports which policy applies to a Kubernetes pod.
type PodStatus struct {
	Namespace    string    `json:"namespace"`
	Name         string    `json:"name"`
	UID          string    `json:"uid"`
	Node         string    `json:"node"`
	UsageBytes   uint64    `json:"usageBytes"`
	LimitBytes   uint64    `json:"limitBytes"`
	UsagePercent uint64    `json:"usagePercent"`
	Policies     []string  `json:"policies"`
	CollectedAt  time.Time `json:"collectedAt"`
}

// Error is the uniform JSON error envelope.
type Error struct {
	Error string `json:"error"`
}
