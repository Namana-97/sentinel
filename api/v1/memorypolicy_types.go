package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// DefaultGracePeriodSeconds is shared by the CRD and REST adapter.
const DefaultGracePeriodSeconds int32 = 30

// MemoryPolicySpec selects pods and defines when Sentinel intervenes.
type MemoryPolicySpec struct {
	// NamespaceSelector selects namespaces containing candidate pods.
	// +optional
	NamespaceSelector metav1.LabelSelector `json:"namespaceSelector,omitempty"`
	// LabelSelector selects pods within matching namespaces.
	// +optional
	LabelSelector metav1.LabelSelector `json:"labelSelector,omitempty"`
	// ThresholdPercent is the percentage of the cgroup limit that triggers intervention.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	ThresholdPercent int32 `json:"thresholdPercent"`
	// GracePeriodSeconds is the delay between SIGTERM and the memory recheck.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=600
	// +kubebuilder:default=30
	GracePeriodSeconds int32 `json:"gracePeriodSeconds"`
}

// MemoryPolicyStatus reports the policy's observed runtime state.
type MemoryPolicyStatus struct {
	ActivePods     int32              `json:"activePods,omitempty"`
	BreachCount    int64              `json:"breachCount,omitempty"`
	LastBreachTime *metav1.Time       `json:"lastBreachTime,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mp
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Threshold",type=integer,JSONPath=`.spec.thresholdPercent`
// +kubebuilder:printcolumn:name="Active Pods",type=integer,JSONPath=`.status.activePods`
// +kubebuilder:printcolumn:name="Breaches",type=integer,JSONPath=`.status.breachCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MemoryPolicy is the cluster-scoped configuration for proactive memory intervention.
type MemoryPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MemoryPolicySpec   `json:"spec,omitempty"`
	Status            MemoryPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MemoryPolicyList is a list of MemoryPolicy objects.
type MemoryPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MemoryPolicy `json:"items"`
}

func init() { SchemeBuilder.Register(&MemoryPolicy{}, &MemoryPolicyList{}) }
