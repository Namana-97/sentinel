// Package kube contains Kubernetes API adapters used outside reconcilers.
package kube

import (
	"context"
	"fmt"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// MemoryPolicyGVR returns the dynamic resource identity used by node monitors.
func MemoryPolicyGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "sentinel.io", Version: "v1", Resource: "memorypolicies"}
}

// PolicyClient performs direct, typed conversions over the dynamic API.
type PolicyClient struct{ Dynamic dynamic.Interface }

// Get retrieves the latest policy before intervention.
func (c PolicyClient) Get(ctx context.Context, name string) (*sentinelv1.MemoryPolicy, error) {
	raw, err := c.Dynamic.Resource(MemoryPolicyGVR()).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get current policy: %w", err)
	}
	out := &sentinelv1.MemoryPolicy{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, out); err != nil {
		return nil, fmt.Errorf("decode policy: %w", err)
	}
	return out, nil
}

// List converts an informer snapshot to typed policies.
func ListPolicies(objects []interface{}) ([]sentinelv1.MemoryPolicy, error) {
	out := make([]sentinelv1.MemoryPolicy, 0, len(objects))
	for _, object := range objects {
		accessor, ok := object.(interface{ UnstructuredContent() map[string]interface{} })
		if !ok {
			return nil, fmt.Errorf("unexpected policy cache type %T", object)
		}
		var item sentinelv1.MemoryPolicy
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(accessor.UnstructuredContent(), &item); err != nil {
			return nil, fmt.Errorf("decode cached policy: %w", err)
		}
		out = append(out, item)
	}
	return out, nil
}
