package kube

import (
	"context"
	"fmt"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

// StatusReporter atomically records breach status with optimistic retries.
type StatusReporter struct{ Dynamic dynamic.Interface }

// RecordBreach increments breachCount and sets lastBreachTime through the status subresource.
func (r StatusReporter) RecordBreach(ctx context.Context, name string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		raw, err := r.Dynamic.Resource(MemoryPolicyGVR()).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		var item sentinelv1.MemoryPolicy
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, &item); err != nil {
			return err
		}
		now := metav1.Now()
		item.Status.BreachCount++
		item.Status.LastBreachTime = &now
		object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&item)
		if err != nil {
			return err
		}
		raw.Object = object
		if _, err := r.Dynamic.Resource(MemoryPolicyGVR()).UpdateStatus(ctx, raw, metav1.UpdateOptions{}); err != nil {
			return err
		}
		return nil
	})
}

// WrapStatusError adds the status operation to a reporter error.
func WrapStatusError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("record policy breach: %w", err)
}
