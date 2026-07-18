// Package eviction implements the Kubernetes-native eviction step.
package eviction

import (
	"context"
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Evictor submits policy/v1 Eviction objects; API server admission enforces PDBs.
type Evictor struct{ Client kubernetes.Interface }

// Evict asks the API server to evict a pod and never deletes it directly.
func (e Evictor) Evict(ctx context.Context, namespace, name string, graceSeconds int64) error {
	deleteOptions := &metav1.DeleteOptions{GracePeriodSeconds: &graceSeconds}
	request := &policyv1.Eviction{TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, DeleteOptions: deleteOptions}
	if err := e.Client.PolicyV1().Evictions(namespace).Evict(ctx, request); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("evict pod %s/%s: %w", namespace, name, err)
	}
	return nil
}
