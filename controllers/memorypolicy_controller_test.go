package controllers

import (
	"context"
	"errors"
	"testing"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	policyutil "github.com/namanakanchan/sentinel/internal/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileRegistersAndUpdatesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sentinelv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	item := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80, GracePeriodSeconds: 5}}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(item).WithObjects(item, ns, pod).Build()
	registry := policyutil.NewRegistry()
	reconciler := MemoryPolicyReconciler{Client: client, Registry: registry}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo"}}); err != nil {
		t.Fatal(err)
	}
	if len(registry.List()) != 1 {
		t.Fatal("policy not registered")
	}
	var got sentinelv1.MemoryPolicy
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ActivePods != 1 || len(got.Status.Conditions) != 1 {
		t.Fatalf("status=%#v", got.Status)
	}
}

func TestReconcileDeletionCleansRegistry(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sentinelv1.AddToScheme(scheme)
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	registry := policyutil.NewRegistry()
	registry.Set(&sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "gone"}})
	reconciler := MemoryPolicyReconciler{Client: base, Registry: registry}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone"}}); err != nil {
		t.Fatal(err)
	}
	if len(registry.List()) != 0 {
		t.Fatal("deleted policy remained in registry")
	}
}

func TestReconcileInvalidSelectors(t *testing.T) {
	for _, field := range []string{"namespace", "pod"} {
		t.Run(field, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = sentinelv1.AddToScheme(scheme)
			selector := metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: "Invalid"}}}
			item := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "invalid"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
			if field == "namespace" {
				item.Spec.NamespaceSelector = selector
			} else {
				item.Spec.LabelSelector = selector
			}
			base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(item).WithObjects(item).Build()
			registry := policyutil.NewRegistry()
			reconciler := MemoryPolicyReconciler{Client: base, Registry: registry}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: item.Name}}); err != nil {
				t.Fatal(err)
			}
			var got sentinelv1.MemoryPolicy
			if err := base.Get(context.Background(), types.NamespacedName{Name: item.Name}, &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Status != metav1.ConditionFalse {
				t.Fatalf("conditions=%#v", got.Status.Conditions)
			}
			if len(registry.List()) != 0 {
				t.Fatal("invalid policy was registered")
			}
		})
	}
}

type conflictClient struct{ client.Client }

func (c conflictClient) Status() client.SubResourceWriter {
	return conflictStatus{SubResourceWriter: c.Client.Status()}
}

type conflictStatus struct{ client.SubResourceWriter }

func (c conflictStatus) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return apierrors.NewConflict(schema.GroupResource{Group: "sentinel.io", Resource: "memorypolicies"}, "demo", errors.New("simulated conflict"))
}

func TestReconcileReturnsStatusConflictForControllerRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sentinelv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	item := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(item).WithObjects(item).Build()
	reconciler := MemoryPolicyReconciler{Client: conflictClient{Client: base}, Registry: policyutil.NewRegistry()}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "demo"}})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}
