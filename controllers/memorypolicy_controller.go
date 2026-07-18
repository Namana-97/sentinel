// Package controllers contains Sentinel reconcilers.
package controllers

import (
	"context"
	"fmt"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	policyutil "github.com/namanakanchan/sentinel/internal/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// MemoryPolicyReconciler validates policies, maintains a runtime registry, and publishes status.
type MemoryPolicyReconciler struct {
	client.Client
	Registry *policyutil.Registry
}

// Reconcile converges one MemoryPolicy from the controller-runtime informer cache.
func (r *MemoryPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var item sentinelv1.MemoryPolicy
	if err := r.Get(ctx, req.NamespacedName, &item); err != nil {
		if apierrors.IsNotFound(err) {
			r.Registry.Delete(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get MemoryPolicy: %w", err)
	}
	if item.DeletionTimestamp != nil {
		r.Registry.Delete(item.Name)
		return ctrl.Result{}, nil
	}
	nsSelector, err := metav1.LabelSelectorAsSelector(&item.Spec.NamespaceSelector)
	if err != nil {
		return ctrl.Result{}, r.setCondition(ctx, &item, metav1.ConditionFalse, "InvalidNamespaceSelector", err.Error())
	}
	podSelector, err := metav1.LabelSelectorAsSelector(&item.Spec.LabelSelector)
	if err != nil {
		return ctrl.Result{}, r.setCondition(ctx, &item, metav1.ConditionFalse, "InvalidPodSelector", err.Error())
	}
	active, err := r.countPods(ctx, nsSelector, podSelector)
	if err != nil {
		return ctrl.Result{}, err
	}
	r.Registry.Set(&item)
	base := item.DeepCopy()
	item.Status.ActivePods = int32(active)
	apiMeta.SetStatusCondition(&item.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "PolicyActive", Message: "Policy is active in node monitors", ObservedGeneration: item.Generation})
	if err := r.Status().Patch(ctx, &item, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch MemoryPolicy status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *MemoryPolicyReconciler) countPods(ctx context.Context, nsSelector, podSelector labels.Selector) (int, error) {
	var namespaces corev1.NamespaceList
	if err := r.List(ctx, &namespaces); err != nil {
		return 0, fmt.Errorf("list namespaces: %w", err)
	}
	total := 0
	for i := range namespaces.Items {
		ns := &namespaces.Items[i]
		if !nsSelector.Matches(labels.Set(ns.Labels)) {
			continue
		}
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(ns.Name), client.MatchingLabelsSelector{Selector: podSelector}); err != nil {
			return 0, fmt.Errorf("list pods in %s: %w", ns.Name, err)
		}
		for j := range pods.Items {
			if pods.Items[j].Status.Phase == corev1.PodRunning {
				total++
			}
		}
	}
	return total, nil
}
func (r *MemoryPolicyReconciler) setCondition(ctx context.Context, item *sentinelv1.MemoryPolicy, status metav1.ConditionStatus, reason, message string) error {
	base := item.DeepCopy()
	apiMeta.SetStatusCondition(&item.Status.Conditions, metav1.Condition{Type: "Ready", Status: status, Reason: reason, Message: message, ObservedGeneration: item.Generation})
	if err := r.Status().Patch(ctx, item, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch invalid policy status: %w", err)
	}
	return nil
}

// SetupWithManager installs the MemoryPolicy watch in controller-runtime.
func (r *MemoryPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sentinelv1.MemoryPolicy{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePolicies)).
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePolicies)).
		Complete(r)
}

func (r *MemoryPolicyReconciler) enqueuePolicies(ctx context.Context, _ client.Object) []reconcile.Request {
	var list sentinelv1.MemoryPolicyList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, len(list.Items))
	for i := range list.Items {
		requests[i] = reconcile.Request{NamespacedName: types.NamespacedName{Name: list.Items[i].Name}}
	}
	return requests
}
