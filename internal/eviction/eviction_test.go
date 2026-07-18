package eviction

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEvictUsesSubresource(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo"}})
	if err := (Evictor{Client: client}).Evict(context.Background(), "demo", "pod", 5); err != nil {
		t.Fatal(err)
	}
	actions := client.Actions()
	if len(actions) != 1 {
		t.Fatalf("actions=%d", len(actions))
	}
	action := actions[0].(clienttesting.CreateAction)
	if action.GetSubresource() != "eviction" {
		t.Fatalf("subresource=%q", action.GetSubresource())
	}
}
