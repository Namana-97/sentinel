package snapshot

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestStorePublishesBoundsAndCleansSnapshots(t *testing.T) {
	client := fake.NewClientset()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	store := Store{Client: client, Namespace: "test", NodeName: "node.example/very-long-name", MaxEntries: 2, Freshness: time.Minute, Now: func() time.Time { return now }}
	ctx := context.Background()
	stale := PodMemory{Namespace: "demo", PodName: "stale", PodUID: "old", Node: store.NodeName, CollectedAt: now.Add(-2 * time.Minute)}
	first := PodMemory{Namespace: "demo", PodName: "one", PodUID: "one", Node: store.NodeName, UsageBytes: 80, LimitBytes: 100, UsagePercent: 80, Policies: []string{"policy"}, CollectedAt: now}
	second := PodMemory{Namespace: "demo", PodName: "two", PodUID: "two", Node: store.NodeName, CollectedAt: now.Add(time.Second)}
	if err := store.Publish(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("test").Get(ctx, ConfigMapName(store.NodeName), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	items, err := Decode(cm)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].PodUID != "two" || items[1].UsagePercent != 80 {
		t.Fatalf("unexpected snapshots: %#v", items)
	}
	if err := store.Delete(ctx, "demo", "one", "one"); err != nil {
		t.Fatal(err)
	}
	cm, _ = client.CoreV1().ConfigMaps("test").Get(ctx, ConfigMapName(store.NodeName), metav1.GetOptions{})
	items, _ = Decode(cm)
	if len(items) != 1 || items[0].PodUID != "two" {
		t.Fatalf("cleanup failed: %#v", items)
	}
}

func TestFreshOmitsExpired(t *testing.T) {
	now := time.Now()
	items := Fresh([]PodMemory{{PodUID: "stale", CollectedAt: now.Add(-time.Minute)}, {PodUID: "fresh", CollectedAt: now}}, now.Add(-15*time.Second))
	if len(items) != 1 || items[0].PodUID != "fresh" {
		t.Fatalf("Fresh()=%#v", items)
	}
}
