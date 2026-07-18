package policy

import (
	"sync"
	"testing"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBreached(t *testing.T) {
	tests := []struct {
		name      string
		reading   Reading
		threshold int32
		want      bool
		wantErr   bool
	}{{"below", Reading{79, 100}, 80, false, false}, {"equal", Reading{80, 100}, 80, true, false}, {"above", Reading{99, 100}, 80, true, false}, {"unlimited", Reading{10, 0}, 80, false, true}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Breached(test.reading, test.threshold)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("Breached() = %v, %v", got, err)
			}
		})
	}
}
func TestSelect(t *testing.T) {
	items := []sentinelv1.MemoryPolicy{{ObjectMeta: metav1.ObjectMeta{Name: "z"}, Spec: sentinelv1.MemoryPolicySpec{NamespaceSelector: metav1.LabelSelector{MatchLabels: map[string]string{"team": "platform"}}, LabelSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a"}, Spec: sentinelv1.MemoryPolicySpec{LabelSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}}}}
	got, err := Select(items, map[string]string{"team": "platform"}, map[string]string{"app": "api"})
	if err != nil || len(got) != 2 || got[0].Name != "a" {
		t.Fatalf("unexpected selection: %#v, %v", got, err)
	}
}
func TestRegistryConcurrent(t *testing.T) {
	registry := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			registry.Set(&sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "shared"}})
		}()
		go func() { defer wg.Done(); _ = registry.List() }()
	}
	wg.Wait()
	if len(registry.List()) != 1 {
		t.Fatal("registry lost item")
	}
}
