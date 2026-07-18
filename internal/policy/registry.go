package policy

import (
	"sync"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
)

// Registry is a concurrency-safe snapshot used by the reconciler.
type Registry struct {
	mu    sync.RWMutex
	items map[string]sentinelv1.MemoryPolicy
}

// NewRegistry returns an empty policy registry.
func NewRegistry() *Registry { return &Registry{items: make(map[string]sentinelv1.MemoryPolicy)} }

// Set inserts or replaces a defensive copy.
func (r *Registry) Set(item *sentinelv1.MemoryPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[item.Name] = *item.DeepCopy()
}

// Delete removes a policy by name.
func (r *Registry) Delete(name string) { r.mu.Lock(); defer r.mu.Unlock(); delete(r.items, name) }

// List returns defensive copies.
func (r *Registry) List() []sentinelv1.MemoryPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]sentinelv1.MemoryPolicy, 0, len(r.items))
	for _, item := range r.items {
		out = append(out, *item.DeepCopy())
	}
	return out
}
