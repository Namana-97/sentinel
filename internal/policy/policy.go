// Package policy selects MemoryPolicies and evaluates exact cgroup readings.
package policy

import (
	"fmt"
	"sort"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Reading is an exact cgroup memory snapshot in bytes.
type Reading struct {
	Usage uint64
	Limit uint64
}

// Percent returns usage as an integer percentage without floating-point overflow.
func (r Reading) Percent() (uint64, error) {
	if r.Limit == 0 {
		return 0, fmt.Errorf("memory limit is zero or unlimited")
	}
	return r.Usage/r.Limit*100 + (r.Usage%r.Limit)*100/r.Limit, nil
}

// Breached reports whether an exact reading meets a threshold.
func Breached(r Reading, threshold int32) (bool, error) {
	if threshold < 1 || threshold > 100 {
		return false, fmt.Errorf("threshold must be in [1,100]")
	}
	if r.Limit == 0 {
		return false, fmt.Errorf("cannot evaluate an unlimited cgroup")
	}
	t := uint64(threshold)
	target := (r.Limit/100)*t + ((r.Limit%100)*t+99)/100
	return r.Usage >= target, nil
}

// Select returns all matching policies ordered deterministically by name.
func Select(policies []sentinelv1.MemoryPolicy, namespaceLabels, podLabels map[string]string) ([]sentinelv1.MemoryPolicy, error) {
	matched := make([]sentinelv1.MemoryPolicy, 0)
	for _, candidate := range policies {
		ns, err := metav1.LabelSelectorAsSelector(&candidate.Spec.NamespaceSelector)
		if err != nil {
			return nil, fmt.Errorf("policy %s namespace selector: %w", candidate.Name, err)
		}
		pods, err := metav1.LabelSelectorAsSelector(&candidate.Spec.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("policy %s pod selector: %w", candidate.Name, err)
		}
		if ns.Matches(labels.Set(namespaceLabels)) && pods.Matches(labels.Set(podLabels)) {
			matched = append(matched, candidate)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })
	return matched, nil
}
