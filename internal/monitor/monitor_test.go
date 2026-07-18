package monitor

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"testing"
	"time"

	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	policyutil "github.com/namanakanchan/sentinel/internal/policy"
	"github.com/namanakanchan/sentinel/internal/snapshot"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

type fakeCgroups struct {
	readings []policyutil.Reading
	signaled int
	readErr  error
}

func (f *fakeCgroups) FindPod(context.Context, string) (string, error) { return "pod", nil }
func (f *fakeCgroups) Read(context.Context, string) (policyutil.Reading, error) {
	if f.readErr != nil {
		return policyutil.Reading{}, f.readErr
	}
	r := f.readings[0]
	if len(f.readings) > 1 {
		f.readings = f.readings[1:]
	}
	return r, nil
}
func (f *fakeCgroups) SignalTERM(context.Context, string) error { f.signaled++; return nil }

type fakePolicies struct{ item *sentinelv1.MemoryPolicy }

func (f fakePolicies) Get(context.Context, string) (*sentinelv1.MemoryPolicy, error) {
	return f.item.DeepCopy(), nil
}

type fakeReporter struct{ count int }

func (f *fakeReporter) RecordBreach(context.Context, string) error { f.count++; return nil }

type fakeEvictor struct{ count int }

func (f *fakeEvictor) Evict(context.Context, string, string, int64) error { f.count++; return nil }

type rejectingEvictor struct {
	count int
	err   error
}

type fakeSnapshots struct {
	published []snapshot.PodMemory
	deleted   int
}

func (f *fakeSnapshots) Publish(_ context.Context, value snapshot.PodMemory) error {
	f.published = append(f.published, value)
	return nil
}
func (f *fakeSnapshots) Delete(context.Context, string, string, string) error {
	f.deleted++
	return nil
}

func (f *rejectingEvictor) Evict(context.Context, string, string, int64) error {
	f.count++
	if f.count == 1 {
		return f.err
	}
	return nil
}

func TestInterveneRecoversWithoutEviction(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80, GracePeriodSeconds: 0}}
	cg := &fakeCgroups{readings: []policyutil.Reading{{Usage: 20, Limit: 100}}}
	reporter := &fakeReporter{}
	evictor := &fakeEvictor{}
	m := Monitor{cgroups: cg, policies: fakePolicies{policy}, reporter: reporter, evictor: evictor, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	if err := m.intervene(context.Background(), pod, "pod", policyutil.Reading{Usage: 90, Limit: 100}, *policy); err != nil {
		t.Fatal(err)
	}
	if cg.signaled != 1 || reporter.count != 1 || evictor.count != 0 {
		t.Fatalf("signal=%d report=%d evict=%d", cg.signaled, reporter.count, evictor.count)
	}
}

func TestInterveneEvictsAfterRecheck(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	cg := &fakeCgroups{readings: []policyutil.Reading{{Usage: 90, Limit: 100}}}
	reporter := &fakeReporter{}
	evictor := &fakeEvictor{}
	m := Monitor{cgroups: cg, policies: fakePolicies{policy}, reporter: reporter, evictor: evictor, interventions: make(map[string]interventionState), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	if err := m.intervene(context.Background(), pod, "pod", policyutil.Reading{Usage: 90, Limit: 100}, *policy); err != nil {
		t.Fatal(err)
	}
	if cg.signaled != 1 || reporter.count != 1 || evictor.count != 1 {
		t.Fatalf("signal=%d report=%d evict=%d", cg.signaled, reporter.count, evictor.count)
	}
}

func TestEvictionRejectionRetriesWithoutDuplicateSignal(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	cg := &fakeCgroups{readings: []policyutil.Reading{{Usage: 90, Limit: 100}}}
	reporter := &fakeReporter{}
	evictor := &rejectingEvictor{err: errors.New("pdb rejected eviction")}
	m := Monitor{cgroups: cg, policies: fakePolicies{policy}, reporter: reporter, evictor: evictor, interventions: make(map[string]interventionState), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	reading := policyutil.Reading{Usage: 90, Limit: 100}
	if err := m.intervene(context.Background(), pod, "pod", reading, *policy); err == nil {
		t.Fatal("expected first eviction rejection")
	}
	if err := m.intervene(context.Background(), pod, "pod", reading, *policy); err != nil {
		t.Fatal(err)
	}
	if cg.signaled != 1 || reporter.count != 1 || evictor.count != 2 {
		t.Fatalf("signal=%d report=%d evict=%d", cg.signaled, reporter.count, evictor.count)
	}
}

func TestPendingEvictionStopsWhenCurrentPolicyNoLongerBreached(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	cg := &fakeCgroups{readings: []policyutil.Reading{{Usage: 90, Limit: 100}}}
	reporter := &fakeReporter{}
	evictor := &rejectingEvictor{err: errors.New("pdb rejected eviction")}
	getter := fakePolicies{policy}
	m := Monitor{cgroups: cg, policies: getter, reporter: reporter, evictor: evictor, interventions: make(map[string]interventionState), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	reading := policyutil.Reading{Usage: 90, Limit: 100}
	if err := m.intervene(context.Background(), pod, "pod", reading, *policy); err == nil {
		t.Fatal("expected first eviction rejection")
	}
	policy.Spec.ThresholdPercent = 95
	if err := m.intervene(context.Background(), pod, "pod", reading, *policy); err != nil {
		t.Fatal(err)
	}
	if evictor.count != 1 || reporter.count != 1 || cg.signaled != 1 {
		t.Fatalf("eviction=%d report=%d signal=%d", evictor.count, reporter.count, cg.signaled)
	}
}

func TestGracePeriodCancellationClearsState(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80, GracePeriodSeconds: 10}}
	cg := &fakeCgroups{}
	m := Monitor{cgroups: cg, policies: fakePolicies{policy}, reporter: &fakeReporter{}, evictor: &fakeEvictor{}, interventions: make(map[string]interventionState), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := m.intervene(ctx, pod, "pod", policyutil.Reading{Usage: 90, Limit: 100}, *policy)
	if !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatalf("error=%v duration=%s", err, time.Since(started))
	}
	m.interventionMu.RLock()
	remaining := len(m.interventions)
	m.interventionMu.RUnlock()
	if remaining != 0 {
		t.Fatalf("intervention state was not cleared: %d", remaining)
	}
}

func TestDeletedCgroupAfterSignalEndsIntervention(t *testing.T) {
	policy := &sentinelv1.MemoryPolicy{ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	cg := &fakeCgroups{readErr: fs.ErrNotExist}
	reporter := &fakeReporter{}
	evictor := &fakeEvictor{}
	m := Monitor{cgroups: cg, policies: fakePolicies{policy}, reporter: reporter, evictor: evictor, interventions: make(map[string]interventionState), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid"}}
	if err := m.intervene(context.Background(), pod, "missing", policyutil.Reading{Usage: 90, Limit: 100}, *policy); err != nil {
		t.Fatal(err)
	}
	if evictor.count != 0 || reporter.count != 1 || cg.signaled != 1 {
		t.Fatalf("unexpected counts: eviction=%d report=%d signal=%d", evictor.count, reporter.count, cg.signaled)
	}
}

func TestProcessPublishesSnapshotAndRunsFullRecoveryPath(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = sentinelv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	policy := &sentinelv1.MemoryPolicy{TypeMeta: metav1.TypeMeta{APIVersion: "sentinel.io/v1", Kind: "MemoryPolicy"}, ObjectMeta: metav1.ObjectMeta{Name: "demo"}, Spec: sentinelv1.MemoryPolicySpec{ThresholdPercent: 80}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{{Group: "sentinel.io", Version: "v1", Resource: "memorypolicies"}: "MemoryPolicyList"}, policy)
	cg := &fakeCgroups{readings: []policyutil.Reading{{Usage: 90, Limit: 100}, {Usage: 20, Limit: 100}}}
	reporter := &fakeReporter{}
	evictor := &fakeEvictor{}
	snapshots := &fakeSnapshots{}
	m, err := New(Config{NodeName: "node-a", ScanInterval: time.Second, Workers: 1, QueueSize: 2, JobTimeout: time.Second}, kubernetesfake.NewSimpleClientset(), dynamicClient, cg, fakePolicies{policy}, reporter, evictor, snapshots, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "demo", UID: "uid", Labels: map[string]string{"app": "demo"}}, Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if err := m.namespaces.Informer().GetStore().Add(ns); err != nil {
		t.Fatal(err)
	}
	if err := m.pods.Informer().GetStore().Add(pod); err != nil {
		t.Fatal(err)
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.policyInformer.GetStore().Add(&unstructured.Unstructured{Object: raw}); err != nil {
		t.Fatal(err)
	}
	if err := m.process(context.Background(), "demo/pod"); err != nil {
		t.Fatal(err)
	}
	if len(snapshots.published) != 1 || snapshots.published[0].UsageBytes != 90 || snapshots.published[0].Policies[0] != "demo" {
		t.Fatalf("snapshot=%#v", snapshots.published)
	}
	if reporter.count != 1 || cg.signaled != 1 || evictor.count != 0 {
		t.Fatalf("report=%d signal=%d eviction=%d", reporter.count, cg.signaled, evictor.count)
	}
}
