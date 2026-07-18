// Package monitor implements the per-node informer-driven memory agent.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	"github.com/namanakanchan/sentinel/internal/cgroup"
	"github.com/namanakanchan/sentinel/internal/kube"
	policyutil "github.com/namanakanchan/sentinel/internal/policy"
	"github.com/namanakanchan/sentinel/internal/snapshot"
	"github.com/namanakanchan/sentinel/internal/workerpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Cgroups describes the host operations needed by the monitor.
type Cgroups interface {
	FindPod(context.Context, string) (string, error)
	Read(context.Context, string) (policyutil.Reading, error)
	SignalTERM(context.Context, string) error
}

// PolicyGetter fetches an uncached policy immediately before intervention.
type PolicyGetter interface {
	Get(context.Context, string) (*sentinelv1.MemoryPolicy, error)
}

// BreachReporter records a policy breach.
type BreachReporter interface {
	RecordBreach(context.Context, string) error
}

// PodEvictor invokes the Kubernetes Eviction API.
type PodEvictor interface {
	Evict(context.Context, string, string, int64) error
}

// SnapshotReporter publishes the node's latest bounded pod observations.
type SnapshotReporter interface {
	Publish(context.Context, snapshot.PodMemory) error
	Delete(context.Context, string, string, string) error
}

// Config controls the node monitor's bounded concurrency and scan cadence.
type Config struct {
	NodeName     string
	ScanInterval time.Duration
	Workers      int
	QueueSize    int
	JobTimeout   time.Duration
}

// Monitor owns shared informers, a rate-limited key queue, and a bounded worker pool.
type Monitor struct {
	cfg            Config
	logger         *slog.Logger
	cgroups        Cgroups
	policies       PolicyGetter
	reporter       BreachReporter
	evictor        PodEvictor
	snapshots      SnapshotReporter
	pods           coreinformers.PodInformer
	namespaces     coreinformers.NamespaceInformer
	policyInformer cache.SharedIndexInformer
	queue          workqueue.TypedRateLimitingInterface[string]
	pool           *workerpool.Pool
	wg             sync.WaitGroup
	deletedMu      sync.Mutex
	deleted        map[string]string
	interventionMu sync.RWMutex
	interventions  map[string]interventionState
}

type interventionState struct {
	ID              string
	Policy          string
	Path            string
	PendingEviction bool
	StartedAt       time.Time
}

// New constructs a node monitor using filtered pod and shared namespace/policy informers.
func New(cfg Config, clients kubernetes.Interface, dynamicClient dynamic.Interface, cgroups Cgroups, policies PolicyGetter, reporter BreachReporter, evictor PodEvictor, snapshots SnapshotReporter, logger *slog.Logger) (*Monitor, error) {
	if cfg.NodeName == "" || cfg.ScanInterval <= 0 {
		return nil, fmt.Errorf("node name and positive scan interval are required")
	}
	pool, err := workerpool.New(workerpool.Config{Workers: cfg.Workers, QueueSize: cfg.QueueSize, Retries: 0, Timeout: cfg.JobTimeout, RetryDelay: 250 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	podFactory := informers.NewSharedInformerFactoryWithOptions(clients, 0, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", cfg.NodeName).String()
	}))
	nsFactory := informers.NewSharedInformerFactory(clients, 0)
	dynamicFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClient, 0, metav1.NamespaceAll, nil)
	m := &Monitor{cfg: cfg, logger: logger, cgroups: cgroups, policies: policies, reporter: reporter, evictor: evictor, snapshots: snapshots, pods: podFactory.Core().V1().Pods(), namespaces: nsFactory.Core().V1().Namespaces(), policyInformer: dynamicFactory.ForResource(kube.MemoryPolicyGVR()).Informer(), queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()), pool: pool, deleted: make(map[string]string), interventions: make(map[string]interventionState)}
	_, err = m.pods.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: m.enqueuePod, UpdateFunc: func(_, current interface{}) { m.enqueuePod(current) }, DeleteFunc: m.enqueueDeletedPod})
	if err != nil {
		return nil, fmt.Errorf("install pod event handler: %w", err)
	}
	_, err = m.policyInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(interface{}) { m.enqueueAll() }, UpdateFunc: func(_, _ interface{}) { m.enqueueAll() }, DeleteFunc: func(interface{}) { m.enqueueAll() }})
	if err != nil {
		return nil, fmt.Errorf("install policy event handler: %w", err)
	}
	return m, nil
}

// Run starts informers and blocks until cancellation, then joins every goroutine.
func (m *Monitor) Run(ctx context.Context) error {
	m.pool.Start(ctx)
	go m.pods.Informer().Run(ctx.Done())
	go m.namespaces.Informer().Run(ctx.Done())
	go m.policyInformer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), m.pods.Informer().HasSynced, m.namespaces.Informer().HasSynced, m.policyInformer.HasSynced) {
		m.pool.Stop()
		return fmt.Errorf("node monitor informer cache did not sync")
	}
	m.wg.Add(3)
	go m.dispatch(ctx)
	go m.consumeResults(ctx)
	go m.resync(ctx)
	<-ctx.Done()
	m.queue.ShutDown()
	m.pool.Stop()
	m.wg.Wait()
	return nil
}

func (m *Monitor) enqueuePod(object interface{}) {
	pod, ok := podFromObject(object)
	if !ok {
		return
	}
	if pod.Status.Phase == corev1.PodRunning {
		m.queue.Add(pod.Namespace + "/" + pod.Name)
	}
}

func (m *Monitor) enqueueDeletedPod(object interface{}) {
	pod, ok := podFromObject(object)
	if !ok {
		return
	}
	key := pod.Namespace + "/" + pod.Name
	m.deletedMu.Lock()
	m.deleted[key] = string(pod.UID)
	m.deletedMu.Unlock()
	m.queue.Add(key)
}

func podFromObject(object interface{}) (*corev1.Pod, bool) {
	pod, ok := object.(*corev1.Pod)
	if ok {
		return pod, true
	}
	tombstone, ok := object.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	pod, ok = tombstone.Obj.(*corev1.Pod)
	return pod, ok
}
func (m *Monitor) enqueueAll() {
	for _, object := range m.pods.Informer().GetStore().List() {
		m.enqueuePod(object)
	}
}
func (m *Monitor) resync(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.enqueueAll()
		}
	}
}
func (m *Monitor) dispatch(ctx context.Context) {
	defer m.wg.Done()
	for {
		key, shutdown := m.queue.Get()
		if shutdown {
			return
		}
		err := m.pool.SubmitNamed(ctx, key, func(jobCtx context.Context) error { return m.process(jobCtx, key) })
		if err != nil {
			m.queue.Done(key)
			m.queue.Forget(key)
			if !errors.Is(err, context.Canceled) && !errors.Is(err, workerpool.ErrClosed) {
				m.logger.Error("submit monitor work", "pod_key", key, "error", err)
			}
		}
	}
}

func (m *Monitor) consumeResults(ctx context.Context) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-m.pool.Results():
			if !ok {
				return
			}
			m.queue.Done(result.ID)
			if result.Status == workerpool.ResultSucceeded {
				m.queue.Forget(result.ID)
				continue
			}
			m.logger.Error("pod memory evaluation failed", "pod_key", result.ID, "status", result.Status, "attempts", result.Attempts, "error", result.Err)
			if result.Status != workerpool.ResultCancelled {
				m.queue.AddRateLimited(result.ID)
			}
		}
	}
}

func (m *Monitor) process(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := m.pods.Lister().Pods(namespace).Get(name)
	if err != nil {
		m.deletedMu.Lock()
		uid := m.deleted[key]
		delete(m.deleted, key)
		m.deletedMu.Unlock()
		m.clearIntervention(uid)
		if m.snapshots != nil {
			if cleanupErr := m.snapshots.Delete(ctx, namespace, name, uid); cleanupErr != nil {
				return fmt.Errorf("delete pod snapshot: %w", cleanupErr)
			}
		}
		return nil
	}
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return nil
	}
	ns, err := m.namespaces.Lister().Get(namespace)
	if err != nil {
		return fmt.Errorf("get namespace: %w", err)
	}
	items, err := kube.ListPolicies(m.policyInformer.GetStore().List())
	if err != nil {
		return err
	}
	matches, err := policyutil.Select(items, ns.Labels, pod.Labels)
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		m.clearIntervention(string(pod.UID))
		if m.snapshots != nil {
			if err := m.snapshots.Delete(ctx, pod.Namespace, pod.Name, string(pod.UID)); err != nil {
				return fmt.Errorf("delete unmatched pod snapshot: %w", err)
			}
		}
		return nil
	}
	path, err := m.cgroups.FindPod(ctx, string(pod.UID))
	if err != nil {
		return err
	}
	reading, err := m.cgroups.Read(ctx, path)
	if errors.Is(err, cgroup.ErrUnlimited) {
		return nil
	}
	if err != nil {
		return err
	}
	policyNames := make([]string, len(matches))
	for i := range matches {
		policyNames[i] = matches[i].Name
	}
	percent, err := reading.Percent()
	if err != nil {
		return err
	}
	if m.snapshots != nil {
		value := snapshot.PodMemory{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), Node: m.cfg.NodeName, UsageBytes: reading.Usage, LimitBytes: reading.Limit, UsagePercent: percent, Policies: policyNames, CollectedAt: time.Now().UTC()}
		if err := m.snapshots.Publish(ctx, value); err != nil {
			return fmt.Errorf("publish pod snapshot: %w", err)
		}
	}
	for i := range matches {
		breached, err := policyutil.Breached(reading, matches[i].Spec.ThresholdPercent)
		if err != nil {
			return err
		}
		if breached {
			return m.intervene(ctx, pod, path, reading, matches[i])
		}
	}
	return nil
}

func (m *Monitor) intervene(ctx context.Context, pod *corev1.Pod, path string, reading policyutil.Reading, cached sentinelv1.MemoryPolicy) error {
	current, err := m.policies.Get(ctx, cached.Name)
	if err != nil {
		return err
	}
	breached, err := policyutil.Breached(reading, current.Spec.ThresholdPercent)
	if err != nil || !breached {
		m.clearIntervention(string(pod.UID))
		return err
	}
	percent, _ := reading.Percent()
	state, created := m.beginIntervention(string(pod.UID), current.Name, path)
	fields := []any{"intervention_id", state.ID, "namespace", pod.Namespace, "pod", pod.Name, "pod_uid", pod.UID, "policy", current.Name, "memory_usage_bytes", reading.Usage, "memory_limit_bytes", reading.Limit, "memory_percent", percent, "threshold_percent", current.Spec.ThresholdPercent}
	if !created {
		if !state.PendingEviction {
			return nil
		}
		m.logger.Info("retrying pending eviction", append(fields, "stage", "eviction_retry")...)
		return m.attemptEviction(ctx, pod, current, reading, fields)
	}
	if err := m.reporter.RecordBreach(ctx, current.Name); err != nil {
		m.clearIntervention(string(pod.UID))
		return kube.WrapStatusError(err)
	}
	m.logger.Warn("memory threshold breached", append(fields, "stage", "breach_detected")...)
	if err := m.cgroups.SignalTERM(ctx, path); err != nil {
		m.clearIntervention(string(pod.UID))
		return fmt.Errorf("send SIGTERM: %w", err)
	}
	m.logger.Info("SIGTERM sent to pod cgroup", append(fields, "stage", "sigterm_sent")...)
	timer := time.NewTimer(time.Duration(current.Spec.GracePeriodSeconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		m.clearIntervention(string(pod.UID))
		return ctx.Err()
	case <-timer.C:
	}
	m.logger.Info("grace period completed", append(fields, "stage", "grace_completed")...)
	rechecked, err := m.cgroups.Read(ctx, path)
	if errors.Is(err, cgroup.ErrUnlimited) {
		m.clearIntervention(string(pod.UID))
		return nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		m.clearIntervention(string(pod.UID))
		return nil
	}
	if err != nil {
		m.clearIntervention(string(pod.UID))
		return fmt.Errorf("recheck memory: %w", err)
	}
	recheckedPercent, _ := rechecked.Percent()
	m.logger.Info("memory rechecked", append(fields, "stage", "memory_rechecked", "rechecked_usage_bytes", rechecked.Usage, "rechecked_percent", recheckedPercent)...)
	stillBreached, err := policyutil.Breached(rechecked, current.Spec.ThresholdPercent)
	if err != nil {
		return err
	}
	if !stillBreached {
		m.clearIntervention(string(pod.UID))
		m.logger.Info("pod memory recovered after SIGTERM", append(fields, "stage", "recovered")...)
		return nil
	}
	return m.attemptEviction(ctx, pod, current, rechecked, fields)
}

func (m *Monitor) attemptEviction(ctx context.Context, pod *corev1.Pod, current *sentinelv1.MemoryPolicy, reading policyutil.Reading, fields []any) error {
	breached, err := policyutil.Breached(reading, current.Spec.ThresholdPercent)
	if err != nil || !breached {
		m.clearIntervention(string(pod.UID))
		return err
	}
	m.logger.Info("pod eviction attempted", append(fields, "stage", "eviction_attempted")...)
	err = m.evictor.Evict(ctx, pod.Namespace, pod.Name, int64(current.Spec.GracePeriodSeconds))
	if err != nil {
		m.markEvictionPending(string(pod.UID))
		m.logger.Error("pod eviction rejected", append(fields, "stage", "eviction_rejected", "eviction_result", "rejected", "error", err)...)
		return err
	}
	m.clearIntervention(string(pod.UID))
	m.logger.Info("pod eviction accepted", append(fields, "stage", "eviction_accepted", "eviction_result", "accepted")...)
	return nil
}

func (m *Monitor) beginIntervention(uid, policy, path string) (interventionState, bool) {
	m.interventionMu.Lock()
	defer m.interventionMu.Unlock()
	if m.interventions == nil {
		m.interventions = make(map[string]interventionState)
	}
	if state, ok := m.interventions[uid]; ok {
		if state.Policy == policy {
			return state, false
		}
		delete(m.interventions, uid)
	}
	if len(m.interventions) >= 1024 {
		var oldestUID string
		var oldest time.Time
		for candidate, state := range m.interventions {
			if oldestUID == "" || state.StartedAt.Before(oldest) {
				oldestUID, oldest = candidate, state.StartedAt
			}
		}
		delete(m.interventions, oldestUID)
	}
	state := interventionState{ID: uuid.NewString(), Policy: policy, Path: path, StartedAt: time.Now()}
	m.interventions[uid] = state
	return state, true
}

func (m *Monitor) markEvictionPending(uid string) {
	m.interventionMu.Lock()
	state, ok := m.interventions[uid]
	if ok {
		state.PendingEviction = true
		m.interventions[uid] = state
	}
	m.interventionMu.Unlock()
}

func (m *Monitor) clearIntervention(uid string) {
	if uid == "" {
		return
	}
	m.interventionMu.Lock()
	delete(m.interventions, uid)
	m.interventionMu.Unlock()
}
