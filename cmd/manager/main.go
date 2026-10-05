// Command manager runs either the Sentinel operator or per-node monitor.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	sentinelv1 "github.com/namanakanchan/sentinel/api/v1"
	"github.com/namanakanchan/sentinel/controllers"
	"github.com/namanakanchan/sentinel/internal/cgroup"
	"github.com/namanakanchan/sentinel/internal/eviction"
	"github.com/namanakanchan/sentinel/internal/kube"
	"github.com/namanakanchan/sentinel/internal/logger"
	"github.com/namanakanchan/sentinel/internal/monitor"
	"github.com/namanakanchan/sentinel/internal/network"
	policyutil "github.com/namanakanchan/sentinel/internal/policy"
	"github.com/namanakanchan/sentinel/internal/snapshot"
	"github.com/namanakanchan/sentinel/web/handlers"
	"github.com/namanakanchan/sentinel/web/middleware"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type options struct {
	networkInterval time.Duration
	networkTimeout  time.Duration
	clusterDomain   string
	mode            string
	metricsAddress  string
	probeAddress    string
	apiAddress      string
	leaderElection  bool
	nodeName        string
	cgroupRoot      string
	scanInterval    time.Duration
	workers         int
	queueSize       int
	jobTimeout      time.Duration
}

func main() {
	opts := parseFlags()
	log := logger.New(slog.LevelInfo)
	ctrl.SetLogger(logr.FromSlogHandler(log.Handler()))
	cfg, err := kube.Config()
	if err != nil {
		log.Error("Kubernetes configuration failed", "error", err)
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	switch opts.mode {
	case "operator":
		err = runOperator(ctx, cfg, opts, log)
	case "network-monitor":
		err = runNetwork(ctx, cfg, opts, log)
	case "node-monitor":
		err = runMonitor(ctx, cfg, opts, log)
	default:
		err = fmt.Errorf("unsupported mode %q", opts.mode)
	}
	if err != nil {
		log.Error("Sentinel stopped with an error", "mode", opts.mode, "error", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.mode, "mode", "operator", "operator, node-monitor or network-monitor")
	flag.StringVar(&opts.metricsAddress, "metrics-bind-address", ":8080", "metrics listen address")
	flag.StringVar(&opts.probeAddress, "health-probe-bind-address", ":8081", "health probe listen address")
	flag.StringVar(&opts.apiAddress, "api-bind-address", ":8090", "REST API listen address")
	flag.BoolVar(&opts.leaderElection, "leader-elect", true, "enable controller leader election")
	flag.StringVar(&opts.nodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name")
	flag.StringVar(&opts.cgroupRoot, "cgroup-root", "/host-sys/fs/cgroup", "mounted host cgroup root")
	flag.DurationVar(&opts.scanInterval, "scan-interval", 2*time.Second, "cgroup evaluation interval")
	flag.IntVar(&opts.workers, "workers", 4, "bounded memory workers")
	flag.IntVar(&opts.queueSize, "queue-size", 256, "bounded worker queue size")
	flag.DurationVar(&opts.jobTimeout, "job-timeout", 2*time.Minute, "per-intervention timeout")
	flag.DurationVar(&opts.networkInterval, "network-interval", 5*time.Second, "Service availability scan interval")
	flag.DurationVar(&opts.networkTimeout, "network-timeout", 3*time.Second, "per-Service network timeout")
	flag.StringVar(&opts.clusterDomain, "cluster-domain", "cluster.local", "cluster DNS domain")
	flag.Parse()
	return opts
}

func scheme() (*runtime.Scheme, error) {
	result := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(result); err != nil {
		return nil, err
	}
	if err := sentinelv1.AddToScheme(result); err != nil {
		return nil, err
	}
	return result, nil
}

func runOperator(ctx context.Context, cfg *rest.Config, opts options, log *slog.Logger) error {
	runtimeScheme, err := scheme()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: runtimeScheme, Metrics: server.Options{BindAddress: opts.metricsAddress}, HealthProbeBindAddress: opts.probeAddress, LeaderElection: opts.leaderElection, LeaderElectionID: "sentinel-operator.sentinel.io"})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	registry := policyutil.NewRegistry()
	if err := (&controllers.MemoryPolicyReconciler{Client: mgr.GetClient(), Registry: registry}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup controller: %w", err)
	}
	api := handlers.API{Client: mgr.GetClient(), SnapshotNamespace: snapshot.DefaultNamespace, SnapshotFreshness: snapshot.DefaultFreshness}
	httpServer := &http.Server{Addr: opts.apiAddress, Handler: middleware.Chain(log, api.Routes()), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	if err := mgr.Add(kube.HTTPServer{Server: httpServer, ShutdownTimeout: 10 * time.Second}); err != nil {
		return fmt.Errorf("add REST server: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	log.Info("starting Sentinel operator", "api_address", opts.apiAddress)
	return mgr.Start(ctx)
}

func runMonitor(ctx context.Context, cfg *rest.Config, opts options, log *slog.Logger) error {
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create dynamic client: %w", err)
	}
	reader := cgroup.NewReader(opts.cgroupRoot)
	snapshotStore := snapshot.Store{Client: typed, Namespace: snapshot.DefaultNamespace, NodeName: opts.nodeName, MaxEntries: snapshot.DefaultMaxEntries, Freshness: snapshot.DefaultFreshness}
	agent, err := monitor.New(monitor.Config{NodeName: opts.nodeName, ScanInterval: opts.scanInterval, Workers: opts.workers, QueueSize: opts.queueSize, JobTimeout: opts.jobTimeout}, typed, dynamicClient, reader, kube.PolicyClient{Dynamic: dynamicClient}, kube.StatusReporter{Dynamic: dynamicClient}, eviction.Evictor{Client: typed}, &snapshotStore, log)
	if err != nil {
		return fmt.Errorf("create node monitor: %w", err)
	}
	log.Info("starting Sentinel node monitor", "node", opts.nodeName, "scan_interval", opts.scanInterval, "workers", opts.workers)
	return agent.Run(ctx)
}

func runNetwork(ctx context.Context, cfg *rest.Config, opts options, log *slog.Logger) error {
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	agent, err := network.New(typed, log, opts.networkInterval, opts.networkTimeout, opts.clusterDomain)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/network/status", agent)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	server := kube.HTTPServer{Server: &http.Server{Addr: opts.apiAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}, ShutdownTimeout: 10 * time.Second}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Start(runCtx) }()
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(runCtx) }()
	select {
	case err = <-done:
		cancel()
		<-agentDone
		return err
	case err = <-agentDone:
		cancel()
		serverErr := <-done
		if err != nil {
			return err
		}
		return serverErr
	}
}
