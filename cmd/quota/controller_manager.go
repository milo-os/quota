package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	controllers "go.miloapis.com/quota/internal/quota/controllers"
	"go.miloapis.com/quota/internal/quota/controllers/core"
	"go.miloapis.com/quota/internal/quota/controllers/lifecycle"
	"go.miloapis.com/quota/internal/quota/informer"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// ProviderFactory builds the multicluster provider the controller manager
// uses to discover additional control planes (for example one per project).
//
// The quota service ships no provider of its own: discovering control planes
// is consumer-specific and would require importing the consumer's types. A
// consumer that needs multi-cluster reconciliation supplies a factory via
// ControllerManagerOptions.ProviderFactory (or WithProviderFactory). The
// returned provider must implement multicluster.Provider; if it also
// implements multicluster.ProviderRunnable it is started with the manager.
// Returning (nil, nil) runs in local-only mode.
type ProviderFactory func(ctx context.Context, cfg *rest.Config) (multicluster.Provider, error)

// ControllerManagerOptions contains configuration for the controller manager.
type ControllerManagerOptions struct {
	Kubeconfig  string
	MasterURL   string
	MetricsAddr string
	// MetricsSecure serves metrics over HTTPS and requires every scrape to
	// authenticate (TokenReview) and be authorized (SubjectAccessReview for
	// get on the /metrics non-resource URL). When false, metrics are plain
	// HTTP with no access control, which Validate allows only on a loopback
	// address.
	MetricsSecure bool
	// MetricsCertDir holds tls.crt and tls.key for the metrics endpoint. When
	// empty or missing, a self-signed certificate is generated in memory.
	MetricsCertDir  string
	HealthProbeAddr string
	// StorageBackend is the apiserver's --quota-storage-backend. When set,
	// it is used to default and validate AllocationMode.
	StorageBackend string
	// AllocationMode is --quota-allocation-mode. Empty means "derive from
	// StorageBackend" (controller when the backend is unknown).
	AllocationMode string
	// BucketNamespace is the fallback namespace for QuotaBuckets whose
	// consumer is not namespaced.
	BucketNamespace string
	// StalePendingClaimAge bounds how long auto-created claims may stay
	// pending before cleanup.
	StalePendingClaimAge time.Duration
	// KubeAPIQPS and KubeAPIBurst set the client-side rate limit for requests
	// to the API server. Every claim decision in controller mode costs several
	// writes, so this limit caps how many claims are decided per second.
	KubeAPIQPS   float32
	KubeAPIBurst int
	// AllocationWorkers is the concurrency of the bucket-allocation and
	// claim controllers in controller allocation mode.
	AllocationWorkers int
	// ProviderFactory, when non-nil, supplies the multicluster provider.
	// Not settable by flag; see ProviderFactory.
	ProviderFactory ProviderFactory
	Logs            *logsapi.LoggingConfiguration
}

// Client-side rate limits for the controller-manager's API clients. client-go's
// own defaults (5 QPS, burst 10) cap controller-mode allocation at a few claim
// decisions per second.
const (
	DefaultKubeAPIQPS   float32 = 500
	DefaultKubeAPIBurst int     = 1000
	// DefaultAllocationWorkers is the default --quota-allocation-workers.
	DefaultAllocationWorkers = 8
)

// NewControllerManagerOptions creates options with default values.
func NewControllerManagerOptions() *ControllerManagerOptions {
	return &ControllerManagerOptions{
		Logs:                 logsapi.NewLoggingConfiguration(),
		MetricsAddr:          ":8443",
		MetricsSecure:        true,
		HealthProbeAddr:      ":8081",
		BucketNamespace:      core.DefaultBucketNamespace,
		StalePendingClaimAge: lifecycle.DefaultStalePendingClaimAge,
		KubeAPIQPS:           DefaultKubeAPIQPS,
		KubeAPIBurst:         DefaultKubeAPIBurst,
		AllocationWorkers:    DefaultAllocationWorkers,
	}
}

// AddFlags adds controller manager flags to the command.
func (o *ControllerManagerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Kubeconfig, "kubeconfig", o.Kubeconfig,
		"Path to a kubeconfig file. Only required if out-of-cluster.")
	fs.StringVar(&o.MasterURL, "master", o.MasterURL,
		"The address of the Kubernetes API server. Overrides any value in kubeconfig.")
	fs.StringVar(&o.MetricsAddr, "metrics-addr", o.MetricsAddr,
		"The address to bind the metrics endpoint. Set to 0 to disable it.")
	fs.BoolVar(&o.MetricsSecure, "metrics-secure", o.MetricsSecure,
		"Serve metrics over HTTPS and require scrapers to authenticate and to be authorized for "+
			"get on the /metrics non-resource URL. Set to false only with a loopback --metrics-addr.")
	fs.StringVar(&o.MetricsCertDir, "metrics-cert-dir", o.MetricsCertDir,
		"Directory containing tls.crt and tls.key for the metrics endpoint. "+
			"Without it a self-signed certificate is generated at startup.")
	fs.StringVar(&o.HealthProbeAddr, "health-probe-addr", o.HealthProbeAddr,
		"The address to bind the health probe endpoint.")
	fs.StringVar(&o.StorageBackend, "quota-storage-backend", o.StorageBackend,
		"The storage backend the quota apiserver runs with (etcd, postgres or dual). "+
			"Set it to the same value as the apiserver's --quota-storage-backend so the "+
			"allocation mode is derived from it and an incompatible combination fails at startup.")
	fs.StringVar(&o.AllocationMode, "quota-allocation-mode", o.AllocationMode,
		"Who owns QuotaClaim/QuotaBucket allocation status: 'controller' (required for etcd/dual) "+
			"registers the bucket-allocation pump and claim aggregator; 'allocator' (required for postgres) leaves "+
			"those writes to the synchronous Postgres allocator. Defaults from --quota-storage-backend, "+
			"or 'controller' when that is unset.")
	fs.StringVar(&o.BucketNamespace, "quota-bucket-namespace", o.BucketNamespace,
		"Namespace for QuotaBuckets whose consumerRef has no namespace. Must match the apiserver's --quota-bucket-namespace.")
	fs.DurationVar(&o.StalePendingClaimAge, "quota-stale-pending-claim-age", o.StalePendingClaimAge,
		"Age after which a pending QuotaClaim created by the admission plugin is deleted as abandoned. "+
			"Keep it well above the admission plugin's watch timeout.")
	fs.Float32Var(&o.KubeAPIQPS, "kube-api-qps", o.KubeAPIQPS,
		"Client-side QPS limit for requests to the API server. In controller allocation mode each "+
			"claim decision takes several writes, so this caps claim decisions per second. A negative value disables the limit.")
	fs.IntVar(&o.KubeAPIBurst, "kube-api-burst", o.KubeAPIBurst,
		"Client-side burst allowance for requests to the API server.")
	fs.IntVar(&o.AllocationWorkers, "quota-allocation-workers", o.AllocationWorkers,
		"How many QuotaBuckets and QuotaClaims are reconciled concurrently in controller allocation mode. "+
			"Different buckets are independent, so more workers raise claim decisions per second.")

	logsapi.AddFlags(o.Logs, fs)
}

// allocationModeForBackend returns the only allocation mode that is correct
// for a storage backend. etcd and dual have no synchronous allocator, so the
// controllers must write allocation state; postgres allocates in the request
// path and the controllers must stay out of the way.
func allocationModeForBackend(backend string) (controllers.AllocationMode, error) {
	switch backend {
	case "etcd", "dual":
		return controllers.AllocationModeController, nil
	case "postgres":
		return controllers.AllocationModeAllocator, nil
	default:
		return "", fmt.Errorf("--quota-storage-backend must be one of: etcd, postgres, dual (got %q)", backend)
	}
}

// Complete derives defaults from other options.
func (o *ControllerManagerOptions) Complete() error {
	if o.AllocationMode != "" {
		return nil
	}
	if o.StorageBackend == "" {
		o.AllocationMode = string(controllers.AllocationModeController)
		return nil
	}
	mode, err := allocationModeForBackend(o.StorageBackend)
	if err != nil {
		return err
	}
	o.AllocationMode = string(mode)
	return nil
}

// Validate ensures the supplied options are internally consistent.
func (o *ControllerManagerOptions) Validate() error {
	switch controllers.AllocationMode(o.AllocationMode) {
	case controllers.AllocationModeController, controllers.AllocationModeAllocator:
	default:
		return fmt.Errorf("--quota-allocation-mode must be one of: controller, allocator (got %q)", o.AllocationMode)
	}
	if o.StorageBackend != "" {
		want, err := allocationModeForBackend(o.StorageBackend)
		if err != nil {
			return err
		}
		// A mismatch is not survivable: controller mode on postgres races the
		// synchronous allocator on the same status fields, and allocator mode
		// on etcd/dual means nothing ever writes claim status.
		if controllers.AllocationMode(o.AllocationMode) != want {
			return fmt.Errorf("--quota-allocation-mode=%s is incompatible with --quota-storage-backend=%s; use --quota-allocation-mode=%s (or omit it to derive it from the backend)",
				o.AllocationMode, o.StorageBackend, want)
		}
	}
	if o.BucketNamespace == "" {
		return fmt.Errorf("--quota-bucket-namespace must not be empty")
	}
	if o.StalePendingClaimAge <= 0 {
		return fmt.Errorf("--quota-stale-pending-claim-age must be positive")
	}
	if o.AllocationWorkers < 1 {
		return fmt.Errorf("--quota-allocation-workers must be at least 1 (got %d)", o.AllocationWorkers)
	}
	if !o.MetricsSecure && o.MetricsAddr != "0" && !isLoopbackAddr(o.MetricsAddr) {
		return fmt.Errorf("--metrics-secure=false serves metrics without authentication and is only allowed on a loopback address (got --metrics-addr=%q)", o.MetricsAddr)
	}
	return nil
}

// isLoopbackAddr reports whether a host:port bind address listens only on
// a loopback interface. An empty host (":8080") listens on every interface.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// metricsOptions returns the controller-runtime metrics server options. In
// secure mode every request is authenticated and authorized against the
// API server, the same way kube-controller-manager protects its metrics.
func (o *ControllerManagerOptions) metricsOptions() metricsserver.Options {
	opts := metricsserver.Options{
		BindAddress: o.MetricsAddr,
	}
	if !o.MetricsSecure {
		return opts
	}
	opts.SecureServing = true
	opts.CertDir = o.MetricsCertDir
	opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	// HTTP/2 is turned off to avoid the HTTP/2 Rapid Reset and Stream
	// Cancellation vulnerabilities (CVE-2023-44487, CVE-2023-39325) on an
	// endpoint that gains nothing from it.
	opts.TLSOpts = []func(*tls.Config){func(c *tls.Config) { c.NextProtos = []string{"http/1.1"} }}
	return opts
}

// NewControllerManagerCommand creates the controller-manager subcommand.
func NewControllerManagerCommand(opts ...ControllerManagerOption) *cobra.Command {
	options := NewControllerManagerOptions()
	for _, opt := range opts {
		opt(options)
	}

	cmd := &cobra.Command{
		Use:   "controller-manager",
		Short: "Run the quota controller manager",
		Long: `Run the controller manager that watches quota resources and reconciles
their desired state. This includes managing QuotaDefinitions, QuotaGrants,
QuotaClaims, QuotaBuckets, and creation policy resources.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			if err := logsapi.ValidateAndApply(options.Logs, utilfeature.DefaultMutableFeatureGate); err != nil {
				return fmt.Errorf("failed to apply logging configuration: %w", err)
			}
			ctrl.SetLogger(klog.NewKlogr())
			return RunControllerManager(options)
		},
	}

	options.AddFlags(cmd.Flags())

	return cmd
}

// ControllerManagerOption customises the controller-manager command.
type ControllerManagerOption func(*ControllerManagerOptions)

// WithProviderFactory installs the consumer-supplied multicluster provider.
func WithProviderFactory(f ProviderFactory) ControllerManagerOption {
	return func(o *ControllerManagerOptions) { o.ProviderFactory = f }
}

// RunControllerManager starts the controller manager.
func RunControllerManager(options *ControllerManagerOptions) error {
	klog.Info("Starting Quota Controller Manager")

	if options.StorageBackend == "" {
		klog.InfoS("--quota-storage-backend not set; allocation mode is not cross-checked against the apiserver backend",
			"allocationMode", options.AllocationMode)
	}
	klog.InfoS("Quota controller-manager configuration",
		"storageBackend", options.StorageBackend,
		"allocationMode", options.AllocationMode,
		"bucketNamespace", options.BucketNamespace,
		"stalePendingClaimAge", options.StalePendingClaimAge)

	var config *rest.Config
	var err error

	if options.Kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags(options.MasterURL, options.Kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return fmt.Errorf("failed to build kubeconfig: %w", err)
	}
	config.QPS = options.KubeAPIQPS
	config.Burst = options.KubeAPIBurst

	// Create a dynamic client for resource type validation
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Build the controller-runtime scheme with quota types
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(quotav1alpha1.AddToScheme(scheme))

	// Use controller-runtime's signal handler for graceful shutdown
	ctx := ctrl.SetupSignalHandler()

	// The provider is consumer-supplied (see ProviderFactory). Without one
	// the manager runs in local-only mode and behaves like a standard
	// controller-runtime manager.
	var provider multicluster.Provider
	if options.ProviderFactory != nil {
		provider, err = options.ProviderFactory(ctx, config)
		if err != nil {
			return fmt.Errorf("failed to build multicluster provider: %w", err)
		}
	}
	if provider == nil {
		klog.Info("No multicluster provider configured; reconciling the local cluster only")
	}

	mcMgr, err := mcmanager.New(config, provider, manager.Options{
		Scheme:                 scheme,
		Metrics:                options.metricsOptions(),
		HealthProbeBindAddress: options.HealthProbeAddr,
	})
	if err != nil {
		return fmt.Errorf("failed to create controller manager: %w", err)
	}

	// Register a minimal healthz/readyz check so controller-runtime's probe
	// endpoints return 200 instead of 404. Without at least one check
	// registered, the kubelet liveness/readiness probes fail and the pod
	// crash-loops even when the manager is running fine.
	if err := mcMgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("failed to add healthz check: %w", err)
	}
	if err := mcMgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("failed to add readyz check: %w", err)
	}

	informerMgr, err := informer.NewManagerFromManager(mcMgr.GetLocalManager())
	if err != nil {
		return fmt.Errorf("failed to create informer manager: %w", err)
	}

	logger := klog.NewKlogr().WithName("quota-controllers")

	if err := controllers.SetupQuotaControllers(mcMgr, dynamicClient, informerMgr, controllers.Options{
		AllocationMode:       controllers.AllocationMode(options.AllocationMode),
		BucketNamespace:      options.BucketNamespace,
		StalePendingClaimAge: options.StalePendingClaimAge,
		AllocationWorkers:    options.AllocationWorkers,
	}, logger); err != nil {
		return fmt.Errorf("failed to setup quota controllers: %w", err)
	}

	klog.Info("Starting controller manager")
	if err := mcMgr.Start(ctx); err != nil {
		return fmt.Errorf("controller manager exited with error: %w", err)
	}

	klog.Info("Controller manager shutdown complete")
	return nil
}
