package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	openapiutil "k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"

	quotaapiserver "go.miloapis.com/quota/internal/apiserver"
	"go.miloapis.com/quota/internal/authn/frontproxy"
	admissionquota "go.miloapis.com/quota/internal/quota/admission"
	admissionvalidation "go.miloapis.com/quota/internal/quota/admission/validation"
	dualstore "go.miloapis.com/quota/internal/storage/dual"
	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	"go.miloapis.com/quota/internal/version"
	admissionenforcement "go.miloapis.com/quota/pkg/admission/enforcement"
	generatedopenapi "go.miloapis.com/quota/pkg/generated/openapi"

	// Register JSON logging format
	_ "k8s.io/component-base/logs/json/register"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilruntime.Must(utilfeature.DefaultMutableFeatureGate.Set("LoggingBetaOptions=true"))
	utilruntime.Must(utilfeature.DefaultMutableFeatureGate.Set("RemoteRequestHeaderUID=true"))
}

// QuotaServerOptions contains configuration for the quota server.
type QuotaServerOptions struct {
	RecommendedOptions *options.RecommendedOptions
	Logs               *logsapi.LoggingConfiguration

	// StorageBackend selects the storage backend: "etcd", "postgres", or "dual".
	StorageBackend string
	// PostgresDSN is the Postgres connection string.
	PostgresDSN string
	// DisableStorageCacher disables the in-memory storage cacher for all
	// quota resources. Only effective when StorageBackend is "postgres".
	DisableStorageCacher bool
	// ChangelogRetention is how long Postgres changelog rows are kept for
	// watch resume and paginated lists. Zero uses the watch hub default.
	ChangelogRetention time.Duration
	// BucketNamespace is the fallback namespace the Postgres allocator uses
	// for QuotaBuckets whose consumerRef has no namespace. It must match the
	// controller-manager's --quota-bucket-namespace.
	BucketNamespace string
	// AllocationMaxBatch caps how many concurrent claim allocations and
	// releases the Postgres allocator commits in one transaction. 1
	// disables batching.
	AllocationMaxBatch int

	// Enforcement configures the QuotaEnforcement admission plugin and
	// webhook.
	Enforcement EnforcementOptions

	// FrontProxyVerifyCacheTTL is how long a successful verification of the
	// front proxy's (kube-apiserver's) client certificate chain is reused.
	// Zero disables the cache.
	FrontProxyVerifyCacheTTL time.Duration

	// namespaceLister backs namespace-annotation consumer mapping. It is set
	// in Config once the core informer factory exists.
	namespaceLister corev1listers.NamespaceLister
	// coreClient is the live fallback for namespaces not yet in the cache.
	coreClient kubernetes.Interface
}

// EnforcementOptions configures the QuotaEnforcement admission plugin.
type EnforcementOptions struct {
	// ClaimTimeout is how long admission waits for a claim decision. Keep it
	// below the enforcement webhook's timeoutSeconds.
	ClaimTimeout time.Duration
	// ClusterScopedClaimNamespace hosts claims for cluster-scoped resources.
	ClusterScopedClaimNamespace string
	// ConsumerFromUserExtra and ConsumerFromNamespaceAnnotation are
	// "<key>=<Kind>[.<apiGroup>]" mappings used when a ClaimCreationPolicy
	// template sets no consumerRef.
	ConsumerFromUserExtra           []string
	ConsumerFromNamespaceAnnotation []string
	// ConsumerRefFromUserExtra entries are "<nameKey>,<kindKey>[,<apiGroupKey>]":
	// the whole consumerRef is read from user-info extras.
	ConsumerRefFromUserExtra []string
}

// NewQuotaServerOptions creates options with default values.
func NewQuotaServerOptions() *QuotaServerOptions {
	o := &QuotaServerOptions{
		RecommendedOptions: options.NewRecommendedOptions(
			"/registry/quota.miloapis.com",
			quotaapiserver.Codecs.LegacyCodec(quotaapiserver.Scheme.PrioritizedVersionsAllGroups()...),
		),
		Logs:                     logsapi.NewLoggingConfiguration(),
		StorageBackend:           "etcd",
		BucketNamespace:          pgstore.DefaultBucketNamespace,
		AllocationMaxBatch:       pgstore.DefaultMaxBatchSize,
		FrontProxyVerifyCacheTTL: frontproxy.DefaultTTL,
		Enforcement: EnforcementOptions{
			ClaimTimeout:                admissionenforcement.ClaimWaitTimeout,
			ClusterScopedClaimNamespace: admissionenforcement.DefaultClusterScopedClaimNamespace,
		},
	}

	// Register quota admission plugins. Enforcement options are built
	// lazily because the plugin factory runs after flags are parsed.
	o.RecommendedOptions.Admission.Plugins = admission.NewPlugins()
	admissionvalidation.Register(o.RecommendedOptions.Admission.Plugins)
	admissionquota.RegisterEnforcement(o.RecommendedOptions.Admission.Plugins, o.enforcementPluginOptions)
	o.RecommendedOptions.Admission.RecommendedPluginOrder = []string{
		admissionvalidation.PluginName,
		admissionenforcement.PluginName,
	}
	o.RecommendedOptions.Admission.DefaultOffPlugins = nil

	return o
}

// AddFlags registers command-line flags for all options.
func (o *QuotaServerOptions) AddFlags(fs *pflag.FlagSet) {
	o.RecommendedOptions.AddFlags(fs)

	fs.StringVar(&o.StorageBackend, "quota-storage-backend", o.StorageBackend,
		"Quota storage backend to use: etcd, postgres, or dual")
	fs.StringVar(&o.PostgresDSN, "postgres-dsn", o.PostgresDSN,
		"PostgreSQL connection string (required when storage-backend is postgres or dual)")
	fs.BoolVar(&o.DisableStorageCacher, "disable-storage-cacher", o.DisableStorageCacher,
		"Disable the in-memory storage cacher for all quota resources (postgres mode only)")
	fs.StringVar(&o.BucketNamespace, "quota-bucket-namespace", o.BucketNamespace,
		"Namespace for QuotaBuckets whose consumerRef has no namespace (postgres mode only). "+
			"Must match the controller-manager's --quota-bucket-namespace.")
	fs.IntVar(&o.AllocationMaxBatch, "postgres-allocation-max-batch", o.AllocationMaxBatch,
		"Maximum number of concurrent QuotaClaim allocations and releases committed together in one "+
			"Postgres transaction (group commit). 1 disables batching (postgres mode only)")
	fs.DurationVar(&o.ChangelogRetention, "postgres-changelog-retention", o.ChangelogRetention,
		"How long Postgres changelog rows are kept; watches or paginated lists older than this get 410 Gone. "+
			"Zero uses the built-in default (postgres and dual modes)")

	fs.DurationVar(&o.FrontProxyVerifyCacheTTL, "front-proxy-verify-cache-ttl", o.FrontProxyVerifyCacheTTL,
		"How long a successful verification of the front proxy's client certificate chain against the requestheader "+
			"client CA is reused. Certificate validity, CA changes and --requestheader-allowed-names are still checked on "+
			"every request. 0 disables the cache.")

	fs.DurationVar(&o.Enforcement.ClaimTimeout, "quota-enforcement-claim-timeout", o.Enforcement.ClaimTimeout,
		"How long quota enforcement admission waits for a QuotaClaim decision. Must stay below the "+
			"enforcement webhook's timeoutSeconds (30s) so callers get a precise timeout error.")
	fs.StringVar(&o.Enforcement.ClusterScopedClaimNamespace, "quota-cluster-scoped-claim-namespace", o.Enforcement.ClusterScopedClaimNamespace,
		"Namespace for QuotaClaims created for cluster-scoped resources when the ClaimCreationPolicy does not pin one.")
	fs.StringSliceVar(&o.Enforcement.ConsumerFromUserExtra, "quota-enforcement-consumer-from-user-extra", o.Enforcement.ConsumerFromUserExtra,
		"When a ClaimCreationPolicy template sets no consumerRef, read the consumer name from this user-info extra. "+
			"Format <extra-key>=<Kind>[.<apiGroup>]; repeatable, first match wins. Requests that resolve no consumer are denied.")
	fs.StringArrayVar(&o.Enforcement.ConsumerRefFromUserExtra, "quota-enforcement-consumer-ref-from-user-extra", o.Enforcement.ConsumerRefFromUserExtra,
		"When a ClaimCreationPolicy template sets no consumerRef, read the consumer's name, kind and API group from "+
			"separate user-info extras (tried before --quota-enforcement-consumer-from-user-extra). "+
			"Format <nameKey>,<kindKey>[,<apiGroupKey>]; repeatable, first match wins.")
	fs.StringSliceVar(&o.Enforcement.ConsumerFromNamespaceAnnotation, "quota-enforcement-consumer-from-namespace-annotation", o.Enforcement.ConsumerFromNamespaceAnnotation,
		"When a ClaimCreationPolicy template sets no consumerRef, read the consumer name from this annotation on the "+
			"resource's namespace (tried after user extras). Format <annotation-key>=<Kind>[.<apiGroup>]; repeatable.")
}

// parseConsumerRefSources parses "<nameKey>,<kindKey>[,<apiGroupKey>]" entries.
func parseConsumerRefSources(flag string, values []string) ([]admissionenforcement.ConsumerRefSource, error) {
	var out []admissionenforcement.ConsumerRefSource
	for _, v := range values {
		parts := strings.Split(v, ",")
		if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("--%s: %q must have the form <nameKey>,<kindKey>[,<apiGroupKey>]", flag, v)
		}
		src := admissionenforcement.ConsumerRefSource{NameKey: parts[0], KindKey: parts[1]}
		if len(parts) == 3 {
			src.APIGroupKey = parts[2]
		}
		out = append(out, src)
	}
	return out, nil
}

// parseConsumerSources parses "<key>=<Kind>[.<apiGroup>]" mappings.
func parseConsumerSources(flag string, values []string) ([]admissionenforcement.ConsumerSource, error) {
	var out []admissionenforcement.ConsumerSource
	for _, v := range values {
		key, kindGroup, ok := strings.Cut(v, "=")
		if !ok || key == "" || kindGroup == "" {
			return nil, fmt.Errorf("--%s: %q must have the form <key>=<Kind>[.<apiGroup>]", flag, v)
		}
		kind, group, _ := strings.Cut(kindGroup, ".")
		if kind == "" {
			return nil, fmt.Errorf("--%s: %q has an empty Kind", flag, v)
		}
		out = append(out, admissionenforcement.ConsumerSource{Key: key, Kind: kind, APIGroup: group})
	}
	return out, nil
}

// enforcementPluginOptions builds the enforcement plugin options from flags.
// It runs inside the admission plugin factory; Validate has already checked
// the mappings, so parse errors cannot occur here.
func (o *QuotaServerOptions) enforcementPluginOptions() []admissionenforcement.Option {
	cfg := admissionenforcement.DefaultConfig()
	cfg.WatchManager.DefaultTimeout = o.Enforcement.ClaimTimeout
	cfg.ClusterScopedClaimNamespace = o.Enforcement.ClusterScopedClaimNamespace
	userExtra, _ := parseConsumerSources("quota-enforcement-consumer-from-user-extra", o.Enforcement.ConsumerFromUserExtra)
	nsAnnotation, _ := parseConsumerSources("quota-enforcement-consumer-from-namespace-annotation", o.Enforcement.ConsumerFromNamespaceAnnotation)
	userExtraRef, _ := parseConsumerRefSources("quota-enforcement-consumer-ref-from-user-extra", o.Enforcement.ConsumerRefFromUserExtra)
	cfg.ConsumerMapping = admissionenforcement.ConsumerMapping{UserExtraRef: userExtraRef, UserExtra: userExtra, NamespaceAnnotation: nsAnnotation}

	opts := []admissionenforcement.Option{admissionenforcement.WithConfig(cfg)}
	if len(nsAnnotation) > 0 {
		opts = append(opts, admissionenforcement.WithNamespaceLookup(o.lookupNamespaceAnnotations))
	}
	return opts
}

// lookupNamespaceAnnotations reads namespace annotations from the core
// API's informer cache, falling back to a live GET for namespaces the cache
// has not seen yet (a resource created right after its namespace).
func (o *QuotaServerOptions) lookupNamespaceAnnotations(ctx context.Context, name string) (map[string]string, error) {
	if o.namespaceLister == nil {
		return nil, fmt.Errorf("namespace lookup is not available: the core API kubeconfig is not configured")
	}
	ns, err := o.namespaceLister.Get(name)
	if err == nil {
		return ns.Annotations, nil
	}
	if !apierrors.IsNotFound(err) || o.coreClient == nil {
		return nil, err
	}
	live, err := o.coreClient.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return live.Annotations, nil
}

// Complete fills in fields that can be derived from other fields.
func (o *QuotaServerOptions) Complete() error {
	return nil
}

// Validate ensures required configuration is provided.
func (o *QuotaServerOptions) Validate() error {
	var errs []error

	// The generic apiserver options (serving, authn/z, admission, audit,
	// etcd). The postgres backend never talks to etcd (Config drops the etcd
	// options), so --etcd-servers is not required there.
	recommended := *o.RecommendedOptions
	if o.StorageBackend == "postgres" {
		recommended.Etcd = nil
	}
	errs = append(errs, recommended.Validate()...)

	switch o.StorageBackend {
	case "etcd", "postgres", "dual":
		// valid
	default:
		errs = append(errs, fmt.Errorf("--quota-storage-backend must be one of: etcd, postgres, dual (got %q)", o.StorageBackend))
	}

	if (o.StorageBackend == "postgres" || o.StorageBackend == "dual") && o.PostgresDSN == "" {
		errs = append(errs, fmt.Errorf("--postgres-dsn is required when --quota-storage-backend is %s", o.StorageBackend))
	}

	if o.Enforcement.ClaimTimeout <= 0 {
		errs = append(errs, fmt.Errorf("--quota-enforcement-claim-timeout must be positive"))
	}
	if _, err := parseConsumerSources("quota-enforcement-consumer-from-user-extra", o.Enforcement.ConsumerFromUserExtra); err != nil {
		errs = append(errs, err)
	}
	if _, err := parseConsumerSources("quota-enforcement-consumer-from-namespace-annotation", o.Enforcement.ConsumerFromNamespaceAnnotation); err != nil {
		errs = append(errs, err)
	}
	if _, err := parseConsumerRefSources("quota-enforcement-consumer-ref-from-user-extra", o.Enforcement.ConsumerRefFromUserExtra); err != nil {
		errs = append(errs, err)
	}

	if o.AllocationMaxBatch < 1 {
		errs = append(errs, fmt.Errorf("--postgres-allocation-max-batch must be at least 1 (got %d)", o.AllocationMaxBatch))
	}

	if o.RecommendedOptions.Authorization != nil {
		for _, p := range o.RecommendedOptions.Authorization.AlwaysAllowPaths {
			if allowPathCovers(p, enforcementWebhookPath) {
				errs = append(errs, fmt.Errorf("--authorization-always-allow-paths must not cover %s (got %q): the enforcement webhook creates QuotaClaims with the apiserver's identity and must only accept authenticated, authorized callers", enforcementWebhookPath, p))
			}
		}
	}

	if o.FrontProxyVerifyCacheTTL < 0 {
		errs = append(errs, fmt.Errorf("--front-proxy-verify-cache-ttl must not be negative"))
	}

	if o.DisableStorageCacher && o.StorageBackend != "postgres" {
		errs = append(errs, fmt.Errorf("--disable-storage-cacher is only supported when --quota-storage-backend=postgres"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation errors: %v", errs)
	}
	return nil
}

// enforcementWebhookPath is the non-resource path the enforcement webhook
// handler is served on.
const enforcementWebhookPath = "/webhook/enforce"

// allowPathCovers reports whether an --authorization-always-allow-paths entry
// matches path. Entries ending in "*" are prefix matches, as in the
// apiserver's path authorizer.
func allowPathCovers(entry, path string) bool {
	if prefix, ok := strings.CutSuffix(entry, "*"); ok {
		return strings.HasPrefix(path, prefix)
	}
	return strings.TrimSuffix(entry, "/") == path
}

// Config builds the complete server configuration from options.
func (o *QuotaServerOptions) Config() (*quotaapiserver.Config, error) {
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", nil, nil); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	genericConfig := genericapiserver.NewRecommendedConfig(quotaapiserver.Codecs)
	genericConfig.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString("1.35", "", "")

	// OpenAPI configuration (required by k8s.io/apiserver v0.35+)
	namer := openapinamer.NewDefinitionNamer(quotaapiserver.Scheme)
	getDefinitionName := func(name string) (string, spec.Extensions) {
		if strings.Contains(name, "/") {
			name = openapiutil.ToRESTFriendlyName(name)
		}
		return namer.GetDefinitionName(name)
	}

	genericConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(generatedopenapi.GetOpenAPIDefinitionsWithUnstructured, namer)
	genericConfig.OpenAPIV3Config.Info.Title = "Quota"
	genericConfig.OpenAPIV3Config.Info.Version = version.Version
	genericConfig.OpenAPIV3Config.GetDefinitionName = getDefinitionName
	genericConfig.OpenAPIV3Config.Definitions = generatedopenapi.GetOpenAPIDefinitionsWithUnstructured(func(name string) spec.Ref {
		defName, _ := getDefinitionName(name)
		return spec.MustCreateRef("#/components/schemas/" + openapicommon.EscapeJsonPointer(defName))
	})

	genericConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(generatedopenapi.GetOpenAPIDefinitionsWithUnstructured, namer)
	genericConfig.OpenAPIConfig.Info.Title = "Quota"
	genericConfig.OpenAPIConfig.Info.Version = version.Version
	genericConfig.OpenAPIConfig.GetDefinitionName = getDefinitionName
	genericConfig.OpenAPIConfig.Definitions = generatedopenapi.GetOpenAPIDefinitionsWithUnstructured(func(name string) spec.Ref {
		defName, _ := getDefinitionName(name)
		return spec.MustCreateRef("#/definitions/" + openapicommon.EscapeJsonPointer(defName))
	})

	// When using the Postgres-only backend, disable the etcd options entirely
	// before ApplyTo to prevent the apiserver from connecting to etcd or
	// registering etcd health checks. For dual mode we keep etcd options.
	if o.StorageBackend == "postgres" {
		o.RecommendedOptions.Etcd = nil
	}

	if err := o.RecommendedOptions.ApplyTo(genericConfig); err != nil {
		return nil, fmt.Errorf("failed to apply recommended options: %w", err)
	}

	// Every request kube-apiserver proxies carries its front-proxy client
	// certificate, and upstream re-verifies that chain (an RSA signature
	// check) per request. Put a cache of successful verifications in front;
	// anything it does not accept goes to the upstream authenticator as is.
	genericConfig.Authentication.Authenticator = frontproxy.Wrap(
		genericConfig.Authentication.Authenticator,
		genericConfig.Authentication.RequestHeaderConfig,
		frontproxy.Options{TTL: o.FrontProxyVerifyCacheTTL},
	)

	// Namespace-annotation consumer mapping reads the core API's namespaces.
	// Registering the informer here, before the server starts, lets the
	// generic apiserver's informer post-start hook start it.
	if len(o.Enforcement.ConsumerFromNamespaceAnnotation) > 0 {
		if genericConfig.SharedInformerFactory == nil || genericConfig.ClientConfig == nil {
			return nil, fmt.Errorf("--quota-enforcement-consumer-from-namespace-annotation requires a core API kubeconfig (--kubeconfig)")
		}
		nsInformer := genericConfig.SharedInformerFactory.Core().V1().Namespaces()
		nsInformer.Informer()
		o.namespaceLister = nsInformer.Lister()
		coreClient, err := kubernetes.NewForConfig(genericConfig.ClientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create core API client for namespace lookup: %w", err)
		}
		o.coreClient = coreClient
	}

	// Run the enforcement plugin's policy engine and watch managers for the
	// lifetime of the server rather than a detached background context.
	if hook := admissionquota.EnforcementPostStartHook(); hook != nil {
		if err := genericConfig.AddPostStartHook(admissionquota.EnforcementPostStartHookName, hook); err != nil {
			return nil, fmt.Errorf("failed to register enforcement post-start hook: %w", err)
		}
	}

	// Wire the configured storage backend's RESTOptionsGetter.
	codec := quotaapiserver.Codecs.LegacyCodec(quotaapiserver.Scheme.PrioritizedVersionsAllGroups()...)
	// allocator is non-nil only for storage backends that support
	// synchronous transactional quota allocation.
	var allocator quotaapiserver.Allocator
	var nativePool *pgstore.NativePool
	switch o.StorageBackend {
	case "postgres":
		pgGetter, err := pgstore.NewRESTOptionsGetter(o.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres RESTOptionsGetter: %w", err)
		}
		pgGetter.SetCodec(codec)
		if o.ChangelogRetention > 0 {
			pgGetter.SetChangelogRetention(o.ChangelogRetention)
		}
		// Stop the shared watch hub (LISTEN connection and compactor) when
		// the server shuts down.
		if err := genericConfig.AddPostStartHook("quota-postgres-watch-hub", func(hookCtx genericapiserver.PostStartHookContext) error {
			go func() {
				<-hookCtx.Done()
				pgGetter.Close()
			}()
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to register postgres watch hub shutdown hook: %w", err)
		}
		if o.DisableStorageCacher {
			pgGetter.SetDisableCacher(true)
			klog.InfoS("Storage cacher disabled for all quota resources in postgres mode")
		}
		// The postgres-native QuotaClaim REST layer serves claim watch
		// events itself (subscribed to the shared watch hub), so the
		// generic watcher should not also emit them. Exclude the claim
		// key prefix from the generic watcher's scan in postgres mode.
		pgGetter.SetWatchExcludedKeyPrefixes([]string{"/quota.miloapis.com/quotaclaims/"})
		genericConfig.RESTOptionsGetter = pgGetter
		// Construct the dedicated pgx-native pool used by the
		// postgres-native QuotaClaim REST layer. The Query pool serves
		// GET/LIST/UPDATE and watch catch-up reads; watch wake-ups come
		// from the hub's single LISTEN connection. The allocator also
		// draws from the Query pool so its pipelined commit path
		// (pgx.Batch SendBatch) shares the pool's prepared-statement
		// cache and skips the database/sql Conn.Raw bridge.
		nativePool, err = pgstore.NewNativePool(context.Background(), o.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres native pool: %w", err)
		}
		// Build the synchronous allocator against the pgxpool (preferred)
		// plus the pooled *sql.DB and the Store's codec so encoded objects
		// round-trip cleanly.
		allocator = pgstore.NewAllocatorWithPool(pgGetter.DB(), nativePool.Query, codec,
			pgstore.WithBucketNamespace(o.BucketNamespace),
			pgstore.WithMaxBatchSize(o.AllocationMaxBatch))
		// Replace the etcd-specific feature support checker with one that
		// advertises RequestWatchProgress as supported. The k8s.io/apiserver
		// cacher uses this signal to enable ConsistentListFromCache, which
		// lets default kubectl reads be served from the in-memory cache
		// instead of round-tripping to Postgres.
		etcdfeature.DefaultFeatureSupportChecker = pgstore.NewFeatureSupportChecker()
	case "dual":
		// Primary = etcd (the RESTOptionsGetter set by ApplyTo).
		// Shadow = Postgres. Dual mode intentionally leaves `allocator`
		// nil: writes go through etcd first and the dual-write getter
		// mirrors them to Postgres. Claim status is written by the
		// controller-mode bucket-allocation pump and claim aggregator
		// on the etcd primary, and those writes propagate to Postgres
		// through the same dual-write path. Operators must therefore
		// run the controller-manager with
		// --quota-allocation-mode=controller (the default) when using
		// this backend.
		etcdGetter := genericConfig.RESTOptionsGetter
		pgGetter, err := pgstore.NewRESTOptionsGetter(o.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres RESTOptionsGetter for dual mode: %w", err)
		}
		pgGetter.SetCodec(codec)
		if o.ChangelogRetention > 0 {
			pgGetter.SetChangelogRetention(o.ChangelogRetention)
		}
		genericConfig.RESTOptionsGetter = dualstore.NewRESTOptionsGetter(etcdGetter, pgGetter)
	}

	serverConfig := &quotaapiserver.Config{
		GenericConfig: genericConfig,
		ExtraConfig: quotaapiserver.ExtraConfig{
			StorageBackend: o.StorageBackend,
			PostgresDSN:    o.PostgresDSN,
			ReadyzChecks: []healthz.HealthChecker{
				admissionvalidation.ReadinessCheck(),
				// Not ready until every ClaimCreationPolicy is loaded:
				// before that the enforcement plugin rejects creates.
				admissionquota.EnforcementReadinessCheck(),
			},
			Allocator:         allocator,
			NativePool:        nativePool,
			EnforcementPlugin: admissionquota.GetEnforcementPlugin(),
		},
	}

	return serverConfig, nil
}

// NewServeCommand creates the serve subcommand that starts the API server.
func NewServeCommand() *cobra.Command {
	o := NewQuotaServerOptions()

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the quota API server",
		Long: `Start the quota API server and begin serving requests.

Exposes QuotaDefinition, QuotaGrant, QuotaClaim, and QuotaBucket resources
as an aggregated Kubernetes API server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return Run(o, cmd.Context())
		},
	}

	flags := cmd.Flags()
	o.AddFlags(flags)

	// Add logging flags
	logsapi.AddFlags(o.Logs, flags)

	return cmd
}

// Run initializes and starts the server.
func Run(o *QuotaServerOptions, ctx context.Context) error {
	if err := logsapi.ValidateAndApply(o.Logs, utilfeature.DefaultMutableFeatureGate); err != nil {
		return fmt.Errorf("failed to apply logging configuration: %w", err)
	}

	config, err := o.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New()
	if err != nil {
		return err
	}

	defer logs.FlushLogs()

	// The apiserver cannot see the controller-manager's flags, so the
	// pairing is enforced there: run the controller-manager with the same
	// --quota-storage-backend and it derives (or rejects) the allocation mode.
	klog.InfoS("Quota storage backend selected; run the controller-manager with the same --quota-storage-backend",
		"storageBackend", o.StorageBackend)

	klog.Info("Starting Quota server...")
	return server.Run(ctx)
}
