package apiserver

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog/v2"

	_ "go.miloapis.com/quota/internal/metrics"
	"go.miloapis.com/quota/internal/registry/quota/claimcreationpolicy"
	"go.miloapis.com/quota/internal/registry/quota/grantcreationpolicy"
	"go.miloapis.com/quota/internal/registry/quota/quotabucket"
	"go.miloapis.com/quota/internal/registry/quota/quotaclaim"
	"go.miloapis.com/quota/internal/registry/quota/quotadefinition"
	"go.miloapis.com/quota/internal/registry/quota/quotagrant"
	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	"go.miloapis.com/quota/internal/webhook"
	"go.miloapis.com/quota/pkg/admission/enforcement"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

var (
	// Scheme defines the runtime type system for API object serialization.
	Scheme = runtime.NewScheme()
	// Codecs provides serializers for API objects.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	install.Install(Scheme)

	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// Register unversioned meta types required by the API machinery.
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// Allocator runs the synchronous transactional quota allocator. It is
// supplied by the serve command when the storage backend can execute
// cross-object transactions (currently just Postgres). When nil, the
// QuotaClaim REST storage falls back to the standard asynchronous
// (controller-driven) allocation flow used by the etcd backend.
//
// The allocator works with internal quota types because that's what the
// REST registry layer and the Postgres codec both use. Conversion to and
// from v1alpha1 is handled by the standard apiserver request pipeline at
// the request/response boundary.
type Allocator interface {
	Allocate(ctx context.Context, claim *quota.QuotaClaim) (*quota.QuotaClaim, error)
	Release(ctx context.Context, claim *quota.QuotaClaim) error
}

// ExtraConfig extends the generic apiserver configuration with quota-specific settings.
type ExtraConfig struct {
	// StorageBackend selects the storage backend: "etcd", "postgres", or "dual".
	StorageBackend string
	// PostgresDSN is the Postgres connection string (used when StorageBackend is "postgres" or "dual").
	PostgresDSN string
	// ReadyzChecks are additional readiness checks to register with the apiserver.
	ReadyzChecks []healthz.HealthChecker
	// Allocator, if non-nil, enables synchronous in-request quota
	// allocation for QuotaClaim creates and releases on delete.
	Allocator Allocator
	// NativePool is the pgx-native pool used by the postgres-native
	// QuotaClaim REST storage. It is set only when StorageBackend is
	// "postgres"; dual and etcd modes leave it nil.
	NativePool *pgstore.NativePool
	// EnforcementPlugin, if non-nil, is registered as a webhook handler at
	// /webhook/enforce so that the kube-apiserver can send non-quota resource
	// creates to the quota apiserver via a ValidatingWebhookConfiguration.
	// The handler requires an authenticated, authorized caller.
	EnforcementPlugin *enforcement.QuotaEnforcementPlugin
}

// Config combines generic and quota-specific configuration.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// QuotaServer is the quota service apiserver.
type QuotaServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig prevents incomplete configuration from being used.
type CompletedConfig struct {
	*completedConfig
}

// Complete validates and fills default values for the configuration.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}
	return CompletedConfig{&c}
}

// New creates and initializes the QuotaServer with storage and API groups.
func (c completedConfig) New() (*QuotaServer, error) {
	genericServer, err := c.GenericConfig.New("quota-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	s := &QuotaServer{
		GenericAPIServer: genericServer,
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(v1alpha1.GroupName, Scheme, metav1.ParameterCodec, Codecs)

	v1alpha1Storage := map[string]rest.Storage{}

	// QuotaDefinition (cluster-scoped)
	definitionStorage, definitionStatusStorage, err := quotadefinition.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, fmt.Errorf("failed to create QuotaDefinition storage: %w", err)
	}
	v1alpha1Storage["quotadefinitions"] = definitionStorage
	v1alpha1Storage["quotadefinitions/status"] = definitionStatusStorage

	// QuotaGrant (namespaced)
	grantStorage, grantStatusStorage, err := quotagrant.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, fmt.Errorf("failed to create QuotaGrant storage: %w", err)
	}
	v1alpha1Storage["quotagrants"] = grantStorage
	v1alpha1Storage["quotagrants/status"] = grantStatusStorage

	// QuotaClaim (namespaced). The wiring branches on the storage backend:
	//
	//  - postgres: use the postgres-native rest.Storage layer that bypasses
	//    genericregistry.Store and the cacher. Requires Allocator +
	//    NativePool.
	//  - dual: fall through to the standard storage + allocator decorator.
	//    The custom path is intentionally NOT used here; dual mode writes
	//    through dualstore to both backends and reads from the etcd cacher.
	//  - etcd (default): standard storage.
	switch c.ExtraConfig.StorageBackend {
	case "postgres":
		if c.ExtraConfig.Allocator == nil {
			return nil, fmt.Errorf("postgres backend requires an allocator")
		}
		if c.ExtraConfig.NativePool == nil {
			return nil, fmt.Errorf("postgres backend requires a native pgx pool")
		}
		native, nativeStatus, err := quotaclaim.NewPostgresNativeStorage(
			Scheme,
			c.GenericConfig.RESTOptionsGetter,
			c.ExtraConfig.Allocator,
			c.ExtraConfig.NativePool,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgres-native QuotaClaim storage: %w", err)
		}
		v1alpha1Storage["quotaclaims"] = native
		v1alpha1Storage["quotaclaims/status"] = nativeStatus
	default:
		// etcd or dual: use the standard storage path unchanged. In dual
		// mode the allocator is intentionally nil (see cmd/quota/serve.go
		// `case "dual"`), so we fall through to NewStorage. In the rare
		// case an allocator is configured with a non-postgres backend
		// (etcd single-mode with the synchronous allocator) we still
		// wrap with the AllocatingREST decorator.
		if c.ExtraConfig.Allocator != nil {
			allocatingClaimStorage, claimStatusStorage, err := quotaclaim.NewAllocatingStorage(
				Scheme,
				c.GenericConfig.RESTOptionsGetter,
				c.ExtraConfig.Allocator,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create allocating QuotaClaim storage: %w", err)
			}
			v1alpha1Storage["quotaclaims"] = allocatingClaimStorage
			v1alpha1Storage["quotaclaims/status"] = claimStatusStorage
		} else {
			claimStorage, claimStatusStorage, err := quotaclaim.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
			if err != nil {
				return nil, fmt.Errorf("failed to create QuotaClaim storage: %w", err)
			}
			v1alpha1Storage["quotaclaims"] = claimStorage
			v1alpha1Storage["quotaclaims/status"] = claimStatusStorage
		}
	}

	// QuotaBucket (namespaced, system-managed)
	bucketStorage, bucketStatusStorage, err := quotabucket.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, fmt.Errorf("failed to create QuotaBucket storage: %w", err)
	}
	v1alpha1Storage["quotabuckets"] = bucketStorage
	v1alpha1Storage["quotabuckets/status"] = bucketStatusStorage

	// GrantCreationPolicy (cluster-scoped)
	grantPolicyStorage, grantPolicyStatusStorage, err := grantcreationpolicy.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, fmt.Errorf("failed to create GrantCreationPolicy storage: %w", err)
	}
	v1alpha1Storage["grantcreationpolicies"] = grantPolicyStorage
	v1alpha1Storage["grantcreationpolicies/status"] = grantPolicyStatusStorage

	// ClaimCreationPolicy (cluster-scoped)
	claimPolicyStorage, claimPolicyStatusStorage, err := claimcreationpolicy.NewStorage(Scheme, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClaimCreationPolicy storage: %w", err)
	}
	v1alpha1Storage["claimcreationpolicies"] = claimPolicyStorage
	v1alpha1Storage["claimcreationpolicies/status"] = claimPolicyStatusStorage

	apiGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = v1alpha1Storage

	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	// Add readiness checks from admission plugins. These are injected by
	// the caller and block the /readyz endpoint until caches are synced.
	for _, check := range c.ExtraConfig.ReadyzChecks {
		if err := s.GenericAPIServer.AddReadyzChecks(check); err != nil {
			return nil, fmt.Errorf("failed to add readyz check %q: %w", check.Name(), err)
		}
	}

	// Register the enforcement webhook handler so the kube-apiserver can
	// route non-quota resource creates to it via a ValidatingWebhookConfiguration.
	// NonGoRestfulMux sits behind the full filter chain, so callers are
	// authenticated and must be authorized for "post" on the /webhook/enforce
	// non-resource URL. Never add the path to --authorization-always-allow-paths;
	// see config/components/enforcement-webhook/README.md.
	if c.ExtraConfig.EnforcementPlugin != nil {
		webhookHandler := webhook.NewEnforcementWebhookHandler(c.ExtraConfig.EnforcementPlugin, Scheme)
		s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/webhook/enforce", webhookHandler)
		klog.Info("Registered enforcement webhook handler at /webhook/enforce")
	}

	klog.Info("Quota server initialized successfully")

	return s, nil
}

// Run starts the server and blocks until the context is cancelled.
func (s *QuotaServer) Run(ctx context.Context) error {
	return s.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
