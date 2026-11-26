package enforcement

import (
	"time"
)

// TTLConfig holds configuration for watch manager TTL-based lifecycle management
type TTLConfig struct {
	// DefaultTTL is the default time-to-live when no active waiters
	DefaultTTL time.Duration

	// MinTTL is the minimum allowed TTL to prevent thrashing
	MinTTL time.Duration

	// MaxTTL is the maximum allowed TTL
	MaxTTL time.Duration
}

// RetryConfig holds configuration for watch stream retry behavior
type RetryConfig struct {
	// InitialDelay is the initial backoff delay after a failure
	InitialDelay time.Duration

	// MaxDelay is the maximum backoff delay (cap for exponential growth)
	MaxDelay time.Duration

	// Multiplier is the backoff multiplier for exponential growth
	Multiplier float64

	// Jitter is the random jitter factor (0.0-1.0) to apply to backoff
	Jitter float64
}

// WatchManagerConfig holds configuration for the ClaimWatchManager
type WatchManagerConfig struct {
	// DefaultTimeout is the default timeout for waiting for QuotaClaim results
	DefaultTimeout time.Duration

	// MaxWaiters is the maximum number of concurrent waiters (0 = unlimited)
	MaxWaiters int

	// TTL configuration for watch manager lifecycle
	TTL TTLConfig

	// Retry configuration for watch stream failures
	Retry RetryConfig
}

// DefaultWatchManagerConfig returns the default configuration for the watch manager
func DefaultWatchManagerConfig() *WatchManagerConfig {
	return &WatchManagerConfig{
		DefaultTimeout: ClaimWaitTimeout,
		MaxWaiters:     1000, // Reasonable default to prevent memory exhaustion
		TTL: TTLConfig{
			DefaultTTL: 5 * time.Minute,
			MinTTL:     30 * time.Second,
			MaxTTL:     30 * time.Minute,
		},
		Retry: RetryConfig{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.25,
		},
	}
}

// DefaultClusterScopedClaimNamespace is where QuotaClaims for cluster-scoped
// target resources are created. Cluster-scoped resources have no namespace to
// inherit, and QuotaClaims are namespaced, so they need a fixed home.
const DefaultClusterScopedClaimNamespace = "quota-system"

// ConsumerSource maps one request attribute to a consumer kind. When the
// attribute has a non-empty value it becomes the consumer's name.
type ConsumerSource struct {
	// Key is the user-info extra key, or the namespace annotation key.
	Key string
	// APIGroup and Kind are copied verbatim into the claim's consumerRef.
	// The quota service treats them as opaque strings.
	APIGroup string
	Kind     string
}

// ConsumerRefSource reads a whole consumer reference from user-info extras:
// the name, the kind and (optionally) the API group each come from their own
// key. Use it when the authenticator records the parent resource's type
// alongside its name, so one source covers several consumer kinds.
type ConsumerRefSource struct {
	NameKey     string
	KindKey     string
	APIGroupKey string
}

// ConsumerMapping configures how the plugin fills in a claim's consumerRef
// when the ClaimCreationPolicy template does not set one. Sources are tried
// in order: every UserExtraRef entry, then every UserExtra entry, then every
// NamespaceAnnotation entry. The
// first one that yields a value wins. If none does, admission is denied:
// the plugin never creates a claim with an empty consumer.
type ConsumerMapping struct {
	// UserExtraRef reads the consumer's name, kind and API group from
	// separate user-info extras. An entry applies only when both its name
	// and kind extras are non-empty.
	UserExtraRef []ConsumerRefSource
	// UserExtra reads the consumer name from the requesting user's extra
	// attributes (for example a parent-resource extra set by the
	// authenticator). The first value of the key is used.
	UserExtra []ConsumerSource
	// NamespaceAnnotation reads the consumer name from an annotation on the
	// triggering resource's namespace. Requires a NamespaceLookup (see
	// WithNamespaceLookup); ignored for cluster-scoped resources.
	NamespaceAnnotation []ConsumerSource
}

// Config holds configuration for the QuotaEnforcementPlugin
type Config struct {
	// WatchManager configuration
	WatchManager *WatchManagerConfig

	// ClusterScopedClaimNamespace is the namespace used for QuotaClaims when
	// the triggering resource is cluster-scoped and the ClaimCreationPolicy
	// does not pin spec.target.resourceClaimTemplate.metadata.namespace.
	// Defaults to DefaultClusterScopedClaimNamespace.
	ClusterScopedClaimNamespace string

	// ConsumerMapping resolves the claim consumer when the policy template
	// leaves it empty.
	ConsumerMapping ConsumerMapping

	// ConditionTimeout bounds how long one policy's trigger constraints may
	// take to evaluate. A request whose evaluation runs out of time is
	// denied. Defaults to DefaultConditionTimeout.
	ConditionTimeout time.Duration
}

// DefaultConditionTimeout is the default Config.ConditionTimeout.
const DefaultConditionTimeout = time.Second

// DefaultConfig returns the default configuration for the enforcement plugin
func DefaultConfig() *Config {
	return &Config{
		WatchManager:                DefaultWatchManagerConfig(),
		ClusterScopedClaimNamespace: DefaultClusterScopedClaimNamespace,
		ConditionTimeout:            DefaultConditionTimeout,
	}
}
