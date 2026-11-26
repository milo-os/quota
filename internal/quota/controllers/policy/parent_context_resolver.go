// Package policy implements controllers for quota policy management.
package policy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultClientTTL is the default TTL for cached clients (1 hour)
	DefaultClientTTL = 1 * time.Hour
)

// ParentContextResolver manages REST configurations for target control planes
// with minimal memory footprint for cross-cluster grant creation.
//
// The resolver uses opaque consumer references ({apiGroup, kind, name}) to determine
// target control planes. Consumer integrations register handlers for specific
// parent context types to provide the actual routing logic.
type ParentContextResolver struct {
	clientCache    map[string]*CachedClient
	mu             sync.RWMutex
	defaultClient  client.Client
	baseRestConfig *rest.Config
	scheme         *runtime.Scheme
	clientTTL      time.Duration
	cleanupCancel  context.CancelFunc

	// handlers maps apiGroup/kind to a function that creates REST configs for that parent context type.
	// Consumer integrations register handlers to support their specific parent context types.
	handlers map[string]ParentContextHandler
}

// ParentContextHandler creates a REST config for a given parent context.
// Consumer integrations provide implementations for their specific parent context types.
type ParentContextHandler func(cfg *rest.Config, parentContext *ParentContextSpec) (*rest.Config, error)

// CachedClient represents a cached client with TTL.
type CachedClient struct {
	client    client.Client
	createdAt time.Time
	ttl       time.Duration
}

// ParentContextResolverOptions configures the ParentContextResolver.
type ParentContextResolverOptions struct {
	ClientTTL time.Duration
}

// NewParentContextResolver creates a new ParentContextResolver.
func NewParentContextResolver(defaultClient client.Client, baseRestConfig *rest.Config, scheme *runtime.Scheme, opts ParentContextResolverOptions) *ParentContextResolver {
	if opts.ClientTTL == 0 {
		opts.ClientTTL = DefaultClientTTL
	}

	resolver := &ParentContextResolver{
		clientCache:    make(map[string]*CachedClient),
		defaultClient:  defaultClient,
		baseRestConfig: baseRestConfig,
		scheme:         scheme,
		clientTTL:      opts.ClientTTL,
		handlers:       make(map[string]ParentContextHandler),
	}

	resolver.startCleanupTask()

	return resolver
}

// RegisterHandler registers a handler for a specific parent context type.
// The key should be in the format "apiGroup/kind" (e.g., "resourcemanager.example.com/Project").
// Consumer integrations use this to add support for their specific parent context types.
func (r *ParentContextResolver) RegisterHandler(apiGroup, kind string, handler ParentContextHandler) {
	key := fmt.Sprintf("%s/%s", apiGroup, kind)
	r.handlers[key] = handler
}

// ResolveClient resolves a client for the given parent context.
func (r *ParentContextResolver) ResolveClient(
	ctx context.Context,
	parentContext *ParentContextSpec,
	triggerObj *unstructured.Unstructured,
) (client.Client, error) {
	logger := log.FromContext(ctx).WithValues(
		"parentContextKind", parentContext.Kind,
		"parentContextAPIGroup", parentContext.APIGroup,
		"parentContextName", parentContext.Name,
	)

	cacheKey := fmt.Sprintf("%s/%s/%s",
		parentContext.APIGroup,
		parentContext.Kind,
		parentContext.Name,
	)

	r.mu.RLock()
	cachedClient, exists := r.clientCache[cacheKey]
	r.mu.RUnlock()

	if exists && time.Since(cachedClient.createdAt) < cachedClient.ttl {
		logger.V(2).Info("Using cached client for parent context")
		return cachedClient.client, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if cachedClient, exists := r.clientCache[cacheKey]; exists &&
		time.Since(cachedClient.createdAt) < cachedClient.ttl {
		return cachedClient.client, nil
	}

	newClient, err := r.createClientForParentContext(ctx, parentContext, triggerObj)
	if err != nil {
		logger.Error(err, "Failed to create client for parent context")
		return r.defaultClient, nil
	}

	r.clientCache[cacheKey] = &CachedClient{
		client:    newClient,
		createdAt: time.Now(),
		ttl:       r.clientTTL,
	}

	logger.V(1).Info("Created and cached new client for parent context")
	return newClient, nil
}

// createClientForParentContext creates a new client for the given parent context.
func (r *ParentContextResolver) createClientForParentContext(
	ctx context.Context,
	parentContext *ParentContextSpec,
	triggerObj *unstructured.Unstructured,
) (client.Client, error) {
	logger := log.FromContext(ctx).WithValues(
		"parentContextKind", parentContext.Kind,
		"parentContextAPIGroup", parentContext.APIGroup,
		"parentContextName", parentContext.Name,
		"triggerObject", triggerObj.GetName(),
	)

	if err := r.validateParentContext(parentContext); err != nil {
		return nil, fmt.Errorf("parent context validation failed: %w", err)
	}

	cfg, err := r.createRestConfigForParent(parentContext)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST config: %w", err)
	}

	targetClient, err := client.New(cfg, client.Options{
		Scheme: r.scheme,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	logger.V(1).Info("Successfully created client for parent context")
	return targetClient, nil
}

// validateParentContext validates that the parent context is supported.
func (r *ParentContextResolver) validateParentContext(parentContext *ParentContextSpec) error {
	if parentContext.Name == "" {
		return fmt.Errorf("parent context name cannot be empty")
	}

	// Check if a handler is registered for this parent context type
	key := fmt.Sprintf("%s/%s", parentContext.APIGroup, parentContext.Kind)
	if _, exists := r.handlers[key]; exists {
		return nil
	}

	// No handler registered - use default URL path-based routing
	// This provides a generic mechanism that works for any control plane proxy pattern
	return nil
}

// createRestConfigForParent creates a REST config for connecting to the parent context's control plane.
func (r *ParentContextResolver) createRestConfigForParent(parentContext *ParentContextSpec) (*rest.Config, error) {
	cfg := rest.CopyConfig(r.baseRestConfig)

	// Check if a handler is registered for this parent context type
	key := fmt.Sprintf("%s/%s", parentContext.APIGroup, parentContext.Kind)
	if handler, exists := r.handlers[key]; exists {
		return handler(cfg, parentContext)
	}

	// Default behavior: construct a URL path using the parent context's API group, kind, and name.
	// This follows the pattern: /apis/{apiGroup}/v1alpha1/{pluralKind}/{name}/control-plane
	// Consumer integrations should register a handler if they need different routing.
	return r.createDefaultRestConfig(cfg, parentContext)
}

// createDefaultRestConfig returns the base REST config when no handler is registered.
// Without a registered ParentContextHandler the resolver cannot know how to route to
// an external control plane, so it falls back to the same cluster. Consumer
// integrations that need cross-cluster routing should register a handler via
// RegisterHandler.
func (r *ParentContextResolver) createDefaultRestConfig(cfg *rest.Config, parentContext *ParentContextSpec) (*rest.Config, error) {
	// Return the base config as-is. The parentContext metadata (apiGroup, kind,
	// name) is used by the caller for templating, not for URL construction.
	return cfg, nil
}

// startCleanupTask starts a background goroutine to periodically clean up expired clients.
func (r *ParentContextResolver) startCleanupTask() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cleanupCancel = cancel

	go func() {
		ticker := time.NewTicker(r.clientTTL)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.cleanupExpiredClients()
			}
		}
	}()
}

// cleanupExpiredClients removes expired clients from the cache.
func (r *ParentContextResolver) cleanupExpiredClients() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for key, cachedClient := range r.clientCache {
		if now.Sub(cachedClient.createdAt) >= cachedClient.ttl {
			delete(r.clientCache, key)
		}
	}
}

// Close stops the background cleanup task and cleans up resources.
func (r *ParentContextResolver) Close() {
	if r.cleanupCancel != nil {
		r.cleanupCancel()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.clientCache = make(map[string]*CachedClient)
}
