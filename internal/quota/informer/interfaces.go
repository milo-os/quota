// Package informer provides dynamic informer management for custom resources.
//
// The informer package allows controllers to watch custom resources dynamically,
// adding and removing watches at runtime without restarting the application.
// It handles resource lifecycle, reference counting, and distributes events to
// multiple consumers interested in the same resource type.
package informer

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Manager provides dynamic informer management for custom resources.
//
// Manager implements controller-runtime's manager.Runnable interface, allowing
// it to be registered with a controller manager and automatically started/stopped
// with the manager's lifecycle.
type Manager interface {
	// AddWatch registers a new watch for the specified resource type.
	AddWatch(ctx context.Context, req WatchRequest) error

	// RemoveWatch unregisters a watch for the specified resource type and consumer.
	RemoveWatch(ctx context.Context, gvk schema.GroupVersionKind, consumerID string) error

	// Start initializes the manager and begins informer management.
	Start(ctx context.Context) error

	// Stop shuts down the manager and all managed informers.
	Stop() error

	// NeedLeaderElection returns true, indicating this component should only
	// run on the leader when leader election is enabled.
	NeedLeaderElection() bool
}

// WatchRequest represents a request to watch a specific resource type.
type WatchRequest struct {
	// GVK is the GroupVersionKind of the resource to watch.
	GVK schema.GroupVersionKind

	// ConsumerID is a unique identifier for the consumer requesting the watch.
	ConsumerID string

	// Handler receives events for resources of this type.
	Handler ResourceEventHandler
}

// ResourceEventHandler receives events for watched resources.
type ResourceEventHandler interface {
	// OnAdd is called when a resource is created.
	OnAdd(obj *unstructured.Unstructured)

	// OnUpdate is called when a resource is updated.
	OnUpdate(old, new *unstructured.Unstructured)

	// OnDelete is called when a resource is deleted.
	OnDelete(obj *unstructured.Unstructured)
}
