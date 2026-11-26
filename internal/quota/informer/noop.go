package informer

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// noOpManager is a no-op implementation of the Manager interface.
// It is intended for unit tests that need a Manager value but do not
// want a real client-go connection to a cluster. Production wire-up
// should use NewManagerFromManager instead.
type noOpManager struct{}

// NewNoOpManager creates a no-op informer Manager.
func NewNoOpManager() Manager {
	return &noOpManager{}
}

func (m *noOpManager) AddWatch(ctx context.Context, req WatchRequest) error {
	return nil
}

func (m *noOpManager) RemoveWatch(ctx context.Context, gvk schema.GroupVersionKind, consumerID string) error {
	return nil
}

func (m *noOpManager) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (m *noOpManager) Stop() error {
	return nil
}

func (m *noOpManager) NeedLeaderElection() bool {
	return true
}
