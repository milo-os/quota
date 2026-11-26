package enforcement

import (
	"context"
	"time"
)

// ClaimResult represents the result of waiting for a QuotaClaim to be processed.
type ClaimResult struct {
	// Granted indicates whether the QuotaClaim was granted (true) or denied (false)
	Granted bool

	// Reason provides the reason for the result (e.g., "quota exceeded", "approved")
	Reason string

	// Error contains any error that occurred during processing
	Error error
}

// ClaimWatchManager provides an interface for watching QuotaClaim status changes
// and waiting for specific claim outcomes.
type ClaimWatchManager interface {
	// RegisterClaimWaiter registers a waiter for a specific QuotaClaim.
	// Returns a channel that will receive the result, a cancel function, and any error.
	RegisterClaimWaiter(ctx context.Context, claimName, namespace string, timeout time.Duration) (<-chan ClaimResult, context.CancelFunc, error)

	// UnregisterClaimWaiter unregisters a waiter for a specific QuotaClaim.
	UnregisterClaimWaiter(claimName, namespace string)

	// Start starts the watch manager's background operations.
	Start(ctx context.Context) error
}
