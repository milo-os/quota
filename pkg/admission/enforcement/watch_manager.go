package enforcement

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// claimWaiter represents a waiter for a specific QuotaClaim
type claimWaiter struct {
	claimName  string
	namespace  string
	resultChan chan ClaimResult
	timeout    time.Duration
	cancelFunc context.CancelFunc
	timer      *time.Timer
	startTime  time.Time
}

// watchManager implements ClaimWatchManager using direct watch streams with efficient,
// stateless operation optimized for admission plugin requirements.
type watchManager struct {
	dynamicClient dynamic.Interface
	logger        logr.Logger
	config        *WatchManagerConfig
	scopeID       string // Scope identifier (empty string for root scope)

	// Watch stream state
	watchLock      sync.RWMutex
	watchInterface watch.Interface
	watchCtx       context.Context
	watchCancel    context.CancelFunc

	// Resumption state. resumeRV is the resource version the next watch
	// starts from: set by a list, then advanced by every event and bookmark
	// of a stream started from it, so it always follows commit order. Empty
	// means "relist first" (startup and 410 Gone).
	resumeRV              string
	lastBookmarkTimestamp time.Time
	bookmarkLock          sync.RWMutex

	// Stream metrics tracking
	streamStartTime time.Time
	streamLock      sync.RWMutex

	// Waiters management
	waitersLock sync.RWMutex
	waiters     map[types.NamespacedName]*claimWaiter

	// TTL management
	ttlMu         sync.Mutex
	ttlTimer      *time.Timer
	activeWaiters int32 // Atomic counter

	// Lifecycle
	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	started   atomic.Bool

	// Callback for TTL expiration (to remove from parent cache)
	onTTLExpired func()
}

// NewWatchManager creates a new watch manager with TTL-based lifecycle management
func NewWatchManager(dynamicClient dynamic.Interface, logger logr.Logger, scopeID string) ClaimWatchManager {
	return newWatchManagerWithConfig(dynamicClient, logger, scopeID, nil)
}

// newWatchManagerWithConfig creates a watch manager with the given config
// (DefaultWatchManagerConfig when nil).
func newWatchManagerWithConfig(dynamicClient dynamic.Interface, logger logr.Logger, scopeID string, config *WatchManagerConfig) *watchManager {
	if config == nil {
		config = DefaultWatchManagerConfig()
	}

	return &watchManager{
		dynamicClient: dynamicClient,
		logger:        logger,
		config:        config,
		scopeID:       scopeID,
		waiters:       make(map[types.NamespacedName]*claimWaiter),
		stopCh:        make(chan struct{}),
	}
}

// SetTTLExpiredCallback sets the callback function to be called when TTL expires
func (w *watchManager) SetTTLExpiredCallback(callback func()) {
	w.onTTLExpired = callback
}

// Start initializes the watch manager. It reads the current resource version
// with a one-item list and watches from there, so no initial state is
// replayed.
func (w *watchManager) Start(ctx context.Context) error {
	var startErr error
	w.startOnce.Do(func() {
		w.logger.Info("Starting watch manager",
			"scope", w.scopeID,
			"ttl", w.config.TTL.DefaultTTL)

		watchManagersCreated.Inc()
		watchManagersActive.Inc()
		watchStreamsDesired.Inc()

		gvr := schema.GroupVersionResource{
			Group:    quotav1alpha1.SchemeGroupVersion.Group,
			Version:  quotav1alpha1.SchemeGroupVersion.Version,
			Resource: "quotaclaims",
		}

		w.bookmarkLock.Lock()
		w.resumeRV = ""
		w.bookmarkLock.Unlock()

		w.watchCtx, w.watchCancel = context.WithCancel(context.Background())

		// Use channel to wait for watch to actually start
		watchStarted := make(chan error, 1)
		go w.watchLoop(w.watchCtx, gvr, watchStarted)

		// Wait for watch to start or fail, respecting the caller's context timeout
		select {
		case err := <-watchStarted:
			if err != nil {
				startErr = fmt.Errorf("failed to start watch: %w", err)
				w.logger.Error(err, "Failed to start watch manager")
				watchStreamsDesired.Dec()
				return
			}
		case <-ctx.Done():
			startErr = fmt.Errorf("watch startup cancelled: %w", ctx.Err())
			w.logger.Error(startErr, "Watch manager startup cancelled")
			watchStreamsDesired.Dec()
			w.watchCancel()
			return
		}

		w.started.Store(true)
		w.logger.Info("Watch manager started",
			"scope", w.scopeID)
	})

	return startErr
}

func (w *watchManager) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stopping watch manager", "scope", w.scopeID)

		w.ttlMu.Lock()
		if w.ttlTimer != nil {
			w.ttlTimer.Stop()
			w.ttlTimer = nil
		}
		w.ttlMu.Unlock()

		if w.watchCancel != nil {
			w.watchCancel()
		}

		close(w.stopCh)

		w.watchLock.Lock()
		if w.watchInterface != nil {
			w.watchInterface.Stop()
			w.watchInterface = nil
		}
		w.watchLock.Unlock()

		w.waitersLock.Lock()
		for key, waiter := range w.waiters {
			if waiter.timer != nil {
				waiter.timer.Stop()
			}
			waiter.cancelFunc()
			close(waiter.resultChan)
			delete(w.waiters, key)
		}
		w.waitersLock.Unlock()

		watchManagersStopped.Inc()
		watchManagersActive.Dec()
		watchStreamsDesired.Dec()

		w.started.Store(false)
		w.logger.Info("Watch manager stopped", "scope", w.scopeID)
	})
}

// RegisterClaimWaiter registers a waiter for a specific QuotaClaim.
func (w *watchManager) RegisterClaimWaiter(ctx context.Context, claimName, namespace string, timeout time.Duration) (<-chan ClaimResult, context.CancelFunc, error) {
	if !w.started.Load() {
		return nil, nil, fmt.Errorf("watch manager not started")
	}

	key := types.NamespacedName{Namespace: namespace, Name: claimName}

	w.logger.V(4).Info("Registering claim waiter",
		"claimName", claimName,
		"namespace", namespace,
		"timeout", timeout,
		"scope", w.scopeID)

	// Check if we've reached the maximum number of waiters
	w.waitersLock.RLock()
	if w.config.MaxWaiters > 0 && len(w.waiters) >= w.config.MaxWaiters {
		w.waitersLock.RUnlock()
		return nil, nil, fmt.Errorf("maximum number of waiters (%d) reached", w.config.MaxWaiters)
	}
	w.waitersLock.RUnlock()

	// Create waiter context for cancellation
	_, cancelFunc := context.WithCancel(ctx)

	resultChan := make(chan ClaimResult, 1)

	waiter := &claimWaiter{
		claimName:  claimName,
		namespace:  namespace,
		resultChan: resultChan,
		timeout:    timeout,
		cancelFunc: cancelFunc,
		timer:      nil,
		startTime:  time.Now(),
	}

	// Register the waiter BEFORE checking for existing claims
	w.waitersLock.Lock()
	w.waiters[key] = waiter
	w.waitersLock.Unlock()

	// Increment active waiter count and update TTL
	activeCount := atomic.AddInt32(&w.activeWaiters, 1)
	w.updateTTL()

	waiterRegistrations.Inc()
	waitersCurrent.Set(float64(activeCount))

	// Return a cancel function that cleans up the waiter
	cancelWithCleanup := func() {
		cancelFunc()
		w.UnregisterClaimWaiter(claimName, namespace)
	}

	// Start the timeout timer
	waiter.timer = time.AfterFunc(timeout, func() {
		w.logger.V(3).Info("Claim waiter timed out",
			"claimName", claimName,
			"namespace", namespace,
			"timeout", timeout,
			"scope", w.scopeID)

		waiterDuration.WithLabelValues("timeout").Observe(time.Since(waiter.startTime).Seconds())

		w.deliver(key, waiter, ClaimResult{
			Granted: false,
			Reason:  "timeout",
			Error:   fmt.Errorf("timeout waiting for QuotaClaim %s/%s after %v", namespace, claimName, timeout),
		})

		w.UnregisterClaimWaiter(claimName, namespace)
	})

	w.logger.V(4).Info("Claim waiter registered successfully",
		"claimName", claimName,
		"namespace", namespace,
		"scope", w.scopeID)

	return resultChan, cancelWithCleanup, nil
}

// UnregisterClaimWaiter unregisters a waiter for a specific QuotaClaim
func (w *watchManager) UnregisterClaimWaiter(claimName, namespace string) {
	key := types.NamespacedName{Namespace: namespace, Name: claimName}

	w.waitersLock.Lock()
	defer w.waitersLock.Unlock()

	if waiter, exists := w.waiters[key]; exists {
		w.logger.V(4).Info("Unregistering claim waiter",
			"claimName", claimName,
			"namespace", namespace,
			"scope", w.scopeID)

		if waiter.timer != nil {
			waiter.timer.Stop()
		}
		waiter.cancelFunc()
		close(waiter.resultChan)
		delete(w.waiters, key)

		activeCount := atomic.AddInt32(&w.activeWaiters, -1)
		w.updateTTL()

		waiterCompletions.WithLabelValues("unregistered").Inc()
		waitersCurrent.Set(float64(activeCount))

		w.logger.V(4).Info("Claim waiter unregistered",
			"claimName", claimName,
			"namespace", namespace,
			"scope", w.scopeID)
	}
}

// updateTTL manages the TTL timer based on active waiter count
func (w *watchManager) updateTTL() {
	w.ttlMu.Lock()
	defer w.ttlMu.Unlock()

	activeCount := atomic.LoadInt32(&w.activeWaiters)

	if activeCount > 0 {
		if w.ttlTimer != nil {
			w.ttlTimer.Stop()
			w.ttlTimer = nil
		}

		w.logger.V(5).Info("TTL paused due to active waiters",
			"scope", w.scopeID)
	} else {
		if w.ttlTimer != nil {
			w.ttlTimer.Stop()
		}

		ttlDuration := w.config.TTL.DefaultTTL
		w.ttlTimer = time.AfterFunc(ttlDuration, func() {
			w.logger.Info("TTL expired, stopping watch manager",
				"ttl", ttlDuration,
				"scope", w.scopeID)

			ttlExpirations.Inc()

			w.Stop()

			if w.onTTLExpired != nil {
				w.onTTLExpired()
			}
		})

		ttlResets.Inc()

		w.logger.V(5).Info("TTL started/reset",
			"scope", w.scopeID)
	}
}

// watchLoop implements infinite retry with exponential backoff
func (w *watchManager) watchLoop(ctx context.Context, gvr schema.GroupVersionResource, watchStarted chan<- error) {
	backoff := w.config.Retry.InitialDelay
	firstAttempt := true

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Watch loop context cancelled", "scope", w.scopeID)
			if firstAttempt {
				watchStarted <- fmt.Errorf("watch loop cancelled before starting")
			}
			return
		case <-w.stopCh:
			w.logger.Info("Watch loop stopped", "scope", w.scopeID)
			if firstAttempt {
				watchStarted <- fmt.Errorf("watch loop stopped before starting")
			}
			return
		default:
		}

		restartStart := time.Now()
		var startedChan chan<- error
		if firstAttempt {
			startedChan = watchStarted
		}
		err := w.processWatchStream(ctx, gvr, startedChan)
		restartDuration := time.Since(restartStart)

		if firstAttempt {
			firstAttempt = false
		}

		if err == nil {
			backoff = w.config.Retry.InitialDelay
			continue
		}

		if ctx.Err() != nil {
			w.logger.Info("Watch loop stopping due to context cancellation",
				"scope", w.scopeID,
				"error", err)
			return
		}

		// 410 Gone: resourceVersion too old. Relist from scratch; the relist
		// re-reads every waiter's claim so decisions made in the gap are
		// not lost.
		if errors.IsResourceExpired(err) || errors.IsGone(err) {
			w.logger.Info("ResourceVersion too old (410 Gone), relisting",
				"scope", w.scopeID)

			w.bookmarkLock.Lock()
			w.resumeRV = ""
			w.bookmarkLock.Unlock()

			watchRestarts.WithLabelValues("410").Inc()
			continue
		}

		jitteredBackoff := w.applyJitter(backoff)

		statusCode := "unknown"
		if statusErr, ok := err.(*errors.StatusError); ok {
			statusCode = fmt.Sprintf("%d", statusErr.Status().Code)
		}

		w.logger.Info("Watch stream failed, retrying",
			"error", err,
			"scope", w.scopeID)

		watchRestarts.WithLabelValues(statusCode).Inc()
		watchRestartDuration.Observe(restartDuration.Seconds())

		select {
		case <-time.After(jitteredBackoff):
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		}

		backoff = time.Duration(float64(backoff) * w.config.Retry.Multiplier)
		if backoff > w.config.Retry.MaxDelay {
			backoff = w.config.Retry.MaxDelay
		}
	}
}

// processWatchStream establishes and processes a single watch stream.
func (w *watchManager) processWatchStream(ctx context.Context, gvr schema.GroupVersionResource, watchStarted chan<- error) error {
	w.bookmarkLock.RLock()
	resumeRV := w.resumeRV
	w.bookmarkLock.RUnlock()

	// Never watch with an empty resourceVersion: that replays every claim
	// as ADDED events in key order, and a stream that ends partway through
	// that replay leaves no resource version that is safe to resume from.
	// Read the current resource version instead and watch from it.
	if resumeRV == "" {
		rv, err := w.relist(ctx, gvr)
		if err != nil {
			if watchStarted != nil {
				watchStarted <- fmt.Errorf("failed to list QuotaClaims: %w", err)
			}
			return fmt.Errorf("failed to list QuotaClaims: %w", err)
		}
		resumeRV = rv
	}

	watchOpts := metav1.ListOptions{
		Watch:               true,
		AllowWatchBookmarks: true,
		ResourceVersion:     resumeRV,
	}
	w.logger.V(4).Info("Starting watch", "resourceVersion", resumeRV, "scope", w.scopeID)

	watchInterface, err := w.dynamicClient.Resource(gvr).Watch(ctx, watchOpts)
	if err != nil {
		if watchStarted != nil {
			watchStarted <- fmt.Errorf("failed to start watch: %w", err)
		}
		return fmt.Errorf("failed to start watch: %w", err)
	}

	streamStartTime := time.Now()
	w.watchLock.Lock()
	w.watchInterface = watchInterface
	w.watchLock.Unlock()

	w.streamLock.Lock()
	w.streamStartTime = streamStartTime
	w.streamLock.Unlock()

	if watchStarted != nil {
		watchStarted <- nil
	}

	defer func() {
		lifetime := time.Since(streamStartTime)
		reason := "error"
		if ctx.Err() != nil {
			reason = "shutdown"
		}

		watchStreamLifetimeSeconds.WithLabelValues(reason).Observe(lifetime.Seconds())
		watchStreamsConnected.Dec()

		w.watchLock.Lock()
		if w.watchInterface != nil {
			w.watchInterface.Stop()
			w.watchInterface = nil
		}
		w.watchLock.Unlock()
	}()

	watchStreamsConnected.Inc()

	metricsCtx, metricsCancel := context.WithCancel(ctx)
	defer metricsCancel()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				uptime := time.Since(streamStartTime)
				watchStreamMaxUptimeSeconds.Set(uptime.Seconds())

				w.bookmarkLock.RLock()
				lastUpdate := w.lastBookmarkTimestamp
				w.bookmarkLock.RUnlock()

				if !lastUpdate.IsZero() {
					age := time.Since(lastUpdate)
					watchBookmarkMaxAgeSeconds.Set(age.Seconds())
				}
			case <-metricsCtx.Done():
				return
			}
		}
	}()

	w.logger.V(3).Info("Watch stream established",
		"resourceVersion", watchOpts.ResourceVersion,
		"scope", w.scopeID)

	for {
		select {
		case event, ok := <-watchInterface.ResultChan():
			if !ok {
				w.logger.V(3).Info("Watch channel closed", "scope", w.scopeID)
				return fmt.Errorf("watch channel closed")
			}

			if err := w.handleWatchEvent(event); err != nil {
				return err
			}

		case <-ctx.Done():
			w.logger.V(3).Info("Watch context cancelled", "scope", w.scopeID)
			return ctx.Err()

		case <-w.stopCh:
			w.logger.V(3).Info("Watch stopped via stop channel", "scope", w.scopeID)
			return nil
		}
	}
}

// handleWatchEvent processes a single watch event
func (w *watchManager) handleWatchEvent(event watch.Event) error {
	switch event.Type {
	case watch.Added, watch.Modified:
		w.handleClaimEvent(event.Object)
		w.advance(event.Object)
		watchEventsReceived.WithLabelValues(string(event.Type)).Inc()

	case watch.Deleted:
		w.handleClaimDeletion(event.Object)
		w.advance(event.Object)
		watchEventsReceived.WithLabelValues("delete").Inc()

	case watch.Bookmark:
		if rv := w.extractResourceVersion(event.Object); rv != "" {
			now := time.Now()
			w.bookmarkLock.Lock()
			w.resumeRV = rv
			w.lastBookmarkTimestamp = now
			w.bookmarkLock.Unlock()

			w.logger.V(5).Info("Bookmark updated",
				"resourceVersion", rv,
				"scope", w.scopeID)
		}
		watchEventsReceived.WithLabelValues("bookmark").Inc()

	case watch.Error:
		return w.extractErrorFromEvent(event)
	}

	return nil
}

// advance records the resource version of an event so a dropped stream
// resumes after it.
func (w *watchManager) advance(obj interface{}) {
	if rv := w.extractResourceVersion(obj); rv != "" {
		w.bookmarkLock.Lock()
		w.resumeRV = rv
		w.bookmarkLock.Unlock()
	}
}

// relist reads the collection's current resource version (a one-item list
// is enough) and re-reads the claim of every registered waiter, so a claim
// decided while no stream was open still reaches its waiter. It returns the
// resource version to watch from.
func (w *watchManager) relist(ctx context.Context, gvr schema.GroupVersionResource) (string, error) {
	list, err := w.dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", err
	}
	rv := list.GetResourceVersion()
	if rv == "" {
		return "", fmt.Errorf("list returned no resourceVersion")
	}

	w.waitersLock.RLock()
	keys := make([]types.NamespacedName, 0, len(w.waiters))
	for key := range w.waiters {
		keys = append(keys, key)
	}
	w.waitersLock.RUnlock()
	for _, key := range keys {
		claim, err := w.dynamicClient.Resource(gvr).Namespace(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				w.logger.V(2).Info("Failed to re-read claim after relist", "claim", key, "error", err)
			}
			continue
		}
		w.handleClaimEvent(claim)
	}

	w.bookmarkLock.Lock()
	w.resumeRV = rv
	w.bookmarkLock.Unlock()
	watchRelists.Inc()
	return rv, nil
}

// handleClaimEvent processes a QuotaClaim event (Added or Modified)
func (w *watchManager) handleClaimEvent(obj interface{}) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		w.logger.Error(nil, "Received non-unstructured object in claim event handler")
		watchEventsProcessed.WithLabelValues("error").Inc()
		return
	}

	key := types.NamespacedName{
		Name:      unstructuredObj.GetName(),
		Namespace: unstructuredObj.GetNamespace(),
	}

	w.waitersLock.RLock()
	waiter, exists := w.waiters[key]
	w.waitersLock.RUnlock()

	if !exists || predatesWaiter(unstructuredObj, waiter) {
		watchEventsProcessed.WithLabelValues("ignored").Inc()
		return
	}

	if result := w.evaluateClaimStatus(unstructuredObj); result != nil {
		outcome := "granted"
		if !result.Granted {
			outcome = "denied"
		}

		waiterDuration.WithLabelValues(outcome).Observe(time.Since(waiter.startTime).Seconds())
		watchEventsProcessed.WithLabelValues(outcome).Inc()

		w.deliver(key, waiter, *result)

		w.logger.V(3).Info("Claim result sent to waiter",
			"claimName", key.Name,
			"namespace", key.Namespace,
			"granted", result.Granted,
			"reason", result.Reason,
			"scope", w.scopeID)

		w.UnregisterClaimWaiter(key.Name, key.Namespace)
	} else {
		watchEventsProcessed.WithLabelValues("pending").Inc()
	}
}

// handleClaimDeletion processes a QuotaClaim deletion event
func (w *watchManager) handleClaimDeletion(obj interface{}) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		w.logger.Error(nil, "Received non-unstructured object in deletion handler")
		return
	}

	key := types.NamespacedName{
		Name:      unstructuredObj.GetName(),
		Namespace: unstructuredObj.GetNamespace(),
	}

	w.waitersLock.RLock()
	waiter, exists := w.waiters[key]
	w.waitersLock.RUnlock()

	// A stale claim from an earlier admission attempt may be deleted just
	// before this attempt's waiter registers; its DELETED event must not
	// fail the new attempt.
	if !exists || predatesWaiter(unstructuredObj, waiter) {
		return
	}

	waiterDuration.WithLabelValues("deleted").Observe(time.Since(waiter.startTime).Seconds())

	w.deliver(key, waiter, ClaimResult{
		Granted: false,
		Reason:  "deleted",
		Error:   fmt.Errorf("QuotaClaim %s/%s was deleted", key.Namespace, key.Name),
	})

	w.UnregisterClaimWaiter(key.Name, key.Namespace)
}

// evaluateClaimStatus evaluates a QuotaClaim's status and returns a result if final
func (w *watchManager) evaluateClaimStatus(claim *unstructured.Unstructured) *ClaimResult {
	return ClaimResultFromObject(claim)
}

// deliver hands a result to a waiter without blocking and without racing
// UnregisterClaimWaiter, which closes the channel under the same lock. It is
// a no-op if the waiter was already replaced or removed, or already holds a
// result.
func (w *watchManager) deliver(key types.NamespacedName, waiter *claimWaiter, result ClaimResult) {
	w.waitersLock.Lock()
	defer w.waitersLock.Unlock()
	if current, ok := w.waiters[key]; !ok || current != waiter {
		return
	}
	select {
	case waiter.resultChan <- result:
	default:
	}
}

// predatesWaiter reports whether obj is a claim that already existed before
// the waiter registered (creationTimestamp has one-second resolution, so the
// comparison is against the start of the registration second). Such an
// object belongs to an earlier admission attempt for the same name.
func predatesWaiter(obj *unstructured.Unstructured, waiter *claimWaiter) bool {
	created := obj.GetCreationTimestamp()
	if created.IsZero() {
		return false
	}
	return created.Time.Before(waiter.startTime.Truncate(time.Second))
}

// applyJitter applies random jitter to backoff duration
func (w *watchManager) applyJitter(duration time.Duration) time.Duration {
	jitter := w.config.Retry.Jitter
	if jitter <= 0 {
		return duration
	}

	jitterAmount := float64(duration) * jitter
	randomJitter := (rand.Float64()*2 - 1) * jitterAmount

	jittered := time.Duration(float64(duration) + randomJitter)
	if jittered < 0 {
		jittered = duration
	}

	return jittered
}

// extractResourceVersion extracts resourceVersion from an object
func (w *watchManager) extractResourceVersion(obj interface{}) string {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return ""
	}

	return unstructuredObj.GetResourceVersion()
}

// extractErrorFromEvent extracts error from a watch error event
func (w *watchManager) extractErrorFromEvent(event watch.Event) error {
	if event.Type != watch.Error {
		return nil
	}

	if statusErr, ok := event.Object.(*metav1.Status); ok {
		return &errors.StatusError{ErrStatus: *statusErr}
	}

	return fmt.Errorf("unknown error event: %v", event.Object)
}
