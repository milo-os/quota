package dual

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

const (
	opCreate   = "create"
	opUpdate   = "update"
	opDelete   = "delete"
	opBackfill = "backfill"
	opRepair   = "repair"

	sourceWrite     = "write"
	sourceReconcile = "reconcile"

	// DefaultShadowWriteTimeout bounds each shadow write.
	DefaultShadowWriteTimeout = 10 * time.Second
)

// Option configures a Store.
type Option func(*Store)

// WithResource sets the resource label used in logs and metrics.
func WithResource(resource string) Option {
	return func(s *Store) { s.resource = resource }
}

// WithShadowWriteTimeout overrides DefaultShadowWriteTimeout.
func WithShadowWriteTimeout(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.shadowTimeout = d
		}
	}
}

// Store wraps two storage.Interface implementations: primary (etcd, source of
// truth) and shadow (Postgres). See the package documentation for semantics.
type Store struct {
	primary       storage.Interface
	shadow        storage.Interface
	resource      string
	shadowTimeout time.Duration
	seq           *sequencer
}

// New creates a new dual-write Store.
func New(primary, shadow storage.Interface, opts ...Option) *Store {
	s := &Store{
		primary:       primary,
		shadow:        shadow,
		resource:      "unknown",
		shadowTimeout: DefaultShadowWriteTimeout,
		seq:           newSequencer(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Versioner delegates to the primary.
func (s *Store) Versioner() storage.Versioner {
	return s.primary.Versioner()
}

// Create writes to the primary, then mirrors the primary's result to the shadow.
func (s *Store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if err := s.primary.Create(ctx, key, obj, out, ttl); err != nil {
		return err
	}
	s.mirrorWrite(ctx, key, out, opCreate)
	return nil
}

// Delete deletes from the primary, then deletes the key from the shadow
// without preconditions. The primary has already enforced preconditions and
// validateDeletion, and the shadow's RVs differ, so replaying them would fail.
func (s *Store) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if err := s.primary.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts); err != nil {
		return err
	}
	s.mirrorDelete(ctx, key, out, opts)
	return nil
}

// GuaranteedUpdate runs tryUpdate against the primary only, then upserts the
// resulting object into the shadow. The closure is never replayed: it carries
// registry state (creating, deleteObj), strategy hooks and RV checks that are
// only valid against the primary.
func (s *Store) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	if err := s.primary.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject); err != nil {
		return err
	}
	s.mirrorWrite(ctx, key, destination, opUpdate)
	return nil
}

// Watch delegates to the primary only.
func (s *Store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.primary.Watch(ctx, key, opts)
}

// Get reads from the primary.
func (s *Store) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	return s.primary.Get(ctx, key, opts, objPtr)
}

// GetList reads from the primary.
func (s *Store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return s.primary.GetList(ctx, key, opts, listObj)
}

// Stats delegates to the primary.
func (s *Store) Stats(ctx context.Context) (storage.Stats, error) {
	return s.primary.Stats(ctx)
}

// GetCurrentResourceVersion delegates to the primary.
func (s *Store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	return s.primary.GetCurrentResourceVersion(ctx)
}

// EnableResourceSizeEstimation delegates to the primary.
func (s *Store) EnableResourceSizeEstimation(f storage.KeysFunc) error {
	return s.primary.EnableResourceSizeEstimation(f)
}

// CompactRevision delegates to the primary.
func (s *Store) CompactRevision() int64 {
	return s.primary.CompactRevision()
}

// ReadinessCheck returns the primary's readiness. Shadow health is logged but
// does not affect the result.
func (s *Store) ReadinessCheck() error {
	if err := s.shadow.ReadinessCheck(); err != nil {
		klog.ErrorS(err, "Shadow storage readiness check failed", "resource", s.resource)
	}
	return s.primary.ReadinessCheck()
}

// RequestWatchProgress delegates to the primary.
func (s *Store) RequestWatchProgress(ctx context.Context) error {
	return s.primary.RequestWatchProgress(ctx)
}

// shadowContext detaches the shadow write from request cancellation. The
// primary has already committed, so abandoning the shadow write because the
// client went away would only create divergence.
func (s *Store) shadowContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.shadowTimeout)
}

// primaryRV returns the primary RV of obj, or 0 if it can't be read.
func (s *Store) primaryRV(obj runtime.Object) uint64 {
	rv, err := s.primary.Versioner().ObjectResourceVersion(obj)
	if err != nil {
		return 0
	}
	return rv
}

// mirrorWrite upserts the object the primary produced into the shadow,
// unless a newer primary state for key has already been mirrored.
func (s *Store) mirrorWrite(ctx context.Context, key string, primaryObj runtime.Object, op string) {
	ctx, cancel := s.shadowContext(ctx)
	defer cancel()

	rv := s.primaryRV(primaryObj)
	ks := s.seq.acquire(key)
	defer s.seq.release(ks)
	if ks.stale(rv) {
		shadowWritesSkipped.WithLabelValues(s.resource).Inc()
		klog.V(4).InfoS("Skipping stale shadow write", "resource", s.resource, "key", key, "primaryRV", rv, "lastMirroredRV", ks.lastRV)
		return
	}

	var err error
	if op == opCreate {
		err = s.shadowCreate(ctx, key, primaryObj)
	} else {
		var backfilled bool
		backfilled, err = s.shadowUpsert(ctx, key, primaryObj)
		if err == nil && backfilled {
			shadowBackfills.WithLabelValues(s.resource, sourceWrite).Inc()
			klog.V(2).InfoS("Backfilled object missing from shadow on write", "resource", s.resource, "key", key)
		}
	}
	if err != nil {
		shadowWriteFailures.WithLabelValues(s.resource, op).Inc()
		klog.ErrorS(err, "Shadow storage write failed", "resource", s.resource, "operation", op, "key", key)
		return
	}
	ks.recordWrite(rv)
}

// mirrorDelete removes key from the shadow and records a tombstone so a
// late, older write cannot bring the object back.
func (s *Store) mirrorDelete(ctx context.Context, key string, primaryOut runtime.Object, opts storage.DeleteOptions) {
	ctx, cancel := s.shadowContext(ctx)
	defer cancel()

	rv := s.primaryRV(primaryOut)
	ks := s.seq.acquire(key)
	defer s.seq.release(ks)

	if err := s.shadowDelete(ctx, key, primaryOut, opts); err != nil {
		shadowWriteFailures.WithLabelValues(s.resource, opDelete).Inc()
		klog.ErrorS(err, "Shadow storage delete failed", "resource", s.resource, "key", key)
		return
	}
	ks.recordDelete(rv)
}

// shadowCreate inserts the primary's object into the shadow. If the key
// already exists there (a stale row from an earlier run or a missed delete),
// it falls back to an upsert so the shadow converges on the primary.
func (s *Store) shadowCreate(ctx context.Context, key string, primaryObj runtime.Object) error {
	in := shadowCopy(primaryObj)
	out := primaryObj.DeepCopyObject()
	err := s.shadow.Create(ctx, key, in, out, 0)
	if err == nil || !storage.IsExist(err) {
		return err
	}
	klog.V(2).InfoS("Shadow already had key on create; overwriting with primary state", "resource", s.resource, "key", key)
	_, err = s.shadowUpsert(ctx, key, primaryObj)
	return err
}

// shadowUpsert writes primaryObj to the shadow at key. It overwrites whatever
// is there, or inserts the object if it is missing. The update closure is
// pure and ignores the shadow's current state, so the shadow's own retries
// are safe. It reports whether the object was missing (backfilled).
func (s *Store) shadowUpsert(ctx context.Context, key string, primaryObj runtime.Object) (bool, error) {
	desired := shadowCopy(primaryObj)
	dest := primaryObj.DeepCopyObject()
	shadowVersioner := s.shadow.Versioner()
	backfilled := false
	err := s.shadow.GuaranteedUpdate(ctx, key, dest, true, nil,
		func(_ runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			out := desired.DeepCopyObject()
			// Translate the RV: give the desired object the shadow's current RV
			// so an unchanged object encodes byte-identically and the shadow
			// can skip the write as a no-op.
			if res.ResourceVersion > 0 {
				_ = shadowVersioner.UpdateObject(out, res.ResourceVersion)
				backfilled = false
			} else {
				backfilled = true
			}
			return out, nil, nil
		}, nil)
	if err != nil && storage.IsNotFound(err) {
		// A backend that doesn't honor ignoreNotFound: insert directly.
		out := primaryObj.DeepCopyObject()
		if err = s.shadow.Create(ctx, key, shadowCopy(primaryObj), out, 0); err == nil {
			return true, nil
		}
	}
	return backfilled, err
}

// shadowDelete removes key from the shadow unconditionally. A missing key
// counts as success because the desired end state already holds.
func (s *Store) shadowDelete(ctx context.Context, key string, primaryOut runtime.Object, opts storage.DeleteOptions) error {
	out := primaryOut.DeepCopyObject()
	err := s.shadow.Delete(ctx, key, out, nil, nil, nil, opts)
	if err != nil && storage.IsNotFound(err) {
		return nil
	}
	return err
}

// shadowCopy returns a deep copy of a primary object with its resourceVersion
// cleared. Primary RVs mean nothing to the shadow, which assigns its own.
func shadowCopy(obj runtime.Object) runtime.Object {
	c := obj.DeepCopyObject()
	// Objects without ObjectMeta (only seen in tests) can't carry an RV.
	_ = storage.APIObjectVersioner{}.UpdateObject(c, 0)
	return c
}
