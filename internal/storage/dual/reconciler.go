package dual

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

// ReconcilerOptions configures the background primary/shadow reconciler.
type ReconcilerOptions struct {
	// Interval between reconcile passes. Zero disables the reconciler.
	Interval time.Duration
	// InitialDelay before the first pass, so it doesn't compete with
	// apiserver startup. A random jitter of up to InitialDelay is added
	// so resources don't all reconcile at once.
	InitialDelay time.Duration
	// QPS and Burst rate-limit shadow repair writes (backfills, drift
	// repair, orphan deletes) so a large backfill can't flood Postgres.
	QPS   float32
	Burst int
	// PageSize is the LIST page size used against the primary.
	PageSize int64
	// RepairDivergence also overwrites drifted shadow rows with the primary
	// state and deletes shadow rows that don't exist in the primary.
	// Missing rows are always backfilled.
	RepairDivergence bool
}

// DefaultReconcilerOptions returns conservative defaults.
func DefaultReconcilerOptions() ReconcilerOptions {
	return ReconcilerOptions{
		Interval:     5 * time.Minute,
		InitialDelay: 30 * time.Second,
		QPS:          20,
		Burst:        20,
		PageSize:     500,
	}
}

// reconciler compares one resource's primary and shadow contents and
// backfills or repairs the shadow.
type reconciler struct {
	store       *Store
	prefix      string
	keyFunc     func(runtime.Object) (string, error)
	newListFunc func() runtime.Object
	getAttrs    storage.AttrFunc
	opts        ReconcilerOptions
	limiter     flowcontrol.RateLimiter
}

func newReconciler(store *Store, prefix string, keyFunc func(runtime.Object) (string, error), newListFunc func() runtime.Object, getAttrs storage.AttrFunc, opts ReconcilerOptions) *reconciler {
	if opts.PageSize <= 0 {
		opts.PageSize = 500
	}
	if opts.QPS <= 0 {
		opts.QPS = 20
	}
	if opts.Burst <= 0 {
		opts.Burst = 1
	}
	return &reconciler{
		store:       store,
		prefix:      prefix,
		keyFunc:     keyFunc,
		newListFunc: newListFunc,
		getAttrs:    getAttrs,
		opts:        opts,
		limiter:     flowcontrol.NewTokenBucketRateLimiter(opts.QPS, opts.Burst),
	}
}

// reconcileResult summarizes one pass.
type reconcileResult struct {
	primaryCount int
	shadowCount  int
	// Divergence that remained after the pass's backfill/repair.
	missing, orphaned, drifted int
	backfilled, repaired       int
}

// run blocks, reconciling every Interval until ctx is done.
func (r *reconciler) run(ctx context.Context) {
	if r.opts.Interval <= 0 {
		return
	}
	delay := r.opts.InitialDelay
	if delay > 0 {
		delay += time.Duration(rand.Int64N(int64(delay)))
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}
	wait.JitterUntilWithContext(ctx, func(ctx context.Context) {
		if _, err := r.reconcileOnce(ctx); err != nil {
			reconcileErrors.WithLabelValues(r.store.resource).Inc()
			klog.ErrorS(err, "Dual storage reconcile pass failed", "resource", r.store.resource)
		}
	}, r.opts.Interval, 0.1, true)
}

func (r *reconciler) reconcileOnce(ctx context.Context) (reconcileResult, error) {
	var res reconcileResult
	start := time.Now()
	resource := r.store.resource

	primaryObjs, err := r.listAll(ctx, r.store.primary, r.opts.PageSize)
	if err != nil {
		return res, fmt.Errorf("listing primary: %w", err)
	}
	// The shadow isn't paged. The Postgres store ignores Limit anyway.
	shadowObjs, err := r.listAll(ctx, r.store.shadow, 0)
	if err != nil {
		return res, fmt.Errorf("listing shadow: %w", err)
	}
	res.primaryCount, res.shadowCount = len(primaryObjs), len(shadowObjs)

	// The two lists aren't taken at the same instant, so every candidate is
	// re-read from both sides before it counts as divergent. That keeps
	// in-flight writes from showing up as false positives.
	var candidates []string
	for key, pObj := range primaryObjs {
		sObj, ok := shadowObjs[key]
		if !ok || objectHash(pObj) != objectHash(sObj) {
			candidates = append(candidates, key)
		}
	}
	for key := range shadowObjs {
		if _, ok := primaryObjs[key]; !ok {
			candidates = append(candidates, key)
		}
	}

	for i, key := range candidates {
		// Rate-limit before taking the key lock so a throttled reconcile
		// never holds up the write path for that key.
		if err := r.limiter.Wait(ctx); err != nil {
			// Count the unresolved candidates as divergent before bailing.
			res.missing += len(candidates) - i
			return res, err
		}
		r.resolve(ctx, key, &res)
	}

	divergentObjects.WithLabelValues(resource, reasonMissing).Set(float64(res.missing))
	divergentObjects.WithLabelValues(resource, reasonOrphaned).Set(float64(res.orphaned))
	divergentObjects.WithLabelValues(resource, reasonDrifted).Set(float64(res.drifted))
	lastReconcileTimestamp.WithLabelValues(resource).Set(float64(time.Now().Unix()))
	r.store.seq.prune()

	logLevel := klog.Level(4)
	if res.missing+res.orphaned+res.drifted+res.backfilled+res.repaired > 0 {
		logLevel = 2
	}
	klog.V(logLevel).InfoS("Dual storage reconcile pass complete",
		"resource", resource,
		"primaryObjects", res.primaryCount,
		"shadowObjects", res.shadowCount,
		"backfilled", res.backfilled,
		"repaired", res.repaired,
		"missing", res.missing,
		"orphaned", res.orphaned,
		"drifted", res.drifted,
		"duration", time.Since(start),
	)
	return res, nil
}

// resolve re-reads key from both backends, classifies it, and backfills or
// repairs it if allowed. It runs under the key's sequencer lock so it can't
// race the write path within this process.
func (r *reconciler) resolve(ctx context.Context, key string, res *reconcileResult) {
	resource := r.store.resource
	ks := r.store.seq.acquire(key)
	defer r.store.seq.release(ks)

	pObj, pFound, err := r.get(ctx, r.store.primary, key)
	if err != nil {
		klog.V(2).InfoS("Reconcile: failed to read primary", "resource", resource, "key", key, "err", err)
		return
	}
	sObj, sFound, err := r.get(ctx, r.store.shadow, key)
	if err != nil {
		klog.V(2).InfoS("Reconcile: failed to read shadow", "resource", resource, "key", key, "err", err)
		return
	}

	switch {
	case !pFound && !sFound:
		return
	case pFound && !sFound:
		if ks.stale(r.store.primaryRV(pObj)) {
			return
		}
		if _, err := r.store.shadowUpsert(ctx, key, pObj); err != nil {
			shadowWriteFailures.WithLabelValues(resource, opBackfill).Inc()
			klog.ErrorS(err, "Reconcile: shadow backfill failed", "resource", resource, "key", key)
			res.missing++
			return
		}
		ks.recordWrite(r.store.primaryRV(pObj))
		shadowBackfills.WithLabelValues(resource, sourceReconcile).Inc()
		klog.V(2).InfoS("Reconcile: backfilled object missing from shadow", "resource", resource, "key", key)
		res.backfilled++
	case !pFound && sFound:
		if !r.opts.RepairDivergence {
			res.orphaned++
			return
		}
		if err := r.store.shadowDelete(ctx, key, sObj, storage.DeleteOptions{}); err != nil {
			shadowWriteFailures.WithLabelValues(resource, opRepair).Inc()
			klog.ErrorS(err, "Reconcile: failed to delete orphaned shadow object", "resource", resource, "key", key)
			res.orphaned++
			return
		}
		klog.V(2).InfoS("Reconcile: deleted orphaned shadow object", "resource", resource, "key", key)
		res.repaired++
	default:
		if objectHash(pObj) == objectHash(sObj) {
			return
		}
		if !r.opts.RepairDivergence || ks.stale(r.store.primaryRV(pObj)) {
			klog.V(2).InfoS("Reconcile: shadow object differs from primary", "resource", resource, "key", key)
			res.drifted++
			return
		}
		if _, err := r.store.shadowUpsert(ctx, key, pObj); err != nil {
			shadowWriteFailures.WithLabelValues(resource, opRepair).Inc()
			klog.ErrorS(err, "Reconcile: failed to repair drifted shadow object", "resource", resource, "key", key)
			res.drifted++
			return
		}
		ks.recordWrite(r.store.primaryRV(pObj))
		klog.V(2).InfoS("Reconcile: repaired drifted shadow object", "resource", resource, "key", key)
		res.repaired++
	}
}

// get reads key from st. The returned bool reports whether the key exists.
func (r *reconciler) get(ctx context.Context, st storage.Interface, key string) (runtime.Object, bool, error) {
	obj, err := r.newItem()
	if err != nil {
		return nil, false, err
	}
	if err := st.Get(ctx, key, storage.GetOptions{}, obj); err != nil {
		if storage.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return obj, true, nil
}

// newItem builds an empty object of the list's item type.
func (r *reconciler) newItem() (runtime.Object, error) {
	list := r.newListFunc()
	itemsPtr, err := meta.GetItemsPtr(list)
	if err != nil {
		return nil, err
	}
	v, err := conversion.EnforcePtr(itemsPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("list items must be a slice: %w", err)
	}
	elem := v.Type().Elem()
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	obj, ok := reflect.New(elem).Interface().(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("list item type %s is not a runtime.Object", elem)
	}
	return obj, nil
}

// listAll lists every object under the resource prefix, keyed by storage
// key. pageSize 0 means unpaged.
func (r *reconciler) listAll(ctx context.Context, st storage.Interface, pageSize int64) (map[string]runtime.Object, error) {
	out := map[string]runtime.Object{}
	cont := ""
	for {
		pred := storage.Everything
		pred.GetAttrs = r.getAttrs
		pred.Limit = pageSize
		pred.Continue = cont
		list := r.newListFunc()
		if err := st.GetList(ctx, r.prefix, storage.ListOptions{Recursive: true, Predicate: pred}, list); err != nil {
			return nil, err
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			key, err := r.keyFunc(item)
			if err != nil {
				return nil, fmt.Errorf("computing key: %w", err)
			}
			out[key] = item
		}
		la, err := meta.ListAccessor(list)
		if err != nil {
			return nil, err
		}
		cont = la.GetContinue()
		if cont == "" || pageSize == 0 {
			return out, nil
		}
	}
}

// objectHash hashes the object's persisted content. It ignores fields that
// legitimately differ between backends: resourceVersion, which each backend
// assigns, managedFields, which are bookkeeping noise, and TypeMeta, which
// is decoder-dependent. Everything else is compared, including metadata
// such as labels, finalizers and deletionTimestamp, so drift anywhere is
// caught, not only in spec and status.
func objectHash(obj runtime.Object) string {
	c := obj.DeepCopyObject()
	if acc, err := meta.Accessor(c); err == nil {
		acc.SetResourceVersion("")
		acc.SetManagedFields(nil)
	}
	c.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	data, err := json.Marshal(c)
	if err != nil {
		// An unhashable object always reads as drifted, so it gets noticed.
		return "unhashable:" + err.Error()
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
