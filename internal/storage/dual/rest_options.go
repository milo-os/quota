package dual

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// cacherDisabler is implemented by shadow getters that can build their
// storage without the in-memory cacher, e.g. *postgres.RESTOptionsGetter.
// Matching it structurally keeps this package free of a postgres import.
type cacherDisabler interface {
	SetDisableCacher(disable bool)
}

// bucketRecomputeDisabler is implemented by shadow getters whose stores can
// derive QuotaBucket allocation totals on insert (*postgres.RESTOptionsGetter).
// The shadow must store what the primary wrote, so dual mode turns it off.
type bucketRecomputeDisabler interface {
	SetRecomputeBucketAllocations(enabled bool)
}

// shadowLifecycle is implemented by shadow getters that own background work
// (*postgres.RESTOptionsGetter: the changelog compactor). Nothing watches
// the shadow, and the Postgres compactor otherwise starts only with the
// first watch, so dual mode starts it with the first shadow store and stops
// it when the last one is destroyed.
type shadowLifecycle interface {
	StartChangelogCompaction()
	Close()
}

// Options configures the dual RESTOptionsGetter.
type Options struct {
	// ShadowWriteTimeout bounds each shadow write. Zero means
	// DefaultShadowWriteTimeout.
	ShadowWriteTimeout time.Duration
	// Reconciler configures the per-resource background reconciler.
	// Interval zero disables it.
	Reconciler ReconcilerOptions
}

// DefaultOptions returns the defaults used by NewRESTOptionsGetter.
func DefaultOptions() Options {
	return Options{
		ShadowWriteTimeout: DefaultShadowWriteTimeout,
		Reconciler:         DefaultReconcilerOptions(),
	}
}

// RESTOptionsGetter wraps a primary and shadow generic.RESTOptionsGetter.
// For each resource, it creates a dual.Store that reads from the primary and
// mirrors writes to the shadow, plus a background reconciler for that
// resource. The reconciler stops when the store's DestroyFunc runs.
type RESTOptionsGetter struct {
	primary generic.RESTOptionsGetter
	shadow  generic.RESTOptionsGetter
	opts    Options

	// liveStores counts shadow stores not yet destroyed; the shadow's
	// background work runs while it is above zero.
	mu         sync.Mutex
	liveStores int
}

// NewRESTOptionsGetter creates a dual-write RESTOptionsGetter with default
// options.
func NewRESTOptionsGetter(primary, shadow generic.RESTOptionsGetter) *RESTOptionsGetter {
	return NewRESTOptionsGetterWithOptions(primary, shadow, DefaultOptions())
}

// NewRESTOptionsGetterWithOptions creates a dual-write RESTOptionsGetter.
//
// Nothing reads or watches the shadow except the reconciler's plain
// LIST/GET, so the shadow needs no cacher. If the shadow getter supports it,
// the cacher is disabled here, and so is the Postgres store's recomputation
// of bucket allocations (the shadow mirrors the primary's status verbatim).
// The changelog compactor, which otherwise starts with the first watch, is
// started with the first shadow store and stopped with the last. With Postgres that also means no watcher
// goroutines or LISTEN connections, since the Postgres watcher starts lazily
// on the first Watch. The shadow getter must therefore be dedicated to dual
// mode and not shared with anything that serves reads.
func NewRESTOptionsGetterWithOptions(primary, shadow generic.RESTOptionsGetter, opts Options) *RESTOptionsGetter {
	if d, ok := shadow.(cacherDisabler); ok {
		d.SetDisableCacher(true)
	} else {
		klog.InfoS("Dual storage shadow getter cannot disable its cacher; shadow will build a watch cache it never uses")
	}
	if d, ok := shadow.(bucketRecomputeDisabler); ok {
		d.SetRecomputeBucketAllocations(false)
	}
	return &RESTOptionsGetter{
		primary: primary,
		shadow:  shadow,
		opts:    opts,
	}
}

// GetRESTOptions returns REST options that produce a dual.Store wrapping both
// the primary and shadow stores for the given resource.
func (r *RESTOptionsGetter) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	primaryOpts, err := r.primary.GetRESTOptions(resource, example)
	if err != nil {
		return generic.RESTOptions{}, fmt.Errorf("failed to get primary REST options for %s: %w", resource, err)
	}
	shadowOpts, err := r.shadow.GetRESTOptions(resource, example)
	if err != nil {
		return generic.RESTOptions{}, fmt.Errorf("failed to get shadow REST options for %s: %w", resource, err)
	}

	primaryDecorator := primaryOpts.Decorator
	shadowDecorator := shadowOpts.Decorator

	primaryOpts.Decorator = func(
		config *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		trigger storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		primaryStore, primaryDestroy, err := primaryDecorator(
			config, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, trigger, indexers,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("primary decorator failed: %w", err)
		}
		// Nothing watches the shadow, so it gets no trigger funcs or indexers.
		shadowStore, shadowDestroy, err := shadowDecorator(
			shadowOpts.StorageConfig, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, nil, nil,
		)
		if err != nil {
			primaryDestroy()
			return nil, nil, fmt.Errorf("shadow decorator failed: %w", err)
		}

		r.shadowStoreCreated()

		store := New(primaryStore, shadowStore,
			WithResource(resource.String()),
			WithShadowWriteTimeout(r.opts.ShadowWriteTimeout),
		)

		ctx, cancel := context.WithCancel(context.Background())
		rec := newReconciler(store, resourcePrefix, keyFunc, newListFunc, getAttrsFunc, r.opts.Reconciler)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec.run(ctx)
		}()

		var once sync.Once
		destroy := func() {
			once.Do(func() {
				cancel()
				wg.Wait()
				primaryDestroy()
				shadowDestroy()
				r.shadowStoreDestroyed()
			})
		}
		return store, destroy, nil
	}
	return primaryOpts, nil
}

// shadowStoreCreated starts the shadow's background work with its first
// store.
func (r *RESTOptionsGetter) shadowStoreCreated() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveStores++
	if r.liveStores == 1 {
		if l, ok := r.shadow.(shadowLifecycle); ok {
			l.StartChangelogCompaction()
		}
	}
}

// shadowStoreDestroyed stops the shadow's background work with its last
// store.
func (r *RESTOptionsGetter) shadowStoreDestroyed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveStores--
	if r.liveStores == 0 {
		if l, ok := r.shadow.(shadowLifecycle); ok {
			l.Close()
		}
	}
}
