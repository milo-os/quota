package postgres

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"

	pgwatch "go.miloapis.com/quota/internal/watch"
)

// RESTOptionsGetter implements generic.RESTOptionsGetter for Postgres-backed storage.
type RESTOptionsGetter struct {
	db    *sql.DB
	dsn   string
	codec runtime.Codec
	// watchExcludedKeyPrefixes is forwarded to every Store the decorator
	// creates, so the polled watcher skips events for those keys. Used
	// when the postgres-native QuotaClaim REST layer is active.
	watchExcludedKeyPrefixes []string
	// disableCacher, when true, causes the storage decorator to return
	// the raw Postgres Store directly without wrapping it in the
	// in-memory cacher. GET/LIST then go to Postgres; WATCH uses the
	// polled PostgresWatcher directly. This trades ~10us cached reads
	// for lower memory and GC pressure at scale.
	disableCacher bool
	// recomputeBucketAllocations is passed to every Store; see
	// Store.SetRecomputeBucketAllocations. On by default (Postgres
	// allocation mode); the dual-mode shadow turns it off.
	recomputeBucketAllocations bool

	// watchOptions configures the shared watch hub (poll and bookmark
	// intervals, changelog retention). Must be set before the first store
	// is created.
	watchOptions pgwatch.Options
	// hubOnce/hub: one LISTEN connection and one changelog cleaner shared
	// by every Store this getter creates, instead of one per resource.
	hubOnce sync.Once
	hub     *pgwatch.Hub
}

// NewRESTOptionsGetter creates a RESTOptionsGetter that produces Postgres-backed stores.
//
// Connection pool sizing matters: the database/sql default MaxOpenConns is
// unlimited, which causes new physical connections under contention and wastes
// time on TCP+TLS+auth handshakes. We cap it explicitly so concurrent
// goroutines share a small set of warm connections. The pgx/stdlib driver
// caches prepared statements per physical connection automatically
// (StatementCacheModePrepare), so a small warm pool is ideal.
//
// The dsn is also stored for the watcher's dedicated LISTEN/NOTIFY connection,
// which uses a separate physical connection from the pooled *sql.DB.
func NewRESTOptionsGetter(dsn string) (*RESTOptionsGetter, error) {
	db, err := openDB(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	return &RESTOptionsGetter{db: db, dsn: dsn, recomputeBucketAllocations: true}, nil
}

// GetRESTOptions returns the REST options for a given resource.
// It builds a custom StorageDecorator that wraps the Postgres Store in the
// standard apiserver in-memory cacher (cacherstorage.Cacher). The cacher
// performs ONE initial LIST and a single WATCH against Postgres at startup,
// then serves all subsequent reads from memory at ~10us latency. Without
// this wrapper, every kubectl get hits Postgres directly, which is what
// caused the 8x read latency gap vs etcd in initial benchmarks.
func (r *RESTOptionsGetter) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	ret := generic.RESTOptions{
		ResourcePrefix: resource.Group + "/" + resource.Resource,
		StorageConfig: &storagebackend.ConfigForResource{
			Config: storagebackend.Config{
				// The type isn't used by our decorator but must be non-empty
				// to satisfy validation in upstream code.
				Type:  "postgres",
				Codec: r.codec,
				// EventsHistoryWindow must be >= 1m15s (the cacher's
				// DefaultEventFreshDuration). It's the minimum duration of
				// changelog events the storage promises to keep — clients
				// resuming a watch within this window can replay missed
				// events. Changelog retention is configurable but clamped
				// to pgwatch.MinChangelogRetention (2m), above this.
				EventsHistoryWindow: 75 * time.Second,
			},
			GroupResource: resource,
		},
		Decorator: func(
			config *storagebackend.ConfigForResource,
			resourcePrefix string,
			keyFunc func(obj runtime.Object) (string, error),
			newFunc func() runtime.Object,
			newListFunc func() runtime.Object,
			getAttrsFunc storage.AttrFunc,
			trigger storage.IndexerFuncs,
			indexers *cache.Indexers,
		) (storage.Interface, factory.DestroyFunc, error) {
			rawStore := NewWithHub(r.db, r.codec, r.Hub(), r.watchExcludedKeyPrefixes)
			rawStore.SetNewFunc(newFunc)
			rawStore.SetRecomputeBucketAllocations(r.recomputeBucketAllocations)

			if r.disableCacher {
				// Return the raw store directly — no cacher, no watch cache.
				// GET/LIST go to Postgres; WATCH uses the polled
				// PostgresWatcher directly.
				return rawStore, func() {}, nil
			}

			cacherConfig := cacherstorage.Config{
				Storage:             rawStore,
				Versioner:           storage.APIObjectVersioner{},
				GroupResource:       config.GroupResource,
				ResourcePrefix:      resourcePrefix,
				KeyFunc:             keyFunc,
				NewFunc:             newFunc,
				NewListFunc:         newListFunc,
				GetAttrsFunc:        getAttrsFunc,
				IndexerFuncs:        trigger,
				Indexers:            indexers,
				Codec:               r.codec,
				EventsHistoryWindow: config.EventsHistoryWindow,
			}
			cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
			if err != nil {
				return nil, func() {}, fmt.Errorf("failed to create cacher for %s: %w", config.GroupResource, err)
			}
			delegator := cacherstorage.NewCacheDelegator(cacher, rawStore)
			var once sync.Once
			destroy := func() {
				once.Do(func() {
					delegator.Stop()
					cacher.Stop()
				})
			}
			return delegator, destroy, nil
		},
	}
	return ret, nil
}

// SetCodec sets the codec used for encoding/decoding objects.
// This must be called before the RESTOptionsGetter is used.
func (r *RESTOptionsGetter) SetCodec(codec runtime.Codec) {
	r.codec = codec
}

// SetWatchExcludedKeyPrefixes configures the polled watcher to skip events
// for keys matching any of the supplied prefixes. Must be called before the
// RESTOptionsGetter produces stores, because each decorator invocation
// captures the current slice.
func (r *RESTOptionsGetter) SetWatchExcludedKeyPrefixes(excludedKeyPrefixes []string) {
	r.watchExcludedKeyPrefixes = append([]string(nil), excludedKeyPrefixes...)
}

// SetDisableCacher controls whether the storage decorator skips the
// in-memory cacher. When true, GET/LIST/WATCH for non-native resources
// (QuotaDefinition, QuotaGrant, QuotaBucket, policies) go directly to
// Postgres. Must be called before the RESTOptionsGetter produces stores.
func (r *RESTOptionsGetter) SetDisableCacher(disable bool) {
	r.disableCacher = disable
}

// SetRecomputeBucketAllocations controls whether QuotaBucket inserts derive
// their allocation totals from existing claims (see
// Store.SetRecomputeBucketAllocations). Must be called before the getter
// produces stores.
func (r *RESTOptionsGetter) SetRecomputeBucketAllocations(enabled bool) {
	r.recomputeBucketAllocations = enabled
}

// DB exposes the underlying *sql.DB so components like the synchronous
// allocator can share the same connection pool as the storage layer.
func (r *RESTOptionsGetter) DB() *sql.DB {
	return r.db
}

// Codec returns the runtime codec configured on the getter.
func (r *RESTOptionsGetter) Codec() runtime.Codec {
	return r.codec
}

// SetWatchOptions configures the shared watch hub (changelog retention,
// cleanup, poll and bookmark intervals). Must be called before the getter
// produces stores; later calls have no effect.
func (r *RESTOptionsGetter) SetWatchOptions(opts pgwatch.Options) {
	r.watchOptions = opts
}

// SetChangelogRetention sets how long changelog rows are kept before
// compaction. Watches and paginated lists that need older history get 410
// Gone. Must be called before the getter produces stores.
func (r *RESTOptionsGetter) SetChangelogRetention(d time.Duration) {
	r.watchOptions.Retention = d
}

// Hub returns the watch hub shared by every Store this getter creates,
// creating it on first use.
func (r *RESTOptionsGetter) Hub() *pgwatch.Hub {
	r.hubOnce.Do(func() {
		r.hub = pgwatch.NewHub(r.db, r.dsn, r.watchOptions)
	})
	return r.hub
}

// StartChangelogCompaction starts the shared hub's changelog cleaner without
// waiting for a watch. Watches start it on first use; a getter whose stores
// are written but never watched (the dual-mode shadow) calls this so its
// changelog is still compacted. Close stops it.
func (r *RESTOptionsGetter) StartChangelogCompaction() {
	r.Hub().StartCompaction()
}

// Close stops the shared watch hub. The *sql.DB is left open because the
// allocator shares it.
func (r *RESTOptionsGetter) Close() {
	if r.hub != nil {
		r.hub.Stop()
	}
}
