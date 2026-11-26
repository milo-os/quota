package dual

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics/testutil"
)

// must unwraps a metric read. A read error means the test itself is broken.
func must(v float64, err error) float64 {
	if err != nil {
		panic(fmt.Sprintf("reading metric: %v", err))
	}
	return v
}

// registryUpdate mimics genericregistry.Store.Update's closure. It enforces
// the client's RV against the object storage hands it, mutates a captured
// variable (like `creating`), and counts invocations.
func registryUpdate(expectedRV string, newSpec string, calls *int, creating *bool) storage.UpdateFunc {
	return func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		*calls++
		in := input.(*testObj)
		*creating = in.ResourceVersion == ""
		if in.ResourceVersion != expectedRV {
			return nil, nil, fmt.Errorf("conflict: object has RV %s, request had %s", in.ResourceVersion, expectedRV)
		}
		out := in.DeepCopyObject().(*testObj)
		out.Spec = newSpec
		return out, nil, nil
	}
}

func TestGuaranteedUpdate_ClosureRunsOnceAndShadowGetsPrimaryState(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	pObj := primary.put("a", "v1")
	shadow.put("a", "v1") // different RV in the shadow

	s := New(primary, shadow, WithResource("test-update"))
	calls := 0
	creating := true
	dest := &testObj{}
	err := s.GuaranteedUpdate(context.Background(), testKey("a"), dest, false, nil,
		registryUpdate(pObj.ResourceVersion, "v2", &calls, &creating), nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if calls != 1 {
		t.Fatalf("tryUpdate ran %d times, want exactly 1 (primary only)", calls)
	}
	if creating {
		t.Fatal("closure variable `creating` was clobbered by a shadow replay")
	}
	got, ok := shadow.peek("a")
	if !ok || got.Spec != "v2" {
		t.Fatalf("shadow = %+v, want spec v2", got)
	}
	if got.ResourceVersion == dest.ResourceVersion {
		t.Fatalf("shadow should assign its own RV, got primary RV %s", got.ResourceVersion)
	}
	if f := must(testutil.GetCounterMetricValue(shadowWriteFailures.WithLabelValues("test-update", opUpdate))); f != 0 {
		t.Fatalf("unexpected shadow failures: %v", f)
	}
}

func TestGuaranteedUpdate_BackfillsMissingShadowObject(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	pObj := primary.put("a", "v1") // created before dual mode: not in shadow

	s := New(primary, shadow, WithResource("test-backfill-write"))
	calls := 0
	var creating bool
	if err := s.GuaranteedUpdate(context.Background(), testKey("a"), &testObj{}, false, nil,
		registryUpdate(pObj.ResourceVersion, "v2", &calls, &creating), nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	got, ok := shadow.peek("a")
	if !ok || got.Spec != "v2" {
		t.Fatalf("shadow = %+v, %v; want backfilled spec v2", got, ok)
	}
	if v := must(testutil.GetCounterMetricValue(shadowBackfills.WithLabelValues("test-backfill-write", sourceWrite))); v != 1 {
		t.Fatalf("backfill counter = %v, want 1", v)
	}
}

func TestCreate_OverwritesStaleShadowRow(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	shadow.put("a", "stale")

	s := New(primary, shadow, WithResource("test-create"))
	out := &testObj{}
	if err := s.Create(context.Background(), testKey("a"), newTestObj("a", "fresh"), out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, _ := shadow.peek("a")
	if got.Spec != "fresh" {
		t.Fatalf("shadow spec = %q, want fresh", got.Spec)
	}
	if out.ResourceVersion != "101" {
		t.Fatalf("out must carry the primary RV, got %s", out.ResourceVersion)
	}
}

func TestDelete_UnconditionalOnShadow(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	pObj := primary.put("a", "v1")
	shadow.put("a", "v1")
	primary.put("b", "v1") // missing from shadow

	s := New(primary, shadow, WithResource("test-delete"))
	rv := pObj.ResourceVersion
	pre := &storage.Preconditions{ResourceVersion: &rv}
	if err := s.Delete(context.Background(), testKey("a"), &testObj{}, pre, nil, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := shadow.peek("a"); ok {
		t.Fatal("shadow still has deleted object")
	}
	if shadow.lastDeletePreconditions != nil {
		t.Fatal("primary RV preconditions must not be replayed against the shadow")
	}
	if err := s.Delete(context.Background(), testKey("b"), &testObj{}, nil, nil, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if f := must(testutil.GetCounterMetricValue(shadowWriteFailures.WithLabelValues("test-delete", opDelete))); f != 0 {
		t.Fatalf("missing shadow key must not count as a failure, got %v", f)
	}
}

func TestMirrorWrite_DropsOutOfOrderWrites(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	s := New(primary, shadow, WithResource("test-order"))

	newer := newTestObj("a", "newer")
	newer.ResourceVersion = "11"
	older := newTestObj("a", "older")
	older.ResourceVersion = "10"

	s.mirrorWrite(context.Background(), testKey("a"), newer, opUpdate)
	s.mirrorWrite(context.Background(), testKey("a"), older, opUpdate)
	if got, _ := shadow.peek("a"); got.Spec != "newer" {
		t.Fatalf("shadow spec = %q, want newer", got.Spec)
	}

	// A late write from before a delete must not bring the object back.
	s.mirrorDelete(context.Background(), testKey("a"), newer, storage.DeleteOptions{})
	s.mirrorWrite(context.Background(), testKey("a"), older, opUpdate)
	if _, ok := shadow.peek("a"); ok {
		t.Fatal("stale write resurrected a deleted shadow object")
	}
	// A real recreate (higher RV) goes through.
	recreated := newTestObj("a", "recreated")
	recreated.ResourceVersion = "20"
	s.mirrorWrite(context.Background(), testKey("a"), recreated, opCreate)
	if got, ok := shadow.peek("a"); !ok || got.Spec != "recreated" {
		t.Fatalf("recreate not mirrored: %+v", got)
	}
}

func TestMirrorWrite_DetachedFromRequestCancellation(t *testing.T) {
	primary, shadow := newMemStore(100), newMemStore(5000)
	s := New(primary, shadow, WithResource("test-cancel"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var seenErr error
	wrapped := &mockStorage{
		createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
			seenErr = ctx.Err()
			return shadow.Create(ctx, key, obj, out, ttl)
		},
	}
	s.shadow = wrapped
	s.mirrorWrite(ctx, testKey("a"), newTestObj("a", "v1"), opCreate)
	if seenErr != nil {
		t.Fatalf("shadow write saw cancelled context: %v", seenErr)
	}
}

func TestObjectHash_IgnoresBackendSpecificFields(t *testing.T) {
	a := newTestObj("a", "v1")
	a.ResourceVersion = "1"
	a.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "x"}}
	a.Kind = "Test"
	b := newTestObj("a", "v1")
	b.ResourceVersion = "9999"
	if objectHash(a) != objectHash(b) {
		t.Fatal("hash must ignore resourceVersion, managedFields and TypeMeta")
	}
	b.Status = "different"
	if objectHash(a) == objectHash(b) {
		t.Fatal("hash must cover status")
	}
	c := newTestObj("a", "v1")
	c.Labels = map[string]string{"x": "y"}
	if objectHash(a) == objectHash(c) {
		t.Fatal("hash must cover metadata like labels")
	}
}

func TestReconciler(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(fmt.Sprintf("repair=%v", repair), func(t *testing.T) {
			resource := "test-reconcile-" + strconv.FormatBool(repair)
			primary, shadow := newMemStore(100), newMemStore(5000)
			primary.put("same", "v1")
			shadow.put("same", "v1")
			primary.put("drifted", "v2")
			shadow.put("drifted", "v1")
			primary.put("missing", "v1")
			shadow.put("orphan", "v1")

			s := New(primary, shadow, WithResource(resource))
			opts := DefaultReconcilerOptions()
			opts.QPS, opts.Burst = 1000, 1000
			opts.RepairDivergence = repair
			r := newReconciler(s, testPrefix, testKeyFunc, newTestList, nil, opts)

			res, err := r.reconcileOnce(context.Background())
			if err != nil {
				t.Fatalf("reconcileOnce: %v", err)
			}
			if res.backfilled != 1 {
				t.Errorf("backfilled = %d, want 1", res.backfilled)
			}
			if got, ok := shadow.peek("missing"); !ok || got.Spec != "v1" {
				t.Errorf("missing object not backfilled: %+v", got)
			}
			wantDrift, wantOrphan := 1, 1
			if repair {
				wantDrift, wantOrphan = 0, 0
			}
			if res.missing != 0 || res.drifted != wantDrift || res.orphaned != wantOrphan {
				t.Errorf("result = %+v, want missing=0 drifted=%d orphaned=%d", res, wantDrift, wantOrphan)
			}
			if g := must(testutil.GetGaugeMetricValue(divergentObjects.WithLabelValues(resource, reasonDrifted))); int(g) != wantDrift {
				t.Errorf("drifted gauge = %v, want %d", g, wantDrift)
			}
			if g := must(testutil.GetGaugeMetricValue(lastReconcileTimestamp.WithLabelValues(resource))); g == 0 {
				t.Error("last reconcile timestamp not set")
			}

			if repair {
				res, err = r.reconcileOnce(context.Background())
				if err != nil {
					t.Fatalf("second reconcileOnce: %v", err)
				}
				if res.missing+res.drifted+res.orphaned+res.backfilled+res.repaired != 0 {
					t.Errorf("second pass should be clean, got %+v", res)
				}
			}
		})
	}
}

// staleListStore returns an empty LIST but serves GET from the real
// store, like a shadow listed just before a write landed.
type staleListStore struct{ *memStore }

func (staleListStore) GetList(_ context.Context, _ string, _ storage.ListOptions, _ runtime.Object) error {
	return nil
}

func TestReconciler_IgnoresTransientListSkew(t *testing.T) {
	// The two lists are taken at different moments. A key that looks
	// missing in the list but is present on re-read must not be counted
	// or backfilled.
	primary, shadow := newMemStore(100), newMemStore(5000)
	primary.put("a", "v1")
	shadow.put("a", "v1")
	s := New(primary, staleListStore{shadow}, WithResource("test-skew"))
	r := newReconciler(s, testPrefix, testKeyFunc, newTestList, nil, ReconcilerOptions{QPS: 1000, Burst: 1000})
	before := shadow.updates + shadow.creates
	res, err := r.reconcileOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.missing+res.drifted+res.orphaned+res.backfilled != 0 {
		t.Fatalf("expected no divergence, got %+v", res)
	}
	if shadow.updates+shadow.creates != before {
		t.Fatal("reconciler wrote to the shadow for a non-divergent key")
	}
}

func TestSequencer_Prune(t *testing.T) {
	seq := newSequencer()
	now := time.Unix(1000, 0)
	seq.now = func() time.Time { return now }
	ks := seq.acquire("k")
	ks.recordWrite(5)
	seq.release(ks)
	seq.prune()
	if seq.size() != 1 {
		t.Fatal("fresh entry pruned")
	}
	now = now.Add(sequencerRetention + time.Second)
	seq.prune()
	if seq.size() != 0 {
		t.Fatal("idle entry not pruned")
	}
}

// fakeGetter is a RESTOptionsGetter backed by a memStore that records
// whether the cacher was disabled.
type fakeGetter struct {
	store          *memStore
	cacherDisabled bool
	gotTrigger     bool
}

func (f *fakeGetter) SetDisableCacher(d bool) { f.cacherDisabled = d }

func (f *fakeGetter) GetRESTOptions(resource schema.GroupResource, _ runtime.Object) (generic.RESTOptions, error) {
	return generic.RESTOptions{
		StorageConfig:  &storagebackend.ConfigForResource{GroupResource: resource},
		ResourcePrefix: "/" + resource.Group + "/" + resource.Resource,
		Decorator: func(_ *storagebackend.ConfigForResource, _ string, _ func(runtime.Object) (string, error), _ func() runtime.Object, _ func() runtime.Object, _ storage.AttrFunc, trigger storage.IndexerFuncs, _ *cache.Indexers) (storage.Interface, factory.DestroyFunc, error) {
			f.gotTrigger = trigger != nil
			return f.store, func() {}, nil
		},
	}, nil
}

func TestRESTOptionsGetter_ShadowWithoutCacher(t *testing.T) {
	pg := &fakeGetter{store: newMemStore(100)}
	sg := &fakeGetter{store: newMemStore(5000)}
	opts := DefaultOptions()
	opts.Reconciler.Interval = 0
	g := NewRESTOptionsGetterWithOptions(pg, sg, opts)
	if !sg.cacherDisabled {
		t.Fatal("shadow cacher not disabled")
	}
	if pg.cacherDisabled {
		t.Fatal("primary cacher must stay enabled")
	}
	ro, err := g.GetRESTOptions(schema.GroupResource{Group: "quota.miloapis.com", Resource: "tests"}, &testObj{})
	if err != nil {
		t.Fatal(err)
	}
	st, destroy, err := ro.Decorator(ro.StorageConfig, testPrefix, testKeyFunc, func() runtime.Object { return &testObj{} }, newTestList, nil, storage.IndexerFuncs{"x": nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer destroy()
	if !pg.gotTrigger || sg.gotTrigger {
		t.Fatalf("trigger funcs: primary=%v shadow=%v; want primary only", pg.gotTrigger, sg.gotTrigger)
	}
	if err := st.Create(context.Background(), testKey("a"), newTestObj("a", "v1"), &testObj{}, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := sg.store.peek("a"); !ok {
		t.Fatal("create not mirrored through decorator-built store")
	}
	destroy() // idempotent
}

// lifecycleGetter is a shadow getter with the Postgres getter's background
// work and bucket-recompute switch.
type lifecycleGetter struct {
	fakeGetter
	recompute bool
	started   int
	closed    int
}

func (l *lifecycleGetter) SetRecomputeBucketAllocations(enabled bool) { l.recompute = enabled }
func (l *lifecycleGetter) StartChangelogCompaction()                  { l.started++ }
func (l *lifecycleGetter) Close()                                     { l.closed++ }

// Nothing watches the shadow, so dual mode must start its changelog
// compactor itself, stop it with the last store, and keep the shadow from
// rewriting bucket status.
func TestRESTOptionsGetter_ShadowLifecycle(t *testing.T) {
	pg := &fakeGetter{store: newMemStore(100)}
	sg := &lifecycleGetter{fakeGetter: fakeGetter{store: newMemStore(5000)}, recompute: true}
	opts := DefaultOptions()
	opts.Reconciler.Interval = 0
	g := NewRESTOptionsGetterWithOptions(pg, sg, opts)
	if sg.recompute {
		t.Fatal("shadow bucket recompute not disabled")
	}

	var destroys []func()
	for _, res := range []string{"a", "b"} {
		ro, err := g.GetRESTOptions(schema.GroupResource{Group: "quota.miloapis.com", Resource: res}, &testObj{})
		if err != nil {
			t.Fatal(err)
		}
		_, destroy, err := ro.Decorator(ro.StorageConfig, testPrefix, testKeyFunc, func() runtime.Object { return &testObj{} }, newTestList, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		destroys = append(destroys, destroy)
	}
	if sg.started != 1 {
		t.Fatalf("compaction started %d times, want 1", sg.started)
	}
	destroys[0]()
	if sg.closed != 0 {
		t.Fatal("shadow closed while a store is still live")
	}
	destroys[1]()
	destroys[1]() // idempotent
	if sg.closed != 1 {
		t.Fatalf("shadow closed %d times, want 1", sg.closed)
	}
}
