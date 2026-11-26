package quotaclaim

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// memStorage is a minimal in-memory storage.Interface covering the calls
// genericregistry.Store makes on the Get/Update/Delete/List paths. Unused
// methods panic via the nil embedded interface.
type memStorage struct {
	storage.Interface
	mu      sync.Mutex
	rv      uint64
	objects map[string]*quota.QuotaClaim
	deletes int // physical deletes performed through memStorage.Delete
	gets    int // reads performed through memStorage.Get
}

func newMemStorage() *memStorage {
	return &memStorage{rv: 100, objects: map[string]*quota.QuotaClaim{}}
}

func (m *memStorage) Versioner() storage.Versioner { return storage.APIObjectVersioner{} }

func (m *memStorage) put(key string, c *quota.QuotaClaim) *quota.QuotaClaim {
	m.rv++
	c = c.DeepCopy()
	c.ResourceVersion = strconv.FormatUint(m.rv, 10)
	m.objects[key] = c
	return c.DeepCopy()
}

func (m *memStorage) Get(_ context.Context, key string, _ storage.GetOptions, out runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	c, ok := m.objects[key]
	if !ok {
		return storage.NewKeyNotFoundError(key, 0)
	}
	*out.(*quota.QuotaClaim) = *c.DeepCopy()
	return nil
}

func (m *memStorage) Delete(_ context.Context, key string, out runtime.Object, pre *storage.Preconditions, _ storage.ValidateObjectFunc, _ runtime.Object, _ storage.DeleteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.objects[key]
	if !ok {
		return storage.NewKeyNotFoundError(key, 0)
	}
	if err := pre.Check(key, c); err != nil {
		return err
	}
	delete(m.objects, key)
	m.deletes++
	*out.(*quota.QuotaClaim) = *c
	return nil
}

func (m *memStorage) GuaranteedUpdate(ctx context.Context, key string, dest runtime.Object, _ bool, pre *storage.Preconditions, tryUpdate storage.UpdateFunc, _ runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.objects[key]
	if !ok {
		return storage.NewKeyNotFoundError(key, 0)
	}
	if err := pre.Check(key, c); err != nil {
		return err
	}
	rv, _ := strconv.ParseUint(c.ResourceVersion, 10, 64)
	updated, _, err := tryUpdate(c.DeepCopy(), storage.ResponseMeta{ResourceVersion: rv})
	if err != nil {
		return err
	}
	*dest.(*quota.QuotaClaim) = *m.put(key, updated.(*quota.QuotaClaim))
	return nil
}

func (m *memStorage) GetList(_ context.Context, key string, _ storage.ListOptions, listObj runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var items []runtime.Object
	for k, c := range m.objects {
		if strings.HasPrefix(k, key) {
			items = append(items, c.DeepCopy())
		}
	}
	if err := meta.SetList(listObj, items); err != nil {
		return err
	}
	return meta.NewAccessor().SetResourceVersion(listObj, strconv.FormatUint(m.rv, 10))
}

func (m *memStorage) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

// fakeAllocator records calls and implements DryRunAllocator and
// ConditionalReleaser against memStorage, conditioning release on RV.
type fakeAllocator struct {
	store        *memStorage
	mu           sync.Mutex
	allocates    int
	dryRuns      int
	released     []string
	attempts     int // ReleaseClaim calls, including ones that conflict
	conflictOnce bool
}

func (f *fakeAllocator) Allocate(_ context.Context, c *quota.QuotaClaim) (*quota.QuotaClaim, error) {
	f.mu.Lock()
	f.allocates++
	f.mu.Unlock()
	return c, nil
}

func (f *fakeAllocator) AllocateDryRun(_ context.Context, c *quota.QuotaClaim) (*quota.QuotaClaim, error) {
	f.mu.Lock()
	f.dryRuns++
	f.mu.Unlock()
	return c, nil
}

func (f *fakeAllocator) Release(ctx context.Context, c *quota.QuotaClaim) error {
	_, err := f.ReleaseClaim(ctx, c)
	return err
}

func (f *fakeAllocator) ReleaseClaim(_ context.Context, c *quota.QuotaClaim) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.conflictOnce {
		f.conflictOnce = false
		return false, apierrors.NewConflict(schema.GroupResource{Resource: "quotaclaims"}, c.Name, fmt.Errorf("changed"))
	}
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	key := testClaimKey(c.Namespace, c.Name)
	stored, ok := f.store.objects[key]
	if !ok {
		return false, nil
	}
	if stored.ResourceVersion != c.ResourceVersion || stored.UID != c.UID {
		return false, apierrors.NewConflict(schema.GroupResource{Resource: "quotaclaims"}, c.Name, fmt.Errorf("stale"))
	}
	delete(f.store.objects, key)
	f.released = append(f.released, c.Name)
	return true, nil
}

func (f *fakeAllocator) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.released)
}

type memRESTOptionsGetter struct {
	store *memStorage
	codec runtime.Codec
}

func (g memRESTOptionsGetter) GetRESTOptions(resource schema.GroupResource, _ runtime.Object) (generic.RESTOptions, error) {
	return generic.RESTOptions{
		ResourcePrefix: resource.Group + "/" + resource.Resource,
		StorageConfig: &storagebackend.ConfigForResource{
			Config:        storagebackend.Config{Type: "memory", Codec: g.codec},
			GroupResource: resource,
		},
		Decorator: func(*storagebackend.ConfigForResource, string, func(runtime.Object) (string, error),
			func() runtime.Object, func() runtime.Object, storage.AttrFunc, storage.IndexerFuncs, *cache.Indexers,
		) (storage.Interface, factory.DestroyFunc, error) {
			return g.store, func() {}, nil
		},
	}, nil
}

func testClaimKey(namespace, name string) string {
	return "/" + v1alpha1.Resource("quotaclaims").Group + "/quotaclaims/" + namespace + "/" + name
}

type allocTestEnv struct {
	rest   *AllocatingREST
	status *QuotaClaimStatusStorage
	store  *memStorage
	alloc  *fakeAllocator
	ctx    context.Context
}

func newAllocTestEnv(t *testing.T) *allocTestEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	install.Install(scheme)
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)
	store := newMemStorage()
	alloc := &fakeAllocator{store: store}
	r, status, err := NewAllocatingStorage(scheme, memRESTOptionsGetter{store: store, codec: codec}, alloc)
	if err != nil {
		t.Fatalf("NewAllocatingStorage: %v", err)
	}
	return &allocTestEnv{
		rest:   r,
		status: status,
		store:  store,
		alloc:  alloc,
		ctx:    genericapirequest.WithNamespace(context.Background(), "ns"),
	}
}

func (e *allocTestEnv) seed(name string, finalizers ...string) *quota.QuotaClaim {
	e.store.mu.Lock()
	defer e.store.mu.Unlock()
	return e.store.put(testClaimKey("ns", name), &quota.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID("uid-" + name), Finalizers: finalizers},
		Spec: quota.QuotaClaimSpec{
			ConsumerRef: quota.ConsumerRef{Kind: "Project", Name: "p"},
			Requests:    []quota.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
		},
	})
}

func (e *allocTestEnv) stored(name string) *quota.QuotaClaim {
	e.store.mu.Lock()
	defer e.store.mu.Unlock()
	if c, ok := e.store.objects[testClaimKey("ns", name)]; ok {
		return c.DeepCopy()
	}
	return nil
}

func strPtr(s string) *string { return &s }

func TestAllocatingREST_Delete(t *testing.T) {
	tests := []struct {
		name         string
		finalizers   []string
		options      func(seeded *quota.QuotaClaim) *metav1.DeleteOptions
		conflictOnce bool
		wantErr      func(error) bool
		wantDeleted  bool
		wantReleases int
		wantPresent  bool
		wantDeleting bool
	}{
		{
			name:         "no finalizers releases and removes",
			options:      func(*quota.QuotaClaim) *metav1.DeleteOptions { return &metav1.DeleteOptions{} },
			wantDeleted:  true,
			wantReleases: 1,
		},
		{
			name:         "finalizers keep object and capacity",
			finalizers:   []string{"example.com/hold"},
			options:      func(*quota.QuotaClaim) *metav1.DeleteOptions { return &metav1.DeleteOptions{} },
			wantPresent:  true,
			wantDeleting: true,
		},
		{
			name: "matching preconditions",
			options: func(s *quota.QuotaClaim) *metav1.DeleteOptions {
				uid := s.UID
				return &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: strPtr(s.ResourceVersion)}}
			},
			wantDeleted:  true,
			wantReleases: 1,
		},
		{
			name: "uid precondition mismatch",
			options: func(*quota.QuotaClaim) *metav1.DeleteOptions {
				uid := types.UID("some-other-uid")
				return &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
			},
			wantErr:     apierrors.IsConflict,
			wantPresent: true,
		},
		{
			name: "resourceVersion precondition mismatch",
			options: func(*quota.QuotaClaim) *metav1.DeleteOptions {
				return &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: strPtr("1")}}
			},
			wantErr:     apierrors.IsConflict,
			wantPresent: true,
		},
		{
			name: "dry run releases nothing",
			options: func(*quota.QuotaClaim) *metav1.DeleteOptions {
				return &metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}
			},
			wantDeleted: true,
			wantPresent: true,
		},
		{
			name:       "dry run with finalizers changes nothing",
			finalizers: []string{"example.com/hold"},
			options: func(*quota.QuotaClaim) *metav1.DeleteOptions {
				return &metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}
			},
			wantPresent:  true,
			wantDeleting: false,
		},
		{
			name:         "release conflict is retried",
			options:      func(*quota.QuotaClaim) *metav1.DeleteOptions { return &metav1.DeleteOptions{} },
			conflictOnce: true,
			wantDeleted:  true,
			wantReleases: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newAllocTestEnv(t)
			env.alloc.conflictOnce = tc.conflictOnce
			seeded := env.seed("c", tc.finalizers...)

			_, deleted, err := env.rest.Delete(env.ctx, "c", rest.ValidateAllObjectFunc, tc.options(seeded))
			if tc.wantErr != nil {
				if !tc.wantErr(err) {
					t.Fatalf("unexpected error: %v", err)
				}
			} else if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if deleted != tc.wantDeleted {
				t.Errorf("deleted=%v, want %v", deleted, tc.wantDeleted)
			}
			if got := env.alloc.releaseCount(); got != tc.wantReleases {
				t.Errorf("releases=%d, want %d", got, tc.wantReleases)
			}
			if env.store.deletes != 0 {
				t.Errorf("claim row removed without the allocator (%d raw deletes)", env.store.deletes)
			}
			got := env.stored("c")
			if (got != nil) != tc.wantPresent {
				t.Fatalf("present=%v, want %v", got != nil, tc.wantPresent)
			}
			if got != nil && (got.DeletionTimestamp != nil) != tc.wantDeleting {
				t.Errorf("deletionTimestamp set=%v, want %v", got.DeletionTimestamp != nil, tc.wantDeleting)
			}
		})
	}
}

// plainAllocator hides fakeAllocator's ConditionalReleaser, so Release
// cannot detect a stale claim.
type plainAllocator struct{ f *fakeAllocator }

func (p plainAllocator) Allocate(ctx context.Context, c *quota.QuotaClaim) (*quota.QuotaClaim, error) {
	return p.f.Allocate(ctx, c)
}

func (p plainAllocator) Release(ctx context.Context, c *quota.QuotaClaim) error {
	return p.f.Release(ctx, c)
}

// TestAllocatingREST_DeleteReads verifies that a delete reads the claim once:
// the release reuses the read genericregistry.Store.Delete performs, and
// reads again only after a conflict or when the allocator cannot condition
// the release on the claim's version.
func TestAllocatingREST_DeleteReads(t *testing.T) {
	tests := []struct {
		name         string
		conflictOnce bool
		plain        bool
		wantGets     int
	}{
		{name: "reuses the store's read", wantGets: 1},
		{name: "re-reads after a release conflict", conflictOnce: true, wantGets: 2},
		{name: "unconditional release reads again", plain: true, wantGets: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newAllocTestEnv(t)
			if tc.plain {
				rs := env.rest.Storage.Storage.(*releasingStorage)
				rs.allocator = plainAllocator{env.alloc}
			}
			env.alloc.conflictOnce = tc.conflictOnce
			env.seed("c")

			if _, deleted, err := env.rest.Delete(env.ctx, "c", rest.ValidateAllObjectFunc, &metav1.DeleteOptions{}); err != nil || !deleted {
				t.Fatalf("Delete: deleted=%v err=%v", deleted, err)
			}
			if env.store.gets != tc.wantGets {
				t.Errorf("reads=%d, want %d", env.store.gets, tc.wantGets)
			}
			if got := env.alloc.releaseCount(); got != 1 {
				t.Errorf("releases=%d, want 1", got)
			}
		})
	}
}

// TestAllocatingREST_DeleteAfterFinalizerUpdate verifies that a delete which
// updates the claim before removing it does not try to release the version
// it read before the update.
func TestAllocatingREST_DeleteAfterFinalizerUpdate(t *testing.T) {
	env := newAllocTestEnv(t)
	env.seed("c", "example.com/hold")
	ctx := withDeleteRead(env.ctx)
	rs := env.rest.Storage.Storage.(*releasingStorage)
	key := testClaimKey("ns", "c")

	read := &quota.QuotaClaim{}
	if err := rs.Get(ctx, key, storage.GetOptions{}, read); err != nil {
		t.Fatal(err)
	}
	updated := &quota.QuotaClaim{}
	if err := rs.GuaranteedUpdate(ctx, key, updated, false, nil, func(in runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		c := in.(*quota.QuotaClaim)
		c.Finalizers = nil
		return c, nil, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	out := &quota.QuotaClaim{}
	if err := rs.Delete(ctx, key, out, nil, nil, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if out.ResourceVersion != updated.ResourceVersion {
		t.Errorf("released resourceVersion %s, want the updated %s", out.ResourceVersion, updated.ResourceVersion)
	}
	if env.alloc.releaseCount() != 1 || env.alloc.attempts != 1 {
		t.Errorf("releases=%d attempts=%d, want 1/1", env.alloc.releaseCount(), env.alloc.attempts)
	}
}

// dropFinalizers is a rest.UpdatedObjectInfo that removes every finalizer
// from the current object.
type dropFinalizers struct{}

func (dropFinalizers) Preconditions() *metav1.Preconditions { return nil }

func (dropFinalizers) UpdatedObject(_ context.Context, old runtime.Object) (runtime.Object, error) {
	c := old.(*quota.QuotaClaim).DeepCopy()
	c.Finalizers = nil
	return c, nil
}

// TestAllocatingREST_FinalizerRemovalReleases verifies that removing the last
// finalizer from a deleting claim through the generic update paths (main
// resource and /status) removes the claim and releases capacity exactly once.
func TestAllocatingREST_FinalizerRemovalReleases(t *testing.T) {
	tests := []struct {
		name   string
		update func(env *allocTestEnv) (runtime.Object, bool, error)
	}{
		{
			name: "main resource update",
			update: func(env *allocTestEnv) (runtime.Object, bool, error) {
				return env.rest.Update(env.ctx, "c", dropFinalizers{}, nil, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
			},
		},
		{
			name: "status subresource update",
			update: func(env *allocTestEnv) (runtime.Object, bool, error) {
				return env.status.Update(env.ctx, "c", dropFinalizers{}, nil, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newAllocTestEnv(t)
			env.seed("c", "example.com/hold")
			if _, deleted, err := env.rest.Delete(env.ctx, "c", rest.ValidateAllObjectFunc, &metav1.DeleteOptions{}); err != nil || deleted {
				t.Fatalf("Delete with finalizer: deleted=%v err=%v", deleted, err)
			}
			if env.alloc.releaseCount() != 0 {
				t.Fatalf("capacity released while finalizers remain")
			}
			if _, _, err := tc.update(env); err != nil {
				t.Fatalf("update: %v", err)
			}
			if env.stored("c") != nil {
				t.Fatalf("claim not removed after last finalizer was dropped")
			}
			if got := env.alloc.releaseCount(); got != 1 {
				t.Errorf("releases=%d, want 1", got)
			}
			if env.store.deletes != 0 {
				t.Errorf("claim row removed without the allocator")
			}
		})
	}
}

func TestAllocatingREST_DeleteCollection(t *testing.T) {
	env := newAllocTestEnv(t)
	env.seed("a")
	env.seed("b")
	env.seed("held", "example.com/hold")

	if _, err := env.rest.DeleteCollection(env.ctx, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{}, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("DeleteCollection: %v", err)
	}
	if got := env.alloc.releaseCount(); got != 2 {
		t.Errorf("releases=%d, want 2 (one per finalizer-free claim)", got)
	}
	if env.store.deletes != 0 {
		t.Errorf("DeleteCollection removed rows without the allocator")
	}
	held := env.stored("held")
	if held == nil || held.DeletionTimestamp == nil {
		t.Errorf("claim with finalizer must remain, marked deleting: %+v", held)
	}
	if env.store.count() != 1 {
		t.Errorf("remaining objects=%d, want 1", env.store.count())
	}
}

func TestAllocatingREST_DeleteNotFoundWhenReleasedConcurrently(t *testing.T) {
	env := newAllocTestEnv(t)
	seeded := env.seed("c")
	// Another request releases the claim between the store's read and ours.
	rs := env.rest.Storage.Storage.(*releasingStorage)
	if _, err := env.alloc.ReleaseClaim(env.ctx, seeded); err != nil {
		t.Fatal(err)
	}
	err := rs.Delete(env.ctx, testClaimKey("ns", "c"), &quota.QuotaClaim{}, nil, nil, nil, storage.DeleteOptions{})
	if !storage.IsNotFound(err) {
		t.Errorf("expected NotFound, got %v", err)
	}
	if got := env.alloc.releaseCount(); got != 1 {
		t.Errorf("releases=%d, want 1", got)
	}
}

func TestAllocatingREST_Create(t *testing.T) {
	tests := []struct {
		name          string
		dryRun        []string
		wantAllocates int
		wantDryRuns   int
	}{
		{name: "real create allocates", wantAllocates: 1},
		{name: "dry run evaluates only", dryRun: []string{metav1.DryRunAll}, wantDryRuns: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newAllocTestEnv(t)
			claim := &quota.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec: quota.QuotaClaimSpec{
					ConsumerRef: quota.ConsumerRef{Kind: "Project", Name: "p"},
					Requests:    []quota.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
				},
			}
			if _, err := env.rest.Create(env.ctx, claim, nil, &metav1.CreateOptions{DryRun: tc.dryRun}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			if env.alloc.allocates != tc.wantAllocates || env.alloc.dryRuns != tc.wantDryRuns {
				t.Errorf("allocates=%d dryRuns=%d, want %d/%d", env.alloc.allocates, env.alloc.dryRuns, tc.wantAllocates, tc.wantDryRuns)
			}
		})
	}
}

func TestAllocatingREST_FinalizeDeletion(t *testing.T) {
	env := newAllocTestEnv(t)

	held := env.seed("held", "example.com/hold")
	held.DeletionTimestamp = &metav1.Time{}
	if err := env.rest.FinalizeDeletion(env.ctx, held, nil); err != nil {
		t.Fatalf("FinalizeDeletion with finalizers: %v", err)
	}
	if env.alloc.releaseCount() != 0 || env.stored("held") == nil {
		t.Fatalf("FinalizeDeletion must be a no-op while finalizers remain")
	}

	live := env.seed("live")
	if err := env.rest.FinalizeDeletion(env.ctx, live, nil); err != nil {
		t.Fatalf("FinalizeDeletion on live claim: %v", err)
	}
	if env.stored("live") == nil {
		t.Fatalf("FinalizeDeletion removed a claim that is not being deleted")
	}

	now := metav1.Now()
	env.store.mu.Lock()
	stored := env.store.objects[testClaimKey("ns", "live")]
	stored.DeletionTimestamp = &now
	ready := stored.DeepCopy()
	env.store.mu.Unlock()

	if err := env.rest.FinalizeDeletion(env.ctx, ready, []string{metav1.DryRunAll}); err != nil {
		t.Fatalf("dry-run FinalizeDeletion: %v", err)
	}
	if env.stored("live") == nil || env.alloc.releaseCount() != 0 {
		t.Fatalf("dry-run FinalizeDeletion changed state")
	}
	if err := env.rest.FinalizeDeletion(env.ctx, ready, nil); err != nil {
		t.Fatalf("FinalizeDeletion: %v", err)
	}
	if env.stored("live") != nil || env.alloc.releaseCount() != 1 {
		t.Fatalf("FinalizeDeletion did not release and remove the claim")
	}
	// Idempotent once gone.
	if err := env.rest.FinalizeDeletion(env.ctx, ready, nil); err != nil {
		t.Fatalf("repeat FinalizeDeletion: %v", err)
	}
}

// flipToGranted is a rest.UpdatedObjectInfo that rewrites a claim's status
// to claim a large grant from an arbitrary bucket.
type flipToGranted struct{}

func (flipToGranted) Preconditions() *metav1.Preconditions { return nil }

func (flipToGranted) UpdatedObject(_ context.Context, old runtime.Object) (runtime.Object, error) {
	c := old.(*quota.QuotaClaim).DeepCopy()
	c.Status.Allocations = []quota.QuotaClaimAllocationStatus{{
		ResourceType:     "cpu",
		Status:           quota.QuotaClaimAllocationStatusGranted,
		Reason:           quota.QuotaClaimGrantedReason,
		AllocatedAmount:  1000,
		AllocatingBucket: "someone-elses-bucket",
	}}
	c.Status.Conditions = []metav1.Condition{{
		Type: quota.QuotaClaimGranted, Status: metav1.ConditionTrue, Reason: quota.QuotaClaimGrantedReason,
		Message: "forged", LastTransitionTime: metav1.Now(),
	}}
	return c, nil
}

// With a synchronous allocator, status writes cannot change the allocation
// outcome: release trusts it.
func TestAllocatingREST_StatusCannotChangeAllocationOutcome(t *testing.T) {
	env := newAllocTestEnv(t)
	env.seed("c")
	_, _, err := env.status.Update(env.ctx, "c", flipToGranted{}, rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("status update that forges a grant: got %v, want Invalid", err)
	}
	if got := env.stored("c"); len(got.Status.Allocations) != 0 {
		t.Fatalf("forged allocations were stored: %+v", got.Status.Allocations)
	}

	// Status changes that leave the outcome alone still work.
	other := statusConditionOnly{}
	if _, _, err := env.status.Update(env.ctx, "c", other, rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unrelated status update: %v", err)
	}
}

// statusConditionOnly adds a non-Granted condition.
type statusConditionOnly struct{}

func (statusConditionOnly) Preconditions() *metav1.Preconditions { return nil }

func (statusConditionOnly) UpdatedObject(_ context.Context, old runtime.Object) (runtime.Object, error) {
	c := old.(*quota.QuotaClaim).DeepCopy()
	c.Status.Conditions = append(c.Status.Conditions, metav1.Condition{
		Type: "Observed", Status: metav1.ConditionTrue, Reason: "Seen", Message: "ok", LastTransitionTime: metav1.Now(),
	})
	return c, nil
}
