package dual

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// mockWatcher implements watch.Interface for testing.
type mockWatcher struct {
	ch   chan watch.Event
	done chan struct{}
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{
		ch:   make(chan watch.Event, 10),
		done: make(chan struct{}),
	}
}

func (w *mockWatcher) ResultChan() <-chan watch.Event { return w.ch }
func (w *mockWatcher) Stop()                          { close(w.done) }

// mockStorage implements storage.Interface for testing.
type mockStorage struct {
	createFunc                       func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error
	deleteFunc                       func(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error
	watchFunc                        func(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error)
	getFunc                          func(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error
	getListFunc                      func(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error
	guaranteedUpdateFunc             func(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error
	statsFunc                        func(ctx context.Context) (storage.Stats, error)
	getCurrentResourceVersionFunc    func(ctx context.Context) (uint64, error)
	enableResourceSizeEstimationFunc func(storage.KeysFunc) error
	compactRevisionFunc              func() int64
	readinessCheckFunc               func() error
	requestWatchFunc                 func(ctx context.Context) error
	versionerFunc                    func() storage.Versioner
}

func (m *mockStorage) Versioner() storage.Versioner {
	if m.versionerFunc != nil {
		return m.versionerFunc()
	}
	return storage.APIObjectVersioner{}
}

func (m *mockStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, key, obj, out, ttl)
	}
	return nil
}

func (m *mockStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
	}
	return nil
}

func (m *mockStorage) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	if m.watchFunc != nil {
		return m.watchFunc(ctx, key, opts)
	}
	return newMockWatcher(), nil
}

func (m *mockStorage) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	if m.getFunc != nil {
		return m.getFunc(ctx, key, opts, objPtr)
	}
	return nil
}

func (m *mockStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	if m.getListFunc != nil {
		return m.getListFunc(ctx, key, opts, listObj)
	}
	return nil
}

func (m *mockStorage) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	if m.guaranteedUpdateFunc != nil {
		return m.guaranteedUpdateFunc(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject)
	}
	return nil
}

func (m *mockStorage) Stats(ctx context.Context) (storage.Stats, error) {
	if m.statsFunc != nil {
		return m.statsFunc(ctx)
	}
	return storage.Stats{}, nil
}

func (m *mockStorage) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	if m.getCurrentResourceVersionFunc != nil {
		return m.getCurrentResourceVersionFunc(ctx)
	}
	return 0, nil
}

func (m *mockStorage) EnableResourceSizeEstimation(f storage.KeysFunc) error {
	if m.enableResourceSizeEstimationFunc != nil {
		return m.enableResourceSizeEstimationFunc(f)
	}
	return nil
}

func (m *mockStorage) CompactRevision() int64 {
	if m.compactRevisionFunc != nil {
		return m.compactRevisionFunc()
	}
	return 0
}

func (m *mockStorage) ReadinessCheck() error {
	if m.readinessCheckFunc != nil {
		return m.readinessCheckFunc()
	}
	return nil
}

func (m *mockStorage) RequestWatchProgress(ctx context.Context) error {
	if m.requestWatchFunc != nil {
		return m.requestWatchFunc(ctx)
	}
	return nil
}

// simpleObject is a trivial runtime.Object for dual store testing.
type simpleObject struct {
	runtime.TypeMeta
}

func (s *simpleObject) DeepCopyObject() runtime.Object {
	return &simpleObject{TypeMeta: s.TypeMeta}
}

func TestDualStore_Create(t *testing.T) {
	t.Run("succeeds when both primary and shadow succeed", func(t *testing.T) {
		var primaryCalled, shadowCalled bool
		primary := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				primaryCalled = true
				return nil
			},
		}
		shadow := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		obj := &simpleObject{}
		out := &simpleObject{}
		err := store.Create(context.Background(), "/test/key", obj, out, 0)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if !primaryCalled {
			t.Error("primary Create was not called")
		}
		if !shadowCalled {
			t.Error("shadow Create was not called")
		}
	})

	t.Run("succeeds when primary succeeds but shadow fails", func(t *testing.T) {
		primary := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				return nil
			},
		}
		shadow := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				return fmt.Errorf("shadow failure")
			},
		}

		store := New(primary, shadow)
		obj := &simpleObject{}
		out := &simpleObject{}
		err := store.Create(context.Background(), "/test/key", obj, out, 0)
		if err != nil {
			t.Fatalf("Create should succeed despite shadow failure, got: %v", err)
		}
	})

	t.Run("fails when primary fails", func(t *testing.T) {
		primaryErr := fmt.Errorf("primary failure")
		primary := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				return primaryErr
			},
		}
		shadowCalled := false
		shadow := &mockStorage{
			createFunc: func(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		obj := &simpleObject{}
		out := &simpleObject{}
		err := store.Create(context.Background(), "/test/key", obj, out, 0)
		if err != primaryErr {
			t.Fatalf("expected primary error, got: %v", err)
		}
		if shadowCalled {
			t.Error("shadow should not be called when primary fails")
		}
	})
}

func TestDualStore_Get(t *testing.T) {
	t.Run("reads from primary only", func(t *testing.T) {
		primaryCalled := false
		shadowCalled := false

		primary := &mockStorage{
			getFunc: func(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
				primaryCalled = true
				return nil
			},
		}
		shadow := &mockStorage{
			getFunc: func(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		err := store.Get(context.Background(), "/test/key", storage.GetOptions{}, &simpleObject{})
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !primaryCalled {
			t.Error("primary Get was not called")
		}
		if shadowCalled {
			t.Error("shadow Get should not be called")
		}
	})
}

func TestDualStore_GetList(t *testing.T) {
	t.Run("reads from primary only", func(t *testing.T) {
		primaryCalled := false
		shadowCalled := false

		primary := &mockStorage{
			getListFunc: func(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				primaryCalled = true
				return nil
			},
		}
		shadow := &mockStorage{
			getListFunc: func(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		err := store.GetList(context.Background(), "/test/key", storage.ListOptions{}, &simpleObject{})
		if err != nil {
			t.Fatalf("GetList failed: %v", err)
		}
		if !primaryCalled {
			t.Error("primary GetList was not called")
		}
		if shadowCalled {
			t.Error("shadow GetList should not be called")
		}
	})
}

func TestDualStore_Delete(t *testing.T) {
	t.Run("deletes from both, shadow failure does not fail operation", func(t *testing.T) {
		var primaryCalled, shadowCalled bool
		primary := &mockStorage{
			deleteFunc: func(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
				primaryCalled = true
				return nil
			},
		}
		shadow := &mockStorage{
			deleteFunc: func(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
				shadowCalled = true
				return fmt.Errorf("shadow delete failure")
			},
		}

		store := New(primary, shadow)
		out := &simpleObject{}
		err := store.Delete(context.Background(), "/test/key", out, nil, nil, nil, storage.DeleteOptions{})
		if err != nil {
			t.Fatalf("Delete should succeed despite shadow failure, got: %v", err)
		}
		if !primaryCalled {
			t.Error("primary Delete was not called")
		}
		if !shadowCalled {
			t.Error("shadow Delete was not called")
		}
	})

	t.Run("fails when primary fails", func(t *testing.T) {
		primaryErr := fmt.Errorf("primary delete failure")
		primary := &mockStorage{
			deleteFunc: func(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
				return primaryErr
			},
		}
		shadowCalled := false
		shadow := &mockStorage{
			deleteFunc: func(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		out := &simpleObject{}
		err := store.Delete(context.Background(), "/test/key", out, nil, nil, nil, storage.DeleteOptions{})
		if err != primaryErr {
			t.Fatalf("expected primary error, got: %v", err)
		}
		if shadowCalled {
			t.Error("shadow should not be called when primary fails")
		}
	})
}

func TestDualStore_GuaranteedUpdate(t *testing.T) {
	t.Run("updates both, shadow failure does not fail operation", func(t *testing.T) {
		var primaryCalled, shadowCalled bool
		primary := &mockStorage{
			guaranteedUpdateFunc: func(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
				primaryCalled = true
				return nil
			},
		}
		shadow := &mockStorage{
			guaranteedUpdateFunc: func(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
				shadowCalled = true
				return fmt.Errorf("shadow update failure")
			},
		}

		store := New(primary, shadow)
		dest := &simpleObject{}
		updateFn := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}
		err := store.GuaranteedUpdate(context.Background(), "/test/key", dest, false, nil, updateFn, nil)
		if err != nil {
			t.Fatalf("GuaranteedUpdate should succeed despite shadow failure, got: %v", err)
		}
		if !primaryCalled {
			t.Error("primary GuaranteedUpdate was not called")
		}
		if !shadowCalled {
			t.Error("shadow GuaranteedUpdate was not called")
		}
	})

	t.Run("fails when primary fails", func(t *testing.T) {
		primaryErr := fmt.Errorf("primary update failure")
		primary := &mockStorage{
			guaranteedUpdateFunc: func(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
				return primaryErr
			},
		}
		shadowCalled := false
		shadow := &mockStorage{
			guaranteedUpdateFunc: func(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
				shadowCalled = true
				return nil
			},
		}

		store := New(primary, shadow)
		dest := &simpleObject{}
		updateFn := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}
		err := store.GuaranteedUpdate(context.Background(), "/test/key", dest, false, nil, updateFn, nil)
		if err != primaryErr {
			t.Fatalf("expected primary error, got: %v", err)
		}
		if shadowCalled {
			t.Error("shadow should not be called when primary fails")
		}
	})
}

func TestDualStore_Watch(t *testing.T) {
	t.Run("delegates to primary only", func(t *testing.T) {
		expectedWatcher := newMockWatcher()
		primaryCalled := false
		shadowCalled := false

		primary := &mockStorage{
			watchFunc: func(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
				primaryCalled = true
				return expectedWatcher, nil
			},
		}
		shadow := &mockStorage{
			watchFunc: func(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
				shadowCalled = true
				return newMockWatcher(), nil
			},
		}

		store := New(primary, shadow)
		w, err := store.Watch(context.Background(), "/test/key", storage.ListOptions{})
		if err != nil {
			t.Fatalf("Watch failed: %v", err)
		}
		if w != expectedWatcher {
			t.Error("expected the primary watcher to be returned")
		}
		if !primaryCalled {
			t.Error("primary Watch was not called")
		}
		if shadowCalled {
			t.Error("shadow Watch should not be called")
		}
	})
}

func TestDualStore_Stats(t *testing.T) {
	t.Run("reads from primary only", func(t *testing.T) {
		primary := &mockStorage{
			statsFunc: func(ctx context.Context) (storage.Stats, error) {
				return storage.Stats{ObjectCount: 42}, nil
			},
		}
		shadow := &mockStorage{
			statsFunc: func(ctx context.Context) (storage.Stats, error) {
				return storage.Stats{ObjectCount: 99}, nil
			},
		}

		store := New(primary, shadow)
		stats, err := store.Stats(context.Background())
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}
		if stats.ObjectCount != 42 {
			t.Errorf("expected count from primary (42), got %d", stats.ObjectCount)
		}
	})
}

func TestDualStore_Versioner(t *testing.T) {
	primary := &mockStorage{
		versionerFunc: func() storage.Versioner {
			return storage.APIObjectVersioner{}
		},
	}
	shadow := &mockStorage{}

	store := New(primary, shadow)
	v := store.Versioner()
	if v == nil {
		t.Error("expected non-nil Versioner")
	}
}

func TestDualStore_ReadinessCheck(t *testing.T) {
	t.Run("returns primary error", func(t *testing.T) {
		primaryErr := fmt.Errorf("primary unhealthy")
		primary := &mockStorage{
			readinessCheckFunc: func() error {
				return primaryErr
			},
		}
		shadow := &mockStorage{
			readinessCheckFunc: func() error {
				return nil
			},
		}

		store := New(primary, shadow)
		err := store.ReadinessCheck()
		if err != primaryErr {
			t.Errorf("expected primary error, got: %v", err)
		}
	})

	t.Run("succeeds when primary healthy, shadow unhealthy", func(t *testing.T) {
		primary := &mockStorage{
			readinessCheckFunc: func() error {
				return nil
			},
		}
		shadow := &mockStorage{
			readinessCheckFunc: func() error {
				return fmt.Errorf("shadow unhealthy")
			},
		}

		store := New(primary, shadow)
		err := store.ReadinessCheck()
		if err != nil {
			t.Errorf("expected nil error when primary healthy, got: %v", err)
		}
	})
}

func TestDualStore_RequestWatchProgress(t *testing.T) {
	primaryCalled := false
	primary := &mockStorage{
		requestWatchFunc: func(ctx context.Context) error {
			primaryCalled = true
			return nil
		},
	}
	shadow := &mockStorage{}

	store := New(primary, shadow)
	err := store.RequestWatchProgress(context.Background())
	if err != nil {
		t.Fatalf("RequestWatchProgress failed: %v", err)
	}
	if !primaryCalled {
		t.Error("primary RequestWatchProgress was not called")
	}
}
