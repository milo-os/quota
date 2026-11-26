package dual

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// testObj is a minimal object with real ObjectMeta so RV handling is
// exercised.
type testObj struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              string `json:"spec,omitempty"`
	Status            string `json:"status,omitempty"`
}

func (o *testObj) DeepCopyObject() runtime.Object {
	c := *o
	o.ObjectMeta.DeepCopyInto(&c.ObjectMeta) //nolint:staticcheck // explicit: testObj has no DeepCopyInto of its own
	return &c
}

type testObjList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []testObj `json:"items"`
}

func (l *testObjList) DeepCopyObject() runtime.Object {
	c := &testObjList{TypeMeta: l.TypeMeta}
	l.ListMeta.DeepCopyInto(&c.ListMeta) //nolint:staticcheck // explicit embedded-field copy
	for i := range l.Items {
		c.Items = append(c.Items, *l.Items[i].DeepCopyObject().(*testObj))
	}
	return c
}

const testPrefix = "/quota.miloapis.com/tests"

func testKey(name string) string { return testPrefix + "/" + name }

func testKeyFunc(obj runtime.Object) (string, error) {
	return testKey(obj.(*testObj).Name), nil
}

func newTestList() runtime.Object { return &testObjList{} }

func newTestObj(name, spec string) *testObj {
	return &testObj{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)}, Spec: spec}
}

// memStore is an in-memory storage.Interface with its own RV sequence. It
// models the parts of etcd/Postgres semantics that matter here: RV
// assignment, preconditions, ignoreNotFound and conflicts.
type memStore struct {
	mu      sync.Mutex
	rv      uint64
	objs    map[string]*testObj
	updates int // GuaranteedUpdate calls
	creates int
	deletes int
	// lastDeletePreconditions records what the last Delete received.
	lastDeletePreconditions *storage.Preconditions
}

func newMemStore(rvStart uint64) *memStore {
	return &memStore{rv: rvStart, objs: map[string]*testObj{}}
}

func (m *memStore) next() uint64 { m.rv++; return m.rv }

func (m *memStore) put(name, spec string) *testObj {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := newTestObj(name, spec)
	o.ResourceVersion = strconv.FormatUint(m.next(), 10)
	m.objs[testKey(name)] = o
	return o.DeepCopyObject().(*testObj)
}

func (m *memStore) peek(name string) (*testObj, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[testKey(name)]
	if !ok {
		return nil, false
	}
	return o.DeepCopyObject().(*testObj), true
}

func copyInto(src *testObj, dst runtime.Object) {
	*dst.(*testObj) = *src.DeepCopyObject().(*testObj)
}

func (m *memStore) Versioner() storage.Versioner { return storage.APIObjectVersioner{} }

func (m *memStore) Create(_ context.Context, key string, obj, out runtime.Object, _ uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates++
	if _, ok := m.objs[key]; ok {
		return storage.NewKeyExistsError(key, 0)
	}
	o := obj.DeepCopyObject().(*testObj)
	o.ResourceVersion = strconv.FormatUint(m.next(), 10)
	m.objs[key] = o
	copyInto(o, out)
	return nil
}

func (m *memStore) Delete(ctx context.Context, key string, out runtime.Object, pre *storage.Preconditions, validate storage.ValidateObjectFunc, _ runtime.Object, _ storage.DeleteOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes++
	m.lastDeletePreconditions = pre
	o, ok := m.objs[key]
	if !ok {
		return storage.NewKeyNotFoundError(key, 0)
	}
	if pre != nil {
		if err := pre.Check(key, o); err != nil {
			return err
		}
	}
	if validate != nil {
		if err := validate(ctx, o); err != nil {
			return err
		}
	}
	delete(m.objs, key)
	copyInto(o, out)
	return nil
}

func (m *memStore) Watch(context.Context, string, storage.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (m *memStore) Get(_ context.Context, key string, _ storage.GetOptions, objPtr runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return storage.NewKeyNotFoundError(key, 0)
	}
	copyInto(o, objPtr)
	return nil
}

func (m *memStore) GetList(_ context.Context, key string, _ storage.ListOptions, listObj runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := listObj.(*testObjList)
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, key+"/") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		l.Items = append(l.Items, *m.objs[k].DeepCopyObject().(*testObj))
	}
	l.ResourceVersion = strconv.FormatUint(m.rv, 10)
	return nil
}

func (m *memStore) GuaranteedUpdate(_ context.Context, key string, dest runtime.Object, ignoreNotFound bool, pre *storage.Preconditions, tryUpdate storage.UpdateFunc, _ runtime.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates++
	existing, ok := m.objs[key]
	var curRV uint64
	if !ok {
		if !ignoreNotFound {
			return storage.NewKeyNotFoundError(key, 0)
		}
		existing = &testObj{}
	} else {
		curRV, _ = strconv.ParseUint(existing.ResourceVersion, 10, 64)
	}
	if pre != nil {
		if err := pre.Check(key, existing); err != nil {
			return err
		}
	}
	ret, _, err := tryUpdate(existing.DeepCopyObject(), storage.ResponseMeta{ResourceVersion: curRV})
	if err != nil {
		return err
	}
	o := ret.DeepCopyObject().(*testObj)
	o.ResourceVersion = strconv.FormatUint(m.next(), 10)
	m.objs[key] = o
	copyInto(o, dest)
	return nil
}

func (m *memStore) Stats(context.Context) (storage.Stats, error) { return storage.Stats{}, nil }
func (m *memStore) GetCurrentResourceVersion(context.Context) (uint64, error) {
	return m.rv, nil
}
func (m *memStore) EnableResourceSizeEstimation(storage.KeysFunc) error { return nil }
func (m *memStore) CompactRevision() int64                              { return 0 }
func (m *memStore) ReadinessCheck() error                               { return nil }
func (m *memStore) RequestWatchProgress(context.Context) error          { return nil }
