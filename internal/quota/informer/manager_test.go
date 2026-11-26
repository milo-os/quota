package informer

import (
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

type deleteRecorder struct{ deleted []string }

func (d *deleteRecorder) OnAdd(*unstructured.Unstructured)         {}
func (d *deleteRecorder) OnUpdate(_, _ *unstructured.Unstructured) {}
func (d *deleteRecorder) OnDelete(obj *unstructured.Unstructured) {
	d.deleted = append(d.deleted, obj.GetName())
}

// A delete missed during a watch outage arrives as a tombstone; it must reach
// consumers (it used to panic on the type assertion).
func TestDispatchDeleteUnwrapsTombstones(t *testing.T) {
	m := &dynamicInformerManager{logger: logr.Discard()}
	rec := &deleteRecorder{}
	inf := &managedInformer{handlers: map[string]ResourceEventHandler{"c": rec}}

	obj := &unstructured.Unstructured{}
	obj.SetName("gone")
	m.dispatchDelete(inf, obj)
	m.dispatchDelete(inf, cache.DeletedFinalStateUnknown{Key: "gone-too", Obj: func() *unstructured.Unstructured {
		u := obj.DeepCopy()
		u.SetName("gone-too")
		return u
	}()})
	m.dispatchDelete(inf, "not an object")

	if len(rec.deleted) != 2 || rec.deleted[0] != "gone" || rec.deleted[1] != "gone-too" {
		t.Fatalf("deleted = %v", rec.deleted)
	}
}
