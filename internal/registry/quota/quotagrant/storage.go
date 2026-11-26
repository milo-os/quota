package quotagrant

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/internal/metrics"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaGrantStorage implements a RESTStorage for QuotaGrant.
type QuotaGrantStorage struct {
	*genericregistry.Store
}

// Delete overrides the embedded Store.Delete to record a QuotaGrantTotal
// "delete" metric before delegating. Create/update increments happen in the
// strategy hooks (PrepareForCreate/PrepareForUpdate).
func (s *QuotaGrantStorage) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	obj, deleted, err := s.Store.Delete(ctx, name, deleteValidation, options)
	if err == nil && deleted {
		metrics.QuotaGrantTotal.WithLabelValues("delete").Inc()
	}
	return obj, deleted, err
}

// QuotaGrantStatusStorage implements the REST endpoint for updating QuotaGrant status.
type QuotaGrantStatusStorage struct {
	store *genericregistry.Store
}

// New creates a new QuotaGrant object.
func (s *QuotaGrantStatusStorage) New() runtime.Object {
	return &quota.QuotaGrant{}
}

// Destroy cleans up resources on shutdown.
func (s *QuotaGrantStatusStorage) Destroy() {}

// Get retrieves the object from the storage.
func (s *QuotaGrantStatusStorage) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.store.Get(ctx, name, options)
}

// Update alters the status subset of an object. forceAllowCreate is pinned
// to false so that Server-Side Apply patches targeting a missing object
// return NotFound instead of creating a grant through the status endpoint.
func (s *QuotaGrantStatusStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, _ bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return s.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// NewStorage creates a new REST storage for QuotaGrant backed by etcd.
func NewStorage(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*QuotaGrantStorage, *QuotaGrantStatusStorage, error) {
	strategy := NewStrategy(scheme)
	statusStrategy := NewStatusStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &quota.QuotaGrant{} },
		NewListFunc:               func() runtime.Object { return &quota.QuotaGrantList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("quotagrants"),
		SingularQualifiedResource: v1alpha1.Resource("quotagrant"),

		CreateStrategy: strategy,
		UpdateStrategy: strategy,
		DeleteStrategy: strategy,

		TableConvertor: NewTableConvertor(),
	}

	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    GetAttrs,
	}

	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = statusStrategy
	statusStore.ResetFieldsStrategy = statusStrategy

	return &QuotaGrantStorage{store}, &QuotaGrantStatusStorage{store: &statusStore}, nil
}
