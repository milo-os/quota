package quotadefinition

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

// QuotaDefinitionStorage implements a RESTStorage for QuotaDefinition.
type QuotaDefinitionStorage struct {
	*genericregistry.Store
}

// Delete overrides the embedded Store.Delete to record a QuotaDefinitionTotal
// "delete" metric before delegating. Create/update increments happen in the
// strategy hooks (PrepareForCreate/PrepareForUpdate).
func (s *QuotaDefinitionStorage) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	obj, deleted, err := s.Store.Delete(ctx, name, deleteValidation, options)
	if err == nil && deleted {
		metrics.QuotaDefinitionTotal.WithLabelValues("delete").Inc()
	}
	return obj, deleted, err
}

// QuotaDefinitionStatusStorage implements the REST endpoint for updating QuotaDefinition status.
type QuotaDefinitionStatusStorage struct {
	store *genericregistry.Store
}

// New creates a new QuotaDefinition object.
func (s *QuotaDefinitionStatusStorage) New() runtime.Object {
	return &quota.QuotaDefinition{}
}

// Destroy cleans up resources on shutdown.
func (s *QuotaDefinitionStatusStorage) Destroy() {}

// Get retrieves the object from the storage.
func (s *QuotaDefinitionStatusStorage) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
func (s *QuotaDefinitionStatusStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return s.store.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// NewStorage creates a new REST storage for QuotaDefinition backed by etcd.
func NewStorage(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*QuotaDefinitionStorage, *QuotaDefinitionStatusStorage, error) {
	strategy := NewStrategy(scheme)
	statusStrategy := NewStatusStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &quota.QuotaDefinition{} },
		NewListFunc:               func() runtime.Object { return &quota.QuotaDefinitionList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("quotadefinitions"),
		SingularQualifiedResource: v1alpha1.Resource("quotadefinition"),

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

	return &QuotaDefinitionStorage{store}, &QuotaDefinitionStatusStorage{store: &statusStore}, nil
}
