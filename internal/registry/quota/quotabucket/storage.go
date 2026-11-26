package quotabucket

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaBucketStorage implements a RESTStorage for QuotaBucket.
type QuotaBucketStorage struct {
	*genericregistry.Store
}

// QuotaBucketStatusStorage implements the REST endpoint for updating QuotaBucket status.
type QuotaBucketStatusStorage struct {
	store *genericregistry.Store
}

// New creates a new QuotaBucket object.
func (s *QuotaBucketStatusStorage) New() runtime.Object {
	return &quota.QuotaBucket{}
}

// Destroy cleans up resources on shutdown.
func (s *QuotaBucketStatusStorage) Destroy() {}

// Get retrieves the object from the storage.
func (s *QuotaBucketStatusStorage) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.store.Get(ctx, name, options)
}

// Update alters the status subset of an object. forceAllowCreate is pinned
// to false so that Server-Side Apply patches targeting a missing object
// return NotFound instead of routing through the (spec) CreateStrategy and
// rejecting the status-only payload on full-object validation.
func (s *QuotaBucketStatusStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return s.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy so the apiserver's
// field manager knows to exclude spec when merging status-subresource
// Server-Side Apply patches.
func (s *QuotaBucketStatusStorage) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return s.store.GetResetFields()
}

// ConvertToTable delegates to the underlying store so table output works
// on the status subresource endpoint.
func (s *QuotaBucketStatusStorage) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return s.store.ConvertToTable(ctx, object, tableOptions)
}

// NewStorage creates a new REST storage for QuotaBucket backed by etcd.
func NewStorage(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*QuotaBucketStorage, *QuotaBucketStatusStorage, error) {
	strategy := NewStrategy(scheme)
	statusStrategy := NewStatusStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &quota.QuotaBucket{} },
		NewListFunc:               func() runtime.Object { return &quota.QuotaBucketList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("quotabuckets"),
		SingularQualifiedResource: v1alpha1.Resource("quotabucket"),

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

	return &QuotaBucketStorage{store}, &QuotaBucketStatusStorage{store: &statusStore}, nil
}
