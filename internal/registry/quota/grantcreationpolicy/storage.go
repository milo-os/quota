package grantcreationpolicy

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// GrantCreationPolicyStorage implements a RESTStorage for GrantCreationPolicy.
type GrantCreationPolicyStorage struct {
	*genericregistry.Store
}

// GrantCreationPolicyStatusStorage implements the REST endpoint for updating GrantCreationPolicy status.
type GrantCreationPolicyStatusStorage struct {
	store *genericregistry.Store
}

// New creates a new GrantCreationPolicy object.
func (s *GrantCreationPolicyStatusStorage) New() runtime.Object {
	return &quota.GrantCreationPolicy{}
}

// Destroy cleans up resources on shutdown.
func (s *GrantCreationPolicyStatusStorage) Destroy() {}

// Get retrieves the object from the storage.
func (s *GrantCreationPolicyStatusStorage) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
func (s *GrantCreationPolicyStatusStorage) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return s.store.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// NewStorage creates a new REST storage for GrantCreationPolicy backed by etcd.
func NewStorage(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*GrantCreationPolicyStorage, *GrantCreationPolicyStatusStorage, error) {
	strategy := NewStrategy(scheme)
	statusStrategy := NewStatusStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &quota.GrantCreationPolicy{} },
		NewListFunc:               func() runtime.Object { return &quota.GrantCreationPolicyList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("grantcreationpolicies"),
		SingularQualifiedResource: v1alpha1.Resource("grantcreationpolicy"),

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

	return &GrantCreationPolicyStorage{store}, &GrantCreationPolicyStatusStorage{store: &statusStore}, nil
}
