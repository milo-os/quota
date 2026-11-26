package quotaclaim

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// New returns an empty *quota.QuotaClaim. Required by rest.Storage.
func (r *PostgresNativeStatusREST) New() runtime.Object { return &quota.QuotaClaim{} }

// Destroy is a no-op; the parent PostgresNativeREST owns the pool.
func (r *PostgresNativeStatusREST) Destroy() {}

// NamespaceScoped mirrors the main claim storage scope.
func (r *PostgresNativeStatusREST) NamespaceScoped() bool { return true }

// Get delegates to the main storage; /status GET returns the full object.
func (r *PostgresNativeStatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.parent.Get(ctx, name, options)
}

// ConvertToTable delegates to the main storage so kubectl table output works
// on the /status endpoint.
func (r *PostgresNativeStatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.parent.ConvertToTable(ctx, object, tableOptions)
}

// GetResetFields tells the field manager to reset spec on status-subresource
// server-side apply patches. This is the same set returned by
// quotaClaimStatusStrategy.GetResetFields.
func (r *PostgresNativeStatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.statusStrategy.GetResetFields()
}
