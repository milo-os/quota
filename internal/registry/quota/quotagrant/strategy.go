package quotagrant

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"go.miloapis.com/quota/internal/metrics"
	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// quotaGrantStrategy implements behavior for QuotaGrant resources.
type quotaGrantStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// quotaGrantStatusStrategy implements behavior for QuotaGrant status updates.
type quotaGrantStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new QuotaGrant strategy.
func NewStrategy(typer runtime.ObjectTyper) quotaGrantStrategy {
	return quotaGrantStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new QuotaGrant status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) quotaGrantStatusStrategy {
	return quotaGrantStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns true because QuotaGrant is namespaced.
func (s quotaGrantStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears status before creation.
func (s quotaGrantStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	grant := obj.(*quota.QuotaGrant)
	grant.Status = quota.QuotaGrantStatus{}
	registryutil.SetGenerationOnCreate(&grant.ObjectMeta)
	metrics.QuotaGrantTotal.WithLabelValues("create").Inc()
}

// PrepareForUpdate preserves status when spec is updated and bumps
// metadata.generation on a spec change, which is what the bucket
// controllers watch to recompute limits.
func (s quotaGrantStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newGrant := obj.(*quota.QuotaGrant)
	oldGrant := old.(*quota.QuotaGrant)
	newGrant.Status = oldGrant.Status
	registryutil.BumpGenerationOnSpecChange(&newGrant.ObjectMeta, &oldGrant.ObjectMeta, newGrant.Spec, oldGrant.Spec)
	metrics.QuotaGrantTotal.WithLabelValues("update").Inc()
}

// Validate validates a new QuotaGrant.
func (s quotaGrantStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	grant := obj.(*quota.QuotaGrant)
	return append(registryutil.ValidateName(&grant.ObjectMeta), validateQuotaGrant(grant)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s quotaGrantStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaGrantStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaGrantStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s quotaGrantStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a QuotaGrant.
func (s quotaGrantStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newGrant := obj.(*quota.QuotaGrant)
	oldGrant := old.(*quota.QuotaGrant)

	allErrs := registryutil.Ratchet(validateQuotaGrant(newGrant), validateQuotaGrant(oldGrant))

	// consumerRef is immutable (Milo has no equivalent rule): buckets are keyed
	// by the whole consumer identity, namespace included, so re-pointing a
	// grant would silently move capacity to another bucket.
	if newGrant.Spec.ConsumerRef != oldGrant.Spec.ConsumerRef {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "consumerRef"),
			"consumerRef is immutable after creation; delete and recreate the grant to allocate quota to a different consumer",
		))
	}

	return allErrs
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s quotaGrantStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// validateQuotaGrant validates a QuotaGrant spec.
func validateQuotaGrant(grant *quota.QuotaGrant) field.ErrorList {
	return registryutil.ValidateGrantSpec(&grant.Spec, field.NewPath("spec"), true)
}

// GetAttrs returns labels and fields of a given QuotaGrant for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	grant, ok := obj.(*quota.QuotaGrant)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a QuotaGrant")
	}
	return grant.Labels, SelectableFields(grant), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(grant *quota.QuotaGrant) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&grant.ObjectMeta, true)
	specificFields := registryutil.ConsumerRefFields(grant.Spec.ConsumerRef)
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchQuotaGrant returns a matcher for QuotaGrant resources.
func MatchQuotaGrant(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns true.
func (s quotaGrantStatusStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForUpdate only allows status changes.
func (s quotaGrantStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newGrant := obj.(*quota.QuotaGrant)
	oldGrant := old.(*quota.QuotaGrant)
	newGrant.Spec = oldGrant.Spec
	newGrant.Labels = oldGrant.Labels
	newGrant.Annotations = oldGrant.Annotations
}

// activeConditionReasons ports Milo's status XValidation rule for Active.
var activeConditionReasons = []string{
	quota.QuotaGrantActiveReason,
	quota.QuotaGrantValidationFailedReason,
	quota.QuotaGrantPendingReason,
}

func validateQuotaGrantStatus(grant *quota.QuotaGrant) field.ErrorList {
	return registryutil.ValidateConditionReasons(grant.Status.Conditions, field.NewPath("status", "conditions"), quota.QuotaGrantActive, activeConditionReasons)
}

// ValidateUpdate validates a status update.
func (s quotaGrantStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return registryutil.Ratchet(
		validateQuotaGrantStatus(obj.(*quota.QuotaGrant)),
		validateQuotaGrantStatus(old.(*quota.QuotaGrant)),
	)
}

// WarningsOnUpdate returns warnings for the status update.
func (s quotaGrantStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaGrantStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaGrantStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s quotaGrantStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s quotaGrantStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
