package quotabucket

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

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// quotaBucketStrategy implements behavior for QuotaBucket resources.
type quotaBucketStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// quotaBucketStatusStrategy implements behavior for QuotaBucket status updates.
type quotaBucketStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new QuotaBucket strategy.
func NewStrategy(typer runtime.ObjectTyper) quotaBucketStrategy {
	return quotaBucketStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new QuotaBucket status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) quotaBucketStatusStrategy {
	return quotaBucketStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns true because QuotaBucket is namespaced.
func (s quotaBucketStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears status before creation.
func (s quotaBucketStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	bucket := obj.(*quota.QuotaBucket)
	bucket.Status = quota.QuotaBucketStatus{}
	registryutil.SetGenerationOnCreate(&bucket.ObjectMeta)
}

// PrepareForUpdate preserves status when spec is updated.
func (s quotaBucketStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newBucket := obj.(*quota.QuotaBucket)
	oldBucket := old.(*quota.QuotaBucket)
	newBucket.Status = oldBucket.Status
	registryutil.BumpGenerationOnSpecChange(&newBucket.ObjectMeta, &oldBucket.ObjectMeta, newBucket.Spec, oldBucket.Spec)
}

// Validate validates a new QuotaBucket.
func (s quotaBucketStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	bucket := obj.(*quota.QuotaBucket)
	return append(registryutil.ValidateName(&bucket.ObjectMeta), validateQuotaBucket(bucket)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s quotaBucketStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaBucketStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaBucketStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s quotaBucketStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a QuotaBucket.
func (s quotaBucketStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newBucket := obj.(*quota.QuotaBucket)
	oldBucket := old.(*quota.QuotaBucket)

	allErrs := registryutil.Ratchet(validateQuotaBucket(newBucket), validateQuotaBucket(oldBucket))

	// Immutable fields: a bucket is the aggregate for one (consumer,
	// resourceType) pair, so its identity cannot change.
	if newBucket.Spec.ResourceType != oldBucket.Spec.ResourceType {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "resourceType"), "resourceType is immutable after creation"))
	}
	if newBucket.Spec.ConsumerRef != oldBucket.Spec.ConsumerRef {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "consumerRef"), "consumerRef is immutable after creation"))
	}

	return allErrs
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s quotaBucketStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// validateQuotaBucket validates a QuotaBucket.
func validateQuotaBucket(bucket *quota.QuotaBucket) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	ref := bucket.Spec.ConsumerRef
	allErrs = append(allErrs, registryutil.ValidateConsumerRef(ref.APIGroup, ref.Kind, ref.Name, ref.Namespace, specPath.Child("consumerRef"), true)...)
	allErrs = registryutil.ValidateResourceType(allErrs, specPath.Child("resourceType"), bucket.Spec.ResourceType)

	return allErrs
}

// GetAttrs returns labels and fields of a given QuotaBucket for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	bucket, ok := obj.(*quota.QuotaBucket)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a QuotaBucket")
	}
	return bucket.Labels, SelectableFields(bucket), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(bucket *quota.QuotaBucket) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&bucket.ObjectMeta, true)
	specificFields := registryutil.ConsumerRefFields(bucket.Spec.ConsumerRef)
	specificFields["spec.resourceType"] = bucket.Spec.ResourceType
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchQuotaBucket returns a matcher for QuotaBucket resources.
func MatchQuotaBucket(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns true.
func (s quotaBucketStatusStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForUpdate only allows status changes.
func (s quotaBucketStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newBucket := obj.(*quota.QuotaBucket)
	oldBucket := old.(*quota.QuotaBucket)
	newBucket.Spec = oldBucket.Spec
	newBucket.Labels = oldBucket.Labels
	newBucket.Annotations = oldBucket.Annotations
}

// validateQuotaBucketStatus ports Milo's `Minimum=0` markers on status.
func validateQuotaBucketStatus(bucket *quota.QuotaBucket) field.ErrorList {
	var errs field.ErrorList
	p := field.NewPath("status")
	nonNegative := func(name string, v int64) {
		if v < 0 {
			errs = append(errs, field.Invalid(p.Child(name), v, "must be greater than or equal to 0"))
		}
	}
	nonNegative("limit", bucket.Status.Limit)
	nonNegative("allocated", bucket.Status.Allocated)
	nonNegative("available", bucket.Status.Available)
	nonNegative("claimCount", int64(bucket.Status.ClaimCount))
	nonNegative("grantCount", int64(bucket.Status.GrantCount))
	for i, ref := range bucket.Status.ContributingGrantRefs {
		if ref.Amount < 0 {
			errs = append(errs, field.Invalid(p.Child("contributingGrantRefs").Index(i).Child("amount"), ref.Amount, "must be greater than or equal to 0"))
		}
	}
	return errs
}

// ValidateUpdate validates a status update.
func (s quotaBucketStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return registryutil.Ratchet(
		validateQuotaBucketStatus(obj.(*quota.QuotaBucket)),
		validateQuotaBucketStatus(old.(*quota.QuotaBucket)),
	)
}

// WarningsOnUpdate returns warnings for the status update.
func (s quotaBucketStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaBucketStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaBucketStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s quotaBucketStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s quotaBucketStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
