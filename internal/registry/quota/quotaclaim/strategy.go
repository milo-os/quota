package quotaclaim

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// quotaClaimStrategy implements behavior for QuotaClaim resources.
type quotaClaimStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// quotaClaimStatusStrategy implements behavior for QuotaClaim status updates.
type quotaClaimStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new QuotaClaim strategy.
func NewStrategy(typer runtime.ObjectTyper) quotaClaimStrategy {
	return quotaClaimStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new QuotaClaim status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) quotaClaimStatusStrategy {
	return quotaClaimStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns true because QuotaClaim is namespaced.
func (s quotaClaimStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate resets status to the pending state Milo's CRD defaulted
// (`Granted=False`, reason PendingEvaluation). The Postgres allocator replaces
// this condition with the final decision via SetStatusCondition inside the
// create transaction; on the etcd path the claim controller does.
func (s quotaClaimStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	claim := obj.(*quota.QuotaClaim)
	claim.Status = quota.QuotaClaimStatus{
		Conditions: []metav1.Condition{PendingCondition(metav1.Now())},
	}
	registryutil.SetGenerationOnCreate(&claim.ObjectMeta)
}

// PendingCondition is the initial Granted condition of every new claim.
func PendingCondition(now metav1.Time) metav1.Condition {
	return metav1.Condition{
		Type:               quota.QuotaClaimGranted,
		Status:             metav1.ConditionFalse,
		Reason:             quota.QuotaClaimPendingReason,
		Message:            "Awaiting capacity evaluation",
		LastTransitionTime: now,
	}
}

// PrepareForUpdate preserves status when spec is updated.
func (s quotaClaimStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*quota.QuotaClaim)
	oldClaim := old.(*quota.QuotaClaim)
	newClaim.Status = oldClaim.Status
	// The claim spec is immutable (see ValidateUpdate), so this only bumps on
	// an update that validation then rejects. It keeps the generation
	// contract identical across kinds.
	registryutil.BumpGenerationOnSpecChange(&newClaim.ObjectMeta, &oldClaim.ObjectMeta, newClaim.Spec, oldClaim.Spec)
}

// Validate validates a new QuotaClaim.
func (s quotaClaimStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	claim := obj.(*quota.QuotaClaim)
	return append(registryutil.ValidateName(&claim.ObjectMeta), validateQuotaClaim(claim)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s quotaClaimStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaClaimStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaClaimStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s quotaClaimStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a QuotaClaim.
func (s quotaClaimStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newClaim := obj.(*quota.QuotaClaim)
	oldClaim := old.(*quota.QuotaClaim)

	// Ratchet so claims stored before the consumerRef/maxItems rules existed
	// can still have metadata (finalizers, labels) updated.
	allErrs := registryutil.Ratchet(validateQuotaClaim(newClaim), validateQuotaClaim(oldClaim))

	// Immutable fields. Milo has no equivalent: the ledger computes
	// allocations once at create time, so changing who or what a claim is
	// for, or how much it asks for, would desynchronise bucket totals.
	if newClaim.Spec.ConsumerRef != oldClaim.Spec.ConsumerRef {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "consumerRef"),
			"consumerRef is immutable after creation; delete and recreate the claim to charge a different consumer",
		))
	}
	if newClaim.Spec.ResourceRef != oldClaim.Spec.ResourceRef {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "resourceRef"),
			"resourceRef is immutable after creation; delete and recreate the claim to reference a different resource",
		))
	}
	if !equality.Semantic.DeepEqual(oldClaim.Spec.Requests, newClaim.Spec.Requests) {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec", "requests"),
			"requests is immutable after creation; allocations are computed once at create time, so delete and recreate the claim to change amounts",
		))
	}

	return allErrs
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s quotaClaimStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// validateQuotaClaim validates a QuotaClaim.
func validateQuotaClaim(claim *quota.QuotaClaim) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	// consumerRef is required by the quota service (Milo left it optional for
	// its admission plugin to fill in). An empty consumer would pool every
	// such claim into one bucket.
	ref := claim.Spec.ConsumerRef
	allErrs = append(allErrs, registryutil.ValidateConsumerRef(ref.APIGroup, ref.Kind, ref.Name, ref.Namespace, specPath.Child("consumerRef"), true)...)

	// resourceRef is optional (Milo: `+optional`), but when any field is set it
	// must identify a resource: kind and name are required, as in Milo's
	// UnversionedObjectReference schema.
	allErrs = append(allErrs, validateResourceRef(claim.Spec.ResourceRef, specPath.Child("resourceRef"))...)

	allErrs = append(allErrs, registryutil.ValidateRequests(claim.Spec.Requests, specPath.Child("requests"))...)

	return allErrs
}

// validateResourceRef treats the zero value as "not set" and otherwise
// requires both kind and name.
func validateResourceRef(ref quota.UnversionedObjectReference, path *field.Path) field.ErrorList {
	if ref == (quota.UnversionedObjectReference{}) {
		return nil
	}
	var errs field.ErrorList
	if ref.Kind == "" {
		errs = append(errs, field.Required(path.Child("kind"), "resourceRef.kind is required when resourceRef is set"))
	}
	if ref.Name == "" {
		errs = append(errs, field.Required(path.Child("name"), "resourceRef.name is required when resourceRef is set"))
	}
	errs = registryutil.MaxLen(errs, path.Child("apiGroup"), ref.APIGroup, registryutil.MaxNameLength)
	errs = registryutil.MaxLen(errs, path.Child("kind"), ref.Kind, registryutil.MaxKindLength)
	errs = registryutil.MaxLen(errs, path.Child("name"), ref.Name, registryutil.MaxNameLength)
	errs = registryutil.MaxLen(errs, path.Child("namespace"), ref.Namespace, registryutil.MaxNamespaceLength)
	return errs
}

// grantedConditionReasons ports Milo's status XValidation rule for Granted.
var grantedConditionReasons = []string{
	quota.QuotaClaimGrantedReason,
	quota.QuotaClaimDeniedReason,
	quota.QuotaClaimValidationFailedReason,
	quota.QuotaClaimPendingReason,
}

var allocationStatuses = []string{
	quota.QuotaClaimAllocationStatusGranted,
	quota.QuotaClaimAllocationStatusDenied,
	quota.QuotaClaimAllocationStatusPending,
}

// validateQuotaClaimStatus ports Milo's status schema: the Granted reason
// enum, metav1.Condition shape, and allocation entry constraints.
func validateQuotaClaimStatus(claim *quota.QuotaClaim) field.ErrorList {
	statusPath := field.NewPath("status")
	allErrs := registryutil.ValidateConditionReasons(claim.Status.Conditions, statusPath.Child("conditions"), quota.QuotaClaimGranted, grantedConditionReasons)
	seen := make(map[string]bool, len(claim.Status.Allocations))
	for i, a := range claim.Status.Allocations {
		p := statusPath.Child("allocations").Index(i)
		if a.ResourceType == "" {
			allErrs = append(allErrs, field.Required(p.Child("resourceType"), "resourceType is required"))
		} else if seen[a.ResourceType] {
			// +listType=map, +listMapKey=resourceType
			allErrs = append(allErrs, field.Duplicate(p.Child("resourceType"), a.ResourceType))
		}
		seen[a.ResourceType] = true
		if !slices.Contains(allocationStatuses, a.Status) {
			allErrs = append(allErrs, field.NotSupported(p.Child("status"), a.Status, allocationStatuses))
		}
		if a.AllocatedAmount < 0 {
			allErrs = append(allErrs, field.Invalid(p.Child("allocatedAmount"), a.AllocatedAmount, "must be greater than or equal to 0"))
		}
		allErrs = registryutil.MaxLen(allErrs, p.Child("message"), a.Message, maxAllocationMessageLength)
	}
	if len(claim.Status.Allocations) > registryutil.MaxRequests {
		allErrs = append(allErrs, field.TooMany(statusPath.Child("allocations"), len(claim.Status.Allocations), registryutil.MaxRequests))
	}
	return allErrs
}

// maxAllocationMessageLength matches metav1.Condition's message bound.
const maxAllocationMessageLength = 32768

// validateAllocationTransitions makes allocation decisions monotonic: once a
// resource type is Granted, its entry cannot be removed, change status, or
// change amount. A writer working from a stale view (for example a
// controller whose cache has not yet seen its own grant) is rejected instead
// of taking back capacity whose resource was already admitted.
func validateAllocationTransitions(newClaim, oldClaim *quota.QuotaClaim) field.ErrorList {
	var errs field.ErrorList
	path := field.NewPath("status", "allocations")
	newByType := make(map[string]quota.QuotaClaimAllocationStatus, len(newClaim.Status.Allocations))
	for _, a := range newClaim.Status.Allocations {
		newByType[a.ResourceType] = a
	}
	for _, old := range oldClaim.Status.Allocations {
		if old.Status != quota.QuotaClaimAllocationStatusGranted {
			continue
		}
		cur, ok := newByType[old.ResourceType]
		if !ok {
			errs = append(errs, field.Forbidden(path, fmt.Sprintf("granted allocation for %q cannot be removed", old.ResourceType)))
			continue
		}
		if cur.Status != old.Status || cur.AllocatedAmount != old.AllocatedAmount {
			errs = append(errs, field.Forbidden(path, fmt.Sprintf("granted allocation for %q is final: status and allocatedAmount cannot change", old.ResourceType)))
		}
	}
	return errs
}

// GetAttrs returns labels and fields of a given QuotaClaim for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	claim, ok := obj.(*quota.QuotaClaim)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a QuotaClaim")
	}
	return claim.Labels, SelectableFields(claim), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(claim *quota.QuotaClaim) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&claim.ObjectMeta, true)
	specificFields := registryutil.ConsumerRefFields(claim.Spec.ConsumerRef)
	specificFields["spec.resourceRef.apiGroup"] = claim.Spec.ResourceRef.APIGroup
	specificFields["spec.resourceRef.kind"] = claim.Spec.ResourceRef.Kind
	specificFields["spec.resourceRef.name"] = claim.Spec.ResourceRef.Name
	specificFields["spec.resourceRef.namespace"] = claim.Spec.ResourceRef.Namespace
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchQuotaClaim returns a matcher for QuotaClaim resources.
func MatchQuotaClaim(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns true.
func (s quotaClaimStatusStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForUpdate only allows status changes.
func (s quotaClaimStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*quota.QuotaClaim)
	oldClaim := old.(*quota.QuotaClaim)
	newClaim.Spec = oldClaim.Spec
	newClaim.Labels = oldClaim.Labels
	newClaim.Annotations = oldClaim.Annotations
}

// ValidateUpdate validates a status update.
func (s quotaClaimStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newClaim, oldClaim := obj.(*quota.QuotaClaim), old.(*quota.QuotaClaim)
	errs := registryutil.Ratchet(validateQuotaClaimStatus(newClaim), validateQuotaClaimStatus(oldClaim))
	return append(errs, validateAllocationTransitions(newClaim, oldClaim)...)
}

// WarningsOnUpdate returns warnings for the status update.
func (s quotaClaimStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaClaimStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaClaimStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s quotaClaimStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s quotaClaimStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
