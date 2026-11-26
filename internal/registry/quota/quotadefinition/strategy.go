package quotadefinition

import (
	"context"
	"fmt"
	"slices"

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

// supportedTypes mirrors Milo's `+kubebuilder:validation:Enum=Entity;Allocation;Feature`.
var supportedTypes = []string{
	quota.QuotaDefinitionTypeEntity,
	quota.QuotaDefinitionTypeAllocation,
	quota.QuotaDefinitionTypeFeature,
}

// quotaDefinitionStrategy implements behavior for QuotaDefinition resources.
type quotaDefinitionStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// quotaDefinitionStatusStrategy implements behavior for QuotaDefinition status updates.
type quotaDefinitionStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new QuotaDefinition strategy.
func NewStrategy(typer runtime.ObjectTyper) quotaDefinitionStrategy {
	return quotaDefinitionStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new QuotaDefinition status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) quotaDefinitionStatusStrategy {
	return quotaDefinitionStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns false because QuotaDefinition is cluster-scoped.
func (s quotaDefinitionStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate clears status before creation.
func (s quotaDefinitionStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	def := obj.(*quota.QuotaDefinition)
	def.Status = quota.QuotaDefinitionStatus{}
	registryutil.SetGenerationOnCreate(&def.ObjectMeta)
	metrics.QuotaDefinitionTotal.WithLabelValues("create").Inc()
}

// PrepareForUpdate preserves status when spec is updated.
func (s quotaDefinitionStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDef := obj.(*quota.QuotaDefinition)
	oldDef := old.(*quota.QuotaDefinition)
	newDef.Status = oldDef.Status
	registryutil.BumpGenerationOnSpecChange(&newDef.ObjectMeta, &oldDef.ObjectMeta, newDef.Spec, oldDef.Spec)
	metrics.QuotaDefinitionTotal.WithLabelValues("update").Inc()
}

// Validate validates a new QuotaDefinition.
func (s quotaDefinitionStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	def := obj.(*quota.QuotaDefinition)
	return append(registryutil.ValidateName(&def.ObjectMeta), validateQuotaDefinition(def)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s quotaDefinitionStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaDefinitionStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaDefinitionStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s quotaDefinitionStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a QuotaDefinition.
func (s quotaDefinitionStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newDef := obj.(*quota.QuotaDefinition)
	oldDef := old.(*quota.QuotaDefinition)

	allErrs := registryutil.Ratchet(validateQuotaDefinition(newDef), validateQuotaDefinition(oldDef))

	// Immutable fields. Messages match Milo's CEL XValidation rules.
	if newDef.Spec.ResourceType != oldDef.Spec.ResourceType {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "resourceType"), "spec.resourceType is immutable"))
	}
	if newDef.Spec.Type != oldDef.Spec.Type {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "type"), "spec.type is immutable"))
	}
	if newDef.Spec.ConsumerType != oldDef.Spec.ConsumerType {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "consumerType"), "spec.consumerType is immutable"))
	}

	return allErrs
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s quotaDefinitionStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// validateQuotaDefinition validates a QuotaDefinition. Rules port Milo's
// ResourceRegistration kubebuilder markers, which an aggregated apiserver
// does not evaluate.
func validateQuotaDefinition(def *quota.QuotaDefinition) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	// Milo's pattern also rejects an empty consumerType.apiGroup, which
	// contradicts its own docs ("use empty string for core resources"). The
	// quota service keeps accepting "" so core-group consumers stay valid.
	consumerTypePath := specPath.Child("consumerType")
	if def.Spec.ConsumerType.APIGroup != "" {
		allErrs = registryutil.MatchPattern(allErrs, consumerTypePath.Child("apiGroup"), def.Spec.ConsumerType.APIGroup, registryutil.DNSSubdomainPattern)
	}
	allErrs = registryutil.MaxLen(allErrs, consumerTypePath.Child("apiGroup"), def.Spec.ConsumerType.APIGroup, registryutil.MaxNameLength)
	if def.Spec.ConsumerType.Kind == "" {
		allErrs = append(allErrs, field.Required(consumerTypePath.Child("kind"), "consumerType kind is required"))
	}
	allErrs = registryutil.MaxLen(allErrs, consumerTypePath.Child("kind"), def.Spec.ConsumerType.Kind, registryutil.MaxKindLength)

	if def.Spec.Type == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("type"), "type is required"))
	} else if !slices.Contains(supportedTypes, def.Spec.Type) {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("type"), def.Spec.Type, supportedTypes))
	}

	if def.Spec.ResourceType == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("resourceType"), "resourceType is required"))
	}
	allErrs = registryutil.MaxLen(allErrs, specPath.Child("resourceType"), def.Spec.ResourceType, 253)
	allErrs = registryutil.MaxLen(allErrs, specPath.Child("description"), def.Spec.Description, 500)

	if def.Spec.BaseUnit == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("baseUnit"), "baseUnit is required"))
	}
	allErrs = registryutil.MaxLen(allErrs, specPath.Child("baseUnit"), def.Spec.BaseUnit, 50)
	if def.Spec.DisplayUnit == "" {
		allErrs = append(allErrs, field.Required(specPath.Child("displayUnit"), "displayUnit is required"))
	}
	allErrs = registryutil.MaxLen(allErrs, specPath.Child("displayUnit"), def.Spec.DisplayUnit, 50)

	if def.Spec.UnitConversionFactor < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("unitConversionFactor"), def.Spec.UnitConversionFactor, "unitConversionFactor must be >= 1"))
	}

	claimingResourcesPath := specPath.Child("claimingResources")
	if len(def.Spec.ClaimingResources) == 0 {
		allErrs = append(allErrs, field.Required(claimingResourcesPath, "at least one claiming resource is required"))
	}
	if len(def.Spec.ClaimingResources) > 20 {
		allErrs = append(allErrs, field.TooMany(claimingResourcesPath, len(def.Spec.ClaimingResources), 20))
	}
	seenClaimingResources := make(map[string]int)
	for i, cr := range def.Spec.ClaimingResources {
		crPath := claimingResourcesPath.Index(i)
		if cr.APIGroup != "" {
			allErrs = registryutil.MatchPattern(allErrs, crPath.Child("apiGroup"), cr.APIGroup, registryutil.DNSSubdomainPattern)
			allErrs = registryutil.MaxLen(allErrs, crPath.Child("apiGroup"), cr.APIGroup, registryutil.MaxNameLength)
		}
		if cr.Kind == "" {
			allErrs = append(allErrs, field.Required(crPath.Child("kind"), "kind is required"))
		}
		allErrs = registryutil.MaxLen(allErrs, crPath.Child("kind"), cr.Kind, 63)
		// Duplicate check matches Milo's admission validation.
		key := fmt.Sprintf("%s/%s", cr.APIGroup, cr.Kind)
		if firstIndex, exists := seenClaimingResources[key]; exists {
			allErrs = append(allErrs, field.Duplicate(
				crPath,
				fmt.Sprintf("duplicate claiming resource '%s' (first occurrence at index %d)", key, firstIndex),
			))
		}
		seenClaimingResources[key] = i
	}

	return allErrs
}

// statusConditionReasons ports Milo's status XValidation rule for Active.
var statusConditionReasons = []string{
	quota.QuotaDefinitionActiveReason,
	quota.QuotaDefinitionValidationFailedReason,
	quota.QuotaDefinitionPendingReason,
}

func validateQuotaDefinitionStatus(def *quota.QuotaDefinition) field.ErrorList {
	return registryutil.ValidateConditionReasons(def.Status.Conditions, field.NewPath("status", "conditions"), quota.QuotaDefinitionActive, statusConditionReasons)
}

// GetAttrs returns labels and fields of a given QuotaDefinition for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	def, ok := obj.(*quota.QuotaDefinition)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a QuotaDefinition")
	}
	return def.Labels, SelectableFields(def), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(def *quota.QuotaDefinition) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&def.ObjectMeta, false)
	specificFields := fields.Set{
		"spec.consumerType.kind":     def.Spec.ConsumerType.Kind,
		"spec.consumerType.apiGroup": def.Spec.ConsumerType.APIGroup,
		"spec.resourceType":          def.Spec.ResourceType,
	}
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchQuotaDefinition returns a matcher for QuotaDefinition resources.
func MatchQuotaDefinition(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns false.
func (s quotaDefinitionStatusStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForUpdate only allows status changes.
func (s quotaDefinitionStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDef := obj.(*quota.QuotaDefinition)
	oldDef := old.(*quota.QuotaDefinition)
	newDef.Spec = oldDef.Spec
	newDef.Labels = oldDef.Labels
	newDef.Annotations = oldDef.Annotations
}

// ValidateUpdate validates a status update.
func (s quotaDefinitionStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return registryutil.Ratchet(
		validateQuotaDefinitionStatus(obj.(*quota.QuotaDefinition)),
		validateQuotaDefinitionStatus(old.(*quota.QuotaDefinition)),
	)
}

// WarningsOnUpdate returns warnings for the status update.
func (s quotaDefinitionStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s quotaDefinitionStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s quotaDefinitionStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s quotaDefinitionStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s quotaDefinitionStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
