package claimcreationpolicy

import (
	"context"
	"fmt"
	"regexp"

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

// claimCreationPolicyStrategy implements behavior for ClaimCreationPolicy resources.
type claimCreationPolicyStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// claimCreationPolicyStatusStrategy implements behavior for ClaimCreationPolicy status updates.
type claimCreationPolicyStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new ClaimCreationPolicy strategy.
func NewStrategy(typer runtime.ObjectTyper) claimCreationPolicyStrategy {
	return claimCreationPolicyStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new ClaimCreationPolicy status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) claimCreationPolicyStatusStrategy {
	return claimCreationPolicyStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns false because ClaimCreationPolicy is cluster-scoped.
func (s claimCreationPolicyStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate clears status before creation.
func (s claimCreationPolicyStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	policy := obj.(*quota.ClaimCreationPolicy)
	policy.Status = quota.ClaimCreationPolicyStatus{}
	defaultDisabled(&policy.Spec.Disabled)
	registryutil.SetGenerationOnCreate(&policy.ObjectMeta)
}

// PrepareForUpdate preserves status when spec is updated.
func (s claimCreationPolicyStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPolicy := obj.(*quota.ClaimCreationPolicy)
	oldPolicy := old.(*quota.ClaimCreationPolicy)
	newPolicy.Status = oldPolicy.Status
	defaultDisabled(&newPolicy.Spec.Disabled)
	// Default a copy of the old spec so a stored nil and a defaulted false
	// compare equal; the stored object itself must not be mutated.
	oldSpec := oldPolicy.Spec.DeepCopy()
	defaultDisabled(&oldSpec.Disabled)
	registryutil.BumpGenerationOnSpecChange(&newPolicy.ObjectMeta, &oldPolicy.ObjectMeta, newPolicy.Spec, *oldSpec)
}

// Validate validates a new ClaimCreationPolicy.
func (s claimCreationPolicyStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	policy := obj.(*quota.ClaimCreationPolicy)
	return append(registryutil.ValidateName(&policy.ObjectMeta), validateClaimCreationPolicy(policy)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s claimCreationPolicyStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s claimCreationPolicyStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s claimCreationPolicyStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s claimCreationPolicyStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a ClaimCreationPolicy.
func (s claimCreationPolicyStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPolicy := obj.(*quota.ClaimCreationPolicy)
	oldPolicy := old.(*quota.ClaimCreationPolicy)
	return registryutil.Ratchet(validateClaimCreationPolicy(newPolicy), validateClaimCreationPolicy(oldPolicy))
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s claimCreationPolicyStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// claimTriggerAPIVersionPattern is Milo's ClaimTriggerResource.apiVersion pattern.
var claimTriggerAPIVersionPattern = regexp.MustCompile(`^(v[0-9]+((alpha|beta)[0-9]*)?|[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\/v[0-9]+((alpha|beta)[0-9]*)?)$`)

// validateClaimCreationPolicy validates a ClaimCreationPolicy. Rules port
// Milo's kubebuilder markers and the ResourceClaimTemplate CEL rule.
func validateClaimCreationPolicy(policy *quota.ClaimCreationPolicy) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	triggerPath := specPath.Child("trigger", "resource")
	res := policy.Spec.Trigger.Resource
	if res.APIVersion == "" {
		allErrs = append(allErrs, field.Required(triggerPath.Child("apiVersion"), "trigger resource apiVersion is required"))
	} else {
		allErrs = registryutil.MatchPattern(allErrs, triggerPath.Child("apiVersion"), res.APIVersion, claimTriggerAPIVersionPattern)
	}
	if res.Kind == "" {
		allErrs = append(allErrs, field.Required(triggerPath.Child("kind"), "trigger resource kind is required"))
	}

	allErrs = append(allErrs, validateConstraints(policy.Spec.Trigger.Constraints, specPath.Child("trigger", "constraints"))...)

	templateSpecPath := specPath.Child("target", "resourceClaimTemplate", "spec")
	template := policy.Spec.Target.ResourceClaimTemplate

	// consumerRef is optional in the template (Milo: Optional): the admission
	// plugin resolves it from the request context when omitted. When set it
	// must name a consumer, as Milo's ConsumerRef schema required.
	if ref := template.Spec.ConsumerRef; ref != (quota.ConsumerRef{}) {
		if ref.Kind == "" {
			allErrs = append(allErrs, field.Required(templateSpecPath.Child("consumerRef", "kind"), "consumer kind is required when consumerRef is set"))
		}
		if ref.Name == "" {
			allErrs = append(allErrs, field.Required(templateSpecPath.Child("consumerRef", "name"), "consumer name is required when consumerRef is set"))
		}
		allErrs = registryutil.MaxLen(allErrs, templateSpecPath.Child("consumerRef", "kind"), ref.Kind, registryutil.MaxKindLength)
		allErrs = registryutil.MaxLen(allErrs, templateSpecPath.Child("consumerRef", "name"), ref.Name, registryutil.MaxNameLength)
	}

	// CEL: resourceRef is populated automatically from the trigger resource.
	if template.Spec.ResourceRef != (quota.UnversionedObjectReference{}) {
		allErrs = append(allErrs, field.Forbidden(templateSpecPath.Child("resourceRef"), "resourceRef field is automatically populated and cannot be set in template"))
	}

	allErrs = append(allErrs, registryutil.ValidateRequests(template.Spec.Requests, templateSpecPath.Child("requests"))...)

	return allErrs
}

// defaultDisabled applies Milo's `+kubebuilder:default=false` on spec.disabled.
func defaultDisabled(d **bool) {
	if *d == nil {
		f := false
		*d = &f
	}
}

// validateConstraints ports the MaxItems/MinLength/MaxLength markers on
// trigger.constraints and ConditionExpression.
func validateConstraints(constraints []quota.ConditionExpression, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	if len(constraints) > 10 {
		errs = append(errs, field.TooMany(path, len(constraints), 10))
	}
	for i, c := range constraints {
		p := path.Index(i)
		if c.Expression == "" {
			errs = append(errs, field.Required(p.Child("expression"), "expression is required"))
		}
		errs = registryutil.MaxLen(errs, p.Child("expression"), c.Expression, 1024)
		errs = registryutil.MaxLen(errs, p.Child("message"), c.Message, 256)
	}
	return errs
}

// GetAttrs returns labels and fields of a given ClaimCreationPolicy for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	policy, ok := obj.(*quota.ClaimCreationPolicy)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a ClaimCreationPolicy")
	}
	return policy.Labels, SelectableFields(policy), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(policy *quota.ClaimCreationPolicy) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&policy.ObjectMeta, false)
	specificFields := fields.Set{
		"spec.trigger.resource.kind":       policy.Spec.Trigger.Resource.Kind,
		"spec.trigger.resource.apiVersion": policy.Spec.Trigger.Resource.APIVersion,
	}
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchClaimCreationPolicy returns a matcher for ClaimCreationPolicy resources.
func MatchClaimCreationPolicy(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns false.
func (s claimCreationPolicyStatusStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForUpdate only allows status changes.
func (s claimCreationPolicyStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPolicy := obj.(*quota.ClaimCreationPolicy)
	oldPolicy := old.(*quota.ClaimCreationPolicy)
	newPolicy.Spec = oldPolicy.Spec
	newPolicy.Labels = oldPolicy.Labels
	newPolicy.Annotations = oldPolicy.Annotations
}

// ValidateUpdate validates a status update.
func (s claimCreationPolicyStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnUpdate returns warnings for the status update.
func (s claimCreationPolicyStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s claimCreationPolicyStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s claimCreationPolicyStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s claimCreationPolicyStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s claimCreationPolicyStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
