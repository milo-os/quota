package grantcreationpolicy

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

// grantCreationPolicyStrategy implements behavior for GrantCreationPolicy resources.
type grantCreationPolicyStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// grantCreationPolicyStatusStrategy implements behavior for GrantCreationPolicy status updates.
type grantCreationPolicyStatusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy creates a new GrantCreationPolicy strategy.
func NewStrategy(typer runtime.ObjectTyper) grantCreationPolicyStrategy {
	return grantCreationPolicyStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NewStatusStrategy creates a new GrantCreationPolicy status strategy.
func NewStatusStrategy(typer runtime.ObjectTyper) grantCreationPolicyStatusStrategy {
	return grantCreationPolicyStatusStrategy{
		ObjectTyper:   typer,
		NameGenerator: names.SimpleNameGenerator,
	}
}

// NamespaceScoped returns false because GrantCreationPolicy is cluster-scoped.
func (s grantCreationPolicyStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate clears status before creation.
func (s grantCreationPolicyStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	policy := obj.(*quota.GrantCreationPolicy)
	policy.Status = quota.GrantCreationPolicyStatus{}
	defaultDisabled(&policy.Spec.Disabled)
	registryutil.SetGenerationOnCreate(&policy.ObjectMeta)
}

// PrepareForUpdate preserves status when spec is updated.
func (s grantCreationPolicyStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPolicy := obj.(*quota.GrantCreationPolicy)
	oldPolicy := old.(*quota.GrantCreationPolicy)
	newPolicy.Status = oldPolicy.Status
	defaultDisabled(&newPolicy.Spec.Disabled)
	// Default a copy of the old spec so a stored nil and a defaulted false
	// compare equal; the stored object itself must not be mutated.
	oldSpec := oldPolicy.Spec.DeepCopy()
	defaultDisabled(&oldSpec.Disabled)
	registryutil.BumpGenerationOnSpecChange(&newPolicy.ObjectMeta, &oldPolicy.ObjectMeta, newPolicy.Spec, *oldSpec)
}

// Validate validates a new GrantCreationPolicy.
func (s grantCreationPolicyStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	policy := obj.(*quota.GrantCreationPolicy)
	return append(registryutil.ValidateName(&policy.ObjectMeta), validateGrantCreationPolicy(policy)...)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (s grantCreationPolicyStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s grantCreationPolicyStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s grantCreationPolicyStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object after validation.
func (s grantCreationPolicyStrategy) Canonicalize(obj runtime.Object) {}

// ValidateUpdate validates an update to a GrantCreationPolicy.
func (s grantCreationPolicyStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPolicy := obj.(*quota.GrantCreationPolicy)
	oldPolicy := old.(*quota.GrantCreationPolicy)
	return registryutil.Ratchet(validateGrantCreationPolicy(newPolicy), validateGrantCreationPolicy(oldPolicy))
}

// WarningsOnUpdate returns warnings for the update of the given object.
func (s grantCreationPolicyStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// grantTriggerAPIVersionPattern is Milo's GrantTriggerResource.apiVersion pattern.
var grantTriggerAPIVersionPattern = regexp.MustCompile(`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\/)?v[0-9]+((alpha|beta)[0-9]*)?$`)

// validateGrantCreationPolicy validates a GrantCreationPolicy. Rules port
// Milo's kubebuilder markers and the three type-level CEL XValidation rules.
func validateGrantCreationPolicy(policy *quota.GrantCreationPolicy) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	triggerPath := specPath.Child("trigger", "resource")
	res := policy.Spec.Trigger.Resource
	if res.APIVersion == "" {
		allErrs = append(allErrs, field.Required(triggerPath.Child("apiVersion"), "trigger resource apiVersion is required"))
	} else {
		allErrs = registryutil.MatchPattern(allErrs, triggerPath.Child("apiVersion"), res.APIVersion, grantTriggerAPIVersionPattern)
	}
	if res.Kind == "" {
		allErrs = append(allErrs, field.Required(triggerPath.Child("kind"), "trigger resource kind is required"))
	} else {
		allErrs = registryutil.MaxLen(allErrs, triggerPath.Child("kind"), res.Kind, 63)
		allErrs = registryutil.MatchPattern(allErrs, triggerPath.Child("kind"), res.Kind, registryutil.KindPattern)
	}

	constraintsPath := specPath.Child("trigger", "constraints")
	allErrs = append(allErrs, validateConstraints(policy.Spec.Trigger.Constraints, constraintsPath)...)

	// CEL: disabled policies should not have trigger constraints.
	if policy.Spec.Disabled != nil && *policy.Spec.Disabled && len(policy.Spec.Trigger.Constraints) > 0 {
		allErrs = append(allErrs, field.Forbidden(constraintsPath, "disabled policies should not have trigger constraints"))
	}

	if pc := policy.Spec.Target.ParentContext; pc != nil {
		pcPath := specPath.Child("target", "parentContext")
		// Milo requires a non-empty apiGroup. The standalone service also
		// accepts "" so core-group parents (for example Namespace) work.
		if pc.APIGroup != "" {
			allErrs = registryutil.MaxLen(allErrs, pcPath.Child("apiGroup"), pc.APIGroup, 253)
			allErrs = registryutil.MatchPattern(allErrs, pcPath.Child("apiGroup"), pc.APIGroup, registryutil.DNSSubdomainPattern)
		}
		if pc.Kind == "" {
			allErrs = append(allErrs, field.Required(pcPath.Child("kind"), "parent context kind is required"))
		} else {
			allErrs = registryutil.MaxLen(allErrs, pcPath.Child("kind"), pc.Kind, 63)
			allErrs = registryutil.MatchPattern(allErrs, pcPath.Child("kind"), pc.Kind, registryutil.KindPattern)
		}
		// CEL: parent context must have a name expression.
		if pc.NameExpression == "" {
			allErrs = append(allErrs, field.Required(pcPath.Child("nameExpression"), "parent context must have a name expression"))
		}
		allErrs = registryutil.MaxLen(allErrs, pcPath.Child("nameExpression"), pc.NameExpression, 512)
	}

	templatePath := specPath.Child("target", "resourceGrantTemplate", "spec")
	template := policy.Spec.Target.ResourceGrantTemplate
	if template.Spec.ConsumerRef.Kind == "" {
		allErrs = append(allErrs, field.Required(templatePath.Child("consumerRef", "kind"), "consumer kind is required"))
	}
	allErrs = registryutil.MaxLen(allErrs, templatePath.Child("consumerRef", "kind"), template.Spec.ConsumerRef.Kind, registryutil.MaxKindLength)
	allErrs = registryutil.MaxLen(allErrs, templatePath.Child("consumerRef", "name"), template.Spec.ConsumerRef.Name, registryutil.MaxNameLength)
	allErrs = append(allErrs, registryutil.ValidateGrantSpec(&template.Spec, templatePath, false)...)

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

// GetAttrs returns labels and fields of a given GrantCreationPolicy for filtering.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	policy, ok := obj.(*quota.GrantCreationPolicy)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a GrantCreationPolicy")
	}
	return policy.Labels, SelectableFields(policy), nil
}

// SelectableFields returns the fields that can be used in field selectors.
func SelectableFields(policy *quota.GrantCreationPolicy) fields.Set {
	objectMetaFields := generic.ObjectMetaFieldsSet(&policy.ObjectMeta, false)
	specificFields := fields.Set{
		"spec.trigger.resource.kind":       policy.Spec.Trigger.Resource.Kind,
		"spec.trigger.resource.apiVersion": policy.Spec.Trigger.Resource.APIVersion,
	}
	return generic.MergeFieldsSets(objectMetaFields, specificFields)
}

// MatchGrantCreationPolicy returns a matcher for GrantCreationPolicy resources.
func MatchGrantCreationPolicy(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// Status strategy methods

// NamespaceScoped returns false.
func (s grantCreationPolicyStatusStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForUpdate only allows status changes.
func (s grantCreationPolicyStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPolicy := obj.(*quota.GrantCreationPolicy)
	oldPolicy := old.(*quota.GrantCreationPolicy)
	newPolicy.Spec = oldPolicy.Spec
	newPolicy.Labels = oldPolicy.Labels
	newPolicy.Annotations = oldPolicy.Annotations
}

// ValidateUpdate validates a status update.
func (s grantCreationPolicyStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnUpdate returns warnings for the status update.
func (s grantCreationPolicyStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowCreateOnUpdate returns false.
func (s grantCreationPolicyStatusStrategy) AllowCreateOnUpdate() bool {
	return false
}

// AllowUnconditionalUpdate allows unconditional updates.
func (s grantCreationPolicyStatusStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// Canonicalize normalizes the object.
func (s grantCreationPolicyStatusStrategy) Canonicalize(obj runtime.Object) {}

// GetResetFields returns fields that should be reset on status update.
func (s grantCreationPolicyStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"quota.miloapis.com/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
