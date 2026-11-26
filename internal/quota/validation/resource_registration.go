package validation

import (
	"fmt"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// QuotaDefinitionValidator validates QuotaDefinition resources.
type QuotaDefinitionValidator struct {
	resourceTypeValidator ResourceTypeValidator
}

// NewQuotaDefinitionValidator creates a new QuotaDefinitionValidator.
func NewQuotaDefinitionValidator(resourceTypeValidator ResourceTypeValidator) *QuotaDefinitionValidator {
	return &QuotaDefinitionValidator{
		resourceTypeValidator: resourceTypeValidator,
	}
}

// Validate performs complete validation of a QuotaDefinition.
// This includes both self-contained validation (duplicate claimingResources)
// and cluster-wide validation (resourceType uniqueness).
func (v *QuotaDefinitionValidator) Validate(definition *quotav1alpha1.QuotaDefinition) field.ErrorList {
	var allErrs field.ErrorList

	if errs := v.validateClaimingResourcesDuplicates(definition); len(errs) > 0 {
		allErrs = append(allErrs, errs...)
	}

	if errs := v.validateResourceTypeUniqueness(definition); len(errs) > 0 {
		allErrs = append(allErrs, errs...)
	}

	return allErrs
}

// validateClaimingResourcesDuplicates checks for duplicate entries in the claimingResources array.
// Moved from CEL validation due to cost limits with nested loops.
func (v *QuotaDefinitionValidator) validateClaimingResourcesDuplicates(definition *quotav1alpha1.QuotaDefinition) field.ErrorList {
	var allErrs field.ErrorList

	if len(definition.Spec.ClaimingResources) <= 1 {
		return nil
	}

	claimingResourcesPath := field.NewPath("spec", "claimingResources")
	seen := make(map[string]int)

	for i, cr := range definition.Spec.ClaimingResources {
		key := fmt.Sprintf("%s/%s", cr.APIGroup, cr.Kind)
		if firstIndex, exists := seen[key]; exists {
			allErrs = append(allErrs, field.Duplicate(
				claimingResourcesPath.Index(i),
				fmt.Sprintf("duplicate claiming resource '%s' (first occurrence at index %d)", key, firstIndex),
			))
		}
		seen[key] = i
	}

	return allErrs
}

// validateResourceTypeUniqueness checks that the resourceType is not already registered.
func (v *QuotaDefinitionValidator) validateResourceTypeUniqueness(definition *quotav1alpha1.QuotaDefinition) field.ErrorList {
	var allErrs field.ErrorList

	if v.resourceTypeValidator.IsResourceTypeRegistered(definition.Spec.ResourceType) {
		allErrs = append(allErrs, field.Duplicate(
			field.NewPath("spec", "resourceType"),
			fmt.Sprintf("resource type '%s' is already registered", definition.Spec.ResourceType),
		))
	}

	return allErrs
}
