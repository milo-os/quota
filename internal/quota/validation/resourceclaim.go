package validation

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/dynamic"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaClaimValidator provides validation capabilities for QuotaClaim objects.
type QuotaClaimValidator interface {
	// Validate performs complete validation of a QuotaClaim including field validation and claiming permissions.
	Validate(ctx context.Context, claim *quotav1alpha1.QuotaClaim) field.ErrorList
}

// quotaClaimValidator implements QuotaClaimValidator using dynamic client for resource access
// and ResourceTypeValidator for fast cached resource type validation.
type quotaClaimValidator struct {
	dynamicClient         dynamic.Interface
	resourceTypeValidator ResourceTypeValidator
}

// NewQuotaClaimValidator creates a new QuotaClaim validator with dynamic client support and
// a required ResourceTypeValidator for optimized resource type validation.
func NewQuotaClaimValidator(dynamicClient dynamic.Interface, resourceTypeValidator ResourceTypeValidator) QuotaClaimValidator {
	return &quotaClaimValidator{
		dynamicClient:         dynamicClient,
		resourceTypeValidator: resourceTypeValidator,
	}
}

// Validate performs complete validation of a QuotaClaim including field validation and claiming rules.
func (v *quotaClaimValidator) Validate(ctx context.Context, claim *quotav1alpha1.QuotaClaim) field.ErrorList {
	var errs field.ErrorList
	resourceRefPath := field.NewPath("spec", "resourceRef")

	if requestErrs := v.validateResourceRequests(ctx, claim); len(requestErrs) > 0 {
		errs = append(errs, requestErrs...)
	}

	if claim.Spec.ResourceRef.Kind == "" {
		errs = append(errs, field.Required(resourceRefPath.Child("kind"), "resourceRef.kind is required"))
	}
	if claim.Spec.ResourceRef.Name == "" {
		errs = append(errs, field.Required(resourceRefPath.Child("name"), "resourceRef.name is required"))
	}

	return errs
}

// validateResourceRequests validates all resource requests including field validation,
// duplicates, resource type registration, and claiming rules (when resourceRef is complete).
func (v *quotaClaimValidator) validateResourceRequests(ctx context.Context, claim *quotav1alpha1.QuotaClaim) field.ErrorList {
	var errs field.ErrorList
	requestsPath := field.NewPath("spec", "requests")
	seenResourceTypes := make(map[string]int)
	resourceRefComplete := claim.Spec.ResourceRef.Kind != "" && claim.Spec.ResourceRef.Name != ""

	for i, request := range claim.Spec.Requests {
		requestPath := requestsPath.Index(i)

		if request.ResourceType == "" {
			errs = append(errs, field.Required(requestPath.Child("resourceType"), "resource type is required"))
			continue
		}

		if firstIndex, exists := seenResourceTypes[request.ResourceType]; exists {
			errs = append(errs, field.Duplicate(
				requestPath.Child("resourceType"),
				fmt.Sprintf("resource type '%s' is already specified in request %d", request.ResourceType, firstIndex)),
			)
		} else {
			seenResourceTypes[request.ResourceType] = i
		}

		if err := v.resourceTypeValidator.ValidateResourceType(ctx, request.ResourceType); err != nil {
			errs = append(errs, field.Invalid(
				requestPath.Child("resourceType"),
				request.ResourceType,
				err.Error(),
			))
		}

		if request.Amount <= 0 {
			errs = append(errs, field.Invalid(requestPath.Child("amount"), request.Amount, "amount must be greater than 0"))
		}

		if resourceRefComplete {
			if claimingErr := v.validateClaimingRulesForRequest(ctx, claim, request, requestPath); claimingErr != nil {
				errs = append(errs, claimingErr)
			}
		}
	}

	return errs
}

// validateClaimingRulesForRequest validates that the claim's resourceRef satisfies
// the claiming rules defined in the QuotaDefinition for the requested resource type.
func (v *quotaClaimValidator) validateClaimingRulesForRequest(
	ctx context.Context,
	claim *quotav1alpha1.QuotaClaim,
	request quotav1alpha1.ResourceRequest,
	requestPath *field.Path,
) *field.Error {
	allowed, allowedList, err := v.resourceTypeValidator.IsClaimingResourceAllowed(
		ctx,
		request.ResourceType,
		claim.Spec.ConsumerRef,
		claim.Spec.ResourceRef.APIGroup,
		claim.Spec.ResourceRef.Kind,
	)
	if err != nil {
		return field.InternalError(
			requestPath.Child("resourceType"),
			fmt.Errorf("failed to check claiming rules for %s: %w", request.ResourceType, err),
		)
	}

	if !allowed {
		claimingResourceStr := claim.Spec.ResourceRef.Kind
		if claim.Spec.ResourceRef.APIGroup != "" {
			claimingResourceStr = fmt.Sprintf("%s/%s", claim.Spec.ResourceRef.APIGroup, claim.Spec.ResourceRef.Kind)
		}

		var message string
		if len(allowedList) == 0 {
			message = fmt.Sprintf("resource type %s does not satisfy claiming rules for %s. No claimingResources configured in QuotaDefinition",
				claimingResourceStr, request.ResourceType)
		} else {
			message = fmt.Sprintf("resource type %s does not satisfy claiming rules for %s. Allowed claiming resources: [%s]",
				claimingResourceStr, request.ResourceType, strings.Join(allowedList, ", "))
		}

		return field.Forbidden(
			requestPath.Child("resourceType"),
			message,
		)
	}

	return nil
}
