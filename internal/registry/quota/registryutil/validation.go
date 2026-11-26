package registryutil

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// Patterns ported verbatim from Milo's kubebuilder markers.
var (
	// DNSSubdomainPattern is Milo's API group pattern.
	DNSSubdomainPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	// KindPattern is Milo's Kind pattern (UpperCamelCase).
	KindPattern = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
)

// MaxLen appends a TooLong error when value exceeds max characters. Length is
// counted in runes, matching OpenAPI maxLength semantics. Unlike
// field.TooLong, the value is kept in BadValue so Ratchet can tell an
// unchanged legacy value from a new one; TooLong errors never render it.
func MaxLen(errs field.ErrorList, path *field.Path, value string, max int) field.ErrorList {
	if utf8.RuneCountInString(value) > max {
		errs = append(errs, &field.Error{
			Type:     field.ErrorTypeTooLong,
			Field:    path.String(),
			BadValue: value,
			Detail:   fmt.Sprintf("may not be more than %d characters", max),
		})
	}
	return errs
}

// MatchPattern appends an Invalid error when value does not match re.
func MatchPattern(errs field.ErrorList, path *field.Path, value string, re *regexp.Regexp) field.ErrorList {
	if !re.MatchString(value) {
		errs = append(errs, field.Invalid(path, value, fmt.Sprintf("must match the regex %s", re.String())))
	}
	return errs
}

// ValidateConditionReasons ports Milo's status XValidation rule
// `self.all(c, c.type == T ? c.reason in allowed : true)` together with the
// metav1.Condition schema checks the CRD applied to every condition.
func ValidateConditionReasons(conditions []metav1.Condition, path *field.Path, conditionType string, allowed []string) field.ErrorList {
	errs := metav1validation.ValidateConditions(conditions, path)
	for i, c := range conditions {
		if c.Type == conditionType && !slices.Contains(allowed, c.Reason) {
			errs = append(errs, field.NotSupported(path.Index(i).Child("reason"), c.Reason, allowed))
		}
	}
	return errs
}

// Ratchet drops errors from newErrs that the old object already had with the
// same type, path and value. Rules ported from Milo's CRD schema did not exist
// in earlier quota releases, so objects stored before the port may violate
// them; without ratcheting those objects could not be updated at all (not even
// to remove a finalizer). Any change to an offending value is still rejected.
func Ratchet(newErrs, oldErrs field.ErrorList) field.ErrorList {
	if len(newErrs) == 0 || len(oldErrs) == 0 {
		return newErrs
	}
	out := make(field.ErrorList, 0, len(newErrs))
	for _, ne := range newErrs {
		preexisting := false
		for _, oe := range oldErrs {
			if ne.Type == oe.Type && ne.Field == oe.Field && reflect.DeepEqual(ne.BadValue, oe.BadValue) {
				preexisting = true
				break
			}
		}
		if !preexisting {
			out = append(out, ne)
		}
	}
	return out
}

// Field length limits shared by every kind.
const (
	// MaxNameLength bounds object-name-shaped strings (consumer and
	// resource names, resource types).
	MaxNameLength = 253
	// MaxKindLength bounds Kind strings, matching Milo's MaxLength=63.
	MaxKindLength = 63
	// MaxNamespaceLength bounds namespace strings (DNS-1123 label).
	MaxNamespaceLength = 63
	// MaxAllowances bounds spec.allowances on grants and grant templates,
	// matching Milo's grant-policy CEL rule.
	MaxAllowances = 20
	// MaxBucketsPerAllowance bounds allowance.buckets.
	MaxBucketsPerAllowance = 20
	// MaxRequests mirrors Milo's `+kubebuilder:validation:MaxItems=20` on
	// QuotaClaim spec.requests (also applied inside claim templates).
	MaxRequests = 20
)

// ValidateName requires metadata.name to be a DNS-1123 subdomain, the rule
// Kubernetes applies to most built-in kinds. Generated bucket names
// ("bucket-" plus 64 hex characters) satisfy it.
func ValidateName(meta *metav1.ObjectMeta) field.ErrorList {
	var errs field.ErrorList
	if meta.Name == "" {
		// GenerateName is resolved before validation; an empty name is
		// reported by the generic ObjectMeta validation.
		return nil
	}
	for _, msg := range apimachineryvalidation.NameIsDNSSubdomain(meta.Name, false) {
		errs = append(errs, field.Invalid(field.NewPath("metadata", "name"), meta.Name, msg))
	}
	return errs
}

// ValidateConsumerRef checks a consumerRef: kind and name are required,
// every part is bounded, apiGroup is a DNS subdomain when set, kind is
// UpperCamelCase and namespace is a DNS label when set. When required is
// false the zero value is accepted (templates resolve it later).
func ValidateConsumerRef(apiGroup, kind, name, namespace string, path *field.Path, required bool) field.ErrorList {
	var errs field.ErrorList
	if !required && apiGroup == "" && kind == "" && name == "" && namespace == "" {
		return nil
	}
	if apiGroup != "" {
		errs = MaxLen(errs, path.Child("apiGroup"), apiGroup, MaxNameLength)
		errs = MatchPattern(errs, path.Child("apiGroup"), apiGroup, DNSSubdomainPattern)
	}
	if kind == "" {
		errs = append(errs, field.Required(path.Child("kind"), "consumer kind is required"))
	} else {
		errs = MaxLen(errs, path.Child("kind"), kind, MaxKindLength)
		errs = MatchPattern(errs, path.Child("kind"), kind, KindPattern)
	}
	if name == "" {
		errs = append(errs, field.Required(path.Child("name"), "consumer name is required"))
	} else {
		errs = MaxLen(errs, path.Child("name"), name, MaxNameLength)
	}
	if namespace != "" {
		for _, msg := range apimachineryvalidation.ValidateNamespaceName(namespace, false) {
			errs = append(errs, field.Invalid(path.Child("namespace"), namespace, msg))
		}
	}
	return errs
}

// ValidateResourceType requires a non-empty, bounded resource type.
func ValidateResourceType(errs field.ErrorList, path *field.Path, resourceType string) field.ErrorList {
	if resourceType == "" {
		return append(errs, field.Required(path, "resourceType is required"))
	}
	return MaxLen(errs, path, resourceType, MaxNameLength)
}

// ValidateAmount checks a quantity lies in [min, quota.MaxAmount]. min is 0
// for grant buckets (a zero allowance is legal) and 1 for claim requests.
func ValidateAmount(errs field.ErrorList, path *field.Path, amount, min int64) field.ErrorList {
	if amount < min {
		return append(errs, field.Invalid(path, amount, fmt.Sprintf("must be greater than or equal to %d", min)))
	}
	if amount > quota.MaxAmount {
		return append(errs, field.Invalid(path, amount, fmt.Sprintf("must be less than or equal to %d", quota.MaxAmount)))
	}
	return errs
}

// SetGenerationOnCreate starts metadata.generation at 1, as Kubernetes does
// for every kind with a spec.
func SetGenerationOnCreate(meta *metav1.ObjectMeta) {
	meta.Generation = 1
}

// BumpGenerationOnSpecChange increments metadata.generation when the spec
// changed. rest.BeforeUpdate has already reset newMeta.Generation to the
// stored value, so a client cannot set it; status-only updates leave it
// untouched. Call it after any defaulting so defaulted fields compare equal.
func BumpGenerationOnSpecChange(newMeta, oldMeta *metav1.ObjectMeta, newSpec, oldSpec any) {
	if !apiequality.Semantic.DeepEqual(newSpec, oldSpec) {
		newMeta.Generation = oldMeta.Generation + 1
	}
}

// ConsumerRefFields returns the spec.consumerRef.* selectable fields.
func ConsumerRefFields(ref quota.ConsumerRef) fields.Set {
	return fields.Set{
		"spec.consumerRef.apiGroup":  ref.APIGroup,
		"spec.consumerRef.kind":      ref.Kind,
		"spec.consumerRef.name":      ref.Name,
		"spec.consumerRef.namespace": ref.Namespace,
	}
}

// ValidateRequests validates spec.requests of a claim or claim template:
// 1 to MaxRequests entries, unique bounded resource types, and amounts in
// [1, quota.MaxAmount]. Milo's schema allows 0 but its admission validator
// rejects it.
func ValidateRequests(requests []quota.ResourceRequest, requestsPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(requests) == 0 {
		allErrs = append(allErrs, field.Required(requestsPath, "at least one resource request is required"))
	}
	if len(requests) > MaxRequests {
		allErrs = append(allErrs, field.TooMany(requestsPath, len(requests), MaxRequests))
	}

	seenResourceTypes := make(map[string]bool)
	for i, req := range requests {
		reqPath := requestsPath.Index(i)
		allErrs = ValidateResourceType(allErrs, reqPath.Child("resourceType"), req.ResourceType)
		allErrs = ValidateAmount(allErrs, reqPath.Child("amount"), req.Amount, 1)
		if seenResourceTypes[req.ResourceType] {
			allErrs = append(allErrs, field.Duplicate(reqPath.Child("resourceType"), req.ResourceType))
		}
		seenResourceTypes[req.ResourceType] = true
	}
	return allErrs
}

// ValidateGrantSpec validates a grant spec. consumerRefRequired is false for
// policy templates, whose consumer name is rendered at creation time.
func ValidateGrantSpec(spec *quota.QuotaGrantSpec, specPath *field.Path, consumerRefRequired bool) field.ErrorList {
	allErrs := field.ErrorList{}

	ref := spec.ConsumerRef
	if consumerRefRequired {
		allErrs = append(allErrs, ValidateConsumerRef(ref.APIGroup, ref.Kind, ref.Name, ref.Namespace, specPath.Child("consumerRef"), true)...)
	}

	allowancesPath := specPath.Child("allowances")
	if len(spec.Allowances) == 0 {
		allErrs = append(allErrs, field.Required(allowancesPath, "at least one allowance is required"))
	}
	if len(spec.Allowances) > MaxAllowances {
		allErrs = append(allErrs, field.TooMany(allowancesPath, len(spec.Allowances), MaxAllowances))
	}
	for i, allowance := range spec.Allowances {
		allowancePath := allowancesPath.Index(i)
		allErrs = ValidateResourceType(allErrs, allowancePath.Child("resourceType"), allowance.ResourceType)
		bucketsPath := allowancePath.Child("buckets")
		if len(allowance.Buckets) == 0 {
			allErrs = append(allErrs, field.Required(bucketsPath, "at least one bucket is required"))
		}
		if len(allowance.Buckets) > MaxBucketsPerAllowance {
			allErrs = append(allErrs, field.TooMany(bucketsPath, len(allowance.Buckets), MaxBucketsPerAllowance))
		}
		for j, bucket := range allowance.Buckets {
			allErrs = ValidateAmount(allErrs, bucketsPath.Index(j).Child("amount"), bucket.Amount, 0)
		}
	}

	return allErrs
}

// SpecFieldKeys returns the sorted non-metadata keys of a selectable field
// set, for comparing a kind's SelectableFields with the field labels the
// scheme converts (v1alpha1.SelectableFieldLabels).
func SpecFieldKeys(set fields.Set) []string {
	var keys []string
	for k := range set {
		if !strings.HasPrefix(k, "metadata.") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
