package v1alpha1

import (
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	localSchemeBuilder.Register(addFieldLabelConversionFuncs)
}

var consumerRefFieldLabels = []string{
	"spec.consumerRef.apiGroup",
	"spec.consumerRef.kind",
	"spec.consumerRef.name",
	"spec.consumerRef.namespace",
}

// SelectableFieldLabels lists, per kind, the spec fields each kind accepts in
// field selectors, in addition to metadata.name and metadata.namespace. The
// registry's SelectableFields for each kind must return exactly these keys;
// the registry tests enforce that.
var SelectableFieldLabels = map[string][]string{
	"QuotaClaim": append(slices.Clone(consumerRefFieldLabels),
		"spec.resourceRef.apiGroup",
		"spec.resourceRef.kind",
		"spec.resourceRef.name",
		"spec.resourceRef.namespace",
	),
	"QuotaGrant":  slices.Clone(consumerRefFieldLabels),
	"QuotaBucket": append(slices.Clone(consumerRefFieldLabels), "spec.resourceType"),
	"QuotaDefinition": {
		"spec.consumerType.apiGroup",
		"spec.consumerType.kind",
		"spec.resourceType",
	},
	"ClaimCreationPolicy": {
		"spec.trigger.resource.apiVersion",
		"spec.trigger.resource.kind",
	},
	"GrantCreationPolicy": {
		"spec.trigger.resource.apiVersion",
		"spec.trigger.resource.kind",
	},
}

// addFieldLabelConversionFuncs registers a field label conversion for every
// kind. Without one, the apiserver rejects every field selector other than
// metadata.name and metadata.namespace with 400 Bad Request.
func addFieldLabelConversionFuncs(scheme *runtime.Scheme) error {
	for kind, labels := range SelectableFieldLabels {
		// metadata.name and metadata.namespace are what the default
		// conversion accepts; keep accepting them for every kind.
		allowed := append([]string{"metadata.name", "metadata.namespace"}, labels...)
		if err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind(kind), fieldLabelConversion(kind, allowed)); err != nil {
			return err
		}
	}
	return nil
}

func fieldLabelConversion(kind string, allowed []string) runtime.FieldLabelConversionFunc {
	return func(label, value string) (string, string, error) {
		if slices.Contains(allowed, label) {
			return label, value, nil
		}
		return "", "", fmt.Errorf("field label not supported for %s: %s", kind, label)
	}
}
