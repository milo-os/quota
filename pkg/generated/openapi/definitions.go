package openapi

import (
	common "k8s.io/kube-openapi/pkg/common"
	openapiutil "k8s.io/kube-openapi/pkg/util"
	spec "k8s.io/kube-openapi/pkg/validation/spec"
)

// GetOpenAPIDefinitionsWithUnstructured wraps the generated GetOpenAPIDefinitions
// and adds the Unstructured type definition. It also duplicates all entries under
// their REST-friendly name so the apiserver's openapi routes can find them.
func GetOpenAPIDefinitionsWithUnstructured(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	originalDefs := GetOpenAPIDefinitions(ref)

	originalDefs["k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Unstructured represents a Kubernetes resource as an arbitrary JSON object.",
				Type:        []string{"object"},
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-preserve-unknown-fields": true,
				},
			},
		},
	}

	// Duplicate definitions under REST-friendly names so the apiserver's
	// openapi routes (which look up by REST-friendly name) can find them.
	for key, def := range originalDefs {
		restName := openapiutil.ToRESTFriendlyName(key)
		if restName != key {
			originalDefs[restName] = def
		}
	}

	return originalDefs
}
