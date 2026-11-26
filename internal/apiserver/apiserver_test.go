package apiserver

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func TestSchemeHasAllExpectedTypes(t *testing.T) {
	gv := quotav1alpha1.SchemeGroupVersion

	tests := []struct {
		kind string
		obj  runtime.Object
	}{
		{"QuotaDefinition", &quotav1alpha1.QuotaDefinition{}},
		{"QuotaDefinitionList", &quotav1alpha1.QuotaDefinitionList{}},
		{"QuotaGrant", &quotav1alpha1.QuotaGrant{}},
		{"QuotaGrantList", &quotav1alpha1.QuotaGrantList{}},
		{"QuotaClaim", &quotav1alpha1.QuotaClaim{}},
		{"QuotaClaimList", &quotav1alpha1.QuotaClaimList{}},
		{"QuotaBucket", &quotav1alpha1.QuotaBucket{}},
		{"QuotaBucketList", &quotav1alpha1.QuotaBucketList{}},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			gvks, _, err := Scheme.ObjectKinds(tt.obj)
			if err != nil {
				t.Fatalf("type %s not registered with scheme: %v", tt.kind, err)
			}
			found := false
			for _, gvk := range gvks {
				if gvk.Group == gv.Group && gvk.Version == gv.Version {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("type %s not found under %s, got GVKs: %v", tt.kind, gv, gvks)
			}
		})
	}
}

func TestSchemeHasInternalTypes(t *testing.T) {
	// Verify internal (unversioned) types are also registered
	internalGV := schema.GroupVersion{Group: quotav1alpha1.GroupName, Version: runtime.APIVersionInternal}

	kinds := []string{
		"QuotaDefinition",
		"QuotaDefinitionList",
		"QuotaGrant",
		"QuotaGrantList",
		"QuotaClaim",
		"QuotaClaimList",
		"QuotaBucket",
		"QuotaBucketList",
	}

	for _, kind := range kinds {
		t.Run("internal/"+kind, func(t *testing.T) {
			obj, err := Scheme.New(internalGV.WithKind(kind))
			if err != nil {
				t.Fatalf("failed to create internal %s: %v", kind, err)
			}
			if obj == nil {
				t.Fatalf("internal %s is nil", kind)
			}
		})
	}
}

func TestCodecsCanEncodeDecodeTypes(t *testing.T) {
	gv := quotav1alpha1.SchemeGroupVersion

	tests := []struct {
		kind string
		obj  runtime.Object
	}{
		{"QuotaDefinition", &quotav1alpha1.QuotaDefinition{}},
		{"QuotaGrant", &quotav1alpha1.QuotaGrant{}},
		{"QuotaClaim", &quotav1alpha1.QuotaClaim{}},
		{"QuotaBucket", &quotav1alpha1.QuotaBucket{}},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			// Get an encoder for the GV
			info, ok := runtime.SerializerInfoForMediaType(Codecs.SupportedMediaTypes(), "application/json")
			if !ok {
				t.Fatal("no JSON serializer found")
			}

			encoder := Codecs.EncoderForVersion(info.Serializer, gv)
			if encoder == nil {
				t.Fatalf("no encoder for %s", gv)
			}

			// Encode
			data, err := runtime.Encode(encoder, tt.obj)
			if err != nil {
				t.Fatalf("failed to encode %s: %v", tt.kind, err)
			}
			if len(data) == 0 {
				t.Fatalf("encoded %s is empty", tt.kind)
			}

			// Decode
			decoder := Codecs.DecoderToVersion(info.Serializer, gv)
			decoded, _, err := decoder.Decode(data, nil, nil)
			if err != nil {
				t.Fatalf("failed to decode %s: %v", tt.kind, err)
			}
			if decoded == nil {
				t.Fatalf("decoded %s is nil", tt.kind)
			}
		})
	}
}

func TestSchemeHasMetaTypes(t *testing.T) {
	// Verify that unversioned meta types are registered (required by API machinery)
	metaTypes := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Status"},
		{Group: "", Version: "v1", Kind: "APIVersions"},
		{Group: "", Version: "v1", Kind: "APIGroupList"},
		{Group: "", Version: "v1", Kind: "APIGroup"},
		{Group: "", Version: "v1", Kind: "APIResourceList"},
	}

	for _, gvk := range metaTypes {
		t.Run(gvk.Kind, func(t *testing.T) {
			obj, err := Scheme.New(gvk)
			if err != nil {
				t.Fatalf("meta type %s not registered: %v", gvk.Kind, err)
			}
			if obj == nil {
				t.Fatalf("meta type %s is nil", gvk.Kind)
			}
		})
	}
}
