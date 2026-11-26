package install

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	Install(s)
	return s
}

func TestInstallRegistersAllTypes(t *testing.T) {
	s := newTestScheme(t)

	tests := []struct {
		name string
		obj  runtime.Object
	}{
		{"QuotaDefinition", &v1alpha1.QuotaDefinition{}},
		{"QuotaDefinitionList", &v1alpha1.QuotaDefinitionList{}},
		{"QuotaGrant", &v1alpha1.QuotaGrant{}},
		{"QuotaGrantList", &v1alpha1.QuotaGrantList{}},
		{"QuotaClaim", &v1alpha1.QuotaClaim{}},
		{"QuotaClaimList", &v1alpha1.QuotaClaimList{}},
		{"QuotaBucket", &v1alpha1.QuotaBucket{}},
		{"QuotaBucketList", &v1alpha1.QuotaBucketList{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gvks, _, err := s.ObjectKinds(tt.obj)
			if err != nil {
				t.Fatalf("type %s not registered: %v", tt.name, err)
			}
			if len(gvks) == 0 {
				t.Fatalf("type %s has no GVKs", tt.name)
			}
		})
	}
}

func TestInstallV1alpha1IsPreferredVersion(t *testing.T) {
	s := newTestScheme(t)

	gv := schema.GroupVersion{Group: v1alpha1.GroupName, Version: "v1alpha1"}
	prioritized := s.PrioritizedVersionsForGroup(v1alpha1.GroupName)
	if len(prioritized) == 0 {
		t.Fatal("no prioritized versions found for group")
	}
	if prioritized[0] != gv {
		t.Errorf("preferred version = %v, want %v", prioritized[0], gv)
	}
}

func TestInstallCanCreateInstances(t *testing.T) {
	s := newTestScheme(t)

	gv := v1alpha1.SchemeGroupVersion

	tests := []struct {
		kind string
	}{
		{"QuotaDefinition"},
		{"QuotaDefinitionList"},
		{"QuotaGrant"},
		{"QuotaGrantList"},
		{"QuotaClaim"},
		{"QuotaClaimList"},
		{"QuotaBucket"},
		{"QuotaBucketList"},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			obj, err := s.New(gv.WithKind(tt.kind))
			if err != nil {
				t.Fatalf("failed to create %s: %v", tt.kind, err)
			}
			if obj == nil {
				t.Fatalf("created object for %s is nil", tt.kind)
			}
		})
	}
}
