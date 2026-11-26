package v1alpha1

import (
	"encoding/json"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("failed to add to scheme: %v", err)
	}
	return s
}

func TestTypesImplementRuntimeObject(t *testing.T) {
	tests := []struct {
		name string
		obj  runtime.Object
	}{
		{"QuotaDefinition", &QuotaDefinition{}},
		{"QuotaDefinitionList", &QuotaDefinitionList{}},
		{"QuotaGrant", &QuotaGrant{}},
		{"QuotaGrantList", &QuotaGrantList{}},
		{"QuotaClaim", &QuotaClaim{}},
		{"QuotaClaimList", &QuotaClaimList{}},
		{"QuotaBucket", &QuotaBucket{}},
		{"QuotaBucketList", &QuotaBucketList{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify the object implements runtime.Object by calling DeepCopyObject
			copy := tt.obj.DeepCopyObject()
			if copy == nil {
				t.Errorf("%s.DeepCopyObject() returned nil", tt.name)
			}
		})
	}
}

func TestTypesRegisteredWithScheme(t *testing.T) {
	s := newTestScheme(t)

	tests := []struct {
		name string
		obj  runtime.Object
	}{
		{"QuotaDefinition", &QuotaDefinition{}},
		{"QuotaDefinitionList", &QuotaDefinitionList{}},
		{"QuotaGrant", &QuotaGrant{}},
		{"QuotaGrantList", &QuotaGrantList{}},
		{"QuotaClaim", &QuotaClaim{}},
		{"QuotaClaimList", &QuotaClaimList{}},
		{"QuotaBucket", &QuotaBucket{}},
		{"QuotaBucketList", &QuotaBucketList{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gvks, _, err := s.ObjectKinds(tt.obj)
			if err != nil {
				t.Fatalf("type %s not registered with scheme: %v", tt.name, err)
			}
			if len(gvks) == 0 {
				t.Fatalf("type %s has no GVKs registered", tt.name)
			}
			found := false
			for _, gvk := range gvks {
				if gvk.Group == GroupName && gvk.Version == "v1alpha1" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("type %s not registered under group %s/v1alpha1, got %v", tt.name, GroupName, gvks)
			}
		})
	}
}

func TestGroupVersion(t *testing.T) {
	expected := schema.GroupVersion{Group: "quota.miloapis.com", Version: "v1alpha1"}
	if SchemeGroupVersion != expected {
		t.Errorf("SchemeGroupVersion = %v, want %v", SchemeGroupVersion, expected)
	}
}

func TestGroupName(t *testing.T) {
	if GroupName != "quota.miloapis.com" {
		t.Errorf("GroupName = %q, want %q", GroupName, "quota.miloapis.com")
	}
}

func TestResourceNames(t *testing.T) {
	tests := []struct {
		resource string
		expected schema.GroupResource
	}{
		{"quotadefinitions", schema.GroupResource{Group: GroupName, Resource: "quotadefinitions"}},
		{"quotagrants", schema.GroupResource{Group: GroupName, Resource: "quotagrants"}},
		{"quotaclaims", schema.GroupResource{Group: GroupName, Resource: "quotaclaims"}},
		{"quotabuckets", schema.GroupResource{Group: GroupName, Resource: "quotabuckets"}},
	}

	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			gr := Resource(tt.resource)
			if gr != tt.expected {
				t.Errorf("Resource(%q) = %v, want %v", tt.resource, gr, tt.expected)
			}
		})
	}
}

func TestQuotaDefinitionJSONRoundtrip(t *testing.T) {
	original := &QuotaDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "QuotaDefinition",
			APIVersion: "quota.miloapis.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-definition",
		},
		Spec: QuotaDefinitionSpec{
			ConsumerType: ConsumerType{
				APIGroup: "example.com",
				Kind:     "Project",
			},
			Type:                 "Entity",
			ResourceType:         "widgets",
			Description:          "Test widgets",
			BaseUnit:             "count",
			DisplayUnit:          "widgets",
			UnitConversionFactor: 1,
			ClaimingResources: []ClaimingResource{
				{APIGroup: "apps.example.com", Kind: "Widget"},
			},
		},
		Status: QuotaDefinitionStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{
					Type:   QuotaDefinitionActive,
					Status: metav1.ConditionTrue,
					Reason: QuotaDefinitionActiveReason,
				},
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	decoded := &QuotaDefinition{}
	if err := json.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, decoded.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", decoded.Spec, original.Spec)
	}
	if original.Name != decoded.Name {
		t.Errorf("name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
}

func TestQuotaGrantJSONRoundtrip(t *testing.T) {
	original := &QuotaGrant{
		TypeMeta: metav1.TypeMeta{
			Kind:       "QuotaGrant",
			APIVersion: "quota.miloapis.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grant",
			Namespace: "default",
		},
		Spec: QuotaGrantSpec{
			ConsumerRef: ConsumerRef{
				APIGroup: "example.com",
				Kind:     "Project",
				Name:     "my-project",
			},
			Allowances: []Allowance{
				{
					ResourceType: "widgets",
					Buckets: []Bucket{
						{Amount: 100},
						{Amount: 50},
					},
				},
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	decoded := &QuotaGrant{}
	if err := json.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, decoded.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", decoded.Spec, original.Spec)
	}
}

func TestQuotaClaimJSONRoundtrip(t *testing.T) {
	original := &QuotaClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "QuotaClaim",
			APIVersion: "quota.miloapis.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "default",
		},
		Spec: QuotaClaimSpec{
			ConsumerRef: ConsumerRef{
				Kind: "Project",
				Name: "my-project",
			},
			Requests: []ResourceRequest{
				{ResourceType: "widgets", Amount: 10},
			},
			ResourceRef: UnversionedObjectReference{
				APIGroup:  "apps.example.com",
				Kind:      "Widget",
				Name:      "my-widget",
				Namespace: "default",
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	decoded := &QuotaClaim{}
	if err := json.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, decoded.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", decoded.Spec, original.Spec)
	}
}

func TestQuotaBucketJSONRoundtrip(t *testing.T) {
	original := &QuotaBucket{
		TypeMeta: metav1.TypeMeta{
			Kind:       "QuotaBucket",
			APIVersion: "quota.miloapis.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-bucket",
			Namespace: "default",
		},
		Spec: QuotaBucketSpec{
			ConsumerRef: ConsumerRef{
				Kind: "Project",
				Name: "my-project",
			},
			ResourceType: "widgets",
		},
		Status: QuotaBucketStatus{
			Limit:      100,
			Allocated:  30,
			Available:  70,
			ClaimCount: 3,
			GrantCount: 2,
			ContributingGrantRefs: []ContributingGrantRef{
				{Name: "grant-1", LastObservedGeneration: 1, Amount: 60},
				{Name: "grant-2", LastObservedGeneration: 2, Amount: 40},
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	decoded := &QuotaBucket{}
	if err := json.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, decoded.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", decoded.Spec, original.Spec)
	}
	if !reflect.DeepEqual(original.Status.ContributingGrantRefs, decoded.Status.ContributingGrantRefs) {
		t.Errorf("status.contributingGrantRefs mismatch after roundtrip")
	}
}

func TestConsumerRefMatchesConsumer(t *testing.T) {
	tests := []struct {
		name     string
		a, b     ConsumerRef
		expected bool
	}{
		{
			name:     "same consumer",
			a:        ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-1"},
			b:        ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-1"},
			expected: true,
		},
		{
			name:     "different name",
			a:        ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-1"},
			b:        ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-2"},
			expected: false,
		},
		{
			name:     "different kind",
			a:        ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-1"},
			b:        ConsumerRef{APIGroup: "example.com", Kind: "Organization", Name: "proj-1"},
			expected: false,
		},
		{
			name:     "different apiGroup",
			a:        ConsumerRef{APIGroup: "a.com", Kind: "Project", Name: "proj-1"},
			b:        ConsumerRef{APIGroup: "b.com", Kind: "Project", Name: "proj-1"},
			expected: false,
		},
		{
			name:     "different namespace",
			a:        ConsumerRef{Kind: "Project", Name: "proj-1", Namespace: "ns-1"},
			b:        ConsumerRef{Kind: "Project", Name: "proj-1", Namespace: "ns-2"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.MatchesConsumer(tt.b)
			if got != tt.expected {
				t.Errorf("MatchesConsumer() = %v, want %v", got, tt.expected)
			}
		})
	}
}
