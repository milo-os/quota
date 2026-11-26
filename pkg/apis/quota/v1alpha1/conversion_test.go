package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"

	"go.miloapis.com/quota/pkg/apis/quota"
)

func TestQuotaDefinitionRoundtripConversion(t *testing.T) {
	original := &QuotaDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-def",
			Generation: 3,
		},
		Spec: QuotaDefinitionSpec{
			ConsumerType: ConsumerType{
				APIGroup: "example.com",
				Kind:     "Project",
			},
			Type:                 "Entity",
			ResourceType:         "widgets",
			Description:          "Track widget count",
			BaseUnit:             "count",
			DisplayUnit:          "widgets",
			UnitConversionFactor: 1,
			ClaimingResources: []ClaimingResource{
				{APIGroup: "apps.example.com", Kind: "Widget"},
				{APIGroup: "", Kind: "Pod"},
			},
		},
		Status: QuotaDefinitionStatus{
			ObservedGeneration: 2,
			Conditions: []metav1.Condition{
				{
					Type:   QuotaDefinitionActive,
					Status: metav1.ConditionTrue,
					Reason: QuotaDefinitionActiveReason,
				},
			},
		},
	}

	scope := conversion.Scope(nil)

	// v1alpha1 -> internal
	internal := &quota.QuotaDefinition{}
	if err := Convert_v1alpha1_QuotaDefinition_To_quota_QuotaDefinition(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	// internal -> v1alpha1
	roundtripped := &QuotaDefinition{}
	if err := Convert_quota_QuotaDefinition_To_v1alpha1_QuotaDefinition(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, roundtripped.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", roundtripped.Spec, original.Spec)
	}
	if !reflect.DeepEqual(original.Status, roundtripped.Status) {
		t.Errorf("status mismatch after roundtrip:\ngot:  %+v\nwant: %+v", roundtripped.Status, original.Status)
	}
	if original.Name != roundtripped.Name {
		t.Errorf("name mismatch: got %q, want %q", roundtripped.Name, original.Name)
	}
}

func TestQuotaDefinitionListRoundtripConversion(t *testing.T) {
	original := &QuotaDefinitionList{
		ListMeta: metav1.ListMeta{
			ResourceVersion: "123",
		},
		Items: []QuotaDefinition{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "def-1"},
				Spec: QuotaDefinitionSpec{
					ConsumerType:         ConsumerType{Kind: "Project"},
					Type:                 "Entity",
					ResourceType:         "widgets",
					BaseUnit:             "count",
					DisplayUnit:          "widgets",
					UnitConversionFactor: 1,
					ClaimingResources:    []ClaimingResource{{Kind: "Widget"}},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "def-2"},
				Spec: QuotaDefinitionSpec{
					ConsumerType:         ConsumerType{Kind: "Org"},
					Type:                 "Allocation",
					ResourceType:         "cpu",
					BaseUnit:             "millicores",
					DisplayUnit:          "cores",
					UnitConversionFactor: 1000,
					ClaimingResources:    []ClaimingResource{{Kind: "Deployment"}},
				},
			},
		},
	}

	scope := conversion.Scope(nil)

	internal := &quota.QuotaDefinitionList{}
	if err := Convert_v1alpha1_QuotaDefinitionList_To_quota_QuotaDefinitionList(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}
	if len(internal.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(internal.Items))
	}

	roundtripped := &QuotaDefinitionList{}
	if err := Convert_quota_QuotaDefinitionList_To_v1alpha1_QuotaDefinitionList(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}
	if len(roundtripped.Items) != 2 {
		t.Fatalf("expected 2 items after roundtrip, got %d", len(roundtripped.Items))
	}
	for i := range original.Items {
		if !reflect.DeepEqual(original.Items[i].Spec, roundtripped.Items[i].Spec) {
			t.Errorf("item[%d] spec mismatch after roundtrip", i)
		}
	}
}

func TestQuotaGrantRoundtripConversion(t *testing.T) {
	original := &QuotaGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grant",
			Namespace: "default",
		},
		Spec: QuotaGrantSpec{
			ConsumerRef: ConsumerRef{
				APIGroup:  "example.com",
				Kind:      "Project",
				Name:      "my-project",
				Namespace: "team-ns",
			},
			Allowances: []Allowance{
				{
					ResourceType: "widgets",
					Buckets: []Bucket{
						{Amount: 100},
						{Amount: 50},
					},
				},
				{
					ResourceType: "gadgets",
					Buckets: []Bucket{
						{Amount: 200},
					},
				},
			},
		},
		Status: QuotaGrantStatus{
			ObservedGeneration: 5,
			Conditions: []metav1.Condition{
				{Type: QuotaGrantActive, Status: metav1.ConditionTrue, Reason: QuotaGrantActiveReason},
			},
		},
	}

	scope := conversion.Scope(nil)

	internal := &quota.QuotaGrant{}
	if err := Convert_v1alpha1_QuotaGrant_To_quota_QuotaGrant(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	// Verify internal fields
	if internal.Spec.ConsumerRef.Name != "my-project" {
		t.Errorf("internal consumerRef.name = %q, want %q", internal.Spec.ConsumerRef.Name, "my-project")
	}
	if len(internal.Spec.Allowances) != 2 {
		t.Fatalf("expected 2 allowances, got %d", len(internal.Spec.Allowances))
	}
	if internal.Spec.Allowances[0].Buckets[0].Amount != 100 {
		t.Errorf("internal allowance[0].buckets[0].amount = %d, want 100", internal.Spec.Allowances[0].Buckets[0].Amount)
	}

	roundtripped := &QuotaGrant{}
	if err := Convert_quota_QuotaGrant_To_v1alpha1_QuotaGrant(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, roundtripped.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", roundtripped.Spec, original.Spec)
	}
	if !reflect.DeepEqual(original.Status, roundtripped.Status) {
		t.Errorf("status mismatch after roundtrip")
	}
}

func TestQuotaGrantListRoundtripConversion(t *testing.T) {
	original := &QuotaGrantList{
		Items: []QuotaGrant{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "g1", Namespace: "ns1"},
				Spec: QuotaGrantSpec{
					ConsumerRef: ConsumerRef{Kind: "Project", Name: "p1"},
					Allowances:  []Allowance{{ResourceType: "widgets", Buckets: []Bucket{{Amount: 10}}}},
				},
			},
		},
	}

	scope := conversion.Scope(nil)
	internal := &quota.QuotaGrantList{}
	if err := Convert_v1alpha1_QuotaGrantList_To_quota_QuotaGrantList(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	roundtripped := &QuotaGrantList{}
	if err := Convert_quota_QuotaGrantList_To_v1alpha1_QuotaGrantList(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}
	if !reflect.DeepEqual(original.Items[0].Spec, roundtripped.Items[0].Spec) {
		t.Error("list item spec mismatch after roundtrip")
	}
}

func TestQuotaClaimRoundtripConversion(t *testing.T) {
	original := &QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "default",
		},
		Spec: QuotaClaimSpec{
			ConsumerRef: ConsumerRef{
				APIGroup:  "example.com",
				Kind:      "Project",
				Name:      "my-project",
				Namespace: "team-ns",
			},
			Requests: []ResourceRequest{
				{ResourceType: "widgets", Amount: 10},
				{ResourceType: "gadgets", Amount: 5},
			},
			ResourceRef: UnversionedObjectReference{
				APIGroup:  "apps.example.com",
				Kind:      "Widget",
				Name:      "my-widget",
				Namespace: "default",
			},
		},
		Status: QuotaClaimStatus{
			ObservedGeneration: 1,
			Allocations: []QuotaClaimAllocationStatus{
				{
					ResourceType:     "widgets",
					Status:           QuotaClaimAllocationStatusGranted,
					AllocatedAmount:  10,
					AllocatingBucket: "bucket-1",
				},
			},
			Conditions: []metav1.Condition{
				{Type: QuotaClaimGranted, Status: metav1.ConditionTrue, Reason: QuotaClaimGrantedReason},
			},
		},
	}

	scope := conversion.Scope(nil)

	internal := &quota.QuotaClaim{}
	if err := Convert_v1alpha1_QuotaClaim_To_quota_QuotaClaim(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	// Verify nested types survive conversion
	if internal.Spec.ResourceRef.Kind != "Widget" {
		t.Errorf("internal resourceRef.kind = %q, want %q", internal.Spec.ResourceRef.Kind, "Widget")
	}
	if len(internal.Status.Allocations) != 1 {
		t.Fatalf("expected 1 allocation, got %d", len(internal.Status.Allocations))
	}
	if internal.Status.Allocations[0].AllocatedAmount != 10 {
		t.Errorf("internal allocation[0].allocatedAmount = %d, want 10", internal.Status.Allocations[0].AllocatedAmount)
	}

	roundtripped := &QuotaClaim{}
	if err := Convert_quota_QuotaClaim_To_v1alpha1_QuotaClaim(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, roundtripped.Spec) {
		t.Errorf("spec mismatch after roundtrip:\ngot:  %+v\nwant: %+v", roundtripped.Spec, original.Spec)
	}
	if !reflect.DeepEqual(original.Status.Allocations, roundtripped.Status.Allocations) {
		t.Errorf("status.allocations mismatch after roundtrip")
	}
}

func TestQuotaClaimListRoundtripConversion(t *testing.T) {
	original := &QuotaClaimList{
		Items: []QuotaClaim{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns1"},
				Spec: QuotaClaimSpec{
					Requests: []ResourceRequest{{ResourceType: "widgets", Amount: 5}},
				},
			},
		},
	}

	scope := conversion.Scope(nil)
	internal := &quota.QuotaClaimList{}
	if err := Convert_v1alpha1_QuotaClaimList_To_quota_QuotaClaimList(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	roundtripped := &QuotaClaimList{}
	if err := Convert_quota_QuotaClaimList_To_v1alpha1_QuotaClaimList(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}
	if !reflect.DeepEqual(original.Items[0].Spec, roundtripped.Items[0].Spec) {
		t.Error("list item spec mismatch after roundtrip")
	}
}

func TestQuotaBucketRoundtripConversion(t *testing.T) {
	now := metav1.Now()
	original := &QuotaBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-bucket",
			Namespace: "default",
		},
		Spec: QuotaBucketSpec{
			ConsumerRef: ConsumerRef{
				APIGroup:  "example.com",
				Kind:      "Project",
				Name:      "my-project",
				Namespace: "team-ns",
			},
			ResourceType: "widgets",
		},
		Status: QuotaBucketStatus{
			ObservedGeneration: 3,
			Limit:              100,
			Allocated:          30,
			Available:          70,
			ClaimCount:         3,
			GrantCount:         2,
			ContributingGrantRefs: []ContributingGrantRef{
				{Name: "grant-1", LastObservedGeneration: 1, Amount: 60},
				{Name: "grant-2", LastObservedGeneration: 2, Amount: 40},
			},
			LastReconciliation: &now,
		},
	}

	scope := conversion.Scope(nil)

	internal := &quota.QuotaBucket{}
	if err := Convert_v1alpha1_QuotaBucket_To_quota_QuotaBucket(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	// Verify nested types
	if len(internal.Status.ContributingGrantRefs) != 2 {
		t.Fatalf("expected 2 contributing grant refs, got %d", len(internal.Status.ContributingGrantRefs))
	}
	if internal.Status.LastReconciliation == nil {
		t.Fatal("lastReconciliation should not be nil")
	}

	roundtripped := &QuotaBucket{}
	if err := Convert_quota_QuotaBucket_To_v1alpha1_QuotaBucket(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}

	if !reflect.DeepEqual(original.Spec, roundtripped.Spec) {
		t.Errorf("spec mismatch after roundtrip")
	}
	if original.Status.Limit != roundtripped.Status.Limit {
		t.Errorf("status.limit = %d, want %d", roundtripped.Status.Limit, original.Status.Limit)
	}
	if original.Status.Allocated != roundtripped.Status.Allocated {
		t.Errorf("status.allocated = %d, want %d", roundtripped.Status.Allocated, original.Status.Allocated)
	}
	if original.Status.Available != roundtripped.Status.Available {
		t.Errorf("status.available = %d, want %d", roundtripped.Status.Available, original.Status.Available)
	}
	if original.Status.ClaimCount != roundtripped.Status.ClaimCount {
		t.Errorf("status.claimCount = %d, want %d", roundtripped.Status.ClaimCount, original.Status.ClaimCount)
	}
	if original.Status.GrantCount != roundtripped.Status.GrantCount {
		t.Errorf("status.grantCount = %d, want %d", roundtripped.Status.GrantCount, original.Status.GrantCount)
	}
	if !reflect.DeepEqual(original.Status.ContributingGrantRefs, roundtripped.Status.ContributingGrantRefs) {
		t.Errorf("status.contributingGrantRefs mismatch after roundtrip")
	}
}

func TestQuotaBucketListRoundtripConversion(t *testing.T) {
	original := &QuotaBucketList{
		Items: []QuotaBucket{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "ns1"},
				Spec: QuotaBucketSpec{
					ConsumerRef:  ConsumerRef{Kind: "Project", Name: "p1"},
					ResourceType: "widgets",
				},
			},
		},
	}

	scope := conversion.Scope(nil)
	internal := &quota.QuotaBucketList{}
	if err := Convert_v1alpha1_QuotaBucketList_To_quota_QuotaBucketList(original, internal, scope); err != nil {
		t.Fatalf("v1alpha1 -> internal: %v", err)
	}

	roundtripped := &QuotaBucketList{}
	if err := Convert_quota_QuotaBucketList_To_v1alpha1_QuotaBucketList(internal, roundtripped, scope); err != nil {
		t.Fatalf("internal -> v1alpha1: %v", err)
	}
	if !reflect.DeepEqual(original.Items[0].Spec, roundtripped.Items[0].Spec) {
		t.Error("list item spec mismatch after roundtrip")
	}
}
