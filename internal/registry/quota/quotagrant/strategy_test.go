package quotagrant

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	install.Install(s)
	return s
}

func validQuotaGrant() *quota.QuotaGrant {
	return &quota.QuotaGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grant",
			Namespace: "default",
		},
		Spec: quota.QuotaGrantSpec{
			ConsumerRef: quota.ConsumerRef{
				APIGroup: "example.com",
				Kind:     "Project",
				Name:     "my-project",
			},
			Allowances: []quota.Allowance{
				{
					ResourceType: "widgets",
					Buckets: []quota.Bucket{
						{Amount: 100},
					},
				},
			},
		},
	}
}

func TestNamespaceScoped(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	if !strategy.NamespaceScoped() {
		t.Error("QuotaGrant should be namespaced (NamespaceScoped=true)")
	}
}

func TestPrepareForCreate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	grant := validQuotaGrant()
	grant.Status = quota.QuotaGrantStatus{
		ObservedGeneration: 5,
		Conditions: []metav1.Condition{
			{Type: "Active", Status: metav1.ConditionTrue},
		},
	}

	strategy.PrepareForCreate(context.Background(), grant)

	if grant.Status.ObservedGeneration != 0 {
		t.Errorf("status.observedGeneration should be cleared, got %d", grant.Status.ObservedGeneration)
	}
	if len(grant.Status.Conditions) != 0 {
		t.Errorf("status.conditions should be cleared, got %d conditions", len(grant.Status.Conditions))
	}
}

func TestPrepareForUpdate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldGrant := validQuotaGrant()
	oldGrant.Status = quota.QuotaGrantStatus{
		ObservedGeneration: 3,
		Conditions: []metav1.Condition{
			{Type: "Active", Status: metav1.ConditionTrue},
		},
	}

	newGrant := validQuotaGrant()
	newGrant.Status = quota.QuotaGrantStatus{ObservedGeneration: 999}

	strategy.PrepareForUpdate(context.Background(), newGrant, oldGrant)

	if newGrant.Status.ObservedGeneration != 3 {
		t.Errorf("status should be preserved from old, got observedGeneration=%d", newGrant.Status.ObservedGeneration)
	}
}

func TestValidateValid(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	grant := validQuotaGrant()
	errs := strategy.Validate(context.Background(), grant)
	if len(errs) != 0 {
		t.Errorf("expected no errors for valid grant, got %v", errs)
	}
}

func TestValidateRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*quota.QuotaGrant)
		errPath string
	}{
		{
			name:    "missing consumerRef kind",
			mutate:  func(g *quota.QuotaGrant) { g.Spec.ConsumerRef.Kind = "" },
			errPath: "spec.consumerRef.kind",
		},
		{
			name:    "missing consumerRef name",
			mutate:  func(g *quota.QuotaGrant) { g.Spec.ConsumerRef.Name = "" },
			errPath: "spec.consumerRef.name",
		},
		{
			name:    "empty allowances",
			mutate:  func(g *quota.QuotaGrant) { g.Spec.Allowances = nil },
			errPath: "spec.allowances",
		},
		{
			name: "missing allowance resourceType",
			mutate: func(g *quota.QuotaGrant) {
				g.Spec.Allowances[0].ResourceType = ""
			},
			errPath: "spec.allowances[0].resourceType",
		},
		{
			name: "empty buckets",
			mutate: func(g *quota.QuotaGrant) {
				g.Spec.Allowances[0].Buckets = nil
			},
			errPath: "spec.allowances[0].buckets",
		},
	}

	strategy := NewStrategy(testScheme(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grant := validQuotaGrant()
			tt.mutate(grant)
			errs := strategy.Validate(context.Background(), grant)
			if len(errs) == 0 {
				t.Fatalf("expected validation errors, got none")
			}
			found := false
			for _, e := range errs {
				if e.Field == tt.errPath {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected error at path %q, got errors: %v", tt.errPath, errs)
			}
		})
	}
}

func TestValidateBucketAmountsNonNegative(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	grant := validQuotaGrant()
	grant.Spec.Allowances[0].Buckets = []quota.Bucket{{Amount: -1}}

	errs := strategy.Validate(context.Background(), grant)
	if len(errs) == 0 {
		t.Fatal("expected error for negative bucket amount")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.allowances[0].buckets[0].amount" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at spec.allowances[0].buckets[0].amount, got %v", errs)
	}
}

func TestValidateZeroBucketAmount(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	grant := validQuotaGrant()
	grant.Spec.Allowances[0].Buckets = []quota.Bucket{{Amount: 0}}

	errs := strategy.Validate(context.Background(), grant)
	if len(errs) != 0 {
		t.Errorf("zero bucket amount should be valid, got errors: %v", errs)
	}
}

func TestGetAttrs(t *testing.T) {
	grant := validQuotaGrant()
	grant.Labels = map[string]string{"env": "test"}

	lbls, flds, err := GetAttrs(grant)
	if err != nil {
		t.Fatalf("GetAttrs error: %v", err)
	}
	if lbls["env"] != "test" {
		t.Errorf("expected label env=test, got %v", lbls)
	}
	if flds["spec.consumerRef.kind"] != "Project" {
		t.Errorf("expected field spec.consumerRef.kind=Project, got %q", flds["spec.consumerRef.kind"])
	}
	if flds["spec.consumerRef.name"] != "my-project" {
		t.Errorf("expected field spec.consumerRef.name=my-project, got %q", flds["spec.consumerRef.name"])
	}
}

func TestGetAttrsWrongType(t *testing.T) {
	_, _, err := GetAttrs(&quota.QuotaDefinition{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}
}

func TestStatusStrategyPrepareForUpdate(t *testing.T) {
	statusStrategy := NewStatusStrategy(testScheme(t))

	oldGrant := validQuotaGrant()
	oldGrant.Labels = map[string]string{"env": "test"}
	oldGrant.Annotations = map[string]string{"note": "original"}

	newGrant := validQuotaGrant()
	newGrant.Spec.ConsumerRef.Name = "should-be-reverted"
	newGrant.Labels = map[string]string{"env": "changed"}
	newGrant.Status = quota.QuotaGrantStatus{ObservedGeneration: 10}

	statusStrategy.PrepareForUpdate(context.Background(), newGrant, oldGrant)

	if newGrant.Spec.ConsumerRef.Name != "my-project" {
		t.Error("spec should be preserved from old object on status update")
	}
	if newGrant.Labels["env"] != "test" {
		t.Error("labels should be preserved from old object on status update")
	}
	if newGrant.Status.ObservedGeneration != 10 {
		t.Error("status should be kept from new object")
	}
}
