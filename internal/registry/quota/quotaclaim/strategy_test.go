package quotaclaim

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	install.Install(s)
	return s
}

func validQuotaClaim() *quota.QuotaClaim {
	return &quota.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "default",
		},
		Spec: quota.QuotaClaimSpec{
			ConsumerRef: quota.ConsumerRef{
				Kind: "Project",
				Name: "my-project",
			},
			Requests: []quota.ResourceRequest{
				{ResourceType: "widgets", Amount: 10},
			},
			ResourceRef: quota.UnversionedObjectReference{
				APIGroup:  "apps.example.com",
				Kind:      "Widget",
				Name:      "my-widget",
				Namespace: "default",
			},
		},
	}
}

func TestNamespaceScoped(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	if !strategy.NamespaceScoped() {
		t.Error("QuotaClaim should be namespaced (NamespaceScoped=true)")
	}
}

func TestPrepareForCreate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Status = quota.QuotaClaimStatus{
		ObservedGeneration: 5,
		Conditions: []metav1.Condition{
			{Type: "Granted", Status: metav1.ConditionTrue},
		},
		Allocations: []quota.QuotaClaimAllocationStatus{
			{ResourceType: "widgets", Status: "Granted", AllocatedAmount: 10},
		},
	}

	strategy.PrepareForCreate(context.Background(), claim)

	if claim.Status.ObservedGeneration != 0 {
		t.Errorf("status.observedGeneration should be cleared, got %d", claim.Status.ObservedGeneration)
	}
	// Milo's CRD defaulted new claims to Granted=False/PendingEvaluation.
	if len(claim.Status.Conditions) != 1 {
		t.Fatalf("expected exactly the pending Granted condition, got %v", claim.Status.Conditions)
	}
	c := claim.Status.Conditions[0]
	if c.Type != quota.QuotaClaimGranted || c.Status != metav1.ConditionFalse || c.Reason != quota.QuotaClaimPendingReason {
		t.Errorf("unexpected initial condition %+v", c)
	}
	if c.LastTransitionTime.IsZero() {
		t.Error("initial condition must carry a lastTransitionTime")
	}
	if len(claim.Status.Allocations) != 0 {
		t.Errorf("status.allocations should be cleared, got %d", len(claim.Status.Allocations))
	}
}

func TestPrepareForUpdate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldClaim := validQuotaClaim()
	oldClaim.Status = quota.QuotaClaimStatus{
		ObservedGeneration: 3,
		Conditions: []metav1.Condition{
			{Type: "Granted", Status: metav1.ConditionTrue},
		},
	}

	newClaim := validQuotaClaim()
	newClaim.Status = quota.QuotaClaimStatus{ObservedGeneration: 999}

	strategy.PrepareForUpdate(context.Background(), newClaim, oldClaim)

	if newClaim.Status.ObservedGeneration != 3 {
		t.Errorf("status should be preserved from old, got observedGeneration=%d", newClaim.Status.ObservedGeneration)
	}
}

func TestValidateValid(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	errs := strategy.Validate(context.Background(), claim)
	if len(errs) != 0 {
		t.Errorf("expected no errors for valid claim, got %v", errs)
	}
}

func TestValidateRequiredRequests(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = nil

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) == 0 {
		t.Fatal("expected error for empty requests")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.requests" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at spec.requests, got %v", errs)
	}
}

func TestValidateRequestAmountsMustBePositive(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name   string
		amount int64
	}{
		{"negative", -1},
		{"zero", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := validQuotaClaim()
			claim.Spec.Requests = []quota.ResourceRequest{
				{ResourceType: "widgets", Amount: tt.amount},
			}

			errs := strategy.Validate(context.Background(), claim)
			if len(errs) == 0 {
				t.Fatalf("expected error for amount=%d", tt.amount)
			}
			found := false
			for _, e := range errs {
				if e.Field == "spec.requests[0].amount" {
					found = true
				}
			}
			if !found {
				t.Errorf("expected error at spec.requests[0].amount, got %v", errs)
			}
		})
	}
}

func TestValidateZeroRequestAmountInvalid(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = []quota.ResourceRequest{
		{ResourceType: "widgets", Amount: 0},
	}

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) == 0 {
		t.Fatal("expected error for zero request amount")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.requests[0].amount" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at spec.requests[0].amount, got %v", errs)
	}
}

func TestValidatePositiveRequestAmount(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = []quota.ResourceRequest{
		{ResourceType: "widgets", Amount: 1},
	}

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) != 0 {
		t.Errorf("positive request amount should be valid, got errors: %v", errs)
	}
}

func TestValidateNoDuplicateResourceTypes(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = []quota.ResourceRequest{
		{ResourceType: "widgets", Amount: 10},
		{ResourceType: "widgets", Amount: 5},
	}

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) == 0 {
		t.Fatal("expected error for duplicate resource types")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.requests[1].resourceType" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected duplicate error at spec.requests[1].resourceType, got %v", errs)
	}
}

func TestValidateMultipleDistinctResourceTypes(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = []quota.ResourceRequest{
		{ResourceType: "widgets", Amount: 10},
		{ResourceType: "gadgets", Amount: 5},
	}

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) != 0 {
		t.Errorf("distinct resource types should be valid, got errors: %v", errs)
	}
}

func TestValidateMissingResourceType(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	claim := validQuotaClaim()
	claim.Spec.Requests = []quota.ResourceRequest{
		{ResourceType: "", Amount: 10},
	}

	errs := strategy.Validate(context.Background(), claim)
	if len(errs) == 0 {
		t.Fatal("expected error for empty resourceType in request")
	}
}

func TestValidateResourceRef(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name     string
		mutate   func(*quota.QuotaClaim)
		errPaths []string // empty means valid
	}{
		{
			name:   "empty resourceRef is allowed",
			mutate: func(c *quota.QuotaClaim) { c.Spec.ResourceRef = quota.UnversionedObjectReference{} },
		},
		{
			name: "kind and name only is allowed",
			mutate: func(c *quota.QuotaClaim) {
				c.Spec.ResourceRef = quota.UnversionedObjectReference{Kind: "Widget", Name: "w"}
			},
		},
		{
			name:     "only kind is rejected",
			mutate:   func(c *quota.QuotaClaim) { c.Spec.ResourceRef = quota.UnversionedObjectReference{Kind: "Widget"} },
			errPaths: []string{"spec.resourceRef.name"},
		},
		{
			name:     "only name is rejected",
			mutate:   func(c *quota.QuotaClaim) { c.Spec.ResourceRef = quota.UnversionedObjectReference{Name: "w"} },
			errPaths: []string{"spec.resourceRef.kind"},
		},
		{
			name: "only apiGroup is rejected",
			mutate: func(c *quota.QuotaClaim) {
				c.Spec.ResourceRef = quota.UnversionedObjectReference{APIGroup: "apps.example.com"}
			},
			errPaths: []string{"spec.resourceRef.kind", "spec.resourceRef.name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := validQuotaClaim()
			tt.mutate(claim)
			errs := strategy.Validate(context.Background(), claim)
			if len(tt.errPaths) == 0 {
				if len(errs) != 0 {
					t.Fatalf("expected no errors, got %v", errs)
				}
				return
			}
			got := map[string]bool{}
			for _, e := range errs {
				got[e.Field] = true
			}
			for _, p := range tt.errPaths {
				if !got[p] {
					t.Errorf("expected error at %q, got %v", p, errs)
				}
			}
			if len(errs) != len(tt.errPaths) {
				t.Errorf("expected %d errors, got %v", len(tt.errPaths), errs)
			}
		})
	}
}

func TestValidateConsumerRefRequired(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name    string
		mutate  func(*quota.QuotaClaim)
		errPath string
	}{
		{"empty consumerRef", func(c *quota.QuotaClaim) { c.Spec.ConsumerRef = quota.ConsumerRef{} }, "spec.consumerRef.kind"},
		{"missing kind", func(c *quota.QuotaClaim) { c.Spec.ConsumerRef.Kind = "" }, "spec.consumerRef.kind"},
		{"missing name", func(c *quota.QuotaClaim) { c.Spec.ConsumerRef.Name = "" }, "spec.consumerRef.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := validQuotaClaim()
			tt.mutate(claim)
			errs := strategy.Validate(context.Background(), claim)
			found := false
			for _, e := range errs {
				if e.Field == tt.errPath && e.Type == field.ErrorTypeRequired {
					found = true
				}
			}
			if !found {
				t.Errorf("expected Required error at %q, got %v", tt.errPath, errs)
			}
		})
	}
}

func TestValidateUpdateRatchetsEmptyConsumerRef(t *testing.T) {
	// Claims stored before consumerRef became required must remain
	// updatable (for example to remove a finalizer).
	strategy := NewStrategy(testScheme(t))
	oldClaim := validQuotaClaim()
	oldClaim.Spec.ConsumerRef = quota.ConsumerRef{}
	newClaim := oldClaim.DeepCopy()
	newClaim.Finalizers = nil
	if errs := strategy.ValidateUpdate(context.Background(), newClaim, oldClaim); len(errs) != 0 {
		t.Fatalf("expected legacy claim update to be allowed, got %v", errs)
	}
}

func TestValidateMaxRequests(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	build := func(n int) *quota.QuotaClaim {
		c := validQuotaClaim()
		c.Spec.Requests = nil
		for i := 0; i < n; i++ {
			c.Spec.Requests = append(c.Spec.Requests, quota.ResourceRequest{ResourceType: fmt.Sprintf("rt-%d", i), Amount: 1})
		}
		return c
	}
	if errs := strategy.Validate(context.Background(), build(20)); len(errs) != 0 {
		t.Errorf("20 requests should be valid, got %v", errs)
	}
	errs := strategy.Validate(context.Background(), build(21))
	if len(errs) != 1 || errs[0].Type != field.ErrorTypeTooMany || errs[0].Field != "spec.requests" {
		t.Errorf("expected a single TooMany on spec.requests, got %v", errs)
	}
}

func TestStatusValidateUpdate(t *testing.T) {
	strategy := NewStatusStrategy(testScheme(t))
	now := metav1.Now()
	tests := []struct {
		name    string
		status  quota.QuotaClaimStatus
		wantErr string
	}{
		{name: "pending condition", status: quota.QuotaClaimStatus{Conditions: []metav1.Condition{PendingCondition(now)}}},
		{
			name: "granted with allocation",
			status: quota.QuotaClaimStatus{
				Conditions:  []metav1.Condition{{Type: quota.QuotaClaimGranted, Status: metav1.ConditionTrue, Reason: quota.QuotaClaimGrantedReason, LastTransitionTime: now}},
				Allocations: []quota.QuotaClaimAllocationStatus{{ResourceType: "widgets", Status: quota.QuotaClaimAllocationStatusGranted, AllocatedAmount: 10}},
			},
		},
		{
			name:    "unknown Granted reason",
			status:  quota.QuotaClaimStatus{Conditions: []metav1.Condition{{Type: quota.QuotaClaimGranted, Status: metav1.ConditionFalse, Reason: "Bogus", LastTransitionTime: now}}},
			wantErr: "status.conditions[0].reason",
		},
		{
			name:    "unknown allocation status",
			status:  quota.QuotaClaimStatus{Allocations: []quota.QuotaClaimAllocationStatus{{ResourceType: "widgets", Status: "Maybe"}}},
			wantErr: "status.allocations[0].status",
		},
		{
			name:    "negative allocated amount",
			status:  quota.QuotaClaimStatus{Allocations: []quota.QuotaClaimAllocationStatus{{ResourceType: "widgets", Status: "Granted", AllocatedAmount: -1}}},
			wantErr: "status.allocations[0].allocatedAmount",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldClaim := validQuotaClaim()
			newClaim := validQuotaClaim()
			newClaim.Status = tt.status
			errs := strategy.ValidateUpdate(context.Background(), newClaim, oldClaim)
			if tt.wantErr == "" {
				if len(errs) != 0 {
					t.Fatalf("expected no errors, got %v", errs)
				}
				return
			}
			found := false
			for _, e := range errs {
				if e.Field == tt.wantErr {
					found = true
				}
			}
			if !found {
				t.Errorf("expected error at %q, got %v", tt.wantErr, errs)
			}
		})
	}
}

func TestValidateUpdateImmutableFields(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name   string
		path   string
		mutate func(*quota.QuotaClaim)
	}{
		{
			name: "consumerRef changed",
			path: "spec.consumerRef",
			mutate: func(c *quota.QuotaClaim) {
				c.Spec.ConsumerRef.Name = "different-project"
			},
		},
		{
			name: "resourceRef changed",
			path: "spec.resourceRef",
			mutate: func(c *quota.QuotaClaim) {
				c.Spec.ResourceRef.Name = "different-widget"
			},
		},
		{
			name: "requests changed",
			path: "spec.requests",
			mutate: func(c *quota.QuotaClaim) {
				c.Spec.Requests[0].Amount = 11
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldClaim := validQuotaClaim()
			newClaim := validQuotaClaim()
			tt.mutate(newClaim)
			errs := strategy.ValidateUpdate(context.Background(), newClaim, oldClaim)
			if len(errs) != 1 {
				t.Fatalf("expected one error for immutable field change, got %v", errs)
			}
			if errs[0].Type != field.ErrorTypeForbidden || errs[0].Field != tt.path {
				t.Errorf("expected Forbidden on %s, got %v", tt.path, errs[0])
			}
		})
	}
}

func TestGetAttrs(t *testing.T) {
	claim := validQuotaClaim()
	claim.Labels = map[string]string{"env": "test"}

	lbls, flds, err := GetAttrs(claim)
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
	if flds["spec.resourceRef.apiGroup"] != "apps.example.com" {
		t.Errorf("expected field spec.resourceRef.apiGroup=apps.example.com, got %q", flds["spec.resourceRef.apiGroup"])
	}
	if flds["spec.resourceRef.kind"] != "Widget" {
		t.Errorf("expected field spec.resourceRef.kind=Widget, got %q", flds["spec.resourceRef.kind"])
	}
	if flds["spec.resourceRef.name"] != "my-widget" {
		t.Errorf("expected field spec.resourceRef.name=my-widget, got %q", flds["spec.resourceRef.name"])
	}
	if flds["spec.resourceRef.namespace"] != "default" {
		t.Errorf("expected field spec.resourceRef.namespace=default, got %q", flds["spec.resourceRef.namespace"])
	}
}

func TestGetAttrsWrongType(t *testing.T) {
	_, _, err := GetAttrs(&quota.QuotaGrant{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}
}

func TestStatusStrategyPrepareForUpdate(t *testing.T) {
	statusStrategy := NewStatusStrategy(testScheme(t))

	oldClaim := validQuotaClaim()
	oldClaim.Labels = map[string]string{"env": "test"}
	oldClaim.Annotations = map[string]string{"note": "original"}

	newClaim := validQuotaClaim()
	newClaim.Spec.ConsumerRef.Name = "should-be-reverted"
	newClaim.Labels = map[string]string{"env": "changed"}
	newClaim.Status = quota.QuotaClaimStatus{ObservedGeneration: 10}

	statusStrategy.PrepareForUpdate(context.Background(), newClaim, oldClaim)

	if newClaim.Spec.ConsumerRef.Name != "my-project" {
		t.Error("spec should be preserved from old object on status update")
	}
	if newClaim.Labels["env"] != "test" {
		t.Error("labels should be preserved from old object on status update")
	}
	if newClaim.Status.ObservedGeneration != 10 {
		t.Error("status should be kept from new object")
	}
}
