package quotaclaim

import (
	"context"
	"slices"
	"testing"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func TestGenerationStartsAtOne(t *testing.T) {
	ctx := genericapirequest.WithNamespace(context.Background(), "default")
	c := validQuotaClaim()
	c.Generation = 3
	rest.FillObjectMetaSystemFields(c)
	if err := rest.BeforeCreate(NewStrategy(testScheme(t)), ctx, c); err != nil {
		t.Fatal(err)
	}
	if c.Generation != 1 {
		t.Fatalf("generation after create = %d, want 1", c.Generation)
	}
}

func TestValidateAmountCapAndName(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	c := validQuotaClaim()
	c.Spec.Requests[0].Amount = quota.MaxAmount + 1
	if errs := strategy.Validate(context.Background(), c); len(errs) == 0 {
		t.Error("amount above quota.MaxAmount accepted")
	}
	c = validQuotaClaim()
	c.Name = "Bad_Name"
	if errs := strategy.Validate(context.Background(), c); len(errs) == 0 {
		t.Error("invalid name accepted")
	}
	c = validQuotaClaim()
	c.Spec.ConsumerRef.Namespace = "Not A Namespace"
	if errs := strategy.Validate(context.Background(), c); len(errs) == 0 {
		t.Error("invalid consumerRef.namespace accepted")
	}
}

func grantedAllocation(amount int64) quota.QuotaClaimAllocationStatus {
	return quota.QuotaClaimAllocationStatus{
		ResourceType:    "widgets",
		Status:          quota.QuotaClaimAllocationStatusGranted,
		Reason:          quota.QuotaClaimGrantedReason,
		AllocatedAmount: amount,
	}
}

func TestGrantedAllocationIsFinal(t *testing.T) {
	strategy := NewStatusStrategy(testScheme(t))
	old := validQuotaClaim()
	old.Status.Allocations = []quota.QuotaClaimAllocationStatus{grantedAllocation(10)}

	denied := old.DeepCopy()
	denied.Status.Allocations[0].Status = quota.QuotaClaimAllocationStatusDenied
	denied.Status.Allocations[0].Reason = quota.QuotaClaimDeniedReason
	denied.Status.Allocations[0].AllocatedAmount = 0
	if errs := strategy.ValidateUpdate(context.Background(), denied, old); len(errs) == 0 {
		t.Error("Granted -> Denied allowed")
	}

	removed := old.DeepCopy()
	removed.Status.Allocations = nil
	if errs := strategy.ValidateUpdate(context.Background(), removed, old); len(errs) == 0 {
		t.Error("removing a granted allocation allowed")
	}

	same := old.DeepCopy()
	same.Status.Allocations[0].Message = "still granted"
	if errs := strategy.ValidateUpdate(context.Background(), same, old); len(errs) != 0 {
		t.Errorf("re-applying a granted allocation rejected: %v", errs)
	}

	pending := validQuotaClaim()
	decided := pending.DeepCopy()
	decided.Status.Allocations = []quota.QuotaClaimAllocationStatus{grantedAllocation(10)}
	if errs := strategy.ValidateUpdate(context.Background(), decided, pending); len(errs) != 0 {
		t.Errorf("pending -> granted rejected: %v", errs)
	}
}

func TestSelectableFieldsMatchFieldLabels(t *testing.T) {
	got := registryutil.SpecFieldKeys(SelectableFields(validQuotaClaim()))
	want := slices.Sorted(slices.Values(v1alpha1.SelectableFieldLabels["QuotaClaim"]))
	if !slices.Equal(got, want) {
		t.Errorf("SelectableFields keys = %v, field labels = %v", got, want)
	}
}
