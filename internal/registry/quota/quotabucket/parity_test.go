package quotabucket

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateResourceTypeMaxLength(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	b := validQuotaBucket()
	b.Spec.ResourceType = strings.Repeat("r", 254)
	errs := strategy.Validate(context.Background(), b)
	if len(errs) != 1 || errs[0].Type != field.ErrorTypeTooLong {
		t.Fatalf("expected TooLong, got %v", errs)
	}
}

func TestValidateUpdateImmutableForbidden(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldB := validQuotaBucket()
	newB := validQuotaBucket()
	newB.Spec.ResourceType = "other"
	errs := strategy.ValidateUpdate(context.Background(), newB, oldB)
	if len(errs) != 1 || errs[0].Type != field.ErrorTypeForbidden {
		t.Fatalf("expected Forbidden, got %v", errs)
	}
}

func TestStatusValidateNonNegative(t *testing.T) {
	strategy := NewStatusStrategy(testScheme(t))
	newB := validQuotaBucket()
	newB.Status.Available = -1
	errs := strategy.ValidateUpdate(context.Background(), newB, validQuotaBucket())
	if len(errs) != 1 || errs[0].Field != "status.available" {
		t.Fatalf("expected error on status.available, got %v", errs)
	}
	// Unchanged legacy negative values are ratcheted.
	if errs := strategy.ValidateUpdate(context.Background(), newB.DeepCopy(), newB); len(errs) != 0 {
		t.Fatalf("expected ratchet, got %v", errs)
	}
}
