package quotagrant

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.miloapis.com/quota/pkg/apis/quota"
)

func TestValidateUpdateConsumerRefForbidden(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldGrant := validQuotaGrant()
	newGrant := validQuotaGrant()
	newGrant.Spec.ConsumerRef.Name = "someone-else"
	errs := strategy.ValidateUpdate(context.Background(), newGrant, oldGrant)
	if len(errs) != 1 || errs[0].Type != field.ErrorTypeForbidden || errs[0].Field != "spec.consumerRef" {
		t.Fatalf("expected Forbidden on spec.consumerRef, got %v", errs)
	}
}

func TestStatusValidateActiveReason(t *testing.T) {
	strategy := NewStatusStrategy(testScheme(t))
	now := metav1.Now()
	for reason, valid := range map[string]bool{
		quota.QuotaGrantActiveReason:           true,
		quota.QuotaGrantPendingReason:          true,
		quota.QuotaGrantValidationFailedReason: true,
		"SomethingElse":                        false,
	} {
		newGrant := validQuotaGrant()
		newGrant.Status.Conditions = []metav1.Condition{{Type: quota.QuotaGrantActive, Status: metav1.ConditionTrue, Reason: reason, LastTransitionTime: now}}
		errs := strategy.ValidateUpdate(context.Background(), newGrant, validQuotaGrant())
		if valid != (len(errs) == 0) {
			t.Errorf("reason %q: valid=%v, errs=%v", reason, valid, errs)
		}
	}
}
