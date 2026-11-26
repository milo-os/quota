package quotadefinition

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// Rules ported from Milo's ResourceRegistration kubebuilder markers.
func TestValidateMiloSchemaRules(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	tests := []struct {
		name    string
		mutate  func(*quota.QuotaDefinition)
		errPath string
		errType field.ErrorType
	}{
		{"feature type accepted", func(d *quota.QuotaDefinition) { d.Spec.Type = quota.QuotaDefinitionTypeFeature }, "", ""},
		{"core consumer group accepted", func(d *quota.QuotaDefinition) { d.Spec.ConsumerType.APIGroup = "" }, "", ""},
		{"unknown type", func(d *quota.QuotaDefinition) { d.Spec.Type = "Boolean" }, "spec.type", field.ErrorTypeNotSupported},
		{"bad consumer apiGroup", func(d *quota.QuotaDefinition) { d.Spec.ConsumerType.APIGroup = "Example.COM" }, "spec.consumerType.apiGroup", field.ErrorTypeInvalid},
		{"bad claiming apiGroup", func(d *quota.QuotaDefinition) { d.Spec.ClaimingResources[0].APIGroup = "-bad" }, "spec.claimingResources[0].apiGroup", field.ErrorTypeInvalid},
		{"claiming kind too long", func(d *quota.QuotaDefinition) { d.Spec.ClaimingResources[0].Kind = strings.Repeat("K", 64) }, "spec.claimingResources[0].kind", field.ErrorTypeTooLong},
		{"resourceType too long", func(d *quota.QuotaDefinition) { d.Spec.ResourceType = strings.Repeat("r", 254) }, "spec.resourceType", field.ErrorTypeTooLong},
		{"description too long", func(d *quota.QuotaDefinition) { d.Spec.Description = strings.Repeat("d", 501) }, "spec.description", field.ErrorTypeTooLong},
		{"baseUnit too long", func(d *quota.QuotaDefinition) { d.Spec.BaseUnit = strings.Repeat("b", 51) }, "spec.baseUnit", field.ErrorTypeTooLong},
		{"displayUnit too long", func(d *quota.QuotaDefinition) { d.Spec.DisplayUnit = strings.Repeat("u", 51) }, "spec.displayUnit", field.ErrorTypeTooLong},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := validQuotaDefinition()
			tt.mutate(def)
			errs := strategy.Validate(context.Background(), def)
			if tt.errPath == "" {
				if len(errs) != 0 {
					t.Fatalf("expected valid, got %v", errs)
				}
				return
			}
			if len(errs) != 1 || errs[0].Field != tt.errPath || errs[0].Type != tt.errType {
				t.Fatalf("expected one %s error at %s, got %v", tt.errType, tt.errPath, errs)
			}
		})
	}
}

func TestValidateUpdateImmutableFieldsForbidden(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldDef := validQuotaDefinition()
	newDef := validQuotaDefinition()
	newDef.Spec.Type = quota.QuotaDefinitionTypeFeature
	errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef)
	if len(errs) != 1 || errs[0].Type != field.ErrorTypeForbidden || errs[0].Detail != "spec.type is immutable" {
		t.Fatalf("expected Forbidden 'spec.type is immutable', got %v", errs)
	}
}

func TestValidateUpdateRatchetsLegacyViolations(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldDef := validQuotaDefinition()
	oldDef.Spec.Description = strings.Repeat("d", 600) // stored before the 500-char limit existed
	newDef := oldDef.DeepCopy()
	newDef.Labels = map[string]string{"touched": "true"}
	if errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef); len(errs) != 0 {
		t.Fatalf("unchanged legacy value should be tolerated, got %v", errs)
	}
	newDef.Spec.Description = strings.Repeat("d", 601)
	if errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef); len(errs) != 1 {
		t.Fatalf("changed over-long value should be rejected, got %v", errs)
	}
}

func TestStatusValidateActiveReason(t *testing.T) {
	strategy := NewStatusStrategy(testScheme(t))
	now := metav1.Now()
	oldDef := validQuotaDefinition()
	for reason, valid := range map[string]bool{
		quota.QuotaDefinitionActiveReason:           true,
		quota.QuotaDefinitionPendingReason:          true,
		quota.QuotaDefinitionValidationFailedReason: true,
		quota.QuotaDefinitionNotFoundReason:         false, // not in Milo's allowed list
	} {
		newDef := validQuotaDefinition()
		newDef.Status.Conditions = []metav1.Condition{{Type: quota.QuotaDefinitionActive, Status: metav1.ConditionTrue, Reason: reason, LastTransitionTime: now}}
		errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef)
		if valid != (len(errs) == 0) {
			t.Errorf("reason %q: valid=%v, errs=%v", reason, valid, errs)
		}
	}
}
