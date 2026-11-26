package claimcreationpolicy

import (
	"context"
	"testing"

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

// validPolicy mirrors milo/config/services/quota/claim-policies/secret-claim-policy.yaml,
// which omits consumerRef so the admission plugin resolves it.
func validPolicy() *quota.ClaimCreationPolicy {
	p := &quota.ClaimCreationPolicy{}
	p.Name = "project-secret-claim-policy"
	p.Spec.Trigger.Resource = quota.ClaimTriggerResource{APIVersion: "v1", Kind: "Secret"}
	p.Spec.Target.ResourceClaimTemplate.Spec.Requests = []quota.ResourceRequest{{ResourceType: "core.miloapis.com/secrets", Amount: 1}}
	return p
}

func TestPrepareForCreateDefaultsDisabled(t *testing.T) {
	p := validPolicy()
	NewStrategy(testScheme(t)).PrepareForCreate(context.Background(), p)
	if p.Spec.Disabled == nil || *p.Spec.Disabled {
		t.Fatalf("expected spec.disabled defaulted to false, got %v", p.Spec.Disabled)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*quota.ClaimCreationPolicy)
		errPath string
		errType field.ErrorType
	}{
		{"template without consumerRef", func(*quota.ClaimCreationPolicy) {}, "", ""},
		{"template with full consumerRef", func(p *quota.ClaimCreationPolicy) {
			p.Spec.Target.ResourceClaimTemplate.Spec.ConsumerRef = quota.ConsumerRef{Kind: "Project", Name: "{{ requestInfo.project }}"}
		}, "", ""},
		{"template with partial consumerRef", func(p *quota.ClaimCreationPolicy) {
			p.Spec.Target.ResourceClaimTemplate.Spec.ConsumerRef = quota.ConsumerRef{Kind: "Project"}
		}, "spec.target.resourceClaimTemplate.spec.consumerRef.name", field.ErrorTypeRequired},
		{"template sets resourceRef", func(p *quota.ClaimCreationPolicy) {
			p.Spec.Target.ResourceClaimTemplate.Spec.ResourceRef = quota.UnversionedObjectReference{Kind: "Secret", Name: "x"}
		}, "spec.target.resourceClaimTemplate.spec.resourceRef", field.ErrorTypeForbidden},
		{"group apiVersion", func(p *quota.ClaimCreationPolicy) { p.Spec.Trigger.Resource.APIVersion = "apps/v1" }, "", ""},
		{"bad apiVersion", func(p *quota.ClaimCreationPolicy) { p.Spec.Trigger.Resource.APIVersion = "apps" }, "spec.trigger.resource.apiVersion", field.ErrorTypeInvalid},
		{"21 requests", func(p *quota.ClaimCreationPolicy) {
			p.Spec.Target.ResourceClaimTemplate.Spec.Requests = make([]quota.ResourceRequest, 21)
			for i := range p.Spec.Target.ResourceClaimTemplate.Spec.Requests {
				p.Spec.Target.ResourceClaimTemplate.Spec.Requests[i] = quota.ResourceRequest{ResourceType: string(rune('a' + i)), Amount: 1}
			}
		}, "spec.target.resourceClaimTemplate.spec.requests", field.ErrorTypeTooMany},
		{"negative amount", func(p *quota.ClaimCreationPolicy) {
			p.Spec.Target.ResourceClaimTemplate.Spec.Requests[0].Amount = -1
		}, "spec.target.resourceClaimTemplate.spec.requests[0].amount", field.ErrorTypeInvalid},
	}
	strategy := NewStrategy(testScheme(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validPolicy()
			tt.mutate(p)
			errs := strategy.Validate(context.Background(), p)
			if tt.errPath == "" {
				if len(errs) != 0 {
					t.Fatalf("expected valid, got %v", errs)
				}
				return
			}
			if len(errs) != 1 || errs[0].Field != tt.errPath || errs[0].Type != tt.errType {
				t.Fatalf("expected one %s at %s, got %v", tt.errType, tt.errPath, errs)
			}
		})
	}
}
