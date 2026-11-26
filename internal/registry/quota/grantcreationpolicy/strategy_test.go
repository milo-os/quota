package grantcreationpolicy

import (
	"context"
	"fmt"
	"strings"
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

func boolPtr(b bool) *bool { return &b }

func validPolicy() *quota.GrantCreationPolicy {
	p := &quota.GrantCreationPolicy{}
	p.Name = "org-defaults"
	p.Spec.Trigger.Resource = quota.GrantTriggerResource{APIVersion: "resourcemanager.miloapis.com/v1alpha1", Kind: "Organization"}
	p.Spec.Trigger.Constraints = []quota.ConditionExpression{{Expression: "true"}}
	p.Spec.Target.ResourceGrantTemplate.Spec = quota.QuotaGrantSpec{
		ConsumerRef: quota.ConsumerRef{APIGroup: "resourcemanager.miloapis.com", Kind: "Organization", Name: "{{ trigger.metadata.name }}"},
		Allowances:  []quota.Allowance{{ResourceType: "projects", Buckets: []quota.Bucket{{Amount: 10}}}},
	}
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
	tooManyAllowances := func(p *quota.GrantCreationPolicy) {
		p.Spec.Target.ResourceGrantTemplate.Spec.Allowances = nil
		for i := 0; i < 21; i++ {
			p.Spec.Target.ResourceGrantTemplate.Spec.Allowances = append(p.Spec.Target.ResourceGrantTemplate.Spec.Allowances,
				quota.Allowance{ResourceType: fmt.Sprintf("rt-%d", i), Buckets: []quota.Bucket{{Amount: 1}}})
		}
	}
	tests := []struct {
		name    string
		mutate  func(*quota.GrantCreationPolicy)
		errPath string
		errType field.ErrorType
	}{
		{"valid", func(*quota.GrantCreationPolicy) {}, "", ""},
		{"core apiVersion", func(p *quota.GrantCreationPolicy) { p.Spec.Trigger.Resource.APIVersion = "v1" }, "", ""},
		{"enabled policy with constraints", func(p *quota.GrantCreationPolicy) { p.Spec.Disabled = boolPtr(false) }, "", ""},
		// The three CEL rules Milo enforced at the type level.
		{"disabled policy with constraints", func(p *quota.GrantCreationPolicy) { p.Spec.Disabled = boolPtr(true) }, "spec.trigger.constraints", field.ErrorTypeForbidden},
		{"parent context without nameExpression", func(p *quota.GrantCreationPolicy) {
			p.Spec.Target.ParentContext = &quota.GrantParentContextSpec{APIGroup: "resourcemanager.miloapis.com", Kind: "Organization"}
		}, "spec.target.parentContext.nameExpression", field.ErrorTypeRequired},
		{"21 allowances", tooManyAllowances, "spec.target.resourceGrantTemplate.spec.allowances", field.ErrorTypeTooMany},
		// Marker ports.
		{"bad apiVersion", func(p *quota.GrantCreationPolicy) { p.Spec.Trigger.Resource.APIVersion = "Foo/v1" }, "spec.trigger.resource.apiVersion", field.ErrorTypeInvalid},
		{"lowercase kind", func(p *quota.GrantCreationPolicy) { p.Spec.Trigger.Resource.Kind = "organization" }, "spec.trigger.resource.kind", field.ErrorTypeInvalid},
		{"11 constraints", func(p *quota.GrantCreationPolicy) {
			p.Spec.Trigger.Constraints = make([]quota.ConditionExpression, 11)
			for i := range p.Spec.Trigger.Constraints {
				p.Spec.Trigger.Constraints[i].Expression = "true"
			}
		}, "spec.trigger.constraints", field.ErrorTypeTooMany},
		{"expression too long", func(p *quota.GrantCreationPolicy) {
			p.Spec.Trigger.Constraints[0].Expression = strings.Repeat("x", 1025)
		}, "spec.trigger.constraints[0].expression", field.ErrorTypeTooLong},
		{"message too long", func(p *quota.GrantCreationPolicy) { p.Spec.Trigger.Constraints[0].Message = strings.Repeat("x", 257) }, "spec.trigger.constraints[0].message", field.ErrorTypeTooLong},
		{"core-group parent context", func(p *quota.GrantCreationPolicy) {
			p.Spec.Target.ParentContext = &quota.GrantParentContextSpec{Kind: "Namespace", NameExpression: "trigger.metadata.name"}
		}, "", ""},
		{"parent context bad apiGroup", func(p *quota.GrantCreationPolicy) {
			p.Spec.Target.ParentContext = &quota.GrantParentContextSpec{APIGroup: "Bad_Group", Kind: "Organization", NameExpression: "x"}
		}, "spec.target.parentContext.apiGroup", field.ErrorTypeInvalid},
		{"parent context bad kind", func(p *quota.GrantCreationPolicy) {
			p.Spec.Target.ParentContext = &quota.GrantParentContextSpec{APIGroup: "resourcemanager.miloapis.com", Kind: "org", NameExpression: "x"}
		}, "spec.target.parentContext.kind", field.ErrorTypeInvalid},
		{"allowance without buckets", func(p *quota.GrantCreationPolicy) {
			p.Spec.Target.ResourceGrantTemplate.Spec.Allowances[0].Buckets = nil
		}, "spec.target.resourceGrantTemplate.spec.allowances[0].buckets", field.ErrorTypeRequired},
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
