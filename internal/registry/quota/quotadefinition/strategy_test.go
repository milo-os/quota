package quotadefinition

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

func validQuotaDefinition() *quota.QuotaDefinition {
	return &quota.QuotaDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-definition",
		},
		Spec: quota.QuotaDefinitionSpec{
			ConsumerType: quota.ConsumerType{
				APIGroup: "example.com",
				Kind:     "Project",
			},
			Type:                 "Entity",
			ResourceType:         "widgets",
			Description:          "Track widgets",
			BaseUnit:             "count",
			DisplayUnit:          "widgets",
			UnitConversionFactor: 1,
			ClaimingResources: []quota.ClaimingResource{
				{APIGroup: "apps.example.com", Kind: "Widget"},
			},
		},
	}
}

func TestNamespaceScoped(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	if strategy.NamespaceScoped() {
		t.Error("QuotaDefinition should be cluster-scoped (NamespaceScoped=false)")
	}
}

func TestPrepareForCreate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	def := validQuotaDefinition()
	def.Status = quota.QuotaDefinitionStatus{
		ObservedGeneration: 5,
		Conditions: []metav1.Condition{
			{Type: "Active", Status: metav1.ConditionTrue},
		},
	}

	strategy.PrepareForCreate(context.Background(), def)

	if def.Status.ObservedGeneration != 0 {
		t.Errorf("status.observedGeneration should be cleared, got %d", def.Status.ObservedGeneration)
	}
	if len(def.Status.Conditions) != 0 {
		t.Errorf("status.conditions should be cleared, got %d conditions", len(def.Status.Conditions))
	}
}

func TestPrepareForUpdate(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldDef := validQuotaDefinition()
	oldDef.Status = quota.QuotaDefinitionStatus{
		ObservedGeneration: 3,
		Conditions: []metav1.Condition{
			{Type: "Active", Status: metav1.ConditionTrue},
		},
	}

	newDef := validQuotaDefinition()
	newDef.Spec.Description = "Updated description"
	newDef.Status = quota.QuotaDefinitionStatus{
		ObservedGeneration: 999,
	}

	strategy.PrepareForUpdate(context.Background(), newDef, oldDef)

	if newDef.Status.ObservedGeneration != 3 {
		t.Errorf("status should be preserved from old, got observedGeneration=%d", newDef.Status.ObservedGeneration)
	}
	if len(newDef.Status.Conditions) != 1 {
		t.Errorf("status conditions should be preserved from old, got %d", len(newDef.Status.Conditions))
	}
}

func TestValidateValid(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	def := validQuotaDefinition()
	errs := strategy.Validate(context.Background(), def)
	if len(errs) != 0 {
		t.Errorf("expected no errors for valid definition, got %v", errs)
	}
}

func TestValidateRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*quota.QuotaDefinition)
		errPath string
	}{
		{
			name:    "missing resourceType",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.ResourceType = "" },
			errPath: "spec.resourceType",
		},
		{
			name:    "missing type",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.Type = "" },
			errPath: "spec.type",
		},
		{
			name:    "missing consumerType kind",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.ConsumerType.Kind = "" },
			errPath: "spec.consumerType.kind",
		},
		{
			name:    "missing baseUnit",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.BaseUnit = "" },
			errPath: "spec.baseUnit",
		},
		{
			name:    "missing displayUnit",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.DisplayUnit = "" },
			errPath: "spec.displayUnit",
		},
		{
			name:    "empty claimingResources",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.ClaimingResources = nil },
			errPath: "spec.claimingResources",
		},
		{
			name:    "claimingResources with empty kind",
			mutate:  func(d *quota.QuotaDefinition) { d.Spec.ClaimingResources = []quota.ClaimingResource{{Kind: ""}} },
			errPath: "spec.claimingResources[0].kind",
		},
	}

	strategy := NewStrategy(testScheme(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := validQuotaDefinition()
			tt.mutate(def)
			errs := strategy.Validate(context.Background(), def)
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

func TestValidateTypeEnum(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		typeVal string
		valid   bool
	}{
		{"Entity", true},
		{"Allocation", true},
		{"Feature", true},
		{"Invalid", false},
		{"entity", false},
	}

	for _, tt := range tests {
		t.Run(tt.typeVal, func(t *testing.T) {
			def := validQuotaDefinition()
			def.Spec.Type = tt.typeVal
			errs := strategy.Validate(context.Background(), def)
			hasTypeErr := false
			for _, e := range errs {
				if e.Field == "spec.type" {
					hasTypeErr = true
				}
			}
			if tt.valid && hasTypeErr {
				t.Errorf("type %q should be valid", tt.typeVal)
			}
			if !tt.valid && !hasTypeErr {
				t.Errorf("type %q should be invalid", tt.typeVal)
			}
		})
	}
}

func TestValidateUnitConversionFactor(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name    string
		factor  int64
		wantErr bool
	}{
		{"positive", 1, false},
		{"large positive", 1000, false},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := validQuotaDefinition()
			def.Spec.UnitConversionFactor = tt.factor
			errs := strategy.Validate(context.Background(), def)
			hasErr := false
			for _, e := range errs {
				if e.Field == "spec.unitConversionFactor" {
					hasErr = true
				}
			}
			if tt.wantErr && !hasErr {
				t.Errorf("expected error for unitConversionFactor=%d", tt.factor)
			}
			if !tt.wantErr && hasErr {
				t.Errorf("unexpected error for unitConversionFactor=%d", tt.factor)
			}
		})
	}
}

func TestValidateUpdateImmutableFields(t *testing.T) {
	strategy := NewStrategy(testScheme(t))

	tests := []struct {
		name   string
		mutate func(*quota.QuotaDefinition)
	}{
		{
			name: "resourceType changed",
			mutate: func(d *quota.QuotaDefinition) {
				d.Spec.ResourceType = "changed"
			},
		},
		{
			name: "type changed",
			mutate: func(d *quota.QuotaDefinition) {
				d.Spec.Type = "Allocation"
			},
		},
		{
			name: "consumerType kind changed",
			mutate: func(d *quota.QuotaDefinition) {
				d.Spec.ConsumerType.Kind = "Organization"
			},
		},
		{
			name: "consumerType apiGroup changed",
			mutate: func(d *quota.QuotaDefinition) {
				d.Spec.ConsumerType.APIGroup = "other.com"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldDef := validQuotaDefinition()
			newDef := validQuotaDefinition()
			tt.mutate(newDef)
			errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef)
			if len(errs) == 0 {
				t.Error("expected validation error for immutable field change, got none")
			}
		})
	}
}

func TestValidateUpdateNoChange(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	oldDef := validQuotaDefinition()
	newDef := validQuotaDefinition()
	newDef.Spec.Description = "changed description"
	newDef.Spec.DisplayUnit = "new-unit"

	errs := strategy.ValidateUpdate(context.Background(), newDef, oldDef)
	if len(errs) != 0 {
		t.Errorf("expected no errors for mutable field changes, got %v", errs)
	}
}

func TestGetAttrs(t *testing.T) {
	def := validQuotaDefinition()
	def.Labels = map[string]string{"env": "test"}

	lbls, flds, err := GetAttrs(def)
	if err != nil {
		t.Fatalf("GetAttrs error: %v", err)
	}
	if lbls["env"] != "test" {
		t.Errorf("expected label env=test, got %v", lbls)
	}
	if flds["spec.consumerType.kind"] != "Project" {
		t.Errorf("expected field spec.consumerType.kind=Project, got %q", flds["spec.consumerType.kind"])
	}
	if flds["spec.consumerType.apiGroup"] != "example.com" {
		t.Errorf("expected field spec.consumerType.apiGroup=example.com, got %q", flds["spec.consumerType.apiGroup"])
	}
	if flds["spec.resourceType"] != "widgets" {
		t.Errorf("expected field spec.resourceType=widgets, got %q", flds["spec.resourceType"])
	}
}

func TestGetAttrsWrongType(t *testing.T) {
	_, _, err := GetAttrs(&quota.QuotaGrant{})
	if err == nil {
		t.Error("expected error for wrong type, got nil")
	}
}

func TestSelectableFields(t *testing.T) {
	def := validQuotaDefinition()
	fields := SelectableFields(def)

	expectedFields := []string{
		"spec.consumerType.kind",
		"spec.consumerType.apiGroup",
		"spec.resourceType",
		"metadata.name",
	}

	for _, f := range expectedFields {
		if _, ok := fields[f]; !ok {
			t.Errorf("expected selectable field %q to be present", f)
		}
	}
}

func TestStatusStrategyPrepareForUpdate(t *testing.T) {
	statusStrategy := NewStatusStrategy(testScheme(t))

	oldDef := validQuotaDefinition()
	oldDef.Labels = map[string]string{"env": "test"}
	oldDef.Annotations = map[string]string{"note": "original"}

	newDef := validQuotaDefinition()
	newDef.Spec.Description = "should be reverted"
	newDef.Labels = map[string]string{"env": "changed"}
	newDef.Annotations = map[string]string{"note": "changed"}
	newDef.Status = quota.QuotaDefinitionStatus{
		ObservedGeneration: 10,
	}

	statusStrategy.PrepareForUpdate(context.Background(), newDef, oldDef)

	if newDef.Spec.Description != "Track widgets" {
		t.Error("spec should be preserved from old object on status update")
	}
	if newDef.Labels["env"] != "test" {
		t.Error("labels should be preserved from old object on status update")
	}
	if newDef.Annotations["note"] != "original" {
		t.Error("annotations should be preserved from old object on status update")
	}
	if newDef.Status.ObservedGeneration != 10 {
		t.Error("status should be kept from new object")
	}
}
