package quotadefinition

import (
	"context"
	"slices"
	"testing"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func TestGenerationLifecycle(t *testing.T) {
	ctx := genericapirequest.WithNamespace(context.Background(), "")
	strategy := NewStrategy(testScheme(t))
	def := validQuotaDefinition()
	rest.FillObjectMetaSystemFields(def)
	if err := rest.BeforeCreate(strategy, ctx, def); err != nil {
		t.Fatal(err)
	}
	if def.Generation != 1 {
		t.Fatalf("generation after create = %d, want 1", def.Generation)
	}
	def.ResourceVersion = "1"
	updated := def.DeepCopy()
	updated.Spec.Description = "changed"
	if err := rest.BeforeUpdate(strategy, ctx, updated, def); err != nil {
		t.Fatal(err)
	}
	if updated.Generation != 2 {
		t.Errorf("generation after spec change = %d, want 2", updated.Generation)
	}
}

func TestValidateNameAndBounds(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	def := validQuotaDefinition()
	def.Name = "Upper_Case"
	if errs := strategy.Validate(context.Background(), def); len(errs) == 0 {
		t.Error("invalid name accepted")
	}
	def = validQuotaDefinition()
	def.Spec.ConsumerType.Kind = string(make([]byte, 64))
	if errs := strategy.Validate(context.Background(), def); len(errs) == 0 {
		t.Error("overlong consumerType.kind accepted")
	}
}

func TestSelectableFieldsMatchFieldLabels(t *testing.T) {
	got := registryutil.SpecFieldKeys(SelectableFields(validQuotaDefinition()))
	want := slices.Sorted(slices.Values(v1alpha1.SelectableFieldLabels["QuotaDefinition"]))
	if !slices.Equal(got, want) {
		t.Errorf("SelectableFields keys = %v, field labels = %v", got, want)
	}
}
