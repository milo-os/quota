package claimcreationpolicy

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
	p := validPolicy()
	rest.FillObjectMetaSystemFields(p)
	if err := rest.BeforeCreate(strategy, ctx, p); err != nil {
		t.Fatal(err)
	}
	if p.Generation != 1 {
		t.Fatalf("generation after create = %d, want 1", p.Generation)
	}
	p.ResourceVersion = "1"

	// A stored nil spec.disabled and a defaulted false are the same spec.
	stored := p.DeepCopy()
	stored.Spec.Disabled = nil
	same := stored.DeepCopy()
	if err := rest.BeforeUpdate(strategy, ctx, same, stored); err != nil {
		t.Fatal(err)
	}
	if same.Generation != 1 {
		t.Errorf("defaulting alone bumped generation to %d", same.Generation)
	}
	if stored.Spec.Disabled != nil {
		t.Error("PrepareForUpdate mutated the stored object")
	}

	changed := p.DeepCopy()
	changed.Spec.Trigger.Resource.Kind = "Other"
	if err := rest.BeforeUpdate(strategy, ctx, changed, p); err != nil {
		t.Fatal(err)
	}
	if changed.Generation != 2 {
		t.Errorf("generation after spec change = %d, want 2", changed.Generation)
	}
}

func TestSelectableFieldsMatchFieldLabels(t *testing.T) {
	got := registryutil.SpecFieldKeys(SelectableFields(validPolicy()))
	want := slices.Sorted(slices.Values(v1alpha1.SelectableFieldLabels["ClaimCreationPolicy"]))
	if !slices.Equal(got, want) {
		t.Errorf("SelectableFields keys = %v, field labels = %v", got, want)
	}
}

func TestTemplateRequestAmounts(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	for _, amount := range []int64{0, -1, 1_000_000_000_000_000_001} {
		p := validPolicy()
		p.Spec.Target.ResourceClaimTemplate.Spec.Requests[0].Amount = amount
		if errs := strategy.Validate(context.Background(), p); len(errs) == 0 {
			t.Errorf("template amount %d accepted; the rendered claim would be rejected", amount)
		}
	}
}
