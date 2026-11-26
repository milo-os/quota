package quotabucket

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
	ctx := genericapirequest.WithNamespace(context.Background(), "default")
	scheme := testScheme(t)
	b := validQuotaBucket()
	b.Generation = 4
	rest.FillObjectMetaSystemFields(b)
	if err := rest.BeforeCreate(NewStrategy(scheme), ctx, b); err != nil {
		t.Fatal(err)
	}
	if b.Generation != 1 {
		t.Fatalf("generation after create = %d, want 1", b.Generation)
	}
	b.ResourceVersion = "1"

	status := b.DeepCopy()
	status.Status.Limit = 10
	status.Status.Available = 10
	if err := rest.BeforeUpdate(NewStatusStrategy(scheme), ctx, status, b); err != nil {
		t.Fatal(err)
	}
	if status.Generation != 1 {
		t.Errorf("status update changed generation to %d", status.Generation)
	}
}

func TestConsumerRefNamespaceImmutable(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	old := validQuotaBucket()
	moved := old.DeepCopy()
	moved.Spec.ConsumerRef.Namespace = "elsewhere"
	if errs := strategy.ValidateUpdate(context.Background(), moved, old); len(errs) == 0 {
		t.Fatal("changing consumerRef.namespace must be rejected")
	}
}

func TestSelectableFieldsMatchFieldLabels(t *testing.T) {
	got := registryutil.SpecFieldKeys(SelectableFields(validQuotaBucket()))
	want := slices.Sorted(slices.Values(v1alpha1.SelectableFieldLabels["QuotaBucket"]))
	if !slices.Equal(got, want) {
		t.Errorf("SelectableFields keys = %v, field labels = %v", got, want)
	}
}
