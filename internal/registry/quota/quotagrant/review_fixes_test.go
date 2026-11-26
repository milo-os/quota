package quotagrant

import (
	"context"
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func nsCtx() context.Context {
	return genericapirequest.WithNamespace(context.Background(), "default")
}

func TestGenerationLifecycle(t *testing.T) {
	scheme := testScheme(t)
	strategy := NewStrategy(scheme)
	statusStrategy := NewStatusStrategy(scheme)

	grant := validQuotaGrant()
	grant.Generation = 7 // client-supplied values are ignored
	rest.FillObjectMetaSystemFields(grant)
	if err := rest.BeforeCreate(strategy, nsCtx(), grant); err != nil {
		t.Fatalf("BeforeCreate: %v", err)
	}
	if grant.Generation != 1 {
		t.Fatalf("generation after create = %d, want 1", grant.Generation)
	}
	grant.ResourceVersion = "1"

	// Spec change bumps.
	raised := grant.DeepCopy()
	raised.Spec.Allowances[0].Buckets[0].Amount = 1000
	if err := rest.BeforeUpdate(strategy, nsCtx(), raised, grant); err != nil {
		t.Fatalf("BeforeUpdate: %v", err)
	}
	if raised.Generation != 2 {
		t.Errorf("generation after spec change = %d, want 2", raised.Generation)
	}

	// Metadata-only change does not bump.
	labeled := raised.DeepCopy()
	labeled.Labels = map[string]string{"a": "b"}
	if err := rest.BeforeUpdate(strategy, nsCtx(), labeled, raised); err != nil {
		t.Fatalf("BeforeUpdate: %v", err)
	}
	if labeled.Generation != 2 {
		t.Errorf("generation after label change = %d, want 2", labeled.Generation)
	}

	// Status updates never bump, even when the payload carries a spec change
	// (it is discarded) or a client-supplied generation.
	status := labeled.DeepCopy()
	status.Generation = 99
	status.Spec.Allowances[0].Buckets[0].Amount = 5
	status.Status.ObservedGeneration = 2
	if err := rest.BeforeUpdate(statusStrategy, nsCtx(), status, labeled); err != nil {
		t.Fatalf("BeforeUpdate(status): %v", err)
	}
	if status.Generation != 2 {
		t.Errorf("generation after status update = %d, want 2", status.Generation)
	}
}

func TestConsumerRefNamespaceImmutable(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	old := validQuotaGrant()
	old.Spec.ConsumerRef.Namespace = "tenant-a"
	moved := old.DeepCopy()
	moved.Spec.ConsumerRef.Namespace = "tenant-b"
	if errs := strategy.ValidateUpdate(context.Background(), moved, old); len(errs) == 0 {
		t.Fatal("changing consumerRef.namespace must be rejected")
	}
	same := old.DeepCopy()
	if errs := strategy.ValidateUpdate(context.Background(), same, old); len(errs) != 0 {
		t.Fatalf("unchanged grant rejected: %v", errs)
	}
}

func TestAmountBounds(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	cases := []struct {
		amount int64
		ok     bool
	}{
		{0, true},
		{quota.MaxAmount, true},
		{quota.MaxAmount + 1, false},
		{-1, false},
	}
	for _, tc := range cases {
		g := validQuotaGrant()
		g.Spec.Allowances[0].Buckets[0].Amount = tc.amount
		errs := strategy.Validate(context.Background(), g)
		if (len(errs) == 0) != tc.ok {
			t.Errorf("amount %d: errs=%v, want ok=%v", tc.amount, errs, tc.ok)
		}
	}
}

func TestValidationBounds(t *testing.T) {
	strategy := NewStrategy(testScheme(t))
	mutate := map[string]func(*quota.QuotaGrant){
		"invalid name":           func(g *quota.QuotaGrant) { g.Name = "Not_Valid" },
		"bad consumer kind":      func(g *quota.QuotaGrant) { g.Spec.ConsumerRef.Kind = "lower" },
		"bad consumer apiGroup":  func(g *quota.QuotaGrant) { g.Spec.ConsumerRef.APIGroup = "Bad_Group" },
		"bad consumer namespace": func(g *quota.QuotaGrant) { g.Spec.ConsumerRef.Namespace = "Bad.NS" },
		"long resourceType": func(g *quota.QuotaGrant) {
			g.Spec.Allowances[0].ResourceType = string(make([]byte, 254))
		},
		"too many allowances": func(g *quota.QuotaGrant) {
			for range registryutil.MaxAllowances {
				g.Spec.Allowances = append(g.Spec.Allowances, g.Spec.Allowances[0])
			}
		},
		"too many buckets": func(g *quota.QuotaGrant) {
			for range registryutil.MaxBucketsPerAllowance {
				g.Spec.Allowances[0].Buckets = append(g.Spec.Allowances[0].Buckets, quota.Bucket{Amount: 1})
			}
		},
	}
	for name, fn := range mutate {
		g := validQuotaGrant()
		fn(g)
		if errs := strategy.Validate(context.Background(), g); len(errs) == 0 {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestSelectableFieldsMatchFieldLabels(t *testing.T) {
	got := registryutil.SpecFieldKeys(SelectableFields(validQuotaGrant()))
	want := slices.Sorted(slices.Values(v1alpha1.SelectableFieldLabels["QuotaGrant"]))
	if !slices.Equal(got, want) {
		t.Errorf("SelectableFields keys = %v, field labels = %v", got, want)
	}
}

// recordingStorage captures the ignoreNotFound flag the store passes down,
// which is true exactly when an update may create the object.
type recordingStorage struct {
	storage.Interface
	ignoreNotFound *bool
}

var errStop = errors.New("stop")

func (r recordingStorage) GuaranteedUpdate(_ context.Context, _ string, _ runtime.Object, ignoreNotFound bool, _ *storage.Preconditions, _ storage.UpdateFunc, _ runtime.Object) error {
	*r.ignoreNotFound = ignoreNotFound
	return errStop
}

func TestStatusUpdateNeverCreates(t *testing.T) {
	scheme := testScheme(t)
	var ignoreNotFound bool
	store := &genericregistry.Store{
		NewFunc:                  func() runtime.Object { return &quota.QuotaGrant{} },
		DefaultQualifiedResource: v1alpha1.Resource("quotagrants"),
		KeyFunc:                  func(context.Context, string) (string, error) { return "/k", nil },
		UpdateStrategy:           NewStatusStrategy(scheme),
		Storage:                  genericregistry.DryRunnableStorage{Storage: recordingStorage{ignoreNotFound: &ignoreNotFound}},
	}
	status := &QuotaGrantStatusStorage{store: store}
	_, _, err := status.Update(nsCtx(), "g", rest.DefaultUpdatedObjectInfo(validQuotaGrant()), nil, nil, true, &metav1.UpdateOptions{})
	if !errors.Is(err, errStop) {
		t.Fatalf("unexpected error: %v", err)
	}
	if ignoreNotFound {
		t.Error("status update passed forceAllowCreate through; a missing grant would be created")
	}
}
