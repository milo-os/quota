package core

import (
	"context"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics/testutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/metrics"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

const testResourceType = "example.com/widgets"

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := quotav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	return s
}

func consumer(name string) quotav1alpha1.ConsumerRef {
	return quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Tenant", Name: name}
}

func testBucket(ref quotav1alpha1.ConsumerRef, limit, allocated int64) *quotav1alpha1.QuotaBucket {
	return &quotav1alpha1.QuotaBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateQuotaBucketName(testResourceType, ref),
			Namespace: DefaultBucketNamespace,
		},
		Spec: quotav1alpha1.QuotaBucketSpec{ConsumerRef: ref, ResourceType: testResourceType},
		Status: quotav1alpha1.QuotaBucketStatus{
			Limit:     limit,
			Allocated: allocated,
			Available: limit - allocated,
		},
	}
}

func pendingClaim(name string, ref quotav1alpha1.ConsumerRef, amount int64) *quotav1alpha1.QuotaClaim {
	return &quotav1alpha1.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: quotav1alpha1.QuotaClaimSpec{
			ConsumerRef: ref,
			Requests:    []quotav1alpha1.ResourceRequest{{ResourceType: testResourceType, Amount: amount}},
		},
	}
}

func grantedClaim(name string, ref quotav1alpha1.ConsumerRef, amount int64) *quotav1alpha1.QuotaClaim {
	c := pendingClaim(name, ref, amount)
	c.Status.Allocations = []quotav1alpha1.QuotaClaimAllocationStatus{{
		ResourceType:    testResourceType,
		Status:          quotav1alpha1.QuotaClaimAllocationStatusGranted,
		Reason:          quotav1alpha1.QuotaClaimGrantedReason,
		AllocatedAmount: amount,
	}}
	return c
}

// claimIndexes registers the cache index the allocation controller lists by
// plus the server-side consumerRef field selectors its live re-read uses,
// so one fake can play both the cache and the apiserver.
func claimIndexes(b *fake.ClientBuilder) *fake.ClientBuilder {
	b = b.WithIndex(&quotav1alpha1.QuotaClaim{}, QuotaClaimConsumerIndex, QuotaClaimConsumerIndexFunc)
	for field, get := range map[string]func(quotav1alpha1.ConsumerRef) string{
		"spec.consumerRef.apiGroup":  func(r quotav1alpha1.ConsumerRef) string { return r.APIGroup },
		"spec.consumerRef.kind":      func(r quotav1alpha1.ConsumerRef) string { return r.Kind },
		"spec.consumerRef.name":      func(r quotav1alpha1.ConsumerRef) string { return r.Name },
		"spec.consumerRef.namespace": func(r quotav1alpha1.ConsumerRef) string { return r.Namespace },
	} {
		b = b.WithIndex(&quotav1alpha1.QuotaClaim{}, field, func(obj client.Object) []string {
			return []string{get(obj.(*quotav1alpha1.QuotaClaim).Spec.ConsumerRef)}
		})
	}
	return b
}

func bucketRequest(b *quotav1alpha1.QuotaBucket) mcreconcile.Request {
	return mcreconcile.Request{Request: ctrl.Request{NamespacedName: types.NamespacedName{Name: b.Name, Namespace: b.Namespace}}}
}

// TestBucketAllocation_ConflictMarksNoClaims verifies the over-allocation
// guard: when the version-checked bucket write loses a race, no claim is
// marked Granted and the reconcile requeues.
func TestBucketAllocation_ConflictMarksNoClaims(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 10, 0)
	claim := pendingClaim("c1", ref, 5)

	claimStatusWrites := 0
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithScheme(scheme).
		WithObjects(bucket, claim).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*quotav1alpha1.QuotaBucket); ok {
					return apierrors.NewConflict(schema.GroupResource{Group: quotav1alpha1.GroupName, Resource: "quotabuckets"}, obj.GetName(), nil)
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
			SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				claimStatusWrites++
				return nil
			},
		}).
		Build()

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	res, err := r.Reconcile(context.Background(), bucketRequest(bucket))
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue after conflict")
	}
	if claimStatusWrites != 0 {
		t.Errorf("claims marked after bucket conflict: %d writes", claimStatusWrites)
	}
}

// TestBucketAllocation_StaleBucketVersionConflicts verifies that the bucket
// write carries the resourceVersion that was read, so a writer working from
// a stale copy is rejected by the store.
func TestBucketAllocation_StaleBucketVersionConflicts(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 10, 0)
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithScheme(scheme).
		WithObjects(bucket, pendingClaim("c1", ref, 5)).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				t.Errorf("claim must not be marked when the bucket write is stale")
				return nil
			},
		}).
		Build()

	var stale quotav1alpha1.QuotaBucket
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(bucket), &stale); err != nil {
		t.Fatal(err)
	}
	// Another writer reserves capacity first.
	winner := stale.DeepCopy()
	winner.Status.Allocated = 8
	winner.Status.Available = 2
	if err := cli.Status().Update(context.Background(), winner); err != nil {
		t.Fatal(err)
	}

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	res, err := r.reconcileAllocations(context.Background(), cli, cli, &stale)
	if err != nil {
		t.Fatalf("reconcileAllocations returned error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected requeue after stale write")
	}
}

// TestBucketAllocation_UsesConsumerIndex verifies that only claims from the
// bucket's consumer are tallied and decided, and that the bucket status is
// written before claims are marked.
func TestBucketAllocation_UsesConsumerIndex(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 10, 0)

	var order []string
	patched := map[string]bool{}
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithScheme(scheme).
		WithObjects(bucket,
			grantedClaim("existing", ref, 4),
			pendingClaim("fits", ref, 5),
			pendingClaim("too-big", ref, 5),
			grantedClaim("other-consumer", consumer("globex"), 100),
		).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				order = append(order, "bucket")
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
			SubResourceApply: func(_ context.Context, _ client.Client, _ string, ac runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
				order = append(order, "claim")
				patched[appliedObject(ac).GetName()] = true
				return nil
			},
		}).
		Build()

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var got quotav1alpha1.QuotaBucket
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(bucket), &got); err != nil {
		t.Fatal(err)
	}
	// existing(4) + one of the two pending 5s; the other-consumer claim is ignored.
	if got.Status.Allocated != 9 || got.Status.Available != 1 || got.Status.ClaimCount != 2 {
		t.Errorf("bucket status = allocated %d available %d claimCount %d, want 9/1/2",
			got.Status.Allocated, got.Status.Available, got.Status.ClaimCount)
	}
	if got.Status.Limit != 10 {
		t.Errorf("grant-derived limit changed: %d", got.Status.Limit)
	}
	if patched["other-consumer"] || patched["existing"] {
		t.Errorf("unexpected claim patches: %v", patched)
	}
	if !patched["fits"] || !patched["too-big"] {
		t.Errorf("both pending claims should get a decision: %v", patched)
	}
	if len(order) == 0 || order[0] != "bucket" {
		t.Errorf("bucket must be written before claims, got order %v", order)
	}
}

func TestGetBucketNamespace(t *testing.T) {
	cases := []struct {
		name     string
		ref      quotav1alpha1.ConsumerRef
		fallback string
		want     string
	}{
		{"namespaced consumer wins", quotav1alpha1.ConsumerRef{Kind: "T", Name: "a", Namespace: "tenant-a"}, "custom", "tenant-a"},
		{"configured fallback", quotav1alpha1.ConsumerRef{Kind: "T", Name: "a"}, "custom", "custom"},
		{"default fallback", quotav1alpha1.ConsumerRef{Kind: "T", Name: "a"}, "", DefaultBucketNamespace},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := getBucketNamespace(tc.ref, tc.fallback); got != tc.want {
				t.Errorf("getBucketNamespace() = %q, want %q", got, tc.want)
			}
		})
	}
}

// A claim decision that fails to land counts as result="error", which the
// QuotaClaimErrors alert watches.
func TestBucketAllocation_ApplyFailureCountsError(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 10, 0)
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithScheme(scheme).
		WithObjects(bucket, pendingClaim("c1", ref, 5)).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				return apierrors.NewInternalError(fmt.Errorf("etcd unavailable"))
			},
		}).
		Build()

	before, err := testutil.GetCounterMetricValue(metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError))
	if err != nil {
		t.Fatal(err)
	}
	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err == nil {
		t.Fatal("Reconcile succeeded, want the apply error")
	}
	after, err := testutil.GetCounterMetricValue(metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError))
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 1 {
		t.Fatalf("quota_claim_total{result=error} rose by %v, want 1", after-before)
	}
}
