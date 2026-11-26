package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotaapi "go.miloapis.com/quota/pkg/apis/quota"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func createdAt(c *quotav1alpha1.QuotaClaim, offset time.Duration) *quotav1alpha1.QuotaClaim {
	c.CreationTimestamp = metav1.NewTime(t0.Add(offset))
	return c
}

// appliedObject decodes an apply configuration the way the apiserver would
// receive it, for interceptors that inspect what a controller applied.
func appliedObject(ac runtime.ApplyConfiguration) *unstructured.Unstructured {
	data, err := json.Marshal(ac)
	if err != nil {
		panic(err)
	}
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(data); err != nil {
		panic(err)
	}
	return u
}

// recordDecisions intercepts per-claim allocation applies and records the
// decided status by claim name.
func recordDecisions(decisions map[string]string) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, ac runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			u := appliedObject(ac)
			if u.GetKind() != "QuotaClaim" {
				return nil
			}
			allocs, _, _ := unstructured.NestedSlice(u.Object, "status", "allocations")
			for _, a := range allocs {
				decisions[u.GetName()] = a.(map[string]interface{})["status"].(string)
			}
			return nil
		},
	}
}

// TestBucketAllocation_StaleCacheDoesNotRevokeGrant reproduces the review
// scenario: limit 1, reconcile 1 granted b-first and wrote allocated=1, and
// reconcile 2 runs before b-first's Granted status reaches the cache. The
// controller used to grant a-new from the stale view and re-decide b-first
// as Denied. It must read claims from the apiserver before deciding, deny
// a-new, and leave b-first alone.
func TestBucketAllocation_StaleCacheDoesNotRevokeGrant(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 1, 1)
	bFirst := createdAt(grantedClaim("b-first", ref, 1), 0)
	aNew := createdAt(pendingClaim("a-new", ref, 1), time.Second)

	decisions := map[string]string{}
	server := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithObjects(bucket, bFirst, aNew).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(recordDecisions(decisions)).
		Build()

	// The cache has not observed b-first's grant yet.
	staleCache := interceptor.NewClient(server, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if claims, ok := list.(*quotav1alpha1.QuotaClaimList); ok {
				for i := range claims.Items {
					claims.Items[i].Status.Allocations = nil
				}
			}
			return nil
		},
	})

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: staleCache, reader: server}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := decisions["b-first"]; got != "" {
		t.Errorf("b-first was already granted and must not be re-decided, got %q", got)
	}
	if got := decisions["a-new"]; got != quotav1alpha1.QuotaClaimAllocationStatusDenied {
		t.Errorf("a-new decision = %q, want Denied (limit 1 already used by b-first)", got)
	}
	var got quotav1alpha1.QuotaBucket
	if err := server.Get(context.Background(), client.ObjectKeyFromObject(bucket), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Allocated != 1 || got.Status.ClaimCount != 1 || got.Status.Available != 0 {
		t.Errorf("bucket = allocated %d claimCount %d available %d, want 1/1/0",
			got.Status.Allocated, got.Status.ClaimCount, got.Status.Available)
	}
}

// TestBucketAllocation_GrantsInCreationOrder verifies the oldest pending
// claim wins contended capacity regardless of list order.
func TestBucketAllocation_GrantsInCreationOrder(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 1, 0)
	decisions := map[string]string{}
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithObjects(bucket,
			createdAt(pendingClaim("a-new", ref, 1), time.Second),
			createdAt(pendingClaim("b-first", ref, 1), 0),
		).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(recordDecisions(decisions)).
		Build()

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if decisions["b-first"] != quotav1alpha1.QuotaClaimAllocationStatusGranted ||
		decisions["a-new"] != quotav1alpha1.QuotaClaimAllocationStatusDenied {
		t.Errorf("decisions = %v, want b-first Granted and a-new Denied", decisions)
	}
}

// TestBucketAllocation_IsolatesConsumersByAPIGroupAndNamespace verifies
// that claims of a same-named consumer in another API group or namespace
// neither draw on nor count against the bucket.
func TestBucketAllocation_IsolatesConsumersByAPIGroupAndNamespace(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	otherGroup := ref
	otherGroup.APIGroup = "other.example.com"
	otherNS := ref
	otherNS.Namespace = "tenant-b"

	bucket := testBucket(ref, 10, 0)
	decisions := map[string]string{}
	cli := claimIndexes(fake.NewClientBuilder().WithScheme(scheme)).
		WithObjects(bucket,
			grantedClaim("other-group-granted", otherGroup, 9),
			grantedClaim("other-ns-granted", otherNS, 9),
			pendingClaim("other-group-pending", otherGroup, 1),
			pendingClaim("mine", ref, 10),
		).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(recordDecisions(decisions)).
		Build()

	r := &QuotaBucketAllocationController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(decisions) != 1 || decisions["mine"] != quotav1alpha1.QuotaClaimAllocationStatusGranted {
		t.Errorf("decisions = %v, want only mine Granted", decisions)
	}

	if generateQuotaBucketName(testResourceType, ref) == generateQuotaBucketName(testResourceType, otherGroup) ||
		generateQuotaBucketName(testResourceType, ref) == generateQuotaBucketName(testResourceType, otherNS) {
		t.Error("consumers differing in apiGroup or namespace share a bucket name")
	}
	if ConsumerIndexKey(ref) == ConsumerIndexKey(otherGroup) || ConsumerIndexKey(ref) == ConsumerIndexKey(otherNS) {
		t.Error("consumers differing in apiGroup or namespace share an index key")
	}
}

func TestGenerateQuotaBucketNameMatchesSharedHelper(t *testing.T) {
	ref := quotav1alpha1.ConsumerRef{APIGroup: "g.example.com", Kind: "Tenant", Name: "n", Namespace: "ns"}
	if got, want := generateQuotaBucketName("r", ref), quotaapi.BucketName("r", "g.example.com", "Tenant", "ns", "n"); got != want {
		t.Errorf("controller bucket name %q != shared helper %q", got, want)
	}
}

func activeGrant(name string, ref quotav1alpha1.ConsumerRef, amounts ...int64) *quotav1alpha1.QuotaGrant {
	var buckets []quotav1alpha1.Bucket
	for _, a := range amounts {
		buckets = append(buckets, quotav1alpha1.Bucket{Amount: a})
	}
	return &quotav1alpha1.QuotaGrant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: quotav1alpha1.QuotaGrantSpec{
			ConsumerRef: ref,
			Allowances:  []quotav1alpha1.Allowance{{ResourceType: testResourceType, Buckets: buckets}},
		},
		Status: quotav1alpha1.QuotaGrantStatus{Conditions: []metav1.Condition{{
			Type: quotav1alpha1.QuotaGrantActive, Status: metav1.ConditionTrue,
			Reason: quotav1alpha1.QuotaGrantActiveReason, LastTransitionTime: metav1.NewTime(t0),
		}}},
	}
}

// applyBucketStatus emulates the apiserver for the bucket controller's
// status apply: it enforces the resourceVersion precondition and copies the
// asserted grant-derived fields onto the stored bucket. patches counts the
// applies it accepted.
func applyBucketStatus(patches *int) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, sub string, ac runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			u := appliedObject(ac)
			if u.GetKind() != "QuotaBucket" {
				return c.SubResource(sub).Apply(ctx, ac, opts...)
			}
			var stored quotav1alpha1.QuotaBucket
			if err := c.Get(ctx, client.ObjectKeyFromObject(u), &stored); err != nil {
				return err
			}
			if rv := u.GetResourceVersion(); rv != "" && rv != stored.ResourceVersion {
				return apierrors.NewConflict(schema.GroupResource{Group: quotav1alpha1.GroupName, Resource: "quotabuckets"}, u.GetName(), fmt.Errorf("stale"))
			}
			limit, _, _ := unstructured.NestedInt64(u.Object, "status", "limit")
			available, _, _ := unstructured.NestedInt64(u.Object, "status", "available")
			grantCount, _, _ := unstructured.NestedInt64(u.Object, "status", "grantCount")
			observed, _, _ := unstructured.NestedInt64(u.Object, "status", "observedGeneration")
			stored.Status.Limit = limit
			stored.Status.Available = available
			stored.Status.GrantCount = int32(grantCount)
			stored.Status.ObservedGeneration = observed
			refs, _, _ := unstructured.NestedSlice(u.Object, "status", "contributingGrantRefs")
			stored.Status.ContributingGrantRefs = nil
			for _, raw := range refs {
				m := raw.(map[string]interface{})
				stored.Status.ContributingGrantRefs = append(stored.Status.ContributingGrantRefs, quotav1alpha1.ContributingGrantRef{
					Name:                   m["name"].(string),
					LastObservedGeneration: m["lastObservedGeneration"].(int64),
					Amount:                 m["amount"].(int64),
				})
			}
			*patches++
			return c.Status().Update(ctx, &stored)
		},
	}
}

func getBucket(t *testing.T, c client.Client, b *quotav1alpha1.QuotaBucket) quotav1alpha1.QuotaBucket {
	t.Helper()
	var got quotav1alpha1.QuotaBucket
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(b), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestBucketLimit_FollowsGrantChanges raises and lowers a grant and checks
// that the watch predicate fires and the reconcile recomputes limit and
// available each time.
func TestBucketLimit_FollowsGrantChanges(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 0, 5)
	bucket.Status.Available = 0
	bucket.Generation = 1
	grant := activeGrant("g", ref, 10)

	patches := 0
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bucket, grant).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaGrant{}).
		WithInterceptorFuncs(applyBucketStatus(&patches)).
		Build()
	r := &QuotaBucketController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	ctx := context.Background()
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, bucketRequest(bucket)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	setAmount := func(amount int64) {
		t.Helper()
		var cur quotav1alpha1.QuotaGrant
		if err := cli.Get(ctx, client.ObjectKeyFromObject(grant), &cur); err != nil {
			t.Fatal(err)
		}
		old := cur.DeepCopy()
		cur.Spec.Allowances[0].Buckets[0].Amount = amount
		cur.Generation++ // what the grant strategy does on a spec change
		if err := cli.Update(ctx, &cur); err != nil {
			t.Fatal(err)
		}
		if !grantChangePredicate().Update(event.UpdateEvent{ObjectOld: old, ObjectNew: &cur}) {
			t.Fatalf("grant amount change %d -> %d filtered out by the watch predicate",
				old.Spec.Allowances[0].Buckets[0].Amount, amount)
		}
	}

	reconcile()
	if got := getBucket(t, cli, bucket); got.Status.Limit != 10 || got.Status.Available != 5 {
		t.Fatalf("initial: limit %d available %d, want 10/5", got.Status.Limit, got.Status.Available)
	}

	setAmount(100)
	reconcile()
	if got := getBucket(t, cli, bucket); got.Status.Limit != 100 || got.Status.Available != 95 {
		t.Errorf("after raise: limit %d available %d, want 100/95", got.Status.Limit, got.Status.Available)
	}

	setAmount(2)
	reconcile()
	if got := getBucket(t, cli, bucket); got.Status.Limit != 2 || got.Status.Available != 0 || got.Status.Allocated != 5 {
		t.Errorf("after lower: limit %d available %d allocated %d, want 2/0/5 (no negative available)",
			got.Status.Limit, got.Status.Available, got.Status.Allocated)
	}
	if patches != 3 {
		t.Errorf("status applies = %d, want 3", patches)
	}
	reconcile()
	if patches != 3 {
		t.Errorf("an unchanged reconcile wrote status again (%d applies)", patches)
	}
}

// TestBucketLimit_ConflictRequeues verifies the limit apply carries the
// read resourceVersion and a lost race requeues instead of writing an
// available computed from a stale allocated.
func TestBucketLimit_ConflictRequeues(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	bucket := testBucket(ref, 0, 0)
	patches := 0
	base := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bucket, activeGrant("g", ref, 10)).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaGrant{}).
		WithInterceptorFuncs(applyBucketStatus(&patches)).
		Build()
	// An allocator bumps the bucket between the controller's read and write.
	racing := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if b, ok := obj.(*quotav1alpha1.QuotaBucket); ok {
				winner := b.DeepCopy()
				winner.Status.Allocated = 3
				return c.Status().Update(ctx, winner)
			}
			return nil
		},
	})
	r := &QuotaBucketController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: racing}}}
	res, err := r.Reconcile(context.Background(), bucketRequest(bucket))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 || patches != 0 {
		t.Errorf("result %+v with %d applies; want a requeue and no write", res, patches)
	}
}

func TestBucketLimit_SaturatesAndIsolatesConsumers(t *testing.T) {
	scheme := testScheme(t)
	ref := consumer("acme")
	otherGroup := ref
	otherGroup.APIGroup = "other.example.com"
	otherNS := ref
	otherNS.Namespace = "tenant-b"
	bucket := testBucket(ref, 0, 0)
	patches := 0
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bucket,
			activeGrant("huge", ref, math.MaxInt64, 1),
			activeGrant("other-group", otherGroup, 7),
			activeGrant("other-ns", otherNS, 7),
		).
		WithStatusSubresource(&quotav1alpha1.QuotaBucket{}, &quotav1alpha1.QuotaGrant{}).
		WithInterceptorFuncs(applyBucketStatus(&patches)).
		Build()
	r := &QuotaBucketController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequest(bucket)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := getBucket(t, cli, bucket)
	if got.Status.Limit != math.MaxInt64 {
		t.Errorf("limit = %d, want saturated MaxInt64", got.Status.Limit)
	}
	if got.Status.GrantCount != 2 {
		t.Errorf("grantCount = %d, want 2 (only acme's own grant buckets)", got.Status.GrantCount)
	}
}

func TestGrantChangePredicate(t *testing.T) {
	ref := consumer("acme")
	base := activeGrant("g", ref, 10)
	p := grantChangePredicate()
	cases := []struct {
		name   string
		mutate func(g *quotav1alpha1.QuotaGrant)
		want   bool
	}{
		{"generation bump", func(g *quotav1alpha1.QuotaGrant) { g.Generation++ }, true},
		{"spec change", func(g *quotav1alpha1.QuotaGrant) { g.Spec.Allowances[0].Buckets[0].Amount = 1 }, true},
		{"deactivated", func(g *quotav1alpha1.QuotaGrant) { g.Status.Conditions[0].Status = metav1.ConditionFalse }, true},
		{"status noise", func(g *quotav1alpha1.QuotaGrant) { g.Status.ObservedGeneration = 9 }, false},
		{"label only", func(g *quotav1alpha1.QuotaGrant) { g.Labels = map[string]string{"x": "y"} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base.DeepCopy()
			tc.mutate(n)
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: n}); got != tc.want {
				t.Errorf("predicate = %v, want %v", got, tc.want)
			}
		})
	}
	if !p.Delete(event.DeleteEvent{Object: base}) || !p.Create(event.CreateEvent{Object: base}) {
		t.Error("create and delete must pass")
	}
}

// claimWithAllocations builds a claim with two requests and the given
// allocation statuses ("" means no entry yet).
func claimWithAllocations(statuses ...string) *quotav1alpha1.QuotaClaim {
	c := &quotav1alpha1.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default", Generation: 1},
		Spec:       quotav1alpha1.QuotaClaimSpec{ConsumerRef: consumer("acme")},
	}
	for i, s := range statuses {
		rt := fmt.Sprintf("example.com/r%d", i)
		c.Spec.Requests = append(c.Spec.Requests, quotav1alpha1.ResourceRequest{ResourceType: rt, Amount: 1})
		if s != "" {
			c.Status.Allocations = append(c.Status.Allocations, quotav1alpha1.QuotaClaimAllocationStatus{
				ResourceType: rt, Status: s, LastTransitionTime: metav1.NewTime(t0),
			})
		}
	}
	return c
}

func TestClaimController_OverallCondition(t *testing.T) {
	G, D, P := quotav1alpha1.QuotaClaimAllocationStatusGranted, quotav1alpha1.QuotaClaimAllocationStatusDenied, quotav1alpha1.QuotaClaimAllocationStatusPending
	cases := []struct {
		name       string
		claim      *quotav1alpha1.QuotaClaim
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{"all granted", claimWithAllocations(G, G), metav1.ConditionTrue, quotav1alpha1.QuotaClaimGrantedReason},
		{"one denied", claimWithAllocations(G, D), metav1.ConditionFalse, quotav1alpha1.QuotaClaimDeniedReason},
		{"denied beats pending", claimWithAllocations("", D), metav1.ConditionFalse, quotav1alpha1.QuotaClaimDeniedReason},
		{"partially decided", claimWithAllocations(G, ""), metav1.ConditionFalse, quotav1alpha1.QuotaClaimPendingReason},
		{"explicit pending", claimWithAllocations(P), metav1.ConditionFalse, quotav1alpha1.QuotaClaimPendingReason},
		{"nothing decided", claimWithAllocations("", ""), metav1.ConditionFalse, quotav1alpha1.QuotaClaimPendingReason},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runClaimController(t, tc.claim)
			if got == nil {
				t.Fatal("no status apply")
			}
			cond := apimeta.FindStatusCondition(got.Status.Conditions, quotav1alpha1.QuotaClaimGranted)
			if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Fatalf("condition = %+v, want %s/%s", cond, tc.wantStatus, tc.wantReason)
			}
			if cond.ObservedGeneration != 1 || got.Status.ObservedGeneration != 1 {
				t.Errorf("observedGeneration not propagated: %+v / %d", cond, got.Status.ObservedGeneration)
			}
		})
	}
}

func TestClaimController_NoWriteWhenUnchanged(t *testing.T) {
	claim := claimWithAllocations(quotav1alpha1.QuotaClaimAllocationStatusGranted)
	claim.Status.ObservedGeneration = 1
	claim.Status.Conditions = []metav1.Condition{{
		Type: quotav1alpha1.QuotaClaimGranted, Status: metav1.ConditionTrue,
		Reason: quotav1alpha1.QuotaClaimGrantedReason, Message: "All 1 resource requests have been granted",
		ObservedGeneration: 1, LastTransitionTime: metav1.NewTime(t0),
	}}
	if got := runClaimController(t, claim); got != nil {
		t.Errorf("unchanged claim was re-applied: %+v", got.Status)
	}

	// A message change with the same status keeps LastTransitionTime.
	claim.Status.Conditions[0].Message = "old message"
	got := runClaimController(t, claim)
	if got == nil {
		t.Fatal("expected an apply for the changed message")
	}
	if lt := got.Status.Conditions[0].LastTransitionTime; !lt.Equal(&claim.Status.Conditions[0].LastTransitionTime) {
		t.Errorf("LastTransitionTime moved from %v to %v without a status change", claim.Status.Conditions[0].LastTransitionTime, lt)
	}
}

// runClaimController reconciles claim and returns the status it applied,
// or nil when it wrote nothing.
func runClaimController(t *testing.T, claim *quotav1alpha1.QuotaClaim) *quotav1alpha1.QuotaClaim {
	t.Helper()
	scheme := testScheme(t)
	var applied *quotav1alpha1.QuotaClaim
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(claim).
		WithStatusSubresource(&quotav1alpha1.QuotaClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(_ context.Context, _ client.Client, _ string, ac runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
				u := appliedObject(ac)
				if u.GetKind() != "QuotaClaim" {
					return fmt.Errorf("unexpected apply object kind %q", u.GetKind())
				}
				c := &quotav1alpha1.QuotaClaim{}
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, c); err != nil {
					return err
				}
				applied = c
				return nil
			},
		}).
		Build()
	r := &QuotaClaimController{Scheme: scheme, Manager: &testManager{cluster: &testCluster{client: cli}}}
	if _, err := r.Reconcile(context.Background(), bucketRequestFor(claim)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return applied
}

func bucketRequestFor(obj client.Object) mcreconcile.Request {
	return mcreconcile.Request{Request: ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)}}
}
