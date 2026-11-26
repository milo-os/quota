package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	pgwatch "go.miloapis.com/quota/internal/watch"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/install"
)

// allocatorTestCodec is a quota-aware codec built locally for the allocator
// tests. The store_test.go test codec only knows testKind; the allocator
// decodes real QuotaBucket/QuotaClaim internal types, so it needs a codec
// whose scheme has the quota API group installed.
var allocatorTestCodec runtime.Codec

func init() {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	codecs := serializer.NewCodecFactory(scheme)
	allocatorTestCodec = codecs.LegacyCodec(schema.GroupVersion{
		Group:   quotainternal.GroupName,
		Version: "v1alpha1",
	})
}

// seedBucket inserts a QuotaBucket row into quota_objects and returns the
// key and the sequence-assigned resource version so tests can assert CAS
// behavior against known state.
func seedBucket(t testing.TB, db *sql.DB, consumer quotainternal.ConsumerRef, resourceType string, limit int64) (key string, rv int64) {
	t.Helper()
	bucketName := bucketNameFor(resourceType, consumer)
	bucketNS := NewAllocator(nil, allocatorTestCodec).bucketNamespaceFor(consumer)
	key = bucketKey(bucketNS, bucketName)

	bucket := &quotainternal.QuotaBucket{
		TypeMeta: metav1.TypeMeta{
			APIVersion: quotainternal.SchemeGroupVersion.String(),
			Kind:       bucketsKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: bucketNS,
		},
		Spec: quotainternal.QuotaBucketSpec{
			ConsumerRef:  consumer,
			ResourceType: resourceType,
		},
		Status: quotainternal.QuotaBucketStatus{
			Limit:     limit,
			Allocated: 0,
			Available: limit,
		},
	}

	// Draw the RV the way every writer must.
	if err := db.QueryRow(`SELECT quota_next_rv()`).Scan(&rv); err != nil {
		t.Fatalf("quota_next_rv: %v", err)
	}
	bucket.ResourceVersion = fmt.Sprintf("%d", rv)

	data, err := runtime.Encode(allocatorTestCodec, bucket)
	if err != nil {
		t.Fatalf("encode bucket: %v", err)
	}

	_, err = db.Exec(
		`INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())`,
		key, rv, bucketsKind, bucketNS, bucketName, data,
	)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	return key, rv
}

// TestPostgresAllocator_Allocate_SerializedSameBucket verifies that two
// concurrent Allocate calls targeting the same bucket serialize on the
// row-level lock (SELECT ... FOR UPDATE) and both succeed with consistent
// accounting. This is the replacement for the old CAS-retry test: there
// is no retry path anymore — the second writer blocks on the first's row
// lock, wakes up after the first commits, re-reads the bucket under the
// now-current state, and commits without conflict.
func TestPostgresAllocator_Allocate_SerializedSameBucket(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-serialize",
		Namespace: "tenant-serialize",
	}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)

	const workers = 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			claim := &quotainternal.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("claim-serialize-%d", idx),
					Namespace: consumer.Namespace,
				},
				Spec: quotainternal.QuotaClaimSpec{
					ConsumerRef: consumer,
					Requests: []quotainternal.ResourceRequest{
						{ResourceType: "cpu", Amount: 5},
					},
				},
			}
			out, err := alloc.Allocate(ctx, claim)
			if err != nil {
				errs <- fmt.Errorf("worker %d: %w", idx, err)
				return
			}
			if len(out.Status.Allocations) != 1 ||
				out.Status.Allocations[0].Status != quotainternal.QuotaClaimAllocationStatusGranted {
				errs <- fmt.Errorf("worker %d: expected granted, got %+v", idx, out.Status.Allocations)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent same-bucket Allocate failed: %v", err)
	}

	// Every grant reserved 5, so the bucket must show exactly
	// workers*5 allocated and workers ClaimCount. Any mismatch is a
	// smoking gun for a lost update.
	b := readBucket(t, db, key)
	if got, want := b.Status.Allocated, int64(workers*5); got != want {
		t.Errorf("serialized bucket allocated=%d, want %d", got, want)
	}
	if got := b.Status.ClaimCount; got != int32(workers) {
		t.Errorf("serialized bucket claimCount=%d, want %d", got, workers)
	}
}

// TestPostgresAllocator_Allocate_NoDeadlockAcrossOverlappingBuckets verifies
// the lock-ordering invariant in commitAllocation / commitRelease: two
// concurrent claims that touch overlapping bucket sets acquire their
// SELECT ... FOR UPDATE locks in the same (sorted) order, so Postgres
// queues rather than deadlocks.
//
// The test seeds two buckets "cpu" and "memory" and runs two workers. Both
// workers request the same two resource types but declare them in the
// opposite order in Spec.Requests. If the allocator took locks in request
// order (A then B vs B then A), the two workers would deadlock immediately;
// by sorting bucket keys before locking they both queue on the same
// first-key lock.
func TestPostgresAllocator_Allocate_NoDeadlockAcrossOverlappingBuckets(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-order",
		Namespace: "tenant-order",
	}
	cpuKey, _ := seedBucket(t, db, consumer, "cpu", 100)
	memKey, _ := seedBucket(t, db, consumer, "memory", 200)

	makeClaim := func(name string, first, second string) *quotainternal.QuotaClaim {
		return &quotainternal.QuotaClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: consumer.Namespace},
			Spec: quotainternal.QuotaClaimSpec{
				ConsumerRef: consumer,
				Requests: []quotainternal.ResourceRequest{
					{ResourceType: first, Amount: 10},
					{ResourceType: second, Amount: 20},
				},
			},
		}
	}

	done := make(chan error, 2)
	start := time.Now()
	go func() {
		_, err := alloc.Allocate(ctx, makeClaim("claim-order-a", "cpu", "memory"))
		done <- err
	}()
	go func() {
		_, err := alloc.Allocate(ctx, makeClaim("claim-order-b", "memory", "cpu"))
		done <- err
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("overlapping-bucket allocate failed: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("overlapping-bucket allocate deadlocked (elapsed %s)", time.Since(start))
		}
	}

	// Each worker requests (first=10, second=20) against a different
	// resource-type ordering, so across both workers each bucket receives
	// one 10 and one 20 = 30.
	if got := readBucket(t, db, cpuKey).Status.Allocated; got != 30 {
		t.Errorf("cpu allocated=%d, want 30", got)
	}
	if got := readBucket(t, db, memKey).Status.Allocated; got != 30 {
		t.Errorf("memory allocated=%d, want 30", got)
	}
}

// TestPostgresAllocator_Allocate_Deny verifies the deny path: when the
// requested amount exceeds bucket capacity, the claim is persisted with a
// Denied allocation status and the bucket is NOT mutated.
func TestPostgresAllocator_Allocate_Deny(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-deny",
		Namespace: "tenant-deny",
	}
	key, seededRV := seedBucket(t, db, consumer, "cpu", 5)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-deny",
			Namespace: "tenant-deny",
		},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 100},
			},
		},
	}

	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		if apierrors.IsForbidden(err) || apierrors.IsConflict(err) {
			t.Fatalf("expected successful return with Denied allocation, got error: %v", err)
		}
		t.Fatalf("Allocate: %v", err)
	}
	if len(out.Status.Allocations) != 1 {
		t.Fatalf("expected 1 allocation, got %d", len(out.Status.Allocations))
	}
	if out.Status.Allocations[0].Status != quotainternal.QuotaClaimAllocationStatusDenied {
		t.Errorf("expected denied allocation, got %+v", out.Status.Allocations[0])
	}

	// Bucket must be untouched: resource_version unchanged, allocated=0.
	var data []byte
	var finalRV int64
	if err := db.QueryRow(
		`SELECT data, resource_version FROM quota_objects WHERE key = $1`, key,
	).Scan(&data, &finalRV); err != nil {
		t.Fatalf("read bucket: %v", err)
	}
	if finalRV != seededRV {
		t.Errorf("bucket resource_version changed on deny: seeded=%d final=%d", seededRV, finalRV)
	}
	obj, err := decodeToObject(allocatorTestCodec, data)
	if err != nil {
		t.Fatalf("decode bucket: %v", err)
	}
	bucket := obj.(*quotainternal.QuotaBucket)
	if bucket.Status.Allocated != 0 {
		t.Errorf("expected allocated=0 on deny, got %d", bucket.Status.Allocated)
	}
}

// readBucket is a small helper that fetches a bucket row by key and decodes
// it so tests can assert on its Status fields.
func readBucket(t *testing.T, db *sql.DB, key string) *quotainternal.QuotaBucket {
	t.Helper()
	var data []byte
	var rv int64
	if err := db.QueryRow(
		`SELECT data, resource_version FROM quota_objects WHERE key = $1`, key,
	).Scan(&data, &rv); err != nil {
		t.Fatalf("read bucket %q: %v", key, err)
	}
	obj, err := decodeToObject(allocatorTestCodec, data)
	if err != nil {
		t.Fatalf("decode bucket %q: %v", key, err)
	}
	return obj.(*quotainternal.QuotaBucket)
}

// TestPostgresAllocator_Release_Idempotent verifies that calling Release twice
// on the same claim does not double-decrement any bucket. This is the
// idempotency guarantee that makes Release safe to retry on apiserver crash,
// 5xx responses, and kubectl delete retries.
func TestPostgresAllocator_Release_Idempotent(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-idem",
		Namespace: "tenant-idem",
	}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-idem", Namespace: "tenant-idem"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 25},
			},
		},
	}

	granted, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 25 {
		t.Fatalf("expected allocated=25 after Allocate, got %d", got)
	}

	// First Release: must decrement the bucket and delete the claim.
	if err := alloc.Release(ctx, granted); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 0 {
		t.Fatalf("expected allocated=0 after first Release, got %d", got)
	}

	// Second Release on the same claim object: claim row is already gone,
	// must be a no-op and must NOT touch the bucket again.
	if err := alloc.Release(ctx, granted); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 0 {
		t.Errorf("second Release double-decremented bucket: allocated=%d, want 0", got)
	}

	// Stronger variant: create a SECOND, unrelated claim against the same
	// bucket, then retry Release on the first claim. The unrelated claim's
	// reservation must remain intact — a buggy double-decrement would drive
	// allocated negative and (with the old clamp) destroy the second
	// claim's accounting.
	claim2 := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-idem-2", Namespace: "tenant-idem"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 40},
			},
		},
	}
	if _, err := alloc.Allocate(ctx, claim2); err != nil {
		t.Fatalf("Allocate claim2: %v", err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 40 {
		t.Fatalf("expected allocated=40 after claim2, got %d", got)
	}
	if err := alloc.Release(ctx, granted); err != nil {
		t.Fatalf("retry Release on already-released claim: %v", err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 40 {
		t.Errorf("retry Release corrupted unrelated claim's accounting: allocated=%d, want 40", got)
	}
}

// TestPostgresAllocator_Release_MultiBucketAtomic verifies that a claim which
// allocated against multiple buckets has all its decrements applied in a
// single transaction.
func TestPostgresAllocator_Release_MultiBucketAtomic(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-multi",
		Namespace: "tenant-multi",
	}
	cpuKey, _ := seedBucket(t, db, consumer, "cpu", 100)
	memKey, _ := seedBucket(t, db, consumer, "memory", 200)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-multi", Namespace: "tenant-multi"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 15},
				{ResourceType: "memory", Amount: 50},
			},
		},
	}
	granted, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if got := readBucket(t, db, cpuKey).Status.Allocated; got != 15 {
		t.Fatalf("cpu allocated=%d after Allocate, want 15", got)
	}
	if got := readBucket(t, db, memKey).Status.Allocated; got != 50 {
		t.Fatalf("memory allocated=%d after Allocate, want 50", got)
	}

	if err := alloc.Release(ctx, granted); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := readBucket(t, db, cpuKey).Status.Allocated; got != 0 {
		t.Errorf("cpu allocated=%d after Release, want 0", got)
	}
	if got := readBucket(t, db, memKey).Status.Allocated; got != 0 {
		t.Errorf("memory allocated=%d after Release, want 0", got)
	}

	// Claim row must be gone.
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM quota_objects WHERE key = $1`,
		claimKey(claim.Namespace, claim.Name),
	).Scan(&count); err != nil {
		t.Fatalf("count claim: %v", err)
	}
	if count != 0 {
		t.Errorf("expected claim row deleted, got count=%d", count)
	}
}

// TestPostgresAllocator_Allocate_DuplicateResourceType verifies that a claim
// with two Spec.Requests entries for the same ResourceType aggregates into a
// single bucket update (one CAS, no thrash) and still produces one
// per-request entry in Status.Allocations.
func TestPostgresAllocator_Allocate_DuplicateResourceType(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-dup",
		Namespace: "tenant-dup",
	}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-dup", Namespace: "tenant-dup"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 7},
				{ResourceType: "cpu", Amount: 13},
			},
		},
	}

	// Planner should aggregate to a single plannedBucket with amount=20.
	planned := alloc.planAllocation(claim)
	if len(planned) != 1 {
		t.Fatalf("expected 1 aggregated bucket plan, got %d", len(planned))
	}
	if planned[0].amount != 20 {
		t.Errorf("expected aggregated amount=20, got %d", planned[0].amount)
	}

	// End-to-end: full Allocate should commit the aggregated bucket
	// decrement and produce two per-request allocation statuses.
	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if len(out.Status.Allocations) != 2 {
		t.Errorf("expected 2 allocation statuses, got %d", len(out.Status.Allocations))
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 20 {
		t.Errorf("expected bucket allocated=20, got %d", got)
	}
	if got := readBucket(t, db, key).Status.ClaimCount; got != 1 {
		t.Errorf("expected bucket ClaimCount=1 (one claim, not two), got %d", got)
	}
}

// TestPostgresAllocator_Allocate_DuplicateResourceType_Deny verifies that
// when the SUM of duplicate requests exceeds capacity the whole claim is
// denied, matching the atomic multi-request claim semantic.
func TestPostgresAllocator_Allocate_DuplicateResourceType_Deny(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	consumer := quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      "proj-dup-deny",
		Namespace: "tenant-dup-deny",
	}
	key, seededRV := seedBucket(t, db, consumer, "cpu", 10)

	// Each request alone fits (6 <= 10), but the sum (12) does not.
	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-dup-deny", Namespace: "tenant-dup-deny"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 6},
				{ResourceType: "cpu", Amount: 6},
			},
		},
	}
	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if len(out.Status.Allocations) != 2 {
		t.Fatalf("expected 2 allocation statuses, got %d", len(out.Status.Allocations))
	}
	for i, a := range out.Status.Allocations {
		if a.Status != quotainternal.QuotaClaimAllocationStatusDenied {
			t.Errorf("allocation[%d] status=%q, want Denied", i, a.Status)
		}
	}
	// Bucket must be untouched.
	if got := readBucket(t, db, key).Status.Allocated; got != 0 {
		t.Errorf("bucket mutated on full deny: allocated=%d", got)
	}
	var finalRV int64
	if err := db.QueryRow(
		`SELECT resource_version FROM quota_objects WHERE key = $1`, key,
	).Scan(&finalRV); err != nil {
		t.Fatalf("read rv: %v", err)
	}
	if finalRV != seededRV {
		t.Errorf("bucket rv changed on deny: seeded=%d final=%d", seededRV, finalRV)
	}
}

// TestPostgresAllocator_ConcurrentDifferentBuckets verifies that the advisory
// lock is gone and two writers targeting different buckets commit in
// parallel without serializing. Before option C landed, every writer took
// pg_advisory_xact_lock(0x717561746150475F) on a fixed key and commits were
// globally serialized; this test would have blocked any two concurrent
// commits onto a single timeline.
//
// We seed N distinct buckets, run N goroutines each performing an Allocate
// against its own bucket, and require every Allocate to return Granted. A
// regression that reintroduces a global lock would still pass on
// correctness but the wall-clock runtime would scale linearly with N; we
// don't assert a hard timing bound (tests run in diverse environments) but
// do record a log line with the elapsed time for manual inspection.
func TestPostgresAllocator_ConcurrentDifferentBuckets(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	const workers = 8
	keys := make([]string, workers)
	consumers := make([]quotainternal.ConsumerRef, workers)
	for i := 0; i < workers; i++ {
		consumers[i] = quotainternal.ConsumerRef{
			APIGroup:  "example.com",
			Kind:      "Project",
			Name:      fmt.Sprintf("proj-par-%d", i),
			Namespace: fmt.Sprintf("tenant-par-%d", i),
		}
		keys[i], _ = seedBucket(t, db, consumers[i], "cpu", 1000)
	}

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			claim := &quotainternal.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("claim-par-%d", idx),
					Namespace: consumers[idx].Namespace,
				},
				Spec: quotainternal.QuotaClaimSpec{
					ConsumerRef: consumers[idx],
					Requests: []quotainternal.ResourceRequest{
						{ResourceType: "cpu", Amount: 10},
					},
				},
			}
			out, err := alloc.Allocate(ctx, claim)
			if err != nil {
				errs <- fmt.Errorf("worker %d: %w", idx, err)
				return
			}
			if len(out.Status.Allocations) != 1 ||
				out.Status.Allocations[0].Status != quotainternal.QuotaClaimAllocationStatusGranted {
				errs <- fmt.Errorf("worker %d: expected granted, got %+v", idx, out.Status.Allocations)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	elapsed := time.Since(start)
	for err := range errs {
		t.Errorf("concurrent allocate failed: %v", err)
	}

	// Every bucket should show exactly one claim of 10.
	for i, k := range keys {
		b := readBucket(t, db, k)
		if b.Status.Allocated != 10 {
			t.Errorf("bucket[%d] allocated=%d, want 10", i, b.Status.Allocated)
		}
		if b.Status.ClaimCount != 1 {
			t.Errorf("bucket[%d] claimCount=%d, want 1", i, b.Status.ClaimCount)
		}
	}
	t.Logf("concurrent allocate across %d disjoint buckets took %s", workers, elapsed)
}

// TestPostgresAllocator_CommittedRVAdvances verifies the allocator follows
// the writer contract: every changelog row it writes passes the
// commit-order trigger, the high-water mark in quota_rv_state ends at the
// highest RV written, and the returned claim RV is the claim's changelog RV.
func TestPostgresAllocator_CommittedRVAdvances(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-rv", Namespace: "tenant-rv"}
	seedBucket(t, db, consumer, "cpu", 100)

	var before int64
	if err := db.QueryRowContext(ctx, `SELECT committed_rv FROM quota_rv_state`).Scan(&before); err != nil {
		t.Fatalf("read committed_rv: %v", err)
	}

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-rv", Namespace: consumer.Namespace},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 5}},
		},
	}
	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := alloc.Release(ctx, out); err != nil {
		t.Fatalf("Release: %v", err)
	}

	var after, maxRV, below int64
	if err := db.QueryRowContext(ctx, `SELECT committed_rv FROM quota_rv_state`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(resource_version), 0), COUNT(*) FILTER (WHERE resource_version <= $1) FROM quota_changelog`, before).Scan(&maxRV, &below); err != nil {
		t.Fatal(err)
	}
	if below != 0 {
		t.Errorf("%d changelog rows at or below the pre-test high-water mark %d", below, before)
	}
	if after != maxRV || after <= before {
		t.Errorf("committed_rv=%d, want max changelog rv %d (> %d)", after, maxRV, before)
	}
	var claimLogRV int64
	if err := db.QueryRowContext(ctx, `SELECT resource_version FROM quota_changelog WHERE key = $1 AND event_type = 'ADDED'`,
		claimKey(claim.Namespace, claim.Name)).Scan(&claimLogRV); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%d", claimLogRV) != out.ResourceVersion {
		t.Errorf("claim ADDED rv=%d, returned rv=%s", claimLogRV, out.ResourceVersion)
	}
}

func TestBucketNamespaceOption(t *testing.T) {
	tests := []struct {
		name     string
		opts     []AllocatorOption
		consumer quotainternal.ConsumerRef
		want     string
	}{
		{name: "default", consumer: quotainternal.ConsumerRef{Kind: "Organization", Name: "o"}, want: DefaultBucketNamespace},
		{name: "configured", opts: []AllocatorOption{WithBucketNamespace("milo-system")}, consumer: quotainternal.ConsumerRef{Kind: "Organization", Name: "o"}, want: "milo-system"},
		{name: "empty option keeps default", opts: []AllocatorOption{WithBucketNamespace("")}, consumer: quotainternal.ConsumerRef{Kind: "Organization", Name: "o"}, want: DefaultBucketNamespace},
		{name: "consumer namespace wins", opts: []AllocatorOption{WithBucketNamespace("milo-system")}, consumer: quotainternal.ConsumerRef{Kind: "Project", Name: "p", Namespace: "tenant"}, want: "tenant"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAllocatorWithPool(nil, nil, allocatorTestCodec, tc.opts...)
			if got := a.bucketNamespaceFor(tc.consumer); got != tc.want {
				t.Errorf("bucketNamespaceFor=%q, want %q", got, tc.want)
			}
			claim := &quotainternal.QuotaClaim{Spec: quotainternal.QuotaClaimSpec{
				ConsumerRef: tc.consumer,
				Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
			}}
			wantKey := bucketKey(tc.want, bucketNameFor("cpu", tc.consumer))
			if got := a.planAllocation(claim)[0].key; got != wantKey {
				t.Errorf("planned key=%q, want %q", got, wantKey)
			}
		})
	}
}

func TestRVDrawsUseCommitOrderedFunction(t *testing.T) {
	allocSQL, _ := buildAllocateCTE([]allocateCTEPlan{{key: "/k/a", amount: 1}}, "/c", "ns", "c", []byte("{}"))
	rv := int64(1)
	relSQL, _ := buildReleaseCTE([]*plannedBucket{{key: "/k/a", amount: 1}}, "/c", "u", &rv)
	for name, q := range map[string]string{"allocate": allocSQL, "release": relSQL} {
		if strings.Contains(q, "nextval(") {
			t.Errorf("%s CTE draws RVs with nextval; must use quota_next_rv():\n%s", name, q)
		}
		if !strings.Contains(q, "quota_next_rv()") {
			t.Errorf("%s CTE draws no RV via quota_next_rv()", name)
		}
	}
	for name, q := range map[string]string{
		"allocate lock": lockBucketForAllocateSQL, "dry-run read": readBucketForDryRunSQL,
		"release bucket lock": lockBucketForReleaseSQL, "release claim lock": lockClaimForReleaseSQL,
	} {
		if strings.Contains(q, "quota_next_rv") || strings.Contains(q, "nextval") {
			t.Errorf("%s statement must not draw an RV before all row locks are held: %s", name, q)
		}
	}
}

// --- Pure unit tests (no database required) ---------------------------------

func testConsumer() quotainternal.ConsumerRef {
	return quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "p", Namespace: "ns"}
}

func TestPlanAllocation_SortedAndAggregated(t *testing.T) {
	a := NewAllocator(nil, allocatorTestCodec)
	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: testConsumer(),
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "memory", Amount: 5},
				{ResourceType: "cpu", Amount: 2},
				{ResourceType: "gpu", Amount: 1},
				{ResourceType: "cpu", Amount: 3},
			},
		},
	}
	planned := a.planAllocation(claim)
	if len(planned) != 3 {
		t.Fatalf("expected 3 planned buckets, got %d", len(planned))
	}
	for i := 1; i < len(planned); i++ {
		if planned[i-1].key >= planned[i].key {
			t.Fatalf("planned buckets not in strictly sorted key order: %q >= %q", planned[i-1].key, planned[i].key)
		}
	}
	amounts := map[string]int64{}
	for _, p := range planned {
		amounts[p.resourceType] = p.amount
	}
	want := map[string]int64{"cpu": 5, "memory": 5, "gpu": 1}
	for rt, amt := range want {
		if amounts[rt] != amt {
			t.Errorf("resource %q amount=%d, want %d", rt, amounts[rt], amt)
		}
	}
}

func TestDecideGrantsAndApplyStatus(t *testing.T) {
	tests := []struct {
		name          string
		requests      []quotainternal.ResourceRequest
		states        map[string]bucketState
		wantGranted   map[string]bool
		wantCondition metav1.ConditionStatus
	}{
		{
			name:          "fits exactly at limit",
			requests:      []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 10}},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: 90, limit: 100}},
			wantGranted:   map[string]bool{"cpu": true},
			wantCondition: metav1.ConditionTrue,
		},
		{
			name:          "exceeds limit",
			requests:      []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 11}},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: 90, limit: 100}},
			wantGranted:   map[string]bool{"cpu": false},
			wantCondition: metav1.ConditionFalse,
		},
		{
			name: "partial grant",
			requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 1},
				{ResourceType: "memory", Amount: 500},
			},
			states: map[string]bucketState{
				"cpu":    {exists: true, allocated: 0, limit: 10},
				"memory": {exists: true, allocated: 0, limit: 100},
			},
			wantGranted:   map[string]bool{"cpu": true, "memory": false},
			wantCondition: metav1.ConditionFalse,
		},
		{
			// A saturated limit (grant sums are capped at MaxInt64) with
			// allocated near it: allocated+amount would wrap negative and
			// pass a naive allocated+amount <= limit check.
			name:          "near int64 max does not overflow into a grant",
			requests:      []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: quotainternal.MaxAmount}},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: math.MaxInt64 - 10, limit: math.MaxInt64}},
			wantGranted:   map[string]bool{"cpu": false},
			wantCondition: metav1.ConditionFalse,
		},
		{
			name:          "fits exactly at saturated limit",
			requests:      []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 10}},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: math.MaxInt64 - 10, limit: math.MaxInt64}},
			wantGranted:   map[string]bool{"cpu": true},
			wantCondition: metav1.ConditionTrue,
		},
		{
			name: "duplicate maximal requests saturate instead of wrapping",
			requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
				{ResourceType: "cpu", Amount: quotainternal.MaxAmount},
			},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: 0, limit: 100}},
			wantGranted:   map[string]bool{"cpu": false},
			wantCondition: metav1.ConditionFalse,
		},
		{
			name: "duplicate requests summed before check",
			requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 6},
				{ResourceType: "cpu", Amount: 6},
			},
			states:        map[string]bucketState{"cpu": {exists: true, allocated: 0, limit: 10}},
			wantGranted:   map[string]bool{"cpu": false},
			wantCondition: metav1.ConditionFalse,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAllocator(nil, allocatorTestCodec)
			claim := &quotainternal.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec:       quotainternal.QuotaClaimSpec{ConsumerRef: testConsumer(), Requests: tc.requests},
			}
			planned := a.planAllocation(claim)
			states := make([]bucketState, len(planned))
			for i, p := range planned {
				states[i] = tc.states[p.resourceType]
			}
			if m := firstMissingBucket(planned, states); m != nil {
				t.Fatalf("unexpected missing bucket %q", m.resourceType)
			}
			granted := decideGrants(planned, states)
			applyAllocationStatus(claim, planned, granted)

			if len(claim.Status.Allocations) != len(tc.requests) {
				t.Fatalf("got %d allocations, want %d", len(claim.Status.Allocations), len(tc.requests))
			}
			for _, alloc := range claim.Status.Allocations {
				wantStatus := quotainternal.QuotaClaimAllocationStatusDenied
				if tc.wantGranted[alloc.ResourceType] {
					wantStatus = quotainternal.QuotaClaimAllocationStatusGranted
				}
				if alloc.Status != wantStatus {
					t.Errorf("%s: status=%q, want %q", alloc.ResourceType, alloc.Status, wantStatus)
				}
				if wantStatus == quotainternal.QuotaClaimAllocationStatusDenied && (alloc.AllocatedAmount != 0 || alloc.AllocatingBucket != "") {
					t.Errorf("%s: denied allocation must not record amount/bucket: %+v", alloc.ResourceType, alloc)
				}
				if wantStatus == quotainternal.QuotaClaimAllocationStatusGranted && alloc.AllocatingBucket != bucketNameFor(alloc.ResourceType, testConsumer()) {
					t.Errorf("%s: allocatingBucket=%q, want %q", alloc.ResourceType, alloc.AllocatingBucket, bucketNameFor(alloc.ResourceType, testConsumer()))
				}
			}
			if len(claim.Status.Conditions) != 1 || claim.Status.Conditions[0].Status != tc.wantCondition {
				t.Errorf("conditions=%+v, want single Granted=%s", claim.Status.Conditions, tc.wantCondition)
			}
		})
	}
}

func TestFirstMissingBucket(t *testing.T) {
	planned := []*plannedBucket{{key: "a", resourceType: "a"}, {key: "b", resourceType: "b"}, {key: "c", resourceType: "c"}}
	tests := []struct {
		name   string
		states []bucketState
		want   string
	}{
		{name: "all exist", states: []bucketState{{exists: true}, {exists: true}, {exists: true}}, want: ""},
		{name: "middle missing", states: []bucketState{{exists: true}, {}, {exists: true}}, want: "b"},
		{name: "first missing reported first", states: []bucketState{{}, {}, {exists: true}}, want: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := firstMissingBucket(planned, tc.states)
			switch {
			case tc.want == "" && got != nil:
				t.Errorf("got missing %q, want none", got.key)
			case tc.want != "" && (got == nil || got.key != tc.want):
				t.Errorf("got %v, want %q", got, tc.want)
			}
		})
	}
	if decideGrants(planned, []bucketState{{}, {}, {}})["a"] {
		t.Errorf("a missing bucket must never be granted")
	}
	err := missingBucketError(&quotainternal.QuotaClaim{ObjectMeta: metav1.ObjectMeta{Name: "c"}}, planned[0])
	if !apierrors.IsForbidden(err) {
		t.Errorf("missing bucket error must be Forbidden, got %v", err)
	}
}

func TestLockStatements(t *testing.T) {
	if !strings.Contains(lockBucketForAllocateSQL, "FOR UPDATE") {
		t.Errorf("allocate lock statement must take a row lock: %s", lockBucketForAllocateSQL)
	}
	if !strings.Contains(lockClaimForReleaseSQL, "FOR UPDATE") {
		t.Errorf("release claim lock statement must take a row lock: %s", lockClaimForReleaseSQL)
	}
	if !strings.Contains(lockBucketForReleaseSQL, "FOR UPDATE") {
		t.Errorf("release lock statement must take a row lock: %s", lockBucketForReleaseSQL)
	}
	if strings.Contains(readBucketForDryRunSQL, "FOR UPDATE") {
		t.Errorf("dry-run read must not lock: %s", readBucketForDryRunSQL)
	}
}

func TestBuildAllocateCTE(t *testing.T) {
	tests := []struct {
		name  string
		plans []allocateCTEPlan
	}{
		{name: "all denied (claim insert only)", plans: nil},
		{name: "one granted bucket", plans: []allocateCTEPlan{{key: "/k/a", amount: 3}}},
		{name: "two granted buckets", plans: []allocateCTEPlan{{key: "/k/a", amount: 3}, {key: "/k/b", amount: 4}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlText, args := buildAllocateCTE(tc.plans, "/claims/ns/c", "ns", "c", []byte("{}"))

			if strings.Contains(sqlText, "_exists") {
				t.Errorf("CTE must not contain unreferenced existence-check arms:\n%s", sqlText)
			}
			if strings.Contains(sqlText, "FOR UPDATE") {
				t.Errorf("bucket locks belong in the ordered lock statements, not the CTE:\n%s", sqlText)
			}
			if got, want := len(args), 2*len(tc.plans)+5; got != want {
				t.Errorf("args=%d, want %d", got, want)
			}
			for i, p := range tc.plans {
				if args[2*i] != p.key || args[2*i+1] != p.amount {
					t.Errorf("plan %d args = (%v, %v), want (%v, %v)", i, args[2*i], args[2*i+1], p.key, p.amount)
				}
				arm := fmt.Sprintf("b%d_updated AS (UPDATE", i)
				if !strings.Contains(sqlText, arm) {
					t.Errorf("missing %q", arm)
				}
				// The assertion must be in the outer SELECT so Postgres evaluates it.
				assert := fmt.Sprintf("(SELECT 1 / count(*) FROM b%d_updated)", i)
				idx := strings.LastIndex(sqlText, "SELECT (SELECT resource_version FROM claim_ins)")
				if idx < 0 || !strings.Contains(sqlText[idx:], assert) {
					t.Errorf("outer SELECT must reference %q:\n%s", assert, sqlText)
				}
			}
			if strings.Contains(sqlText, fmt.Sprintf("b%d_updated", len(tc.plans))) {
				t.Errorf("CTE has more bucket arms than granted plans")
			}
			if !strings.Contains(sqlText, "claim_ins AS (INSERT INTO quota_objects") || !strings.Contains(sqlText, "'ADDED'") {
				t.Errorf("CTE must insert the claim and its ADDED changelog row:\n%s", sqlText)
			}
		})
	}
}

func TestBuildReleaseCTE(t *testing.T) {
	rv := int64(42)
	tests := []struct {
		name      string
		uid       string
		rv        *int64
		plans     []*plannedBucket
		wantWhere string
		wantArgs  int
	}{
		{
			name:      "unconditional, no buckets",
			wantWhere: "DELETE FROM quota_objects WHERE key = $1 RETURNING data",
			wantArgs:  1,
		},
		{
			name:      "rv and uid preconditions",
			uid:       "uid-1",
			rv:        &rv,
			plans:     []*plannedBucket{{key: "/k/a", amount: 3}, {key: "/k/b", amount: 4}},
			wantWhere: "DELETE FROM quota_objects WHERE key = $1 AND resource_version = $2 AND data_json->'metadata'->>'uid' = $3 RETURNING data",
			wantArgs:  3 + 4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlText, args := buildReleaseCTE(tc.plans, "/claims/ns/c", tc.uid, tc.rv)
			if !strings.Contains(sqlText, tc.wantWhere) {
				t.Errorf("missing conditional delete %q in:\n%s", tc.wantWhere, sqlText)
			}
			if len(args) != tc.wantArgs {
				t.Errorf("args=%d, want %d", len(args), tc.wantArgs)
			}
			if strings.Contains(sqlText, "FOR UPDATE") {
				t.Errorf("bucket locks belong in the ordered lock statements, not the CTE:\n%s", sqlText)
			}
			delIdx := strings.Index(sqlText, "claim_del AS")
			for i := range tc.plans {
				arm := fmt.Sprintf("b%d_updated AS (", i)
				armIdx := strings.Index(sqlText, arm)
				if armIdx < 0 || armIdx < delIdx {
					t.Fatalf("bucket arm %d missing or defined before claim_del", i)
				}
				end := strings.Index(sqlText[armIdx:], "RETURNING")
				if !strings.Contains(sqlText[armIdx:armIdx+end], "EXISTS (SELECT 1 FROM claim_del)") {
					t.Errorf("bucket arm %d must be gated on claim_del:\n%s", i, sqlText[armIdx:armIdx+end])
				}
			}
			if !strings.HasSuffix(sqlText, "SELECT (SELECT count(*) FROM claim_del), (SELECT resource_version FROM quota_objects WHERE key = $1)") {
				t.Errorf("unexpected outer SELECT:\n%s", sqlText)
			}
		})
	}
}

func TestPlanRelease(t *testing.T) {
	consumer := testConsumer()
	cpuBucket := bucketNameFor("cpu", consumer)
	memBucket := bucketNameFor("memory", consumer)
	claim := &quotainternal.QuotaClaim{
		Spec: quotainternal.QuotaClaimSpec{ConsumerRef: consumer, Requests: []quotainternal.ResourceRequest{
			{ResourceType: "memory", Amount: 9},
			{ResourceType: "cpu", Amount: 2},
			{ResourceType: "cpu", Amount: 3},
			{ResourceType: "gpu", Amount: 4},
			{ResourceType: "disk", Amount: 0},
		}},
		// The amounts and buckets in status are deliberately wrong: the
		// plan must come from the spec, with status only saying which
		// resource types were granted.
		Status: quotainternal.QuotaClaimStatus{Allocations: []quotainternal.QuotaClaimAllocationStatus{
			{ResourceType: "memory", Status: quotainternal.QuotaClaimAllocationStatusGranted, AllocatedAmount: 900, AllocatingBucket: "someone-elses-bucket"},
			{ResourceType: "cpu", Status: quotainternal.QuotaClaimAllocationStatusGranted, AllocatedAmount: 2, AllocatingBucket: cpuBucket},
			{ResourceType: "cpu", Status: quotainternal.QuotaClaimAllocationStatusGranted, AllocatedAmount: 3, AllocatingBucket: cpuBucket},
			{ResourceType: "gpu", Status: quotainternal.QuotaClaimAllocationStatusDenied, AllocatedAmount: 4, AllocatingBucket: "gpu-bucket"},
			{ResourceType: "disk", Status: quotainternal.QuotaClaimAllocationStatusGranted, AllocatedAmount: 0, AllocatingBucket: "x"},
		}},
	}
	planned := NewAllocator(nil, allocatorTestCodec).planRelease(claim)
	if len(planned) != 2 {
		t.Fatalf("expected 2 buckets (denied/zero skipped, duplicates merged), got %d", len(planned))
	}
	if planned[0].key >= planned[1].key {
		t.Errorf("release plan not sorted: %q, %q", planned[0].key, planned[1].key)
	}
	got := map[string]int64{}
	for _, p := range planned {
		got[p.key] = p.amount
	}
	ns := NewAllocator(nil, allocatorTestCodec).bucketNamespaceFor(consumer)
	if got[bucketKey(ns, cpuBucket)] != 5 || got[bucketKey(ns, memBucket)] != 9 {
		t.Errorf("unexpected release amounts: %v", got)
	}
}

func TestReleaseOutcome(t *testing.T) {
	claim := &quotainternal.QuotaClaim{ObjectMeta: metav1.ObjectMeta{Name: "c", ResourceVersion: "5"}}
	tests := []struct {
		name         string
		deleted      int64
		currentRV    *int64
		wantReleased bool
		wantConflict bool
	}{
		{name: "deleted", deleted: 1, currentRV: ptr[int64](5), wantReleased: true},
		{name: "already gone", deleted: 0, currentRV: nil},
		{name: "modified since read", deleted: 0, currentRV: ptr[int64](6), wantConflict: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			released, err := releaseOutcome(claim, tc.deleted, tc.currentRV)
			if released != tc.wantReleased {
				t.Errorf("released=%v, want %v", released, tc.wantReleased)
			}
			if tc.wantConflict != apierrors.IsConflict(err) {
				t.Errorf("err=%v, wantConflict=%v", err, tc.wantConflict)
			}
			if !tc.wantConflict && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// --- DB-gated tests ---------------------------------------------------------

// TestPostgresAllocator_Allocate_MissingBucket_Forbidden verifies the
// missing-bucket check actually runs: the claim is rejected with Forbidden,
// nothing is written, and an existing sibling bucket is left untouched.
func TestPostgresAllocator_Allocate_MissingBucket_Forbidden(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-missing", Namespace: "tenant-missing"}
	cpuKey, cpuRV := seedBucket(t, db, consumer, "cpu", 100)

	tests := []struct {
		name     string
		requests []quotainternal.ResourceRequest
	}{
		{name: "only bucket missing", requests: []quotainternal.ResourceRequest{{ResourceType: "memory", Amount: 1}}},
		{name: "one of two missing", requests: []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}, {ResourceType: "memory", Amount: 1}}},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claim := &quotainternal.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("claim-missing-%d", i), Namespace: consumer.Namespace},
				Spec:       quotainternal.QuotaClaimSpec{ConsumerRef: consumer, Requests: tc.requests},
			}
			_, err := alloc.Allocate(ctx, claim)
			if !apierrors.IsForbidden(err) {
				t.Fatalf("expected Forbidden, got %v", err)
			}
			var claims, logs int
			if err := db.QueryRow(`SELECT COUNT(*) FROM quota_objects WHERE kind = $1`, claimsKind).Scan(&claims); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM quota_changelog`).Scan(&logs); err != nil {
				t.Fatal(err)
			}
			if claims != 0 || logs != 0 {
				t.Errorf("forbidden allocate wrote data: claims=%d changelog=%d", claims, logs)
			}
			var rv int64
			if err := db.QueryRow(`SELECT resource_version FROM quota_objects WHERE key = $1`, cpuKey).Scan(&rv); err != nil {
				t.Fatal(err)
			}
			if rv != cpuRV {
				t.Errorf("sibling bucket modified: rv %d -> %d", cpuRV, rv)
			}
		})
	}
}

// TestPostgresAllocator_Allocate_DenyIsAtomic verifies that a denied claim is
// committed exactly once, already Denied: a single ADDED changelog row whose
// payload is Denied, and no MODIFIED follow-up.
func TestPostgresAllocator_Allocate_DenyIsAtomic(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-atomic", Namespace: "tenant-atomic"}
	seedBucket(t, db, consumer, "cpu", 5)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-atomic", Namespace: consumer.Namespace},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 50}},
		},
	}
	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	// Read the data as the watch does: an ADDED claim row has no data and
	// is rebuilt from quota_objects.
	rows, err := db.Query(`SELECT c.event_type, c.resource_version, `+pgwatch.ChangelogDataSQL("c")+`
		  FROM quota_changelog c WHERE c.key = $1 ORDER BY c.id`,
		claimKey(claim.Namespace, claim.Name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var events []string
	for rows.Next() {
		var ev string
		var rv int64
		var data []byte
		if err := rows.Scan(&ev, &rv, &data); err != nil {
			t.Fatal(err)
		}
		events = append(events, ev)
		obj, err := decodeToObject(allocatorTestCodec, data)
		if err != nil {
			t.Fatalf("decode changelog claim: %v", err)
		}
		logged := obj.(*quotainternal.QuotaClaim)
		if logged.Status.Allocations[0].Status != quotainternal.QuotaClaimAllocationStatusDenied {
			t.Errorf("changelog %s carries %q, want Denied", ev, logged.Status.Allocations[0].Status)
		}
		if fmt.Sprintf("%d", rv) != out.ResourceVersion {
			t.Errorf("changelog rv=%d, returned rv=%s", rv, out.ResourceVersion)
		}
	}
	if len(events) != 1 || events[0] != "ADDED" {
		t.Errorf("expected exactly one ADDED claim event, got %v", events)
	}
}

// TestPostgresAllocator_Allocate_PartialGrant verifies a claim whose buckets
// are decided independently: the fitting bucket is reserved, the other is
// untouched, and Release returns only the reserved portion.
func TestPostgresAllocator_Allocate_PartialGrant(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-partial", Namespace: "tenant-partial"}
	cpuKey, _ := seedBucket(t, db, consumer, "cpu", 10)
	memKey, memRV := seedBucket(t, db, consumer, "memory", 10)

	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-partial", Namespace: consumer.Namespace},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests: []quotainternal.ResourceRequest{
				{ResourceType: "cpu", Amount: 4},
				{ResourceType: "memory", Amount: 40},
			},
		},
	}
	out, err := alloc.Allocate(ctx, claim)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if claimResultLabel(out) != "partial" {
		t.Errorf("result label=%q, want partial", claimResultLabel(out))
	}
	if got := readBucket(t, db, cpuKey).Status.Allocated; got != 4 {
		t.Errorf("cpu allocated=%d, want 4", got)
	}
	var rv int64
	if err := db.QueryRow(`SELECT resource_version FROM quota_objects WHERE key = $1`, memKey).Scan(&rv); err != nil {
		t.Fatal(err)
	}
	if rv != memRV {
		t.Errorf("denied bucket modified")
	}
	if err := alloc.Release(ctx, out); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := readBucket(t, db, cpuKey).Status.Allocated; got != 0 {
		t.Errorf("cpu allocated=%d after release, want 0", got)
	}
	if got := readBucket(t, db, memKey).Status.Allocated; got != 0 {
		t.Errorf("memory allocated=%d after release, want 0", got)
	}
}

// TestPostgresAllocator_Release_ConcurrentNoDoubleDecrement verifies that
// concurrent releases of the same claim decrement the bucket exactly once.
func TestPostgresAllocator_Release_ConcurrentNoDoubleDecrement(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-dbl", Namespace: "tenant-dbl"}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)

	newClaim := func(name string, amount int64) *quotainternal.QuotaClaim {
		return &quotainternal.QuotaClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: consumer.Namespace},
			Spec: quotainternal.QuotaClaimSpec{
				ConsumerRef: consumer,
				Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: amount}},
			},
		}
	}
	victim, err := alloc.Allocate(ctx, newClaim("claim-victim", 25))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.Allocate(ctx, newClaim("claim-bystander", 40)); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	released := 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := alloc.ReleaseClaim(ctx, victim.DeepCopy())
			if err != nil {
				t.Errorf("ReleaseClaim: %v", err)
				return
			}
			if ok {
				mu.Lock()
				released++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if released != 1 {
		t.Errorf("released=%d, want exactly 1", released)
	}
	b := readBucket(t, db, key)
	if b.Status.Allocated != 40 || b.Status.ClaimCount != 1 {
		t.Errorf("bucket allocated=%d claimCount=%d, want 40/1", b.Status.Allocated, b.Status.ClaimCount)
	}
	var deletes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM quota_changelog WHERE key = $1 AND event_type = 'DELETED'`,
		claimKey(victim.Namespace, victim.Name)).Scan(&deletes); err != nil {
		t.Fatal(err)
	}
	if deletes != 1 {
		t.Errorf("DELETED changelog rows=%d, want 1", deletes)
	}
}

// TestPostgresAllocator_Release_StaleResourceVersion verifies that releasing
// a stale copy of a claim returns Conflict and changes nothing.
func TestPostgresAllocator_Release_StaleResourceVersion(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-stale", Namespace: "tenant-stale"}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)

	granted, err := alloc.Allocate(ctx, &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-stale", Namespace: consumer.Namespace},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 10}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cKey := claimKey(granted.Namespace, granted.Name)
	if _, err := db.Exec(`UPDATE quota_objects SET resource_version = quota_next_rv() WHERE key = $1`, cKey); err != nil {
		t.Fatal(err)
	}

	released, err := alloc.ReleaseClaim(ctx, granted)
	if !apierrors.IsConflict(err) || released {
		t.Fatalf("expected Conflict and released=false, got released=%v err=%v", released, err)
	}
	if got := readBucket(t, db, key).Status.Allocated; got != 10 {
		t.Errorf("bucket allocated=%d after conflicting release, want 10", got)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM quota_objects WHERE key = $1`, cKey).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("claim row deleted by conflicting release")
	}
}

// TestPostgresAllocator_AllocateDryRun verifies dry-run allocation reports
// the outcome a real Allocate would have and writes nothing.
func TestPostgresAllocator_AllocateDryRun(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-dry", Namespace: "tenant-dry"}
	key, seededRV := seedBucket(t, db, consumer, "cpu", 10)

	newClaim := func(name, rt string, amount int64) *quotainternal.QuotaClaim {
		return &quotainternal.QuotaClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: consumer.Namespace},
			Spec: quotainternal.QuotaClaimSpec{
				ConsumerRef: consumer,
				Requests:    []quotainternal.ResourceRequest{{ResourceType: rt, Amount: amount}},
			},
		}
	}

	tests := []struct {
		name       string
		claim      *quotainternal.QuotaClaim
		wantStatus string
		wantErr    func(error) bool
	}{
		{name: "would grant", claim: newClaim("dry-grant", "cpu", 5), wantStatus: quotainternal.QuotaClaimAllocationStatusGranted},
		{name: "would deny", claim: newClaim("dry-deny", "cpu", 50), wantStatus: quotainternal.QuotaClaimAllocationStatusDenied},
		{name: "missing bucket", claim: newClaim("dry-missing", "memory", 1), wantErr: apierrors.IsForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := alloc.AllocateDryRun(ctx, tc.claim)
			if tc.wantErr != nil {
				if !tc.wantErr(err) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("AllocateDryRun: %v", err)
			}
			if out.Status.Allocations[0].Status != tc.wantStatus {
				t.Errorf("status=%q, want %q", out.Status.Allocations[0].Status, tc.wantStatus)
			}
		})
	}

	var objects, logs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM quota_objects`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM quota_changelog`).Scan(&logs); err != nil {
		t.Fatal(err)
	}
	if objects != 1 || logs != 0 {
		t.Errorf("dry run wrote data: objects=%d changelog=%d", objects, logs)
	}
	var rv int64
	if err := db.QueryRow(`SELECT resource_version FROM quota_objects WHERE key = $1`, key).Scan(&rv); err != nil {
		t.Fatal(err)
	}
	if rv != seededRV {
		t.Errorf("dry run modified bucket")
	}

	// An existing claim makes the dry run report AlreadyExists.
	if _, err := alloc.Allocate(ctx, newClaim("dry-exists", "cpu", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.AllocateDryRun(ctx, newClaim("dry-exists", "cpu", 1)); !apierrors.IsAlreadyExists(err) {
		t.Errorf("expected AlreadyExists, got %v", err)
	}
}

// Release must return what Allocate charged, whatever the claim's status
// says about buckets and amounts: a status writer could otherwise point a
// claim at another consumer's bucket or inflate its amount and drain
// capacity on delete.
func TestPostgresAllocator_Release_IgnoresTamperedStatus(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1))

	mine := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-mine", Namespace: "tenant-mine"}
	victim := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-victim", Namespace: "tenant-mine"}
	myKey, _ := seedBucket(t, db, mine, "cpu", 100)
	victimKey, _ := seedBucket(t, db, victim, "cpu", 100)

	victimClaim, err := alloc.Allocate(ctx, &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "victim", Namespace: "tenant-mine"},
		Spec: quotainternal.QuotaClaimSpec{ConsumerRef: victim,
			Requests: []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 60}}},
	})
	if err != nil || victimClaim.Status.Allocations[0].Status != quotainternal.QuotaClaimAllocationStatusGranted {
		t.Fatalf("victim Allocate: %v", err)
	}
	granted, err := alloc.Allocate(ctx, &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: "tenant-mine"},
		Spec: quotainternal.QuotaClaimSpec{ConsumerRef: mine,
			Requests: []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 10}}},
	})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	// Tamper with the stored claim's status, as a status writer could.
	tampered := granted.DeepCopy()
	tampered.Status.Allocations[0].AllocatingBucket = victimKey[strings.LastIndex(victimKey, "/")+1:]
	tampered.Status.Allocations[0].AllocatedAmount = 60
	tampered.ResourceVersion = ""
	data, err := runtime.Encode(allocatorTestCodec, tampered)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cKey := claimKey(granted.Namespace, granted.Name)
	var rv int64
	if err := db.QueryRow(
		`WITH rv AS MATERIALIZED (SELECT quota_next_rv() AS v),
		 upd AS (UPDATE quota_objects SET data = $2, resource_version = (SELECT v FROM rv) WHERE key = $1 RETURNING resource_version),
		 log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) SELECT $1, resource_version, 'MODIFIED', $2 FROM upd)
		 SELECT resource_version FROM upd`, cKey, data).Scan(&rv); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	tampered.ResourceVersion = fmt.Sprint(rv)

	if err := alloc.Release(ctx, tampered); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := readBucket(t, db, victimKey).Status.Allocated; got != 60 {
		t.Errorf("release drained another consumer's bucket: allocated=%d, want 60", got)
	}
	if got := readBucket(t, db, myKey).Status.Allocated; got != 0 {
		t.Errorf("release did not return the claim's own capacity: allocated=%d, want 0", got)
	}
}

// When the limit drops below what is allocated, a release must not write a
// negative available (the bucket status validation rejects it).
func TestPostgresAllocator_Release_AvailableNeverNegative(t *testing.T) {
	for _, batch := range []int{1, DefaultMaxBatchSize} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			db := getTestDB(t)
			cleanupTestData(t, db)
			t.Cleanup(func() { cleanupTestData(t, db) })
			ctx := context.Background()
			alloc := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(batch))
			consumer := quotainternal.ConsumerRef{Kind: "Project", Name: fmt.Sprintf("proj-neg-%d", batch), Namespace: "tenant-neg"}
			key, _ := seedBucket(t, db, consumer, "cpu", 100)

			var claims []*quotainternal.QuotaClaim
			for i := 0; i < 2; i++ {
				c, err := alloc.Allocate(ctx, &quotainternal.QuotaClaim{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("neg-%d", i), Namespace: "tenant-neg"},
					Spec: quotainternal.QuotaClaimSpec{ConsumerRef: consumer,
						Requests: []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 40}}},
				})
				if err != nil {
					t.Fatalf("Allocate: %v", err)
				}
				claims = append(claims, c)
			}
			// Lower the limit below allocated (80), as a grant change would.
			if _, err := db.Exec(
				`UPDATE quota_objects SET data = (jsonb_set(data_json, '{status,limit}', '10'))::text::bytea WHERE key = $1`, key); err != nil {
				t.Fatalf("lower limit: %v", err)
			}
			if err := alloc.Release(ctx, claims[0]); err != nil {
				t.Fatalf("Release: %v", err)
			}
			b := readBucket(t, db, key)
			if b.Status.Allocated != 40 || b.Status.Available != 0 {
				t.Errorf("after release: allocated=%d available=%d, want 40 and 0", b.Status.Allocated, b.Status.Available)
			}
		})
	}
}

// A QuotaBucket recreated while Granted claims still hold capacity in it
// must start with their allocations, so new claims cannot over-allocate and
// later releases do not drift.
func TestStore_BucketCreateRecomputesAllocations(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1))

	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-recreate", Namespace: "tenant-re"}
	other := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-recreate", Namespace: "tenant-other"}
	key, _ := seedBucket(t, db, consumer, "cpu", 100)
	otherKey, _ := seedBucket(t, db, other, "cpu", 100)

	mk := func(name, ns string, ref quotainternal.ConsumerRef, reqs ...quotainternal.ResourceRequest) *quotainternal.QuotaClaim {
		c, err := alloc.Allocate(ctx, &quotainternal.QuotaClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       quotainternal.QuotaClaimSpec{ConsumerRef: ref, Requests: reqs},
		})
		if err != nil {
			t.Fatalf("Allocate %s: %v", name, err)
		}
		return c
	}
	mk("re-a", "tenant-re", consumer, quotainternal.ResourceRequest{ResourceType: "cpu", Amount: 30})
	mk("re-b", "tenant-re", consumer, quotainternal.ResourceRequest{ResourceType: "cpu", Amount: 20}, quotainternal.ResourceRequest{ResourceType: "cpu", Amount: 5})
	mk("re-denied", "tenant-re", consumer, quotainternal.ResourceRequest{ResourceType: "cpu", Amount: 500})
	// Same consumer name in another namespace: a different bucket.
	mk("re-other", "tenant-other", other, quotainternal.ResourceRequest{ResourceType: "cpu", Amount: 7})
	if got := readBucket(t, db, key).Status.Allocated; got != 55 {
		t.Fatalf("setup: allocated=%d, want 55", got)
	}
	_ = otherKey

	// Delete the bucket row (the bucket controller would then recreate it).
	if _, err := db.Exec(`DELETE FROM quota_objects WHERE key = $1`, key); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	store := New(db, allocatorTestCodec, "")
	store.SetNewFunc(func() runtime.Object { return &quotainternal.QuotaBucket{} })
	store.SetRecomputeBucketAllocations(true)
	name := key[strings.LastIndex(key, "/")+1:]
	ns := strings.Split(strings.TrimPrefix(key, bucketsPath+"/"), "/")[0]
	created := &quotainternal.QuotaBucket{}
	if err := store.Create(ctx, key, &quotainternal.QuotaBucket{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       quotainternal.QuotaBucketSpec{ConsumerRef: consumer, ResourceType: "cpu"},
	}, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Status.Allocated != 55 || created.Status.ClaimCount != 2 || created.Status.Available != 0 {
		t.Fatalf("recreated bucket status = %+v, want allocated 55, claimCount 2, available 0", created.Status)
	}
	stored := readBucket(t, db, key)
	if stored.Status.Allocated != 55 || stored.Status.ClaimCount != 2 {
		t.Fatalf("stored bucket status = %+v", stored.Status)
	}
	var kind string
	if err := db.QueryRow(`SELECT kind FROM quota_objects WHERE key = $1`, key).Scan(&kind); err != nil || kind != "QuotaBucket" {
		t.Fatalf("kind column = %q (%v), want QuotaBucket", kind, err)
	}
}
