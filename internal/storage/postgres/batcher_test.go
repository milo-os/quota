package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

func batchTestClaim(name string, consumer quotainternal.ConsumerRef, reqs ...quotainternal.ResourceRequest) *quotainternal.QuotaClaim {
	return &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: consumer.Namespace},
		Spec:       quotainternal.QuotaClaimSpec{ConsumerRef: consumer, Requests: reqs},
	}
}

func req(rt string, amount int64) quotainternal.ResourceRequest {
	return quotainternal.ResourceRequest{ResourceType: rt, Amount: amount}
}

// allocOp and releaseOp build batch operations the way Allocate and
// ReleaseClaim do, for driving commitBatch directly with a known batch.
func allocOp(a *PostgresAllocator, claim *quotainternal.QuotaClaim) *batchOp {
	return &batchOp{kind: opAllocate, claim: claim, cKey: claimKey(claim.Namespace, claim.Name), planned: a.planAllocation(claim)}
}

func releaseOp(t *testing.T, a *PostgresAllocator, claim *quotainternal.QuotaClaim) *batchOp {
	t.Helper()
	var rv *int64
	if claim.ResourceVersion != "" {
		v, err := strconv.ParseInt(claim.ResourceVersion, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		rv = &v
	}
	return &batchOp{kind: opRelease, claim: claim, cKey: claimKey(claim.Namespace, claim.Name), planned: a.planRelease(claim), rv: rv}
}

// outcome is a compact, comparable form of one operation's result.
type outcome string

const (
	outGranted       outcome = "granted"
	outDenied        outcome = "denied"
	outPartial       outcome = "partial"
	outForbidden     outcome = "forbidden"
	outAlreadyExists outcome = "already-exists"
	outConflict      outcome = "conflict"
	outReleased      outcome = "released"
	outGone          outcome = "gone"
	outError         outcome = "error"
)

func classify(r batchResult, kind batchOpKind) outcome {
	switch {
	case r.err != nil && apierrors.IsForbidden(r.err):
		return outForbidden
	case r.err != nil && apierrors.IsAlreadyExists(r.err):
		return outAlreadyExists
	case r.err != nil && apierrors.IsConflict(r.err):
		return outConflict
	case r.err != nil:
		return outError
	case kind == opRelease && r.released:
		return outReleased
	case kind == opRelease:
		return outGone
	}
	return outcome(claimResultLabel(r.claim))
}

type bucketCounts struct {
	allocated, available int64
	claimCount           int32
}

func readBucketCounts(t *testing.T, db *sql.DB, key string) bucketCounts {
	t.Helper()
	b := readBucket(t, db, key)
	return bucketCounts{allocated: b.Status.Allocated, available: b.Status.Available, claimCount: b.Status.ClaimCount}
}

// assertChangelogConsistent checks the watch-facing invariants after a test:
// every stored claim has exactly one ADDED row carrying its current RV,
// every changelog RV is unique, and the high-water mark equals the max RV.
func assertChangelogConsistent(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT o.key, o.resource_version,
		(SELECT count(*) FROM quota_changelog c WHERE c.key = o.key AND c.event_type = 'ADDED'),
		(SELECT max(c.resource_version) FROM quota_changelog c WHERE c.key = o.key AND c.event_type = 'ADDED')
		FROM quota_objects o WHERE o.kind = $1`, claimsKind)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key string
		var rv, added int64
		var addedRV sql.NullInt64
		if err := rows.Scan(&key, &rv, &added, &addedRV); err != nil {
			t.Fatal(err)
		}
		if added != 1 || !addedRV.Valid || addedRV.Int64 != rv {
			t.Errorf("claim %s: %d ADDED rows (rv %v), want exactly one at rv %d", key, added, addedRV, rv)
		}
	}
	var dupRVs, maxRV, committed int64
	if err := db.QueryRow(`SELECT count(*) - count(DISTINCT resource_version), COALESCE(max(resource_version), 0) FROM quota_changelog`).Scan(&dupRVs, &maxRV); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT committed_rv FROM quota_rv_state`).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if dupRVs != 0 {
		t.Errorf("%d duplicate changelog resource versions", dupRVs)
	}
	if maxRV != 0 && committed != maxRV {
		t.Errorf("committed_rv=%d, want max changelog rv %d", committed, maxRV)
	}
}

// TestCommitBatch_Outcomes drives one known batch through commitBatch and
// checks every operation gets exactly its single-claim outcome, the bucket
// ends up exact, and each touched bucket is written once.
func TestCommitBatch_Outcomes(t *testing.T) {
	db := getTestDB(t)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-batch", Namespace: "tenant-batch"}
	other := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-batch-2", Namespace: "tenant-batch"}

	type step func(t *testing.T, a *PostgresAllocator) *batchOp
	alloc := func(name string, reqs ...quotainternal.ResourceRequest) step {
		return func(_ *testing.T, a *PostgresAllocator) *batchOp {
			return allocOp(a, batchTestClaim(name, consumer, reqs...))
		}
	}
	tests := []struct {
		name string
		// limits seeds buckets for consumer (resource type -> limit).
		limits map[string]int64
		// before runs single-claim allocations first (to be released or to
		// collide with), by name.
		before map[string]int64
		ops    func(prior map[string]*quotainternal.QuotaClaim) []step
		want   []outcome
		// wantCPU is the final cpu bucket state.
		wantCPU bucketCounts
		// wantCPULogRows is how many MODIFIED rows the batch writes for cpu.
		wantCPULogRows int64
	}{
		{
			name:   "same bucket limited capacity",
			limits: map[string]int64{"cpu": 3},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("c1", req("cpu", 1)), alloc("c2", req("cpu", 1)), alloc("c3", req("cpu", 1)), alloc("c4", req("cpu", 1)), alloc("c5", req("cpu", 1))}
			},
			want:           []outcome{outGranted, outGranted, outGranted, outDenied, outDenied},
			wantCPU:        bucketCounts{allocated: 3, available: 0, claimCount: 3},
			wantCPULogRows: 1,
		},
		{
			name:   "duplicate name in batch",
			limits: map[string]int64{"cpu": 10},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("dup", req("cpu", 1)), alloc("other", req("cpu", 1)), alloc("dup", req("cpu", 5))}
			},
			want:           []outcome{outGranted, outGranted, outAlreadyExists},
			wantCPU:        bucketCounts{allocated: 2, available: 8, claimCount: 2},
			wantCPULogRows: 1,
		},
		{
			name:   "name already stored",
			limits: map[string]int64{"cpu": 10},
			before: map[string]int64{"exists": 1},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("exists", req("cpu", 1)), alloc("new", req("cpu", 1))}
			},
			want:           []outcome{outAlreadyExists, outGranted},
			wantCPU:        bucketCounts{allocated: 2, available: 8, claimCount: 2},
			wantCPULogRows: 1,
		},
		{
			name:   "missing bucket only fails its claim",
			limits: map[string]int64{"cpu": 10},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("a", req("cpu", 1)), alloc("b", req("cpu", 1), req("memory", 1)), alloc("c", req("cpu", 1))}
			},
			want:           []outcome{outGranted, outForbidden, outGranted},
			wantCPU:        bucketCounts{allocated: 2, available: 8, claimCount: 2},
			wantCPULogRows: 1,
		},
		{
			name:   "forbidden claim frees its name for a later duplicate",
			limits: map[string]int64{"cpu": 10},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("x", req("memory", 1)), alloc("x", req("cpu", 1))}
			},
			want:           []outcome{outForbidden, outGranted},
			wantCPU:        bucketCounts{allocated: 1, available: 9, claimCount: 1},
			wantCPULogRows: 1,
		},
		{
			name:   "partial grant across buckets",
			limits: map[string]int64{"cpu": 10, "memory": 1},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("p1", req("cpu", 2), req("memory", 1)), alloc("p2", req("cpu", 2), req("memory", 1))}
			},
			want:           []outcome{outGranted, outPartial},
			wantCPU:        bucketCounts{allocated: 4, available: 6, claimCount: 2},
			wantCPULogRows: 1,
		},
		{
			name:   "release ahead of claim frees capacity",
			limits: map[string]int64{"cpu": 1},
			before: map[string]int64{"held": 1},
			ops: func(prior map[string]*quotainternal.QuotaClaim) []step {
				return []step{
					func(t *testing.T, a *PostgresAllocator) *batchOp { return releaseOp(t, a, prior["held"]) },
					alloc("next", req("cpu", 1)),
				}
			},
			want:           []outcome{outReleased, outGranted},
			wantCPU:        bucketCounts{allocated: 1, available: 0, claimCount: 1},
			wantCPULogRows: 1,
		},
		{
			name:   "claim ahead of release is decided first",
			limits: map[string]int64{"cpu": 1},
			before: map[string]int64{"held": 1},
			ops: func(prior map[string]*quotainternal.QuotaClaim) []step {
				return []step{
					alloc("next", req("cpu", 1)),
					func(t *testing.T, a *PostgresAllocator) *batchOp { return releaseOp(t, a, prior["held"]) },
				}
			},
			want:           []outcome{outDenied, outReleased},
			wantCPU:        bucketCounts{allocated: 0, available: 1, claimCount: 0},
			wantCPULogRows: 1,
		},
		{
			name:   "stale release conflicts, gone release is a no-op",
			limits: map[string]int64{"cpu": 10},
			before: map[string]int64{"held": 2},
			ops: func(prior map[string]*quotainternal.QuotaClaim) []step {
				return []step{
					func(t *testing.T, a *PostgresAllocator) *batchOp {
						stale := prior["held"].DeepCopy()
						stale.ResourceVersion = "1"
						return releaseOp(t, a, stale)
					},
					func(t *testing.T, a *PostgresAllocator) *batchOp {
						gone := prior["held"].DeepCopy()
						gone.Name = "never-existed"
						return releaseOp(t, a, gone)
					},
					alloc("fresh", req("cpu", 1)),
				}
			},
			want:           []outcome{outConflict, outGone, outGranted},
			wantCPU:        bucketCounts{allocated: 3, available: 7, claimCount: 2},
			wantCPULogRows: 1,
		},
		{
			name:   "all rejected writes nothing",
			limits: map[string]int64{"cpu": 1},
			ops: func(map[string]*quotainternal.QuotaClaim) []step {
				return []step{alloc("m1", req("memory", 1)), alloc("m2", req("memory", 1))}
			},
			want:           []outcome{outForbidden, outForbidden},
			wantCPU:        bucketCounts{allocated: 0, available: 1, claimCount: 0},
			wantCPULogRows: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cleanupTestData(t, db)
			t.Cleanup(func() { cleanupTestData(t, db) })
			ctx := context.Background()
			a := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1))

			keys := map[string]string{}
			for rt, limit := range tc.limits {
				keys[rt], _ = seedBucket(t, db, consumer, rt, limit)
			}
			seedBucket(t, db, other, "cpu", 100) // untouched bystander
			prior := map[string]*quotainternal.QuotaClaim{}
			for name, amount := range tc.before {
				out, err := a.Allocate(ctx, batchTestClaim(name, consumer, req("cpu", amount)))
				if err != nil {
					t.Fatal(err)
				}
				prior[name] = out
			}
			var logBefore int64
			if err := db.QueryRow(`SELECT count(*) FROM quota_changelog WHERE key = $1`, keys["cpu"]).Scan(&logBefore); err != nil {
				t.Fatal(err)
			}

			var ops []*batchOp
			for _, s := range tc.ops(prior) {
				ops = append(ops, s(t, a))
			}
			results, err := a.commitBatch(ctx, ops, nil)
			if err != nil {
				t.Fatalf("commitBatch: %v", err)
			}
			for i, r := range results {
				if got := classify(r, ops[i].kind); got != tc.want[i] {
					t.Errorf("op %d (%s): got %s (%v), want %s", i, ops[i].cKey, got, r.err, tc.want[i])
				}
				if r.claim != nil && r.claim.ResourceVersion == "" {
					t.Errorf("op %d: committed claim has no resource version", i)
				}
			}
			if got := readBucketCounts(t, db, keys["cpu"]); got != tc.wantCPU {
				t.Errorf("cpu bucket = %+v, want %+v", got, tc.wantCPU)
			}
			var logAfter int64
			if err := db.QueryRow(`SELECT count(*) FROM quota_changelog WHERE key = $1`, keys["cpu"]).Scan(&logAfter); err != nil {
				t.Fatal(err)
			}
			if got := logAfter - logBefore; got != tc.wantCPULogRows {
				t.Errorf("cpu bucket changelog rows written by batch = %d, want %d", got, tc.wantCPULogRows)
			}
			assertChangelogConsistent(t, db)
		})
	}
}

// TestBatcher_ConcurrentLimitedCapacity sends many concurrent claims at one
// bucket through the batching allocator: exactly limit are granted.
func TestBatcher_ConcurrentLimitedCapacity(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	db.SetMaxOpenConns(32)

	const workers, limit = 200, 37
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-cap", Namespace: "tenant-cap"}
	key, _ := seedBucket(t, db, consumer, "cpu", limit)

	a := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(16))
	var maxBatch atomic.Int64
	a.batcher.onBatch = func(n int) {
		for {
			cur := maxBatch.Load()
			if int64(n) <= cur || maxBatch.CompareAndSwap(cur, int64(n)) {
				return
			}
		}
	}

	var granted, denied atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out, err := a.Allocate(context.Background(), batchTestClaim(fmt.Sprintf("cap-%d", i), consumer, req("cpu", 1)))
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			switch claimResultLabel(out) {
			case "granted":
				granted.Add(1)
			case "denied":
				denied.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if granted.Load() != limit || denied.Load() != workers-limit {
		t.Errorf("granted=%d denied=%d, want %d and %d", granted.Load(), denied.Load(), limit, workers-limit)
	}
	if got, want := readBucketCounts(t, db, key), (bucketCounts{allocated: limit, available: 0, claimCount: limit}); got != want {
		t.Errorf("bucket = %+v, want %+v", got, want)
	}
	if maxBatch.Load() < 2 {
		t.Errorf("largest batch was %d; %d concurrent claims never batched", maxBatch.Load(), workers)
	}
	if maxBatch.Load() > 16 {
		t.Errorf("largest batch %d exceeds the configured max 16", maxBatch.Load())
	}
	t.Logf("largest batch: %d", maxBatch.Load())
	assertChangelogConsistent(t, db)
	assertBucketLogMonotonic(t, db, key)
}

// assertBucketLogMonotonic checks that a bucket's MODIFIED rows, read in
// resource-version order (watch order), show allocated never decreasing --
// i.e. RV order is the order the writes were applied under the row lock.
// Only valid for allocation-only workloads.
func assertBucketLogMonotonic(t *testing.T, db *sql.DB, key string) {
	t.Helper()
	rows, err := db.Query(`SELECT resource_version, data FROM quota_changelog WHERE key = $1 AND event_type = 'MODIFIED' ORDER BY resource_version`, key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	prev := int64(-1)
	for rows.Next() {
		var rv int64
		var data []byte
		if err := rows.Scan(&rv, &data); err != nil {
			t.Fatal(err)
		}
		var b struct {
			Status struct {
				Allocated int64 `json:"allocated"`
			} `json:"status"`
		}
		if err := json.Unmarshal(data, &b); err != nil {
			t.Fatal(err)
		}
		if b.Status.Allocated < prev {
			t.Errorf("bucket rv %d has allocated %d after %d: changelog RV order is not commit order", rv, b.Status.Allocated, prev)
		}
		prev = b.Status.Allocated
	}
}

// TestBatcher_NoDeadlockMixedPaths runs overlapping multi-bucket claims,
// declared in opposite request orders, through two batching allocators (two
// replicas) and one unbatched allocator at once, each claim followed by its
// release. Lock order is global, so nothing deadlocks: a deadlock would
// surface as a 40P01 error, a fallback, or a timeout.
func TestBatcher_NoDeadlockMixedPaths(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	db.SetMaxOpenConns(48)

	consumers := []quotainternal.ConsumerRef{
		{APIGroup: "example.com", Kind: "Project", Name: "dl-a", Namespace: "tenant-dl"},
		{APIGroup: "example.com", Kind: "Project", Name: "dl-b", Namespace: "tenant-dl"},
	}
	var keys []string
	for _, c := range consumers {
		for _, rt := range []string{"cpu", "memory", "disk"} {
			k, _ := seedBucket(t, db, c, rt, 1<<30)
			keys = append(keys, k)
		}
	}
	allocators := []*PostgresAllocator{
		NewAllocator(db, allocatorTestCodec),
		NewAllocator(db, allocatorTestCodec),
		NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1)),
	}
	var fallbacks atomic.Int64
	for _, a := range allocators[:2] {
		a.batcher.onFallback = func(err error) {
			fallbacks.Add(1)
			t.Logf("fallback: %v", err)
		}
	}

	orders := [][]string{{"cpu", "memory", "disk"}, {"disk", "memory", "cpu"}, {"memory", "cpu"}, {"disk", "cpu"}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var seq atomic.Int64
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			a := allocators[w%len(allocators)]
			for i := 0; i < 15; i++ {
				n := seq.Add(1)
				var reqs []quotainternal.ResourceRequest
				for _, rt := range orders[int(n)%len(orders)] {
					reqs = append(reqs, req(rt, 1))
				}
				out, err := a.Allocate(ctx, batchTestClaim(fmt.Sprintf("dl-%d", n), consumers[int(n)%2], reqs...))
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				if err := a.Release(ctx, out); err != nil {
					t.Errorf("release: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("timed out: possible deadlock")
	}
	if fallbacks.Load() != 0 {
		t.Errorf("%d fallbacks", fallbacks.Load())
	}
	for _, k := range keys {
		if got := readBucketCounts(t, db, k); got.allocated != 0 || got.claimCount != 0 {
			t.Errorf("bucket %s = %+v after every claim was released", k, got)
		}
	}
	assertChangelogConsistent(t, db)
}

// TestBatcher_SizeOneMatchesUnbatched runs the same sequence with batching
// disabled and with a batching allocator used serially (every batch has one
// operation). Outcomes and bucket state must be identical.
func TestBatcher_SizeOneMatchesUnbatched(t *testing.T) {
	db := getTestDB(t)
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-one", Namespace: "tenant-one"}

	if NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1)).batcher != nil {
		t.Fatal("WithMaxBatchSize(1) must disable the batcher")
	}

	run := func(t *testing.T, a *PostgresAllocator) ([]outcome, bucketCounts) {
		cleanupTestData(t, db)
		ctx := context.Background()
		key, _ := seedBucket(t, db, consumer, "cpu", 3)
		var got []outcome
		var held []*quotainternal.QuotaClaim
		for i := 0; i < 5; i++ {
			out, err := a.Allocate(ctx, batchTestClaim(fmt.Sprintf("one-%d", i), consumer, req("cpu", 1)))
			got = append(got, classify(batchResult{claim: out, err: err}, opAllocate))
			if err == nil {
				held = append(held, out)
			}
		}
		_, err := a.Allocate(ctx, batchTestClaim("one-0", consumer, req("cpu", 1)))
		got = append(got, classify(batchResult{err: err}, opAllocate))
		_, err = a.Allocate(ctx, batchTestClaim("one-missing", consumer, req("memory", 1)))
		got = append(got, classify(batchResult{err: err}, opAllocate))
		released, err := a.ReleaseClaim(ctx, held[0])
		got = append(got, classify(batchResult{released: released, err: err}, opRelease))
		released, err = a.ReleaseClaim(ctx, held[0])
		got = append(got, classify(batchResult{released: released, err: err}, opRelease))
		stale := held[1].DeepCopy()
		stale.ResourceVersion = "1"
		released, err = a.ReleaseClaim(ctx, stale)
		got = append(got, classify(batchResult{released: released, err: err}, opRelease))
		assertChangelogConsistent(t, db)
		return got, readBucketCounts(t, db, key)
	}

	var batchSizes []int
	batched := NewAllocator(db, allocatorTestCodec)
	batched.batcher.onBatch = func(n int) { batchSizes = append(batchSizes, n) }

	unbatchedOut, unbatchedBucket := run(t, NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1)))
	batchedOut, batchedBucket := run(t, batched)
	t.Cleanup(func() { cleanupTestData(t, db) })

	want := []outcome{outGranted, outGranted, outGranted, outDenied, outDenied, outAlreadyExists, outForbidden, outReleased, outGone, outConflict}
	if fmt.Sprint(unbatchedOut) != fmt.Sprint(want) {
		t.Errorf("unbatched outcomes %v, want %v", unbatchedOut, want)
	}
	if fmt.Sprint(batchedOut) != fmt.Sprint(unbatchedOut) || batchedBucket != unbatchedBucket {
		t.Errorf("batched %v %+v differs from unbatched %v %+v", batchedOut, batchedBucket, unbatchedOut, unbatchedBucket)
	}
	for _, n := range batchSizes {
		if n != 1 {
			t.Errorf("serial calls formed a batch of %d", n)
		}
	}
}

// TestBatcher_FallbackOnUnexpectedFailure races a concurrent writer into the
// gap between the batch's probe and its INSERT: the batch fails with a
// unique violation it could not anticipate, and every operation is retried
// on its own. Only the colliding claim gets AlreadyExists.
func TestBatcher_FallbackOnUnexpectedFailure(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	ctx := context.Background()

	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-fb", Namespace: "tenant-fb"}
	key, _ := seedBucket(t, db, consumer, "cpu", 10)
	a := NewAllocator(db, allocatorTestCodec)
	other := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1))

	ops := []*batchOp{
		allocOp(a, batchTestClaim("fb-1", consumer, req("cpu", 1))),
		allocOp(a, batchTestClaim("fb-race", consumer, req("cpu", 1))),
		allocOp(a, batchTestClaim("fb-3", consumer, req("cpu", 1))),
	}
	// Racing writer: another replica creates fb-race after this batch's
	// probe. It uses its own bucket so it does not wait on the batch's row
	// lock, and the batch has drawn no RV yet, so it does not wait on the
	// commit-order lock either.
	racer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-fb-racer", Namespace: "tenant-fb"}
	seedBucket(t, db, racer, "cpu", 10)
	var raced bool
	var fellBack error
	a.batcher.onFallback = func(err error) { fellBack = err }
	a.batcher.beforeWrite = func() {
		if raced {
			return
		}
		raced = true
		if _, err := other.Allocate(ctx, batchTestClaim("fb-race", racer, req("cpu", 1))); err != nil {
			t.Errorf("racer: %v", err)
		}
	}
	for _, op := range ops {
		op.done = make(chan batchResult, 1)
		op.state = opTaken
	}
	a.batcher.executeDetached(ops)

	if fellBack == nil || !isUniqueViolation(errors.Unwrap(fellBack)) {
		t.Errorf("batch should have failed with a unique violation and fallen back, got %v", fellBack)
	}
	want := []outcome{outGranted, outAlreadyExists, outGranted}
	for i, op := range ops {
		r := <-op.done
		if got := classify(r, op.kind); got != want[i] {
			t.Errorf("op %d: got %s (%v), want %s", i, got, r.err, want[i])
		}
	}
	if got, want := readBucketCounts(t, db, key), (bucketCounts{allocated: 2, available: 8, claimCount: 2}); got != want {
		t.Errorf("bucket = %+v, want %+v", got, want)
	}
	assertChangelogConsistent(t, db)
}

// TestBatcher_CancelWhileQueued occupies every batch slot, queues three
// requests and cancels one of them while it waits: it is dropped and never
// written. The other two are then taken into a batch and their context is
// cancelled mid-batch; they were already taken, so their outcome stands.
// Each holder uses its own bucket: batches holding a common bucket are not
// in flight together. (A request on the idle fast path runs on its own context, exactly like the
// unbatched path, so it is not part of this check.)
func TestBatcher_CancelWhileQueued(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-cancel", Namespace: "tenant-cancel"}
	key, _ := seedBucket(t, db, consumer, "cpu", 10)
	a := NewAllocator(db, allocatorTestCodec)

	// Phase 0: the fast-path holders block in onBatch until holdGate.
	// Phase 1: queued batches block until batchGate.
	var phase atomic.Int32
	holding := make(chan struct{}, maxInFlightBatches)
	holdGate, batchGate := make(chan struct{}), make(chan struct{})
	batchEntered := make(chan struct{}, 4)
	a.batcher.onBatch = func(int) {
		if phase.Load() == 0 {
			holding <- struct{}{}
			<-holdGate
			return
		}
		batchEntered <- struct{}{}
		<-batchGate
	}
	queueLen := func() int {
		a.batcher.mu.Lock()
		defer a.batcher.mu.Unlock()
		return len(a.batcher.queue)
	}
	type res struct {
		out *quotainternal.QuotaClaim
		err error
	}
	allocate := func(ctx context.Context, name string, out chan<- res) {
		go func() {
			claim, err := a.Allocate(ctx, batchTestClaim(name, consumer, req("cpu", 1)))
			out <- res{claim, err}
		}()
	}

	held := make(chan res, maxInFlightBatches)
	for i := 0; i < maxInFlightBatches; i++ {
		holder := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: fmt.Sprintf("proj-hold-%d", i), Namespace: "tenant-cancel"}
		seedBucket(t, db, holder, "cpu", 10)
		go func(i int) {
			claim, err := a.Allocate(context.Background(), batchTestClaim(fmt.Sprintf("cancel-hold-%d", i), holder, req("cpu", 1)))
			held <- res{claim, err}
		}(i)
		<-holding
	}

	takenCtx, cancelTaken := context.WithCancel(context.Background())
	taken := make(chan res, 2)
	allocate(takenCtx, "cancel-taken-a", taken)
	allocate(takenCtx, "cancel-taken-b", taken)
	droppedCtx, cancelDropped := context.WithCancel(context.Background())
	dropped := make(chan res, 1)
	allocate(droppedCtx, "cancel-dropped", dropped)
	for queueLen() != 3 {
		time.Sleep(time.Millisecond)
	}

	cancelDropped()
	select {
	case r := <-dropped:
		if r.err == nil || !strings.Contains(r.err.Error(), "cancelled") {
			t.Errorf("cancelled queued request: err=%v, want a cancellation error", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled queued request did not return while it was queued")
	}

	// Let the holders finish; a runner takes the two remaining requests.
	phase.Store(1)
	close(holdGate)
	<-batchEntered
	for queueLen() != 0 {
		time.Sleep(time.Millisecond)
	}
	cancelTaken() // both are in a batch now
	close(batchGate)

	for i := 0; i < maxInFlightBatches; i++ {
		if r := <-held; r.err != nil {
			t.Errorf("holder: %v", r.err)
		}
	}
	for i := 0; i < 2; i++ {
		r := <-taken
		if r.err != nil || claimResultLabel(r.out) != "granted" {
			t.Errorf("request taken before its context was cancelled: %v %v, want granted", r.out, r.err)
		}
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM quota_objects WHERE key = $1`, claimKey(consumer.Namespace, "cancel-dropped")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("cancelled queued claim was written")
	}
	want := int64(2)
	if got := readBucketCounts(t, db, key); got.allocated != want || int64(got.claimCount) != want {
		t.Errorf("bucket = %+v, want allocated and claimCount %d", got, want)
	}
}

// TestTakeLocked_SkipsHeldBuckets checks that a batch is formed only from
// operations whose buckets no in-flight batch holds, that skipped operations
// keep their order, and that releasing the hold makes them takeable.
func TestTakeLocked_SkipsHeldBuckets(t *testing.T) {
	op := func(key string, buckets ...string) *batchOp {
		o := &batchOp{kind: opAllocate, cKey: key}
		for _, bk := range buckets {
			o.planned = append(o.planned, &plannedBucket{key: bk})
		}
		return o
	}
	keys := func(ops []*batchOp) string {
		var out []string
		for _, o := range ops {
			out = append(out, o.cKey)
		}
		return fmt.Sprint(out)
	}
	b := newBatcher(nil, 8)
	inFlight := []*batchOp{op("held", "b1")}
	b.holdLocked(inFlight, 1)
	b.queue = []*batchOp{op("c1", "b1"), op("c2", "b2"), op("c3", "b2", "b1"), op("c4", "b3")}
	if got := keys(b.takeLocked()); got != "[c2 c4]" {
		t.Fatalf("taken %s, want [c2 c4]", got)
	}
	if got := keys(b.queue); got != "[c1 c3]" {
		t.Fatalf("rest %s, want [c1 c3]", got)
	}
	b.holdLocked(inFlight, -1)
	if len(b.held) != 0 {
		t.Fatalf("held = %v after release, want empty", b.held)
	}
	if got := keys(b.takeLocked()); got != "[c1 c3]" {
		t.Fatalf("taken after release %s, want [c1 c3]", got)
	}
}

// TestTakeLocked checks batch formation: arrival order, the max size,
// dropping cancelled ops, and deferring ops whose claim key is already in
// the batch (except create after create).
func TestTakeLocked(t *testing.T) {
	op := func(kind batchOpKind, key string, state batchOpState) *batchOp {
		return &batchOp{kind: kind, cKey: key, state: state}
	}
	tests := []struct {
		name      string
		max       int
		queue     []*batchOp
		wantTaken []string
		wantRest  []string
	}{
		{name: "takes in order up to max", max: 2,
			queue:     []*batchOp{op(opAllocate, "a", opQueued), op(opAllocate, "b", opQueued), op(opAllocate, "c", opQueued)},
			wantTaken: []string{"a", "b"}, wantRest: []string{"c"}},
		{name: "drops cancelled", max: 4,
			queue:     []*batchOp{op(opAllocate, "a", opCancelled), op(opRelease, "b", opQueued)},
			wantTaken: []string{"b"}, wantRest: nil},
		{name: "duplicate creates share a batch", max: 4,
			queue:     []*batchOp{op(opAllocate, "a", opQueued), op(opAllocate, "a", opQueued)},
			wantTaken: []string{"a", "a"}, wantRest: nil},
		{name: "release and create of one key are split", max: 4,
			queue:     []*batchOp{op(opRelease, "a", opQueued), op(opAllocate, "a", opQueued), op(opAllocate, "b", opQueued)},
			wantTaken: []string{"a", "b"}, wantRest: []string{"a"}},
		{name: "two releases of one key are split", max: 4,
			queue:     []*batchOp{op(opRelease, "a", opQueued), op(opRelease, "a", opQueued)},
			wantTaken: []string{"a"}, wantRest: []string{"a"}},
	}
	keys := func(ops []*batchOp) []string {
		var out []string
		for _, o := range ops {
			out = append(out, o.cKey)
		}
		return out
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBatcher(nil, tc.max)
			b.queue = tc.queue
			taken := b.takeLocked()
			if fmt.Sprint(keys(taken)) != fmt.Sprint(tc.wantTaken) || fmt.Sprint(keys(b.queue)) != fmt.Sprint(tc.wantRest) {
				t.Errorf("taken %v rest %v, want %v and %v", keys(taken), keys(b.queue), tc.wantTaken, tc.wantRest)
			}
			for _, o := range taken {
				if o.state != opTaken {
					t.Errorf("taken op %s not marked taken", o.cKey)
				}
			}
		})
	}
}

// TestBatchLocksSorted checks the lock list is the sorted union of buckets
// and claim rows (released and created), with buckets before claims.
func TestBatchLocksSorted(t *testing.T) {
	a := NewAllocator(nil, allocatorTestCodec)
	c1 := testConsumer()
	c2 := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "q", Namespace: "ns"}
	held := batchTestClaim("held", c2, req("cpu", 1))
	held.Status.Allocations = []quotainternal.QuotaClaimAllocationStatus{{
		ResourceType: "cpu", Status: quotainternal.QuotaClaimAllocationStatusGranted, AllocatedAmount: 1,
		AllocatingBucket: bucketNameFor("cpu", c2),
	}}
	ops := []*batchOp{
		allocOp(a, batchTestClaim("z", c1, req("memory", 1), req("cpu", 1))),
		releaseOp(t, a, held),
		allocOp(a, batchTestClaim("y", c1, req("cpu", 1))),
	}
	locks, buckets, claims := batchLocks(ops)
	var got []string
	for _, l := range locks {
		got = append(got, l.key)
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("locks not sorted: %v", got)
	}
	if len(buckets) != 3 || len(claims) != 3 || len(locks) != 6 {
		t.Errorf("buckets=%d claims=%d locks=%d, want 3, 3, 6", len(buckets), len(claims), len(locks))
	}
	for i, l := range locks {
		if (i < 3) != (l.bucket != nil) {
			t.Errorf("every bucket lock must come before every claim row lock: %v", got)
		}
	}
}

// TestBatchStatementsFollowWriterContract: lock and probe statements draw no
// RV, and the write statement draws every RV with quota_next_rv().
func TestBatchStatementsFollowWriterContract(t *testing.T) {
	for name, q := range map[string]string{"bucket lock": lockBucketForBatchSQL, "claim lock": lockClaimForBatchSQL} {
		if containsAny(q, "quota_next_rv", "nextval") {
			t.Errorf("%s must not draw an RV: %s", name, q)
		}
	}
	for _, q := range []string{lockBucketForBatchSQL, lockClaimForBatchSQL} {
		if !containsAny(q, "FOR UPDATE") {
			t.Errorf("lock statement takes no row lock: %s", q)
		}
	}
	if containsAny(commitBatchSQL, "nextval(", "FOR UPDATE") || !containsAny(commitBatchSQL, "quota_next_rv()") {
		t.Errorf("batch write must draw RVs only via quota_next_rv() and take no new row lock:\n%s", commitBatchSQL)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// TestBatcher_PanicAnswersEveryWaiter: a panic inside a batch must not
// crash the process or strand callers; every op gets an InternalError and
// nothing is committed.
func TestBatcher_PanicAnswersEveryWaiter(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-panic", Namespace: "tenant-panic"}
	key, _ := seedBucket(t, db, consumer, "cpu", 10)
	a := NewAllocator(db, allocatorTestCodec)
	a.batcher.beforeWrite = func() { panic("boom") }

	ops := []*batchOp{
		allocOp(a, batchTestClaim("panic-1", consumer, req("cpu", 1))),
		allocOp(a, batchTestClaim("panic-2", consumer, req("cpu", 1))),
	}
	for _, op := range ops {
		op.done = make(chan batchResult, 1)
		op.state = opTaken
	}
	a.batcher.executeDetached(ops)
	for i, op := range ops {
		if r := <-op.done; r.err == nil || !apierrors.IsInternalError(r.err) && !strings.Contains(r.err.Error(), "panic") {
			t.Errorf("op %d: err=%v, want an internal error", i, r.err)
		}
	}
	if got := readBucketCounts(t, db, key); got.allocated != 0 || got.claimCount != 0 {
		t.Errorf("bucket = %+v after a panicked batch, want untouched", got)
	}
}

// TestCommitBatch_PlanStableWithStaleStats reproduces the fresh-deployment
// case: statistics describe a near-empty quota_objects, the batch
// statements run often enough on one connection for Postgres to consider a
// cached generic plan, then the table grows without a new ANALYZE. A batch
// must still find its rows through the primary key, not by scanning the
// table (planStableSQL). Measured with seq_tup_read, which a generic
// seq-scan plan would raise by roughly the table size per lock statement.
func TestCommitBatch_PlanStableWithStaleStats(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })
	db.SetMaxOpenConns(1) // one connection, so its statement cache is the one under test
	ctx := context.Background()

	// An autovacuum ANALYZE after the filler insert would invalidate the
	// cached plans and hide the problem.
	if _, err := db.Exec(`ALTER TABLE quota_objects SET (autovacuum_enabled = false)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`ALTER TABLE quota_objects RESET (autovacuum_enabled)`); err != nil {
			t.Errorf("reset autovacuum: %v", err)
		}
	})
	// TRUNCATE drops the pages earlier tests left behind, so after ANALYZE
	// the statistics say "one row, one page", as on a fresh database.
	if _, err := db.Exec(`TRUNCATE quota_objects`); err != nil {
		t.Fatal(err)
	}
	consumer := quotainternal.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "proj-plan", Namespace: "tenant-plan"}
	seedBucket(t, db, consumer, "cpu", 1<<30)
	if _, err := db.Exec(`ANALYZE quota_objects`); err != nil {
		t.Fatal(err)
	}
	a := NewAllocator(db, allocatorTestCodec)
	var n int
	batch := func() {
		t.Helper()
		var ops []*batchOp
		for i := 0; i < 3; i++ {
			n++
			ops = append(ops, allocOp(a, batchTestClaim(fmt.Sprintf("plan-%d", n), consumer, req("cpu", 1))))
		}
		if _, err := a.commitBatch(ctx, ops, nil); err != nil {
			t.Fatalf("commitBatch: %v", err)
		}
	}
	for i := 0; i < 8; i++ { // past the 5 custom plans before a generic one is considered
		batch()
	}

	const filler = 20000
	if _, err := db.Exec(`INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data)
		SELECT '/plan-filler/' || g, 1, 'PlanFiller', 'plan-filler', 'f' || g, convert_to('{}', 'UTF8')
		FROM generate_series(1, $1) AS g`, filler); err != nil {
		t.Fatal(err)
	}

	seqRead := func() int64 {
		t.Helper()
		var v int64
		// Statistics reach shared memory when a transaction ends after
		// the flush is requested, so end one more before reading.
		if _, err := db.Exec(`SELECT pg_stat_force_next_flush()`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT 1`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`SELECT pg_stat_clear_snapshot()`); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COALESCE(seq_tup_read, 0) FROM pg_stat_user_tables WHERE relname = 'quota_objects'`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	// Control: a deliberate sequential scan must show up, or the
	// measurement below proves nothing.
	c0 := seqRead()
	if _, err := db.Exec(`SELECT count(*) FROM quota_objects WHERE name || '' = 'x'`); err != nil {
		t.Fatal(err)
	}
	if got := seqRead() - c0; got < filler {
		t.Fatalf("control seq scan read %d rows, want >= %d: seq_tup_read is not being flushed", got, filler)
	}
	before := seqRead()
	batch()
	if got := seqRead() - before; got >= filler {
		t.Errorf("one batch read %d quota_objects rows by sequential scan (table has %d+ rows): a stale generic plan is in use", got, filler)
	}
}
