package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

// The benchmarks below measure the write path under concurrency, and in
// particular the cost of the global advisory lock that quota_next_rv()
// takes. Every writer holds that lock from its RV draw
// until COMMIT, so throughput across unrelated keys is bounded by how
// quickly transactions can pass through it.
//
// They need POSTGRES_DSN and skip without it. Run them with:
//
//	go test -run '^$' -bench . -benchtime 10s ./internal/storage/postgres/
//
// Each reports ops/s alongside ns/op so results from runs with different
// parallelism can be compared directly.

// benchParallelism multiplies GOMAXPROCS to get the number of concurrent
// writers. Writers spend most of their time waiting on Postgres, so more
// goroutines than CPUs are needed to put the lock under contention.
const benchParallelism = 8

// batchStats, when set (batcher_bench_test.go sets it), attaches to an
// allocator's group-commit batcher and returns a function that reports the
// batch sizes seen. It is a variable so this file also builds against an
// allocator without batching, for before/after comparisons.
var batchStats func(b *testing.B, alloc *PostgresAllocator) (report func())

func startBatchStats(b *testing.B, alloc *PostgresAllocator) func() {
	if batchStats == nil {
		return func() {}
	}
	return batchStats(b, alloc)
}

func reportThroughput(b *testing.B) {
	b.Helper()
	if s := b.Elapsed().Seconds(); s > 0 {
		b.ReportMetric(float64(b.N)/s, "ops/s")
	}
}

// BenchmarkNextRV isolates the lock: each operation is a transaction that
// draws one RV and commits, with no other work holding the lock open.
func BenchmarkNextRV(b *testing.B) {
	db := getTestDB(b)
	db.SetMaxOpenConns(64)
	ctx := context.Background()

	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Error(err)
				return
			}
			var rv int64
			if err := tx.QueryRowContext(ctx, `SELECT quota_next_rv()`).Scan(&rv); err != nil {
				_ = tx.Rollback()
				b.Error(err)
				return
			}
			if err := tx.Commit(); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportThroughput(b)
}

// BenchmarkStoreCreate measures Store.Create on distinct keys: the object
// insert, the changelog insert and the RV draw in one transaction. Writers
// never touch the same row, so any serialization comes from the lock.
func BenchmarkStoreCreate(b *testing.B) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })
	db.SetMaxOpenConns(64)

	store := New(db, testCodec, "")
	ctx := context.Background()
	var seq atomic.Int64

	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			name := fmt.Sprintf("bench-%d", n)
			obj := newTestObject(name, "v")
			if err := store.Create(ctx, "/quota/default/"+name, obj, &testKind{}, 0); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportThroughput(b)
}

// BenchmarkAllocate measures the transactional claim path. "distinct-buckets"
// gives each writer its own bucket, so the only shared resource is the RV
// lock. "same-bucket" sends every writer to one bucket, adding row-lock
// contention on top. The gap between the two shows how much of the cost is
// the bucket lock rather than the global one.
func BenchmarkAllocate(b *testing.B) {
	b.Run("distinct-buckets", func(b *testing.B) { benchmarkAllocate(b, false) })
	b.Run("same-bucket", func(b *testing.B) { benchmarkAllocate(b, true) })
}

func benchmarkAllocate(b *testing.B, shared bool) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })
	db.SetMaxOpenConns(64)

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)

	// Large enough that no claim is ever denied for the length of a run.
	const limit = int64(1) << 40
	consumerFor := func(i int64) quotainternal.ConsumerRef {
		return quotainternal.ConsumerRef{
			APIGroup:  "example.com",
			Kind:      "Project",
			Name:      fmt.Sprintf("bench-%d", i),
			Namespace: fmt.Sprintf("bench-tenant-%d", i),
		}
	}

	// One bucket per writer goroutine, seeded up front so the timed loop
	// never hits a missing bucket.
	writers := int64(benchParallelism * runtime.GOMAXPROCS(0))
	if shared {
		seedBucket(b, db, consumerFor(0), "cpu", limit)
	} else {
		for i := int64(0); i < writers; i++ {
			seedBucket(b, db, consumerFor(i), "cpu", limit)
		}
	}

	vacuumForBench(b, db)
	report := startBatchStats(b, alloc)
	var writerSeq, claimSeq atomic.Int64

	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		consumer := consumerFor(0)
		if !shared {
			consumer = consumerFor((writerSeq.Add(1) - 1) % writers)
		}
		for pb.Next() {
			claim := &quotainternal.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("bench-claim-%d", claimSeq.Add(1)),
					Namespace: consumer.Namespace,
				},
				Spec: quotainternal.QuotaClaimSpec{
					ConsumerRef: consumer,
					Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
				},
			}
			if _, err := alloc.Allocate(ctx, claim); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportThroughput(b)
	report()
}

// BenchmarkAllocateRelease measures the load-test shape: every operation
// creates a claim and then deletes it, so half the writes are releases.
func BenchmarkAllocateRelease(b *testing.B) {
	b.Run("distinct-buckets", func(b *testing.B) { benchmarkAllocateRelease(b, false) })
	b.Run("same-bucket", func(b *testing.B) { benchmarkAllocateRelease(b, true) })
}

func benchmarkAllocateRelease(b *testing.B, shared bool) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })
	db.SetMaxOpenConns(64)

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	writers := int64(benchParallelism * runtime.GOMAXPROCS(0))
	consumers := seedBenchBuckets(b, db, shared, writers)
	report := startBatchStats(b, alloc)

	var writerSeq, claimSeq atomic.Int64
	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		consumer := consumers[(writerSeq.Add(1)-1)%int64(len(consumers))]
		for pb.Next() {
			out, err := alloc.Allocate(ctx, benchClaim(consumer, claimSeq.Add(1)))
			if err != nil {
				b.Error(err)
				return
			}
			if err := alloc.Release(ctx, out); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportThroughput(b)
	report()
}

// BenchmarkAllocateSingleWriter is the idle-path latency check: one writer,
// no concurrency, so any batching can only add overhead here. ns/op is the
// per-request latency. "allocate+release" mirrors the load-test shape.
func BenchmarkAllocateSingleWriter(b *testing.B) {
	b.Run("allocate", func(b *testing.B) { benchmarkSingleWriter(b, false) })
	b.Run("allocate+release", func(b *testing.B) { benchmarkSingleWriter(b, true) })
}

func benchmarkSingleWriter(b *testing.B, release bool) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	consumer := seedBenchBuckets(b, db, true, 1)[0]
	report := startBatchStats(b, alloc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := alloc.Allocate(ctx, benchClaim(consumer, int64(i)))
		if err != nil {
			b.Fatal(err)
		}
		if release {
			if err := alloc.Release(ctx, out); err != nil {
				b.Fatal(err)
			}
		}
	}
	reportThroughput(b)
	report()
}

func benchConsumer(i int64) quotainternal.ConsumerRef {
	return quotainternal.ConsumerRef{
		APIGroup:  "example.com",
		Kind:      "Project",
		Name:      fmt.Sprintf("bench-%d", i),
		Namespace: fmt.Sprintf("bench-tenant-%d", i),
	}
}

// seedBenchBuckets seeds one effectively unlimited "cpu" bucket per writer
// (or a single shared one) and returns the consumers to use.
func seedBenchBuckets(b *testing.B, db *sql.DB, shared bool, writers int64) []quotainternal.ConsumerRef {
	b.Helper()
	const limit = int64(1) << 40
	if shared {
		writers = 1
	}
	consumers := make([]quotainternal.ConsumerRef, writers)
	for i := range consumers {
		consumers[i] = benchConsumer(int64(i))
		seedBucket(b, db, consumers[i], "cpu", limit)
	}
	vacuumForBench(b, db)
	return consumers
}

// benchFillerRows is how many unrelated rows vacuumForBench seeds.
const benchFillerRows = 10000

// vacuumForBench makes every benchmark start from the same, realistic table
// state. It seeds benchFillerRows unrelated objects so quota_objects is not
// a handful of rows (a production table holds every quota object), then
// vacuums and analyzes. Without this the planner's statistics describe a
// near-empty table, and the cached generic plan for `WHERE key = $1 ...
// FOR UPDATE` becomes a sequential scan that slows down by an order of
// magnitude as create/delete churn bloats the table -- which made results
// depend on when autovacuum last ran. Cleanup removes the filler rows with
// everything else.
func vacuumForBench(b *testing.B, db *sql.DB) {
	b.Helper()
	if _, err := db.Exec(`INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data)
		SELECT '/bench-filler/' || g, 1, 'BenchFiller', 'bench-filler', 'filler-' || g, convert_to('{"kind":"BenchFiller"}', 'UTF8')
		FROM generate_series(1, $1) AS g`, benchFillerRows); err != nil {
		b.Fatalf("seed filler rows: %v", err)
	}
	if _, err := db.Exec(`VACUUM (ANALYZE, PARALLEL 0) quota_objects, quota_changelog, quota_rv_state`); err != nil {
		b.Fatalf("vacuum: %v", err)
	}
}

func benchClaim(consumer quotainternal.ConsumerRef, n int64) *quotainternal.QuotaClaim {
	return &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("bench-claim-%d", n),
			Namespace: consumer.Namespace,
		},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
		},
	}
}
