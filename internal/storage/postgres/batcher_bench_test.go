package postgres

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func init() {
	batchStats = func(b *testing.B, alloc *PostgresAllocator) func() {
		if alloc.batcher == nil {
			return func() {}
		}
		var mu sync.Mutex
		var batches, ops, largest int
		alloc.batcher.onBatch = func(n int) {
			mu.Lock()
			batches++
			ops += n
			largest = max(largest, n)
			mu.Unlock()
		}
		return func() {
			mu.Lock()
			defer mu.Unlock()
			if batches > 0 {
				b.ReportMetric(float64(ops)/float64(batches), "ops/batch")
				b.ReportMetric(float64(largest), "max-batch")
			}
		}
	}
}

// BenchmarkAllocateBatchSize sweeps the group-commit cap on the load-test
// shape (allocate then release, 8 writers per CPU) and reports the mean and
// largest batch observed. max-1 is the unbatched path.
//
//	go test -run '^$' -bench BenchmarkAllocateBatchSize -cpu 4,16 -benchtime 4s ./internal/storage/postgres/
func BenchmarkAllocateBatchSize(b *testing.B) {
	for _, shared := range []bool{false, true} {
		name := "distinct-buckets"
		if shared {
			name = "same-bucket"
		}
		for _, maxBatch := range []int{1, 8, 16, 32, 64, 128} {
			b.Run(fmt.Sprintf("%s/max-%d", name, maxBatch), func(b *testing.B) {
				benchmarkBatchSize(b, shared, maxBatch)
			})
		}
	}
}

func benchmarkBatchSize(b *testing.B, shared bool, maxBatch int) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })
	db.SetMaxOpenConns(64)

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(maxBatch))
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
