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

// BenchmarkChangelogFootprint runs the load-test shape (create a claim, then
// delete it) with claims shaped like the ones the enforcement plugin creates,
// and reports how much quota_changelog grows per claim. The changelog is
// truncated first so relation sizes measure only this run.
//
//	go test -run '^$' -bench ChangelogFootprint -benchtime 20000x ./internal/storage/postgres/
//
// Reported metrics, all per claim (one create and one delete):
//
//	changelog-B/claim  heap + TOAST + every index
//	heap-B/claim       heap and TOAST only
//	index-B/claim      all indexes
//	rows/claim         changelog rows written
//	data-B/claim       stored bytes of the data column (after compression)
func BenchmarkChangelogFootprint(b *testing.B) {
	db := getTestDB(b)
	cleanupTestData(b, db)
	b.Cleanup(func() { cleanupTestData(b, db) })
	db.SetMaxOpenConns(64)
	if _, err := db.Exec(`TRUNCATE quota_changelog`); err != nil {
		b.Fatalf("truncate changelog: %v", err)
	}

	ctx := context.Background()
	alloc := NewAllocator(db, allocatorTestCodec)
	writers := int64(benchParallelism * runtime.GOMAXPROCS(0))
	consumers := seedBenchBuckets(b, db, false, writers)

	var before footprint
	before.read(b, db)

	var writerSeq, claimSeq atomic.Int64
	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		consumer := consumers[(writerSeq.Add(1)-1)%int64(len(consumers))]
		for pb.Next() {
			out, err := alloc.Allocate(ctx, enforcementShapedClaim(consumer, claimSeq.Add(1)))
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
	b.StopTimer()
	reportThroughput(b)

	var after footprint
	after.read(b, db)
	n := float64(b.N)
	b.ReportMetric(float64(after.heap+after.index-before.heap-before.index)/n, "changelog-B/claim")
	b.ReportMetric(float64(after.heap-before.heap)/n, "heap-B/claim")
	b.ReportMetric(float64(after.index-before.index)/n, "index-B/claim")
	b.ReportMetric(float64(after.rows-before.rows)/n, "rows/claim")
	b.ReportMetric(float64(after.data-before.data)/n, "data-B/claim")
	logChangelogComposition(b, db)
}

// logChangelogComposition logs, per event type and resource, how many rows
// were written, the average stored size of the data column, the average
// whole-row size, and how many values are compressed or elided. Run with -v.
func logChangelogComposition(b *testing.B, db *sql.DB) {
	b.Helper()
	rows, err := db.Query(`SELECT event_type, split_part(key, '/', 3), count(*),
		       COALESCE(avg(pg_column_size(data)), 0)::int, avg(pg_column_size(c.*))::int,
		       count(*) FILTER (WHERE pg_column_compression(data) IS NOT NULL),
		       count(*) FILTER (WHERE data IS NULL)
		  FROM quota_changelog c GROUP BY 1, 2 ORDER BY 1, 2`)
	if err != nil {
		b.Fatalf("changelog composition: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var event, resource string
		var count, dataSize, rowSize, compressed, elided int64
		if err := rows.Scan(&event, &resource, &count, &dataSize, &rowSize, &compressed, &elided); err != nil {
			b.Fatalf("scan composition: %v", err)
		}
		b.Logf("%-8s %-14s rows=%d avg-data-B=%d avg-row-B=%d compressed=%d elided=%d",
			event, resource, count, dataSize, rowSize, compressed, elided)
	}
	if err := rows.Err(); err != nil {
		b.Fatalf("composition rows: %v", err)
	}

	// Rows inserted with a lower resource version than the row inserted
	// just before them land in the middle of the rv-leading indexes, where
	// a page split leaves both halves half full.
	var outOfOrder float64
	if err := db.QueryRow(`SELECT COALESCE(avg((resource_version < prev)::int), 0) FROM (
		  SELECT resource_version, lag(resource_version) OVER (ORDER BY id) AS prev FROM quota_changelog) s`,
	).Scan(&outOfOrder); err != nil {
		b.Fatalf("changelog insert order: %v", err)
	}
	b.Logf("rows inserted below the previous row's resource version: %.1f%%", 100*outOfOrder)

	idx, err := db.Query(`SELECT i.indexrelid::regclass::text, pg_relation_size(i.indexrelid),
		       pg_relation_size(i.indexrelid)::float8 / GREATEST((SELECT count(*) FROM quota_changelog), 1)
		  FROM pg_index i WHERE i.indrelid = 'quota_changelog'::regclass ORDER BY 1`)
	if err != nil {
		b.Fatalf("changelog indexes: %v", err)
	}
	defer func() { _ = idx.Close() }()
	for idx.Next() {
		var name string
		var size int64
		var perRow float64
		if err := idx.Scan(&name, &size, &perRow); err != nil {
			b.Fatalf("scan index size: %v", err)
		}
		b.Logf("index %-34s %8d B  %6.1f B/row", name, size, perRow)
	}
	if err := idx.Err(); err != nil {
		b.Fatalf("index rows: %v", err)
	}
}

type footprint struct {
	heap, index, rows, data int64
}

// read samples the changelog's relation sizes, row count and the stored
// size of its data column.
func (f *footprint) read(b *testing.B, db *sql.DB) {
	b.Helper()
	if err := db.QueryRow(`SELECT pg_table_size('quota_changelog'), pg_indexes_size('quota_changelog'),
		(SELECT count(*) FROM quota_changelog), (SELECT COALESCE(sum(pg_column_size(data)), 0) FROM quota_changelog)`,
	).Scan(&f.heap, &f.index, &f.rows, &f.data); err != nil {
		b.Fatalf("read changelog footprint: %v", err)
	}
}

// enforcementShapedClaim builds a claim with the metadata the enforcement
// plugin sets (pkg/admission/enforcement/plugin.go): three labels, four
// annotations and a namespaced resourceRef. The allocator adds the status.
func enforcementShapedClaim(consumer quotainternal.ConsumerRef, n int64) *quotainternal.QuotaClaim {
	resource := fmt.Sprintf("load-test-%06d", n)
	return &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("project-%s-4f9a2c", resource),
			Namespace: consumer.Namespace,
			Labels: map[string]string{
				"quota.miloapis.com/auto-created": "true",
				"quota.miloapis.com/policy":       "projects-per-organization",
				"quota.miloapis.com/gvk":          "resourcemanager.miloapis.com.v1alpha1.Project",
			},
			Annotations: map[string]string{
				"quota.miloapis.com/created-by":    "claim-creation-plugin",
				"quota.miloapis.com/created-at":    "2026-11-22T18:04:11Z",
				"quota.miloapis.com/resource-name": resource,
				"quota.miloapis.com/policy":        "projects-per-organization",
			},
		},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: consumer,
			ResourceRef: quotainternal.UnversionedObjectReference{
				APIGroup:  "resourcemanager.miloapis.com",
				Kind:      "Project",
				Name:      resource,
				Namespace: consumer.Namespace,
			},
			Requests: []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
		},
	}
}
