package postgres

import (
	"context"
	"testing"

	"go.miloapis.com/quota/internal/testutil/pgtest"
)

// TestSessionParams checks that both service pools run with parallel query
// off.
func TestSessionParams(t *testing.T) {
	dsn := pgtest.DSN(t)
	ctx := context.Background()

	db, err := openDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRowContext(ctx, `SHOW max_parallel_workers_per_gather`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "0" {
		t.Errorf("database/sql pool: max_parallel_workers_per_gather = %s, want 0", v)
	}

	pool, err := NewNativePool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Query.QueryRow(ctx, `SHOW max_parallel_workers_per_gather`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "0" {
		t.Errorf("native pool: max_parallel_workers_per_gather = %s, want 0", v)
	}
}
