package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NativePool is the pgx-native pool used by the postgres-native REST layer
// for QuotaClaim and by the synchronous allocator. It is separate from the
// *sql.DB pool of the standard Store path so its prepared-statement cache
// (StatementCacheModePrepare, the pgx/v5 default) stays warm for the hot
// claim statements.
//
// Claim watches do not hold connections from it: they share the watch
// hub's single LISTEN connection for wake-ups and check a connection out
// only for each changelog read.
type NativePool struct {
	// Query is the pool used for GET / LIST / UPDATE / WATCH reads and the
	// allocator. Connections are checked out per statement or transaction.
	Query *pgxpool.Pool
}

// NewNativePool constructs the native pgx pool from a DSN. If dsn is empty
// the function returns an error; the native layer requires a live pool.
func NewNativePool(ctx context.Context, dsn string) (*NativePool, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres: native pool requires a non-empty DSN")
	}

	queryCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse query pool config: %w", err)
	}
	applySessionParams(queryCfg.ConnConfig)
	queryPool, err := pgxpool.NewWithConfig(ctx, queryCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: open query pool: %w", err)
	}
	if err := queryPool.Ping(ctx); err != nil {
		queryPool.Close()
		return nil, fmt.Errorf("postgres: ping query pool: %w", err)
	}
	return &NativePool{Query: queryPool}, nil
}

// Close releases the pool. Safe to call multiple times.
func (p *NativePool) Close() {
	if p == nil {
		return
	}
	if p.Query != nil {
		p.Query.Close()
	}
}
