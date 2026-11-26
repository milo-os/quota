package postgres

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// sessionParams are set on every pooled service connection unless the DSN
// sets them. Every statement the service runs is a short indexed query;
// parallel query only adds worker startup to it (the watch drain showed
// IPC:MessageQueueInternal and IPC:BgWorkerShutdown waits under load).
var sessionParams = map[string]string{
	"max_parallel_workers_per_gather": "0",
}

func applySessionParams(cfg *pgx.ConnConfig) {
	for k, v := range sessionParams {
		if _, ok := cfg.RuntimeParams[k]; !ok {
			cfg.RuntimeParams[k] = v
		}
	}
}

// openDB opens a database/sql pool on dsn with sessionParams applied.
func openDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	applySessionParams(cfg)
	return stdlib.OpenDB(*cfg), nil
}
