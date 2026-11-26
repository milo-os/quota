// Package pgtest holds helpers shared by the Postgres-backed tests.
//
// Postgres tests read the database from POSTGRES_DSN. Without it they skip,
// so `go test ./...` works on a laptop with no database. CI sets
// QUOTA_REQUIRE_POSTGRES=1 as well, which turns a missing POSTGRES_DSN into
// a test failure: a CI job that silently skipped every Postgres test would
// otherwise pass.
package pgtest

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // Postgres driver for OpenDB
)

const (
	// DSNEnv names the variable holding the test database DSN.
	DSNEnv = "POSTGRES_DSN"
	// RequireEnv, when "1" or "true", makes a missing DSN fail the test.
	RequireEnv = "QUOTA_REQUIRE_POSTGRES"

	// migrationLockKey serializes schema application across test packages
	// that share one database.
	migrationLockKey = 42424242
)

// DSN returns the test database DSN. Without one it skips the test, or
// fails it when RequireEnv is set.
func DSN(tb testing.TB) string {
	tb.Helper()
	dsn := os.Getenv(DSNEnv)
	if dsn != "" {
		return dsn
	}
	if Required() {
		tb.Fatalf("%s is not set but %s=%s: Postgres tests must run", DSNEnv, RequireEnv, os.Getenv(RequireEnv))
	}
	tb.Skipf("%s not set, skipping Postgres integration test", DSNEnv)
	return ""
}

// Required reports whether RequireEnv asks for Postgres tests to run.
func Required() bool {
	switch os.Getenv(RequireEnv) {
	case "1", "true", "TRUE", "True":
		return true
	}
	return false
}

// MigrationFiles returns the repository's migration files in apply order.
// It finds the migrations directory by walking up from the working
// directory to the module root.
func MigrationFiles(tb testing.TB) []string {
	tb.Helper()
	dir, err := os.Getwd()
	if err != nil {
		tb.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			tb.Fatalf("go.mod not found above the working directory")
		}
		dir = parent
	}
	files, err := filepath.Glob(filepath.Join(dir, "migrations", "*.sql"))
	if err != nil || len(files) == 0 {
		tb.Fatalf("no migrations found under %s: %v", dir, err)
	}
	sort.Strings(files)
	return files
}

// ApplyMigrations applies every migration file to db, under a session
// advisory lock so test packages sharing one database do not race on
// CREATE OR REPLACE.
func ApplyMigrations(tb testing.TB, db *sql.DB) {
	tb.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		tb.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		tb.Fatalf("lock: %v", err)
	}
	defer func() { _, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey) }()
	for _, f := range MigrationFiles(tb) {
		body, err := os.ReadFile(f)
		if err != nil {
			tb.Fatalf("read %s: %v", f, err)
		}
		if _, err := conn.ExecContext(ctx, string(body)); err != nil {
			tb.Fatalf("apply %s: %v", f, err)
		}
	}
}

// OpenDB opens the test database (skipping or failing per DSN) and applies
// the migrations. The connection is closed when the test ends.
func OpenDB(tb testing.TB) *sql.DB {
	tb.Helper()
	dsn := DSN(tb)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		tb.Fatalf("open postgres: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	ApplyMigrations(tb, db)
	return db
}
