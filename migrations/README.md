# Quota Service Migrations

SQL migrations for the Postgres storage backend. They are needed only with `--quota-storage-backend=postgres` or `dual`. PostgreSQL 13 or later is required (the commit-order trigger uses `pg_current_xact_id()`).

## Migrations

| File | Purpose |
|------|---------|
| `001_initial_schema.sql` | The whole schema: `quota_objects`, `quota_changelog`, the resource-version sequence, commit-order enforcement, watch notifications and the `schema_migrations` tracking table. The file's comments explain each design choice |

Migrations 001 to 009 were folded into 001 on 2026-11-24, before any production deployment. A database created from the earlier files must be recreated.

## Naming Convention

Files follow the pattern `{NNN}_{description}.sql` where `NNN` is a zero-padded
three-digit version number.

## Running Migrations

### Locally

`migrate.sh` reads `MIGRATIONS_DIR`, which defaults to `/migrations` (the path in a container). Point it at this directory. It connects with `POSTGRES_DSN` if set, otherwise with the `PG*` variables:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=quota
export PGPASSWORD=quota
export PGDATABASE=quota
export MIGRATIONS_DIR=./migrations
./migrations/migrate.sh
```

`migrate.sh` records applied versions, names and checksums in `schema_migrations`. On a later run it compares each applied migration's checksum with the file and fails if they differ. Before release, schema changes are made by editing migrations in place, so a mismatch means the database was built from an older copy of the file: recreate it (drop the database or its schema) and run `migrate.sh` again.

### In Kubernetes

A Job in `config/components/postgres-migrations/` runs `migrate.sh` against the database with the DSN from the `postgres-credentials` Secret. It mounts `migrate.sh` and the SQL files from a ConfigMap that `hack/update-migrations-configmap.sh` generates from this directory, so the Job runs exactly these files.

## Adding New Migrations

1. Create a new file: `migrations/{NNN}_{description}.sql`
2. Use `IF NOT EXISTS` / `IF EXISTS` guards where possible so migrations are
   idempotent. The Go tests re-apply every file before each package runs.
3. Run `hack/update-migrations-configmap.sh` (or `task generate`). CI fails
   if the ConfigMap is out of date.

## Writer Contract

Resource versions must be assigned in commit order. Every writer of
`quota_objects` / `quota_changelog` must:

1. Take all row locks first (`SELECT ... FOR UPDATE`, sorted key order).
2. Draw resource versions with `quota_next_rv()`, not
   `nextval('quota_resource_version_seq')`. The first call takes a
   transaction-scoped advisory lock that is held until COMMIT.
3. After that call, wait on no other row lock; commit promptly.
4. Before an UPDATE of `quota_objects`, copy the object's current data into
   its latest changelog row if that row has none (`ChangelogBackfillCTE`),
   before calling `quota_next_rv()`. The allocator writes a claim's ADDED
   row without data, and readers can rebuild it only from the live object
   or from a DELETED successor.
5. Look up changelog rows by key with `hashtext(key) = hashtext($k) AND
   key = $k` so `idx_quota_changelog_key` is used.

A statement-level AFTER INSERT trigger on `quota_changelog` rejects any RV
drawn before the lock with SQLSTATE 40001. The current RV is
`quota_rv_state.committed_rv` and the changelog compaction floor is
`quota_changelog_compaction.compacted_rv`.

The lock is global, so it serializes the window from the first `quota_next_rv()` call to commit across all writers. See [docs/storage-backends.md](../docs/storage-backends.md#throughput-trade-off) for the cost.
