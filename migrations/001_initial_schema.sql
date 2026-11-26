-- Schema for the Postgres storage backend.
--
-- quota_objects holds one row per object, keyed by its storage path
-- (/<group>/<resource>/[<namespace>/]<name>). quota_changelog holds one row
-- per change and feeds watches. Resource versions come from
-- quota_resource_version_seq and are assigned in commit order (see the
-- writer contract in migrations/README.md and "Commit order" below).
--
-- Idempotent: every statement can be re-applied to a database that already
-- has this schema. The Go tests re-apply it before each package runs.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- convert_from is only STABLE, so a generated column cannot call it
-- directly. The input is always UTF-8 JSON written by this service, which
-- makes the conversion immutable in practice.
CREATE OR REPLACE FUNCTION quota_bytea_to_jsonb(b bytea)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT convert_from(b, 'UTF8')::jsonb
$$;

-- Objects.
--
-- key uses the "C" collation: keys are compared and range-scanned bytewise
-- (prefix lists, continue tokens), and the database default collation
-- (en_US.utf8 in the postgres image) is several times slower per
-- comparison.
--
-- data_json is the parsed object. The allocator reads and patches bucket
-- status through it, and the native QuotaClaim list filters on it.
--
-- The claim_* columns copy the claim fields that have indexes. They are
-- NULL for every other kind, and a claim's spec is immutable, so no update
-- changes them. For a claim an absent field is '' rather than NULL: the
-- fields are omitempty in JSON, and field selectors treat a missing field
-- as the empty string, so "spec.resourceRef.kind!=Foo" must match a claim
-- without a resourceRef and "spec.resourceRef.kind=" must match it too.
-- kind is written once, when the row is inserted, and never updated. No index references data_json or data, so an update that
-- leaves key, kind, namespace and name alone (every bucket status write)
-- can be a heap-only tuple (HOT) update and skips every index. fillfactor
-- leaves room on each page for that.
CREATE TABLE IF NOT EXISTS quota_objects (
    key TEXT COLLATE "C" PRIMARY KEY,
    resource_version BIGINT NOT NULL DEFAULT 0,
    kind TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_json JSONB GENERATED ALWAYS AS (quota_bytea_to_jsonb(data)) STORED,
    claim_consumer_kind TEXT GENERATED ALWAYS AS (
        CASE WHEN kind = 'QuotaClaim' THEN COALESCE(quota_bytea_to_jsonb(data)->'spec'->'consumerRef'->>'kind', '') END) STORED,
    claim_consumer_name TEXT GENERATED ALWAYS AS (
        CASE WHEN kind = 'QuotaClaim' THEN COALESCE(quota_bytea_to_jsonb(data)->'spec'->'consumerRef'->>'name', '') END) STORED,
    claim_resource_kind TEXT GENERATED ALWAYS AS (
        CASE WHEN kind = 'QuotaClaim' THEN COALESCE(quota_bytea_to_jsonb(data)->'spec'->'resourceRef'->>'kind', '') END) STORED,
    claim_resource_name TEXT GENERATED ALWAYS AS (
        CASE WHEN kind = 'QuotaClaim' THEN COALESCE(quota_bytea_to_jsonb(data)->'spec'->'resourceRef'->>'name', '') END) STORED
) WITH (fillfactor = 70);

CREATE INDEX IF NOT EXISTS idx_quota_objects_kind_namespace ON quota_objects (kind, namespace);

CREATE INDEX IF NOT EXISTS idx_qc_namespace_name
    ON quota_objects (namespace, name)
    WHERE kind = 'QuotaClaim';

CREATE INDEX IF NOT EXISTS idx_qc_consumer_kind_name
    ON quota_objects (claim_consumer_kind, claim_consumer_name)
    WHERE kind = 'QuotaClaim';

CREATE INDEX IF NOT EXISTS idx_qc_resource_ref
    ON quota_objects (claim_resource_kind, claim_resource_name)
    WHERE kind = 'QuotaClaim';

-- Changelog.
--
-- data is the object after the change; for DELETED, the object before it.
-- On an ADDED row for a claim the allocator created, data is NULL: the row
-- would repeat exactly the bytes written to quota_objects in the same
-- statement. Readers rebuild it, in the row's snapshot, from quota_objects
-- while the claim still has that resource version, or else from the
-- claim's DELETED row, which carries the same last state. A writer that
-- updates an object first copies its current data into the object's
-- latest changelog row when that row has none (ChangelogBackfillCTE in
-- internal/storage/postgres/codec.go), so the successor of an elided row is
-- always a DELETED row. A reader that finds neither source ends the watch
-- with 410 Gone.
--
-- idx_quota_changelog_key serves only equality lookups (keyChangedSince,
-- single-key watches, the successor lookup above), which say
-- hashtext(key) = hashtext($k) AND key = $k. An int4 index entry is
-- smaller and cheaper to insert than the key.
CREATE SEQUENCE IF NOT EXISTS quota_resource_version_seq;

CREATE TABLE IF NOT EXISTS quota_changelog (
    id BIGSERIAL PRIMARY KEY,
    key TEXT COLLATE "C" NOT NULL,
    resource_version BIGINT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('ADDED', 'MODIFIED', 'DELETED')),
    data BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quota_changelog_rv_key ON quota_changelog (resource_version, key);
CREATE INDEX IF NOT EXISTS idx_quota_changelog_rv_id ON quota_changelog (resource_version, id);
CREATE INDEX IF NOT EXISTS idx_quota_changelog_key ON quota_changelog (hashtext(key));
CREATE INDEX IF NOT EXISTS idx_quota_changelog_created_at ON quota_changelog (created_at);

-- Commit order.
--
-- A watch resumes from a resource version, so a version must never become
-- visible after a higher one. Writers draw versions with quota_next_rv(),
-- which takes a transaction-scoped advisory lock first; the lock is held
-- until COMMIT, so versions commit in the order they were drawn. The
-- trigger below rejects, with SQLSTATE 40001, any changelog row whose
-- version is not above what other transactions have committed (a version
-- drawn before the lock). quota_rv_state.committed_rv is the current
-- resource version.
CREATE TABLE IF NOT EXISTS quota_rv_state (
    singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    committed_rv BIGINT  NOT NULL,
    writer_xid   BIGINT  NOT NULL DEFAULT 0,
    writer_floor BIGINT  NOT NULL DEFAULT 0
);

INSERT INTO quota_rv_state (singleton, committed_rv)
VALUES (TRUE, 1)
ON CONFLICT (singleton) DO NOTHING;

-- The first nextval must be above committed_rv.
SELECT setval('quota_resource_version_seq', (SELECT committed_rv FROM quota_rv_state WHERE singleton), true)
 WHERE (SELECT last_value + CASE WHEN is_called THEN 1 ELSE 0 END FROM quota_resource_version_seq)
    <= (SELECT committed_rv FROM quota_rv_state WHERE singleton);

-- Watches older than compacted_rv get 410 Gone.
CREATE TABLE IF NOT EXISTS quota_changelog_compaction (
    singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    compacted_rv BIGINT  NOT NULL DEFAULT 0,
    compacted_at TIMESTAMPTZ
);

INSERT INTO quota_changelog_compaction (singleton, compacted_rv, compacted_at)
SELECT TRUE, committed_rv, NOW() FROM quota_rv_state WHERE singleton
ON CONFLICT (singleton) DO NOTHING;

-- The advisory lock key must be the same in these three functions.
CREATE OR REPLACE FUNCTION quota_rv_lock() RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7234018233453215744);
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION quota_next_rv() RETURNS bigint AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7234018233453215744);
    RETURN nextval('quota_resource_version_seq');
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Runs once per statement over the statement's new rows. When a statement
-- has several INSERTs into quota_changelog (the allocator CTEs have up to
-- three), PostgreSQL captures them in one transition table and fires the
-- trigger once. writer_xid and writer_floor let a transaction that writes
-- changelog rows in several statements be checked against other
-- transactions only.
CREATE OR REPLACE FUNCTION quota_changelog_enforce_commit_order() RETURNS trigger AS $$
DECLARE
    my_xid    BIGINT := pg_current_xact_id()::text::bigint;
    lo        BIGINT;
    hi        BIGINT;
    cur_rv    BIGINT;
    cur_xid   BIGINT;
    cur_floor BIGINT;
    bad_key   TEXT;
BEGIN
    SELECT min(resource_version), max(resource_version)
      INTO lo, hi
      FROM quota_changelog_new_rows;
    IF lo IS NULL THEN
        -- INSERT ... SELECT that matched nothing.
        RETURN NULL;
    END IF;

    -- Re-entrant: free if quota_next_rv() already took it in this
    -- transaction. For a non-compliant writer this is the late acquisition
    -- that the floor check below guards.
    PERFORM pg_advisory_xact_lock(7234018233453215744);

    SELECT committed_rv, writer_xid, writer_floor
      INTO cur_rv, cur_xid, cur_floor
      FROM quota_rv_state
     WHERE singleton;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'quota_rv_state is empty; re-apply migrations/001_initial_schema.sql';
    END IF;

    IF cur_xid IS DISTINCT FROM my_xid THEN
        -- First changelog statement of this transaction. Everything at or
        -- below cur_rv was committed by other transactions.
        cur_floor := cur_rv;
    END IF;

    IF lo <= cur_floor THEN
        SELECT key INTO bad_key
          FROM quota_changelog_new_rows
         WHERE resource_version = lo
         LIMIT 1;
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'quota: resource_version %s for key %s is not above the committed high-water mark %s; '
                'it was drawn before the commit-order lock',
                lo, bad_key, cur_floor),
            HINT = 'Draw resource versions with quota_next_rv() after taking row locks, then retry.';
    END IF;

    UPDATE quota_rv_state
       SET committed_rv = GREATEST(cur_rv, hi),
           writer_xid   = my_xid,
           writer_floor = cur_floor
     WHERE singleton;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS quota_changelog_commit_order ON quota_changelog;
CREATE TRIGGER quota_changelog_commit_order
    AFTER INSERT ON quota_changelog
    REFERENCING NEW TABLE AS quota_changelog_new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION quota_changelog_enforce_commit_order();

-- Watch wake-ups: at most one notification per statement, delivered at
-- COMMIT. One LISTEN connection per apiserver (the watch hub in
-- internal/watch/postgres.go) receives it and wakes every watch, including
-- the native QuotaClaim watches, which then read quota_changelog.
CREATE OR REPLACE FUNCTION quota_notify_changelog() RETURNS trigger AS $$
DECLARE
    hi BIGINT;
BEGIN
    SELECT max(resource_version) INTO hi FROM quota_changelog_new_rows;
    IF hi IS NOT NULL THEN
        PERFORM pg_notify('quota_changes', hi::text);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS quota_changelog_notify ON quota_changelog;
CREATE TRIGGER quota_changelog_notify
    AFTER INSERT ON quota_changelog
    REFERENCING NEW TABLE AS quota_changelog_new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION quota_notify_changelog();
