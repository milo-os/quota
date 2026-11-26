package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// ChangelogBackfillCTE copies an object's current data into its current
// changelog row when that row was written without data (the ADDED row of a
// claim the allocator created). Every UPDATE of quota_objects runs it, so the row
// stays readable once the object has moved past that version: the reader
// can only rebuild an elided row from the live object or from a DELETED
// successor, never from a MODIFIED one.
//
// It expects a MATERIALIZED CTE named tgt with the locked row's key and
// data, and the locked resource version in $2. Make the quota_next_rv() CTE
// read from it ("FROM tgt, (SELECT count(*) FROM backfill) bf") so it runs
// before the commit-order lock is taken: it may wait on the compactor's row
// lock, which is allowed only before the lock (writer contract in
// migrations/README.md). It matches nothing, at the cost of one index probe, for rows that
// already have data.
const ChangelogBackfillCTE = `backfill AS (
	   UPDATE quota_changelog l SET data = tgt.data FROM tgt
	    WHERE l.resource_version = $2 AND l.key = tgt.key AND l.data IS NULL
	   RETURNING 1
	 )`

// decode decodes data into an object and sets its resource version.
func decode(codec runtime.Codec, data []byte, into runtime.Object, rv int64) error {
	_, _, err := codec.Decode(data, nil, into)
	if err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to decode object: %w", err))
	}
	versioner := storage.APIObjectVersioner{}
	if err := versioner.UpdateObject(into, uint64(rv)); err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to set resource version: %w", err))
	}
	return nil
}

// decodeToObject decodes data into a new runtime.Object.
func decodeToObject(codec runtime.Codec, data []byte) (runtime.Object, error) {
	obj, _, err := codec.Decode(data, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decode object: %w", err)
	}
	return obj, nil
}

// extractMetadata extracts kind, namespace, and name from a runtime.Object.
func extractMetadata(obj runtime.Object) (kind, namespace, name string) {
	accessor, err := meta.Accessor(obj)
	if err == nil {
		namespace = accessor.GetNamespace()
		name = accessor.GetName()
	}
	if gvk := obj.GetObjectKind().GroupVersionKind(); gvk.Kind != "" {
		kind = gvk.Kind
	}
	return kind, namespace, name
}

// isUniqueViolation checks if the error is a Postgres unique constraint violation.
func isUniqueViolation(err error) bool {
	// SQLSTATE 23505 = unique_violation. Prefer the typed pgx error path and
	// fall back to string matching so the check remains robust even if the
	// error arrives wrapped in a context we don't control.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
		strings.Contains(err.Error(), "23505")
}

// checkPreconditions validates storage preconditions against the existing object.
func checkPreconditions(key string, preconditions *storage.Preconditions, existing runtime.Object) error {
	if preconditions == nil {
		return nil
	}
	return preconditions.Check(key, existing)
}

// predicateEmpty reports whether a predicate filters nothing. Unlike
// SelectionPredicate.Empty it accepts a zero-value predicate (nil Label and
// Field), which direct callers pass when they mean "no selector".
func predicateEmpty(predicate storage.SelectionPredicate) bool {
	return (predicate.Label == nil || predicate.Label.Empty()) &&
		(predicate.Field == nil || predicate.Field.Empty())
}

// matchesPredicate checks if an object matches a storage selection predicate.
func matchesPredicate(obj runtime.Object, predicate storage.SelectionPredicate) bool {
	if predicateEmpty(predicate) {
		return true
	}
	matches, err := predicate.Matches(obj)
	if err != nil {
		return false
	}
	return matches
}
