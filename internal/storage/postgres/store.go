// Package postgres implements k8s.io/apiserver/pkg/storage.Interface backed by PostgreSQL.
//
// Objects are stored in a quota_objects table as JSON-encoded byte arrays.
// Watch support is provided via a quota_changelog table that records all mutations.
//
// # Resource versions
//
// Every write draws its resource version with quota_next_rv() (migration
// 006), which takes a transaction-scoped advisory lock held until COMMIT.
// Because PostgreSQL releases that lock only after the commit is visible,
// RVs are assigned in commit order and every snapshot sees an exact RV
// prefix. The current RV is quota_rv_state.committed_rv, a high-water mark
// that changelog cleanup never deletes, so it never goes backwards.
//
// Writes follow the etcd3 store's optimistic pattern: read without locks,
// run caller logic (preconditions, tryUpdate, validateDeletion) in Go, then
// apply the change in ONE autocommit statement conditioned on the RV that
// was read. That statement locks the target row first and only then calls
// quota_next_rv(), which keeps the commit-order lock window to server-side
// execution of one statement plus its commit and respects the lock-order
// rule (row locks before the commit-order lock). If the row changed, the
// statement affects nothing and the operation retries from a fresh read.
package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // Postgres driver (pgx)
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"

	pgwatch "go.miloapis.com/quota/internal/watch"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

const (
	// maxStorageRetries bounds retries of storage-level races (the row
	// changed between the read and the conditional write, or Postgres
	// reported a serialization failure or deadlock). Caller errors are never
	// retried.
	maxStorageRetries = 50
	// maxListBatch caps the per-query row count of a paginated list when
	// predicate filtering forces extra fetches. Mirrors etcd3's maxLimit.
	maxListBatch = 10000

	// Messages copied from k8s.io/apiserver/pkg/storage/etcd3/errors.go so
	// clients see the same text from either backend.
	listExpiredMsg       = "The resourceVersion for the provided list is too old."
	continueExpiredMsg   = "The provided continue parameter is too old to display a consistent list result. You can start a new list without the continue parameter."
	inconsistentContinue = "The provided continue parameter is too old to display a consistent list result. You can start a new list without the continue parameter, or use the continue token in this response to retrieve the remainder of the results. Continuing with the provided token results in an inconsistent list - objects that were created, modified, or deleted between the time the first chunk was returned and now may show up in the list."
)

// Store implements storage.Interface against PostgreSQL.
type Store struct {
	db        *sql.DB
	codec     runtime.Codec
	versioner storage.Versioner
	// newFunc creates a zero-value object of the stored type, used for bookmark events.
	newFunc func() runtime.Object
	// kind is the kind column written for new rows: the Go type name of
	// newFunc's object. Objects reaching storage are internal versions
	// with empty TypeMeta, so the object itself cannot be trusted for it.
	// See kindFor.
	kind string
	// recomputeBucketAllocations makes a QuotaBucket insert derive
	// allocated and claimCount from the Granted claims that already hold
	// capacity in it. See SetRecomputeBucketAllocations.
	recomputeBucketAllocations bool
	// watcher provides watch functionality via the changelog table.
	watcher *pgwatch.PostgresWatcher
}

// New creates a Postgres-backed storage.Interface with its own watch hub.
// dsn is used for the watcher's dedicated LISTEN/NOTIFY connection; if it is
// empty the watcher falls back to polling only.
func New(db *sql.DB, codec runtime.Codec, dsn string) *Store {
	return NewWithWatchExclusions(db, codec, dsn, nil)
}

// NewWithWatchExclusions creates a Store with its own watch hub whose
// watcher skips keys under the given prefixes.
func NewWithWatchExclusions(db *sql.DB, codec runtime.Codec, dsn string, excludedKeyPrefixes []string) *Store {
	return &Store{
		db:        db,
		codec:     codec,
		versioner: storage.APIObjectVersioner{},
		watcher:   pgwatch.NewWithExclusions(db, codec, dsn, excludedKeyPrefixes),
	}
}

// NewWithHub creates a Store whose watcher shares hub (one LISTEN
// connection and one changelog cleaner) with every other Store on the same
// database.
func NewWithHub(db *sql.DB, codec runtime.Codec, hub *pgwatch.Hub, excludedKeyPrefixes []string) *Store {
	return &Store{
		db:        db,
		codec:     codec,
		versioner: storage.APIObjectVersioner{},
		watcher:   pgwatch.NewWithHub(hub, codec, excludedKeyPrefixes),
	}
}

// Versioner returns the storage versioner.
func (s *Store) Versioner() storage.Versioner {
	return s.versioner
}

// encodeForStorage encodes obj with its resourceVersion cleared. The
// authoritative RV is the row column, which is assigned inside the write
// statement; every reader stamps the column value onto the decoded object.
func (s *Store) encodeForStorage(obj runtime.Object) ([]byte, error) {
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return nil, fmt.Errorf("prepare object for storage: %w", err)
	}
	return runtime.Encode(s.codec, obj)
}

// Create inserts a new object. It fails if the key already exists or if the
// object already carries a resourceVersion.
func (s *Store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if err := s.validateKey(key); err != nil {
		return err
	}
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}

	data, err := s.encodeForStorage(obj)
	if err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to encode object: %w", err))
	}

	rv, data, err := s.insertObject(ctx, key, obj, data)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.NewKeyExistsError(key, 0)
		}
		return storage.NewInternalError(fmt.Errorf("failed to insert object: %w", err))
	}
	return decode(s.codec, data, out, rv)
}

// insertObject inserts obj, already encoded as data, and returns its RV and
// the bytes actually stored. A QuotaBucket goes through insertBucket when
// recomputeBucketAllocations is set; everything else is one insert.
func (s *Store) insertObject(ctx context.Context, key string, obj runtime.Object, data []byte) (int64, []byte, error) {
	_, namespace, name := extractMetadata(obj)
	if bucket, ok := obj.(*quotainternal.QuotaBucket); ok && s.recomputeBucketAllocations {
		return s.insertBucket(ctx, key, bucket, namespace, name)
	}
	rv, err := s.insert(ctx, key, s.kindFor(obj), namespace, name, data)
	return rv, data, err
}

// SetRecomputeBucketAllocations controls how QuotaBucket rows are
// inserted. When enabled (Postgres allocation mode), a new bucket's
// allocated, available and claimCount are computed from the Granted claims
// that already reserve capacity in it, in the transaction that inserts it.
//
// The synchronous allocator only adjusts allocated incrementally, and the
// controller that would rebuild it is not run in this mode. A bucket that
// is deleted and recreated (the bucket controller recreates it from the
// grants with an empty status) would otherwise start at zero while claims
// still hold capacity: new claims would over-allocate, and releasing the
// old ones would be clamped at zero.
//
// Leave it disabled where the stored status is authoritative as written,
// such as the dual-mode shadow, which mirrors the etcd primary verbatim.
func (s *Store) SetRecomputeBucketAllocations(enabled bool) {
	s.recomputeBucketAllocations = enabled
}

// lockBucketClaimsSQL locks, in key order, every claim that holds a Granted
// allocation for resource type $3 by the consumer ($1 kind, $2 name, $4
// apiGroup, $5 namespace), and returns the amount it requested for that
// resource type. These are exactly the claims Allocate charged to the
// bucket for that consumer and resource type: the bucket key is a function
// of both, and Release returns the spec amounts for granted resource types
// (planRelease). The consumer kind/name columns use idx_qc_consumer_kind_name.
//
// Under READ COMMITTED, a claim deleted by a Release that commits while this
// statement waits for its row lock is skipped, and one whose release has not
// started blocks on the lock until the bucket is committed; its release then
// sees the bucket and decrements it.
const lockBucketClaimsSQL = `SELECT c.key,
	  (SELECT LEAST(COALESCE(SUM((r->>'amount')::bigint), 0), 9223372036854775807)::bigint
	     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(c.data_json->'spec'->'requests') = 'array'
	                                    THEN c.data_json->'spec'->'requests' ELSE '[]'::jsonb END) r
	    WHERE r->>'resourceType' = $3)
	 FROM quota_objects c
	WHERE c.kind = 'QuotaClaim'
	  AND c.claim_consumer_kind = $1 AND c.claim_consumer_name = $2
	  AND COALESCE(c.data_json->'spec'->'consumerRef'->>'apiGroup', '') = $4
	  AND COALESCE(c.data_json->'spec'->'consumerRef'->>'namespace', '') = $5
	  AND EXISTS (
	    SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(c.data_json->'status'->'allocations') = 'array'
	                                            THEN c.data_json->'status'->'allocations' ELSE '[]'::jsonb END) a
	     WHERE a->>'resourceType' = $3 AND a->>'status' = 'Granted')
	ORDER BY c.key
	FOR UPDATE OF c`

// insertBucket inserts a QuotaBucket whose allocated, available and
// claimCount reflect the Granted claims already charged to it (see
// SetRecomputeBucketAllocations). It follows the writer contract: the claim
// row locks come first, in key order, then the insert draws its RV, then
// COMMIT.
func (s *Store) insertBucket(ctx context.Context, key string, bucket *quotainternal.QuotaBucket, namespace, name string) (int64, []byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxStorageRetries; attempt++ {
		rv, data, err := s.insertBucketOnce(ctx, key, bucket, namespace, name)
		if err != nil && isRetryableTxError(err) {
			lastErr = err
			continue
		}
		return rv, data, err
	}
	return 0, nil, lastErr
}

func (s *Store) insertBucketOnce(ctx context.Context, key string, bucket *quotainternal.QuotaBucket, namespace, name string) (int64, []byte, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			klog.V(4).InfoS("Rollback after bucket insert failure", "key", key, "err", rbErr)
		}
	}()

	ref := bucket.Spec.ConsumerRef
	rows, err := tx.QueryContext(ctx, lockBucketClaimsSQL,
		ref.Kind, ref.Name, bucket.Spec.ResourceType, ref.APIGroup, ref.Namespace)
	if err != nil {
		return 0, nil, fmt.Errorf("lock claims charged to bucket: %w", err)
	}
	var allocated int64
	var claims int32
	for rows.Next() {
		var claimKey string
		var amount int64
		if err := rows.Scan(&claimKey, &amount); err != nil {
			_ = rows.Close()
			return 0, nil, fmt.Errorf("scan claim charged to bucket: %w", err)
		}
		allocated = quotainternal.SaturatingAdd(allocated, amount)
		claims++
	}
	if err := rows.Close(); err != nil {
		return 0, nil, err
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	stored := bucket.DeepCopy()
	stored.Status.Allocated = allocated
	stored.Status.ClaimCount = claims
	stored.Status.Available = stored.Status.Limit - allocated
	if stored.Status.Available < 0 {
		stored.Status.Available = 0
	}
	data, err := s.encodeForStorage(stored)
	if err != nil {
		return 0, nil, storage.NewInternalError(fmt.Errorf("failed to encode bucket: %w", err))
	}

	var rv int64
	if err := tx.QueryRowContext(ctx, insertSQL, key, s.kindFor(bucket), namespace, name, data).Scan(&rv); err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}
	if claims > 0 {
		klog.V(2).InfoS("Created QuotaBucket with allocations of existing claims",
			"key", key, "allocated", allocated, "claimCount", claims)
	}
	return rv, data, nil
}

// insertSQL inserts a row and its ADDED changelog entry; see insert.
const insertSQL = `WITH rv AS MATERIALIZED (SELECT quota_next_rv() AS v),
		 ins AS (
		   INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data, created_at, updated_at)
		   SELECT $1, rv.v, $2, $3, $4, $5, NOW(), NOW() FROM rv
		   RETURNING resource_version
		 ),
		 log AS (
		   INSERT INTO quota_changelog (key, resource_version, event_type, data, created_at)
		   SELECT $1, resource_version, 'ADDED', $5, NOW() FROM ins
		 )
		 SELECT resource_version FROM ins`

// insert writes a new row and its ADDED changelog entry in one autocommit
// statement. The RV CTE is materialized and feeds the INSERT, so the
// commit-order lock is taken before the row is inserted; a concurrent
// compliant inserter of the same key would already hold that lock, so the
// unique-index wait cannot invert the lock order.
func (s *Store) insert(ctx context.Context, key, kind, namespace, name string, data []byte) (int64, error) {
	var rv int64
	err := s.db.QueryRowContext(ctx, insertSQL, key, kind, namespace, name, data).Scan(&rv)
	return rv, err
}

// Delete removes the object at the given key and returns its last state
// stamped with the delete RV, as etcd3 does.
func (s *Store) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	for attempt := 0; attempt < maxStorageRetries; attempt++ {
		existingData, existingRV, err := s.readRow(ctx, key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return storage.NewKeyNotFoundError(key, 0)
			}
			return storage.NewInternalError(fmt.Errorf("failed to read existing object: %w", err))
		}
		existing, err := s.decodeWithRV(existingData, existingRV)
		if err != nil {
			return storage.NewInternalError(fmt.Errorf("failed to decode existing object: %w", err))
		}
		if err := checkPreconditions(key, preconditions, existing); err != nil {
			return err
		}
		if validateDeletion != nil {
			if err := validateDeletion(ctx, existing); err != nil {
				return err
			}
		}

		// The target row is locked (and its RV re-checked) before
		// quota_next_rv() runs, because rv reads FROM tgt.
		var deleteRV int64
		err = s.db.QueryRowContext(ctx,
			`WITH tgt AS MATERIALIZED (
			   SELECT key, data FROM quota_objects WHERE key = $1 AND resource_version = $2 FOR UPDATE
			 ),
			 rv AS MATERIALIZED (SELECT quota_next_rv() AS v FROM tgt),
			 del AS (
			   DELETE FROM quota_objects o USING tgt WHERE o.key = tgt.key RETURNING o.key
			 ),
			 log AS (
			   INSERT INTO quota_changelog (key, resource_version, event_type, data, created_at)
			   SELECT $1, rv.v, 'DELETED', tgt.data, NOW() FROM rv, tgt, del
			 )
			 SELECT v FROM rv`,
			key, existingRV,
		).Scan(&deleteRV)
		if errors.Is(err, sql.ErrNoRows) {
			// The row changed or vanished since the read: re-read.
			continue
		}
		if err != nil {
			if isRetryableTxError(err) {
				continue
			}
			return storage.NewInternalError(fmt.Errorf("failed to delete object: %w", err))
		}
		return decode(s.codec, existingData, out, deleteRV)
	}
	return storage.NewResourceVersionConflictsError(key, 0)
}

// Watch starts a watch on the given key with the given options.
func (s *Store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.watcher.Watch(ctx, key, opts, s.newFunc)
}

// SetNewFunc sets the factory function for creating zero-value objects.
// This is used to construct bookmark events during watch.
func (s *Store) SetNewFunc(f func() runtime.Object) {
	s.newFunc = f
	s.kind = ""
	if f != nil {
		if t := reflect.TypeOf(f()); t != nil {
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			s.kind = t.Name()
		}
	}
}

// kindFor returns the kind column for a new row holding obj. The kind is
// fixed when the row is inserted and never rewritten: the claim_* generated
// columns and the native QuotaClaim layer's partial indexes and queries
// select rows by kind = 'QuotaClaim', so a row whose kind changed would
// vanish from native LIST and WATCH while still holding capacity.
//
// Objects reaching storage are usually internal versions with empty
// TypeMeta, so the store's own kind (from newFunc) wins. TypeMeta is the
// fallback for stores without a newFunc (tests).
func (s *Store) kindFor(obj runtime.Object) string {
	if s.kind != "" {
		return s.kind
	}
	kind, _, _ := extractMetadata(obj)
	return kind
}

// Get retrieves an object by key. A non-zero opts.ResourceVersion is a
// minimum: if storage has not reached it, Get returns a "too large resource
// version" error, as etcd3 does.
func (s *Store) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	if err := s.validateKey(key); err != nil {
		return err
	}
	minRV, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}

	var (
		data    []byte
		rv      sql.NullInt64
		current int64
	)
	err = s.db.QueryRowContext(ctx,
		`SELECT st.committed_rv, o.data, o.resource_version
		   FROM quota_rv_state st
		   LEFT JOIN quota_objects o ON o.key = $1
		  WHERE st.singleton`,
		key,
	).Scan(&current, &data, &rv)
	if err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to get object: %w", err))
	}
	if minRV > 0 && uint64(current) < minRV {
		return storage.NewTooLargeResourceVersionError(minRV, uint64(current), 0)
	}
	if !rv.Valid {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(objPtr)
		}
		return storage.NewKeyNotFoundError(key, 0)
	}
	return decode(s.codec, data, objPtr, rv.Int64)
}

// GetList retrieves objects under key. Rows and the list RV come from one
// REPEATABLE READ snapshot; since every snapshot sees an exact RV prefix,
// the list is exactly the state at its RV.
//
// limit/continue use the upstream continue-token encoding. Postgres keeps
// only the latest version of each object, so a page at an older RV (a
// continue token, or ResourceVersionMatch=Exact) is served only if nothing
// in the remaining key range changed since that RV. Otherwise the request
// fails with 410 Gone, and a continue request also gets an "inconsistent
// continue" token, the same response etcd3 gives after compaction.
func (s *Store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to get items pointer: %w", err))
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return storage.NewInternalError(fmt.Errorf("need ptr to slice: %w", err))
	}

	keyPrefix := key
	if opts.Recursive && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	withRev, continueKey, err := storage.ValidateListOptions(keyPrefix, s.versioner, opts)
	if err != nil {
		return err
	}
	paging := opts.Recursive && opts.Predicate.Limit > 0

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to begin list snapshot: %w", err))
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			klog.ErrorS(rbErr, "Failed to rollback list snapshot", "key", key)
		}
	}()

	var committed, compacted int64
	if err := tx.QueryRowContext(ctx, pgwatch.StateQuery).Scan(&committed, &compacted); err != nil {
		return storage.NewInternalError(fmt.Errorf("failed to read resource version state: %w", err))
	}

	// Range of keys this request covers: [lo, hi) plus an optional
	// continuation lower bound.
	lo, hi := keyPrefix, PrefixRangeEnd(keyPrefix)
	from, fromInclusive := "", false
	if continueKey != "" {
		from, fromInclusive = continueStart(continueKey)
	}

	listRV := committed
	if withRev > 0 {
		if withRev > committed {
			return storage.NewTooLargeResourceVersionError(uint64(withRev), uint64(committed), 0)
		}
		if withRev < committed {
			changed := withRev < compacted
			if !changed && opts.Recursive {
				changed, err = changedSince(ctx, tx, withRev, lo, hi, from)
			} else if !changed {
				changed, err = keyChangedSince(ctx, tx, withRev, key)
			}
			if err != nil {
				return storage.NewInternalError(fmt.Errorf("failed to check changes since %d: %w", withRev, err))
			}
			if changed {
				if continueKey != "" {
					return expiredContinueError(continueKey, keyPrefix)
				}
				return apierrors.NewResourceExpired(listExpiredMsg)
			}
		}
		listRV = withRev
	} else if minRV, _ := s.versioner.ParseResourceVersion(opts.ResourceVersion); minRV > uint64(committed) {
		// NotOlderThan / legacy semantics: storage must have reached the
		// requested RV.
		return storage.NewTooLargeResourceVersionError(minRV, uint64(committed), 0)
	}

	newItem := func() runtime.Object { return reflect.New(v.Type().Elem()).Interface().(runtime.Object) }
	appendItem := func(data []byte, rv int64) error {
		obj := newItem()
		if err := decode(s.codec, data, obj, rv); err != nil {
			return err
		}
		if matchesPredicate(obj, opts.Predicate) {
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}
		return nil
	}

	if !opts.Recursive {
		var data []byte
		var rv int64
		err := tx.QueryRowContext(ctx, `SELECT data, resource_version FROM quota_objects WHERE key = $1`, key).Scan(&data, &rv)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return storage.NewInternalError(fmt.Errorf("failed to list objects: %w", err))
		default:
			if err := appendItem(data, rv); err != nil {
				return storage.NewInternalError(fmt.Errorf("failed to decode list item: %w", err))
			}
		}
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		return s.versioner.UpdateList(listObj, uint64(listRV), "", nil)
	}

	limit := opts.Predicate.Limit
	batch := limit
	var lastKey string
	hasMore := false
	for {
		args := []any{lo, hi}
		query := `SELECT key, data, resource_version FROM quota_objects
			WHERE key >= $1 AND key < $2`
		if from != "" {
			args = append(args, from)
			op := ">"
			if fromInclusive {
				op = ">="
			}
			query += fmt.Sprintf(` AND key %s $%d`, op, len(args))
		}
		query += ` ORDER BY key`
		if paging {
			args = append(args, batch)
			query += fmt.Sprintf(" LIMIT $%d", len(args))
		}

		n, more, err := s.scanListBatch(ctx, tx, query, args, func(k string, data []byte, rv int64) (bool, error) {
			if paging && int64(v.Len()) >= limit {
				return false, nil
			}
			lastKey = k
			if err := appendItem(data, rv); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return storage.NewInternalError(fmt.Errorf("failed to list objects: %w", err))
		}
		if more {
			hasMore = true
			break
		}
		if !paging || int64(n) < batch {
			break
		}
		// Predicate filtering left the page short: keep fetching, doubling
		// the batch like etcd3 does.
		from, fromInclusive = lastKey, false
		if batch < maxListBatch {
			batch *= 2
			if batch > maxListBatch {
				batch = maxListBatch
			}
		}
	}

	if v.IsNil() {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}

	var continueValue string
	var remaining *int64
	if hasMore {
		continueValue, err = storage.EncodeContinue(lastKey+"\x00", keyPrefix, listRV)
		if err != nil {
			return storage.NewInternalError(fmt.Errorf("failed to encode continue token: %w", err))
		}
		if predicateEmpty(opts.Predicate) {
			var count int64
			if err := tx.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM quota_objects WHERE key > $1 AND key < $2`,
				lastKey, hi,
			).Scan(&count); err != nil {
				return storage.NewInternalError(fmt.Errorf("failed to count remaining items: %w", err))
			}
			remaining = &count
		}
	}
	return s.versioner.UpdateList(listObj, uint64(listRV), continueValue, remaining)
}

// scanListBatch runs query and feeds rows to fn until fn returns false.
// It returns the number of rows consumed and whether rows remained when fn
// stopped.
func (s *Store) scanListBatch(ctx context.Context, tx *sql.Tx, query string, args []any, fn func(key string, data []byte, rv int64) (bool, error)) (int, bool, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var k string
		var data []byte
		var rv int64
		if err := rows.Scan(&k, &data, &rv); err != nil {
			return n, false, err
		}
		ok, err := fn(k, data, rv)
		if err != nil {
			return n, false, err
		}
		if !ok {
			return n, true, nil
		}
		n++
	}
	return n, false, rows.Err()
}

// changedSince reports whether any changelog row in (rv, now] touches the
// key range [lo, hi) at or after from.
func changedSince(ctx context.Context, tx *sql.Tx, rv int64, lo, hi, from string) (bool, error) {
	if from != "" && from > lo {
		lo = from
	}
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM quota_changelog
		    WHERE resource_version > $1
		      AND key >= $2 AND key < $3
		 )`,
		rv, lo, hi,
	).Scan(&exists)
	return exists, err
}

// keyChangedSince reports whether key changed after rv.
func keyChangedSince(ctx context.Context, tx *sql.Tx, rv int64, key string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM quota_changelog
		    WHERE resource_version > $1 AND hashtext(key) = hashtext($2) AND key = $2)`,
		rv, key,
	).Scan(&exists)
	return exists, err
}

// continueStart converts a decoded continue key into a lower bound. Tokens
// produced by EncodeContinue end in "\x00" (the smallest key after the last
// one returned). Postgres text cannot hold NUL, so that form becomes an
// exclusive bound on the key before it.
func continueStart(continueKey string) (string, bool) {
	if strings.HasSuffix(continueKey, "\x00") {
		return strings.TrimSuffix(continueKey, "\x00"), false
	}
	return continueKey, true
}

// expiredContinueError mirrors etcd3's handleCompactedErrorForPaging: a 410
// carrying a token that continues at the latest RV (rv=-1).
func expiredContinueError(continueKey, keyPrefix string) error {
	token, err := storage.EncodeContinue(continueKey, keyPrefix, -1)
	if err != nil {
		return apierrors.NewResourceExpired(continueExpiredMsg)
	}
	status := apierrors.NewResourceExpired(inconsistentContinue)
	status.ErrStatus.Continue = token
	return status
}

// PrefixRangeEnd returns the smallest string greater than every string with
// the given prefix (etcd's GetPrefixRangeEnd), or "\U0010FFFF" style
// sentinel when no such string exists.
func PrefixRangeEnd(prefix string) string {
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0x7f {
			end[i]++
			return string(end[:i+1])
		}
	}
	// Prefix is empty or all bytes are >= 0x7f; use a bound above any
	// valid storage key.
	return "\x7f\x7f\x7f\x7f"
}

// GuaranteedUpdate performs a read-modify-write cycle with optimistic
// concurrency. tryUpdate and precondition errors are returned as-is; only
// storage-level races (the row changed between the read and the write, a
// concurrent create, a serialization failure, or a deadlock) are retried,
// re-reading the current state each time, as etcd3 does.
func (s *Store) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	for attempt := 0; attempt < maxStorageRetries; attempt++ {
		data, rv, retry, err := s.guaranteedUpdateOnce(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate)
		if retry {
			klog.V(4).InfoS("Postgres GuaranteedUpdate storage race; retrying", "key", key, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return err
		}
		return decode(s.codec, data, destination, rv)
	}
	return storage.NewResourceVersionConflictsError(key, 0)
}

// guaranteedUpdateOnce performs one attempt. retry is true when the attempt
// lost a storage-level race and should be repeated from a fresh read.
func (s *Store) guaranteedUpdateOnce(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc) (data []byte, rv int64, retry bool, err error) {
	existingData, existingRV, err := s.readRow(ctx, key)
	exists := true
	if errors.Is(err, sql.ErrNoRows) {
		if !ignoreNotFound {
			return nil, 0, false, storage.NewKeyNotFoundError(key, 0)
		}
		exists = false
	} else if err != nil {
		return nil, 0, false, storage.NewInternalError(fmt.Errorf("failed to read existing object: %w", err))
	}

	var existing runtime.Object
	if exists {
		existing, err = s.decodeWithRV(existingData, existingRV)
		if err != nil {
			return nil, 0, false, storage.NewInternalError(fmt.Errorf("failed to decode existing object: %w", err))
		}
	} else {
		existing = reflect.New(reflect.TypeOf(destination).Elem()).Interface().(runtime.Object)
	}

	if err := checkPreconditions(key, preconditions, existing); err != nil {
		return nil, 0, false, err
	}

	ret, _, err := tryUpdate(existing.DeepCopyObject(), storage.ResponseMeta{ResourceVersion: uint64(existingRV)})
	if err != nil {
		return nil, 0, false, err
	}

	newData, err := s.encodeForStorage(ret)
	if err != nil {
		return nil, 0, false, storage.NewInternalError(fmt.Errorf("failed to encode updated object: %w", err))
	}
	if exists {
		// No-op detection compares like with like: both sides encoded with
		// the RV cleared.
		oldData, err := s.encodeForStorage(existing.DeepCopyObject())
		if err == nil && bytes.Equal(oldData, newData) {
			return existingData, existingRV, false, nil
		}
	}

	_, namespace, name := extractMetadata(ret)

	if !exists {
		newRV, insertedData, err := s.insertObject(ctx, key, ret, newData)
		if err != nil {
			if isUniqueViolation(err) || isRetryableTxError(err) {
				return nil, 0, true, nil
			}
			return nil, 0, false, storage.NewInternalError(fmt.Errorf("failed to insert object: %w", err))
		}
		return insertedData, newRV, false, nil
	}

	var newRV int64
	err = s.db.QueryRowContext(ctx,
		`WITH tgt AS MATERIALIZED (
		   SELECT key, data FROM quota_objects WHERE key = $1 AND resource_version = $2 FOR UPDATE
		 ),
		 `+ChangelogBackfillCTE+`,
		 rv AS MATERIALIZED (SELECT quota_next_rv() AS v FROM tgt, (SELECT count(*) FROM backfill) bf),
		 upd AS (
		   UPDATE quota_objects o
		      SET resource_version = rv.v, namespace = $3, name = $4, data = $5, updated_at = NOW()
		     FROM rv, tgt
		    WHERE o.key = tgt.key
		   RETURNING o.resource_version
		 ),
		 log AS (
		   INSERT INTO quota_changelog (key, resource_version, event_type, data, created_at)
		   SELECT $1, resource_version, 'MODIFIED', $5, NOW() FROM upd
		 )
		 SELECT resource_version FROM upd`,
		key, existingRV, namespace, name, newData,
	).Scan(&newRV)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, true, nil
	}
	if err != nil {
		if isRetryableTxError(err) {
			return nil, 0, true, nil
		}
		return nil, 0, false, storage.NewInternalError(fmt.Errorf("failed to update object: %w", err))
	}
	return newData, newRV, false, nil
}

// readRow reads one row without locking.
func (s *Store) readRow(ctx context.Context, key string) ([]byte, int64, error) {
	var data []byte
	var rv int64
	err := s.db.QueryRowContext(ctx,
		`SELECT data, resource_version FROM quota_objects WHERE key = $1`, key,
	).Scan(&data, &rv)
	return data, rv, err
}

// decodeWithRV decodes data and stamps rv on the result.
func (s *Store) decodeWithRV(data []byte, rv int64) (runtime.Object, error) {
	obj, err := decodeToObject(s.codec, data)
	if err != nil {
		return nil, err
	}
	if err := s.versioner.UpdateObject(obj, uint64(rv)); err != nil {
		return nil, err
	}
	return obj, nil
}

// isRetryableTxError reports Postgres errors that mean "this attempt lost a
// race; redo it": serialization_failure (40001, including the commit-order
// trigger's rejection) and deadlock_detected (40P01).
func isRetryableTxError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

// Stats returns storage statistics.
func (s *Store) Stats(ctx context.Context) (storage.Stats, error) {
	var count int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM quota_objects`,
	).Scan(&count)
	if err != nil {
		return storage.Stats{}, storage.NewInternalError(fmt.Errorf("failed to count objects: %w", err))
	}
	return storage.Stats{ObjectCount: count}, nil
}

// GetCurrentResourceVersion returns the highest committed resource version.
func (s *Store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	rv, err := s.currentResourceVersion(ctx)
	if err != nil {
		return 0, storage.NewInternalError(fmt.Errorf("failed to get current resource version: %w", err))
	}
	return uint64(rv), nil
}

// EnableResourceSizeEstimation is a no-op for the Postgres backend.
func (s *Store) EnableResourceSizeEstimation(storage.KeysFunc) error {
	return nil
}

// CompactRevision returns the changelog compaction floor: the highest RV
// whose history may have been deleted. Watches and paginated lists that
// need history at or below it get 410 Gone.
func (s *Store) CompactRevision() int64 {
	return s.watcher.Hub().CompactedRV()
}

// ReadinessCheck verifies the Postgres connection is healthy.
func (s *Store) ReadinessCheck() error {
	return s.db.Ping()
}

// RequestWatchProgress asks every watch on this Store to drain and emit a
// bookmark at the RV it has delivered up to. The cacher uses this for
// ConsistentListFromCache: it reads GetCurrentResourceVersion, requests
// progress, and waits for a bookmark at or above that RV. Each watch drains
// from a snapshot taken after this call, so its bookmark covers it, and a
// bookmark never runs ahead of an undelivered event.
func (s *Store) RequestWatchProgress(ctx context.Context) error {
	s.watcher.NotifyProgress()
	return nil
}

// currentResourceVersion returns quota_rv_state.committed_rv. It is
// maintained by the commit-order trigger and never deleted, so it is
// monotonic and at least 1 (the schema seeds it), which avoids the
// apiserver's "illegal resource version from storage: 0" error on an empty
// database.
func (s *Store) currentResourceVersion(ctx context.Context) (int64, error) {
	var rv int64
	err := s.db.QueryRowContext(ctx, pgwatch.CurrentRVQuery).Scan(&rv)
	return rv, err
}

// validateKey checks that the key is not empty.
func (s *Store) validateKey(key string) error {
	if key == "" {
		return storage.NewInternalError(fmt.Errorf("key must not be empty"))
	}
	return nil
}
