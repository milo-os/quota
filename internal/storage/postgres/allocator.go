// Package postgres also provides a transactional quota allocator that runs
// the bucket-and-claim allocation logic synchronously inside the user's CREATE
// request, rather than asynchronously via a controller.
//
// Why a separate allocator?
//
// The standard Kubernetes pattern (etcd-backed) puts cross-object reasoning
// in controllers because etcd doesn't expose multi-row transactions in the
// shape Kubernetes uses (read-modify-write with optimistic concurrency).
// Controllers reconcile state out-of-band, accepting eventual consistency.
//
// Postgres supports multi-row atomic transactions, so the same logic runs
// synchronously inside the request path.
//
// Allocate protocol (one transaction, two pipelined round-trips):
//
//  1. Batch 1: BEGIN + one `SELECT ... FOR UPDATE` statement per bucket, in
//     sorted key order. Each statement is a separate protocol message, so
//     the server acquires the row locks strictly in that order; lock order
//     never depends on how the planner evaluates a compound statement.
//  2. Go-side: every bucket must exist (a missing bucket is Forbidden and
//     the transaction is rolled back). The capacity check runs against the
//     locked values and decides Granted/Denied per resource type.
//  3. Batch 2: one compound CTE that updates only the granted buckets,
//     inserts the claim carrying its FINAL status, writes every changelog
//     row, then COMMIT.
//
// The first committed version of a claim therefore already carries its final
// Granted/Denied status. Watchers see exactly one ADDED event and the
// admission waiter cannot resolve on a transient Granted state. A crash at
// any point before COMMIT leaves nothing behind.
//
// Release protocol (one transaction, one pipelined round-trip; the claim row
// lock is taken after the bucket locks and before any RV is drawn, so the
// claim_log write cannot deadlock against Allocate):
//
//	BEGIN + one `SELECT ... FOR UPDATE` per bucket (sorted)
//	      + `SELECT ... FOR UPDATE` on the claim row + CTE + COMMIT.
//
// The CTE deletes the claim row conditioned on its resource version (and UID),
// and every bucket decrement is gated on `EXISTS (SELECT 1 FROM claim_del)`.
// Only the transaction that actually removed the claim row decrements the
// buckets, so concurrent or retried releases are idempotent.
//
// Lock ordering: both Allocate and Release lock buckets in sorted key order
// first and only then touch the claim row, so the two cannot deadlock against
// each other or against themselves.
//
// The bucket data column is updated by merging the new status fields into
// the data_json generated column, avoiding Go-side
// decode/encode (see bucketStatusPatchOpen). The
// JSONB result is cast back to bytea via ::text::bytea, which round-trips
// faithfully because data_json is defined as
// convert_from(data, 'UTF8')::jsonb. Readers (store.Get, watcher) always
// override the decoded resource_version with the row's resource_version
// column, so the in-blob RV is left stale (benign).
//
// Resource versions: every RV is drawn with quota_next_rv(),
// which takes the transaction-scoped commit-order advisory lock, so RV order
// equals commit order (see internal/watch/postgres.go). The writer contract
// is: take every row lock first, then draw RVs, then commit without waiting
// on any further row lock. Both protocols comply -- all FOR UPDATE locks are
// separate statements that draw no RV, and the only statement that calls
// quota_next_rv() is the final CTE, which touches only rows this transaction
// already locked (or inserts a new claim key). The commit-order lock is
// therefore held only for the CTE's execution plus COMMIT, with no client
// round trip inside it. Writers on disjoint buckets still wait on each other
// for that short window, but never for each other's bucket row locks.
//
// Group commit (batcher.go): by default concurrent Allocate and Release calls
// in one process are combined, so one transaction -- one commit-order lock
// acquisition and one commit flush -- decides and writes many claims. A
// batch locks the union of its bucket rows and claim rows (created and
// released) in globally sorted key order (the same rule as above), decides
// each operation in arrival order against a running copy of the locked
// bucket state, and writes every touched bucket once with its final state.
// At most two batches per process are in flight. A batch of one operation
// runs the single-claim protocol above unchanged. WithMaxBatchSize(1)
// disables batching.
//
// The allocator is wired in cmd/quota/serve.go when --quota-storage-backend=postgres
// and threaded through ExtraConfig to the QuotaClaim REST storage, which
// uses it via the AllocatingStorage decorator (see quotaclaim/storage.go).
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/internal/metrics"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

// Allocator runs synchronous quota allocation against Postgres.
//
// Implementations are safe for concurrent use.
type Allocator interface {
	// Allocate inserts the claim with full status (allocations + overall
	// Granted condition), updates the affected bucket(s) atomically, and
	// returns the persisted claim. Concurrent calls to Allocate against
	// claims targeting the same bucket serialize on the bucket's row-level
	// lock acquired via SELECT ... FOR UPDATE; claims targeting disjoint
	// buckets run in parallel.
	//
	// If the claim's spec is invalid (no matching bucket, etc.), Allocate
	// returns a typed apierror suitable for returning to the client.
	Allocate(ctx context.Context, claim *quotainternal.QuotaClaim) (*quotainternal.QuotaClaim, error)

	// Release reverses the bucket allocations made by a previously-granted
	// claim and deletes the claim. Used by the QuotaClaim REST storage's
	// Delete handler.
	Release(ctx context.Context, claim *quotainternal.QuotaClaim) error
}

// PostgresAllocator implements Allocator against the Postgres storage backend.
//
// The allocator prefers a *pgxpool.Pool when available because the hot path
// uses pgx.Batch pipelining (Conn.SendBatch). If only a *sql.DB is set (the
// test-only path), the allocator acquires a pgx connection via sqlConn.Raw
// and uses the same pipelining code path.
type PostgresAllocator struct {
	db    *sql.DB
	pool  *pgxpool.Pool
	codec runtime.Codec
	// bucketNamespace is where buckets for cluster-scoped consumers (no
	// consumerRef.namespace) live. See WithBucketNamespace.
	bucketNamespace string
	// maxBatch caps how many operations share one transaction. See
	// WithMaxBatchSize.
	maxBatch int
	// batcher is nil when maxBatch is 1: every call runs on the caller's
	// goroutine with the single-claim protocol.
	batcher *batcher
}

// DefaultBucketNamespace is the namespace holding QuotaBuckets for consumers
// without a namespace. It matches the bucket controller's default.
const DefaultBucketNamespace = "quota-system"

// AllocatorOption configures a PostgresAllocator.
type AllocatorOption func(*PostgresAllocator)

// WithBucketNamespace sets the namespace in which the allocator looks up
// QuotaBuckets for consumers that carry no namespace. It must match the
// bucket controller's namespace. Empty keeps DefaultBucketNamespace.
func WithBucketNamespace(namespace string) AllocatorOption {
	return func(a *PostgresAllocator) {
		if namespace != "" {
			a.bucketNamespace = namespace
		}
	}
}

// DefaultMaxBatchSize is the default cap on operations per group-commit
// transaction. See WithMaxBatchSize and docs/storage-backends.md for how it
// was chosen.
const DefaultMaxBatchSize = 64

// WithMaxBatchSize caps how many concurrent Allocate/Release operations are
// committed together in one transaction. 1 disables batching: every call
// runs the single-claim protocol on the caller's goroutine. Values below 1
// keep DefaultMaxBatchSize.
func WithMaxBatchSize(n int) AllocatorOption {
	return func(a *PostgresAllocator) {
		if n >= 1 {
			a.maxBatch = n
		}
	}
}

func newAllocator(db *sql.DB, pool *pgxpool.Pool, codec runtime.Codec, opts []AllocatorOption) *PostgresAllocator {
	a := &PostgresAllocator{db: db, pool: pool, codec: codec, bucketNamespace: DefaultBucketNamespace, maxBatch: DefaultMaxBatchSize}
	for _, opt := range opts {
		opt(a)
	}
	if a.maxBatch > 1 {
		a.batcher = newBatcher(a, a.maxBatch)
	}
	return a
}

// NewAllocator constructs a PostgresAllocator backed by a *sql.DB. Pipelining
// still runs: the commit path dereferences the underlying pgx connection via
// sqlConn.Raw. Prefer NewAllocatorWithPool when a pgxpool.Pool is available.
func NewAllocator(db *sql.DB, codec runtime.Codec, opts ...AllocatorOption) *PostgresAllocator {
	return newAllocator(db, nil, codec, opts)
}

// NewAllocatorWithPool constructs a PostgresAllocator that acquires
// connections directly from a pgxpool.Pool. This is the preferred path: no
// database/sql round-trip is needed to unwrap the pgx connection, and the
// pool's prepared-statement cache is shared with the rest of the native
// query path. The *sql.DB is retained only to service callers that still
// reach through unrelated helpers.
func NewAllocatorWithPool(db *sql.DB, pool *pgxpool.Pool, codec runtime.Codec, opts ...AllocatorOption) *PostgresAllocator {
	return newAllocator(db, pool, codec, opts)
}

// withPgxConn acquires a *pgx.Conn and invokes fn with it. When a pgxpool is
// configured the connection comes directly from the pool; otherwise the
// allocator falls back to database/sql's Conn+Raw bridge. The caller must
// not retain the *pgx.Conn after fn returns.
func (a *PostgresAllocator) withPgxConn(ctx context.Context, fn func(conn *pgx.Conn) error) error {
	if a.pool != nil {
		poolConn, err := a.pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire pool conn: %w", err)
		}
		defer poolConn.Release()
		return fn(poolConn.Conn())
	}
	sqlConn, err := a.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire sql conn: %w", err)
	}
	defer func() { _ = sqlConn.Close() }()
	return sqlConn.Raw(func(driverConn any) error {
		stdConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver conn type %T", driverConn)
		}
		return fn(stdConn.Conn())
	})
}

// rollback aborts the open transaction on conn. It uses a context detached
// from the request's cancellation so a cancelled client request still
// returns the connection to the pool in a clean state.
func rollback(ctx context.Context, conn *pgx.Conn, op string) {
	if _, err := conn.Exec(context.WithoutCancel(ctx), "ROLLBACK"); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		klog.V(4).InfoS("Rollback after allocator failure", "op", op, "err", err)
	}
}

// claim/bucket key construction. These mirror the keys produced by
// k8s.io/apiserver's standard storage layout, which is what the Store
// itself uses for its INSERT/SELECT/UPDATE statements.
const (
	keyPrefix   = "/" + quotainternal.GroupName
	bucketsPath = keyPrefix + "/quotabuckets"
	claimsPath  = keyPrefix + "/quotaclaims"
	bucketsKind = "QuotaBucket"
	claimsKind  = "QuotaClaim"
)

func bucketKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", bucketsPath, namespace, name)
}

func claimKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", claimsPath, namespace, name)
}

// Lock statements. Each bucket is locked by its own statement so the lock
// acquisition order is exactly the order the statements are queued in.
const (
	// lockBucketForAllocateSQL locks a bucket row and returns the values the
	// capacity check needs. A missing row yields pgx.ErrNoRows.
	lockBucketForAllocateSQL = `SELECT COALESCE((data_json->'status'->>'allocated')::bigint, 0), ` +
		`COALESCE((data_json->'status'->>'limit')::bigint, 0) ` +
		`FROM quota_objects WHERE key = $1 FOR UPDATE`

	// readBucketForDryRunSQL is the non-locking variant used by dry-run
	// allocations, which must not block or be blocked by real writers.
	readBucketForDryRunSQL = `SELECT COALESCE((data_json->'status'->>'allocated')::bigint, 0), ` +
		`COALESCE((data_json->'status'->>'limit')::bigint, 0) ` +
		`FROM quota_objects WHERE key = $1`

	// lockBucketForReleaseSQL locks a bucket row before the release CTE runs.
	// A missing bucket simply locks nothing.
	lockBucketForReleaseSQL = `SELECT 1 FROM quota_objects WHERE key = $1 FOR UPDATE`

	// lockClaimForReleaseSQL locks the claim row after the bucket locks and
	// before the release CTE. The CTE's statement snapshot is then taken
	// while this transaction owns the claim row (or after a concurrent
	// release has already committed its delete), so the CTE's "does the
	// row still exist" column is consistent with its DELETE.
	lockClaimForReleaseSQL = `SELECT 1 FROM quota_objects WHERE key = $1 FOR UPDATE`

	// claimExistsSQL is used by dry-run allocations to report AlreadyExists.
	claimExistsSQL = `SELECT EXISTS (SELECT 1 FROM quota_objects WHERE key = $1)`
)

// Postgres SQLSTATE for division_by_zero. The allocate CTE uses it as an
// in-statement assertion that every granted bucket was actually updated.
const pgDivisionByZero = "22012"

func isDivisionByZero(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgDivisionByZero
}

var claimsGR = schema.GroupResource{Group: quotainternal.GroupName, Resource: "quotaclaims"}

// plannedBucket is the planner's per-unique-bucket record of what the commit
// phase must do. It is computed before the transaction opens so the
// transaction only holds row locks for the minimum time needed.
//
// key is the bucket row key; the commit phase uses this both to acquire the
// FOR UPDATE lock and to target the UPDATE. amount is the total amount to add
// (Allocate) or subtract (Release).
type plannedBucket struct {
	key          string
	resourceType string
	amount       int64
}

// sortPlanned orders planned buckets by key. Every lock acquisition in this
// file goes through this order; do not acquire a bucket lock outside it.
func sortPlanned(planned []*plannedBucket) {
	sort.Slice(planned, func(i, j int) bool { return planned[i].key < planned[j].key })
}

// bucketState is the locked state of one bucket as read in batch 1.
type bucketState struct {
	exists    bool
	allocated int64
	limit     int64
}

// Allocate runs the full allocation flow in a single transaction. Bucket
// rows are locked in sorted key order, the capacity decision is made against
// the locked state, and the granted bucket updates plus the claim INSERT
// (already carrying its final status) land in one COMMIT.
func (a *PostgresAllocator) Allocate(ctx context.Context, claim *quotainternal.QuotaClaim) (*quotainternal.QuotaClaim, error) {
	start := time.Now()
	if len(claim.Spec.Requests) == 0 {
		return nil, apierrors.NewBadRequest("QuotaClaim has no requests")
	}

	planned := a.planAllocation(claim)
	var committed *quotainternal.QuotaClaim
	var err error
	if a.batcher != nil {
		committed, err = a.batcher.allocate(ctx, claim, planned)
	} else {
		committed, err = a.commitAllocation(ctx, claim, planned, false)
	}
	if err != nil {
		if isServerError(ctx, err) {
			metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError).Inc()
		}
		return nil, err
	}

	result := claimResultLabel(committed)
	metrics.QuotaClaimTotal.WithLabelValues(result).Inc()
	metrics.QuotaClaimLatency.WithLabelValues(result).Observe(time.Since(start).Seconds())
	return committed, nil
}

// AllocateDryRun evaluates the claim exactly as Allocate would (missing
// bucket => Forbidden, existing claim => AlreadyExists, otherwise the
// Granted/Denied decision against current bucket state) but writes nothing
// and takes no row locks. The returned claim carries the status a real
// Allocate would have produced at this instant; it has no resource version.
func (a *PostgresAllocator) AllocateDryRun(ctx context.Context, claim *quotainternal.QuotaClaim) (*quotainternal.QuotaClaim, error) {
	if len(claim.Spec.Requests) == 0 {
		return nil, apierrors.NewBadRequest("QuotaClaim has no requests")
	}
	return a.commitAllocation(ctx, claim, a.planAllocation(claim), true)
}

// isServerError reports whether an Allocate error is the server's fault and
// so counts as result="error": not a client error (missing bucket,
// duplicate name, bad request, all API statuses below 500) and not the
// caller giving up.
func isServerError(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code > 0 && status.Status().Code < 500 {
		return false
	}
	return true
}

// claimResultLabel classifies a committed claim into a metric label value:
// "granted" if every allocation succeeded, "denied" if every allocation was
// denied, or "partial" if some succeeded and some were denied.
func claimResultLabel(claim *quotainternal.QuotaClaim) string {
	granted, denied := 0, 0
	for _, alloc := range claim.Status.Allocations {
		switch alloc.Status {
		case quotainternal.QuotaClaimAllocationStatusGranted:
			granted++
		case quotainternal.QuotaClaimAllocationStatusDenied:
			denied++
		}
	}
	switch {
	case granted > 0 && denied == 0:
		return "granted"
	case denied > 0 && granted == 0:
		return "denied"
	default:
		return "partial"
	}
}

// planAllocation aggregates Spec.Requests by ResourceType into a deduplicated
// set of plannedBucket records, sorted by bucket key. A claim with two
// requests against the same bucket produces exactly one plannedBucket (and
// one UPDATE at commit time) carrying the SUMMED amount; this preserves
// atomicity of multi-request claims and is essential for correctness --
// without aggregation, two entries targeting the same bucket could each
// independently pass their capacity check when their sum does not fit.
//
// The planner does NOT touch the database. Capacity checks run inside
// commitAllocation, against the state that was actually row-locked.
func (a *PostgresAllocator) planAllocation(claim *quotainternal.QuotaClaim) []*plannedBucket {
	agg := make(map[string]int64, len(claim.Spec.Requests))
	order := make([]string, 0, len(claim.Spec.Requests))
	for _, req := range claim.Spec.Requests {
		if _, ok := agg[req.ResourceType]; !ok {
			order = append(order, req.ResourceType)
		}
		agg[req.ResourceType] = quotainternal.SaturatingAdd(agg[req.ResourceType], req.Amount)
	}

	bucketNamespace := a.bucketNamespaceFor(claim.Spec.ConsumerRef)
	planned := make([]*plannedBucket, 0, len(order))
	for _, resourceType := range order {
		planned = append(planned, &plannedBucket{
			key:          bucketKey(bucketNamespace, bucketNameFor(resourceType, claim.Spec.ConsumerRef)),
			resourceType: resourceType,
			amount:       agg[resourceType],
		})
	}
	sortPlanned(planned)
	return planned
}

// firstMissingBucket returns the first planned bucket (in lock order) whose
// row did not exist, or nil if all buckets exist.
func firstMissingBucket(planned []*plannedBucket, states []bucketState) *plannedBucket {
	for i, p := range planned {
		if !states[i].exists {
			return p
		}
	}
	return nil
}

// decideGrants runs the capacity check for every planned bucket against its
// locked state and returns the set of granted resource types. Each bucket is
// decided independently (matching Milo's per-allocation semantics): a claim
// may be partially granted, in which case its overall condition is Denied
// and Release later returns only the granted portions.
func decideGrants(planned []*plannedBucket, states []bucketState) map[string]bool {
	granted := make(map[string]bool, len(planned))
	for i, p := range planned {
		st := states[i]
		if st.exists && fits(st.allocated, p.amount, st.limit) {
			granted[p.resourceType] = true
		}
	}
	return granted
}

// fits reports whether amount more fits in a bucket holding allocated of
// limit. It compares against the remaining capacity instead of computing
// allocated+amount, which could overflow int64 when a saturated limit
// (sums of grants are capped at math.MaxInt64) admits allocations near it.
func fits(allocated, amount, limit int64) bool {
	return amount <= limit-allocated
}

// applyAllocationStatus writes the final per-request allocation statuses and
// the overall Granted condition onto claim.
func applyAllocationStatus(claim *quotainternal.QuotaClaim, planned []*plannedBucket, granted map[string]bool) {
	bucketNameByRT := make(map[string]string, len(planned))
	for _, p := range planned {
		bucketNameByRT[p.resourceType] = p.key[strings.LastIndex(p.key, "/")+1:]
	}

	now := metav1.Now()
	allGranted := true
	allocations := make([]quotainternal.QuotaClaimAllocationStatus, 0, len(claim.Spec.Requests))
	for _, req := range claim.Spec.Requests {
		if granted[req.ResourceType] {
			allocations = append(allocations, quotainternal.QuotaClaimAllocationStatus{
				ResourceType:       req.ResourceType,
				Status:             quotainternal.QuotaClaimAllocationStatusGranted,
				Reason:             quotainternal.QuotaClaimGrantedReason,
				Message:            "Capacity reserved",
				AllocatedAmount:    req.Amount,
				AllocatingBucket:   bucketNameByRT[req.ResourceType],
				LastTransitionTime: now,
			})
			continue
		}
		allGranted = false
		allocations = append(allocations, quotainternal.QuotaClaimAllocationStatus{
			ResourceType:       req.ResourceType,
			Status:             quotainternal.QuotaClaimAllocationStatusDenied,
			Reason:             quotainternal.QuotaClaimDeniedReason,
			Message:            fmt.Sprintf("Resource quota exceeded for %s", req.ResourceType),
			LastTransitionTime: now,
		})
	}

	claim.Status.Allocations = allocations
	claim.Status.ObservedGeneration = claim.Generation
	cond := metav1.Condition{
		Type:               quotainternal.QuotaClaimGranted,
		Status:             metav1.ConditionTrue,
		Reason:             quotainternal.QuotaClaimGrantedReason,
		Message:            "All resource requests granted",
		LastTransitionTime: now,
	}
	if !allGranted {
		cond.Status = metav1.ConditionFalse
		cond.Reason = quotainternal.QuotaClaimDeniedReason
		cond.Message = "One or more resource requests denied"
	}
	apimeta.SetStatusCondition(&claim.Status.Conditions, cond)
}

// missingBucketError is the Forbidden returned when a claim references a
// resource type with no QuotaBucket. The grant controller pre-creates
// buckets before marking a grant Active (see core/grant.go), so a missing
// bucket means no active grant covers the request.
func missingBucketError(claim *quotainternal.QuotaClaim, p *plannedBucket) error {
	return apierrors.NewForbidden(claimsGR, claim.Name,
		fmt.Errorf("no QuotaBucket exists for resource type %q (consumer %s/%s); ensure a QuotaGrant exists",
			p.resourceType, claim.Spec.ConsumerRef.Kind, claim.Spec.ConsumerRef.Name))
}

// readBucketStates consumes the BEGIN result and one result per planned
// bucket from batch 1. It always consumes every queued result so the
// connection stays in a consistent state.
func readBucketStates(res pgx.BatchResults, n int) ([]bucketState, error) {
	if _, err := res.Exec(); err != nil {
		return nil, storage.NewInternalError(fmt.Errorf("begin tx: %w", err))
	}
	states := make([]bucketState, n)
	for i := range states {
		err := res.QueryRow().Scan(&states[i].allocated, &states[i].limit)
		switch {
		case err == nil:
			states[i].exists = true
		case errors.Is(err, pgx.ErrNoRows):
			// Missing bucket: reported by the caller.
		default:
			return nil, storage.NewInternalError(fmt.Errorf("lock bucket: %w", err))
		}
	}
	return states, nil
}

// commitAllocation implements the Allocate protocol described in the package
// comment. With dryRun=true it reads without locking, decides, checks for an
// existing claim, and rolls back without writing anything.
func (a *PostgresAllocator) commitAllocation(
	ctx context.Context,
	claim *quotainternal.QuotaClaim,
	planned []*plannedBucket,
	dryRun bool,
) (*quotainternal.QuotaClaim, error) {
	prepareClaim(claim)
	cKey := claimKey(claim.Namespace, claim.Name)

	lockSQL := lockBucketForAllocateSQL
	if dryRun {
		lockSQL = readBucketForDryRunSQL
	}

	var claimRV int64
	runErr := a.withPgxConn(ctx, func(conn *pgx.Conn) (fnErr error) {
		defer func() {
			if fnErr != nil {
				rollback(ctx, conn, "allocate")
			}
		}()

		// Batch 1: BEGIN + per-bucket locks in sorted key order.
		batch := &pgx.Batch{}
		if dryRun {
			batch.Queue("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY")
		} else {
			batch.Queue("BEGIN ISOLATION LEVEL READ COMMITTED")
		}
		for _, p := range planned {
			batch.Queue(lockSQL, p.key)
		}
		if dryRun {
			batch.Queue(claimExistsSQL, cKey)
		}
		res := conn.SendBatch(ctx, batch)
		states, err := readBucketStates(res, len(planned))
		var claimExists bool
		if err == nil && dryRun {
			if scanErr := res.QueryRow().Scan(&claimExists); scanErr != nil {
				err = storage.NewInternalError(fmt.Errorf("probe claim: %w", scanErr))
			}
		}
		if closeErr := res.Close(); err == nil && closeErr != nil {
			err = storage.NewInternalError(fmt.Errorf("close lock batch: %w", closeErr))
		}
		if err != nil {
			return err
		}

		// Decision against the locked state.
		if missing := firstMissingBucket(planned, states); missing != nil {
			return missingBucketError(claim, missing)
		}
		granted := decideGrants(planned, states)
		applyAllocationStatus(claim, planned, granted)

		if dryRun {
			if claimExists {
				return apierrors.NewAlreadyExists(claimsGR, claim.Name)
			}
			if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
				return storage.NewInternalError(fmt.Errorf("end dry-run tx: %w", err))
			}
			return nil
		}

		claimData, err := runtime.Encode(a.codec, claim)
		if err != nil {
			return storage.NewInternalError(fmt.Errorf("encode claim: %w", err))
		}
		ctePlans := make([]allocateCTEPlan, 0, len(planned))
		for _, p := range planned {
			if granted[p.resourceType] {
				ctePlans = append(ctePlans, allocateCTEPlan{key: p.key, amount: p.amount})
			}
		}
		cteSQL, args := buildAllocateCTE(ctePlans, cKey, claim.Namespace, claim.Name, claimData)

		// Batch 2: CTE + COMMIT.
		batch = &pgx.Batch{}
		batch.Queue(cteSQL, args...)
		batch.Queue("COMMIT")
		res = conn.SendBatch(ctx, batch)

		dst := make([]any, 1+len(ctePlans))
		dst[0] = &claimRV
		asserts := make([]int64, len(ctePlans))
		for i := range asserts {
			dst[i+1] = &asserts[i]
		}
		if err := res.QueryRow().Scan(dst...); err != nil {
			_ = res.Close()
			switch {
			case isUniqueViolation(err):
				return apierrors.NewAlreadyExists(claimsGR, claim.Name)
			case isDivisionByZero(err):
				// A granted bucket was not updated even though its row
				// lock is held by this transaction. That cannot happen
				// unless another writer bypassed the row lock; refuse to
				// commit rather than record an unbacked grant.
				klog.ErrorS(err, "Granted bucket not updated under row lock", "claim", klog.KObj(claim))
				return storage.NewInternalError(fmt.Errorf("allocate cte: granted bucket was not updated"))
			default:
				return storage.NewInternalError(fmt.Errorf("allocate cte: %w", err))
			}
		}
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return storage.NewInternalError(fmt.Errorf("commit: %w", err))
		}
		if err := res.Close(); err != nil {
			return storage.NewInternalError(fmt.Errorf("close batch: %w", err))
		}
		return nil
	})
	if runErr != nil {
		return nil, runErr
	}

	if !dryRun {
		claim.ResourceVersion = strconv.FormatInt(claimRV, 10)
	}
	return claim, nil
}

// prepareClaim fills the type and system metadata a stored claim must carry.
func prepareClaim(claim *quotainternal.QuotaClaim) {
	claim.APIVersion = quotainternal.SchemeGroupVersion.String()
	claim.Kind = claimsKind
	if claim.UID == "" {
		claim.UID = generateUID()
	}
	if claim.CreationTimestamp.IsZero() {
		claim.CreationTimestamp = metav1.Now()
	}
}

// allocateCTEPlan carries the per-bucket parameters that buildAllocateCTE
// needs for a GRANTED bucket. Denied buckets are not passed in: they are
// left untouched.
type allocateCTEPlan struct {
	key    string
	amount int64
}

// bucketStatusExpr returns the SQL expression for a status field of the
// bucket row being updated, defaulting to 0 when absent.
func bucketStatusExpr(field, cast string) string {
	return fmt.Sprintf("COALESCE((qo.data_json->'status'->>'%s')::%s, 0)", field, cast)
}

// bucketStatusPatchOpen and bucketStatusPatchClose bracket the expression
// every allocator path uses for a bucket's new data_json. The caller puts
// the 'allocated', <v>, 'available', <v>, 'claimCount', <v>, pairs between
// them; the close adds lastReconciliation.
//
// The status object is merged with || in one step. That runs inside the
// commit-order window and replaced four nested jsonb_set calls, each of
// which copied the whole document; the output is byte-identical because
// jsonb orders object keys itself. As with jsonb_set on a missing parent, a
// bucket whose status is absent or not an object is left unchanged.
const (
	bucketStatusPatchOpen = `CASE WHEN jsonb_typeof(qo.data_json->'status') = 'object' ` +
		`THEN jsonb_set(qo.data_json, '{status}', (qo.data_json->'status') || jsonb_build_object(`
	bucketStatusPatchClose = `'lastReconciliation', to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))) ` +
		`ELSE qo.data_json END`
)

// buildAllocateCTE assembles the compound CTE that updates every granted
// bucket, inserts the claim with its final status, and writes all changelog
// rows in one statement. The caller has already locked every bucket row in
// this transaction, so the capacity guard in each UPDATE's WHERE clause is a
// defensive assertion, not the decision.
//
// Shape (for N granted buckets):
//
//	WITH
//	  b0_updated AS (
//	    UPDATE quota_objects qo
//	       SET data = <status || {allocated + $2, available, claimCount + 1,
//	                  lastReconciliation}>::text::bytea,
//	           resource_version = quota_next_rv(),
//	           updated_at = NOW()
//	     WHERE qo.key = $1
//	       AND $2 <= limit - allocated
//	    RETURNING qo.resource_version AS bucket_rv, qo.key, qo.data
//	  ),
//	  b0_log AS (INSERT INTO quota_changelog ... FROM b0_updated),
//	  ...
//	  claim_ins AS (INSERT INTO quota_objects ... RETURNING resource_version),
//	  claim_log AS (INSERT INTO quota_changelog ... 'ADDED' ... FROM claim_ins)
//	SELECT (SELECT resource_version FROM claim_ins),
//	       (SELECT 1 / count(*) FROM b0_updated),
//	       ...
//
// The `1 / count(*)` columns are referenced by the outer SELECT, so they are
// always evaluated: if any granted bucket's UPDATE matched zero rows the
// statement fails with division_by_zero and the transaction is rolled back.
// (Data-modifying CTEs always run to completion; plain SELECT CTEs only run
// if referenced, which is why the check lives in the outer SELECT.)
func buildAllocateCTE(plans []allocateCTEPlan, cKey, namespace, name string, claimData []byte) (string, []interface{}) {
	var sb strings.Builder
	args := make([]interface{}, 0, 2*len(plans)+5)
	next := func(v interface{}) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	allocated := bucketStatusExpr("allocated", "bigint")
	limit := bucketStatusExpr("limit", "bigint")
	claimCount := bucketStatusExpr("claimCount", "int")

	sb.WriteString("WITH ")
	for i, p := range plans {
		keyPH := next(p.key)
		amtPH := next(p.amount)

		fmt.Fprintf(&sb,
			"b%d_updated AS ("+
				"UPDATE quota_objects qo "+
				"SET data = ("+bucketStatusPatchOpen+
				"'allocated', %s + %s::bigint, "+
				"'available', %s - %s - %s::bigint, "+
				"'claimCount', %s + 1, "+
				bucketStatusPatchClose+")::text::bytea, "+
				"resource_version = quota_next_rv(), "+
				"updated_at = NOW() "+
				"WHERE qo.key = %s "+
				"AND %s::bigint <= %s - %s "+
				"RETURNING qo.resource_version AS bucket_rv, qo.key, qo.data), ",
			i,
			allocated, amtPH,
			limit, allocated, amtPH,
			claimCount,
			keyPH,
			amtPH, limit, allocated,
		)
		fmt.Fprintf(&sb,
			"b%d_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) "+
				"SELECT key, bucket_rv, 'MODIFIED', data FROM b%d_updated "+
				"RETURNING resource_version), ",
			i, i,
		)
	}

	cKeyPH := next(cKey)
	kindPH := next(claimsKind)
	nsPH := next(namespace)
	namePH := next(name)
	dataPH := next(claimData)

	fmt.Fprintf(&sb,
		"claim_ins AS (INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data, created_at, updated_at) "+
			"VALUES (%s, quota_next_rv(), %s, %s, %s, %s, NOW(), NOW()) "+
			"RETURNING resource_version), ",
		cKeyPH, kindPH, nsPH, namePH, dataPH,
	)
	fmt.Fprintf(&sb,
		"claim_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) "+
			"SELECT %s, resource_version, 'ADDED', NULL FROM claim_ins "+
			"RETURNING resource_version) ",
		cKeyPH,
	)

	sb.WriteString("SELECT (SELECT resource_version FROM claim_ins)")
	for i := range plans {
		fmt.Fprintf(&sb, ", (SELECT 1 / count(*) FROM b%d_updated)", i)
	}
	return sb.String(), args
}

// Release reverses bucket allocations and deletes the claim in a single
// atomic transaction. It is idempotent: if the claim row is already gone it
// returns nil without touching any bucket. If the row exists but no longer
// matches claim's resource version / UID, Release returns a Conflict.
func (a *PostgresAllocator) Release(ctx context.Context, claim *quotainternal.QuotaClaim) error {
	_, err := a.ReleaseClaim(ctx, claim)
	return err
}

// ReleaseClaim is Release with an explicit outcome: released is true only if
// this call deleted the claim row (and therefore applied the bucket
// decrements). released=false with a nil error means the claim row was
// already absent.
//
// The delete is conditioned on claim.ResourceVersion (and claim.UID) when
// set, so the decrements applied are exactly the allocations recorded on the
// version being deleted. A mismatch returns a Conflict; callers re-read and
// retry.
func (a *PostgresAllocator) ReleaseClaim(ctx context.Context, claim *quotainternal.QuotaClaim) (bool, error) {
	var rv *int64
	if claim.ResourceVersion != "" {
		parsed, err := strconv.ParseInt(claim.ResourceVersion, 10, 64)
		if err != nil {
			return false, apierrors.NewBadRequest(fmt.Sprintf("invalid resourceVersion %q: %v", claim.ResourceVersion, err))
		}
		rv = &parsed
	}

	planned := a.planRelease(claim)
	if a.batcher != nil {
		return a.batcher.release(ctx, claim, planned, rv)
	}
	return a.releaseOne(ctx, claim, planned, rv)
}

// releaseOne runs the single-claim Release protocol described in the
// package comment.
func (a *PostgresAllocator) releaseOne(ctx context.Context, claim *quotainternal.QuotaClaim, planned []*plannedBucket, rv *int64) (bool, error) {
	cKey := claimKey(claim.Namespace, claim.Name)
	cteSQL, args := buildReleaseCTE(planned, cKey, string(claim.UID), rv)

	var deleted int64
	var currentRV *int64
	err := a.withPgxConn(ctx, func(conn *pgx.Conn) (fnErr error) {
		defer func() {
			if fnErr != nil {
				rollback(ctx, conn, "release")
			}
		}()

		batch := &pgx.Batch{}
		batch.Queue("BEGIN ISOLATION LEVEL READ COMMITTED")
		for _, p := range planned {
			batch.Queue(lockBucketForReleaseSQL, p.key)
		}
		batch.Queue(lockClaimForReleaseSQL, cKey)
		batch.Queue(cteSQL, args...)
		batch.Queue("COMMIT")
		res := conn.SendBatch(ctx, batch)

		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return storage.NewInternalError(fmt.Errorf("begin tx: %w", err))
		}
		for range len(planned) + 1 {
			if _, err := res.Exec(); err != nil {
				_ = res.Close()
				return storage.NewInternalError(fmt.Errorf("lock rows: %w", err))
			}
		}
		if err := res.QueryRow().Scan(&deleted, &currentRV); err != nil {
			_ = res.Close()
			return storage.NewInternalError(fmt.Errorf("release cte: %w", err))
		}
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return storage.NewInternalError(fmt.Errorf("commit: %w", err))
		}
		if err := res.Close(); err != nil {
			return storage.NewInternalError(fmt.Errorf("close batch: %w", err))
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return releaseOutcome(claim, deleted, currentRV)
}

// releaseOutcome interprets the release CTE's result row. currentRV is the
// claim row's resource version as seen by the statement snapshot (nil if the
// row did not exist).
func releaseOutcome(claim *quotainternal.QuotaClaim, deleted int64, currentRV *int64) (bool, error) {
	switch {
	case deleted > 0:
		return true, nil
	case currentRV == nil:
		return false, nil
	default:
		return false, apierrors.NewConflict(claimsGR, claim.Name,
			fmt.Errorf("claim was modified (resourceVersion %d, expected %s) or recreated; re-read and retry",
				*currentRV, claim.ResourceVersion))
	}
}

// planRelease enumerates, in sorted key order, the buckets the claim's
// granted allocations reserved capacity in, and how much to return to each.
//
// Bucket keys and amounts come from the claim's spec, which is immutable,
// exactly as planAllocation computed them at Allocate time. The status only
// says which resource types were granted. status.allocations[].
// allocatingBucket and allocatedAmount are not used: anyone allowed to write
// quotaclaims/status could point them at another consumer's bucket or
// inflate the amount, and a release would then drain capacity that claim
// never held. The QuotaClaim storage also rejects status writes that change
// the allocation outcome (quotaclaim.allocationOutcomeChanged), so the
// granted set itself is what Allocate recorded.
func (a *PostgresAllocator) planRelease(claim *quotainternal.QuotaClaim) []*plannedBucket {
	granted := make(map[string]bool, len(claim.Status.Allocations))
	for _, alloc := range claim.Status.Allocations {
		if alloc.Status == quotainternal.QuotaClaimAllocationStatusGranted {
			granted[alloc.ResourceType] = true
		}
	}
	if len(granted) == 0 {
		return nil
	}
	all := a.planAllocation(claim)
	planned := all[:0]
	for _, p := range all {
		if granted[p.resourceType] && p.amount != 0 {
			planned = append(planned, p)
		}
	}
	// planAllocation returns them sorted by key already.
	return planned
}

// buildReleaseCTE assembles the compound CTE that deletes the claim row and,
// only if that DELETE removed a row, decrements every bucket and writes all
// changelog entries. The caller has already locked the bucket rows in sorted
// key order earlier in the same transaction.
//
// Shape (for N buckets):
//
//	WITH
//	  claim_del AS (
//	    DELETE FROM quota_objects
//	     WHERE key = $1 [AND resource_version = $2] [AND data_json->'metadata'->>'uid' = $3]
//	    RETURNING data
//	  ),
//	  b0_updated AS (
//	    UPDATE quota_objects qo
//	       SET data = <status || {allocated - $a, available, claimCount - 1, ...}>::text::bytea, ...
//	     WHERE qo.key = $k AND EXISTS (SELECT 1 FROM claim_del)
//	    RETURNING qo.resource_version, qo.key, qo.data
//	  ),
//	  b0_log AS (...),
//	  claim_log AS (INSERT ... 'DELETED' ... FROM claim_del)
//	SELECT (SELECT count(*) FROM claim_del),
//	       (SELECT resource_version FROM quota_objects WHERE key = $1)
//
// Two concurrent releases of the same claim: the second blocks on the first's
// bucket or claim row lock; once it proceeds its DELETE matches no row, so
// claim_del is empty and every bucket UPDATE is a no-op. Buckets deleted
// since the allocation are skipped (the UPDATE matches nothing).
//
// The outer SELECT's second column reads the statement snapshot, which still
// shows the claim row if it existed when the statement began; the caller
// uses it to tell "already gone" (NULL) from "precondition mismatch".
//
// The GREATEST clamps prevent negative allocated, available and
// claimCount. available can only go negative when the limit was lowered
// below what is allocated; decisions use limit - allocated, not available.
func buildReleaseCTE(plans []*plannedBucket, cKey, uid string, rv *int64) (string, []interface{}) {
	var sb strings.Builder
	args := make([]interface{}, 0, 2*len(plans)+3)
	next := func(v interface{}) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	cKeyPH := next(cKey)
	sb.WriteString("WITH claim_del AS (DELETE FROM quota_objects WHERE key = ")
	sb.WriteString(cKeyPH)
	if rv != nil {
		fmt.Fprintf(&sb, " AND resource_version = %s", next(*rv))
	}
	if uid != "" {
		fmt.Fprintf(&sb, " AND data_json->'metadata'->>'uid' = %s", next(uid))
	}
	sb.WriteString(" RETURNING data), ")

	allocated := bucketStatusExpr("allocated", "bigint")
	limit := bucketStatusExpr("limit", "bigint")
	claimCount := bucketStatusExpr("claimCount", "int")

	for i, p := range plans {
		keyPH := next(p.key)
		amtPH := next(p.amount)
		newAllocated := fmt.Sprintf("GREATEST(%s - %s::bigint, 0)", allocated, amtPH)

		fmt.Fprintf(&sb,
			"b%d_updated AS ("+
				"UPDATE quota_objects qo "+
				"SET data = ("+bucketStatusPatchOpen+
				"'allocated', %s, "+
				"'available', GREATEST(%s - %s, 0), "+
				"'claimCount', GREATEST(%s - 1, 0), "+
				bucketStatusPatchClose+")::text::bytea, "+
				"resource_version = quota_next_rv(), "+
				"updated_at = NOW() "+
				"WHERE qo.key = %s AND EXISTS (SELECT 1 FROM claim_del) "+
				"RETURNING qo.resource_version, qo.key, qo.data), ",
			i,
			newAllocated,
			limit, newAllocated,
			claimCount,
			keyPH,
		)
		fmt.Fprintf(&sb,
			"b%d_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) "+
				"SELECT key, resource_version, 'MODIFIED', data FROM b%d_updated "+
				"RETURNING resource_version), ",
			i, i,
		)
	}

	fmt.Fprintf(&sb,
		"claim_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) "+
			"SELECT %s, quota_next_rv(), 'DELETED', data FROM claim_del "+
			"RETURNING resource_version) ",
		cKeyPH,
	)
	fmt.Fprintf(&sb,
		"SELECT (SELECT count(*) FROM claim_del), (SELECT resource_version FROM quota_objects WHERE key = %s)",
		cKeyPH,
	)
	return sb.String(), args
}

// ptr returns a pointer to its argument. Convenience helper.
func ptr[T any](v T) *T { return &v }

// generateUID returns a random UID. Defensive fallback when the request
// flow hasn't already assigned one. Uses time-based generation rather than
// a UUID library to avoid pulling in another dependency.
func generateUID() types.UID {
	return types.UID(fmt.Sprintf("alloc-%d", time.Now().UnixNano()))
}

// bucketNameFor computes the deterministic QuotaBucket name for a given
// (resourceType, consumerRef) pair. It calls the same quotainternal.BucketName
// the bucket controllers use, so the allocator locates the bucket row
// without a lookup table and the two cannot drift.
func bucketNameFor(resourceType string, ownerRef quotainternal.ConsumerRef) string {
	return quotainternal.BucketName(resourceType, ownerRef.APIGroup, ownerRef.Kind, ownerRef.Namespace, ownerRef.Name)
}

// bucketNamespaceFor determines the namespace where a QuotaBucket lives for
// the given consumer reference. If the consumer ref carries a namespace the
// bucket is co-located with it; otherwise it falls back to the system
// namespace (WithBucketNamespace, default DefaultBucketNamespace). This
// mirrors the bucket controller's placement logic.
func (a *PostgresAllocator) bucketNamespaceFor(consumerRef quotainternal.ConsumerRef) string {
	if consumerRef.Namespace != "" {
		return consumerRef.Namespace
	}
	if a.bucketNamespace == "" {
		return DefaultBucketNamespace
	}
	return a.bucketNamespace
}
