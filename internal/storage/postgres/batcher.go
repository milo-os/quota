package postgres

// Group commit for the allocator.
//
// Every allocator write draws resource versions under the global
// commit-order lock, and that lock is held through COMMIT, so
// one transaction at a time passes through it and each pays a commit flush.
// The batcher amortizes both: concurrent Allocate and Release calls in one
// process are combined into a single transaction.
//
// Batch formation is natural batching. There is no timer. A runner goroutine
// takes everything queued (up to the max batch size) and commits it; callers
// that arrive while it is in flight queue up, and when it finishes it takes
// the next batch. At low load a batch holds one operation, and a
// one-operation batch runs the ordinary single-claim protocol in
// allocator.go. When nothing is queued and a batch slot is free, the caller
// runs its own one-operation batch on its own goroutine (no handoff).
//
// Up to maxInFlightBatches runners exist at once, so one batch can take its
// row locks (a round trip that does not hold the commit-order lock) while
// another is in its write round trip (which does). Two batches in flight
// measured about 35% more throughput than one on disjoint buckets and the
// same on a single shared bucket; three or four were slower, because batches
// got smaller and contended for the commit-order lock again.
//
// That overlap only exists when the batches touch different buckets: a batch
// that needs a bucket row another in-flight batch holds waits on it
// (Lock:transactionid) until that batch commits. So a batch is formed only
// from operations whose buckets no in-flight batch holds. The others stay
// queued, in order, for the batch holding their buckets to take when it
// finishes.
//
// A multi-operation batch is one transaction with two pipelined round trips:
//
//  1. BEGIN + one `SELECT ... FOR UPDATE` per key in the union of every
//     bucket and every claim key in the batch (released or created), in
//     globally sorted key order: the same ordering rule as the single-claim
//     paths, the Store and other replicas. Bucket keys sort before claim
//     keys, so this is also "buckets first, then claim rows". For a claim
//     being created the lock doubles as the existence check: a missing row
//     locks nothing, an existing row means AlreadyExists.
//  2. Go-side: every operation is decided in arrival order against a running
//     copy of the locked bucket state, so two claims against the same bucket
//     cannot over-allocate, and a release ahead of a claim in the batch frees
//     capacity for it. The batch is equivalent to running its operations
//     serially in arrival order. Per-operation outcomes are exactly the
//     single-claim ones: Forbidden for a missing bucket, AlreadyExists for a
//     duplicate name, Granted/Denied per request, and for releases
//     released/already gone/Conflict. A rejected operation writes nothing and
//     does not affect the others.
//  3. One constant-text CTE writes the final state of each touched bucket
//     once (guarded by the resource version read under the row lock),
//     deletes the released claims, inserts the new claims with their final
//     status, writes every changelog row with quota_next_rv(), then COMMIT.
//     An in-statement assertion aborts the transaction unless every guarded
//     bucket update and claim delete matched its row.
//
// The writer contract (migrations/README.md) holds: all row locks are taken by the
// statements in step 1, which draw no RV; the only statement that draws RVs
// touches only rows already locked by this transaction, or inserts new claim
// keys; and no client round trip happens between the first RV draw and
// COMMIT.
//
// If the batch transaction fails for any reason (a claim name inserted
// concurrently by another writer, a 40001 from the commit-order trigger, a
// lost connection), each operation is retried on its own through the
// single-claim path, so one bad operation never turns into errors for the
// rest of the batch. The one ambiguous case is a connection lost after
// COMMIT was sent: if the batch did commit, the retries report what they
// find (AlreadyExists for a created claim, already gone for a released one),
// never a second allocation -- the same ambiguity a single-claim request
// has when its COMMIT response is lost.
//
// Cancellation: a caller whose context ends while its operation is still
// queued is dropped from the queue and gets an error. Once the operation has
// been taken into a batch the outcome stands -- the caller waits for it --
// just as a cancelled single-claim request may still commit. A queued batch
// runs on a detached context bounded by batchTimeout; the idle fast path
// runs on the caller's context, exactly as the unbatched path does.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/internal/metrics"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

// maxInFlightBatches is how many batch transactions one process runs at once.
// See the file comment.
const maxInFlightBatches = 2

// batchTimeout bounds one batch transaction, which is no longer tied to any
// one request's context. It is above the enforcement admission wait (25s by
// default) so a waiting caller times out on its own deadline first.
const batchTimeout = 30 * time.Second

type batchOpKind int

const (
	opAllocate batchOpKind = iota
	opRelease
)

type batchOpState int

const (
	opQueued batchOpState = iota
	opTaken
	opCancelled
)

// batchOp is one queued Allocate or Release.
type batchOp struct {
	kind    batchOpKind
	claim   *quotainternal.QuotaClaim
	cKey    string
	planned []*plannedBucket
	// rv is the release precondition (nil: unconditional).
	rv *int64

	// state is guarded by batcher.mu.
	state batchOpState
	// done receives exactly one result once the op has been taken.
	done chan batchResult
	// answered is set by the goroutine that sends on done.
	answered bool
}

// answer delivers op's result. Only the runner (or one fallback goroutine)
// handling op calls it.
func (op *batchOp) answer(r batchResult) {
	op.answered = true
	op.done <- r
}

type batchResult struct {
	claim    *quotainternal.QuotaClaim
	released bool
	err      error
}

// batcher queues allocator operations and commits them in groups, with at
// most maxInFlightBatches batches in flight.
type batcher struct {
	a   *PostgresAllocator
	max int

	mu      sync.Mutex
	queue   []*batchOp
	running int // runner goroutines, at most maxInFlightBatches
	// held counts, per bucket key, the in-flight batches that will lock it.
	held map[string]int

	// Test hooks. onBatch sees every batch size before it runs;
	// beforeWrite runs between the decision and the write round trip of a
	// multi-operation batch.
	onBatch     func(size int)
	beforeWrite func()
	onFallback  func(err error)
}

func newBatcher(a *PostgresAllocator, maxBatch int) *batcher {
	return &batcher{a: a, max: maxBatch}
}

func (b *batcher) allocate(ctx context.Context, claim *quotainternal.QuotaClaim, planned []*plannedBucket) (*quotainternal.QuotaClaim, error) {
	r := b.submit(ctx, &batchOp{
		kind:    opAllocate,
		claim:   claim,
		cKey:    claimKey(claim.Namespace, claim.Name),
		planned: planned,
	})
	return r.claim, r.err
}

func (b *batcher) release(ctx context.Context, claim *quotainternal.QuotaClaim, planned []*plannedBucket, rv *int64) (bool, error) {
	r := b.submit(ctx, &batchOp{
		kind:    opRelease,
		claim:   claim,
		cKey:    claimKey(claim.Namespace, claim.Name),
		planned: planned,
		rv:      rv,
	})
	return r.released, r.err
}

func cancelledErr(ctx context.Context) error {
	return storage.NewInternalError(fmt.Errorf("quota allocation cancelled before it was sent: %w", context.Cause(ctx)))
}

// submit queues op and waits for its outcome.
func (b *batcher) submit(ctx context.Context, op *batchOp) batchResult {
	if ctx.Err() != nil {
		return batchResult{err: cancelledErr(ctx)}
	}
	op.done = make(chan batchResult, 1)

	b.mu.Lock()
	if len(b.queue) == 0 && b.running < maxInFlightBatches && !b.conflictsLocked(op) {
		// Idle fast path: nothing to batch with and a batch slot is free,
		// so run the one-operation batch on the caller's goroutine and
		// context -- exactly the unbatched path, including its
		// cancellation behavior. Handing it to a runner goroutine on a
		// detached context instead measurably added idle-path latency (a
		// cross-thread wake per request, plus pgx's per-query context
		// watcher). Arrivals in the meantime queue and start their own
		// runner, since this call holds only one of the slots.
		b.running++
		op.state = opTaken
		ops := []*batchOp{op}
		b.holdLocked(ops, 1)
		b.mu.Unlock()
		b.execute(ctx, ops)
		b.mu.Lock()
		b.holdLocked(ops, -1)
		b.running--
		if len(b.queue) > 0 && b.running < maxInFlightBatches {
			b.running++
			go b.run()
		}
		b.mu.Unlock()
		return <-op.done
	}
	b.queue = append(b.queue, op)
	if b.running < maxInFlightBatches {
		b.running++
		go b.run()
	}
	b.mu.Unlock()

	select {
	case r := <-op.done:
		return r
	case <-ctx.Done():
	}

	b.mu.Lock()
	if op.state == opQueued {
		// Still waiting for a batch: drop it. The runner discards
		// cancelled ops when it next forms a batch.
		op.state = opCancelled
		b.mu.Unlock()
		return batchResult{err: cancelledErr(ctx)}
	}
	b.mu.Unlock()
	// Already part of a batch: the outcome stands.
	return <-op.done
}

// run commits batches until it finds nothing it can take. Invariant:
// whenever the queue is non-empty, at least one batch slot is in use (a
// runner goroutine, or a caller on the idle fast path that starts one when
// it finishes). A runner that finds only operations whose buckets another
// batch holds may exit: that batch's slot is in use, and it takes them when
// it finishes.
func (b *batcher) run() {
	var ops []*batchOp
	for {
		b.mu.Lock()
		b.holdLocked(ops, -1)
		ops = b.takeLocked()
		if len(ops) == 0 {
			b.running--
			b.mu.Unlock()
			return
		}
		b.holdLocked(ops, 1)
		b.mu.Unlock()
		b.executeDetached(ops)
	}
}

// holdLocked adds delta to the hold count of every bucket ops lock.
func (b *batcher) holdLocked(ops []*batchOp, delta int) {
	for _, op := range ops {
		for _, p := range op.planned {
			if b.held == nil {
				b.held = make(map[string]int)
			}
			if b.held[p.key] += delta; b.held[p.key] <= 0 {
				delete(b.held, p.key)
			}
		}
	}
}

// conflictsLocked reports whether an in-flight batch holds one of op's
// buckets.
func (b *batcher) conflictsLocked(op *batchOp) bool {
	for _, p := range op.planned {
		if b.held[p.key] > 0 {
			return true
		}
	}
	return false
}

// executeDetached runs a queued batch. It is not tied to any one caller's
// context, so it runs on a detached one bounded by batchTimeout.
func (b *batcher) executeDetached(ops []*batchOp) {
	ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
	defer cancel()
	b.execute(ctx, ops)
}

// takeLocked removes up to b.max operations from the queue, in arrival order.
// Cancelled operations are discarded. An operation is left queued for a
// later batch when an in-flight batch holds one of its buckets, or when its
// claim key is already in the batch unless both are creates (the later
// create then gets AlreadyExists within the batch): one transaction never
// deletes and inserts, or deletes twice, the same row.
func (b *batcher) takeLocked() []*batchOp {
	var ops []*batchOp
	var keys map[string]batchOpKind
	rest := b.queue[:0]
	for _, op := range b.queue {
		if op.state == opCancelled {
			continue
		}
		if len(ops) == b.max || b.conflictsLocked(op) {
			rest = append(rest, op)
			continue
		}
		if keys == nil {
			keys = make(map[string]batchOpKind)
		}
		if prev, ok := keys[op.cKey]; ok && (prev != opAllocate || op.kind != opAllocate) {
			rest = append(rest, op)
			continue
		}
		keys[op.cKey] = op.kind
		op.state = opTaken
		ops = append(ops, op)
	}
	// Clear the tail so dropped ops can be collected.
	for i := len(rest); i < len(b.queue); i++ {
		b.queue[i] = nil
	}
	b.queue = rest
	return ops
}

// answerPanic turns a panic while handling ops into an InternalError for
// every op not yet answered. In the single-claim path a panic is recovered by
// the apiserver's request handler; a runner goroutine has no such handler,
// and an unrecovered panic there would crash the process and strand every
// waiter.
func answerPanic(ops ...*batchOp) {
	p := recover()
	if p == nil {
		return
	}
	err := fmt.Errorf("panic in quota allocation batch: %v", p)
	klog.ErrorS(err, "Recovered panic in quota allocation batch", "batchSize", len(ops))
	for _, op := range ops {
		if !op.answered {
			op.answer(batchResult{err: storage.NewInternalError(err)})
		}
	}
}

// execute commits one batch and delivers every operation's result.
func (b *batcher) execute(ctx context.Context, ops []*batchOp) {
	defer answerPanic(ops...)
	metrics.QuotaAllocationBatchSize.Observe(float64(len(ops)))
	if b.onBatch != nil {
		b.onBatch(len(ops))
	}

	if len(ops) == 1 {
		ops[0].answer(b.a.runSingle(ctx, ops[0]))
		return
	}

	results, err := b.a.commitBatch(ctx, ops, b.beforeWrite)
	if err == nil {
		for i, op := range ops {
			op.answer(results[i])
		}
		return
	}

	klog.V(2).InfoS("Batched quota allocation failed; retrying each operation on its own",
		"batchSize", len(ops), "err", err)
	if b.onFallback != nil {
		b.onFallback(err)
	}
	fctx, fcancel := context.WithTimeout(context.Background(), batchTimeout)
	defer fcancel()
	var wg sync.WaitGroup
	for _, op := range ops {
		wg.Add(1)
		go func(op *batchOp) {
			defer wg.Done()
			defer answerPanic(op)
			op.answer(b.a.runSingle(fctx, op))
		}(op)
	}
	wg.Wait()
}

// runSingle runs one operation through the single-claim protocol.
func (a *PostgresAllocator) runSingle(ctx context.Context, op *batchOp) batchResult {
	if op.kind == opRelease {
		released, err := a.releaseOne(ctx, op.claim, op.planned, op.rv)
		return batchResult{released: released, err: err}
	}
	claim, err := a.commitAllocation(ctx, op.claim, op.planned, false)
	return batchResult{claim: claim, err: err}
}

const (
	// planStableSQL makes the batch transaction's plans independent of
	// table statistics. A generic plan cached while quota_objects was empty
	// or tiny (a fresh deployment, or statistics from that time) is a
	// sequential scan for every lookup shape here -- `key = $1 ... FOR
	// UPDATE`, unnest joins, `key = ANY($1)` -- and keeps being reused as
	// the table grows: about 30 full scans per batch. With sequential and
	// bitmap scans disabled the only remaining path for these statements is
	// the primary-key index scan, so the cached generic plan is that, at any
	// table size. Every statement text that runs under this setting runs
	// only in batch transactions, so its cached plans are always built under
	// it. (Forcing custom plans instead also works, but replanning ~30
	// statements per batch cost 10-18% of batched throughput.) SET LOCAL
	// ends with the transaction.
	planStableSQL = `SELECT set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true)`

	// lockBucketForBatchSQL locks a bucket row and returns everything the
	// batch needs to compute and write its final state. The resource
	// version guards the batch's UPDATE. A missing row yields
	// pgx.ErrNoRows.
	lockBucketForBatchSQL = `SELECT resource_version, ` +
		`COALESCE((data_json->'status'->>'allocated')::bigint, 0), ` +
		`COALESCE((data_json->'status'->>'limit')::bigint, 0), ` +
		`COALESCE((data_json->'status'->>'claimCount')::bigint, 0) ` +
		`FROM quota_objects WHERE key = $1 FOR UPDATE`

	// lockClaimForBatchSQL locks a claim row in the batch and returns what
	// release preconditions are checked against. For a claim being created
	// it is the existence check. It has the same `key = $1 ... FOR UPDATE`
	// shape as every other lock statement here; a standalone existence
	// probe (`EXISTS (...)`, `key = ANY($1)`) was tried and, with the
	// statistics of a small, churning table, got generic plans that scanned
	// the whole collated key index.
	lockClaimForBatchSQL = `SELECT resource_version, data_json->'metadata'->>'uid' ` +
		`FROM quota_objects WHERE key = $1 FOR UPDATE`

	// commitBatchSQL writes a whole batch. Every input is an array, so the
	// statement text is constant and stays prepared. Rows are found with
	// `key = ANY(keys)` (a primary-key index scan under planStableSQL;
	// joining quota_objects to unnest() is not plan-stable even then) and
	// each row's values are taken from the parallel arrays by
	// array_position, which is fine for batch-sized arrays.
	//
	//	$1..$4   buckets: key, locked rv, final allocated, final claimCount
	//	$5..$6   released claims: key, locked rv
	//	$7..$10  created claims: key, namespace, name, data
	//	$11,$12  expected bucket updates and claim deletes
	//
	// Bucket updates and claim deletes are guarded by the resource version
	// read under this transaction's row lock, so they cannot match a version
	// this batch did not decide against. The last output column divides by
	// zero (SQLSTATE 22012, aborting the transaction before COMMIT) unless
	// every guarded write matched. Data-modifying CTEs always run, so the
	// changelog inserts need no reference from the outer SELECT.
	commitBatchSQL = `WITH ` +
		`b_upd AS (UPDATE quota_objects qo ` +
		`SET data = (` + bucketStatusPatchOpen +
		`'allocated', ($3::bigint[])[array_position($1::text[], qo.key)], ` +
		`'available', GREATEST(COALESCE((qo.data_json->'status'->>'limit')::bigint, 0) - ($3::bigint[])[array_position($1::text[], qo.key)], 0), ` +
		`'claimCount', ($4::bigint[])[array_position($1::text[], qo.key)], ` +
		bucketStatusPatchClose + `)::text::bytea, ` +
		`resource_version = quota_next_rv(), ` +
		`updated_at = NOW() ` +
		`WHERE qo.key = ANY($1::text[]) AND qo.resource_version = ($2::bigint[])[array_position($1::text[], qo.key)] ` +
		`RETURNING qo.key, qo.resource_version, qo.data), ` +
		`b_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) ` +
		`SELECT key, resource_version, 'MODIFIED', data FROM b_upd), ` +
		`c_del AS (DELETE FROM quota_objects qo ` +
		`WHERE qo.key = ANY($5::text[]) AND qo.resource_version = ($6::bigint[])[array_position($5::text[], qo.key)] ` +
		`RETURNING qo.key, qo.data), ` +
		`c_del_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) ` +
		`SELECT key, quota_next_rv(), 'DELETED', data FROM c_del), ` +
		`c_ins AS (INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data, created_at, updated_at) ` +
		`SELECT d.key, quota_next_rv(), '` + claimsKind + `', d.namespace, d.name, d.data, NOW(), NOW() ` +
		`FROM unnest($7::text[], $8::text[], $9::text[], $10::bytea[]) AS d(key, namespace, name, data) ` +
		`RETURNING key, resource_version, data), ` +
		`c_log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data) ` +
		`SELECT key, resource_version, 'ADDED', NULL FROM c_ins) ` +
		`SELECT COALESCE((SELECT array_agg(key) FROM c_ins), '{}'::text[]), ` +
		`COALESCE((SELECT array_agg(resource_version) FROM c_ins), '{}'::bigint[]), ` +
		`1 / (CASE WHEN (SELECT count(*) FROM b_upd) = $11::bigint ` +
		`AND (SELECT count(*) FROM c_del) = $12::bigint THEN 1 ELSE 0 END)`
)

// batchBucket is the running state of one locked bucket within a batch.
type batchBucket struct {
	exists     bool
	lockedRV   int64
	allocated  int64
	limit      int64
	claimCount int64
	dirty      bool
}

// batchClaimRow is the locked state of a claim row being released.
type batchClaimRow struct {
	exists bool
	rv     int64
	uid    *string
}

// batchLock is one row lock in batch 1, in sorted key order.
type batchLock struct {
	key    string
	bucket *batchBucket
	claim  *batchClaimRow
}

// batchWrites collects the batch CTE's array parameters.
type batchWrites struct {
	bucketKeys, delKeys, insKeys     []string
	bucketRVs, allocated, claimCount []int64
	delRVs                           []int64
	insNamespaces, insNames          []string
	insData                          [][]byte
	insOps                           []int // op index per created claim
}

func (w *batchWrites) empty() bool {
	return len(w.bucketKeys) == 0 && len(w.delKeys) == 0 && len(w.insKeys) == 0
}

// batchLocks builds the sorted lock list: the union of every bucket and
// every claim key (released or created) in the batch.
func batchLocks(ops []*batchOp) (locks []batchLock, buckets map[string]*batchBucket, claimRows map[string]*batchClaimRow) {
	buckets = make(map[string]*batchBucket)
	claimRows = make(map[string]*batchClaimRow)
	for _, op := range ops {
		for _, p := range op.planned {
			if _, ok := buckets[p.key]; !ok {
				buckets[p.key] = &batchBucket{}
				locks = append(locks, batchLock{key: p.key, bucket: buckets[p.key]})
			}
		}
		if _, ok := claimRows[op.cKey]; !ok {
			claimRows[op.cKey] = &batchClaimRow{}
			locks = append(locks, batchLock{key: op.cKey, claim: claimRows[op.cKey]})
		}
	}
	// The single global lock order. Bucket keys sort before claim keys
	// ("quotabuckets" < "quotaclaims"), so this is also buckets first.
	sort.Slice(locks, func(i, j int) bool { return locks[i].key < locks[j].key })
	return locks, buckets, claimRows
}

// commitBatch runs one multi-operation batch transaction. A non-nil error
// means nothing was committed with certainty and the caller must fall back;
// otherwise results holds each operation's outcome.
//
// beforeWrite, if set, runs after the decision and before the write round
// trip (tests use it to race a concurrent writer).
func (a *PostgresAllocator) commitBatch(ctx context.Context, ops []*batchOp, beforeWrite func()) ([]batchResult, error) {
	locks, buckets, claimRows := batchLocks(ops)
	var results []batchResult
	var writes *batchWrites

	err := a.withPgxConn(ctx, func(conn *pgx.Conn) (fnErr error) {
		defer func() {
			if p := recover(); p != nil {
				// Do not hand the connection back mid-transaction.
				rollback(ctx, conn, "allocate-batch")
				panic(p)
			}
			if fnErr != nil {
				rollback(ctx, conn, "allocate-batch")
			}
		}()

		// Batch 1: BEGIN + plan settings + row locks in sorted key order.
		batch := &pgx.Batch{}
		batch.Queue("BEGIN ISOLATION LEVEL READ COMMITTED")
		batch.Queue(planStableSQL)
		for _, l := range locks {
			if l.bucket != nil {
				batch.Queue(lockBucketForBatchSQL, l.key)
			} else {
				batch.Queue(lockClaimForBatchSQL, l.key)
			}
		}
		res := conn.SendBatch(ctx, batch)
		err := readBatchLocks(res, locks)
		if closeErr := res.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close lock batch: %w", closeErr)
		}
		if err != nil {
			return err
		}

		results, writes = a.decideBatch(ops, buckets, claimRows)
		if writes.empty() {
			if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
				return fmt.Errorf("end read-only batch: %w", err)
			}
			return nil
		}

		if beforeWrite != nil {
			beforeWrite()
		}

		// Batch 2: CTE + COMMIT.
		batch = &pgx.Batch{}
		batch.Queue(commitBatchSQL,
			writes.bucketKeys, writes.bucketRVs, writes.allocated, writes.claimCount,
			writes.delKeys, writes.delRVs,
			writes.insKeys, writes.insNamespaces, writes.insNames, writes.insData,
			int64(len(writes.bucketKeys)), int64(len(writes.delKeys)),
		)
		batch.Queue("COMMIT")
		res = conn.SendBatch(ctx, batch)
		var insKeys []string
		var insRVs []int64
		var assert int64
		if err := res.QueryRow().Scan(&insKeys, &insRVs, &assert); err != nil {
			_ = res.Close()
			if isDivisionByZero(err) {
				klog.ErrorS(err, "Batched quota write did not match a row locked by this transaction", "batchSize", len(ops))
			}
			return fmt.Errorf("batch cte: %w", err)
		}
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return fmt.Errorf("commit: %w", err)
		}
		if err := res.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		rvByKey := make(map[string]int64, len(insKeys))
		for i, k := range insKeys {
			if i < len(insRVs) {
				rvByKey[k] = insRVs[i]
			}
		}
		for _, i := range writes.insOps {
			op := ops[i]
			rv, ok := rvByKey[op.cKey]
			if !ok {
				// Committed, but the statement did not report this claim.
				// Cannot happen: the INSERT either adds every row or fails.
				results[i] = batchResult{err: storage.NewInternalError(fmt.Errorf("batch commit did not return claim %s", op.cKey))}
				continue
			}
			op.claim.ResourceVersion = strconv.FormatInt(rv, 10)
			results[i] = batchResult{claim: op.claim}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// readBatchLocks consumes batch 1: BEGIN, the plan settings, then one result
// per lock. It fills the lock targets in place.
func readBatchLocks(res pgx.BatchResults, locks []batchLock) error {
	if _, err := res.Exec(); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if _, err := res.Exec(); err != nil {
		return fmt.Errorf("set plan settings: %w", err)
	}
	for _, l := range locks {
		var err error
		if l.bucket != nil {
			b := l.bucket
			err = res.QueryRow().Scan(&b.lockedRV, &b.allocated, &b.limit, &b.claimCount)
			b.exists = err == nil
		} else {
			c := l.claim
			err = res.QueryRow().Scan(&c.rv, &c.uid)
			c.exists = err == nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock %s: %w", l.key, err)
		}
	}
	return nil
}

// decideBatch decides every operation in arrival order against the running
// bucket state and returns per-operation results (final for rejected
// operations and releases, filled in after COMMIT for created claims) and the
// writes to commit. It touches no database state.
func (a *PostgresAllocator) decideBatch(
	ops []*batchOp,
	buckets map[string]*batchBucket,
	claimRows map[string]*batchClaimRow,
) ([]batchResult, *batchWrites) {
	results := make([]batchResult, len(ops))
	w := &batchWrites{}
	created := make(map[string]bool)

	for i, op := range ops {
		switch op.kind {
		case opAllocate:
			results[i] = a.decideBatchAllocate(i, op, buckets, claimRows[op.cKey].exists, created, w)
		case opRelease:
			results[i] = decideBatchRelease(op, buckets, claimRows[op.cKey], w)
		}
	}

	// Each touched bucket is written once, with its final state. Map order
	// does not matter: these rows are already locked.
	for key, b := range buckets {
		if !b.dirty {
			continue
		}
		w.bucketKeys = append(w.bucketKeys, key)
		w.bucketRVs = append(w.bucketRVs, b.lockedRV)
		w.allocated = append(w.allocated, b.allocated)
		w.claimCount = append(w.claimCount, b.claimCount)
	}
	return results, w
}

// decideBatchAllocate applies the single-claim Allocate decision to one
// claim against the running bucket state.
func (a *PostgresAllocator) decideBatchAllocate(
	i int,
	op *batchOp,
	buckets map[string]*batchBucket,
	exists bool,
	created map[string]bool,
	w *batchWrites,
) batchResult {
	claim := op.claim
	// Missing bucket first, then duplicate name: the single-claim path
	// reports Forbidden before it ever reaches the INSERT.
	for _, p := range op.planned {
		if !buckets[p.key].exists {
			return batchResult{err: missingBucketError(claim, p)}
		}
	}
	if exists || created[op.cKey] {
		return batchResult{err: apierrors.NewAlreadyExists(claimsGR, claim.Name)}
	}

	// Decide every bucket before applying any delta. Planned buckets are
	// unique per claim, so this matches decideGrants exactly.
	granted := make(map[string]bool, len(op.planned))
	for _, p := range op.planned {
		b := buckets[p.key]
		if fits(b.allocated, p.amount, b.limit) {
			granted[p.resourceType] = true
		}
	}
	prepareClaim(claim)
	applyAllocationStatus(claim, op.planned, granted)
	data, err := runtime.Encode(a.codec, claim)
	if err != nil {
		return batchResult{err: storage.NewInternalError(fmt.Errorf("encode claim: %w", err))}
	}

	for _, p := range op.planned {
		if !granted[p.resourceType] {
			continue
		}
		b := buckets[p.key]
		b.allocated += p.amount
		b.claimCount++
		b.dirty = true
	}
	created[op.cKey] = true
	w.insKeys = append(w.insKeys, op.cKey)
	w.insNamespaces = append(w.insNamespaces, claim.Namespace)
	w.insNames = append(w.insNames, claim.Name)
	w.insData = append(w.insData, data)
	w.insOps = append(w.insOps, i)
	return batchResult{}
}

// decideBatchRelease applies the single-claim Release decision against the
// locked claim row: already gone, precondition mismatch (Conflict), or
// released, in which case the claim's granted amounts come off the running
// bucket state with the same clamps as the release CTE.
func decideBatchRelease(op *batchOp, buckets map[string]*batchBucket, row *batchClaimRow, w *batchWrites) batchResult {
	claim := op.claim
	if !row.exists {
		return batchResult{}
	}
	uidMismatch := claim.UID != "" && (row.uid == nil || *row.uid != string(claim.UID))
	if (op.rv != nil && *op.rv != row.rv) || uidMismatch {
		_, err := releaseOutcome(claim, 0, &row.rv)
		return batchResult{err: err}
	}
	for _, p := range op.planned {
		b := buckets[p.key]
		if !b.exists {
			// Deleted since the allocation: nothing to give back.
			continue
		}
		b.allocated = max(b.allocated-p.amount, 0)
		b.claimCount = max(b.claimCount-1, 0)
		b.dirty = true
	}
	w.delKeys = append(w.delKeys, op.cKey)
	w.delRVs = append(w.delRVs, row.rv)
	return batchResult{released: true}
}
