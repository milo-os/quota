# 0001 - Dual-backend quota allocation: controller-driven (etcd) and synchronous (postgres)

- **Status**: Accepted with caveats. Partly superseded by the [2026-11-22 addendum](#addendum-2026-11-22-commit-ordered-resource-versions-reinstate-a-global-lock): the xmin-horizon watch cursor described below was replaced by commit-ordered resource versions.
- **Date**: 2026-04-11 (same-day rewrite of the 2026-04-10 draft)
- **Deciders**: Quota service maintainers
- **Supersedes**: The 2026-04-10 draft of this ADR, which framed the design as
  "Postgres-first" and described the etcd path as legacy. That framing led to
  an over-eager cleanup pass that removed the etcd controller path entirely;
  this rewrite restores the dual-backend framing and records the architecture
  as it actually stands.

## Context

### Extraction from Milo

The quota service was extracted from Milo as a standalone, Kubernetes-native
quota ledger. The extraction charter (see `CLAUDE.md`) required that the
service:

1. be consumable by any Datum control plane (Milo is the first consumer, not
   the only one);
2. keep the API surface Kubernetes-native so existing consumers continue to
   use `QuotaDefinition`, `QuotaGrant`, `QuotaClaim`, and `QuotaBucket` with
   no new SDK; and
3. support multiple storage backends so the ledger can run on etcd (matching
   Milo's current topology) or on a relational store for deployments that
   need cross-row transactions.

The service ships three backend selectors via `--quota-storage-backend`
(`cmd/quota/serve.go`):

| Backend    | Allocation path                         | Intended use                   |
|------------|------------------------------------------|--------------------------------|
| `etcd`     | Controller-driven (async)                | Default; matches Milo          |
| `postgres` | Synchronous transactional allocator      | Sync claim status, cross-row   |
| `dual`     | Etcd primary + Postgres shadow           | Migration validation tool      |

The default is `etcd` — set in `NewQuotaServerOptions` and asserted in the
shipped deployment manifest (`config/base/deployment.yaml`, `STORAGE_BACKEND`
env var). The dev overlay (`config/overlays/dev/kustomization.yaml`)
composes the `etcd` component, not the `postgres` component; an operator
running `task dev:setup` gets etcd.

### Why two backends, and not one

The original extraction carried Milo's controller-driven topology forward
as-is. Every claim CREATE hit etcd with an empty status, woke up the
`QuotaBucketController` via a watch, and the controller reconciled bucket
arithmetic and per-allocation status fields back onto both objects. Users
observed an eventual-consistency window between the `201 Created` and the
point at which `status.conditions[Granted]` became terminal — typically tens
of milliseconds, occasionally much more under contention.

A performance investigation in early 2026 explored whether Postgres could
remove that window by running allocation inside the user's request. The
motivation was:

- **Cross-row transactions**: etcd's Kubernetes contract is single-key
  optimistic concurrency. A claim that reserves capacity in three buckets
  cannot commit atomically across all three — the controller path has to
  approximate this with per-bucket reconciles and accept that a crash
  mid-pump leaves state wedged between buckets.
- **Synchronous claim status**: for users who want to create a claim and
  then immediately create the resource that claim authorizes, the pending
  window forces them to poll or watch. Synchronous allocation collapses the
  round trip.
- **Decoupling from etcd's single-key concurrency model**: relational
  storage opens the door to features that are not expressible on etcd (hard
  quotas across disjoint grants, priority classes that preempt lower-tier
  claims, etc.). None of these are implemented today, but the design leaves
  the door open.

The investigation did not conclude "Postgres is always better." It concluded
that each backend has a different bottleneck and different operational
story, and that picking one would either force etcd users onto Postgres
(operational burden) or block Postgres adoption (synchronous claim status
remains out of reach forever). The honest answer is to support both.

### Benchmark history

The numbers below come from a repeated 20-VU k6 workload issuing claim POSTs
against a fixed set of buckets. They are load-specific and capture the
bottleneck of each configuration, not an absolute "fastest" ranking.

| Configuration                                                | Claims/s | Notes                                             |
|--------------------------------------------------------------|---------:|---------------------------------------------------|
| Postgres, controllers on, naive poll-based watch             |     12.8 | Initial port; watch poll dominates                |
| Postgres, controllers on, `synchronous_commit=off` + RWP     |     43.5 | Upper bound of the controller-on Postgres path    |
| Postgres, controllers on, LISTEN/NOTIFY changelog driver     |     38.1 | Steady state with push watch                      |
| Postgres, sync allocator + controllers on (lock contention)  |     32.2 | Two writers racing on the same rows               |
| Postgres, sync allocator, controllers off (round 1, FOR UPDATE) |  37.6 | Pessimistic row locks; clean split                |
| Postgres, sync allocator, controllers off (round 2, CAS)     |     32.2 | CAS replaced FOR UPDATE; throughput dropped       |
| Postgres, sync allocator, controllers off (round 3, CAS)     |     30.3 | Further CAS tuning; still regressing              |
| Postgres, sync allocator, controllers off (round 4, CAS)     |     24.3 | Continued regression                              |
| Postgres, sync allocator, controllers off (round 5, CAS)     |    13.85 | Exhaustive measurement; 31% rollback rate         |
| Etcd reference (controller-driven, Milo status quo)          |     40.6 | The throughput target                             |
| Postgres, sync allocator, controllers off (round 6, FOR UPDATE) |    TBD | Revert to row locks; benchmark pending            |

The shape of the history is the important thing: **CAS regressed throughput
every round it was in place (37.6 -> 32.2 -> 30.3 -> 24.3 -> 13.85
claims/s), while the round-1 implementation using `SELECT ... FOR UPDATE`
sustained 37.6 claims/s on the same workload.** Round-5 load profiling
recorded a 31% transaction rollback rate from CAS conflicts (777 of 2471
UPDATE statements), UPDATE mean latency of 67 ms against an uncontended
baseline of 0.6 ms, and an 82% CAS retry-exhaustion failure rate on the
hot-tenant scenario (40 VUs, 1 bucket). The birthday-paradox math lines
up with the observation: at 50 writers against 100 buckets the per-round
collision probability is ~39%, and retried work accumulates faster than
FOR UPDATE queue wait time.

Round 6 reverts to pessimistic row locking via `SELECT ... FOR UPDATE` on
every affected bucket, acquired in sorted key order to prevent deadlocks
across concurrent claims with overlapping bucket sets. The round-6
benchmark has not yet re-run against the etcd reference; the design is in
measurement. The headline p50 latency on a claim CREATE was better on
Postgres in earlier rounds (~112 ms vs ~204 ms on etcd, because the user no
longer waits for a controller round-trip), and FOR UPDATE is expected to
restore at least the round-1 throughput of 37.6 claims/s.

## Decision

### Dual-backend, first-class

Both backends are supported as first-class production options. Neither is
deprecated. The storage backend is selected per-deployment via
`--quota-storage-backend`.

### Etcd uses controller-driven allocation

When the backend is `etcd`:

- `QuotaClaim` CREATE writes the claim with an empty status and returns
  `201 Created`. The user observes `Granted=Unknown` until a controller
  reconciles.
- The bucket-allocation pump (part of `QuotaBucketController`'s current
  code, with its allocation behavior gated on a controller-manager flag —
  see below) owns the dynamic allocation state fields:
  `bucket.Status.Allocated`, `bucket.Status.Available`,
  `bucket.Status.ClaimCount`, `bucket.Status.LastReconciliation`, and the
  per-allocation `claim.Status.Allocations[]` entries.
- `QuotaClaimController` aggregates per-allocation statuses into the
  overall `claim.Status.Conditions[Granted]` condition.
- Typical eventual-consistency window: ~50 ms steady-state, hundreds of
  milliseconds under contention.

This matches Milo's original behavior bit-for-bit. Deployments that already
run etcd and do not want to adopt Postgres get exactly what they had
before.

### Postgres uses a synchronous transactional allocator

When the backend is `postgres`:

- `cmd/quota/serve.go` constructs a `PostgresAllocator`
  (`internal/storage/postgres/allocator.go`) against the same pooled
  `*sql.DB` and codec the Postgres Store uses, and threads it through
  `ExtraConfig.Allocator` to the apiserver config.
- `internal/apiserver/apiserver.go` checks for a non-nil allocator and, if
  present, wraps the `QuotaClaim` REST storage with
  `quotaclaim.NewAllocatingStorage`. On `etcd` and `dual` backends the
  allocator is nil and the apiserver falls through to the standard
  `quotaclaim.NewStorage`. There is no way to accidentally enable the
  allocator against etcd.
- `AllocatingREST.Create`
  (`internal/registry/quota/quotaclaim/allocating_storage.go`) populates
  system metadata, runs the standard `rest.BeforeCreate` pipeline
  (PrepareForCreate + Validate), then delegates to
  `Allocator.Allocate`, which runs the full allocation transaction in
  Postgres and returns the persisted claim with `Status.Allocations` and
  `Status.Conditions[Granted]` fully populated.
- `AllocatingREST.Delete` fetches the existing claim, runs delete
  validation, and calls `Allocator.Release`, which decrements the affected
  buckets and removes the claim row transactionally.
- The user's POST/DELETE response body contains the final allocation
  state. No controller round-trip. No watch echo. No eventual-consistency
  window on the CREATE side.

The allocator uses **pessimistic `SELECT ... FOR UPDATE` row locking** on
every affected bucket, acquired in sorted key order inside a single
transaction. There is no retry loop; each `Allocate` and `Release` is a
single-attempt commit.

1. `planAllocation` is a pure in-memory aggregation step. It walks
   `claim.Spec.Requests`, deduplicates by resource type, and produces the
   set of bucket keys the commit phase will need to touch. No database
   reads happen in `planAllocation`.
2. `commitAllocation` opens a short transaction, then for every unique
   bucket key, **sorted lexicographically**, issues
   `SELECT data, resource_version FROM quota_objects WHERE key = $1 FOR UPDATE`.
   Each statement acquires a row-level lock held until COMMIT. Because
   every allocator acquires its locks in the same sorted order, two
   concurrent claims with overlapping bucket sets cannot deadlock — the
   second one simply waits for the first to commit and then proceeds
   against the just-updated state.
3. With the row locks held, the allocator decodes each bucket, checks
   capacity in Go against the just-locked state, and builds the granted
   and denied request lists. Over-quota requests are marked `Denied` and
   their bucket UPDATE is skipped; the row lock is released at COMMIT
   either way.
4. For each granted bucket the allocator issues a plain UPDATE (no CAS
   predicate — the row lock guarantees nothing else has moved the row
   since the SELECT) and writes the bucket's changelog row. The claim row
   is then INSERTed and its changelog row written. The transaction
   commits.

`Release` uses the same single-transaction, single-attempt structure.
`planRelease` walks the claim's granted allocations to enumerate which
buckets need to be decremented; it also issues an unlocked claim-existence
probe so that a second DELETE on an already-released claim returns cleanly
without opening a transaction (idempotency). `commitRelease` then opens one
transaction, issues `SELECT ... FOR UPDATE` on every planned bucket in
sorted key order, applies every bucket decrement, deletes the claim row,
writes every bucket changelog row and the claim `DELETED` changelog row,
and commits. The sorted lock order matches `commitAllocation` so a Release
and an Allocate racing on overlapping bucket sets cannot deadlock either.

Multi-bucket atomicity follows directly from holding every affected bucket
row lock inside a single transaction: either all decrements (or
increments) commit together or none do. Writers touching disjoint bucket
sets never serialize on bucket locks. *(Superseded in part: since migration 006 every writer also takes a global commit-order lock for a short window, so claims against different buckets still wait on each other briefly. See the 2026-11-22 addendum.)*

*(Superseded by the 2026-11-22 addendum: the xmin-horizon cursor was not commit order, and migration 006 reinstated a writer-side lock.)*
The sequence-vs-commit-order hazard that once motivated a writer-side
advisory lock (round 2) was handled by the changelog watcher's
xmin-horizon cursor in `internal/watch/postgres.go`. That mitigation was
added in round 3, is orthogonal to the allocator's locking primitive, and
stays in place under the round-6 design. See the package comment on
`internal/storage/postgres/allocator.go` for the full rationale behind the
FOR UPDATE revert, including the round-5 measurement evidence.

### Controller-manager allocation-mode flag

The controller-manager subcommand (`cmd/quota/controller_manager.go`) is
the counterpart to the apiserver subcommand. It accepts a
`--quota-allocation-mode={controller|allocator}` flag (default
`controller`) that selects which writer owns the dynamic allocation state
on `QuotaBucket` and `QuotaClaim` status:

- `controller` (default): the bucket-allocation pump and claim status
  aggregator are registered. They own
  `bucket.Status.Allocated/Available/ClaimCount` and
  `claim.Status.Allocations[]` / `claim.Status.Conditions[Granted]`. This
  is the mode to run against the `etcd` backend.
- `allocator`: the bucket-allocation pump and claim status aggregator are
  NOT registered. The synchronous Postgres allocator owns those writes.
  This is the mode to run against the `postgres` backend.

`QuotaBucketController`'s grant-derived work — `Limit`, `GrantCount`,
`ContributingGrantRefs`, `ObservedGeneration`, and the safety-net bucket
re-creation via `ensureBucketFromGrants` — runs in both modes. Only the
allocation-state writes are gated. The controller uses Server-Side Apply
with the `quota-bucket-controller` field manager and intentionally omits
the allocation-state fields from its SSA patches so the allocator (whether
the sync allocator or the bucket-allocation pump) retains ownership of
those fields.

The bucket-allocation pump and the sync allocator use distinct SSA field
managers — `quota-bucket-allocator` for the controller-side pump and an
inline transaction for the sync allocator — so an operator who
accidentally runs both simultaneously at least sees clear ownership
conflicts in the apiserver logs rather than a silent mis-accounting.

### Reads are served from the cacher on both backends

On both backends the REST storage is wrapped by the standard apiserver
in-memory cacher (`k8s.io/apiserver/pkg/storage/cacher`). For Postgres
this is done by the custom `Decorator` in
`internal/storage/postgres/rest_options.go`, which performs one initial
`LIST` + one `WATCH` against Postgres at startup and serves all
subsequent reads from memory at ~10 µs latency. For etcd it is the
upstream default.

Postgres-backed cacher watches are fed by `internal/watch/postgres.go`,
which holds one dedicated pgx connection per apiserver LISTENing on the
`quota_changes` channel (shared by every watch through the hub, including
the native QuotaClaim watches); Postgres emits a `NOTIFY` from an AFTER INSERT
trigger on `quota_changelog`, and the watcher kicks every active watch
goroutine to drain new rows immediately. A periodic safety poll (5 seconds as of the 2026-11-22 addendum)
catches notifications missed during the LISTEN connection's reconnect
window.

The Postgres backend also registers a custom `FeatureSupportChecker`
(`pgstore.NewFeatureSupportChecker()` in `serve.go`) that advertises
`RequestWatchProgress` as supported, so the upstream cacher enables
`ConsistentListFromCache` and default reads stay in RAM.

### Dual mode

`--quota-storage-backend=dual` uses etcd as primary and Postgres as
shadow, via `dualstore.NewRESTOptionsGetter` in `cmd/quota/serve.go`.
`ExtraConfig.Allocator` is nil in this mode, so the apiserver uses the
standard `quotaclaim.NewStorage` and the controller-driven allocation
path drives writes to etcd; the dual storage layer fans out shadow writes
to Postgres for verification. Dual mode is a migration validation tool,
not a production configuration — operators use it to confirm that the
Postgres shadow contains the data they expect before flipping the
apiserver to `postgres`-only.

## Why two backends instead of one

- **Etcd is the operational default for clusters that already run etcd
  and do not want to operate Postgres.** It matches Milo's behavior. An
  operator running `task dev:setup` today gets the etcd path and does not
  need to deploy Postgres to use the quota service.
- **Postgres unlocks the synchronous allocation path.** Users who want
  claims to be `Granted` by the time the API call returns pay for it
  with a Postgres dependency in exchange for roughly half the p50
  latency on a claim CREATE. Postgres also makes multi-bucket claims
  genuinely atomic: the allocator holds every affected bucket's row
  lock inside one transaction, so either all decrements commit or none
  do. The controller-driven etcd path handles multi-bucket claims
  approximately, and a mid-pump controller crash can leave per-bucket
  allocations out of sync with claim status until the next reconcile.
- **Picking one backend would be the wrong call.** Etcd-only would block
  synchronous claim status forever; Postgres-only would force every
  operator to deploy Postgres for a feature they may not need.

## Tradeoffs of each backend

### Etcd backend

- Controller-driven: CREATE returns `201 Created` with an empty status,
  terminal `Granted` condition arrives ~50 ms later.
- Cross-bucket "transactions" are racy by construction: the bucket pump
  handles multi-bucket claims one bucket at a time. A crash mid-pump
  leaves partial allocation state until the next reconcile.
- Eventual-consistency window observable by clients that create a claim
  and immediately create the resource it authorizes; they must poll or
  watch.
- Operationally simple — no additional infrastructure beyond etcd, which
  the rest of the apiserver already requires.
- Reference throughput in the 20-VU workload: ~40.6 claims/s.

### Postgres backend

- Synchronous: CREATE returns `201 Created` with a fully-populated
  `Status.Conditions[Granted]`. Clients never observe `Granted=Unknown`
  on the happy path.
- Multi-bucket claims commit atomically in one transaction.
- p50 claim CREATE latency ~112 ms (20-VU workload) vs ~204 ms on etcd —
  clients that previously dominated their wall clock on poll intervals
  see the biggest improvement.
- Sustained throughput under the round-6 FOR UPDATE design is still in
  measurement. The round-1 FOR UPDATE implementation hit 37.6 claims/s
  on the same 20-VU workload against an etcd reference of 40.6 claims/s.
  The intervening CAS rounds regressed every step (32.2 -> 30.3 -> 24.3
  -> 13.85); reverting to FOR UPDATE is expected to restore at least the
  round-1 figure, but this is not yet confirmed against the current code.
- Hot buckets (many concurrent claimants against the same bucket)
  serialize on row-level locks rather than retry-and-fail. Clients see
  longer tail latency on contended buckets instead of 409 Conflict
  responses; the allocator no longer surfaces contention as a retryable
  client-visible error.
- Requires Postgres as operational infrastructure. Running the Postgres
  backend in production means running Postgres — not just starting one.

## Consequences

### Positive (Postgres backend)

- Claim CREATE returns final status synchronously; no controller
  round-trip, no watch echo.
- Multi-bucket claims are truly atomic.
- Cache reads at ~10 µs via the standard apiserver cacher.
- LISTEN/NOTIFY push-based watch with a periodic safety poll (5 seconds since the 2026-11-22 addendum).

### Positive (etcd backend)

- Matches Milo bit-for-bit; no operational change for existing consumers.
- No new infrastructure dependency.
- Mature operational story — the apiserver already depends on etcd.

### Positive (both)

- Deployment choice is a single flag flip, keyed to operational
  constraints rather than baked into the binary.
- Both paths exercise the same REST strategies, admission plugins, and
  status subresource handling. The allocator replaces the storage write;
  it does not replace validation or admission.

### Negative

- **Two code paths.** The bucket-allocation pump is gated on
  `--quota-allocation-mode=controller`. The sync allocator is gated on
  `--quota-storage-backend=postgres`. Tests must cover both paths; a bug
  in one does not affect the other, but neither does a fix.
- **Round-6 throughput is unmeasured.** The FOR UPDATE revert landed
  after round-5 measurement exposed the CAS regression, but the matching
  20-VU benchmark has not yet been re-run. The design is in measurement;
  the throughput regression tracked in the open-issues addendum is
  expected to be addressed, not confirmed.
- **Cache consistency failure observed under load was resolved by the
  xmin-horizon cursor.** A 60-second load run once produced a
  cacher/store divergence on `quotabucket` rv=61590 caused by
  sequence-vs-commit-order in concurrent allocator transactions. The
  changelog watcher in `internal/watch/postgres.go` was reworked in
  round 3 to advance its cursor using the xmin horizon rather than
  max-RV-seen, and `Store.GetList` now uses `currentResourceVersion()`
  so the cacher's safety-net consistency check stops false-firing
  against the authoritative state in `quota_objects`.
- **Hot-bucket contention surfaces as latency, not 409s.** With FOR
  UPDATE, concurrent claimants against the same bucket queue on the row
  lock and proceed in commit order. Tail latency on contended buckets
  grows; the allocator no longer returns `Conflict` as a retryable
  client-visible contention signal.
- **Dual mode is not a general-purpose production option.** It is a
  migration validation tool and does not get the synchronous allocator.

## Alternatives considered

- **Postgres-only (delete etcd entirely)**. Rejected. Operators who do
  not need synchronous claim status would be forced to deploy Postgres
  just to stay on the supported path. Too much operational change for a
  refactor. This is the framing the 2026-04-10 draft took, and it led
  directly to the over-eager cleanup that this rewrite records as a
  mistake.
- **Etcd-only (keep Milo's design unchanged)**. Rejected. Synchronous
  claim status is a real user-visible capability. Forcing every consumer
  onto poll-based clients forever is worse than maintaining two code
  paths.
- **NATS-driven event queue**. Rejected. Moving allocation onto a NATS
  JetStream consumer does not solve the sync-vs-async tradeoff — the
  caller still observes a pending window — and it adds NATS as a hard
  dependency in the critical path of every claim.
- **Single backend, abstracted storage**. Rejected. The two backends have
  fundamentally different concurrency models (single-key optimistic vs
  multi-row transactional); an abstraction layer would either expose the
  differences to the caller or hide them and mis-account under
  contention. The honest answer is to expose both at the configuration
  layer.

## References

- `CLAUDE.md` — extraction charter and repo layout
- `cmd/quota/serve.go` — backend selection, allocator construction,
  feature-support checker wiring, dual-store wiring
- `cmd/quota/controller_manager.go` — controller-manager subcommand
- `internal/apiserver/apiserver.go` — allocator wiring (`if c.ExtraConfig.Allocator != nil`)
- `internal/storage/postgres/allocator.go` — `PostgresAllocator`,
  `planAllocation` (in-memory aggregation), `commitAllocation` (sorted
  `SELECT ... FOR UPDATE` + UPDATE + changelog in one tx), `Release`,
  `planRelease` (unlocked idempotency probe), `commitRelease` (sorted
  `SELECT ... FOR UPDATE` + DELETE + changelog in one tx). Package
  comment records the round-6 CAS-to-FOR-UPDATE revert rationale.
- `internal/storage/postgres/allocator_test.go` —
  `TestPostgresAllocator_Allocate_SerializedSameBucket`,
  `TestPostgresAllocator_Allocate_NoDeadlockAcrossOverlappingBuckets`,
  `TestPostgresAllocator_Release_MultiBucketAtomic`,
  `TestPostgresAllocator_ConcurrentDifferentBuckets`,
  `TestPostgresAllocator_CommitXidPopulated`
- `internal/storage/postgres/store.go` — `Store.GetList` uses
  `currentResourceVersion()` so the cacher's consistency check aligns
  with the xmin-horizon-filtered max RV, not a raw sequence read
- `internal/registry/quota/quotaclaim/allocating_storage.go` —
  `AllocatingREST`, `NewAllocatingStorage`
- `internal/storage/postgres/rest_options.go` — cacher decorator,
  connection pool sizing
- `internal/watch/postgres.go` — LISTEN/NOTIFY watcher, safety poll
- `internal/quota/controllers/core/bucket.go` — grant-derived bucket
  status, field-manager split
- `internal/quota/controllers/core/claim.go` — claim presence reconciler
- `internal/quota/controllers/setup.go` — controller registration
- `config/base/deployment.yaml` — default `STORAGE_BACKEND=etcd`
- `config/overlays/dev/kustomization.yaml` — dev overlay composes the
  etcd component

## Open issues addendum

This section exists because the 2026-04-10 draft shipped without it and
the post-refactor verification found real problems the original ADR did
not acknowledge. Each item below is a known issue under active
observation; none of them block the dual-backend architecture but all of
them are operationally relevant.

### Throughput regression on Postgres (round-6 revert in measurement)

Rounds 2 through 5 used an optimistic compare-and-swap pattern on bucket
rows, and throughput regressed every round it was in place (32.2 -> 30.3
-> 24.3 -> 13.85 claims/s) against a round-1 FOR UPDATE baseline of
37.6 claims/s. Round-5 load profiling recorded a 31% transaction
rollback rate from CAS conflicts (777 of 2471 UPDATE statements), UPDATE
mean latency of 67 ms against an uncontended baseline of 0.6 ms, and an
82% CAS retry-exhaustion failure rate on the hot-tenant scenario
(40 VUs, 1 bucket). Round 6 reverts to pessimistic `SELECT ... FOR
UPDATE` per bucket with sorted-key lock ordering for deadlock
prevention. The round-6 20-VU benchmark has not yet been run against
the current code; the design is in measurement. The round-1 FOR UPDATE
implementation hit 37.6 claims/s on the same workload, so the revert is
expected to restore at least that throughput, but operators expecting
Postgres to outperform etcd under sustained CREATE+DELETE load should
benchmark their own workload.

### Cache consistency failure during load (addressed in rounds 3 and 6)

One observed cacher/store divergence in a 60-second load run on
`quotabucket` rv=61590. Root cause: two concurrent allocator
transactions interleaved `nextval('quota_resource_version_seq')` and
`COMMIT`, publishing RVs out of commit order. The cacher advanced its
watch cursor by max-RV-seen and therefore skipped the later-committed
row, diverging from `quota_objects`. Round 3 replaced the watch cursor
with an xmin-horizon filter in `internal/watch/postgres.go`, so rows
from still-in-flight transactions are held back until their originating
transactions commit. Round 6 additionally changed `Store.GetList` to
use `currentResourceVersion()` (same xmin-horizon-filtered max) so the
cacher's safety-net consistency check stops false-firing against rows
that are visible to the backend but not yet to the watch stream. The
writer-side advisory lock that earlier rounds used around
nextval-to-commit is no longer needed and has been removed; the
allocator holds no global locks.

### e2e coverage gap

*(Out of date: `test/e2e/` now holds ten suites, none of which reference `resourcemanager.miloapis.com`, and CI has a kind e2e job. See the 2026-11-22 addendum.)* Seven of the eight Chainsaw tests in `test/e2e/` depended on Milo
`resourcemanager.miloapis.com` CRDs (`Organization`, `Project`,
`OrganizationMembership`) that are not installed in the standalone test
cluster. Only `quota-definition-validation` runs against the standalone
target. The dual-backend architecture is therefore verified primarily by
unit and integration tests plus manual load runs; broader e2e coverage
is blocked on either installing the Milo CRDs in the standalone cluster
or rewriting the tests to use opaque consumer references.

### Cacher `couldn't create cachingObject` log noise

The apiserver cacher logs `couldn't create cachingObject` once per claim
create because the registry returns internal objects without populated
`TypeMeta`. The upstream cacher falls back to a non-cached path and the
object is still returned to the client correctly, so this is cosmetic —
but it is loud in production logs and should not be mistaken for an
allocator failure when diagnosing contention.

### Round 6: revert CAS to `SELECT ... FOR UPDATE`

Round 6 replaced the optimistic compare-and-swap bucket update pattern
with pessimistic `SELECT ... FOR UPDATE` row locking and deleted the
supporting retry machinery (`casBackoffSleep`, `maxAllocateAttempts`,
the outer retry loop, the jittered backoff). The motivation was the
round-5 measurement evidence: 31% transaction rollback rate from CAS
conflicts (777 of 2471 UPDATE statements), 67 ms UPDATE mean latency
against a 0.6 ms uncontended baseline, 82% CAS retry-exhaustion failure
rate on the hot-tenant scenario, and a monotonically regressing
throughput line across the four rounds CAS was in place
(32.2 -> 30.3 -> 24.3 -> 13.85 claims/s) versus 37.6 claims/s for the
round-1 FOR UPDATE baseline. Under contention, CAS retries waste more
aggregate work than FOR UPDATE queue wait time does, and at 50 writers
against 100 buckets the per-round collision probability is ~39% by the
birthday-paradox math — which matches the observed failure rate.

Round 6 also made `planAllocation` a pure in-memory aggregation step
(no DB reads) and moved all bucket reads under the FOR UPDATE lock
inside `commitAllocation`; `planRelease` does an unlocked
claim-existence probe for idempotency and the bucket reads happen
inside `commitRelease` under locks in the same sorted key order. A new
concurrency test `TestPostgresAllocator_Allocate_NoDeadlockAcrossOverlappingBuckets`
guards the sorted lock-acquisition invariant. The
xmin-horizon watcher (round 3) and single-transaction Release (round 2)
are orthogonal to the locking primitive and stay in place. The round-6
20-VU benchmark has not yet re-run; the design is in measurement.

### `QuotaClaimOwnershipController` busy-loop on unknown kinds (fixed in round 2)

Fixed in round 2 in `internal/quota/controllers/lifecycle/ownership.go`:
`meta.IsNoMatchError` is now treated the same as `apierrors.IsNotFound`,
so unknown consumer kinds go through the ownership grace-period path
instead of bubbling back out as an immediate requeue. Kept here for
historical context — operators reading older logs may still see the
former noise pattern, but the underlying busy-loop is gone.

### Default deployment ships etcd, not Postgres

`config/base/deployment.yaml` sets `STORAGE_BACKEND=etcd`. The dev
overlay composes the etcd component, not the Postgres component. This
ADR's "dual-backend, first-class" decision applies to the set of
supported configurations, not to the default. An operator who wants the
synchronous allocator must explicitly flip the flag, deploy Postgres,
and run the controller-manager with
`--quota-allocation-mode=allocator`. This is the intended operational
posture — etcd is the path of least resistance for the common case — but
it is worth stating because the 2026-04-10 draft claimed the dev overlay
already defaulted to Postgres, which was and is false.

## Addendum (2026-11-22): commit-ordered resource versions reinstate a global lock

This addendum records a decision that reverses part of round 3 and changes the cost model described above.

### Background

Resource versions in the Postgres backend come from `quota_resource_version_seq`. `nextval()` returns a value at statement time, not at commit time. Two writers can therefore commit in the opposite order to their resource versions. A watcher that advances its cursor by resource version can skip a row that commits late.

The early fix was a global advisory lock that serialized every writer from `nextval()` to commit. Migration 003 removed it. Its header gives the reason: the lock "throttled every tenant onto one key and capped Postgres throughput below the etcd backend's". Migration 003 replaced it with an xmin-style cursor. Each changelog row recorded `commit_xid`, the inserting transaction's 64-bit id, and the watcher emitted rows in `(commit_xid, id)` order below the snapshot's xmin horizon.

### The bug in the xid-ordered cursor

A transaction id is assigned at a transaction's first write. It is not assigned at commit. Take two transactions:

1. Transaction A writes a row for some other key and gets xid 100. It keeps working.
2. Transaction B starts, gets xid 101, writes key K and commits.
3. Transaction A then writes key K, on top of B's committed version, and commits. A's version of K is newer and has the higher resource version.

Commit order is B, then A. The changelog row each transaction wrote for K carries its own xid as `commit_xid`: 100 for A and 101 for B. The watcher orders by `(commit_xid, id)`, so it emits A's version of K first and B's second. The last event a client sees for K is B's older state, and the resource version a watcher sees goes backwards. A client that reconciles from watch events ends on stale state.

The xmin horizon did not help. It decides when a row is safe to emit, which is after every older transaction has finished. It does not change the order in which safe rows are emitted. The cursor matched commit order only when transactions did not overlap.

### Decision

Migration 006 (now part of `migrations/001_initial_schema.sql`; migrations 001 to 009 were folded into one file on 2026-11-24, before any deployment) assigns resource versions in commit order:

- Writers draw resource versions with `quota_next_rv()`. The first call in a transaction takes a transaction-scoped advisory lock. The lock is held until commit.
- PostgreSQL releases transaction-scoped locks only after the commit is visible to new snapshots. The next writer therefore cannot draw a resource version until every lower one is visible.
- Resource-version order is commit order. Every snapshot sees an exact prefix of resource versions: if it sees N, it sees every committed version below N.
- A writer must take its row locks first, draw resource versions in the statement that writes, and then commit without waiting on another row lock.
- A trigger on `quota_changelog` (per row in 006, once per statement since migration 008) rejects any row whose resource version is not above the high-water mark committed by other transactions. It raises SQLSTATE 40001, so a writer that ignores the contract gets a retryable error rather than corrupting the order.
- `quota_rv_state.committed_rv` is the high-water mark and the current resource version. `quota_changelog_compaction.compacted_rv` is the compaction floor. Watches and lists below the floor get 410 Gone.

The watcher now scans by `(resource_version, id)` up to `committed_rv` in a single snapshot. It supports exact resume, 410 Gone, pagination and bookmarks for delivered versions only. The `commit_xid` column from migration 003 is no longer the cursor.

### Cost

This is the lock migration 003 removed, in a narrower form. The lock is global, so the window from "draw the resource version" to "commit is visible" is serialized across all writers, including writers on unrelated buckets. Writers keep the window short. The allocator, for example, sends its final statement and COMMIT in one pipelined batch, with no client round trip inside the window. Each changelog insert also updates the single `quota_rv_state` row.

The implementer estimated a global ceiling of roughly 500 to 1000 writes per second, from an assumed lock hold of one to two milliseconds per writer. Later measurement confirmed the shape: without batching, an end-to-end load test levelled off near 600 claims per second (a create and a delete each) regardless of client count, and about 40% of the hold time was the commit flush. Group commit (several claims per transaction, `--postgres-allocation-max-batch`) spreads one hold and one flush over a batch and reached about 2,355 claims per second at 240 clients on Linux. The ceiling still falls if commit latency rises (slow storage, synchronous replication). [storage-backends.md](../storage-backends.md#throughput-trade-off) has the numbers.

The rest of the allocator design is unchanged. Bucket row locks still use `SELECT ... FOR UPDATE` in sorted key order, and a claim create is still one transaction. Contention on a single hot bucket is still the main limit, as the round-5 data shows.

### Why not keep the xid cursor

The Kubernetes storage contract expects watch events in resource-version order and lists that are consistent with a resource version. A transaction id cannot supply that order, because it is fixed at first write and commit order is not. The order has to be assigned at commit, and the lock is the mechanism that does it.

The reasoning recorded here is that correctness outweighs throughput. The bug could corrupt client state silently. The cost has since been measured; see [storage-backends.md](../storage-backends.md#throughput-trade-off).

### Follow-ups

- Benchmark on production-like storage and commit settings. Local VM and host results are in [storage-backends.md](../storage-backends.md#throughput-trade-off); the effect of `synchronous_commit` and replication has not been measured.
- Add a metric for time spent waiting on the commit-order lock, so the ceiling shows up before it becomes a limit.
- Batching is done (group commit, above). Sharding the lock by key range has not been evaluated. It would give up the single global order that watchers rely on, so it needs its own design.
- The migrations ConfigMap is now generated from `migrations/` by `hack/update-migrations-configmap.sh`, and CI verifies it.

### Superseded statements above

| Statement | Now |
|-----------|-----|
| The watcher uses an xmin-horizon cursor | The watcher scans by resource version up to `committed_rv`. See migration 006 |
| The allocator holds no global locks | The allocator holds a global commit-order lock for the last statement and commit |
| Writers on disjoint buckets never serialize | They do not wait on each other's bucket locks, but they do serialize on the commit-order window |
| `Store.GetList` uses an xmin-filtered `currentResourceVersion()` | The current resource version is `quota_rv_state.committed_rv` |
| Safety poll of 1 second | 5 seconds. LISTEN/NOTIFY still drives the steady state |
| `Release` does an unlocked claim-existence probe | `Release` takes the bucket locks, then locks the claim row, then deletes it conditioned on its resource version and UID |
| Seven of eight e2e tests depend on Milo CRDs | Ten suites exist, none reference `resourcemanager.miloapis.com`. CI runs them, but has not yet completed a run |
