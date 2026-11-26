# Storage Backends

The quota apiserver selects its storage backend with `--quota-storage-backend`. The controller-manager takes the same flag so it can pick the matching allocation mode.

> Last verified: 2026-11-26 against `cmd/quota/serve.go`, `cmd/quota/controller_manager.go`, `migrations/001_initial_schema.sql`, `internal/watch/postgres.go` and `internal/storage/`.

| Backend | Flag value | Claim allocation | Intended use |
|---------|-----------|------------------|--------------|
| etcd | `etcd` (default) | Controller-driven, asynchronous | Default. Same behavior as Milo's built-in quota |
| Postgres | `postgres` | Synchronous and transactional, in the create request | Deployments that want the decision in the create response |
| Dual | `dual` | Controller-driven, asynchronous | Validating a Postgres copy before cutover |

Only `postgres` allocates synchronously. `etcd` and `dual` use the controller-manager to decide claims, and enforcement admission waits for that decision. See [Allocation modes](#allocation-modes).

## Apiserver flags

Flags defined in `cmd/quota/serve.go`. The standard `k8s.io/apiserver` flags (`--secure-port`, `--etcd-servers` and so on) are also accepted.

| Flag | Default | Description |
|------|---------|-------------|
| `--quota-storage-backend` | `etcd` | `etcd`, `postgres` or `dual`. Any other value fails at startup |
| `--postgres-dsn` | empty | PostgreSQL connection string. Required for `postgres` and `dual` |
| `--disable-storage-cacher` | `false` | Disable the in-memory cache for all quota resources. Valid only with `postgres` |
| `--postgres-changelog-retention` | `0` (use the built-in 24h) | How long changelog rows are kept. Watches and paginated lists older than this get 410 Gone. Values under 2 minutes are raised to 2 minutes. Size this against the write rate. Most writes keep a copy of the object. At 1,700 claims/s (a create and a delete each) the changelog grew by about 9 GB in 20 minutes when every row held a copy; claim ADDED rows no longer do, which saves about a third of the bytes per claim. See [Changelog size](#changelog-size). `postgres` and `dual` (the shadow's changelog) |
| `--quota-bucket-namespace` | `quota-system` | Namespace for QuotaBuckets whose `consumerRef` has no namespace. Must match the controller-manager |
| `--postgres-allocation-max-batch` | `64` | Most concurrent claim creates and deletes committed together in one transaction. See [Group commit](#group-commit). `1` disables batching. Must be at least 1. `postgres` only |
| `--front-proxy-verify-cache-ttl` | `10m` | How long a successful verification of `kube-apiserver`'s front-proxy client certificate chain is reused. Certificate validity, CA changes and `--requestheader-allowed-names` are still checked on every request. `0` disables the cache. Must not be negative |
| `--quota-enforcement-claim-timeout` | `25s` | How long admission waits for a claim decision. Must be positive. Keep it below the webhook `timeoutSeconds` (30) |
| `--quota-cluster-scoped-claim-namespace` | `quota-system` | Namespace for claims created for cluster-scoped resources when the policy does not pin one |
| `--quota-enforcement-consumer-ref-from-user-extra` | none | `<nameKey>,<kindKey>[,<apiGroupKey>]`. Reads the whole consumerRef from user-info extras; tried first. Repeatable |
| `--quota-enforcement-consumer-from-user-extra` | none | `<extra-key>=<Kind>[.<apiGroup>]`. Repeatable. See [Consumer resolution](integration-guide.md#consumer-resolution) |
| `--quota-enforcement-consumer-from-namespace-annotation` | none | `<annotation-key>=<Kind>[.<apiGroup>]`. Repeatable. Needs a core API kubeconfig (`--kubeconfig`) |

The database password does not come from an environment variable. Pass the whole DSN with `--postgres-dsn`. The shipped Deployment builds the flag from a `POSTGRES_DSN` environment variable sourced from the `postgres-credentials` Secret (`$(POSTGRES_DSN)` in `config/base/deployment.yaml`), but that substitution belongs to the manifest. The binary reads only the flag.

## Controller-manager flags

Flags defined in `cmd/quota/controller_manager.go`.

| Flag | Default | Description |
|------|---------|-------------|
| `--quota-storage-backend` | empty | The apiserver's backend. Set it to the same value. Derives the allocation mode and rejects mismatches |
| `--quota-allocation-mode` | derived | `controller` or `allocator`. Normally omitted |
| `--quota-bucket-namespace` | `quota-system` | Must match the apiserver. Must not be empty |
| `--quota-stale-pending-claim-age` | `5m` | Age after which an auto-created claim still pending is deleted as abandoned. Must be positive. Keep it well above the admission wait |
| `--quota-allocation-workers` | `8` | How many buckets and claims are reconciled at once in controller mode. Buckets are independent, so more workers decide more claims per second |
| `--kube-api-qps` | `500` | Client-side request rate limit. Each claim decision takes several writes, so this caps decisions per second. Negative disables the limit |
| `--kube-api-burst` | `1000` | Client-side burst allowance |
| `--kubeconfig` | empty | Needed only out of cluster |
| `--master` | empty | API server address. Overrides the kubeconfig |
| `--metrics-addr` | `:8443` | Metrics bind address. `0` disables metrics |
| `--metrics-secure` | `true` | Serve metrics over HTTPS and require an authenticated, authorized scraper (`get` on `/metrics`, granted by `quota-metrics-reader`). `false` is allowed only with a loopback `--metrics-addr` |
| `--metrics-cert-dir` | empty | Directory with `tls.crt` and `tls.key` for metrics. Without it a self-signed certificate is generated |
| `--health-probe-addr` | `:8081` | Health probe bind address |

## Allocation modes

The controller-manager's `--quota-allocation-mode` decides who writes claim and bucket allocation status.

| Backend | Required mode | What runs |
|---------|---------------|-----------|
| `etcd`, `dual` | `controller` | The bucket allocation pump and claim aggregator write allocation state |
| `postgres` | `allocator` | Neither controller is registered. The Postgres allocator writes allocation state in the create request |

When `--quota-storage-backend` is set on the controller-manager, the mode is derived from it. If you also pass `--quota-allocation-mode` and it disagrees, the controller-manager exits at startup. The mismatch is fatal because controller mode on Postgres races the allocator on the same status fields, and allocator mode on etcd or dual means nothing writes claim status.

The apiserver cannot see the controller-manager's flags. Keep the two `--quota-storage-backend` values equal. The shipped manifests do this through a `STORAGE_BACKEND` environment variable in each Deployment, and the overlays patch both. If you leave the flag unset on the controller-manager, it assumes `controller` mode and logs that it did not cross-check.

The grant-driven part of the bucket controller (limits, grant counts, bucket creation) runs in both modes.

## etcd (default)

Uses the standard `k8s.io/apiserver` etcd3 storage.

| Flag | Default |
|------|---------|
| `--etcd-servers` | none. Set it to your etcd endpoints |
| `--etcd-prefix` | `/registry/quota.miloapis.com` |
| `--etcd-cafile`, `--etcd-certfile`, `--etcd-keyfile` | none |

Claims are created with status `Granted=False`, reason `PendingEvaluation`. The controller-manager then grants or denies them. With the default flags, a local benchmark at 30 concurrent clients decided about 230 to 290 claims per second, at a median of about 90 ms from create to `Granted`.

The decision rate depends on `--kube-api-qps` and `--quota-allocation-workers`. client-go's own defaults (5 requests per second, one worker) decide about 3 claims per second and leave a claim pending for 4 seconds or more under load.

## Postgres

Stores objects as JSON rows in `quota_objects` and records every change in `quota_changelog`. The apiserver does not connect to etcd in this mode.

Requirements:

- PostgreSQL 13 or later. The commit-order trigger uses `pg_current_xact_id()`.
- The schema in `migrations/` applied. See [migrations/README.md](../migrations/README.md).
- `--postgres-dsn` set.
- The controller-manager running with `--quota-storage-backend=postgres`.

### Allocation

A claim create runs in one transaction:

1. Lock every affected bucket row with `SELECT ... FOR UPDATE`, in sorted key order, so overlapping claims cannot deadlock.
2. If any bucket is missing, roll back and return Forbidden. A missing bucket means no active grant covers the request.
3. Check capacity against the locked values and decide Granted or Denied for each request.
4. Update the granted buckets, insert the claim with its final status, write the changelog rows and commit.

Watchers see one ADDED event with the final status. A crash before commit leaves nothing behind.

Release runs in one transaction too. It deletes the claim row conditioned on its resource version and decrements buckets only if that delete removed the row. A repeated or concurrent delete does not decrement twice.

#### Group commit

Concurrent claim creates and deletes in one apiserver are committed together, so one transaction pays for the commit-order lock and the commit flush once for many claims. There is no timer. While a batch is in flight, new requests queue. When it finishes, the next batch takes everything queued, up to `--postgres-allocation-max-batch`. At most two batches are in flight per apiserver, so one can take its row locks while the other writes. With no concurrent requests a batch holds one claim and runs exactly the single-claim steps above.

A batch with several claims:

1. Locks the union of every bucket and claim row it touches, in the same sorted key order. Batches, single claims, other writers and other replicas therefore cannot deadlock.
2. Decides each request in arrival order against a running copy of the locked bucket state. Two claims on one bucket cannot over-allocate, and a delete ahead of a create frees capacity for it. The result is the same as running the requests one after another.
3. Writes each touched bucket once with its final state, removes the deleted claims, inserts the new claims with their final status, writes the changelog rows and commits.

Each request still gets its own outcome. A missing bucket is Forbidden for that claim only, a duplicate name is AlreadyExists for that claim only, and a stale delete is a Conflict for that delete only. Watchers see one ADDED event per created claim and one MODIFIED event per bucket per batch. If the batch transaction fails unexpectedly, for example because another replica inserted the same claim name or with a serialization failure, every request in it is retried on its own with the single-claim steps.

A request cancelled while queued is dropped. Once its batch has started, the outcome stands, as with a single claim whose commit is already under way.

The `quota_allocation_batch_size` histogram shows how many requests each transaction carried.

Batch transactions disable sequential and bitmap scans for their own statements (`SET LOCAL`), so every row lookup uses the primary key whatever the table statistics say. Without this, a plan cached while `quota_objects` was empty, as on a fresh database, scans the whole table for every lookup once the table has grown.

### Resource versions and watch

Resource versions come from `quota_next_rv()`. It takes a transaction-scoped advisory lock that is held until commit, then draws from the sequence. PostgreSQL releases the lock only after the commit is visible, so resource-version order equals commit order. A watcher scanning the changelog by resource version never meets a lower version later.

This gives the behavior Kubernetes clients expect:

- **Exact resume.** A watch from resource version N emits exactly the rows after N.
- **410 Gone after compaction.** The cleaner records the highest deleted resource version before it deletes rows. A watch or list that needs history at or below that point gets 410 Gone, not a silent gap.
- **Pagination.** `limit` and `continue` work, with keyset continuation in the same byte order etcd uses.
- **Bookmarks** carry only resource versions the watcher has actually delivered.
- **Shared watch hub.** One LISTEN connection and one cleaner serve every resource type. Safety polling runs every 5 seconds in case a notification is lost, bookmarks every 30 seconds, and compaction every hour.

Writers must follow the contract in [migrations/README.md](../migrations/README.md#writer-contract). A trigger rejects a row whose resource version was drawn before the lock and returns SQLSTATE 40001, so a non-compliant writer fails with a retryable error rather than corrupting watch order.

### Changelog size

Originally each changelog row held a full copy of the object. One claim created and then deleted wrote about 5.4 KB: the ADDED row with the claim (1.4 KB, uncompressed), the DELETED row with the claim again (0.8 KB, compressed by PostgreSQL), one bucket MODIFIED row per bucket touched (0.5 KB each) and about 0.3 KB of index entries per row. PostgreSQL compresses only rows over about 2 KB. That threshold is a compile-time constant, so column compression and `toast_tuple_target` settings do not help.

Two changes cut that:

- **No copy on claim ADDED rows.** The allocator writes the ADDED row of a claim with no data, because it would repeat the bytes written to `quota_objects` in the same statement. The watch rebuilds the event in the same query. While the claim still has that resource version, the data comes from `quota_objects`. After the claim is deleted, it comes from the DELETED row, which carries the claim's last state. Every update of an object first copies its data into the object's current changelog row if that row has none, so the rebuild never has to cross a MODIFIED row. Watchers receive exactly the events they did before, including on replay from an old resource version. If a row ever cannot be rebuilt, the watch ends with 410 Gone and the client relists, so an event is never skipped.
- **Hash key index.** `idx_quota_changelog_key` indexes `hashtext(key)` instead of `key`. The old index compared keys with the database collation inside the commit-order window. The new index entries are an int4 instead of the key.

Measured with `BenchmarkChangelogFootprint` in `internal/storage/postgres/changelog_bench_test.go` (claims shaped like the enforcement plugin's, one bucket per writer, 3 runs each):

| | Before | After |
|---|---|---|
| Changelog bytes per claim | 5,405-5,410 | 3,714-3,724 |
| Heap and TOAST | 4,113 | 2,577 |
| Indexes | 1,291-1,296 | 1,136-1,147 |
| ADDED claim row | 1,532 | 140 |

Retention sets the steady-state size: bytes per claim times claims per second times `--postgres-changelog-retention`.

Rebuilding costs readers a little. A 500-row claim poll took 1.8-2.0 ms before the change and 2.6-2.7 ms with rebuilt ADDED rows.

### Throughput trade-off

The commit-order lock is global. Every writer, including writers on different buckets, waits for the previous writer's commit to become visible. The lock is held from the moment a writer draws its resource version until its commit. Writers keep that window short, but it is serialized across the whole database.

Without batching (`--postgres-allocation-max-batch=1`) the lock is the limit. An end-to-end load test through `kube-apiserver` sustained about 600 claims per second, each claim a create and a delete. Throughput stayed flat from 5 concurrent clients up, and adding clients only added queueing. Adding apiserver replicas does not raise that ceiling, because every replica waits on the same lock.

While a writer holds the lock it runs its whole statement and flushes its commit to disk. In sampling, about 40% of the hold time was the commit flush and the rest was CPU in the statement, triggers and index maintenance. The flush share means the ceiling is roughly the inverse of commit latency, so a slow disk or synchronous replication lowers it.

[Group commit](#group-commit) raises the ceiling by spreading one lock hold and one flush over many claims, and by writing a shared bucket once per batch instead of once per claim. It does not remove the lock: batches from different replicas, and other writers, still pass through it one at a time. The per-row statement and trigger cost inside the lock remains.

Measured with the allocator benchmarks (`internal/storage/postgres/bench_test.go`, 32 concurrent writers, Postgres 16 with 2 CPUs on a local VM, medians of 10 runs, operations per second):

| Workload | Without batching | With batching (default) | Mean batch |
|----------|-----------------:|------------------------:|-----------:|
| Create, one bucket per writer | 1,600 | 4,350 | 9 |
| Create, all writers on one bucket | 780 | 5,300 | 11 |
| Create then delete, one bucket per writer | 700 pairs | 2,430 pairs | 9 |
| Create then delete, one shared bucket | 450 pairs | 3,230 pairs | 11 |
| One writer, create | 1.23 ms | 1.24 ms | 1 |

The same end-to-end load test (k6 `claim-throughput.js` through `kube-apiserver`, 20 tenants, one apiserver limited to 2 CPUs, Postgres 16 with 2 CPUs):

| Concurrent clients | Without batching | With batching (default) |
|-------------------:|-----------------:|------------------------:|
| 5 | 594 claims/s, create p50 3.6 ms | 606 claims/s, create p50 3.4 ms |
| 30 | 617 claims/s, create p50 19.3 ms | 1,285 claims/s, create p50 10.7 ms |
| 60 | 612 claims/s, create p50 44.8 ms | 1,527 claims/s, create p50 18.3 ms |

Those runs had every process on a macOS host. Repeated on Linux, with each process pinned to its own CPUs (apiserver 2, `kube-apiserver` 3, Postgres 2), batching on:

| Concurrent clients | Claims/s | Create p50 | Apiserver CPU | `kube-apiserver` CPU | Postgres CPU |
|-------------------:|---------:|-----------:|--------------:|---------------------:|-------------:|
| 10 | 1,116 | 4.0 ms | 122% | 91% | 109% |
| 30 | 1,481 | 9.3 ms | 132% | 93% | 102% |
| 60 | 1,675 | 16.5 ms | 140% | 101% | 106% |
| 120 | 1,898 | 29.1 ms | 151% | 110% | 106% |
| 240 | 2,052 | 54.8 ms | 161% | 118% | 116% |

Throughput levels off near 2,000 claims/s with no process out of CPU, so more apiserver CPU or replicas do not raise it. The limit is inside Postgres and is serial: at any moment one session runs a batch's write statement while the other in-flight batch waits on the bucket rows it holds (`Lock:transactionid`). Spreading the same load over 600 buckets instead of 60 was slower (1,640 claims/s at 240 clients), because each batch then locks more rows. The ceiling is the time one session spends in the write statement per batch, so lowering per-claim cost in that statement is what raises it. Migration 008 removed the bucket-column trigger and made the changelog triggers run once per statement, which cut the statement from about 7.2 to 5.1 ms per 64-operation batch; 009 stopped copying created claims into the changelog. These measurements predate both.

The same sweep after the trigger and changelog changes (then migrations 008 and 009, now part of the single schema file) and the front-proxy verification cache:

| Concurrent clients | Before | After | Apiserver CPU per request |
|-------------------:|-------:|------:|--------------------------:|
| 10 | 1,116 claims/s | 1,251 claims/s | 0.549 → 0.490 ms |
| 30 | 1,481 | 1,697 | 0.457 → 0.404 ms |
| 60 | 1,675 | 1,833 | 0.420 → 0.373 ms |
| 120 | 1,898 | 2,130 | 0.399 → 0.357 ms |
| 240 | 2,052 | 2,355 | 0.393 → 0.346 ms |

Front-proxy certificate verification no longer shows in the apiserver profile. The changelog averaged about 4.1 KB per claim, down from about 5.3 KB: ADDED claim rows dropped to about 125 bytes. Most of what remains is bucket MODIFIED rows, about 1.25 per claim at 1.7 KB each, and almost half of each bucket object is `managedFields`.

The single schema file also makes bucket writes heap-only (HOT) updates. Earlier, the claim indexes were expressions on `data_json`, which every bucket write changes, so each bucket update inserted an entry into every index on `quota_objects`. The indexed claim fields now have their own generated columns, which are NULL for buckets and never change for claims, and the table has `fillfactor = 70`. In `BenchmarkAllocateRelease` (Mac host, 5 s, three alternating rounds), 99.6% of updates were HOT, against 0.1% before. The same-bucket case went from 133 to 115 µs per op. The distinct-buckets case went from 188-522 µs per op, varying with index bloat from earlier runs, to a steady 128-130 µs. End to end on the Linux setup above it made no measurable difference: alternating the two index layouts on the same database, two rounds each, gave 2,194 and 2,170 claims/s at 240 clients with HOT updates and 2,303 and 1,769 without, inside the run-to-run noise of a busy host. The ceiling is still the serial write statement and the row-lock wait behind it.

Each apiserver request cost about 0.4 ms of apiserver CPU and 0.3 ms of `kube-apiserver` CPU. In the apiserver, about 9% went to verifying `kube-apiserver`'s front-proxy client certificate on every request (since cached; see [Authentication](architecture/README.md#authentication)), 7% to the claim read that starts each delete, 6% to feeding watches, and 13% to garbage collection. `kube-apiserver` spent its time proxying: TLS, HTTP/2 framing and fair queuing.

A single writer is not slower: with nothing queued the request runs the single-claim steps on its own goroutine and context. At 128 writers the mean batch grew to about 42 and a cap of 32 to 128 performed alike; 16 was clearly lower. The default of 64 leaves room above that while bounding how long one batch holds the lock.

Migration 003 had removed an earlier writer-side lock to improve throughput, replacing it with a transaction-id cursor. That cursor turned out not to be commit order. See the [ADR addendum](adr/0001-postgres-first-architecture.md#addendum-2026-11-22-commit-ordered-resource-versions-reinstate-a-global-lock) for the bug and the decision.

The numbers above come from local VMs and hosts. The ceiling tracks commit latency, so measure on your own storage and commit settings (for example synchronous replication) before sizing a deployment.

### Claim storage

On this backend, QuotaClaim reads and watches bypass the generic store and the cache. They go to Postgres through a dedicated pgx pool. Claim watches subscribe to the shared watch hub for wake-ups, so an apiserver holds one LISTEN connection however many watches are open. Each watch checks a pool connection out only while it reads the changelog to catch up.

## Dual

etcd is the source of truth. Postgres is a write-only shadow that lets you check the Postgres copy of your data before cutting over.

Requires both etcd and `--postgres-dsn`. The controller-manager runs with `--quota-storage-backend=dual`, which selects `controller` allocation.

How it works:

- Every write runs against etcd exactly once. After it commits, the object etcd produced is mirrored to Postgres.
- Shadow writes are upserts by storage key. A missing row is inserted, which backfills objects that predate dual mode. Deletes are by key, and a row already gone counts as success.
- A per-key sequencer drops a shadow write if a newer etcd version of that key was already mirrored by this process.
- A background reconciler runs for each resource every 5 minutes, starting 30 seconds after startup. It compares key sets and a content hash that ignores resource versions and managed fields. It backfills missing keys at 20 writes per second and exports divergence metrics.
- Repair is opt-in. With it enabled the reconciler also overwrites drifted rows and deletes orphaned ones. The `quota` binary has no flag for it. It is available only through `dual.Options.Reconciler.RepairDivergence` in code.
- Shadow failures never fail a client request. They are logged and counted. Each shadow write is bounded by a 10 second timeout.

Metrics (namespace `quota`, subsystem `dual`):

| Metric | Meaning |
|--------|---------|
| `quota_dual_shadow_write_failures_total` | Shadow writes that failed after etcd succeeded |
| `quota_dual_shadow_backfills_total` | Objects inserted because they were missing |
| `quota_dual_shadow_writes_skipped_total` | Shadow writes skipped because a newer state was already mirrored |
| `quota_dual_divergent_objects` | Unresolved differences found by the last pass |
| `quota_dual_last_reconcile_timestamp_seconds` | Time of the last completed pass |
| `quota_dual_reconcile_errors_total` | Reconcile passes that failed |

What dual mode does not tell you:

- **Resource versions change at cutover.** Postgres assigns its own. Every client, informer and cache must relist after you switch to `postgres`. A watch resumed with an etcd resource version fails or returns wrong events.
- **It does not exercise the Postgres allocator.** The allocator is not wired in dual mode, and claim status is computed by the etcd controllers and mirrored as is. A clean shadow shows that the data migrated. It says nothing about the allocator's correctness or throughput. Test those on a `postgres` deployment.
- **It does not exercise the Postgres watch or cache paths.** Only the reconciler reads the shadow.
- **Ordering across replicas is per-process.** Two replicas writing the same key can briefly leave an older state in the shadow until the next write or reconcile pass.
- **It is not atomic.** A crash between the etcd commit and the shadow write loses that write until the reconciler finds it.

## Watch contract

All backends honor the Kubernetes watch contract: initial list, ordered event stream, bookmarks, and 410 Gone when the requested history is gone.
