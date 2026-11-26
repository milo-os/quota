// Package dual implements the `--storage-backend=dual` mode: etcd is the
// primary and the source of truth, and Postgres is a write-only shadow kept
// in step so operators can check the Postgres copy before cutting over
// (see docs/adr/0001-postgres-first-architecture.md, "Dual mode").
//
// # How writes reach the shadow
//
// Every mutation runs against the primary exactly once. That includes the
// registry's tryUpdate closure, strategy hooks, admission side effects and
// the closure variables genericregistry.Store.Update reads afterwards
// (creating, deleteObj). After the primary commits, the object the primary
// produced is mirrored to the shadow:
//
//   - Create and GuaranteedUpdate become an unconditional upsert keyed by
//     storage key. The shadow's update closure ignores what the shadow holds
//     and returns the primary's object, so the registry's RV checks and
//     preconditions never run against Postgres RVs. The shadow assigns its
//     own RV. A missing row is inserted, which backfills objects created
//     before dual mode was enabled.
//   - Delete becomes an unconditional delete by key. A row that is already
//     missing counts as success.
//
// A per-key sequencer drops a shadow write when a newer primary RV for the
// same key has already been mirrored by this process, so concurrent
// requests cannot leave an older state in Postgres.
//
// A background reconciler runs per resource. It lists both backends and
// compares key sets plus a content hash that ignores resourceVersion,
// managedFields and TypeMeta. It backfills keys missing from the shadow
// (rate-limited) and exports divergence metrics (see metrics.go). With
// Options.RepairDivergence it also overwrites drifted rows and removes
// orphaned ones.
//
// # What dual mode guarantees
//
//   - Clients see exactly etcd behavior. Get, List, Watch, RVs, conflicts,
//     admission and the controller-driven allocation path are unchanged.
//     Shadow failures never fail a request; they are logged and counted.
//   - Without errors, every primary write is mirrored before the request
//     returns, bounded by Options.ShadowWriteTimeout. Shadow writes are
//     detached from request cancellation.
//   - Divergence the write path misses is detected within one reconcile
//     interval. Missing keys are backfilled.
//
// # What dual mode does NOT guarantee
//
//   - Resource versions are not preserved. Postgres assigns its own RVs, so
//     an RV observed in dual mode means nothing after cutover. Every client,
//     informer and cacher must relist after switching to `postgres`. A
//     watch resumed with an etcd RV gets an error or the wrong events.
//   - Strict ordering across apiserver replicas. The sequencer is
//     per-process. Replicas racing on one key can briefly leave an older
//     state in the shadow until the next write or reconcile pass fixes it.
//   - Atomicity. A crash between the primary commit and the shadow write
//     loses that shadow write until the reconciler sees it.
//   - The Postgres synchronous allocator is never exercised. In dual mode
//     ExtraConfig.Allocator is nil, and claim status is computed by the etcd
//     controller path and mirrored verbatim. A clean shadow proves the data
//     migrated. It proves nothing about the allocator's correctness or
//     throughput, which need separate testing on a `postgres` deployment.
//   - The Postgres watch, cacher and read paths are not exercised. The
//     shadow is built without a cacher, and nothing reads or watches it
//     except the reconciler's plain LIST/GET.
package dual
