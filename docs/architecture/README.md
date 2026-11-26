# Quota Service Architecture

> Last verified: 2026-11-22 against `cmd/quota/`, `internal/apiserver/`, `internal/storage/`, `internal/quota/controllers/setup.go` and `pkg/admission/enforcement/`.

## Overview

The quota service is a standalone, Kubernetes-native quota enforcement service. It is an aggregated API server plus a controller-manager. Any control plane can consume it. Consumer references are opaque `{apiGroup, kind, name}` strings and the service imports none of its consumers' types.

The API group is `quota.miloapis.com`, matching Milo. Milo is the first consumer. See [milo-migration.md](../milo-migration.md).

## Components

The `quota` binary has three subcommands.

| Subcommand | Role |
|------------|------|
| `quota serve` | The aggregated apiserver. Serves the quota API, runs the two quota admission plugins, and serves the enforcement webhook at `/webhook/enforce` |
| `quota controller-manager` | Reconciles quota resources. Which controllers run depends on the allocation mode |
| `quota version` | Prints build information |

```
                       +----------------------------+
 consumer control      |   kube-apiserver           |
 plane (e.g. Milo)     |   (aggregation layer)      |
                       +-----+----------------+-----+
                             |                |
              quota API      |                | ValidatingWebhookConfiguration
              (APIService)   |                | POST /webhook/enforce
                             v                v
                       +----------------------------+        +-----------------------+
                       |   quota apiserver          |        | quota controller-     |
                       |   - QuotaValidation        |<-------| manager               |
                       |   - QuotaEnforcement       |        | (reconcile, allocate  |
                       |   - webhook handler        |        |  in controller mode)  |
                       +-------------+--------------+        +-----------------------+
                                     |
                      +--------------+--------------+
                      |              |              |
                   +--v---+      +---v----+     +---v---+
                   | etcd |      |Postgres|     | dual: |
                   |      |      |        |     | both  |
                   +------+      +--------+     +-------+
```

### Resource hierarchy

```
QuotaDefinition (what can be quota-controlled)
  |
  +-- QuotaGrant (allowance for a consumer)
        |
        +-- QuotaBucket (running total per consumer and resource type)
              |
              +-- QuotaClaim (use of that allowance)

ClaimCreationPolicy  (which creations produce claims)
GrantCreationPolicy  (which resources produce grants)
```

Field-level detail is in the [API reference](../api.md).

### Controller-manager

`controllers.SetupQuotaControllers` registers these controllers:

| Controller | Runs in |
|------------|---------|
| QuotaDefinition, QuotaGrant, QuotaBucket (limits, grant counts, bucket creation) | Both modes |
| ClaimCreationPolicy and GrantCreationPolicy validation | Both modes |
| Grant creation and claim creation from policies | Both modes |
| QuotaClaim ownership (owner references, orphan cleanup) | Both modes |
| Denied and stale-pending auto-claim cleanup | Both modes |
| Bucket allocation pump and claim aggregator | `controller` mode only |

The controller-manager takes `--quota-storage-backend` and derives `--quota-allocation-mode` from it. A mismatch is a startup error. See [storage-backends.md](../storage-backends.md#allocation-modes).

It can reconcile more than one control plane through a multicluster provider. The quota service ships none, because discovering control planes is consumer-specific. A consumer supplies one through `ProviderFactory` (see [Extension points](#extension-points)).

## Request flows

### Claim created by a client

The path depends on the storage backend.

**Postgres (synchronous).** One transaction locks the affected buckets, decides each request, updates the buckets, inserts the claim with its final `Granted` condition and commits. The create response carries the decision. A missing bucket returns 403 Forbidden. A denied claim is still created, with `Granted=False` and reason `QuotaExceeded`.

**etcd and dual (asynchronous).** The create stores the claim with `Granted=False`, reason `PendingEvaluation`, and returns. The controller-manager's allocation pump decides each request against the bucket, and the claim aggregator sets the `Granted` condition. The caller observes the decision by reading or watching the claim.

There is no synchronous 409 on any backend. Capacity is decided either inside the create (Postgres) or after it (etcd, dual).

### Resource creation protected by a policy

This is the path that blocks a request. A ClaimCreationPolicy names a resource kind. When that kind is created:

1. The enforcement admission plugin finds the policy, builds a claim from the template and resolves the consumer.
2. It resolves any claim left by an earlier attempt. A granted claim short-circuits. A denied claim fails the request as a conflict until cleanup removes it. A pending one is deleted.
3. It registers a waiter, creates the claim, and waits for the decision for up to `--quota-enforcement-claim-timeout` (25 seconds).
4. On the Postgres backend the create response already has the decision, so it does not wait.
5. Granted admits the resource. Anything else returns 403 Forbidden with a message that names the cause: denied, timed out, conflict, internal error or misconfigured policy.

The plugin is registered in the quota apiserver's admission chain, where it skips quota API kinds because `QuotaValidation` handles those. Resources in other control planes reach it through `/webhook/enforce`, which the kube-apiserver calls through a `ValidatingWebhookConfiguration` and which hands the request to the same plugin. A control plane that embeds the plugin directly (as Milo does today) calls it in-process. Only `CREATE` operations are checked, and dry-run requests are skipped.

See the [integration guide](../integration-guide.md#enforcement-admission) for consumer resolution, retry handling and webhook availability.

## Authentication

The apiserver uses the generic apiserver's delegated authentication. Requests that kube-apiserver proxies are authenticated as a front proxy: the TLS client certificate must chain to the requestheader client CA from the `kube-system/extension-apiserver-authentication` ConfigMap and its common name must be in `requestheader-allowed-names`. The user comes from the headers the ConfigMap names, usually `X-Remote-User`, `X-Remote-Group` and `X-Remote-Extra-*`. Direct client certificates and bearer tokens (via TokenReview) are also accepted.

Upstream verifies the front proxy's certificate chain on every request, which is an RSA signature check. kube-apiserver reuses its HTTP/2 connections, so it presents the same certificate on many requests in a row. `internal/authn/frontproxy` keeps successful chain verifications for `--front-proxy-verify-cache-ttl` (default 10m, `0` disables), up to 256 entries. On every request, cached or not, it still checks:

- that the CA pool is the one the entry was verified against. A change to the ConfigMap's CA bundle produces a new pool, so a rotation or removal takes effect on the next request,
- that every certificate in the verified chain is inside its validity period,
- the common name against the current allowed names,
- the user headers, using the upstream header extractor.

Failures are not cached. A request the cache does not authenticate goes to the unchanged upstream authenticator, so rejections, direct client certificates, tokens and anonymous requests behave as before.

## Policies

The service is not a bare ledger. Two policy kinds decide when quota applies:

- **ClaimCreationPolicy** names a trigger kind, optional CEL constraints and a claim template. Creating a matching resource produces a claim and, through admission, a quota check.
- **GrantCreationPolicy** names a trigger kind, optional CEL constraints, an optional parent context and a grant template. A matching resource produces a grant.

The ledger itself, meaning definitions, grants, buckets and claims, stays independent of any consumer's model. The policies are how a consumer connects its own resources to it.

## Storage backends

| Backend | Allocation | Watch | Use |
|---------|-----------|-------|-----|
| `etcd` (default) | Controller-driven | etcd native | Default. Matches Milo's built-in behavior |
| `postgres` | Synchronous, transactional | Changelog with LISTEN/NOTIFY, commit-ordered | Decision in the create response |
| `dual` | Controller-driven on etcd | etcd native | Validate a Postgres shadow before cutover |

See [storage-backends.md](../storage-backends.md) for flags, the commit-order lock and its throughput cost. The reasoning behind both allocation paths is in [ADR 0001](../adr/0001-postgres-first-architecture.md).

## Extension points

Consumers embed or wrap the quota service through these hooks.

The admission plugin is in `pkg/admission/enforcement`, which other modules can import. Pass these `Option` values to `NewQuotaEnforcementPlugin`:

| Option | Purpose |
|--------|---------|
| `WithClientResolver` | Pick the control plane a request's claim lives in. Returns a scope ID and a dynamic client. For consumers that run one control plane per tenant |
| `WithWatchManagerFactory` | Replace how per-scope claim watch managers are built, for example to share an informer |
| `WithConsumerResolver` | Derive the consumer when the policy template sets none. Runs before the configured mappings |
| `WithNamespaceLookup` | Supply namespace annotations for the namespace-annotation mapping |
| `WithConfig`, `WithLogger` | Configuration and logging |

The controller-manager's `ProviderFactory` and `WithProviderFactory` hook supply the multicluster provider. They are in package `main` (`cmd/quota`), so another module cannot import them yet. That is a known follow-up.

## Design principles

1. **Policy kinds are part of the service.** They configure when quota is enforced.
2. **Synchronous allocation is a Postgres property.** Only `--quota-storage-backend=postgres` decides a claim inside the create request. Other backends allocate from controllers, and admission waits for the decision.
3. **Consumer-agnostic.** Opaque references, no typed consumer imports.
4. **Storage-pluggable.** Backend selection is a flag.
5. **Watch-compatible.** Initial list, ordered events, bookmarks and 410 Gone on every backend.
6. **No Milo dependency in the module.** The service does not import Milo.

## Known gaps and follow-ups

- **Postgres integration tests and the e2e suites have not run in CI yet.** `.github/workflows/testing.yaml` now provides a Postgres service and a kind e2e job, but neither has completed a run.
- **`ProviderFactory` cannot be imported.** It lives in `cmd/quota` (package `main`). Multi-cluster consumers need it moved to an importable package.
- **Compaction does not hold back for slow watchers.** Retention is time-based. A connected watcher that falls behind the floor gets 410 Gone.
- **No exact equivalent of Milo's enforcement-edge-cases suite.** The standalone e2e suites cover enforcement but not every case in Milo's.
- **Dual-mode repair has no flag.** Repair and the reconciler interval are code options only.
- **The Postgres write ceiling is measured only on local hardware.** About 2,355 claims/s at 240 clients on one Linux host ([storage-backends.md](../storage-backends.md#throughput-trade-off)). Production storage, `synchronous_commit` and replication are unmeasured, and there is no metric for time spent waiting on the commit-order lock.
- **The claim-creation controller charges existing objects outside the webhook's scope.** It creates a claim for every matching object that has none, including objects in namespaces without the `quota.miloapis.com/enforcement=enabled` label, which admission never checks.
