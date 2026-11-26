# Quota Service

A standalone, Kubernetes-native quota enforcement service with pluggable storage backends (etcd and Postgres). Designed to be consumed by any control plane — Milo is the first consumer but the service has no dependency on Milo's types.

## Reference Repositories

References:

- Milo source: [github.com/datum-cloud/milo](https://github.com/datum-cloud/milo) (no local clone). The quota types and registry implementations were copied from it.
- `/tmp/activity-ref` — Activity service. **Follow this repo's structure exactly** for repo layout, config organization, Dockerfile, Taskfile, and deployment patterns.

## Bootstrapping from Milo

Copy quota types and registry implementations from Milo. Key source paths:

| Milo Source Path | Purpose | Copy Target |
|-----------------|---------|-------------|
| `pkg/apis/quota/` | API types (ResourceRegistration, ResourceGrant, ResourceClaim, AllowanceBucket, and the policy kinds) | `pkg/apis/quota/v1alpha1/` |
| `internal/registry/quota/` | rest.Storage implementations | `internal/registry/quota/` |
| `internal/apiserver/` | Aggregated apiserver wiring | `internal/apiserver/` |
| `cmd/milo/` | Binary entrypoint (reference for flags/options) | `cmd/quota/` |

### Refactoring rules when copying from Milo:

1. **Rename types:** ResourceRegistration → QuotaDefinition, ResourceGrant → QuotaGrant, ResourceClaim → QuotaClaim, AllowanceBucket → QuotaBucket. Keep GrantCreationPolicy and ClaimCreationPolicy — they configure when the quota system applies (which resource creations produce claims, which consumers get grants).
2. **Keep the API group:** `quota.miloapis.com`. Do not rename it.
3. **Strip Milo imports:** Replace all `github.com/datum-cloud/milo/...` with this module, `go.miloapis.com/quota/...`. Zero Milo imports.
4. **Make consumer refs opaque:** Plain strings `{apiGroup, kind, name}` plus an optional `namespace`. No typed references to Organization, Project, etc. `consumerRef` is required on claims and grants.
5. **Preserve field semantics:** Keep grant allowances and buckets, claim requests, bucket status (limit, allocated, available, claimCount) and `Granted`/`Active` conditions. Claims start at `Granted=False`/`PendingEvaluation`. Milo had no immutability rules; here grant `consumerRef` and claim `consumerRef`, `resourceRef` and `requests` are immutable.

## Repo Layout (aligned to datum-cloud/activity)

```
quota/
├── CLAUDE.md
├── Dockerfile
├── Taskfile.yaml
├── go.mod
├── go.sum
├── renovate.json
├── LICENSE
├── README.md
│
├── cmd/
│   └── quota/
│       ├── main.go                  # Binary entrypoint, subcommands
│       ├── serve.go                 # Apiserver serve subcommand
│       └── controller_manager.go    # Controller-manager subcommand, ProviderFactory
│
├── pkg/
│   ├── apis/
│   │   └── quota/
│   │       ├── doc.go               # +k8s:deepcopy-gen=package (internal version)
│   │       ├── register.go
│   │       ├── types.go             # Internal types
│   │       ├── zz_generated.deepcopy.go
│   │       ├── install/
│   │       │   └── install.go       # Scheme installer
│   │       └── v1alpha1/
│   │           ├── doc.go
│   │           ├── register.go
│   │           ├── types.go         # QuotaDefinition, QuotaGrant, QuotaClaim, QuotaBucket, policies
│   │           └── zz_generated.*.go
│   ├── admission/
│   │   └── enforcement/             # Exported enforcement admission plugin and options
│   ├── client/                      # Generated: clientset, informers, listers
│   └── generated/
│       └── openapi/
│
├── internal/
│   ├── apiserver/
│   │   └── apiserver.go             # Aggregated apiserver setup
│   ├── quota/                       # Admission validation, controllers, CEL, policy engine
│   ├── registry/
│   │   └── quota/
│   │       ├── quotadefinition/
│   │       ├── quotagrant/
│   │       ├── quotaclaim/          # Allocating and Postgres-native storage
│   │       ├── quotabucket/
│   │       ├── claimcreationpolicy/
│   │       ├── grantcreationpolicy/
│   │       └── registryutil/        # Shared validation and table helpers
│   ├── storage/
│   │   ├── backend.go               # Backend selection
│   │   ├── etcd/                    # etcd configuration
│   │   ├── postgres/                # Store, allocator, native pool
│   │   └── dual/                    # etcd primary, Postgres shadow, reconciler
│   ├── watch/
│   │   └── postgres.go              # Changelog-based watch hub
│   ├── webhook/
│   │   └── enforcement.go           # /webhook/enforce handler
│   ├── testutil/
│   │   └── pgtest/                  # Shared Postgres test database setup (POSTGRES_DSN)
│   ├── metrics/
│   │   └── metrics.go               # Prometheus metrics
│   └── version/
│       └── version.go
│
├── migrations/                       # SQL migrations (repo root, like activity)
│   ├── README.md
│   ├── 001_initial_schema.sql       # Whole schema (001-009 folded together before any deploy)
│   └── migrate.sh
│
├── config/
│   ├── base/                        # Apiserver (2 replicas + PDB) and controller-manager Deployments, RBAC. No Secret
│   ├── components/
│   │   ├── README.md                # Documents available components
│   │   ├── dev-postgres-credentials/ # Dev-only postgres-credentials Secret (dev/perf overlays)
│   │   ├── test-infra-access/       # Dev/CI RBAC: metrics scraping, e2e policy trigger kinds
│   │   ├── api-registration/        # APIService registration
│   │   ├── namespace/
│   │   ├── cert-manager-ca/         # TLS certs via cert-manager
│   │   ├── etcd/                    # Single-node etcd (for dev/standalone)
│   │   ├── postgres/                # Postgres for quota storage
│   │   ├── postgres-migrations/     # Schema migration Job
│   │   ├── nats-streams/            # NATS JetStream streams (optional)
│   │   ├── enforcement-webhook/     # ValidatingWebhookConfiguration (failurePolicy Fail), caller RBAC, README
│   │   ├── observability/           # ServiceMonitors, PrometheusRule alerts, dashboards
│   │   ├── k6-performance-tests/
│   │   └── perf-profile/            # Normalizes log level for the perf-* overlays
│   ├── dependencies/                # External operator/chart dependencies
│   │   └── nats/
│   ├── milo/                        # Milo-specific integration (first consumer)
│   │   ├── kustomization.yaml       # kind: Component. Apply ./standalone, not this directory
│   │   ├── standalone/              # Applicable wrapper
│   │   ├── iam/                     # protected-resources, roles, policies
│   │   ├── registrations/           # QuotaDefinitions ported from Milo
│   │   ├── claim-policies/          # ClaimCreationPolicies ported from Milo
│   │   ├── rbac/                    # Controller-manager read access to Milo's trigger kinds (aggregated)
│   │   ├── enforcement-webhook/     # Webhook scope for Milo's kinds; list after components/enforcement-webhook
│   │   └── telemetry/               # ResourceMetricsPolicy (milo_quota_* metrics)
│   └── overlays/
│       ├── dev/                     # etcd
│       ├── dev-postgres/            # postgres backend
│       ├── dev-dual/                # dual backend
│       ├── perf-etcd/, perf-postgres/, perf-dual/  # CI load tests (dev-* + perf-profile)
│       └── test-infra/              # CI e2e
│
├── hack/
│   ├── boilerplate.go.txt
│   ├── update-codegen.sh
│   ├── update-migrations-configmap.sh
│   ├── verify-codegen.sh
│   └── bundle-k6.sh
│
├── observability/                   # Jsonnet source for Grafana dashboards
│
├── test/
│   ├── e2e/                         # Chainsaw suites, .chainsaw.yaml
│   └── load/                        # k6 performance tests, ci/ helpers for the CI job
│
├── examples/
│   └── basic/
│       ├── kustomization.yaml
│       ├── definition.yaml
│       ├── grant.yaml
│       └── claim.yaml
│
├── docs/
│   ├── api.md
│   ├── architecture/
│   │   └── README.md
│   ├── storage-backends.md
│   ├── integration-guide.md
│   ├── milo-migration.md
│   ├── adr/                         # Architecture decision records
│   └── enhancements/                # Design proposals
│
├── .github/
│   └── workflows/
│       ├── build-apiserver.yaml
│       ├── testing.yaml
│       └── performance.yaml         # k6 per backend + Postgres Go benchmarks
```

## Key Patterns from Activity Service to Follow

1. **config/base/** has the core Deployment, Service, SA, RBAC. Uses `images:` transformer + `replacements:` for image tag propagation.
2. **config/components/** are `kind: Component` kustomizations. Each is self-contained and optional. Overlays pick which components to include.
3. **config/overlays/** compose base + components + patches. `dev`, `dev-postgres` and `dev-dual` use standalone/single-node infra. Production overlays use HA components.
4. **config/dependencies/** are external Helm charts (currently NATS) managed via FluxCD HelmRelease/HelmRepository.
5. **config/milo/** is Milo-specific integration (IAM, ported definitions and claim policies, metrics policy). This is the consumer integration point. It is a Component; apply `config/milo/standalone`.
6. **migrations/** lives at repo root with numbered SQL files and a migrate.sh script.
7. **internal/** for private code (apiserver, registry, storage, watch, metrics). **pkg/** for public API types and generated clients.
8. **cmd/quota/** with subcommand files (main.go, serve.go, controller_manager.go) in one package — same pattern as `cmd/activity/`.
9. **Deployment** uses env vars for all config, cert-manager CSI for TLS, security context with nonroot/readonly/drop-all-caps.
10. **test/e2e/** for Chainsaw tests (`task e2e`). **examples/** for reference manifests.

## Key Design Decisions

1. **Bootstrap from Milo, depend on nothing.** Copy and refactor. go.mod must not reference `github.com/datum-cloud/milo`.
2. **Consumer references are opaque.** `{apiGroup, kind, name}` strings, not Go type imports.
3. **Transactional allocation in the request path, Postgres only.** With `--quota-storage-backend=postgres`, QuotaClaim CREATE decides synchronously and atomically. `etcd` (default) and `dual` use controller-driven asynchronous allocation, and enforcement admission waits for the claim decision. The controller-manager takes `--quota-storage-backend`, derives `--quota-allocation-mode` from it, and refuses a mismatch.
4. **Storage backend is config-driven.** `--quota-storage-backend` selects `etcd`, `postgres`, or `dual`. Postgres needs `--postgres-dsn`.
5. **Watch contract honored.** All backends: initial list, event stream, bookmarks, 410 Gone. Postgres assigns resource versions in commit order under a global advisory lock (`quota_next_rv()` in the schema); see the ADR addendum for the throughput cost.
6. **Policy kinds stay in the service.** GrantCreationPolicy and ClaimCreationPolicy configure when quota is enforced; they are part of the API, not consumer-side code.
7. **etcd is the default.** Migration safety net — identical behavior to Milo's current quota system. `dual` keeps etcd as the source of truth while a Postgres shadow is validated.

## Conventions

- Use `k8s.io/apiserver` and `k8s.io/apimachinery` libraries
- Error handling: typed API errors (`errors.NewConflict`, `errors.NewForbidden`)
- Logging: `klog.V(2).InfoS(...)` operational, `klog.ErrorS(...)` errors
- Metrics: Prometheus via `k8s.io/component-base/metrics`
- Tests: table-driven, Chainsaw for e2e
- Match activity service patterns for Dockerfile, Taskfile, CI workflows
- Security: nonroot, readonly rootfs, drop all caps, seccomp RuntimeDefault

## Verification Commands

```bash
go build ./...
go test ./pkg/... ./internal/... ./cmd/... -count=1
golangci-lint run ./...
./hack/verify-codegen.sh
hack/update-migrations-configmap.sh --verify
```

Postgres-backed tests skip without a database. To run them, point `POSTGRES_DSN` at a disposable database; `QUOTA_REQUIRE_POSTGRES=1` makes them fail instead of skip, and `-p 1` is required because the packages share one database:

```bash
POSTGRES_DSN='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  QUOTA_REQUIRE_POSTGRES=1 go test -p 1 -count=1 ./...
```
