# Quota Service Performance Tests

k6-based performance tests for the quota service. The tests answer:

- How does claim creation latency scale as the number of tenants grows?
- What's the maximum sustainable claim throughput?
- How does read latency (list/get) behave under tenant scale?
- How do the **etcd**, **postgres**, and **dual** storage backends compare?

## Prerequisites

- A running quota apiserver (any storage backend) reachable via `kubectl proxy`
- `k6` installed locally (`brew install k6` on macOS)
- `kubectl` configured against the cluster

## Test scripts

| Script | Purpose |
|--------|---------|
| `src/setup-tenants.js` | Provision N tenants × M definitions × N×M grants. Run once before throughput/reads. |
| `src/claim-throughput.js` | Hot path: sustained QuotaClaim create/delete with configurable concurrency. |
| `src/read-latency.js` | Mixed read workload (per-tenant list, cluster list, get, definition list, bucket list). |
| `src/tenant-scale.js` | Walks through escalating tenant counts and measures at each step. |
| `src/lib/quota-client.js` | Shared HTTP client with typed helpers for the six quota resources. |

## Quick start

In one terminal, start the kubectl proxy that gives k6 unauthenticated HTTP access:

```bash
KUBECONFIG=.test-infra/kubeconfig kubectl proxy --port=8001
```

Or via task:

```bash
task -t test/load/Taskfile.yaml proxy
```

In another terminal, provision tenants and run the tests:

```bash
# 1. Provision 50 tenants × 5 resource types
task -t test/load/Taskfile.yaml setup TENANT_COUNT=50 DEFINITION_COUNT=5

# 2. Sustained claim throughput, 20 concurrent VUs for 2 minutes
task -t test/load/Taskfile.yaml throughput VUS=20 DURATION=2m

# 3. Mixed read workload
task -t test/load/Taskfile.yaml reads VUS=10 DURATION=2m

# 4. Walk through tenant scale (10 → 500)
task -t test/load/Taskfile.yaml scale TENANT_STEPS=10,50,100,250,500

# 5. Tear down
task -t test/load/Taskfile.yaml cleanup
```

Results land in `test/load/results/`.

## Comparing storage backends

The quota service supports three backends. To benchmark each:

1. Deploy with the desired overlay (`config/overlays/dev`, `dev-postgres`, or `dev-dual`)
2. Run the proxy and the test sequence above
3. Compare summaries in `test/load/results/`

A typical comparison run:

```bash
# Etcd
task dev:setup
task -t test/load/Taskfile.yaml setup throughput reads
mv test/load/results test/load/results-etcd

# Postgres
kubectl delete ns quota-system
kubectl apply -k config/overlays/dev-postgres
task -t test/load/Taskfile.yaml setup throughput reads
mv test/load/results test/load/results-postgres

# Dual
kubectl delete ns quota-system
kubectl apply -k config/overlays/dev-dual
task -t test/load/Taskfile.yaml setup throughput reads
mv test/load/results test/load/results-dual
```

## Configuration via environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `QUOTA_API_URL` | `http://localhost:8001` | All scripts |
| `QUOTA_TOKEN` | (none) | All scripts (optional bearer token) |
| `K6_INSECURE_SKIP_TLS_VERIFY` | `true` | All scripts |
| `TENANT_COUNT` | `50` | setup, throughput, reads |
| `DEFINITION_COUNT` | `5` | setup, throughput, reads, scale |
| `GRANT_AMOUNT` | `100` | setup, scale |
| `VUS` | `20` (throughput), `10` (reads) | throughput, reads |
| `DURATION` | `2m` | throughput, reads |
| `TENANT_STEPS` | `10,50,100,250,500` | scale |
| `STEP_OPS` | `200` | scale |

## Interpreting results

k6 prints a summary at the end of each run. The metrics that matter most for the quota service:

- **`claim_create_latency_ms` p95/p99** — the headline number for claim throughput
- **`claim_success_rate`** — should be >0.95; lower means quota exhaustion or errors
- **`tenant_grant_list_ms`** — controllers polling per-tenant grants
- **`cluster_grant_list_ms`** — UI/admin listing all grants (this scales with total grant count)
- **`http_req_failed`** — should be near zero

For the `tenant-scale.js` walk, the metrics are tagged with `tenants=<count>`. To see how each metric varies with tenant count, query the JSON output (e.g., with `jq`) or load `results/scale-raw.json` into Grafana / k6 Cloud.

## Notes

- The hot path test (`claim-throughput.js`) creates a claim, immediately deletes it, then loops. This avoids exhausting allowances during long runs and isolates the create/delete latency from quota arithmetic.
- The read tests deliberately mix per-tenant and cluster-wide lists; cluster-wide lists scale with total object count, not with the number of tenants you're querying.
- The scale walk does its provisioning and measurement in a single VU to keep the steps cleanly serialized. For pure throughput numbers at a fixed tenant count, use `claim-throughput.js` directly.
- All test resources live in namespaces named `quota-perf-tenant-N`. The cleanup task removes them.

## Running in-cluster via the k6 operator

The kustomize component at `config/components/k6-performance-tests/` packages the same scripts as Kubernetes-native `TestRun` resources executed by the [k6 operator](https://github.com/grafana/k6-operator). The component contains:

- A `ConfigMap` with auto-generated, self-contained scripts
- A `ServiceAccount` + `ClusterRole` + `ClusterRoleBinding` granting the runner permissions on `quota.miloapis.com` resources and namespaces
- Four `TestRun` resources (one per script) with sensible default parameters

The bundled scripts are generated from `test/load/src/` by `hack/bundle-k6.sh`, which inlines the shared lib so each TestRun script is a single self-contained file (the k6 operator can't follow relative imports across files).

### Workflow

```bash
# 1. Bundle k6 scripts into the kustomize component (run after editing src/)
task -t test/load/Taskfile.yaml generate

# 2. (one-time) Install the k6 operator into the cluster
task -t test/load/Taskfile.yaml k6:install-operator

# 3. Apply the kustomize component (creates SA, RBAC, ConfigMap, TestRuns)
task -t test/load/Taskfile.yaml k6:apply

# 4. Run any one of the four tests
task -t test/load/Taskfile.yaml k6:run TEST=setup
task -t test/load/Taskfile.yaml k6:run TEST=throughput
task -t test/load/Taskfile.yaml k6:run TEST=reads
task -t test/load/Taskfile.yaml k6:run TEST=scale

# 5. Tail logs while it runs
task -t test/load/Taskfile.yaml k6:logs TEST=throughput
```

### How the in-cluster scripts authenticate

When run via the k6 operator, the test pod's ServiceAccount token is mounted at the standard path `/var/run/secrets/kubernetes.io/serviceaccount/token`. The lib's `loadToken()` reads it at init time and uses it as a Bearer token for all requests. No `QUOTA_TOKEN` env var is needed in-cluster.

For local runs via `kubectl proxy`, no token is needed because the proxy handles auth.

### Customizing test parameters

Each `testrun-*.yaml` file has env vars (`TENANT_COUNT`, `VUS`, `DURATION`, etc.) that you can edit before applying. Or override at apply time with kustomize patches in your overlay.

### Source of truth

`test/load/src/` is the source of truth. The `generated/` directory under the kustomize component is auto-generated and should not be edited directly. Re-run `task generate` after any change to scripts or the lib.

## Running in CI

`.github/workflows/performance.yaml` runs these tests on GitHub Actions. It runs nightly, on demand from the Actions tab, and on pull requests labeled `performance`. It does not run on every push.

It has three jobs:

- **k6**: one run per backend, each on its own kind cluster. Each run deploys `config/overlays/perf-etcd`, `perf-postgres` or `perf-dual`, then runs `setup`, `throughput` and `reads`. The perf overlays are the dev overlays with `config/components/perf-profile` applied, which sets the same log level on all three so the backend is the only difference.
- **linux-sweep**: the number to track over time. `ci/linux-sweep.sh` runs etcd and kube-apiserver from envtest, the quota apiserver and controller-manager from a locally built binary, and Postgres 16 from a pinned, checksummed binary build, each pinned to its own CPUs, with no cluster and no Docker. k6 sends claim creates and deletes through kube-apiserver's aggregation proxy, as in production. It measures throughput at 10, 30, 60 and 120 clients, three runs each, and reports medians with the min–max range, latency, CPU per request for the quota apiserver and kube-apiserver, Postgres CPU and Postgres transactions per claim. `ci/summarize-sweep.sh` compares the result with the last scheduled run on `main` when that ran on the same CPU model and layout, and warns (without failing) when a client count is more than 15% slower. The job fails only if claims fail.
- **go-benchmarks**: runs the benchmarks in `internal/storage/postgres/bench_test.go` against a Postgres service container. They measure the global resource-version lock (`quota_next_rv()`): an RV draw on its own, `Store.Create` on distinct keys, and `Allocate` on distinct buckets and on one shared bucket.

Defaults are 20 tenants, 3 definitions, 10 VUs and 2 minutes per test. Manual runs can change these and pick one backend.

Results appear in each job's summary and are uploaded as the `k6-<backend>`, `linux-sweep` and `go-benchmarks` artifacts. `ci/summarize.sh` builds the summary and applies the gates. A run fails if:

- the claim success rate falls below 95%,
- the HTTP failure rate rises above 5%,
- or the read success rate falls below 95%.

Latency is reported but not gated, because shared runners are too noisy for a latency limit to tell a regression from a slow machine. Set `MAX_CREATE_P95_MS` or `MAX_READ_P95_MS` on that step to gate on latency.

`ci/wait-for-buckets.sh` runs between setup and the throughput test. `setup-tenants.js` returns once the grants are written, but buckets are created asynchronously, and claims against a missing bucket count as denials.

Treat the k6 job's numbers as a smoke test, not a trend line. Each run is one 4-vCPU runner hosting kind, the apiserver, the backend and k6, and the Postgres in kind runs with `synchronous_commit=off`.

### Where the sweep runs

The job runs on the self-hosted `milo-linux` pool defined in `datum-cloud/infra` (`infrastructure/actions-runner-controller/production/milo-linux-helm-release.yaml`). Each runner pod requests 3 CPUs on an `n2d-standard-4` node, so only one runner fits per node and the job has the machine to itself apart from node daemons. The pods run unprivileged with no Docker, which is why the script starts its own Postgres from binaries.

With 4 CPUs, Postgres, the quota apiserver and kube-apiserver get one CPU each, and etcd, the controller-manager and k6 share the fourth. An `n2d-standard-4` is two physical cores with two threads each, so some of those CPUs are hyperthread siblings. With 8 CPUs Postgres and the quota apiserver get two each, and with 10 or more the layout matches the measurements in [docs/storage-backends.md](../../docs/storage-backends.md#throughput-trade-off).

Things that still move the numbers:

- The pool is on spot VMs. A preempted node fails the job; rerun it.
- GCE can place `n2d` VMs on more than one AMD EPYC generation. The summary records the CPU model and compares only against a baseline from the same model and layout.
- Node daemons (logging, metrics, DNS cache) share the node.

Set the repository variable `PERF_RUNNER` to run on another pool, for example `datum-linux` if the repository is in the `datum-cloud` organization, or `ubuntu-latest`.

To run the sweep on a Linux machine:

```bash
CGO_ENABLED=0 go build -o /tmp/quota ./cmd/quota
export KUBEBUILDER_ASSETS=$(setup-envtest use 1.34.x -p path)
QUOTA_BIN=/tmp/quota test/load/ci/linux-sweep.sh /tmp/sweep
test/load/ci/summarize-sweep.sh /tmp/sweep/results.json
```

It needs Go, k6, curl, unzip, xz, jq, openssl and `taskset`, and at least 4 CPUs. It downloads Postgres itself; set `PGURL` to use an existing database instead.
