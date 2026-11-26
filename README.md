# Quota Service

A standalone, Kubernetes-native quota enforcement service with pluggable storage backends (etcd, Postgres, and a dual-write migration mode). Designed to be consumed by any control plane. Milo is the first consumer.

## Overview

The service is an aggregated API server plus a controller-manager. It serves these kinds in `quota.miloapis.com/v1alpha1`, the same group as Milo's built-in quota API:

- **QuotaDefinition** defines a quotable resource type.
- **QuotaGrant** gives a consumer an allowance.
- **QuotaBucket** is the running total for a consumer and resource type. Controllers maintain it.
- **QuotaClaim** uses part of an allowance.
- **ClaimCreationPolicy** and **GrantCreationPolicy** configure when quota is enforced: which resource creations produce claims, and which resources produce grants.

The kinds are renamed from Milo: `ResourceRegistration` to `QuotaDefinition`, `ResourceGrant` to `QuotaGrant`, `ResourceClaim` to `QuotaClaim` and `AllowanceBucket` to `QuotaBucket`.

### How claims are decided

Allocation is synchronous and transactional only with `--quota-storage-backend=postgres`. The decision is in the create response.

With `etcd` (the default) and `dual`, the controller-manager decides claims after the create returns, and a new claim reads `Granted=False`, reason `PendingEvaluation` until then. For resources covered by a ClaimCreationPolicy, admission creates the claim and waits for the decision, 25 seconds by default, before it admits or denies the resource.

No backend returns a 409 for lack of quota. See [docs/architecture/README.md](docs/architecture/README.md).

## Quick Start

### Prerequisites

- Go 1.25 or later (`go.mod`)
- Kubernetes 1.30 or later (the enforcement webhook needs it)
- [Task](https://taskfile.dev/)
- [kind](https://kind.sigs.k8s.io/) for local development
- cert-manager with the CSI driver
- FluxCD

### Build and test

```bash
task build           # builds bin/quota
task test            # go test ./pkg/... ./internal/...
task lint            # golangci-lint run ./...
./hack/verify-codegen.sh
```

### Development environment

```bash
export TASK_X_REMOTE_TASKFILES=1
task dev:setup
```

This creates a kind cluster, installs NATS and the observability stack, builds and loads the image, and deploys the `dev` overlay (etcd backend). Other overlays are `dev-postgres`, `dev-dual` and `test-infra`.

### Manual deployment

```bash
kubectl apply -k config/overlays/dev
kubectl get apiservice v1alpha1.quota.miloapis.com
```

The overlays assume the Prometheus Operator and Grafana Operator CRDs and the NATS CRDs are installed. `task dev:setup` installs them. See the [integration guide](docs/integration-guide.md#prerequisites).

### End-to-end tests

```bash
task e2e    # chainsaw test test/e2e/ --config test/e2e/.chainsaw.yaml
```

## API Usage

Amounts are integers in the definition's base unit.

```yaml
# 1. Define what can be quota-controlled
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaDefinition
metadata:
  name: compute-vcpu
spec:
  resourceType: "compute.datumapis.com/vCPU"
  type: Allocation
  consumerType:
    apiGroup: resourcemanager.miloapis.com
    kind: Project
  baseUnit: millicpu
  displayUnit: vCPU
  unitConversionFactor: 1000
  claimingResources:
    - apiGroup: compute.datumapis.com
      kind: VirtualMachine
---
# 2. Grant quota to a consumer
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaGrant
metadata:
  name: project-compute-vcpu
  namespace: default
spec:
  consumerRef:
    apiGroup: resourcemanager.miloapis.com
    kind: Project
    name: example-project
  allowances:
    - resourceType: "compute.datumapis.com/vCPU"
      buckets:
        - amount: 100000
---
# 3. Claim quota
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaClaim
metadata:
  name: my-vm
  namespace: default
spec:
  consumerRef:
    apiGroup: resourcemanager.miloapis.com
    kind: Project
    name: example-project
  requests:
    - resourceType: "compute.datumapis.com/vCPU"
      amount: 10000
  resourceRef:
    apiGroup: compute.datumapis.com
    kind: VirtualMachine
    name: my-vm
    namespace: default
```

Working copies are in `examples/basic/`. Check the result with:

```bash
kubectl get quotaclaim my-vm -n default -o jsonpath='{.status.conditions[?(@.type=="Granted")]}'
```

## Storage Backends

| Backend | Flag | Allocation | Description |
|---------|------|-----------|-------------|
| etcd | `--quota-storage-backend=etcd` | Controller-driven | Default. Matches Milo's current behavior |
| Postgres | `--quota-storage-backend=postgres` | Synchronous, transactional | Needs `--postgres-dsn` and the schema in `migrations/` |
| Dual | `--quota-storage-backend=dual` | Controller-driven | etcd is the source of truth. Postgres is a shadow for validating a migration |

Run the controller-manager with the same `--quota-storage-backend`. It derives its allocation mode from the flag and exits at startup on a mismatch. See [docs/storage-backends.md](docs/storage-backends.md) for every flag and the Postgres throughput trade-off.

## Documentation

- [API Reference](docs/api.md)
- [Architecture](docs/architecture/README.md), including known gaps
- [Storage Backends](docs/storage-backends.md)
- [Integration Guide](docs/integration-guide.md)
- [Migrating Milo to the quota service](docs/milo-migration.md)
- [ADR 0001: dual-backend allocation](docs/adr/0001-postgres-first-architecture.md)
- [Enhancement 0001: edge-delegated quota](docs/enhancements/0001-edge-delegated-quota.md)
- [Migrations](migrations/README.md)

## License

GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.
