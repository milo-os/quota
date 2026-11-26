# Quota Performance Tests (k6 operator)

Kustomize component that runs the quota service performance tests in-cluster
via the [k6 operator](https://github.com/grafana/k6-operator).

**This is a generated component.** The scripts under `generated/` are produced
from `test/load/src/` by `hack/bundle-k6.sh`. Do not edit `generated/` directly.

## Contents

| File | Purpose |
|------|---------|
| `kustomization.yaml` | Component manifest with `configMapGenerator` for the bundled scripts |
| `rbac.yaml` | ServiceAccount + ClusterRole + ClusterRoleBinding for the runner |
| `testrun-setup.yaml` | TestRun: provision N tenants × M definitions × N×M grants |
| `testrun-throughput.yaml` | TestRun: sustained QuotaClaim create/delete throughput |
| `testrun-reads.yaml` | TestRun: mixed read latency workload |
| `testrun-scale.yaml` | TestRun: walks through escalating tenant counts |
| `generated/*.js` | Auto-generated bundled k6 scripts (do not edit) |

## Source of truth

`test/load/src/` contains the editable source. Each test imports from
`./lib/quota-client.js`. The bundler at `hack/bundle-k6.sh` inlines the lib
into each script and writes self-contained files into `generated/` (the k6
operator references a single ConfigMap key per TestRun and cannot follow
relative imports across files).

## Workflow

```bash
# Regenerate the bundled scripts after editing src/
task -t test/load/Taskfile.yaml generate

# Install the k6 operator (one-time)
task -t test/load/Taskfile.yaml k6:install-operator

# Apply the component (creates ConfigMap, RBAC, TestRun resources)
task -t test/load/Taskfile.yaml k6:apply

# Trigger a single test
task -t test/load/Taskfile.yaml k6:run TEST=throughput

# Tail logs
task -t test/load/Taskfile.yaml k6:logs TEST=throughput
```

## Authentication

The runner pod uses the projected ServiceAccount token mounted at
`/var/run/secrets/kubernetes.io/serviceaccount/token`. The k6 quota client
reads this file at init and uses it as a Bearer token. The bound
`ClusterRole` grants full CRUD on `quota.miloapis.com` resources and
namespace create/delete.

## Test parameters

Each TestRun has env vars you can edit before applying. Defaults:

| TestRun | Knobs |
|---------|-------|
| `setup` | `TENANT_COUNT=50`, `DEFINITION_COUNT=5`, `GRANT_AMOUNT=1000` |
| `throughput` | `VUS=20`, `DURATION=2m`, `TENANT_COUNT=50` |
| `reads` | `VUS=10`, `DURATION=2m`, `TENANT_COUNT=50` |
| `scale` | `TENANT_STEPS=10,50,100,250,500`, `STEP_OPS=200` |

To change these for a specific overlay, patch the TestRun env vars in your
overlay's kustomization or duplicate the testrun YAML with different values.

## Including in an overlay

Add the component to a dev/test overlay:

```yaml
# config/overlays/dev/kustomization.yaml
components:
  - ../../components/k6-performance-tests
```

The component is namespace-agnostic via the `quota-system` defaults but inherits
the overlay's `namespace:` setting if you set one.
