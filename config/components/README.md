# Kustomize Components

Optional components that can be selectively included in overlays.

## Available Components

- **namespace** - Creates the quota-system namespace
- **api-registration** - Kubernetes API aggregation (APIService registration). cert-manager injects the quota CA into its `caBundle`
- **cert-manager-ca** - The quota CA and the `quota-ca-issuer` Issuer that signs the apiserver's serving certificate. Required by `config/base`; the APIService and the webhook trust this CA
- **etcd** - Single-node etcd for quota storage (default backend)
- **postgres** - PostgreSQL for quota storage (alternative backend)
- **postgres-migrations** - Database schema migrations for Postgres
- **nats-streams** - NATS JetStream stream for quota events
- **observability** - ServiceMonitors (apiserver + controller-manager), PrometheusRule alerts, Grafana dashboard, and the `quota-metrics-reader` ClusterRole. Both metrics endpoints require a bearer token authorized for `get` on `/metrics`
- **enforcement-webhook** - ValidatingWebhookConfiguration for the enforcement admission plugin (failurePolicy Fail). Opt-in per namespace with the label `quota.miloapis.com/enforcement=enabled`, and the kube-apiserver must authenticate to it. Read [enforcement-webhook/README.md](enforcement-webhook/README.md) first
- **dev-postgres-credentials** - DEV ONLY: the `postgres-credentials` Secret with a fixed password, for the dev Postgres. Other deployments create that Secret themselves (keys `dsn` and `password`)
- **test-infra-access** - DEV/CI ONLY: RBAC for the test-infra kind cluster (metrics scraping from `telemetry-system`, controller-manager read access for the e2e policy kinds)
- **k6-performance-tests** - k6 operator TestRuns for the load tests in `test/load/`
- **perf-profile** - Normalizes apiserver log level so the `perf-*` overlays differ only in storage backend

## Usage

Include components in your overlay's `kustomization.yaml`:

```yaml
components:
  - ../../components/namespace
  - ../../components/cert-manager-ca
  - ../../components/etcd
```

## Controller-manager read access

The controller-manager watches the kinds that GrantCreationPolicies and ClaimCreationPolicies trigger on. `config/base` gives it no access to them: the `quota-controller-manager-policy-targets` ClusterRole is aggregated from every ClusterRole labeled `quota.miloapis.com/aggregate-to-controller-manager: "true"`. Ship one such ClusterRole with your policies, listing `get`, `list` and `watch` on the trigger kinds. `config/milo/rbac` is Milo's.
