# Integration Guide

How to deploy the quota service and integrate it into a control plane.

> Last verified: 2026-11-26 against `config/`, `cmd/quota/` and `pkg/admission/enforcement/`.

## Overview

The quota service is an aggregated API server (`v1alpha1.quota.miloapis.com`) plus a controller-manager. It keeps a ledger of definitions, grants, buckets and claims, and it can enforce quota at admission time for resources you choose. Consumer references are opaque `{apiGroup, kind, name}` tuples, so no consumer types are imported.

The API group is `quota.miloapis.com`, the same as Milo's built-in quota API. A cluster cannot serve both. See [milo-migration.md](milo-migration.md) if you are moving Milo over.

## Deployment

### Prerequisites

- Kubernetes 1.30 or later. The enforcement webhook uses `matchConditions`, which need 1.30. Without the webhook, the service itself needs only aggregated API support. I did not test older versions.
- cert-manager with the CSI driver. The Deployments mount serving certificates through it.
- FluxCD, for the `HelmRelease` objects that install etcd, Postgres and NATS.
- Prometheus Operator CRDs (`ServiceMonitor`, `PrometheusRule`) and the Grafana Operator, if you include the `observability` component. The dev overlays do.
- The NATS and JetStream CRDs, if you include `nats-streams`. The dev overlays do. Install them with `kubectl apply -k config/dependencies/nats`.

### Local development

`task dev:setup` creates a kind cluster, installs the dependencies, builds and loads the image, and deploys the `dev` overlay. It uses the remote `test-infra` Taskfile, so set `TASK_X_REMOTE_TASKFILES=1`.

### Overlays

| Overlay | Backend | Notes |
|---------|---------|-------|
| `config/overlays/dev` | etcd | Includes the observability and enforcement-webhook components |
| `config/overlays/dev-postgres` | postgres | Adds Postgres and the migration Job. The controller-manager is patched to `STORAGE_BACKEND=postgres` |
| `config/overlays/dev-dual` | dual | etcd primary and Postgres shadow. The controller-manager is patched to `dual` |
| `config/overlays/test-infra` | etcd | Used by the CI e2e job. Does not include the enforcement-webhook component |

```bash
kubectl apply -k config/overlays/dev
kubectl get apiservice v1alpha1.quota.miloapis.com
```

The base Deployments set the backend through a `STORAGE_BACKEND` environment variable that feeds `--quota-storage-backend`. The apiserver and controller-manager each have one, and the overlays patch both. They must agree. See [storage-backends.md](storage-backends.md#allocation-modes).

`config/components/README.md` lists the available components. The enforcement-webhook component is covered below.

## Consuming the API

Field details are in the [API reference](api.md). Amounts are integers in the definition's base unit.

### Step 1: define a quota type

```yaml
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaDefinition
metadata:
  name: compute-vcpu
spec:
  resourceType: "compute.example.com/vCPU"
  type: Allocation
  consumerType:
    apiGroup: your.api.group
    kind: Project
  baseUnit: millicpu
  displayUnit: vCPU
  unitConversionFactor: 1000
  claimingResources:
    - apiGroup: compute.example.com
      kind: VirtualMachine
```

### Step 2: grant quota

```yaml
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaGrant
metadata:
  name: project-compute
  namespace: default
spec:
  consumerRef:
    apiGroup: your.api.group
    kind: Project
    name: my-project
  allowances:
    - resourceType: "compute.example.com/vCPU"
      buckets:
        - amount: 100000
```

The controller-manager creates a `QuotaBucket` for the consumer and resource type and marks the grant `Active`.

### Step 3: claim quota

```yaml
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaClaim
metadata:
  name: my-vm
  namespace: default
spec:
  consumerRef:
    apiGroup: your.api.group
    kind: Project
    name: my-project
  requests:
    - resourceType: "compute.example.com/vCPU"
      amount: 10000
  resourceRef:
    apiGroup: compute.example.com
    kind: VirtualMachine
    name: my-vm
    namespace: default
```

The claim's `consumerRef` must match the grant's, because the bucket is keyed on resource type, consumer kind and consumer name.

How you learn the result depends on the backend:

```bash
kubectl get quotaclaim my-vm -n default
kubectl get quotaclaim my-vm -n default -o jsonpath='{.status.conditions[?(@.type=="Granted")]}'
```

- **postgres:** the create response already holds the decision. A claim without quota is created with `Granted=False`, reason `QuotaExceeded`. A claim whose resource type has no bucket is rejected with 403.
- **etcd and dual:** the claim is created with `Granted=False`, reason `PendingEvaluation`. Read or watch it until the reason changes.

Nothing returns a 409 for lack of quota.

## Policy integration

Policies make the service decide when quota applies. You do not have to create claims from your own code.

- A **ClaimCreationPolicy** names a resource kind. Creating that kind produces a claim, and admission blocks the create until the claim is decided.
- A **GrantCreationPolicy** names a resource kind that should produce a grant, for example giving every new project a default allowance.

You can still create claims directly. Direct claims are not touched by the cleanup controllers, which act only on claims that the admission plugin created.

How policies are matched and evaluated:

- **Matching is by API group and kind.** The version in `trigger.resource.apiVersion` is ignored, so a policy for `example.com/v1` also covers creates served as `example.com/v1beta1`.
- **Several policies can target one kind.** Every enabled policy whose constraints match produces its own claim, and all of them must be granted. If one fails, the claims already granted for that request are deleted and the create is denied.
- **CEL errors fail closed.** A constraint that fails to evaluate, for example one that reads a field the object does not set, denies the create instead of skipping the policy. The requester controls the object, so an error must not be a way around quota. Guard optional fields with `has()`, as in `has(object.spec.size) && object.spec.size > 10`. For GrantCreationPolicies an evaluation error leaves grants unchanged and the trigger is retried.
- **The controller-manager also creates claims for objects that already exist.** It watches each policy's trigger kind and creates a claim for a matching object that has none, for example one created before the policy. It first looks for a claim from the same policy whose `resourceRef` is the object, so an object admission already charged is not charged again.

Labels and annotations the service sets:

| Key | On | Meaning |
|-----|----|---------|
| `quota.miloapis.com/policy` | claims, grants | The ClaimCreationPolicy or GrantCreationPolicy that created it |
| `quota.miloapis.com/auto-created` | claims | `true` for claims the admission plugin created. The cleanup controllers act only on these |
| `quota.miloapis.com/resource-uid` | claims | UID of the object whose create produced the claim. The ownership controller matches the claim to that object by UID, so a later object reusing the name does not inherit it |
| `quota.miloapis.com/trigger-uid` | grants | UID of the trigger object. A grant whose trigger UID differs belongs to an earlier object with the same name and is left alone |

### Controller-manager read access

The controller-manager has no wildcard read access. It watches trigger kinds through the `quota-controller-manager-policy-targets` ClusterRole in `config/base`, which aggregates every ClusterRole labeled `quota.miloapis.com/aggregate-to-controller-manager: "true"`. Ship one such ClusterRole with `get`, `list` and `watch` on the kinds your policies trigger on. `config/milo/rbac` is Milo's. Aggregation needs the clusterrole-aggregation controller; where it does not run, bind your ClusterRole to the `quota-system/quota-controller-manager` ServiceAccount directly.

Example policy that charges one note per `Note` created:

```yaml
apiVersion: quota.miloapis.com/v1alpha1
kind: ClaimCreationPolicy
metadata:
  name: project-note-claim-policy
spec:
  trigger:
    resource:
      apiVersion: notes.miloapis.com/v1alpha1
      kind: Note
  target:
    resourceClaimTemplate:
      metadata:
        name: "note-{{ trigger.metadata.name }}"
        namespace: "{{requestInfo.namespace}}"
      spec:
        requests:
          - resourceType: notes.miloapis.com/notes
            amount: 1
```

The template must not set `spec.resourceRef`. Admission fills it in. This policy sets no `consumerRef`, so the plugin must resolve the consumer, as described next.

## Enforcement admission

### Consumer resolution

A claim needs a consumer, because an empty consumer would pool unrelated claims into one bucket. When a ClaimCreationPolicy template sets no `consumerRef`, the plugin tries these sources in order and uses the first that yields a value:

1. The policy template's `consumerRef`, if `kind` and `name` are both set.
2. A resolver hook installed with `WithConsumerResolver`.
3. User-info extras. `--quota-enforcement-consumer-ref-from-user-extra` reads the name, kind and API group from separate extras and is tried first. `--quota-enforcement-consumer-from-user-extra` reads only the name, with a fixed kind. The first non-empty value of the named extra is used.
4. A namespace annotation, from `--quota-enforcement-consumer-from-namespace-annotation`. The annotation on the triggering resource's namespace becomes the consumer name. This is not used for cluster-scoped resources.

If none resolves a consumer, the request is denied with a message naming the policy. The plugin never creates a claim with an empty consumer.

The flag format for both mappings is `<key>=<Kind>[.<apiGroup>]`. For example, this reads the project name from a user extra and makes the claim's consumer a `Project` in `resourcemanager.miloapis.com`:

```
--quota-enforcement-consumer-from-user-extra=example.com/project-name=Project.resourcemanager.miloapis.com
```

Both flags are repeatable. A malformed value is a startup error. The namespace-annotation mapping needs the apiserver to have a core API kubeconfig (`--kubeconfig`), because it reads namespaces from that API.

The shipped manifests configure no mapping. The claim policies in `config/milo/claim-policies` set no `consumerRef` either. Until you add a mapping or a resolver, creates covered by those policies are denied.

### Waiting and retries

- The plugin waits up to `--quota-enforcement-claim-timeout` (default 25s) for the decision. The webhook's `timeoutSeconds` is 30, so the plugin answers with a precise timeout before the kube-apiserver gives up.
- When the policy template pins `metadata.name`, a client retry meets the claim from the earlier attempt. A granted claim lets the retry through at once. A denied claim fails the retry as a conflict until the cleanup controller removes it. A pending claim is deleted and recreated.
- Denied auto-created claims are removed by the controller-manager immediately. Pending ones are removed after `--quota-stale-pending-claim-age` (default 5m). Neither applies to claims you created by hand.
- Templates that set no name get `generateName: <policy>-claim-`, so each attempt creates a new claim.
- Claim names must be DNS subdomains. A rendered name that is not one (uppercase, `:`, over 253 characters) is lowercased, invalid characters become `-`, and a hash of the original is appended so distinct names stay distinct.
- The apiserver is not ready (`/readyz` check `quota-enforcement-policies`) until every ClaimCreationPolicy is loaded. Before that, the plugin rejects matched creates rather than admit them unchecked.
- Failures reach the caller as 403 Forbidden with a message that names the cause: quota reached, timeout, a previous attempt still being cleaned up, a misconfigured policy, or an internal error.

### Cluster-scoped resources

QuotaClaims are namespaced, so claims for cluster-scoped resources need a home. The plugin uses the policy template's `metadata.namespace` if set, and otherwise `--quota-cluster-scoped-claim-namespace` (default `quota-system`). Milo's plugin used `milo-system`.

### Extension options

These `Option` values for `NewQuotaEnforcementPlugin` are exported from `go.miloapis.com/quota/pkg/admission/enforcement`:

| Option | Use |
|--------|-----|
| `WithClientResolver` | Choose the control plane a request's claim lives in. Needed when each tenant or project has its own control plane |
| `WithWatchManagerFactory` | Build per-scope watch managers yourself, for example to reuse an informer |
| `WithConsumerResolver` | Derive the consumer from the request |
| `WithNamespaceLookup` | Supply namespace annotations for the namespace-annotation mapping |
| `WithConfig`, `WithLogger` | Configuration and logging |

For the controller-manager, `ProviderFactory` and `WithProviderFactory` plug in a multicluster provider, so controllers reconcile claims and grants in your tenant control planes. They are defined in package `main` of `cmd/quota`, so another module cannot import them yet.

### Webhook

The `config/components/enforcement-webhook` component installs a `ValidatingWebhookConfiguration` named `quota-enforcement` with two webhooks, both calling `/webhook/enforce` on the `quota-apiserver` service. Its [README](../config/components/enforcement-webhook/README.md) has the full setup.

| Webhook | Matches by default |
|---------|--------------------|
| `enforce.quota.miloapis.com` | `CREATE` of any namespaced kind, only in namespaces labeled `quota.miloapis.com/enforcement=enabled`. `kube-system`, `kube-public`, `kube-node-lease` and `quota-system` are excluded even when labeled. `matchConditions` drop infrastructure traffic |
| `enforce-cluster.quota.miloapis.com` | Nothing. `rules` is empty |

**Scope.** Enforcement is opt-in per namespace, so a quota outage cannot block unlabeled namespaces such as cert-manager or monitoring. Anyone who can create or relabel a namespace can turn enforcement off for it. If tenants create their own namespaces, set the label for them or replace the selector with an explicit list of kinds. A `namespaceSelector` cannot scope cluster-scoped objects, so list the cluster-scoped kinds your policies target with a patch on `/webhooks/1/rules`. `config/milo/enforcement-webhook` does both for Milo: it matches exactly the kinds Milo's claim policies target, in every non-system namespace. List it after the enforcement-webhook component.

**Caller authentication.** `/webhook/enforce` creates claims with the apiserver's own identity, so it sits behind delegated authentication and authorization like any other path. The caller must be authenticated and allowed `post` on the non-resource URL `/webhook/enforce`; the component's `rbac.yaml` grants that to the user `quota-enforcement-webhook` only. The kube-apiserver sends no credentials to webhooks by default. Give it an admission configuration (`--admission-control-config-file`) whose kubeconfig presents a client certificate with common name `quota-enforcement-webhook`, signed by the cluster client CA. The README has the files. The apiserver refuses to start if `--authorization-always-allow-paths` covers `/webhook/enforce`. Managed control planes that do not let you set the admission configuration cannot use the webhook; embed the plugin in your own apiserver instead.

Other settings:

- **Validating, not mutating.** The plugin creates a claim as a side effect, and validating webhooks run after every mutating one. A later validating webhook can still reject the request. The controller-manager removes the orphaned claim.
- **`failurePolicy: Fail`.** If the quota apiserver cannot be reached, creates of matched resources are rejected. This matches Milo's in-process behavior.
- **`timeoutSeconds: 30`** against a 25 second claim wait.
- **`sideEffects: NoneOnDryRun`.** Dry-run requests are not enforced.

**Availability.** With `Fail`, an outage of the quota apiserver blocks creation of every matched resource. `config/base` runs two apiserver replicas with a PodDisruptionBudget. Keep at least two. Change to `Ignore` only if admitting unchecked resources during an outage is acceptable.

## IAM integration (Milo)

For Milo-based control planes, `config/milo` holds Milo's integration. It is a Kustomize Component, so it cannot be applied directly. Apply the wrapper:

```bash
kubectl apply -k config/milo/standalone
```

It contains:

- **ProtectedResources** for each kind, named in the singular, for example `quota.miloapis.com-quotaclaim`.
- **Roles** with Milo-compatible names: `quota.miloapis.com-admin`, `-manager`, `-viewer`, `-operator`, `-organization-quota-manager` and `-resource-registration-viewer`. Permission strings follow the kind names, so `resourceclaims.*` became `quotaclaims.*`.
- **A PolicyBinding** that gives `system:authenticated` read access to QuotaDefinitions.
- **QuotaDefinitions** for Milo's quota-enforced types, and **ClaimCreationPolicies** for them.
- **A ResourceMetricsPolicy** that keeps the `milo_quota_*` metric names.

The wrapper patches the PolicyBinding into `milo-system`. Roles carry `namespace: milo-system` themselves. These objects need Milo's IAM and resource-metrics CRDs installed. To compose the Component into your own overlay, list `config/milo` under `components:`.

## Monitoring

The `config/components/observability` component adds:

- ServiceMonitors for the apiserver and the controller-manager.
- A PrometheusRule with alerts for claim denial rate and latency, apiserver errors and availability, controller reconcile errors, enforcement watch health, and Postgres shadow failures and divergence (the `quota_dual_*` metrics).
- A Grafana dashboard, "Quota Performance".

Both components serve metrics over HTTPS on port 8443 and require a bearer token: the apiserver on its serving port, the controller-manager on `--metrics-addr` (default `:8443`). The scraper's ServiceAccount needs the `quota-metrics-reader` ClusterRole, which the observability component installs. Bind it yourself; `metrics-reader.yaml` has an example.

Metrics you can query:

- `quota_claim_total` and `quota_claim_duration_seconds`, labeled by `result`: `granted`, `denied`, `partial`, or `error` for allocations that failed on a storage or status write.
- `quota_enforcement_result_total`, admission outcomes by policy and resource.
- `quota_enforcement_*` watch and waiter metrics.
- `quota_dual_*`, in dual mode. See [storage-backends.md](storage-backends.md#dual).
- Standard apiserver and controller-runtime metrics.

The `milo_quota_*` bucket and policy metrics come from the Milo ResourceMetricsPolicy, not from the service.

## Testing

```bash
task test        # go test ./pkg/... ./internal/... ./cmd/...
task test:postgres  # also the Postgres tests; needs POSTGRES_DSN
task lint        # golangci-lint
task e2e         # chainsaw test test/e2e/ --config test/e2e/.chainsaw.yaml
./hack/verify-codegen.sh
```

CI (`.github/workflows/testing.yaml`) runs `go test -p 1 ./...` against a Postgres 16 service, runs golangci-lint, verifies generated code, the migrations ConfigMap and the Grafana dashboards, and runs the Chainsaw suites on a kind cluster with the `test-infra` overlay. The Postgres and e2e jobs have not completed a run in CI yet.

Load tests are in `test/load` and use k6. See `test/load/README.md`.
