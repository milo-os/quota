# Quota Service API Reference

API group: `quota.miloapis.com`
Version: `v1alpha1`

The group matches Milo's, so existing manifests keep their `apiVersion`. The kinds are renamed from Milo:

| Milo kind | Quota service kind | Scope |
|-----------|--------------------|-------|
| `ResourceRegistration` | `QuotaDefinition` | Cluster |
| `ResourceGrant` | `QuotaGrant` | Namespaced |
| `ResourceClaim` | `QuotaClaim` | Namespaced |
| `AllowanceBucket` | `QuotaBucket` | Namespaced |
| `GrantCreationPolicy` | `GrantCreationPolicy` | Cluster |
| `ClaimCreationPolicy` | `ClaimCreationPolicy` | Cluster |

> Last verified: 2026-11-22 against `pkg/apis/quota/v1alpha1/types.go` and the strategies in `internal/registry/quota/`.

## How the kinds fit together

- A **QuotaDefinition** declares a quota-controlled resource type (for example `notes.miloapis.com/notes`) and which kind of consumer holds quota for it.
- A **QuotaGrant** gives a consumer an allowance of one or more resource types.
- A **QuotaBucket** is the running total for one consumer and one resource type. The controller-manager creates and maintains buckets from grants. You do not create them by hand.
- A **QuotaClaim** asks to use some of that allowance. Claims are created either by a client or automatically by the enforcement admission plugin.
- A **ClaimCreationPolicy** says which resource creations produce claims. A **GrantCreationPolicy** says which resources produce grants. See [Policies](#policies).

Amounts are integers in the definition's `baseUnit`. `unitConversionFactor` converts base units to display units.

## Consumer references

A `consumerRef` identifies who holds quota. The service treats it as opaque strings and imports no consumer types.

| Field | Required | Description |
|-------|----------|-------------|
| `apiGroup` | No | API group of the consumer kind |
| `kind` | Yes | Consumer kind, for example `Project` |
| `name` | Yes | Consumer name |
| `namespace` | No | Namespace of the consumer. When set, the bucket is placed in this namespace |

A bucket is identified by resource type, consumer `kind` and consumer `name`. `apiGroup` is not part of the bucket name.

## Resources

### QuotaDefinition

Declares a quota-controlled resource type. Cluster-scoped.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `spec.consumerType.apiGroup` | string | Yes | API group of the kind that receives grants |
| `spec.consumerType.kind` | string | Yes | Kind that receives grants and creates claims |
| `spec.type` | string | Yes | `Entity`, `Allocation` or `Feature`. Immutable |
| `spec.resourceType` | string | Yes | Resource type to track, 1 to 253 characters. Immutable |
| `spec.description` | string | No | Up to 500 characters |
| `spec.baseUnit` | string | Yes | Internal unit for calculations, up to 50 characters |
| `spec.displayUnit` | string | Yes | Unit shown to users, up to 50 characters |
| `spec.unitConversionFactor` | int64 | Yes | Base units per display unit. Minimum 1 |
| `spec.claimingResources[]` | list | Yes | Kinds allowed to create claims for this type. 1 to 20 items, no duplicates |
| `spec.claimingResources[].apiGroup` | string | No | API group. Empty means the core group |
| `spec.claimingResources[].kind` | string | Yes | Kind, 1 to 63 characters |
| `status.conditions` | []Condition | | `Active`. Reasons: `RegistrationActive`, `ValidationFailed`, `RegistrationPending` |

`spec.type` values:

- `Entity` counts discrete resource instances. Each instance uses one unit.
- `Allocation` measures numeric capacity, such as millicores or bytes.
- `Feature` is an on/off entitlement. Grants convey availability rather than capacity, and no claim machinery is involved.

`spec.type`, `spec.resourceType` and `spec.consumerType` cannot be changed after creation.

```yaml
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
  description: "Virtual CPU allocation quota"
  baseUnit: millicpu
  displayUnit: vCPU
  unitConversionFactor: 1000
  claimingResources:
    - apiGroup: compute.datumapis.com
      kind: VirtualMachine
```

### QuotaGrant

Gives a consumer an allowance. Namespaced.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `spec.consumerRef` | ConsumerRef | Yes | Who receives the allowance. Immutable |
| `spec.allowances[]` | list | Yes | At least one |
| `spec.allowances[].resourceType` | string | Yes | Resource type covered |
| `spec.allowances[].buckets[].amount` | int64 | Yes | Capacity in base units. Minimum 0. At least one bucket per allowance |
| `status.conditions` | []Condition | | `Active`. Reasons: `GrantActive`, `ValidationFailed`, `GrantPending` |

Here "bucket" inside an allowance is one slice of capacity. It is not a `QuotaBucket`. The controller sums the amounts of every active grant for a consumer and resource type into that consumer's `QuotaBucket` limit.

To give a consumer a different allowance, change `spec.allowances`. To give the quota to a different consumer, delete the grant and create a new one.

```yaml
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
```

### QuotaClaim

Requests part of a consumer's allowance. Namespaced.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `spec.consumerRef` | ConsumerRef | Yes | Who is charged. `kind` and `name` must be non-empty. Immutable |
| `spec.requests[]` | list | Yes | 1 to 20 requests, one per resource type. Immutable |
| `spec.requests[].resourceType` | string | Yes | Must match a registered QuotaDefinition |
| `spec.requests[].amount` | int64 | Yes | Must be greater than 0 |
| `spec.resourceRef` | object | See below | The resource this claim is for: `apiGroup`, `kind`, `name`, `namespace`. Immutable |
| `status.conditions` | []Condition | | `Granted`. Reasons: `QuotaAvailable`, `QuotaExceeded`, `ValidationFailed`, `PendingEvaluation` |
| `status.allocations[]` | list | | One entry per request: `resourceType`, `status` (`Granted`, `Denied`, `Pending`), `reason`, `message`, `allocatedAmount`, `allocatingBucket`, `lastTransitionTime` |

**`resourceRef` is optional in the storage strategy but required on create.** The strategy accepts an empty `resourceRef`. When any field is set, `kind` and `name` are both required. Admission validation (`QuotaValidation`) then requires `resourceRef.kind` and `resourceRef.name` on every create and checks that the resource's kind is listed in the definition's `claimingResources`. In practice, set `resourceRef` on every claim.

**New claims start at `Granted=False`, reason `PendingEvaluation`.** The decision replaces that condition. On the Postgres backend the decision is part of the create transaction, so the first stored version already carries its final status. On etcd and dual a controller writes the decision after the create returns. See [Allocation by backend](#allocation-by-backend).

The `Granted` condition is `True` when every request is granted. It is `False` with reason `QuotaExceeded` if any request is denied.

`consumerRef`, `resourceRef` and `requests` cannot be changed after creation. Allocations are computed once, so changing amounts would put bucket totals out of step. Delete and recreate the claim instead.

```yaml
apiVersion: quota.miloapis.com/v1alpha1
kind: QuotaClaim
metadata:
  name: project-compute-vcpu
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

### QuotaBucket

The running total for one consumer and one resource type. Namespaced and system-managed.

| Field | Type | Description |
|-------|------|-------------|
| `spec.consumerRef` | ConsumerRef | Consumer. Immutable |
| `spec.resourceType` | string | Resource type. Immutable |
| `status.limit` | int64 | Sum of all contributing grants |
| `status.allocated` | int64 | Amount used by granted claims |
| `status.available` | int64 | `limit` minus `allocated` |
| `status.claimCount` | int32 | Number of claims drawing on the bucket |
| `status.grantCount` | int32 | Number of grants contributing |
| `status.contributingGrantRefs[]` | list | `name`, `lastObservedGeneration`, `amount` for each grant |
| `status.lastReconciliation` | time | Last controller update |

The bucket name is `bucket-` followed by the SHA-256 of `resourceType + consumer kind + consumer name`. The namespace is `consumerRef.namespace` when set. Otherwise it is the value of `--quota-bucket-namespace`, which defaults to `quota-system`. The apiserver and the controller-manager must use the same value.

## Policies

Both policy kinds are part of the service. They configure when quota applies.

### ClaimCreationPolicy

Cluster-scoped. Says which resource creations are quota-checked and how to build the claim.

| Field | Description |
|-------|-------------|
| `spec.trigger.resource.apiVersion`, `.kind` | The resource whose creation triggers a claim |
| `spec.trigger.constraints[]` | CEL `expression` and optional `message`. The policy applies only when every constraint is true |
| `spec.target.resourceClaimTemplate.metadata` | `name`, `generateName`, `namespace`, `labels`, `annotations`. String fields accept `{{ }}` expressions |
| `spec.target.resourceClaimTemplate.spec` | A QuotaClaim spec: `requests`, and optionally `consumerRef` |
| `spec.disabled` | Skip the policy when true |
| `status.conditions` | `Ready`. Reasons: `PolicyReady`, `ValidationFailed`, `PolicyDisabled` |

The template must not set `spec.resourceRef`. Admission fills it in from the triggering resource, and a template that sets it is rejected.

If the template sets no `consumerRef`, the enforcement plugin resolves one. See [Consumer resolution](integration-guide.md#consumer-resolution).

### GrantCreationPolicy

Cluster-scoped. Creates QuotaGrants when a watched resource meets conditions.

| Field | Description |
|-------|-------------|
| `spec.trigger.resource.apiVersion`, `.kind` | The resource to watch |
| `spec.trigger.constraints[]` | CEL conditions, as above |
| `spec.target.parentContext` | Optional. `apiGroup`, `kind` and `nameExpression` pick the parent control plane in which to create the grant |
| `spec.target.resourceGrantTemplate` | `metadata` and a QuotaGrant `spec`. String fields accept `{{ }}` expressions |
| `spec.disabled` | Skip the policy when true |
| `status.conditions` | `Ready`, `ParentContextReady` |

## Allocation by backend

| Backend | Who decides a claim | When the decision is visible |
|---------|---------------------|------------------------------|
| `postgres` | The Postgres allocator, inside the create request | In the create response |
| `etcd` (default) | The controller-manager (bucket allocation pump and claim aggregator) | After the create returns |
| `dual` | The controller-manager, writing to etcd | After the create returns |

Only the Postgres backend is synchronous and transactional. A claim denied for lack of quota is still created, with `Granted=False` and reason `QuotaExceeded`. It is not a 409.

For resources protected by a ClaimCreationPolicy, admission creates the claim and waits for the decision before it admits the resource. The wait is 25 seconds by default. The caller sees success or a 403, never a pending claim. See [Enforcement admission](integration-guide.md#enforcement-admission).

## Errors

| Code | When |
|------|------|
| 403 Forbidden | Postgres backend: a claim names a resource type for which no `QuotaBucket` exists, meaning no active grant covers the request |
| 403 Forbidden | Enforcement admission: the claim was denied, timed out, or the policy cannot resolve a consumer. The message says which |
| 409 Conflict | Standard optimistic-concurrency conflicts on update |
| 409 AlreadyExists | A claim with the same name already exists in the namespace |
| 422 Invalid | Validation failures, such as `amount` of 0 or less, more than 20 requests, a duplicate `resourceType`, an empty `consumerRef.kind` or `name`, or a change to an immutable field |

Delete of a claim on the Postgres backend releases the claim's capacity in the same transaction. Release is idempotent: only the transaction that removes the claim row decrements buckets. The delete response is a `Status` object.

## Printer columns

`kubectl get` columns match Milo's. Columns marked wide appear with `-o wide`.

| Kind | Columns |
|------|---------|
| QuotaDefinition | Type, Active, Age |
| QuotaGrant | Active, Age. Wide: Consumer Group, Consumer Type, Consumer |
| QuotaClaim | Granted, Age. Wide: Resource (first request's resource type) |
| QuotaBucket | Resource Type, Limit, Allocated, Available, Claims, Age |
| ClaimCreationPolicy | Target, Disabled, Ready, Age |
| GrantCreationPolicy | Trigger, Disabled, Ready, Age |

## Verbs

All kinds support `create`, `get`, `list`, `update`, `patch`, `delete` and `watch`, plus a `status` subresource.
