# Migrating Milo to the Quota Service

A plan for moving Milo from its built-in quota system to this service.

> Status: plan only. No part of the Milo side has been changed, no cutover has been run, and no migration tooling exists. Last verified: 2026-11-26 against this repository and [datum-cloud/milo](https://github.com/datum-cloud/milo).

## What is and is not implemented

| Area | State |
|------|-------|
| Quota apiserver, controller-manager and APIService manifest | Implemented in this repo |
| Kinds renamed from Milo, same API group | Implemented |
| Policy kinds (GrantCreationPolicy, ClaimCreationPolicy) | Implemented |
| Enforcement admission plugin and validating webhook component | Implemented |
| Consumer resolution flags and hooks | Implemented |
| Milo IAM roles, ProtectedResources, ported definitions, claim policies, metrics policy (`config/milo`) | Implemented |
| etcd backend (Milo-equivalent), Postgres and dual backends | Implemented |
| Removing the CRDs, plugin and controllers from Milo | Not done |
| Export, convert and import of existing objects | Not done. No script exists |
| Milo consumer resolver and per-project client resolver | Not done |
| Importable multicluster `ProviderFactory` | Not done. It is in package `main` |
| Discovery-context registration for the new resources in Milo | Not done |
| Consumer mapping for Milo | Flag implemented (`--quota-enforcement-consumer-ref-from-user-extra`). Not yet set in a deployment manifest |
| PodDisruptionBudget and more than one apiserver replica | Implemented in `config/base` (two replicas; dev overlays pin 1) |
| Webhook scope and controller-manager read access for Milo's kinds | Implemented (`config/milo/enforcement-webhook`, `config/milo/rbac`) |
| Equivalent of Milo's `enforcement-edge-cases` e2e suite | Not done |
| Rehearsal of the cutover, including rollback | Not done |

## Why the order matters

Milo serves `quota.miloapis.com` through CRDs. This service serves the same group through an APIService. A group can have only one owner. The CRDs and the APIService cannot coexist, so the sequence is:

1. Remove Milo's CRDs and controllers.
2. Register the APIService.

Deleting a CRD deletes every object of that kind. Export first.

## Milo touchpoints

All paths are relative to the root of the [datum-cloud/milo](https://github.com/datum-cloud/milo) repository.

| What | Where |
|------|-------|
| Quota types installed into Milo's schemes | `cmd/milo/apiserver/config.go:375-376` (`quotaapi.Install`) |
| `ResourceQuotaEnforcement` plugin ordering | `cmd/milo/apiserver/admission.go:36` |
| Plugin registration | `cmd/milo/apiserver/server.go:229` (`admissionquota.Register`) |
| Readiness check `quota-validator` | `cmd/milo/apiserver/config.go:459` |
| Quota types in the controller-manager scheme | `cmd/milo/controller-manager/controllermanager.go:183` |
| Multicluster provider and quota controllers | `cmd/milo/controller-manager/controllermanager.go:566-651` |
| CRDs | `config/crd/bases/quota/` |
| Controller-manager RBAC for the old kinds | `config/controller-manager/overlays/core-control-plane/rbac/role.yaml` |
| Quota IAM, definitions, policies, metrics | `config/services/quota/` |
| Old e2e suites | `test/quota/` |

## What changes for existing objects

### Kinds and fields

The `apiVersion` stays `quota.miloapis.com/v1alpha1`. The kinds change:

| Milo | Standalone |
|------|-----------|
| `ResourceRegistration` | `QuotaDefinition` |
| `ResourceGrant` | `QuotaGrant` |
| `ResourceClaim` | `QuotaClaim` |
| `AllowanceBucket` | `QuotaBucket` |

Plural resource names change with them, for example `resourceclaims` to `quotaclaims`. The spec fields of the first three are the same shape. Policy kinds keep their names and template field names.

### Do not import buckets or claim status

- **Buckets** are derived. The controller-manager rebuilds them from grants and claims. Do not import them.
- **Claim status** is reset on create. Every claim starts at `Granted=False`, reason `PendingEvaluation`, and is decided again. Importing claims therefore re-runs allocation for each one.
- **Grants created by a GrantCreationPolicy** carry a policy label. Apply the policies first and let the controller recreate those grants, rather than importing them. Check the result against the export.

Re-evaluating claims has a risk. If the new grants add up to less than the existing claims, some claims are denied. A denied claim that an admission plugin created is deleted by the cleanup controller. The resource it covered then has no claim, and the consumer's usage is under-counted. Rehearse this with real data and compare totals before and after.

### Bucket placement

Milo placed buckets in `organization-<name>` for Organization consumers and in `milo-system` for everything else. The standalone service places a bucket in `consumerRef.namespace` when set, and otherwise in `--quota-bucket-namespace` (default `quota-system`).

Bucket names change too. Milo hashed the resource type, consumer kind and consumer name. The standalone name (`bucket-<sha256>`) also hashes the consumer's API group and namespace, and length-prefixes every part, so consumers that share a kind and name in different groups or namespaces get separate buckets. No standalone bucket has the name Milo gave it. Buckets are rebuilt, not imported, so this matters only to anything that looks buckets up by name.

- To keep non-organization buckets where Milo put them, set `--quota-bucket-namespace=milo-system` on both the apiserver and the controller-manager.
- Organization buckets move to that namespace as well. Anything that reads buckets by namespace, such as dashboards, queries and IAM scoping, needs updating. Decide this before cutover.

### Storage keys

Do not point the standalone service at Milo's etcd and expect it to adopt the data. Milo's CRD objects are stored under `/registry/quota.miloapis.com/<old plural>/...`. The standalone default prefix is also `/registry/quota.miloapis.com` (`--etcd-prefix`), with the new plural names after it. The prefixes overlap even though the keys do not collide, and the encodings differ. I derived the key layout from the code and did not check it against a running cluster. Use a separate etcd, as the shipped overlays do, and move data through the API. Postgres keys have the form `/quota.miloapis.com/<resource>/<namespace>/<name>`.

Start on the etcd backend. It behaves like Milo's current system. Moving to Postgres can come later, through dual mode. See [storage-backends.md](storage-backends.md).

### Stricter validation

Objects that Milo accepted can be rejected here:

| Rule | Milo | Standalone |
|------|------|-----------|
| `QuotaClaim.spec.consumerRef` | Optional. Admission filled it in | Required. `kind` and `name` must be non-empty |
| `QuotaClaim.spec.consumerRef`, `resourceRef`, `requests` | Mutable | Immutable after create |
| `QuotaGrant.spec.consumerRef` | Mutable | Immutable after create |
| `AllowanceBucket.spec` / `QuotaBucket.spec` | No immutability rule | `consumerRef` and `resourceType` immutable |
| `QuotaClaim.spec.resourceRef` | Optional | Optional in the strategy, required on create by admission validation. When set, `kind` and `name` are required |
| `requests[].amount` | Schema allowed 0, admission rejected it | Rejected at the strategy: must be greater than 0 |
| `requests` | At most 20 | At most 20, no duplicate `resourceType` |
| ClaimCreationPolicy template `resourceRef` | Rejected by a CEL rule | Rejected by the strategy |

Updates to objects that already violate a rule are not blocked when the update touches only metadata, such as finalizers or labels. The strategy compares the new object's errors with the old object's and reports only the new ones.

A `Feature`-type definition is supported, as before.

## IAM

Role names are kept so existing PolicyBindings that reference them still work: `quota.miloapis.com-admin`, `-manager`, `-viewer`, `-operator`, `-organization-quota-manager` and `-resource-registration-viewer`. The last keeps Milo's "resource-registration" name on purpose.

Everything else changes with the kinds:

- **Permission strings.** `quota.miloapis.com/resourceclaims.*` becomes `quotaclaims.*`, `resourcegrants.*` becomes `quotagrants.*`, `resourceregistrations.*` becomes `quotadefinitions.*`, and `allowancebuckets.*` becomes `quotabuckets.*`.
- **ProtectedResources** are named in the singular: `quota.miloapis.com-quotaclaim`, `-quotagrant`, `-quotadefinition`, `-quotabucket`, `-claimcreationpolicy` and `-grantcreationpolicy`. Delete the old `-resourceclaim`, `-resourcegrant`, `-resourceregistration` and `-allowancebucket` ProtectedResources after cutover.
- **PolicyBindings** with a `resourceSelector` on an old kind, such as the one that grants `system:authenticated` read access to registrations, must select the new kind. The shipped binding selects `QuotaDefinition`.
- **Controller-manager RBAC** in Milo still lists the old resources and can be trimmed.
- **The standalone controller-manager's read access** to the kinds Milo's policies trigger on (ConfigMap, Secret, Note, ClusterNote, Organization, Project) comes from `config/milo/rbac`. It aggregates into `quota-controller-manager-policy-targets` through the `quota.miloapis.com/aggregate-to-controller-manager` label. Add rules there when you add a policy for a new kind.

Apply the new IAM objects with `kubectl apply -k config/milo/standalone`. `config/milo` itself is a Component and cannot be applied directly.

## Registrations and claim policies

`config/milo` ports Milo's definitions (configmaps, notes, secrets) and its four claim policies (configmaps, notes, cluster notes, secrets) from `config/services/quota/`. It also ports the `ResourceMetricsPolicy`, which keeps the `milo_quota_*` metric names so dashboards and alerts built on them keep working.

The ported claim policies set no `consumerRef`. Milo's plugin supplied the Project or Organization from the request context. Milo also records the parent resource in user-info extras, and its own webhooks read them (`milo/pkg/webhook/cluster_aware_server.go:66-71`). Point the standalone plugin at those extras:

```
--quota-enforcement-consumer-ref-from-user-extra=iam.miloapis.com/parent-name,iam.miloapis.com/parent-type,iam.miloapis.com/parent-api-group
```

This reads the consumer's name, kind and API group from three separate extras, so one setting covers both Project and Organization requests. Confirm in a rehearsal that Milo sets all three extras on the requests the webhook sees, including requests made against project control planes. Without a consumer the plugin denies the create. Other options:

- A resolver installed with `WithConsumerResolver`.
- `--quota-enforcement-consumer-from-user-extra`, if the Project name reaches the plugin as a user-info extra.
- `--quota-enforcement-consumer-from-namespace-annotation`, if the namespace carries the Project name.

If the extras turn out to be missing on some requests, see [Open decisions](#open-decisions).

## Discovery contexts

Milo's CRDs carry the annotation `discovery.miloapis.com/parent-contexts`, which its discovery filter reads. Once the CRDs are gone the annotation goes too. A resource with no registration is visible in every context. Register the new resources in Milo's apiserver, either with `RegisterStatic` in `NewConfig` (`cmd/milo/apiserver/config.go`) or with a `DiscoveryContextPolicy`. A policy takes precedence over static registrations.

Use the contexts the CRDs had:

| Resource | Context |
|----------|---------|
| `quotadefinitions`, `claimcreationpolicies`, `grantcreationpolicies` | Platform |
| `quotagrants`, `quotaclaims`, `quotabuckets` | Organization, Project |

I did not check whether Milo's discovery filter is applied to groups served through an APIService. Confirm that in a test cluster.

## Project control planes

In Milo, claims, grants and buckets live in project control planes, and the plugin builds a client for the request's project. The standalone service has no knowledge of Milo's projects, so Milo supplies two things:

- A **client resolver** (`WithClientResolver`), so the plugin creates and watches claims in the project's control plane. This is an exported option in `pkg/admission/enforcement`.
- A **multicluster provider** for the controller-manager, so controllers reconcile claims and grants in each project. Milo's controller-manager builds one with `miloprovider.New` today. The hook for it is `ProviderFactory` and `WithProviderFactory`. They are in package `main` and cannot be imported yet. Until that changes, a Milo-specific build of the controller-manager has no supported way to install it.

Without these, the standalone service reconciles only the cluster it runs in.

If Milo embeds the plugin with its own policy and template engines instead of the webhook, those engines must implement the interfaces in `pkg/admission/enforcement/interfaces.go`, which changed from Milo's:

- `PolicyEngine.GetPoliciesForKind(schema.GroupKind)` returns every ready, enabled policy for the kind, matched by group and kind only. Several policies per kind are allowed, and the plugin requires all of their claims. It must return an error, never an empty list, before the policies are loaded.
- `PolicyEngine.HasSynced()` reports that the policies are loaded. The plugin rejects matched creates until it is true.
- `TemplateEngine.EvaluateConditions(ctx, conditions, obj)` takes a context and must stop when it is done. Any error denies the request.

## Webhook availability

The enforcement webhook uses `failurePolicy: Fail`. This matches Milo's in-process enforcement, where an unavailable plugin also blocked creates. If the quota apiserver is down, creates of every matched resource fail. Before cutover:

- Keep at least two apiserver replicas. `config/base` runs two with a PodDisruptionBudget.
- Scope the webhook with `config/milo/enforcement-webhook`. It matches only the kinds Milo's claim policies target, in every non-system namespace, instead of the default namespace opt-in label (`quota.miloapis.com/enforcement=enabled`), which tenants creating their own namespaces could leave off. An outage then blocks only those creates. Keep its rules in step with `config/milo/claim-policies`.
- Give Milo's apiserver an admission kubeconfig that authenticates to the webhook as `quota-enforcement-webhook` (see `config/components/enforcement-webhook/README.md`). Without it every call is rejected and, with `Fail`, so is every matched create.
- Require Kubernetes 1.30 or later, for the webhook's `matchConditions`.
- Keep `--quota-enforcement-claim-timeout` below the webhook's `timeoutSeconds` (25s against 30s by default).

## Cutover sequence

Do these in a maintenance window. Nothing here has been rehearsed.

**Before the window**

1. Deploy the standalone service to a staging cluster with a copy of production data. Practice the export, conversion and import. Compare bucket totals before and after.
2. Decide the open items below.
3. Build the Milo change: remove the scheme install, plugin, registration, readiness check, controllers and CRD references listed above. Add the discovery registration. Add the client resolver, consumer resolver and provider if you run the plugin in Milo's process.
4. Deploy the standalone Deployments in production without registering the APIService and without the webhook. Verify the pods are healthy.
5. Configure Milo's apiserver with the admission configuration and kubeconfig that authenticate it to the webhook as `quota-enforcement-webhook`. This needs a restart, so do it before the window.
6. Take an etcd snapshot of Milo's control plane.

**In the window**

7. Export every object of the six old kinds, with `kubectl get <resource> -A -o yaml`, for `resourceregistrations`, `resourcegrants`, `resourceclaims`, `allowancebuckets`, `claimcreationpolicies` and `grantcreationpolicies` in `quota.miloapis.com`. Do this in the core control plane and in every project control plane.
8. Roll out the Milo build without the quota plugin and controllers. From here until step 13, Milo enforces no quota.
9. Delete the six CRDs. This deletes their objects. If a delete stalls on finalizers, handle that deliberately, and keep the export.
10. Register the APIService (`config/components/api-registration`). Wait for `kubectl get apiservice v1alpha1.quota.miloapis.com` to show it available.
11. Apply `kubectl apply -k config/milo/standalone`. This installs the definitions, claim policies, IAM objects, metrics policy and the controller-manager's read access to Milo's trigger kinds (`config/milo/rbac`).
12. Convert the export and import it in this order: definitions, grants, then claims. Rename the kinds and plurals. Strip `uid`, `resourceVersion`, `creationTimestamp`, `managedFields`, `ownerReferences` and `status`. The ownership controller sets owner references. Skip buckets and policy-created grants. Fix any object that fails the stricter validation.
13. Apply the enforcement webhook (`config/components/enforcement-webhook` with `config/milo/enforcement-webhook`) last. It rejects creates if the claim path is not working, so it goes on only after the steps above succeed.

**After the window**

14. Verify. List definitions, grants, buckets and claims. Compare each bucket's `limit` and `allocated` with the pre-cutover values. Create a resource in a project with quota to confirm admission grants it, and one past quota to confirm it is denied.
15. Delete the old ProtectedResources and update the PolicyBindings.

## Rollback

Rollback is possible until step 9 without loss. After that, the export is the only record of the old objects.

1. Delete the enforcement webhook, then the APIService. Creates stop being quota-checked.
2. Reinstall Milo's CRDs from `config/crd/bases/quota/`, deploy the previous Milo build with its plugin and controllers.
3. Reapply the exported objects. Use the original export, with the old kinds. Strip server-set fields as in step 12. Buckets are rebuilt by Milo's controllers.
4. Objects created in the standalone service after cutover are not in the export. Export them from the standalone service first, rename the kinds back, and apply them, or accept that those claims and grants must be recreated.
5. If the CRD deletion or import went wrong, restore from the etcd snapshot instead.

The enforcement gap runs from step 8 to step 13 on the way forward, and again between steps 1 and 2 of a rollback.

## Open decisions

- **How the project reaches the webhook.** Milo's plugin read the project from the request context. A webhook call carries only the AdmissionReview, so the plan relies on Milo's `iam.miloapis.com/parent-*` user-info extras (see [Registrations and claim policies](#registrations-and-claim-policies)). Milo's own webhooks depend on them, but nobody has checked that they are present on every request quota enforcement sees. The alternative is to embed the plugin in Milo's apiserver with `NewQuotaEnforcementPlugin` and the resolver options, and skip the webhook.
- **Bucket namespaces** for Organization consumers, and who reads them.
- **Whether to import claims at all.** Re-creating claims re-runs allocation. The alternative is to let policies and resource ownership regenerate them, which may be simpler but needs verifying against real data.
- **The cluster-scoped claim namespace.** Milo's default was `milo-system`. This service defaults to `quota-system`. Set `--quota-cluster-scoped-claim-namespace` if existing policies or queries depend on the old value.
