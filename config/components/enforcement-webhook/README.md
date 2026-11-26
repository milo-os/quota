# Enforcement webhook

This component installs the `quota-enforcement` ValidatingWebhookConfiguration. It sends `CREATE` requests to `/webhook/enforce` on the quota apiserver, which runs the quota enforcement plugin and creates a QuotaClaim for each matching ClaimCreationPolicy. It also installs the RBAC that decides who may call the webhook.

The webhook uses `failurePolicy: Fail`. When the quota apiserver is down, matched creates are rejected. That is why enforcement is opt-in and the kube-apiserver must authenticate.

## 1. Let the kube-apiserver authenticate

`/webhook/enforce` creates QuotaClaims with the quota apiserver's own identity. A forged AdmissionReview from any pod could drain any tenant's quota. So the endpoint sits behind the apiserver's delegated authentication and authorization like every other path:

- An unauthenticated or anonymous caller gets 403. The handler also rejects anonymous callers itself.
- An authenticated caller must be allowed `post` on the non-resource URL `/webhook/enforce`. `rbac.yaml` grants that to the user `quota-enforcement-webhook` and to nobody else.
- Never add `/webhook/enforce` to `--authorization-always-allow-paths`. The apiserver refuses to start if an entry covers it, including wildcards such as `/webhook/*`.

By default the kube-apiserver sends no credentials to webhooks. Give it an admission kubeconfig with a client certificate whose common name is `quota-enforcement-webhook`, signed by the cluster's client CA (the kube-apiserver's `--client-ca-file`, which the quota apiserver reads from the `extension-apiserver-authentication` ConfigMap):

```yaml
# --admission-control-config-file=/etc/kubernetes/quota-webhook/admission.yaml
apiVersion: apiserver.config.k8s.io/v1
kind: AdmissionConfiguration
plugins:
- name: ValidatingAdmissionWebhook
  configuration:
    apiVersion: apiserver.config.k8s.io/v1
    kind: WebhookAdmissionConfiguration
    kubeConfigFile: /etc/kubernetes/quota-webhook/kubeconfig
---
# /etc/kubernetes/quota-webhook/kubeconfig
apiVersion: v1
kind: Config
users:
# <service>.<namespace>.svc of the webhook's clientConfig. Append :<port>
# if the webhook's service port is not 443.
- name: quota-apiserver.quota-system.svc
  user:
    client-certificate: /etc/kubernetes/quota-webhook/client.crt
    client-key: /etc/kubernetes/quota-webhook/client.key
```

A bearer token works too: put `token:` (for example a ServiceAccount token) in the kubeconfig user and change the subject of the `quota-enforcement-webhook-caller` ClusterRoleBinding to that identity.

Managed control planes that do not let you set `--admission-control-config-file` cannot authenticate to this webhook. Embed the enforcement plugin in your own apiserver instead (`pkg/admission/enforcement`).

For the kind cluster from `task dev:setup`, `task dev:webhook-client` does all of this: it issues the certificate from the kind cluster CA and restarts the kube-apiserver with the admission configuration.

## 2. Choose what is enforced

The namespaced webhook matches every kind, but only in namespaces labeled:

```yaml
metadata:
  labels:
    quota.miloapis.com/enforcement: enabled
```

Unlabeled namespaces (cert-manager, flux-system, monitoring, and so on) are never sent to the webhook, so a quota outage cannot block them. `kube-system`, `kube-public`, `kube-node-lease` and `quota-system` are excluded even when labeled. Namespace creation itself is never matched.

The label is the enforcement boundary. Anyone who can create or relabel a namespace can turn enforcement off for it. If tenants create their own namespaces, either set the label for them (for example with a mutating admission policy) or replace the label selector with an explicit list of kinds, as `config/milo/enforcement-webhook` does.

The cluster-scoped webhook matches nothing by default. A `namespaceSelector` cannot scope cluster-scoped objects, and an `objectSelector` would let the creator skip enforcement by leaving a label off. List the cluster-scoped kinds your ClaimCreationPolicies target with a patch on `/webhooks/1/rules`. See the comment in `validating-webhook-configuration.yaml`.

## 3. Trust the serving certificate

The `cert-manager.io/inject-ca-from: quota-system/quota-ca` annotation makes cert-manager's cainjector fill each webhook's `caBundle` with the quota CA from `config/components/cert-manager-ca`. That CA also issues the apiserver's serving certificate and is injected into the APIService.

## Availability

- Run at least two quota apiserver replicas. `config/base` ships two and a PodDisruptionBudget.
- Keep `--quota-enforcement-claim-timeout` (25s) below `timeoutSeconds` (30s).
- `matchConditions` need Kubernetes 1.30 or later.
