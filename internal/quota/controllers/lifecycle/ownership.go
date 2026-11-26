package lifecycle

import (
	"context"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaClaimOwnershipController establishes owner references on QuotaClaims and
// performs safety-net cleanup for true orphans.
type QuotaClaimOwnershipController struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager

	restMapper meta.RESTMapper
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch

// Reconcile identifies and cleans up orphaned QuotaClaims.
func (r *QuotaClaimOwnershipController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("quotaclaim-ownership")
	if req.ClusterName != "" {
		logger = logger.WithValues("cluster", req.ClusterName)
		ctx = log.IntoContext(ctx, logger)
	}

	cluster, err := r.Manager.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get cluster %q: %w", req.ClusterName, err)
	}
	clusterClient := cluster.GetClient()

	var claim quotav1alpha1.QuotaClaim
	if err := clusterClient.Get(ctx, req.NamespacedName, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaClaim: %w", err)
	}

	if !claim.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Only process claims created by the admission plugin; manually created
	// claims are managed externally and must never have owner references
	// imposed on them or be garbage-collected as orphans.
	if !isAutoCreatedClaim(&claim) {
		logger.V(2).Info("Skipping manually created claim", "name", claim.Name, "ns", claim.Namespace)
		return ctrl.Result{}, nil
	}

	// A claim without a resourceRef has no owner to resolve, so there is
	// nothing to attach and nothing to consider orphaned.
	if !hasResourceRef(&claim) {
		logger.V(2).Info("Claim has no resourceRef; skipping", "name", claim.Name, "ns", claim.Namespace)
		return ctrl.Result{}, nil
	}

	if !isQuotaClaimGranted(&claim) {
		logger.V(2).Info("Claim not granted; skipping", "name", claim.Name, "ns", claim.Namespace)
		return ctrl.Result{}, nil
	}

	if len(claim.OwnerReferences) > 0 {
		logger.V(2).Info("QuotaClaim has owner references, skipping", "claim", claim.Name)
		return ctrl.Result{}, nil
	}

	claimAge := time.Since(claim.CreationTimestamp.Time)

	// Fast path: attempt to resolve owner immediately and set ownerRef
	ownerObj, _, ownerNS, err := r.resolveOwner(ctx, cluster, &claim)
	if err == nil && ownerObj != nil {
		return r.actOnOwner(ctx, clusterClient, &claim, ownerObj, ownerNS, 500*time.Millisecond)
	}

	// The resourceRef's kind is not served by this cluster's API, so the owner
	// cannot be resolved here. That is not evidence of an orphan (the kind may
	// live in another control plane), so leave the claim alone rather than
	// deleting it after the grace period.
	if meta.IsNoMatchError(err) {
		logger.V(2).Info("ResourceRef kind not served by this cluster; skipping ownership", "claim", claim.Name,
			"apiGroup", claim.Spec.ResourceRef.APIGroup, "kind", claim.Spec.ResourceRef.Kind)
		return ctrl.Result{}, nil
	}

	if apierrors.IsNotFound(err) || err == nil {
		grace := r.getOwnershipGracePeriod()
		if claimAge < grace {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		ownerObj2, _, ownerNS2, err2 := r.resolveOwner(ctx, cluster, &claim)
		if err2 == nil && ownerObj2 != nil {
			return r.actOnOwner(ctx, clusterClient, &claim, ownerObj2, ownerNS2, time.Second)
		}

		maxAge := r.getOrphanMaxAge()
		if claimAge > maxAge {
			logger.Info("Deleting orphaned QuotaClaim after max age", "claim", claim.Name, "age", claimAge, "maxAge", maxAge)
			return ctrl.Result{}, clusterClient.Delete(ctx, &claim)
		}

		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, err
}

// ownershipAction is what the controller does with a claim once an object
// with the claim's resourceRef name has been found.
type ownershipAction int

const (
	// attachOwner sets the object as the claim's owner.
	attachOwner ownershipAction = iota
	// deleteStaleClaim deletes a claim whose recorded resource UID is not
	// the object's: its create never landed and another object now holds
	// the name, so keeping it would charge twice.
	deleteStaleClaim
	// skipCrossNamespace leaves the claim without an owner: owner
	// references cannot cross namespaces, and the garbage collector would
	// delete the claim (releasing the allocation) while the resource lives.
	skipCrossNamespace
)

// decideOwnership matches a claim to the object found under its
// resourceRef. ownerNamespace is "" for a cluster-scoped owner.
func decideOwnership(claim *quotav1alpha1.QuotaClaim, owner *unstructured.Unstructured, ownerNamespace string) ownershipAction {
	if uid := claim.Annotations[resourceUIDAnnotation]; uid != "" && uid != string(owner.GetUID()) {
		return deleteStaleClaim
	}
	if ownerNamespace != "" && ownerNamespace != claim.Namespace {
		return skipCrossNamespace
	}
	return attachOwner
}

// actOnOwner applies decideOwnership's verdict.
func (r *QuotaClaimOwnershipController) actOnOwner(ctx context.Context, clusterClient client.Client, claim *quotav1alpha1.QuotaClaim, owner *unstructured.Unstructured, ownerNS string, retry time.Duration) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	switch decideOwnership(claim, owner, ownerNS) {
	case deleteStaleClaim:
		logger.Info("Deleting QuotaClaim whose resource was never created; another object holds the name",
			"claim", claim.Name, "claimResourceUID", claim.Annotations[resourceUIDAnnotation], "objectUID", owner.GetUID())
		// Precondition on the resourceVersion: admission may have just
		// re-pointed the claim at this object (a retry with a pinned claim
		// name), and that claim must survive.
		rv := claim.ResourceVersion
		err := clusterClient.Delete(ctx, claim, client.Preconditions{ResourceVersion: &rv})
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	case skipCrossNamespace:
		logger.Info("Not setting a cross-namespace owner reference on QuotaClaim",
			"claim", claim.Name, "claimNamespace", claim.Namespace, "ownerNamespace", ownerNS)
		return ctrl.Result{}, nil
	default:
		if err := r.applyOwnerReferenceSSA(ctx, clusterClient, claim, owner); err != nil {
			logger.Error(err, "Failed to set ownerReference via SSA; requeue")
			return ctrl.Result{RequeueAfter: retry}, nil
		}
		return ctrl.Result{}, nil
	}
}

// getOwnershipGracePeriod returns the grace period before considering a claim for orphan analysis.
func (r *QuotaClaimOwnershipController) getOwnershipGracePeriod() time.Duration {
	if envVal := os.Getenv("QUOTACLAIM_GRACE_PERIOD"); envVal != "" {
		if duration, err := time.ParseDuration(envVal); err == nil {
			return duration
		}
	}
	return 30 * time.Second
}

// getOrphanMaxAge returns the maximum age before deleting truly orphaned claims.
func (r *QuotaClaimOwnershipController) getOrphanMaxAge() time.Duration {
	if envVal := os.Getenv("QUOTACLAIM_MAX_ORPHAN_AGE"); envVal != "" {
		if duration, err := time.ParseDuration(envVal); err == nil {
			return duration
		}
	}
	return 30 * time.Second
}

// SetupWithManager sets up the controller with the Manager.
func (r *QuotaClaimOwnershipController) SetupWithManager(mgr mcmanager.Manager) error {
	r.restMapper = mgr.GetLocalManager().GetRESTMapper()

	onlyGrantedMissingOwner := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			claim, ok := e.Object.(*quotav1alpha1.QuotaClaim)
			if !ok {
				return false
			}
			return isOwnershipCandidate(claim) && isQuotaClaimGranted(claim) && len(claim.GetOwnerReferences()) == 0
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			newClaim, ok := e.ObjectNew.(*quotav1alpha1.QuotaClaim)
			if !ok {
				return false
			}
			if !isOwnershipCandidate(newClaim) {
				return false
			}
			if !isQuotaClaimGranted(newClaim) || len(newClaim.GetOwnerReferences()) > 0 {
				return false
			}
			oldClaim, ok := e.ObjectOld.(*quotav1alpha1.QuotaClaim)
			if !ok {
				return true
			}
			// Only fire on the transition into "granted without owner".
			return !isQuotaClaimGranted(oldClaim) || len(oldClaim.GetOwnerReferences()) > 0
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaClaim{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
			mcbuilder.WithPredicates(onlyGrantedMissingOwner)).
		Named("quota-claim-ownership").
		Complete(r)
}

// resolveOwner resolves the owning object and returns it, along with its GVK and namespace used for GET.
func (r *QuotaClaimOwnershipController) resolveOwner(ctx context.Context, cluster interface {
	GetConfig() *rest.Config
}, claim *quotav1alpha1.QuotaClaim) (*unstructured.Unstructured, schema.GroupVersionKind, string, error) {
	if r.restMapper == nil {
		return nil, schema.GroupVersionKind{}, "", fmt.Errorf("RESTMapper not initialized")
	}

	dynamicClient, err := dynamic.NewForConfig(cluster.GetConfig())
	if err != nil {
		return nil, schema.GroupVersionKind{}, "", fmt.Errorf("failed to create dynamic client for cluster: %w", err)
	}

	if !hasResourceRef(claim) {
		return nil, schema.GroupVersionKind{}, "", fmt.Errorf("claim %s/%s has no resourceRef", claim.Namespace, claim.Name)
	}
	gk := schema.GroupKind{Group: claim.Spec.ResourceRef.APIGroup, Kind: claim.Spec.ResourceRef.Kind}
	mapping, err := r.restMapper.RESTMapping(gk)
	if err != nil {
		return nil, schema.GroupVersionKind{}, "", err
	}
	gvr := mapping.Resource
	gvk := mapping.GroupVersionKind

	ownerNS := ""
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		if claim.Spec.ResourceRef.Namespace != "" {
			ownerNS = claim.Spec.ResourceRef.Namespace
		} else {
			ownerNS = claim.Namespace
		}
	}

	obj, err := dynamicClient.Resource(gvr).Namespace(ownerNS).Get(ctx, claim.Spec.ResourceRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, schema.GroupVersionKind{}, ownerNS, err
	}
	if obj.GetAPIVersion() == "" {
		obj.SetAPIVersion(gvk.GroupVersion().String())
	}
	if obj.GetKind() == "" {
		obj.SetKind(gvk.Kind)
	}
	return obj, gvk, ownerNS, nil
}

// applyOwnerReferenceSSA sets a single controller ownerReference on the claim using Server-Side Apply.
func (r *QuotaClaimOwnershipController) applyOwnerReferenceSSA(ctx context.Context, clusterClient client.Client, claim *quotav1alpha1.QuotaClaim, owner *unstructured.Unstructured) error {
	patch := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": quotav1alpha1.SchemeGroupVersion.String(),
		"kind":       "QuotaClaim",
		"metadata": map[string]interface{}{
			"name":      claim.Name,
			"namespace": claim.Namespace,
			"ownerReferences": []interface{}{
				map[string]interface{}{
					"apiVersion":         owner.GetAPIVersion(),
					"kind":               owner.GetKind(),
					"name":               owner.GetName(),
					"uid":                string(owner.GetUID()),
					"controller":         false,
					"blockOwnerDeletion": false,
				},
			},
		},
	}}
	return clusterClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(patch), client.FieldOwner("quotaclaim-ownership-metadata"))
}

// isQuotaClaimGranted checks if the claim has Granted=True.
func isQuotaClaimGranted(claim *quotav1alpha1.QuotaClaim) bool {
	return meta.IsStatusConditionTrue(claim.Status.Conditions, quotav1alpha1.QuotaClaimGranted)
}

// Markers the admission plugin stamps on every claim it creates. Both must be
// present for a claim to be treated as auto-created.
const (
	autoCreatedLabel      = "quota.miloapis.com/auto-created"
	createdByAnnotation   = "quota.miloapis.com/created-by"
	createdByPluginMarker = "claim-creation-plugin"
	// resourceUIDAnnotation is enforcement.ResourceUIDAnnotation: the UID of
	// the object whose create produced the claim.
	resourceUIDAnnotation = "quota.miloapis.com/resource-uid"
)

// isAutoCreatedClaim returns true when the claim was created by the admission
// plugin, identified by both the auto-created label and created-by annotation.
func isAutoCreatedClaim(claim *quotav1alpha1.QuotaClaim) bool {
	return claim.Labels[autoCreatedLabel] == "true" &&
		claim.Annotations[createdByAnnotation] == createdByPluginMarker
}

// hasResourceRef reports whether the claim names a triggering resource.
// spec.resourceRef is an optional value struct, so an empty Kind or Name means
// "no resourceRef".
func hasResourceRef(claim *quotav1alpha1.QuotaClaim) bool {
	return claim.Spec.ResourceRef.Kind != "" && claim.Spec.ResourceRef.Name != ""
}

// isOwnershipCandidate reports whether the ownership controller may act on the
// claim at all.
func isOwnershipCandidate(claim *quotav1alpha1.QuotaClaim) bool {
	return isAutoCreatedClaim(claim) && hasResourceRef(claim)
}
