// Package core implements the core quota controllers that manage QuotaBuckets,
// QuotaClaims, QuotaGrants, and QuotaDefinitions.
//
// The QuotaBucketController owns the lifecycle of QuotaBuckets and the
// grant-derived portion of bucket status (Limit, GrantCount,
// ContributingGrantRefs, ObservedGeneration). It always runs, regardless of
// which storage backend is in use. Dynamic allocation state
// (Allocated/Available/ClaimCount) is owned either by the synchronous
// Postgres allocator (when --quota-allocation-mode=allocator) or by the
// QuotaBucketAllocationController (when --quota-allocation-mode=controller).
// Both writers use Server-Side Apply with distinct field managers to avoid
// racing each other on shared status.
package core

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotaapi "go.miloapis.com/quota/pkg/apis/quota"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// Field manager names used for Server-Side Apply writes to QuotaBucket and
// QuotaClaim status. Each controller uses its own manager so writes to
// disjoint status fields do not race.
const (
	bucketControllerFieldManager = "quota-bucket-controller"
	bucketAllocatorFieldManager  = "quota-bucket-allocator"
	claimControllerFieldManager  = "quota-claim-controller"
)

// QuotaBucketController reconciles QuotaBucket objects and maintains the
// spec-derived Limit (and its contributing grant refs) from active QuotaGrants.
// It is the single writer for the grant-derived portions of bucket status;
// the dynamic allocation state (Allocated/Available/ClaimCount) is owned by
// the synchronous allocator in the claim CREATE request path.
type QuotaBucketController struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager

	// BucketNamespace is the fallback namespace for buckets whose consumer
	// is not namespaced. Empty means DefaultBucketNamespace.
	BucketNamespace string
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotabuckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotabuckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotagrants,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims,verbs=get;list;watch

// Reconcile maintains QuotaBucket Limit and contributing grant references by
// watching QuotaGrants across all control planes. It writes bucket status at
// most once per reconcile, and only touches grant-derived fields; the
// allocation state (Allocated/Available/ClaimCount) is owned by the
// synchronous allocator in the claim CREATE path.
func (r *QuotaBucketController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if req.ClusterName != "" {
		logger = logger.WithValues("cluster", req.ClusterName)
		ctx = log.IntoContext(ctx, logger)
	}

	cluster, err := r.Manager.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get cluster %q: %w", req.ClusterName, err)
	}
	clusterClient := cluster.GetClient()

	var bucket quotav1alpha1.QuotaBucket
	if err := clusterClient.Get(ctx, req.NamespacedName, &bucket); err != nil {
		if apierrors.IsNotFound(err) {
			// Safety net: the QuotaGrantController is the primary creator of
			// buckets (ensureBucketsFromGrant runs when a grant becomes
			// Active). If a bucket is deleted while an active grant still
			// references it, this path recreates it from the grant spec so
			// the synchronous allocator finds a bucket waiting for it when
			// the next claim arrives.
			if err := r.ensureBucketFromGrants(ctx, clusterClient, req.NamespacedName); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaBucket: %w", err)
	}

	originalStatus := bucket.Status.DeepCopy()

	bucket.Status.ObservedGeneration = bucket.Generation

	if err := r.updateLimitsFromGrants(ctx, clusterClient, &bucket); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update limits from grants: %w", err)
	}

	return r.updateStatusIfChanged(ctx, clusterClient, &bucket, originalStatus)
}

// limitConflictRequeue is how soon a limit write that lost the bucket
// version race retries.
const limitConflictRequeue = 100 * time.Millisecond

// updateLimitsFromGrants calculates total quota limits from active QuotaGrants.
func (r *QuotaBucketController) updateLimitsFromGrants(ctx context.Context, clusterClient client.Client, bucket *quotav1alpha1.QuotaBucket) error {
	var grants quotav1alpha1.QuotaGrantList
	if err := clusterClient.List(ctx, &grants); err != nil {
		return fmt.Errorf("failed to list QuotaGrants: %w", err)
	}

	var totalLimit int64
	var contributingGrants []quotav1alpha1.ContributingGrantRef

	for _, grant := range grants.Items {
		if !r.isQuotaGrantActive(&grant) {
			continue
		}

		// The whole consumer identity must match: a same-named consumer
		// in another API group or namespace has its own bucket.
		if grant.Spec.ConsumerRef != bucket.Spec.ConsumerRef {
			continue
		}

		for _, allowance := range grant.Spec.Allowances {
			if allowance.ResourceType != bucket.Spec.ResourceType {
				continue
			}

			for _, allowanceBucket := range allowance.Buckets {
				// Saturate instead of wrapping: a negative limit would fail
				// status validation and wedge the bucket forever.
				totalLimit = quotaapi.SaturatingAdd(totalLimit, allowanceBucket.Amount)
				contributingGrants = append(contributingGrants, quotav1alpha1.ContributingGrantRef{
					Name:                   grant.Name,
					LastObservedGeneration: grant.Generation,
					Amount:                 allowanceBucket.Amount,
				})
			}
		}
	}

	bucket.Status.Limit = totalLimit
	bucket.Status.Available = availableFor(totalLimit, bucket.Status.Allocated)
	bucket.Status.GrantCount = int32(len(contributingGrants))
	bucket.Status.ContributingGrantRefs = contributingGrants

	return nil
}

// availableFor is the remaining capacity of a bucket. It is never negative:
// a limit lowered below what is already allocated leaves nothing available,
// it does not take capacity back.
func availableFor(limit, allocated int64) int64 {
	if allocated >= limit {
		return 0
	}
	return limit - allocated
}

// ensureBucketFromGrants creates the bucket spec from a referencing grant if
// found. This is the safety-net path for NotFound Reconciles — primary
// bucket creation happens in QuotaGrantController.ensureBucketsFromGrant
// when a grant becomes Active. If a bucket is deleted out-of-band while an
// active grant still references it, this path recreates it so the
// synchronous allocator has a bucket waiting when the next claim arrives.
func (r *QuotaBucketController) ensureBucketFromGrants(ctx context.Context, clusterClient client.Client, bucketKey types.NamespacedName) error {
	var grants quotav1alpha1.QuotaGrantList
	if err := clusterClient.List(ctx, &grants); err != nil {
		return fmt.Errorf("failed to list QuotaGrants: %w", err)
	}
	for _, grant := range grants.Items {
		for _, allowance := range grant.Spec.Allowances {
			name := generateQuotaBucketName(allowance.ResourceType, grant.Spec.ConsumerRef)
			if name == bucketKey.Name {
				bucket := &quotav1alpha1.QuotaBucket{
					ObjectMeta: metav1.ObjectMeta{
						Name:      bucketKey.Name,
						Namespace: bucketKey.Namespace,
						Labels: map[string]string{
							"quota.miloapis.com/consumer-kind": grant.Spec.ConsumerRef.Kind,
							"quota.miloapis.com/consumer-name": grant.Spec.ConsumerRef.Name,
						},
					},
					Spec: quotav1alpha1.QuotaBucketSpec{
						ConsumerRef:  grant.Spec.ConsumerRef,
						ResourceType: allowance.ResourceType,
					},
				}
				if err := clusterClient.Create(ctx, bucket); err != nil && !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("failed to create QuotaBucket %s from grant %s: %w", bucketKey.Name, grant.Name, err)
				}
				return nil
			}
		}
	}
	return nil
}

// isQuotaGrantActive checks if a QuotaGrant has an Active condition with status True.
func (r *QuotaBucketController) isQuotaGrantActive(grant *quotav1alpha1.QuotaGrant) bool {
	return apimeta.IsStatusConditionTrue(grant.Status.Conditions, quotav1alpha1.QuotaGrantActive)
}

// updateStatusIfChanged writes the grant-derived portion of bucket status
// via Server-Side Apply with the bucket controller's field manager. It
// asserts Limit/GrantCount/ContributingGrantRefs/ObservedGeneration/
// LastReconciliation plus Available, recomputed as max(limit-allocated, 0)
// from the Allocated value it read, so a limit change is reflected in
// Available immediately. Allocated and ClaimCount are omitted so the
// allocator (synchronous or controller) keeps ownership of them.
//
// The patch carries the resourceVersion that was read. If an allocator
// changed Allocated in the meantime, the apiserver rejects the patch with a
// Conflict and the reconcile retries from fresh state, so Available is
// never computed from a stale Allocated.
//
// The patch is built as an Unstructured object so that only the fields this
// controller owns are serialised into the SSA payload. Using a typed struct
// would include zero-valued int fields (allocated, available, claimCount)
// in the JSON because they lack omitempty, causing the bucket controller's
// field manager to claim ownership of allocation fields and fight with the
// allocation controller.
func (r *QuotaBucketController) updateStatusIfChanged(ctx context.Context, clusterClient client.Client, bucket *quotav1alpha1.QuotaBucket, originalStatus *quotav1alpha1.QuotaBucketStatus) (ctrl.Result, error) {
	grantDerivedChanged := bucket.Status.Limit != originalStatus.Limit ||
		bucket.Status.Available != originalStatus.Available ||
		bucket.Status.GrantCount != originalStatus.GrantCount ||
		bucket.Status.ObservedGeneration != originalStatus.ObservedGeneration ||
		!equality.Semantic.DeepEqual(bucket.Status.ContributingGrantRefs, originalStatus.ContributingGrantRefs)
	if !grantDerivedChanged {
		return ctrl.Result{}, nil
	}

	// Build contributing grant refs as unstructured slice.
	var grantRefs []interface{}
	for _, ref := range bucket.Status.ContributingGrantRefs {
		grantRefs = append(grantRefs, map[string]interface{}{
			"name":                   ref.Name,
			"lastObservedGeneration": ref.LastObservedGeneration,
			"amount":                 ref.Amount,
		})
	}

	status := map[string]interface{}{
		"observedGeneration": bucket.Status.ObservedGeneration,
		"limit":              bucket.Status.Limit,
		"available":          bucket.Status.Available,
		"grantCount":         int64(bucket.Status.GrantCount),
		"lastReconciliation": time.Now().UTC().Format(time.RFC3339),
	}
	if len(grantRefs) > 0 {
		status["contributingGrantRefs"] = grantRefs
	}

	patch := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": quotav1alpha1.SchemeGroupVersion.String(),
		"kind":       "QuotaBucket",
		"metadata": map[string]interface{}{
			"name":            bucket.Name,
			"namespace":       bucket.Namespace,
			"resourceVersion": bucket.ResourceVersion,
		},
		"status": status,
	}}

	if err := clusterClient.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(patch), client.FieldOwner(bucketControllerFieldManager), client.ForceOwnership); err != nil {
		if apierrors.IsConflict(err) {
			log.FromContext(ctx).V(1).Info("QuotaBucket changed while recomputing its limit; requeueing",
				"bucket", bucket.Name, "namespace", bucket.Namespace)
			return ctrl.Result{RequeueAfter: limitConflictRequeue}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to apply QuotaBucket status: %w", err)
	}

	return ctrl.Result{}, nil
}

// generateQuotaBucketName creates the deterministic name for a QuotaBucket.
// It delegates to quotaapi.BucketName, which the Postgres allocator also
// uses, so both always address the same bucket.
func generateQuotaBucketName(resourceType string, ref quotav1alpha1.ConsumerRef) string {
	return quotaapi.BucketName(resourceType, ref.APIGroup, ref.Kind, ref.Namespace, ref.Name)
}

// DefaultBucketNamespace is the namespace QuotaBuckets are placed in when the
// consumerRef carries no namespace and no override is configured.
const DefaultBucketNamespace = "quota-system"

// getBucketNamespace determines the namespace where a QuotaBucket lives.
// Buckets are co-located with a namespaced consumer; otherwise they fall back
// to the configured namespace (DefaultBucketNamespace when unset). The
// placement must match the synchronous allocator's, so the fallback is
// configured once in the controller-manager and apiserver flags.
func getBucketNamespace(consumerRef quotav1alpha1.ConsumerRef, fallback string) string {
	if consumerRef.Namespace != "" {
		return consumerRef.Namespace
	}
	if fallback != "" {
		return fallback
	}
	return DefaultBucketNamespace
}

// QuotaClaimConsumerIndex is the field index over QuotaClaim consumers. The
// key is the full consumer identity (apiGroup, kind, namespace, name; see
// quotaapi.ConsumerKey), the same identity the bucket name hashes, so an
// index lookup returns exactly the claims that can draw on a bucket.
const QuotaClaimConsumerIndex = "spec.consumerRef"

// ConsumerIndexKey returns the QuotaClaimConsumerIndex value for a consumer.
func ConsumerIndexKey(ref quotav1alpha1.ConsumerRef) string {
	return quotaapi.ConsumerKey(ref.APIGroup, ref.Kind, ref.Namespace, ref.Name)
}

// grantChangePredicate passes grant events that can change a bucket's
// limit: creation, deletion, any spec change (metadata.generation, which the
// grant strategy bumps on every spec change, or the spec itself for safety)
// and a flip of the Active condition.
func grantChangePredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldGrant, okOld := e.ObjectOld.(*quotav1alpha1.QuotaGrant)
			newGrant, okNew := e.ObjectNew.(*quotav1alpha1.QuotaGrant)
			if !okOld || !okNew {
				return true
			}
			if oldGrant.Generation != newGrant.Generation ||
				!equality.Semantic.DeepEqual(oldGrant.Spec, newGrant.Spec) {
				return true
			}
			oldActive := apimeta.IsStatusConditionTrue(oldGrant.Status.Conditions, quotav1alpha1.QuotaGrantActive)
			newActive := apimeta.IsStatusConditionTrue(newGrant.Status.Conditions, quotav1alpha1.QuotaGrantActive)
			return oldActive != newActive
		},
	}
}

// QuotaClaimConsumerIndexFunc extracts the QuotaClaimConsumerIndex value.
func QuotaClaimConsumerIndexFunc(obj client.Object) []string {
	claim, ok := obj.(*quotav1alpha1.QuotaClaim)
	if !ok {
		return nil
	}
	return []string{ConsumerIndexKey(claim.Spec.ConsumerRef)}
}

// SetupWithManager sets up the controller with the Manager.
func (r *QuotaBucketController) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaBucket{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Watches(
			&quotav1alpha1.QuotaGrant{},
			mchandler.TypedEnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []mcreconcile.Request {
					return r.enqueueAffectedBuckets(ctx, obj)
				},
			),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
			mcbuilder.WithPredicates(grantChangePredicate()),
		).
		Named("quota-bucket").
		Complete(r)
}

// enqueueAffectedBuckets determines which QuotaBuckets need to be reconciled.
func (r *QuotaBucketController) enqueueAffectedBuckets(ctx context.Context, obj client.Object) []mcreconcile.Request {
	var requests []mcreconcile.Request

	clusterName, _ := mccontext.ClusterFrom(ctx)

	switch o := obj.(type) {
	case *quotav1alpha1.QuotaGrant:
		for _, allowance := range o.Spec.Allowances {
			bucketName := generateQuotaBucketName(allowance.ResourceType, o.Spec.ConsumerRef)
			bucketNamespace := getBucketNamespace(o.Spec.ConsumerRef, r.BucketNamespace)
			requests = append(requests, mcreconcile.Request{
				ClusterName: clusterName,
				Request: ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      bucketName,
						Namespace: bucketNamespace,
					},
				},
			})
		}
	}

	return requests
}
