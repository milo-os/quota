// Package core: QuotaBucketAllocationController runs the controller-mode
// allocation pump. It only runs when --quota-allocation-mode=controller
// (the etcd/dual default); in allocator mode the synchronous Postgres
// allocator owns all allocation writes and this reconciler is not
// registered with the manager.
package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/metrics"
	quotaapi "go.miloapis.com/quota/pkg/apis/quota"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaBucketAllocationController drives the controller-mode allocation
// pump. For each QuotaBucket it scans referencing QuotaClaims, decides
// Granted/Denied for each unprocessed request based on current capacity,
// aggregates the deltas, then writes the bucket allocation status exactly
// once via Server-Side Apply and patches each affected claim's allocation
// entry via Server-Side Apply. The bucket write and claim writes use
// distinct field managers from the QuotaBucketController so they do not
// race on disjoint fields.
type QuotaBucketAllocationController struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager

	// BucketNamespace is the fallback namespace for buckets whose consumer
	// is not namespaced. Empty means DefaultBucketNamespace.
	BucketNamespace string
	// Workers is how many buckets are reconciled concurrently. Zero means
	// one. Concurrent reconciles of different buckets are safe: each bucket
	// write is version-checked and each bucket patches claims under its own
	// field manager.
	Workers int
}

// allocationConflictRequeue is how soon a reconcile that lost the bucket
// version race retries. The winning write also emits a watch event, so this
// is only a backstop.
const allocationConflictRequeue = 100 * time.Millisecond

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotabuckets,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotabuckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims/status,verbs=get;update;patch

// Reconcile computes allocation changes for a single QuotaBucket.
func (r *QuotaBucketAllocationController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
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
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaBucket: %w", err)
	}

	return r.reconcileAllocations(ctx, clusterClient, cluster.GetAPIReader(), &bucket)
}

// reconcileAllocations is the aggregate pump: it lists matching claims,
// recomputes the used/claim-count from granted allocations, decides new
// Granted/Denied outcomes for any pending requests that target this
// bucket, and writes everything with a single bucket write and one apply
// per affected claim.
//
// Decisions are made only from an authoritative claim list. The informer
// cache can lag this controller's own writes: a claim granted by the
// previous reconcile can still look pending, and deciding from that view
// would grant a newer claim against the same capacity and then re-decide
// the older one as Denied after its resource was already admitted. So
// whenever the cached view has anything to decide, the claims are re-read
// from the apiserver (a quorum read that includes every claim patch the
// previous reconcile made) before any decision. The cache alone is used
// only when nothing is pending, where it can at worst make the informational
// Allocated/Available briefly stale. The claim status strategy additionally
// rejects any write that changes a Granted allocation.
func (r *QuotaBucketAllocationController) reconcileAllocations(ctx context.Context, clusterClient client.Client, liveReader client.Reader, bucket *quotav1alpha1.QuotaBucket) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	reconcileStart := time.Now()

	// Only claims from this bucket's consumer can draw on it; the field
	// index keeps the reconcile from scanning every claim in the cluster.
	var claims quotav1alpha1.QuotaClaimList
	if err := clusterClient.List(ctx, &claims,
		client.MatchingFields{QuotaClaimConsumerIndex: ConsumerIndexKey(bucket.Spec.ConsumerRef)},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list QuotaClaims: %w", err)
	}

	if r.hasPendingRequests(claims.Items, bucket) {
		if liveReader == nil {
			return ctrl.Result{}, fmt.Errorf("no live reader available to confirm pending claims for bucket %s/%s", bucket.Namespace, bucket.Name)
		}
		ref := bucket.Spec.ConsumerRef
		var live quotav1alpha1.QuotaClaimList
		if err := liveReader.List(ctx, &live, client.MatchingFields{
			"spec.consumerRef.apiGroup":  ref.APIGroup,
			"spec.consumerRef.kind":      ref.Kind,
			"spec.consumerRef.name":      ref.Name,
			"spec.consumerRef.namespace": ref.Namespace,
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to list QuotaClaims from the apiserver: %w", err)
		}
		claims = live
	}

	// Decide in creation order so the oldest pending claim is served first,
	// whatever order the list returned.
	sort.SliceStable(claims.Items, func(i, j int) bool {
		a, b := &claims.Items[i], &claims.Items[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})

	limit := bucket.Status.Limit
	var allocated int64
	var claimCount int32

	// First pass: tally already-granted allocations that match this bucket.
	for _, claim := range claims.Items {
		if claim.Spec.ConsumerRef != bucket.Spec.ConsumerRef {
			continue
		}
		hasGranted := false
		for _, allocation := range claim.Status.Allocations {
			if allocation.Status != quotav1alpha1.QuotaClaimAllocationStatusGranted {
				continue
			}
			if allocation.ResourceType != bucket.Spec.ResourceType {
				continue
			}
			allocated = quotaapi.SaturatingAdd(allocated, allocation.AllocatedAmount)
			hasGranted = true
		}
		if hasGranted {
			claimCount++
		}
	}

	// Second pass: decide pending requests and stage per-claim patches.
	type claimAllocationPatch struct {
		claim      *quotav1alpha1.QuotaClaim
		allocation quotav1alpha1.QuotaClaimAllocationStatus
	}
	var claimPatches []claimAllocationPatch

	for i := range claims.Items {
		claim := &claims.Items[i]
		if claim.Spec.ConsumerRef != bucket.Spec.ConsumerRef {
			continue
		}
		if !claim.DeletionTimestamp.IsZero() {
			continue
		}
		for _, request := range claim.Spec.Requests {
			if request.ResourceType != bucket.Spec.ResourceType {
				continue
			}
			// A decided request (Granted or Denied) is never re-decided.
			if r.isAllocationProcessed(claim, request.ResourceType) {
				continue
			}

			available := availableFor(limit, allocated)
			if available < request.Amount {
				logger.Info("Insufficient quota for claim request",
					"claim", claim.Name,
					"namespace", claim.Namespace,
					"resourceType", request.ResourceType,
					"requested", request.Amount,
					"available", available,
				)
				claimPatches = append(claimPatches, claimAllocationPatch{
					claim: claim,
					allocation: quotav1alpha1.QuotaClaimAllocationStatus{
						ResourceType:       request.ResourceType,
						Status:             quotav1alpha1.QuotaClaimAllocationStatusDenied,
						Reason:             quotav1alpha1.QuotaClaimDeniedReason,
						Message:            fmt.Sprintf("Resource quota exceeded: requested %d, available %d", request.Amount, available),
						LastTransitionTime: metav1.Now(),
					},
				})
				continue
			}

			allocated = quotaapi.SaturatingAdd(allocated, request.Amount)
			claimCount++
			claimPatches = append(claimPatches, claimAllocationPatch{
				claim: claim,
				allocation: quotav1alpha1.QuotaClaimAllocationStatus{
					ResourceType:       request.ResourceType,
					Status:             quotav1alpha1.QuotaClaimAllocationStatusGranted,
					Reason:             quotav1alpha1.QuotaClaimGrantedReason,
					Message:            "Capacity reserved",
					AllocatedAmount:    request.Amount,
					AllocatingBucket:   bucket.Name,
					LastTransitionTime: metav1.Now(),
				},
			})
		}
	}

	available := availableFor(limit, allocated)

	// Write the aggregate bucket allocation state exactly once per
	// reconcile, BEFORE any claim is marked. The write is a version-checked
	// Update: it carries the resourceVersion this reconcile read, so if any
	// other writer (a concurrent reconcile, another replica, or a stale
	// cache read) has changed the bucket since, the API server rejects it
	// with a Conflict. On Conflict no claim is marked, so two reconciles can
	// never both grant against the same capacity; the loser requeues and
	// re-decides from fresh state. This mirrors Milo's reservation step.
	//
	// Update (rather than Server-Side Apply with ForceOwnership) is what
	// provides the precondition. It only changes the allocation-owned fields
	// (allocated, available, claimCount, lastReconciliation); the
	// grant-derived fields are carried through unchanged from the object we
	// read, and because the write is version-checked they cannot be stale.
	if bucket.Status.Allocated != allocated ||
		bucket.Status.Available != available ||
		bucket.Status.ClaimCount != claimCount ||
		len(claimPatches) > 0 {
		updated := bucket.DeepCopy()
		updated.Status.Allocated = allocated
		updated.Status.Available = available
		updated.Status.ClaimCount = claimCount
		now := metav1.Now()
		updated.Status.LastReconciliation = &now
		if err := clusterClient.Status().Update(ctx, updated, client.FieldOwner(bucketAllocatorFieldManager)); err != nil {
			if apierrors.IsConflict(err) {
				logger.V(1).Info("QuotaBucket changed during allocation; requeueing without marking claims",
					"bucket", bucket.Name, "namespace", bucket.Namespace, "pendingDecisions", len(claimPatches))
				return ctrl.Result{RequeueAfter: allocationConflictRequeue}, nil
			}
			// Every pending decision in this reconcile failed to land.
			metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError).Add(float64(len(claimPatches)))
			return ctrl.Result{}, fmt.Errorf("failed to update QuotaBucket allocation status: %w", err)
		}
	}

	// Apply each per-claim allocation delta. Each patch uses a field
	// manager scoped to the originating bucket so that different buckets
	// writing allocations for the same claim (multi-resource claims) do
	// not clobber each other. The allocations list is a map-list keyed
	// by resourceType, so SSA merges entries from distinct field managers
	// correctly.
	perBucketFM := fmt.Sprintf("%s-%s", bucketAllocatorFieldManager, bucket.Name)
	for _, p := range claimPatches {
		alloc := map[string]interface{}{
			"resourceType":       p.allocation.ResourceType,
			"status":             string(p.allocation.Status),
			"reason":             p.allocation.Reason,
			"lastTransitionTime": p.allocation.LastTransitionTime.UTC().Format(time.RFC3339),
		}
		if p.allocation.Message != "" {
			alloc["message"] = p.allocation.Message
		}
		if p.allocation.AllocatedAmount != 0 {
			alloc["allocatedAmount"] = p.allocation.AllocatedAmount
		}
		if p.allocation.AllocatingBucket != "" {
			alloc["allocatingBucket"] = p.allocation.AllocatingBucket
		}

		allocationPatch := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": quotav1alpha1.SchemeGroupVersion.String(),
			"kind":       "QuotaClaim",
			"metadata": map[string]interface{}{
				"name":      p.claim.Name,
				"namespace": p.claim.Namespace,
			},
			"status": map[string]interface{}{
				"allocations": []interface{}{alloc},
			},
		}}
		if err := clusterClient.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(allocationPatch), client.FieldOwner(perBucketFM)); err != nil {
			logger.Error(err, "failed to apply claim allocation status",
				"claim", p.claim.Name, "namespace", p.claim.Namespace, "resourceType", p.allocation.ResourceType)
			metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError).Inc()
			return ctrl.Result{}, fmt.Errorf("failed to apply claim allocation status: %w", err)
		}
		// Record the per-allocation decision. Because the controller is async
		// and works per-bucket, the observed latency is a proxy: elapsed time
		// for this reconcile, not end-to-end from claim CREATE to decision.
		// The isAllocationProcessed guard above ensures we only count real
		// pending -> granted/denied transitions, not re-reconciles.
		var result string
		if p.allocation.Status == quotav1alpha1.QuotaClaimAllocationStatusGranted {
			result = "granted"
		} else {
			result = "denied"
		}
		metrics.QuotaClaimTotal.WithLabelValues(result).Inc()
		metrics.QuotaClaimLatency.WithLabelValues(result).Observe(time.Since(reconcileStart).Seconds())
	}

	return ctrl.Result{}, nil
}

// hasPendingRequests reports whether any live claim of the bucket's consumer
// has an undecided request for the bucket's resource type.
func (r *QuotaBucketAllocationController) hasPendingRequests(claims []quotav1alpha1.QuotaClaim, bucket *quotav1alpha1.QuotaBucket) bool {
	for i := range claims {
		claim := &claims[i]
		if claim.Spec.ConsumerRef != bucket.Spec.ConsumerRef || !claim.DeletionTimestamp.IsZero() {
			continue
		}
		for _, request := range claim.Spec.Requests {
			if request.ResourceType == bucket.Spec.ResourceType && !r.isAllocationProcessed(claim, request.ResourceType) {
				return true
			}
		}
	}
	return false
}

// isAllocationProcessed checks if a specific request has already been
// decided (Granted or Denied).
func (r *QuotaBucketAllocationController) isAllocationProcessed(claim *quotav1alpha1.QuotaClaim, resourceType string) bool {
	for _, allocation := range claim.Status.Allocations {
		if allocation.ResourceType != resourceType {
			continue
		}
		if allocation.Status == quotav1alpha1.QuotaClaimAllocationStatusGranted ||
			allocation.Status == quotav1alpha1.QuotaClaimAllocationStatusDenied {
			return true
		}
	}
	return false
}

// SetupWithManager wires the allocation controller to watch QuotaBuckets,
// QuotaGrants (to refresh limits) and QuotaClaims (to process new
// requests).
func (r *QuotaBucketAllocationController) SetupWithManager(mgr mcmanager.Manager) error {
	if err := RegisterQuotaClaimConsumerIndex(context.Background(), mgr); err != nil {
		return err
	}
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaBucket{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Watches(
			&quotav1alpha1.QuotaGrant{},
			mchandler.TypedEnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []mcreconcile.Request {
					return r.enqueueAffectedBucketsFromGrant(ctx, obj)
				},
			),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
			mcbuilder.WithPredicates(predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool { return true },
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldGrant := e.ObjectOld.(*quotav1alpha1.QuotaGrant)
					newGrant := e.ObjectNew.(*quotav1alpha1.QuotaGrant)
					if oldGrant.Generation != newGrant.Generation {
						return true
					}
					oldActive := apimeta.IsStatusConditionTrue(oldGrant.Status.Conditions, quotav1alpha1.QuotaGrantActive)
					newActive := apimeta.IsStatusConditionTrue(newGrant.Status.Conditions, quotav1alpha1.QuotaGrantActive)
					return oldActive != newActive
				},
			}),
		).
		Watches(
			&quotav1alpha1.QuotaClaim{},
			mchandler.TypedEnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []mcreconcile.Request {
					return r.enqueueAffectedBucketsFromClaim(ctx, obj)
				},
			),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
			mcbuilder.WithPredicates(predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool { return true },
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldClaim := e.ObjectOld.(*quotav1alpha1.QuotaClaim)
					newClaim := e.ObjectNew.(*quotav1alpha1.QuotaClaim)
					return oldClaim.Generation != newClaim.Generation
				},
				DeleteFunc: func(e event.DeleteEvent) bool { return true },
			}),
		).
		Named("quota-bucket-allocation").
		WithOptions(controller.TypedOptions[mcreconcile.Request]{MaxConcurrentReconciles: r.Workers}).
		Complete(r)
}

// enqueueAffectedBucketsFromGrant maps a grant change to the set of
// buckets whose allocation state may be affected.
func (r *QuotaBucketAllocationController) enqueueAffectedBucketsFromGrant(ctx context.Context, obj client.Object) []mcreconcile.Request {
	clusterName, _ := mccontext.ClusterFrom(ctx)
	grant, ok := obj.(*quotav1alpha1.QuotaGrant)
	if !ok {
		return nil
	}
	var requests []mcreconcile.Request
	for _, allowance := range grant.Spec.Allowances {
		requests = append(requests, mcreconcile.Request{
			ClusterName: clusterName,
			Request: ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      generateQuotaBucketName(allowance.ResourceType, grant.Spec.ConsumerRef),
					Namespace: getBucketNamespace(grant.Spec.ConsumerRef, r.BucketNamespace),
				},
			},
		})
	}
	return requests
}

// enqueueAffectedBucketsFromClaim maps a claim change to the set of
// buckets whose allocation state may be affected.
func (r *QuotaBucketAllocationController) enqueueAffectedBucketsFromClaim(ctx context.Context, obj client.Object) []mcreconcile.Request {
	clusterName, _ := mccontext.ClusterFrom(ctx)
	claim, ok := obj.(*quotav1alpha1.QuotaClaim)
	if !ok {
		return nil
	}
	var requests []mcreconcile.Request
	for _, request := range claim.Spec.Requests {
		requests = append(requests, mcreconcile.Request{
			ClusterName: clusterName,
			Request: ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      generateQuotaBucketName(request.ResourceType, claim.Spec.ConsumerRef),
					Namespace: getBucketNamespace(claim.Spec.ConsumerRef, r.BucketNamespace),
				},
			},
		})
	}
	return requests
}

// RegisterQuotaClaimConsumerIndex registers QuotaClaimConsumerIndex on the
// provider clusters and on the local cluster so indexed List calls work
// wherever a bucket lives.
//
// With a nil provider the multicluster indexer already targets the local
// cache, so a second local registration would fail as a duplicate index.
func RegisterQuotaClaimConsumerIndex(ctx context.Context, mgr mcmanager.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &quotav1alpha1.QuotaClaim{},
		QuotaClaimConsumerIndex, QuotaClaimConsumerIndexFunc); err != nil {
		return fmt.Errorf("failed to index QuotaClaim consumerRef on provider clusters: %w", err)
	}
	if mgr.GetProvider() == nil {
		return nil
	}
	if err := mgr.GetLocalManager().GetFieldIndexer().IndexField(ctx, &quotav1alpha1.QuotaClaim{},
		QuotaClaimConsumerIndex, QuotaClaimConsumerIndexFunc); err != nil {
		return fmt.Errorf("failed to index QuotaClaim consumerRef on local cluster: %w", err)
	}
	return nil
}
