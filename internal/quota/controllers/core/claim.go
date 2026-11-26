// Package core: QuotaClaimController aggregates the per-allocation
// statuses written by the QuotaBucketAllocationController into the
// overall claim Granted condition. It only runs when
// --quota-allocation-mode=controller. In allocator mode the synchronous
// Postgres allocator sets every claim field transactionally inside the
// CREATE request, so no reconciler is needed.
package core

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaClaimController aggregates the per-allocation results produced by
// QuotaBucketAllocationController into the overall Granted condition.
type QuotaClaimController struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager
	// Workers is how many claims are reconciled concurrently. Zero means one.
	Workers int
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims/status,verbs=get;update;patch

// Reconcile computes the overall Granted condition from
// claim.Status.Allocations and writes it via Server-Side Apply.
func (r *QuotaClaimController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
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

	var claim quotav1alpha1.QuotaClaim
	if err := clusterClient.Get(ctx, req.NamespacedName, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("QuotaClaim not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaClaim: %w", err)
	}

	if !claim.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.updateOverallClaimConditionFromAllocations(ctx, clusterClient, &claim)
}

// updateOverallClaimConditionFromAllocations derives the overall Granted
// condition from the per-resourceType allocations and writes it via
// Server-Side Apply.
func (r *QuotaClaimController) updateOverallClaimConditionFromAllocations(ctx context.Context, clusterClient client.Client, claim *quotav1alpha1.QuotaClaim) error {
	allocationMap := make(map[string]quotav1alpha1.QuotaClaimAllocationStatus, len(claim.Status.Allocations))
	for _, allocation := range claim.Status.Allocations {
		allocationMap[allocation.ResourceType] = allocation
	}

	var grantedCount, deniedCount, pendingCount int
	totalRequests := len(claim.Spec.Requests)

	for _, request := range claim.Spec.Requests {
		allocation, exists := allocationMap[request.ResourceType]
		if !exists {
			pendingCount++
			continue
		}
		switch allocation.Status {
		case quotav1alpha1.QuotaClaimAllocationStatusGranted:
			grantedCount++
		case quotav1alpha1.QuotaClaimAllocationStatusDenied:
			deniedCount++
		case quotav1alpha1.QuotaClaimAllocationStatusPending:
			pendingCount++
		default:
			pendingCount++
		}
	}

	var conditionStatus metav1.ConditionStatus
	var reason, message string
	switch {
	case grantedCount == totalRequests:
		conditionStatus = metav1.ConditionTrue
		reason = quotav1alpha1.QuotaClaimGrantedReason
		message = fmt.Sprintf("All %d resource requests have been granted", totalRequests)
	case deniedCount > 0:
		conditionStatus = metav1.ConditionFalse
		reason = quotav1alpha1.QuotaClaimDeniedReason
		message = "Insufficient quota resources. Contact your account administrator to review quota limits and usage."
	default:
		conditionStatus = metav1.ConditionFalse
		reason = quotav1alpha1.QuotaClaimPendingReason
		message = fmt.Sprintf("Awaiting capacity evaluation: %d granted, %d pending", grantedCount, pendingCount)
	}

	return r.applyOverallClaimCondition(ctx, clusterClient, claim, conditionStatus, reason, message)
}

// applyOverallClaimCondition writes the overall Granted condition and
// ObservedGeneration via Server-Side Apply with the claim controller's
// field manager.
func (r *QuotaClaimController) applyOverallClaimCondition(ctx context.Context, clusterClient client.Client, claim *quotav1alpha1.QuotaClaim, status metav1.ConditionStatus, reason, message string) error {
	existing := apimeta.FindStatusCondition(claim.Status.Conditions, quotav1alpha1.QuotaClaimGranted)
	if existing != nil &&
		existing.Status == status &&
		existing.Reason == reason &&
		existing.Message == message &&
		claim.Status.ObservedGeneration == claim.Generation {
		return nil
	}

	condition := metav1.Condition{
		Type:               quotav1alpha1.QuotaClaimGranted,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: claim.Generation,
		LastTransitionTime: metav1.Now(),
	}
	if existing != nil && existing.Status == status {
		condition.LastTransitionTime = existing.LastTransitionTime
	}

	// The apply configuration is built from the typed object, which
	// serializes exactly like the former typed apply patch: status fields
	// without omitempty are zero only where this controller owns them.
	typed := &quotav1alpha1.QuotaClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: quotav1alpha1.SchemeGroupVersion.String(),
			Kind:       "QuotaClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      claim.Name,
			Namespace: claim.Namespace,
		},
		Status: quotav1alpha1.QuotaClaimStatus{
			ObservedGeneration: claim.Generation,
			Conditions:         []metav1.Condition{condition},
		},
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		return fmt.Errorf("failed to build QuotaClaim condition apply configuration: %w", err)
	}
	patch := client.ApplyConfigurationFromUnstructured(&unstructured.Unstructured{Object: content})
	if err := clusterClient.Status().Apply(ctx, patch, client.FieldOwner(claimControllerFieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply overall QuotaClaim condition: %w", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *QuotaClaimController) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaClaim{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Named("quota-claim").
		WithOptions(controller.TypedOptions[mcreconcile.Request]{MaxConcurrentReconciles: r.Workers}).
		Complete(r)
}
