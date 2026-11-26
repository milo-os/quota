// Package core implements the core quota controllers that manage QuotaBuckets,
// QuotaClaims, QuotaGrants, and QuotaDefinitions.
//
// The QuotaGrantController validates QuotaGrants against QuotaDefinitions
// and manages their Active status condition.
package core

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/quota/validation"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaGrantController reconciles a QuotaGrant object.
type QuotaGrantController struct {
	Scheme         *runtime.Scheme
	Manager        mcmanager.Manager
	GrantValidator *validation.QuotaGrantValidator

	// BucketNamespace is the fallback namespace for buckets whose consumer
	// is not namespaced. Empty means DefaultBucketNamespace.
	BucketNamespace string
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotagrants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotagrants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotadefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotabuckets,verbs=get;create

// Reconcile manages the lifecycle of QuotaGrant objects across all control planes.
func (r *QuotaGrantController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if req.ClusterName != "" {
		logger = logger.WithValues("cluster", req.ClusterName)
	}

	cluster, err := r.Manager.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get cluster %q: %w", req.ClusterName, err)
	}
	clusterClient := cluster.GetClient()

	var grant quotav1alpha1.QuotaGrant
	if err := clusterClient.Get(ctx, req.NamespacedName, &grant); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("QuotaGrant not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaGrant: %w", err)
	}

	// Determine whether the grant would become Active given its current spec,
	// then ensure buckets exist BEFORE writing Active=True. A fast claim
	// reaching the synchronous allocator between "Active=True" and
	// "bucket exists" would otherwise see a missing bucket and return a
	// misleading Forbidden.
	wouldBeActive := len(r.GrantValidator.Validate(ctx, &grant, validation.ControllerValidationOptions())) == 0
	if wouldBeActive {
		if err := r.ensureBucketsFromGrant(ctx, clusterClient, &grant); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to pre-create quota buckets: %w", err)
		}
	}

	if err := r.updateQuotaGrantStatus(ctx, clusterClient, &grant); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updateQuotaGrantStatus updates the status of the QuotaGrant.
func (r *QuotaGrantController) updateQuotaGrantStatus(ctx context.Context, clusterClient client.Client, grant *quotav1alpha1.QuotaGrant) error {
	logger := log.FromContext(ctx)
	originalStatus := grant.Status.DeepCopy()

	grant.Status.ObservedGeneration = grant.Generation

	if validationErrs := r.GrantValidator.Validate(ctx, grant, validation.ControllerValidationOptions()); len(validationErrs) > 0 {
		logger.Info("QuotaGrant validation failed", "errors", validationErrs.ToAggregate())

		apimeta.SetStatusCondition(&grant.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.QuotaGrantActive,
			Status:  metav1.ConditionFalse,
			Reason:  quotav1alpha1.QuotaGrantValidationFailedReason,
			Message: fmt.Sprintf("Validation failed: %v", validationErrs.ToAggregate()),
		})

		return r.updateStatusIfChanged(ctx, clusterClient, grant, originalStatus)
	}

	r.setActiveCondition(grant)

	return r.updateStatusIfChanged(ctx, clusterClient, grant, originalStatus)
}

// setActiveCondition sets the active condition on the grant.
func (r *QuotaGrantController) setActiveCondition(grant *quotav1alpha1.QuotaGrant) {
	condition := metav1.Condition{
		Type:               quotav1alpha1.QuotaGrantActive,
		Status:             metav1.ConditionTrue,
		Reason:             quotav1alpha1.QuotaGrantActiveReason,
		Message:            "QuotaGrant is active",
		ObservedGeneration: grant.Generation,
	}
	apimeta.SetStatusCondition(&grant.Status.Conditions, condition)
}

// updateStatusIfChanged updates the status only if it has actually changed.
func (r *QuotaGrantController) updateStatusIfChanged(ctx context.Context, clusterClient client.Client, grant *quotav1alpha1.QuotaGrant, originalStatus *quotav1alpha1.QuotaGrantStatus) error {
	if equality.Semantic.DeepEqual(&grant.Status, originalStatus) {
		return nil
	}

	if err := clusterClient.Status().Update(ctx, grant); err != nil {
		return fmt.Errorf("failed to update QuotaGrant status: %w", err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *QuotaGrantController) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaGrant{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
		).
		Named("quota-grant").
		Complete(r)
}

// ensureBucketsFromGrant pre-creates dimensionless quota buckets when a grant becomes active.
func (r *QuotaGrantController) ensureBucketsFromGrant(ctx context.Context, clusterClient client.Client, grant *quotav1alpha1.QuotaGrant) error {
	logger := log.FromContext(ctx).WithValues(
		"grant", grant.Name,
		"grantNamespace", grant.Namespace,
		"consumerKind", grant.Spec.ConsumerRef.Kind,
		"consumerName", grant.Spec.ConsumerRef.Name,
	)

	logger.Info("Pre-creating QuotaBuckets from active grant",
		"allowanceCount", len(grant.Spec.Allowances))

	for _, allowance := range grant.Spec.Allowances {
		bucketName := generateQuotaBucketName(allowance.ResourceType, grant.Spec.ConsumerRef)
		bucketNamespace := getBucketNamespace(grant.Spec.ConsumerRef, r.BucketNamespace)

		logger.Info("Checking if bucket needs pre-creation",
			"bucket", bucketName,
			"namespace", bucketNamespace,
			"resourceType", allowance.ResourceType)

		var existingBucket quotav1alpha1.QuotaBucket
		err := clusterClient.Get(ctx, types.NamespacedName{
			Name:      bucketName,
			Namespace: bucketNamespace,
		}, &existingBucket)

		if err == nil {
			logger.V(2).Info("Bucket already exists, skipping pre-creation",
				"bucket", bucketName,
				"namespace", bucketNamespace)
			continue
		}

		if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to check bucket existence",
				"bucket", bucketName,
				"namespace", bucketNamespace)
			return fmt.Errorf("failed to check bucket existence: %w", err)
		}

		bucket := &quotav1alpha1.QuotaBucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bucketName,
				Namespace: bucketNamespace,
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

		logger.Info("Creating pre-created QuotaBucket",
			"bucket", bucketName,
			"namespace", bucketNamespace,
			"resourceType", allowance.ResourceType)

		if err := clusterClient.Create(ctx, bucket); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				logger.Error(err, "Failed to create bucket",
					"bucket", bucketName,
					"namespace", bucketNamespace)
				return fmt.Errorf("failed to pre-create bucket %s: %w", bucketName, err)
			}
			logger.Info("Bucket already exists (race condition)",
				"bucket", bucketName,
				"namespace", bucketNamespace)
		} else {
			logger.Info("Successfully pre-created dimensionless QuotaBucket",
				"bucket", bucketName,
				"namespace", bucketNamespace,
				"resourceType", allowance.ResourceType,
				"consumer", grant.Spec.ConsumerRef.Name)
		}
	}

	return nil
}
