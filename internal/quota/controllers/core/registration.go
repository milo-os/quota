// Package core implements the core quota controllers that manage QuotaBuckets,
// QuotaClaims, QuotaGrants, and QuotaDefinitions.
//
// The QuotaDefinitionController validates QuotaDefinitions and manages
// their Active status condition.
package core

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// QuotaDefinitionController reconciles QuotaDefinition objects.
type QuotaDefinitionController struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotadefinitions,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotadefinitions/status,verbs=get;update;patch

// Reconcile reconciles a QuotaDefinition object by validating it and updating the
// status to reflect whether the definition is active.
func (r *QuotaDefinitionController) Reconcile(ctx context.Context, req mcreconcile.Request) (_ ctrl.Result, err error) {
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

	var definition quotav1alpha1.QuotaDefinition
	if err := clusterClient.Get(ctx, req.NamespacedName, &definition); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("QuotaDefinition not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get QuotaDefinition: %w", err)
	}

	if !definition.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.updateDefinitionStatus(ctx, clusterClient, &definition)
}

// updateDefinitionStatus validates the definition and updates its status.
func (r *QuotaDefinitionController) updateDefinitionStatus(ctx context.Context, clusterClient client.Client, definition *quotav1alpha1.QuotaDefinition) error {
	originalStatus := definition.Status.DeepCopy()

	definition.Status.ObservedGeneration = definition.Generation

	activeCondition := r.initializeActiveCondition(definition)
	r.setActiveCondition(activeCondition)

	apimeta.SetStatusCondition(&definition.Status.Conditions, *activeCondition)

	return r.updateStatusIfChanged(ctx, clusterClient, definition, originalStatus)
}

// initializeActiveCondition gets or creates the active condition.
func (r *QuotaDefinitionController) initializeActiveCondition(definition *quotav1alpha1.QuotaDefinition) *metav1.Condition {
	activeCondition := apimeta.FindStatusCondition(definition.Status.Conditions, quotav1alpha1.QuotaDefinitionActive)
	if activeCondition == nil {
		return &metav1.Condition{
			Type:               quotav1alpha1.QuotaDefinitionActive,
			Status:             metav1.ConditionFalse,
			Reason:             quotav1alpha1.QuotaDefinitionPendingReason,
			Message:            "The definition is pending validation",
			ObservedGeneration: definition.Generation,
		}
	}

	activeCondition = activeCondition.DeepCopy()
	activeCondition.ObservedGeneration = definition.Generation
	return activeCondition
}

// setActiveCondition sets the condition to reflect successful validation.
func (r *QuotaDefinitionController) setActiveCondition(condition *metav1.Condition) {
	condition.Status = metav1.ConditionTrue
	condition.Reason = quotav1alpha1.QuotaDefinitionActiveReason
	condition.Message = "The registration is active and resource grants and claims can now be created for this resource type."
}

// updateStatusIfChanged updates the status only if it has changed.
func (r *QuotaDefinitionController) updateStatusIfChanged(ctx context.Context, clusterClient client.Client, definition *quotav1alpha1.QuotaDefinition, originalStatus *quotav1alpha1.QuotaDefinitionStatus) error {
	if !equality.Semantic.DeepEqual(originalStatus, &definition.Status) {
		if err := clusterClient.Status().Update(ctx, definition); err != nil {
			return fmt.Errorf("failed to update QuotaDefinition status: %w", err)
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *QuotaDefinitionController) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.QuotaDefinition{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Named("quota-definition").
		Complete(r)
}
