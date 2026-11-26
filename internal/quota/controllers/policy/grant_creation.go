// Package policy implements controllers for quota policy management.
package policy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mccontext "sigs.k8s.io/multicluster-runtime/pkg/context"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/quota/validation"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// GrantCreationPolicyReconciler reconciles a GrantCreationPolicy object.
type GrantCreationPolicyReconciler struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager
	// PolicyValidator validates GrantCreationPolicy resources.
	PolicyValidator *validation.GrantCreationPolicyValidator
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=grantcreationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=grantcreationpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotadefinitions,verbs=get;list;watch

// Reconcile reconciles a GrantCreationPolicy object by validating it and setting its Ready status.
func (r *GrantCreationPolicyReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
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

	var policy quotav1alpha1.GrantCreationPolicy
	logger.V(1).Info("Reconciling GrantCreationPolicy", "name", req.Name)

	if err := clusterClient.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("GrantCreationPolicy was deleted", "name", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get GrantCreationPolicy: %w", err)
	}

	originalStatus := policy.Status.DeepCopy()

	validationErrs := r.PolicyValidator.Validate(ctx, &policy, validation.ControllerValidationOptions())

	r.updatePolicyStatus(&policy, validationErrs)

	policy.Status.ObservedGeneration = policy.Generation

	if !equality.Semantic.DeepEqual(&policy.Status, originalStatus) {
		if err := clusterClient.Status().Update(ctx, &policy); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update GrantCreationPolicy status: %w", err)
		}
		logger.V(1).Info("Updated GrantCreationPolicy status",
			"policy", policy.Name,
			"ready", apimeta.IsStatusConditionTrue(policy.Status.Conditions, quotav1alpha1.GrantCreationPolicyReady))
	}

	return ctrl.Result{}, nil
}

// updatePolicyStatus updates the policy status conditions based on validation results.
func (r *GrantCreationPolicyReconciler) updatePolicyStatus(policy *quotav1alpha1.GrantCreationPolicy, validationErrs field.ErrorList) {
	if len(validationErrs) > 0 {
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.GrantCreationPolicyReady,
			Status:  metav1.ConditionFalse,
			Reason:  quotav1alpha1.GrantCreationPolicyValidationFailedReason,
			Message: validationErrs.ToAggregate().Error(),
		})

		apimeta.RemoveStatusCondition(&policy.Status.Conditions, quotav1alpha1.GrantCreationPolicyParentContextReady)
		return
	}

	if policy.Spec.Disabled != nil && *policy.Spec.Disabled {
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.GrantCreationPolicyReady,
			Status:  metav1.ConditionFalse,
			Reason:  quotav1alpha1.GrantCreationPolicyDisabledReason,
			Message: "Policy is disabled",
		})

		apimeta.RemoveStatusCondition(&policy.Status.Conditions, quotav1alpha1.GrantCreationPolicyParentContextReady)
		return
	}

	apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    quotav1alpha1.GrantCreationPolicyReady,
		Status:  metav1.ConditionTrue,
		Reason:  quotav1alpha1.GrantCreationPolicyReadyReason,
		Message: "Policy is ready and active",
	})

	if policy.Spec.Target.ParentContext != nil {
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.GrantCreationPolicyParentContextReady,
			Status:  metav1.ConditionTrue,
			Reason:  quotav1alpha1.GrantCreationPolicyParentContextReadyReason,
			Message: "Parent context resolution is ready",
		})
	} else {
		apimeta.RemoveStatusCondition(&policy.Status.Conditions, quotav1alpha1.GrantCreationPolicyParentContextReady)
	}
}

// enqueueAffectedPolicies finds all GrantCreationPolicies that reference a QuotaDefinition.
func (r *GrantCreationPolicyReconciler) enqueueAffectedPolicies(ctx context.Context, obj client.Object) []mcreconcile.Request {
	definition, ok := obj.(*quotav1alpha1.QuotaDefinition)
	if !ok {
		return nil
	}

	clusterName, _ := mccontext.ClusterFrom(ctx)

	cluster, err := r.Manager.GetCluster(ctx, clusterName)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get cluster client when enqueuing affected policies", "clusterName", clusterName)
		return nil
	}
	clusterClient := cluster.GetClient()

	var policyList quotav1alpha1.GrantCreationPolicyList
	if err := clusterClient.List(ctx, &policyList); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list GrantCreationPolicies for QuotaDefinition change",
			"definition", definition.Name)
		return nil
	}

	var requests []mcreconcile.Request
	for _, policy := range policyList.Items {
		for _, allowance := range policy.Spec.Target.ResourceGrantTemplate.Spec.Allowances {
			if allowance.ResourceType == definition.Spec.ResourceType {
				requests = append(requests, mcreconcile.Request{
					Request: ctrl.Request{
						NamespacedName: client.ObjectKeyFromObject(&policy),
					},
				})
				break
			}
		}
	}

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrantCreationPolicyReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.GrantCreationPolicy{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Watches(
			&quotav1alpha1.QuotaDefinition{},
			mchandler.TypedEnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []mcreconcile.Request {
					return r.enqueueAffectedPolicies(ctx, obj)
				},
			),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true),
		).
		Named("grant-creation-policy").
		Complete(r)
}
