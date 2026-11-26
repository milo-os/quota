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
	"k8s.io/klog/v2"
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

// ClaimCreationPolicyReconciler reconciles a ClaimCreationPolicy object.
type ClaimCreationPolicyReconciler struct {
	Scheme  *runtime.Scheme
	Manager mcmanager.Manager
	// PolicyValidator validates ClaimCreationPolicy resources.
	PolicyValidator *validation.ClaimCreationPolicyValidator
}

// +kubebuilder:rbac:groups=quota.miloapis.com,resources=claimcreationpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=claimcreationpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotadefinitions,verbs=get;list;watch

// Reconcile reconciles a ClaimCreationPolicy object by validating it and setting its Ready status.
func (r *ClaimCreationPolicyReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
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

	var policy quotav1alpha1.ClaimCreationPolicy
	logger.V(1).Info("Reconciling ClaimCreationPolicy", "name", req.Name)

	if err := clusterClient.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("ClaimCreationPolicy was deleted", "name", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get ClaimCreationPolicy: %w", err)
	}

	originalStatus := policy.Status.DeepCopy()

	validationErrs := r.PolicyValidator.Validate(ctx, &policy, validation.ControllerValidationOptions())

	r.updatePolicyStatus(&policy, validationErrs)

	policy.Status.ObservedGeneration = policy.Generation

	if !equality.Semantic.DeepEqual(&policy.Status, originalStatus) {
		if err := clusterClient.Status().Update(ctx, &policy); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update ClaimCreationPolicy status: %w", err)
		}
		logger.V(1).Info("Updated ClaimCreationPolicy status",
			"policy", policy.Name,
			"ready", apimeta.IsStatusConditionTrue(policy.Status.Conditions, quotav1alpha1.ClaimCreationPolicyReady))
	}

	return ctrl.Result{}, nil
}

// updatePolicyStatus updates the policy status conditions based on validation results.
func (r *ClaimCreationPolicyReconciler) updatePolicyStatus(policy *quotav1alpha1.ClaimCreationPolicy, validationErrs field.ErrorList) {
	if len(validationErrs) > 0 {
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.ClaimCreationPolicyReady,
			Status:  metav1.ConditionFalse,
			Reason:  quotav1alpha1.ClaimCreationPolicyValidationFailedReason,
			Message: validationErrs.ToAggregate().Error(),
		})
		return
	}

	if policy.Spec.Disabled != nil && *policy.Spec.Disabled {
		apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:    quotav1alpha1.ClaimCreationPolicyReady,
			Status:  metav1.ConditionFalse,
			Reason:  quotav1alpha1.ClaimCreationPolicyDisabledReason,
			Message: "Policy is disabled",
		})
		return
	}

	apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    quotav1alpha1.ClaimCreationPolicyReady,
		Status:  metav1.ConditionTrue,
		Reason:  quotav1alpha1.ClaimCreationPolicyReadyReason,
		Message: "Policy is ready and active",
	})
}

// enqueueAffectedPolicies finds all ClaimCreationPolicies that reference a QuotaDefinition.
func (r *ClaimCreationPolicyReconciler) enqueueAffectedPolicies(ctx context.Context, obj client.Object) []mcreconcile.Request {
	definition, ok := obj.(*quotav1alpha1.QuotaDefinition)
	if !ok {
		return nil
	}

	clusterName, _ := mccontext.ClusterFrom(ctx)

	cluster, err := r.Manager.GetCluster(ctx, clusterName)
	if err != nil {
		klog.V(1).ErrorS(err, "failed to get cluster client when enqueuing affected policies", "clusterName", clusterName)
		return nil
	}
	clusterClient := cluster.GetClient()

	var policyList quotav1alpha1.ClaimCreationPolicyList
	if err := clusterClient.List(ctx, &policyList); err != nil {
		klog.V(1).ErrorS(err, "failed to list claim creation policies when enqueuing affected policies")
		return nil
	}

	var requests []mcreconcile.Request
	for _, policy := range policyList.Items {
		for _, request := range policy.Spec.Target.ResourceClaimTemplate.Spec.Requests {
			if request.ResourceType == definition.Spec.ResourceType {
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
func (r *ClaimCreationPolicyReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.ClaimCreationPolicy{},
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
		Named("claim-creation-policy").
		Complete(r)
}
