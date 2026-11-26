// Package policy implements controllers for quota policy management.
package policy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/internal/quota/informer"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// GrantCreationController watches trigger resources and creates grants based on active policies.
type GrantCreationController struct {
	Scheme                *runtime.Scheme
	Manager               mcmanager.Manager
	TemplateEngine        engine.TemplateEngine
	CELEngine             engine.CELEngine
	ParentContextResolver *ParentContextResolver
	EventRecorder         events.EventRecorder

	informerManager informer.Manager
	logger          logr.Logger

	// watched records the trigger kind each policy's watch was added for,
	// so the watch can be removed after the policy is deleted or retargeted.
	watchedMu sync.Mutex
	watched   map[string]schema.GroupVersionKind
}

// Labels stamped on every grant the controller creates. They identify the
// grant's policy and trigger so it is deleted when the trigger goes away,
// including grants in a parent context, which cannot carry an owner
// reference to the trigger.
const (
	grantPolicyLabel     = "quota.miloapis.com/policy"
	grantTriggerUIDLabel = "quota.miloapis.com/trigger-uid"
)

// grantCreationHandler implements informer.ResourceEventHandler for grant creation.
type grantCreationHandler struct {
	controller *GrantCreationController
	policyName string
}

// OnAdd implements informer.ResourceEventHandler.
func (h *grantCreationHandler) OnAdd(obj *unstructured.Unstructured) {
	h.controller.processTriggerResource(obj, h.policyName, "ADD")
}

// OnUpdate implements informer.ResourceEventHandler.
func (h *grantCreationHandler) OnUpdate(old, new *unstructured.Unstructured) {
	h.controller.processTriggerResource(new, h.policyName, "UPDATE")
}

// OnDelete implements informer.ResourceEventHandler.
func (h *grantCreationHandler) OnDelete(obj *unstructured.Unstructured) {
	h.controller.processTriggerResource(obj, h.policyName, "DELETE")
}

// NewGrantCreationController creates a new GrantCreationController.
func NewGrantCreationController(
	manager mcmanager.Manager,
	scheme *runtime.Scheme,
	templateEngine engine.TemplateEngine,
	celEngine engine.CELEngine,
	parentContextResolver *ParentContextResolver,
	eventRecorder events.EventRecorder,
	informerManager informer.Manager,
) *GrantCreationController {
	logger := ctrl.Log.WithName("grant-creation")

	return &GrantCreationController{
		Manager:               manager,
		Scheme:                scheme,
		TemplateEngine:        templateEngine,
		CELEngine:             celEngine,
		ParentContextResolver: parentContextResolver,
		EventRecorder:         eventRecorder,
		informerManager:       informerManager,
		logger:                logger,
		watched:               map[string]schema.GroupVersionKind{},
	}
}

// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotagrants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=grantcreationpolicies,verbs=get;list;watch

// processTriggerResource processes a trigger resource event.
func (r *GrantCreationController) processTriggerResource(obj *unstructured.Unstructured, policyName, eventType string) {
	ctx := context.Background()
	logger := r.logger.WithValues(
		"triggerResource", obj.GetName(),
		"triggerKind", obj.GetKind(),
		"namespace", obj.GetNamespace(),
		"policy", policyName,
		"eventType", eventType,
	)

	policy, err := r.getPolicyByName(ctx, policyName)
	if err != nil {
		logger.Error(err, "Failed to get policy")
		return
	}

	if policy == nil {
		logger.V(2).Info("Policy not found, removing watch")
		if err := r.removeWatchForPolicy(ctx, policyName); err != nil {
			logger.Error(err, "Failed to remove watch for deleted policy")
		}
		return
	}

	if eventType == "DELETE" {
		// The trigger is gone: delete the grant it produced. Grants in the
		// local cluster are also owned by the trigger, but grants in a
		// parent context have no owner reference and are only removed here.
		if err := r.cleanupGrant(ctx, policy, obj); err != nil {
			logger.Error(err, "Failed to delete grant of deleted trigger")
		}
		return
	}

	logger.Info("Processing trigger resource for grant creation")

	if err := r.processPolicy(ctx, policy, obj); err != nil {
		logger.Error(err, "Failed to process policy")
		r.EventRecorder.Eventf(obj, nil, "Warning", "PolicyProcessingFailed", "ProcessPolicy",
			"Failed to process grant creation policy %s: %v", policy.Name, err)
	}
}

// Reconcile handles GrantCreationPolicy changes.
func (r *GrantCreationController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("policyName", req.Name)
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
	if err := clusterClient.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Policy deleted, removing watch")

			if err := r.removeWatchForPolicy(ctx, req.Name); err != nil {
				logger.Error(err, "Failed to remove watch for deleted policy")
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	isReady := r.isPolicyReady(&policy)
	logger.V(1).Info("Policy reconciled", "ready", isReady)

	if isReady {
		if err := r.addWatchForPolicy(ctx, &policy); err != nil {
			logger.Error(err, "Failed to add watch for policy")
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
	} else {
		if err := r.removeWatchForPolicy(ctx, req.Name); err != nil {
			logger.Error(err, "Failed to remove watch for not-ready policy")
		}
	}

	logger.Info("Successfully processed policy change")
	return ctrl.Result{}, nil
}

// isPolicyReady checks if a GrantCreationPolicy has Ready=True status condition.
func (r *GrantCreationController) isPolicyReady(policy *quotav1alpha1.GrantCreationPolicy) bool {
	for _, condition := range policy.Status.Conditions {
		if condition.Type == quotav1alpha1.GrantCreationPolicyReady && condition.Status == "True" {
			return true
		}
	}
	return false
}

// addWatchForPolicy adds a dynamic watch for a policy's trigger resource,
// first removing the watch on the previous target if the policy was
// retargeted.
func (r *GrantCreationController) addWatchForPolicy(ctx context.Context, policy *quotav1alpha1.GrantCreationPolicy) error {
	gvk := policy.Spec.Trigger.Resource.GetGVK()
	consumerID := fmt.Sprintf("grant-creation-policy-%s", policy.Name)

	r.watchedMu.Lock()
	previous, had := r.watched[policy.Name]
	r.watchedMu.Unlock()
	if had && previous != gvk {
		if err := r.informerManager.RemoveWatch(ctx, previous, consumerID); err != nil {
			return fmt.Errorf("failed to remove watch on previous target %s: %w", previous, err)
		}
	}

	handler := &grantCreationHandler{
		controller: r,
		policyName: policy.Name,
	}

	req := informer.WatchRequest{
		GVK:        gvk,
		ConsumerID: consumerID,
		Handler:    handler,
	}

	if err := r.informerManager.AddWatch(ctx, req); err != nil {
		return err
	}
	r.watchedMu.Lock()
	r.watched[policy.Name] = gvk
	r.watchedMu.Unlock()
	return nil
}

// removeWatchForPolicy removes the dynamic watch added for a policy. It works
// after the policy is deleted: the watched kind is remembered, not re-read.
func (r *GrantCreationController) removeWatchForPolicy(ctx context.Context, policyName string) error {
	r.watchedMu.Lock()
	gvk, ok := r.watched[policyName]
	r.watchedMu.Unlock()
	if !ok {
		return nil
	}
	consumerID := fmt.Sprintf("grant-creation-policy-%s", policyName)
	if err := r.informerManager.RemoveWatch(ctx, gvk, consumerID); err != nil {
		return err
	}
	r.watchedMu.Lock()
	if r.watched[policyName] == gvk {
		delete(r.watched, policyName)
	}
	r.watchedMu.Unlock()
	return nil
}

// getPolicyByName retrieves a GrantCreationPolicy by name.
func (r *GrantCreationController) getPolicyByName(ctx context.Context, name string) (*quotav1alpha1.GrantCreationPolicy, error) {
	policy := &quotav1alpha1.GrantCreationPolicy{}
	key := client.ObjectKey{Name: name}

	clusterClient := r.Manager.GetLocalManager().GetClient()

	if err := clusterClient.Get(ctx, key, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return policy, nil
}

// processPolicy processes a single policy against a trigger resource.
func (r *GrantCreationController) processPolicy(
	ctx context.Context,
	policy *quotav1alpha1.GrantCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx).WithValues("policy", policy.Name)

	conditionsMet, err := r.TemplateEngine.EvaluateConditions(ctx, policy.Spec.Trigger.Constraints, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to evaluate conditions: %w", err)
	}

	if !conditionsMet {
		logger.V(2).Info("Trigger conditions not met, skipping grant creation")
		return r.cleanupGrant(ctx, policy, triggerObj)
	}

	logger.Info("Trigger conditions met, creating/updating grant")

	targetClient, err := r.resolveTargetClient(ctx, policy, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to resolve target client: %w", err)
	}

	grant, err := r.TemplateEngine.RenderGrant(policy, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to render grant: %w", err)
	}
	stampGrantLabels(grant, policy, triggerObj)

	if err := r.createOrUpdateGrant(ctx, targetClient, grant, policy, triggerObj); err != nil {
		return fmt.Errorf("failed to create/update grant: %w", err)
	}

	logger.Info("Successfully processed policy", "grantName", grant.Name, "grantNamespace", grant.Namespace)
	return nil
}

// resolveTargetClient determines the target client for grant creation.
func (r *GrantCreationController) resolveTargetClient(
	ctx context.Context,
	policy *quotav1alpha1.GrantCreationPolicy,
	triggerObj *unstructured.Unstructured,
) (client.Client, error) {
	if policy.Spec.Target.ParentContext == nil {
		return r.Manager.GetLocalManager().GetClient(), nil
	}

	variables := map[string]interface{}{
		"trigger": triggerObj.Object,
	}
	parentContextName, err := r.CELEngine.EvaluateTemplateExpression(
		policy.Spec.Target.ParentContext.NameExpression,
		variables,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate parent context name: %w", err)
	}

	parentContext := policy.Spec.Target.ParentContext
	targetClient, err := r.ParentContextResolver.ResolveClient(ctx, &ParentContextSpec{
		APIGroup: parentContext.APIGroup,
		Kind:     parentContext.Kind,
		Name:     parentContextName,
	}, triggerObj)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve parent context client: %w", err)
	}

	return targetClient, nil
}

// createOrUpdateGrant creates or updates a QuotaGrant.
func (r *GrantCreationController) createOrUpdateGrant(
	ctx context.Context,
	targetClient client.Client,
	grant *quotav1alpha1.QuotaGrant,
	policy *quotav1alpha1.GrantCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx).WithValues("grantName", grant.Name, "grantNamespace", grant.Namespace)

	existingGrant := &quotav1alpha1.QuotaGrant{}
	err := targetClient.Get(ctx, client.ObjectKey{
		Name:      grant.Name,
		Namespace: grant.Namespace,
	}, existingGrant)

	if err != nil {
		if apierrors.IsNotFound(err) {
			// Owner references are only valid within the same cluster.
			if policy.Spec.Target.ParentContext == nil {
				if err := controllerutil.SetControllerReference(triggerObj, grant, r.Scheme); err != nil {
					return fmt.Errorf("failed to set owner reference: %w", err)
				}
			}
			logger.Info("Creating new QuotaGrant")
			if err := targetClient.Create(ctx, grant); err != nil {
				return fmt.Errorf("failed to create grant: %w", err)
			}

			r.EventRecorder.Eventf(triggerObj, nil, "Normal", "GrantCreated", "CreateGrant",
				"Created QuotaGrant %s/%s from policy %s", grant.Namespace, grant.Name, policy.Name)

			return nil
		}
		return fmt.Errorf("failed to check existing grant: %w", err)
	}

	original := existingGrant.DeepCopy()

	existingGrant.Spec = grant.Spec
	existingGrant.Labels = grant.Labels
	existingGrant.Annotations = grant.Annotations

	if equality.Semantic.DeepEqual(existingGrant, original) {
		logger.V(1).Info("QuotaGrant unchanged, skipping update")
		return nil
	}

	logger.Info("Updating existing QuotaGrant")
	if err := targetClient.Update(ctx, existingGrant); err != nil {
		return fmt.Errorf("failed to update grant: %w", err)
	}

	r.EventRecorder.Eventf(triggerObj, nil, "Normal", "GrantUpdated", "UpdateGrant",
		"Updated QuotaGrant %s/%s from policy %s", grant.Namespace, grant.Name, policy.Name)

	return nil
}

// cleanupGrant removes a grant if conditions are no longer met.
func (r *GrantCreationController) cleanupGrant(
	ctx context.Context,
	policy *quotav1alpha1.GrantCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx).WithValues("policy", policy.Name)

	targetClient, err := r.resolveTargetClient(ctx, policy, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to resolve target client: %w", err)
	}

	grant, err := r.TemplateEngine.RenderGrant(policy, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to render grant for cleanup: %w", err)
	}

	existingGrant := &quotav1alpha1.QuotaGrant{}
	err = targetClient.Get(ctx, client.ObjectKey{
		Name:      grant.Name,
		Namespace: grant.Namespace,
	}, existingGrant)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to check existing grant: %w", err)
	}

	if !grantBelongsTo(existingGrant, policy, triggerObj) {
		return nil
	}
	logger.Info("Deleting grant: trigger deleted or no longer matches", "grantName", existingGrant.Name)
	if err := targetClient.Delete(ctx, existingGrant); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete grant: %w", err)
	}
	r.EventRecorder.Eventf(triggerObj, nil, "Normal", "GrantDeleted", "DeleteGrant",
		"Deleted QuotaGrant %s/%s from policy %s", existingGrant.Namespace, existingGrant.Name, policy.Name)
	return nil
}

// stampGrantLabels marks a rendered grant with its policy and trigger.
func stampGrantLabels(grant *quotav1alpha1.QuotaGrant, policy *quotav1alpha1.GrantCreationPolicy, trigger *unstructured.Unstructured) {
	if grant.Labels == nil {
		grant.Labels = map[string]string{}
	}
	grant.Labels[grantPolicyLabel] = policy.Name
	if uid := trigger.GetUID(); uid != "" {
		grant.Labels[grantTriggerUIDLabel] = string(uid)
	}
}

// grantBelongsTo reports whether an existing grant was produced by this
// policy for this trigger object. A grant stamped with another trigger's UID
// (a different object that reused the name) is left alone.
func grantBelongsTo(grant *quotav1alpha1.QuotaGrant, policy *quotav1alpha1.GrantCreationPolicy, trigger *unstructured.Unstructured) bool {
	if grant.Labels[grantPolicyLabel] != policy.Name {
		return false
	}
	uid := grant.Labels[grantTriggerUIDLabel]
	return uid == "" || trigger.GetUID() == "" || uid == string(trigger.GetUID())
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrantCreationController) SetupWithManager(mgr mcmanager.Manager) error {
	r.logger.Info("Setting up GrantCreationController")

	controller := mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.GrantCreationPolicy{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Named("grant-creation-controller")

	r.logger.Info("GrantCreationController setup completed successfully")

	return controller.Complete(r)
}

// ParentContextSpec is a simplified version for the resolver.
type ParentContextSpec struct {
	// APIGroup is the API group of the parent context resource.
	APIGroup string
	// Kind is the kind of the parent context resource.
	Kind string
	// Name is the resolved name of the parent context resource.
	Name string
}
