// Package policy implements controllers for quota policy management.
package policy

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/internal/quota/informer"
	"go.miloapis.com/quota/pkg/admission/enforcement"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// ClaimCreationController watches trigger resources and creates claims based on active ClaimCreationPolicies.
type ClaimCreationController struct {
	Scheme         *runtime.Scheme
	Manager        mcmanager.Manager
	TemplateEngine engine.TemplateEngine
	CELEngine      engine.CELEngine
	EventRecorder  record.EventRecorder

	informerManager informer.Manager
	logger          logr.Logger

	// watched records the trigger kind each policy's watch was added for,
	// so the watch can be removed after the policy is deleted or retargeted.
	watchedMu sync.Mutex
	watched   map[string]schema.GroupVersionKind
}

// claimCreationHandler implements informer.ResourceEventHandler for claim creation.
type claimCreationHandler struct {
	controller *ClaimCreationController
	policyName string
}

// OnAdd implements informer.ResourceEventHandler.
func (h *claimCreationHandler) OnAdd(obj *unstructured.Unstructured) {
	h.controller.processTriggerResource(obj, h.policyName, "ADD")
}

// OnUpdate implements informer.ResourceEventHandler.
func (h *claimCreationHandler) OnUpdate(old, new *unstructured.Unstructured) {
	h.controller.processTriggerResource(new, h.policyName, "UPDATE")
}

// OnDelete implements informer.ResourceEventHandler.
func (h *claimCreationHandler) OnDelete(obj *unstructured.Unstructured) {
	h.controller.processTriggerResource(obj, h.policyName, "DELETE")
}

// NewClaimCreationController creates a new ClaimCreationController.
func NewClaimCreationController(
	manager mcmanager.Manager,
	scheme *runtime.Scheme,
	templateEngine engine.TemplateEngine,
	celEngine engine.CELEngine,
	eventRecorder record.EventRecorder,
	informerManager informer.Manager,
) *ClaimCreationController {
	logger := ctrl.Log.WithName("claim-creation")

	return &ClaimCreationController{
		Manager:         manager,
		Scheme:          scheme,
		TemplateEngine:  templateEngine,
		CELEngine:       celEngine,
		EventRecorder:   eventRecorder,
		informerManager: informerManager,
		logger:          logger,
		watched:         map[string]schema.GroupVersionKind{},
	}
}

// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=quotaclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=quota.miloapis.com,resources=claimcreationpolicies,verbs=get;list;watch

// processTriggerResource processes a trigger resource event for claim creation.
func (r *ClaimCreationController) processTriggerResource(obj *unstructured.Unstructured, policyName, eventType string) {
	ctx := context.Background()
	logger := r.logger.WithValues(
		"triggerResource", obj.GetName(),
		"triggerKind", obj.GetKind(),
		"namespace", obj.GetNamespace(),
		"policy", policyName,
		"eventType", eventType,
	)

	if eventType == "DELETE" {
		logger.V(2).Info("Skipping DELETE event")
		return
	}

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

	logger.Info("Processing trigger resource for claim creation")

	if err := r.processPolicy(ctx, policy, obj); err != nil {
		logger.Error(err, "Failed to process policy")
		r.EventRecorder.Eventf(obj, "Warning", "PolicyProcessingFailed",
			"Failed to process claim creation policy %s: %v", policy.Name, err)
	}
}

// Reconcile handles ClaimCreationPolicy changes.
func (r *ClaimCreationController) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
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

	var policy quotav1alpha1.ClaimCreationPolicy
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

// isPolicyReady checks if a ClaimCreationPolicy has Ready=True status condition.
func (r *ClaimCreationController) isPolicyReady(policy *quotav1alpha1.ClaimCreationPolicy) bool {
	for _, condition := range policy.Status.Conditions {
		if condition.Type == quotav1alpha1.ClaimCreationPolicyReady && condition.Status == "True" {
			return true
		}
	}
	return false
}

// addWatchForPolicy adds a dynamic watch for a policy's trigger resource.
func (r *ClaimCreationController) addWatchForPolicy(ctx context.Context, policy *quotav1alpha1.ClaimCreationPolicy) error {
	gvk := policy.Spec.Trigger.Resource.GetGVK()
	consumerID := fmt.Sprintf("claim-creation-policy-%s", policy.Name)

	r.watchedMu.Lock()
	previous, had := r.watched[policy.Name]
	r.watchedMu.Unlock()
	if had && previous != gvk {
		if err := r.informerManager.RemoveWatch(ctx, previous, consumerID); err != nil {
			return fmt.Errorf("failed to remove watch on previous target %s: %w", previous, err)
		}
	}

	handler := &claimCreationHandler{
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
func (r *ClaimCreationController) removeWatchForPolicy(ctx context.Context, policyName string) error {
	r.watchedMu.Lock()
	gvk, ok := r.watched[policyName]
	r.watchedMu.Unlock()
	if !ok {
		return nil
	}
	consumerID := fmt.Sprintf("claim-creation-policy-%s", policyName)
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

// getPolicyByName retrieves a ClaimCreationPolicy by name.
func (r *ClaimCreationController) getPolicyByName(ctx context.Context, name string) (*quotav1alpha1.ClaimCreationPolicy, error) {
	policy := &quotav1alpha1.ClaimCreationPolicy{}
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

// processPolicy processes a single ClaimCreationPolicy against a trigger resource.
func (r *ClaimCreationController) processPolicy(
	ctx context.Context,
	policy *quotav1alpha1.ClaimCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx).WithValues("policy", policy.Name)

	conditionsMet, err := r.TemplateEngine.EvaluateConditions(ctx, policy.Spec.Trigger.Constraints, triggerObj)
	if err != nil {
		return fmt.Errorf("failed to evaluate conditions: %w", err)
	}

	if !conditionsMet {
		logger.V(2).Info("Trigger conditions not met, skipping claim creation")
		return r.cleanupClaim(ctx, policy, triggerObj)
	}

	logger.Info("Trigger conditions met, creating/updating claim")

	// Build an EvaluationContext from the trigger object. In the controller
	// path there is no real admission request, so we synthesise a
	// RequestInfo from the trigger object's metadata and GVK so that
	// templates using {{requestInfo.namespace}} etc. work correctly.
	gvk := triggerObj.GroupVersionKind()
	evalContext := &engine.EvaluationContext{
		Object:    triggerObj,
		Namespace: triggerObj.GetNamespace(),
		RequestInfo: &request.RequestInfo{
			IsResourceRequest: true,
			Verb:              "create",
			Namespace:         triggerObj.GetNamespace(),
			Name:              triggerObj.GetName(),
			Resource:          gvkToResource(gvk),
			APIGroup:          gvk.Group,
			APIVersion:        gvk.Version,
		},
		GVK: struct {
			Group   string
			Version string
			Kind    string
		}{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind,
		},
	}

	claim, err := r.TemplateEngine.RenderClaim(policy, evalContext)
	if err != nil {
		return fmt.Errorf("failed to render claim: %w", err)
	}

	// Populate resourceRef from the trigger object. The template validation
	// rule forbids setting resourceRef in the template; the executor fills
	// it automatically so the ownership controller can track the trigger.
	claim.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{
		APIGroup:  triggerObj.GetObjectKind().GroupVersionKind().Group,
		Kind:      triggerObj.GetObjectKind().GroupVersionKind().Kind,
		Name:      triggerObj.GetName(),
		Namespace: triggerObj.GetNamespace(),
	}

	localManager := r.Manager.GetLocalManager()
	if err := r.createOrUpdateClaim(ctx, localManager.GetClient(), localManager.GetAPIReader(), claim, policy, triggerObj); err != nil {
		return fmt.Errorf("failed to create/update claim: %w", err)
	}

	logger.Info("Successfully processed policy", "claimName", claim.Name, "claimNamespace", claim.Namespace)
	return nil
}

// createOrUpdateClaim creates the trigger's QuotaClaim unless one already
// exists for it.
//
// Admission (the enforcement plugin) normally creates the claim before the
// trigger object is persisted, so by the time this controller sees the
// object its claim usually exists. Creating another would charge the same
// object twice. A claim already exists when either:
//
//   - the policy pins a name and a claim has that name (admission uses the
//     same sanitized name, enforcement.ClaimName), or
//   - a claim from this policy points its resourceRef at the trigger. This
//     is the only check for generateName templates, whose names are random:
//     without it every ADD and UPDATE event created a new claim.
//
// The informer cache is asked first. A miss is confirmed with a live read
// because the claim and the trigger come from different watches: the
// trigger's ADD event can arrive before the cache has seen the claim that
// admission created moments earlier.
func (r *ClaimCreationController) createOrUpdateClaim(
	ctx context.Context,
	targetClient client.Client,
	liveReader client.Reader,
	claim *quotav1alpha1.QuotaClaim,
	policy *quotav1alpha1.ClaimCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	if claim.Name != "" {
		claim.Name = enforcement.ClaimName(claim.Name)
		claim.GenerateName = ""
	}
	logger := log.FromContext(ctx).WithValues("claimName", claim.Name, "claimNamespace", claim.Namespace)

	// Add policy label for tracking
	if claim.Labels == nil {
		claim.Labels = make(map[string]string)
	}
	claim.Labels[policyLabel] = policy.Name

	for _, reader := range []client.Reader{targetClient, liveReader} {
		if reader == nil {
			continue
		}
		existing, err := findTriggerClaim(ctx, reader, claim, policy)
		if err != nil {
			return fmt.Errorf("failed to check existing claim: %w", err)
		}
		if existing != "" {
			logger.V(1).Info("QuotaClaim already exists for trigger, skipping create", "existingClaim", existing)
			return nil
		}
	}

	// Set owner reference so claim is cleaned up when trigger resource is deleted.
	// Owner references are only valid within the same namespace.
	if triggerObj.GetNamespace() == claim.Namespace {
		if err := controllerutil.SetControllerReference(triggerObj, claim, r.Scheme); err != nil {
			logger.V(1).Info("Could not set owner reference on claim", "error", err)
		}
	}
	logger.Info("Creating new QuotaClaim")
	if err := targetClient.Create(ctx, claim); err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.V(1).Info("QuotaClaim created concurrently, skipping")
			return nil
		}
		return fmt.Errorf("failed to create claim: %w", err)
	}

	r.EventRecorder.Eventf(triggerObj, "Normal", "ClaimCreated",
		"Created QuotaClaim %s/%s from policy %s", claim.Namespace, claim.Name, policy.Name)
	return nil
}

// policyLabel marks the ClaimCreationPolicy a QuotaClaim was created from,
// by admission or by this controller.
const policyLabel = "quota.miloapis.com/policy"

// findTriggerClaim returns the name of an existing claim for the rendered
// claim's trigger: the claim at its pinned name, or any claim from policy
// whose resourceRef is the trigger. It returns "" when there is none.
func findTriggerClaim(ctx context.Context, reader client.Reader, claim *quotav1alpha1.QuotaClaim, policy *quotav1alpha1.ClaimCreationPolicy) (string, error) {
	if claim.Name != "" {
		var existing quotav1alpha1.QuotaClaim
		err := reader.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: claim.Name}, &existing)
		switch {
		case err == nil:
			return existing.Name, nil
		case !apierrors.IsNotFound(err):
			return "", err
		}
	}

	var list quotav1alpha1.QuotaClaimList
	opts := []client.ListOption{client.MatchingLabels{policyLabel: policy.Name}}
	if claim.Namespace != "" {
		opts = append(opts, client.InNamespace(claim.Namespace))
	}
	if err := reader.List(ctx, &list, opts...); err != nil {
		return "", err
	}
	for i := range list.Items {
		if list.Items[i].Spec.ResourceRef == claim.Spec.ResourceRef {
			return list.Items[i].Name, nil
		}
	}
	return "", nil
}

// cleanupClaim removes a claim if conditions are no longer met.
func (r *ClaimCreationController) cleanupClaim(
	ctx context.Context,
	policy *quotav1alpha1.ClaimCreationPolicy,
	triggerObj *unstructured.Unstructured,
) error {
	logger := log.FromContext(ctx).WithValues("policy", policy.Name)

	gvk := triggerObj.GroupVersionKind()
	evalContext := &engine.EvaluationContext{
		Object:    triggerObj,
		Namespace: triggerObj.GetNamespace(),
		RequestInfo: &request.RequestInfo{
			IsResourceRequest: true,
			Verb:              "create",
			Namespace:         triggerObj.GetNamespace(),
			Name:              triggerObj.GetName(),
			Resource:          gvkToResource(gvk),
			APIGroup:          gvk.Group,
			APIVersion:        gvk.Version,
		},
		GVK: struct {
			Group   string
			Version string
			Kind    string
		}{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind,
		},
	}

	claim, err := r.TemplateEngine.RenderClaim(policy, evalContext)
	if err != nil {
		return fmt.Errorf("failed to render claim for cleanup: %w", err)
	}

	// Only attempt cleanup if the claim has a deterministic name
	if claim.Name == "" {
		return nil
	}
	claim.Name = enforcement.ClaimName(claim.Name)

	clusterClient := r.Manager.GetLocalManager().GetClient()

	existingClaim := &quotav1alpha1.QuotaClaim{}
	err = clusterClient.Get(ctx, client.ObjectKey{
		Name:      claim.Name,
		Namespace: claim.Namespace,
	}, existingClaim)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to check existing claim: %w", err)
	}

	if existingClaim.Labels[policyLabel] == policy.Name {
		logger.Info("Cleaning up claim due to unmet conditions", "claimName", existingClaim.Name)

		if err := clusterClient.Delete(ctx, existingClaim); err != nil {
			return fmt.Errorf("failed to delete claim: %w", err)
		}

		r.EventRecorder.Eventf(triggerObj, "Normal", "ClaimDeleted",
			"Deleted QuotaClaim %s/%s due to unmet conditions", existingClaim.Namespace, existingClaim.Name)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClaimCreationController) SetupWithManager(mgr mcmanager.Manager) error {
	r.logger.Info("Setting up ClaimCreationController")

	controller := mcbuilder.ControllerManagedBy(mgr).
		For(&quotav1alpha1.ClaimCreationPolicy{},
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(true)).
		Named("claim-creation-controller")

	r.logger.Info("ClaimCreationController setup completed successfully")

	return controller.Complete(r)
}

// gvkToResource converts a GVK to a lowercase plural resource name.
// This is a best-effort conversion for synthesising RequestInfo in the controller path.
func gvkToResource(gvk schema.GroupVersionKind) string {
	kind := gvk.Kind
	if kind == "" {
		return ""
	}
	// Simple pluralisation: lowercase + "s". Not perfect for all English words,
	// but sufficient for the RequestInfo.Resource field used in CEL templates.
	lower := strings.ToLower(kind)
	if strings.HasSuffix(lower, "s") {
		return lower + "es"
	}
	return lower + "s"
}
