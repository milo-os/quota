package enforcement

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

const (
	// PluginName is the name of the enforcement admission plugin.
	PluginName = "QuotaEnforcement"

	// ClaimWaitTimeout is the default maximum time to wait for a QuotaClaim
	// to be granted. It is deliberately shorter than the enforcement
	// webhook's timeoutSeconds (30s) so the plugin always answers with a
	// precise timeout error before the calling apiserver gives up on the
	// webhook and applies its failurePolicy.
	ClaimWaitTimeout = 25 * time.Second

	// watchManagerStartupTimeout bounds how long a request waits for a new
	// scope's watch stream to come up.
	watchManagerStartupTimeout = 30 * time.Second
)

var quotaClaimGVR = schema.GroupVersionResource{
	Group:    quotav1alpha1.SchemeGroupVersion.Group,
	Version:  quotav1alpha1.SchemeGroupVersion.Version,
	Resource: "quotaclaims",
}

// ClientResolver selects the control plane a request's QuotaClaim lives in.
// It returns a stable scope ID (used to cache one watch manager per scope;
// "" is the root scope) and the dynamic client for that scope. Consumers
// that run one control plane per tenant/project install one with
// WithClientResolver so claims are created and watched where the triggering
// resource lives.
type ClientResolver func(ctx context.Context) (scopeID string, client dynamic.Interface, err error)

// WatchManagerFactory builds the ClaimWatchManager for a scope. The default
// is NewWatchManager. Consumers can supply their own via
// WithWatchManagerFactory, for example to share an existing informer.
type WatchManagerFactory func(client dynamic.Interface, logger logr.Logger, scopeID string) ClaimWatchManager

// QuotaEnforcementPlugin enforces quota by creating QuotaClaims for applicable resources.
//
// CRDs are always decoded as *unstructured.Unstructured by apiextensions-apiserver regardless
// of scheme registration. Validation converts to typed structs via runtime.DefaultUnstructuredConverter.
type QuotaEnforcementPlugin struct {
	*admission.Handler
	dynamicClient       dynamic.Interface
	loopbackConfig      *rest.Config
	policyEngine        PolicyEngine
	templateEngine      TemplateEngine
	watchManagers       sync.Map // map[string]ClaimWatchManager (scopeID -> watch manager, "" = root)
	config              *Config
	logger              logr.Logger
	clientResolver      ClientResolver
	watchManagerFactory WatchManagerFactory
	consumerResolver    ConsumerResolver
	namespaceLookup     NamespaceLookup
}

// Ensure QuotaEnforcementPlugin implements the required initializer interfaces
var _ initializer.WantsDynamicClient = &QuotaEnforcementPlugin{}
var _ admission.ValidationInterface = &QuotaEnforcementPlugin{}
var _ admission.InitializationValidator = &QuotaEnforcementPlugin{}

// Option is a function that configures the QuotaEnforcementPlugin.
type Option func(*QuotaEnforcementPlugin)

// WithConfig sets the plugin configuration. Unset fields keep their defaults.
func WithConfig(config *Config) Option {
	return func(p *QuotaEnforcementPlugin) {
		if config == nil {
			return
		}
		merged := *config
		if merged.WatchManager == nil {
			merged.WatchManager = DefaultWatchManagerConfig()
		}
		if merged.ConditionTimeout <= 0 {
			merged.ConditionTimeout = DefaultConditionTimeout
		}
		p.config = &merged
	}
}

// WithLogger sets the plugin logger.
func WithLogger(logger logr.Logger) Option {
	return func(p *QuotaEnforcementPlugin) {
		p.logger = logger
	}
}

// WithClientResolver routes each request's claim to a scope-specific client.
func WithClientResolver(resolver ClientResolver) Option {
	return func(p *QuotaEnforcementPlugin) {
		p.clientResolver = resolver
	}
}

// WithWatchManagerFactory overrides how per-scope watch managers are built.
func WithWatchManagerFactory(factory WatchManagerFactory) Option {
	return func(p *QuotaEnforcementPlugin) {
		p.watchManagerFactory = factory
	}
}

// WithConsumerResolver installs a resolver that derives the consumer when the
// policy template leaves consumerRef empty. It runs before ConsumerMapping.
func WithConsumerResolver(resolver ConsumerResolver) Option {
	return func(p *QuotaEnforcementPlugin) {
		p.consumerResolver = resolver
	}
}

// WithNamespaceLookup supplies namespace annotations for
// ConsumerMapping.NamespaceAnnotation.
func WithNamespaceLookup(lookup NamespaceLookup) Option {
	return func(p *QuotaEnforcementPlugin) {
		p.namespaceLookup = lookup
	}
}

// NewQuotaEnforcementPlugin creates a new QuotaEnforcementPlugin with the given
// policy and template engines and optional configuration.
func NewQuotaEnforcementPlugin(policyEngine PolicyEngine, templateEngine TemplateEngine, opts ...Option) (*QuotaEnforcementPlugin, error) {
	logger := klog.NewKlogr().WithName("quota-enforcement-plugin")
	klog.V(1).InfoS("Creating QuotaEnforcement admission plugin instance")

	plugin := &QuotaEnforcementPlugin{
		Handler:        admission.NewHandler(admission.Create),
		policyEngine:   policyEngine,
		templateEngine: templateEngine,
		config:         DefaultConfig(),
		logger:         logger,
	}

	for _, opt := range opts {
		opt(plugin)
	}

	if plugin.watchManagerFactory == nil {
		wmConfig := plugin.config.WatchManager
		plugin.watchManagerFactory = func(client dynamic.Interface, logger logr.Logger, scopeID string) ClaimWatchManager {
			return newWatchManagerWithConfig(client, logger, scopeID, wmConfig)
		}
	}

	return plugin, nil
}

// SetDynamicClient implements initializer.WantsDynamicClient
func (p *QuotaEnforcementPlugin) SetDynamicClient(dynamicClient dynamic.Interface) {
	p.dynamicClient = dynamicClient
	p.logger.V(2).Info("Dynamic client set", "plugin", PluginName)
}

// SetLoopbackConfig records the loopback config so a consumer's
// ClientResolver can derive scope-specific clients from it.
func (p *QuotaEnforcementPlugin) SetLoopbackConfig(cfg *rest.Config) {
	p.loopbackConfig = cfg
	p.logger.V(2).Info("Loopback config injected", "plugin", PluginName)
}

// LoopbackConfig returns the config set by SetLoopbackConfig, if any.
func (p *QuotaEnforcementPlugin) LoopbackConfig() *rest.Config {
	return p.loopbackConfig
}

// SetPolicyEngine sets the policy engine (for deferred initialization).
func (p *QuotaEnforcementPlugin) SetPolicyEngine(engine PolicyEngine) {
	p.policyEngine = engine
}

// SetTemplateEngine sets the template engine (for deferred initialization).
func (p *QuotaEnforcementPlugin) SetTemplateEngine(engine TemplateEngine) {
	p.templateEngine = engine
}

// ValidateInitialization implements admission.InitializationValidator
func (p *QuotaEnforcementPlugin) ValidateInitialization() error {
	if p.dynamicClient == nil && p.clientResolver == nil {
		return fmt.Errorf("dynamic client not initialized")
	}
	if p.policyEngine == nil {
		return fmt.Errorf("policy engine not initialized")
	}
	if p.templateEngine == nil {
		return fmt.Errorf("template engine not initialized")
	}
	return nil
}

// Shutdown stops every watch manager the plugin started. It is safe to call
// more than once.
func (p *QuotaEnforcementPlugin) Shutdown() {
	p.watchManagers.Range(func(key, value any) bool {
		if s, ok := value.(interface{ Stop() }); ok {
			s.Stop()
		}
		p.watchManagers.Delete(key)
		return true
	})
}

// resolveScope returns the scope ID and dynamic client for the request. The
// default is the root scope and the injected dynamic client.
func (p *QuotaEnforcementPlugin) resolveScope(ctx context.Context) (string, dynamic.Interface, error) {
	if p.clientResolver != nil {
		scopeID, client, err := p.clientResolver(ctx)
		if err != nil {
			return "", nil, fmt.Errorf("failed to resolve client for request: %w", err)
		}
		if client == nil {
			return "", nil, fmt.Errorf("client resolver returned no client for scope %q", scopeID)
		}
		return scopeID, client, nil
	}
	if p.dynamicClient == nil {
		return "", nil, fmt.Errorf("dynamic client not initialized")
	}
	return "", p.dynamicClient, nil
}

// getWatchManager returns the started watch manager for a scope, creating it
// on first use.
func (p *QuotaEnforcementPlugin) getWatchManager(scopeID string, client dynamic.Interface) (ClaimWatchManager, error) {
	if cached, ok := p.watchManagers.Load(scopeID); ok {
		return cached.(ClaimWatchManager), nil
	}

	logger := p.logger.WithName("watch-manager")
	if scopeID != "" {
		logger = logger.WithValues("scope", scopeID)
	}

	wm := p.watchManagerFactory(client, logger, scopeID)

	if wmWithCallback, ok := wm.(interface{ SetTTLExpiredCallback(func()) }); ok {
		wmWithCallback.SetTTLExpiredCallback(func() {
			p.logger.Info("Watch manager TTL expired, removing from cache", "scope", scopeID)
			p.watchManagers.CompareAndDelete(scopeID, wm)
		})
	}

	// Dedicated timeout prevents admission request timeout from affecting watch manager startup
	startupCtx, startupCancel := context.WithTimeout(context.Background(), watchManagerStartupTimeout)
	defer startupCancel()

	startChan := make(chan error, 1)
	go func() {
		startChan <- wm.Start(startupCtx)
	}()

	select {
	case err := <-startChan:
		if err != nil {
			return nil, fmt.Errorf("failed to start watch manager: %w", err)
		}
	case <-startupCtx.Done():
		return nil, fmt.Errorf("watch manager startup timed out after %v: %w", watchManagerStartupTimeout, startupCtx.Err())
	}

	actual, loaded := p.watchManagers.LoadOrStore(scopeID, wm)
	if loaded {
		// A concurrent request won the race; release the duplicate stream.
		if s, ok := wm.(interface{ Stop() }); ok {
			s.Stop()
		}
	} else {
		p.logger.V(2).Info("Created and started watch manager", "scope", scopeID)
	}
	return actual.(ClaimWatchManager), nil
}

// Validate implements admission.ValidationInterface and orchestrates the main enforcement flow
func (p *QuotaEnforcementPlugin) Validate(ctx context.Context, attrs admission.Attributes, _ admission.ObjectInterfaces) error {
	p.logger.V(3).Info("QuotaEnforcement admission plugin triggered",
		"operation", attrs.GetOperation(),
		"resource.group", attrs.GetKind().Group,
		"resource.version", attrs.GetKind().Version,
		"resource.kind", attrs.GetKind().Kind,
		"name", attrs.GetName(),
		"namespace", attrs.GetNamespace(),
		"user", attrs.GetUserInfo().GetName(),
		"dryRun", attrs.IsDryRun(),
	)

	// Skip quota API types - those are handled by the validation plugin
	if attrs.GetKind().Group == quotav1alpha1.GroupName {
		return nil
	}

	// Only handle CREATE operations for non-quota resources
	if attrs.GetOperation() != admission.Create {
		p.logger.V(4).Info("Skipping non-CREATE operation", "operation", attrs.GetOperation())
		return nil
	}

	// Skip dry run requests to avoid creating QuotaClaims during validation
	if attrs.IsDryRun() {
		return nil
	}

	return p.handleQuotaEnforcement(ctx, attrs)
}

// handleQuotaEnforcement enforces resource quotas by creating and validating
// QuotaClaims.
//
// Every ready, enabled policy that targets the resource's group and kind is
// considered (any version; see PolicyEngine). Each policy whose constraints
// match creates its own claim, in policy-name order, and every claim must be
// granted for the request to be admitted. When one is not, claims already
// granted for this request are deleted so their capacity is released.
//
// The plugin fails closed: if the policy cache is not loaded, a constraint
// cannot be evaluated, or a claim cannot be decided, the request is denied.
func (p *QuotaEnforcementPlugin) handleQuotaEnforcement(ctx context.Context, attrs admission.Attributes) error {
	spanAttrs := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("operation", string(attrs.GetOperation())),
			attribute.String("resource.name", attrs.GetName()),
			attribute.String("resource.namespace", attrs.GetNamespace()),
			attribute.String("resource.group", attrs.GetKind().Group),
			attribute.String("resource.version", attrs.GetKind().Version),
			attribute.String("resource.kind", attrs.GetKind().Kind),
			attribute.String("user.name", attrs.GetUserInfo().GetName()),
			attribute.Bool("dry_run", attrs.IsDryRun()),
		),
	}

	ctx, span := p.startSpan(ctx, "quota.admission.QuotaEnforcement", spanAttrs...)
	defer span.End()

	gvk := attrs.GetKind()
	gr := schema.GroupResource{Group: gvk.Group, Resource: attrs.GetResource().Resource}

	// Bound the whole decision (constraint evaluation plus every claim wait)
	// by the claim timeout, which is set below the webhook's timeoutSeconds.
	ctx, cancel := context.WithTimeout(ctx, p.config.WatchManager.DefaultTimeout)
	defer cancel()

	policies, err := p.lookupPoliciesForResource(ctx, gvk.GroupKind())
	if err != nil {
		admissionResultTotal.WithLabelValues("policies_unavailable", "", "", gvk.Group, gvk.Kind).Inc()
		if !p.policyEngine.HasSynced() {
			p.logger.V(2).Info("Quota policies not loaded yet; rejecting with a retryable error", "gvk", gvk)
			return newNotReadyError()
		}
		p.logger.Error(err, "Failed to look up ClaimCreationPolicies", "gvk", gvk)
		return errors.NewForbidden(gr, attrs.GetName(), userFacingClaimError(newInternalFailure(err)))
	}
	if len(policies) == 0 {
		p.logger.V(3).Info("No policy found for kind, skipping QuotaClaim creation", "gvk", gvk)
		return nil
	}

	obj, err := toUnstructured(attrs.GetObject())
	if err != nil {
		return errors.NewForbidden(gr, attrs.GetName(), userFacingClaimError(newInternalFailure(err)))
	}
	evalContext := p.buildEvaluationContext(attrs, obj)

	matched, err := p.matchingPolicies(ctx, policies, attrs, obj)
	if err != nil {
		return errors.NewForbidden(gr, attrs.GetName(), userFacingClaimError(asFailure(err)))
	}

	var granted []grantedClaim
	for _, policy := range matched {
		claim, err := p.createAndWaitForQuotaClaim(ctx, attrs, policy, evalContext)
		if err != nil {
			failure := asFailure(err)
			outcome := admissionOutcomeFor(failure.kind)
			admissionResultTotal.WithLabelValues(outcome, policy.Name, policy.Namespace, gvk.Group, gvk.Kind).Inc()
			p.logger.Error(err, "QuotaClaim not granted, denying resource creation",
				"policy", policy.Name,
				"resourceName", attrs.GetName(),
				"gvk", gvk,
				"failureKind", outcome)
			p.releaseGrantedClaims(granted)
			return errors.NewForbidden(gr, attrs.GetName(), userFacingClaimError(failure))
		}
		if claim != nil {
			granted = append(granted, *claim)
		}
		admissionResultTotal.WithLabelValues("granted", policy.Name, policy.Namespace, gvk.Group, gvk.Kind).Inc()
		p.logger.V(2).Info("QuotaClaim granted", "policy", policy.Name, "resourceName", attrs.GetName())
	}
	return nil
}

// grantedClaim identifies a claim granted during the current request.
type grantedClaim struct {
	client          dynamic.Interface
	name, namespace string
}

// releaseGrantedClaims deletes claims this request created and that were
// granted before a later policy's claim failed, so the rejected request does
// not keep holding capacity. Failures are logged; the ownership controller
// deletes any claim left behind once it finds no resource with its UID.
func (p *QuotaEnforcementPlugin) releaseGrantedClaims(claims []grantedClaim) {
	for _, c := range claims {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := c.client.Resource(quotaClaimGVR).Namespace(c.namespace).Delete(ctx, c.name, metav1.DeleteOptions{})
		cancel()
		if err != nil && !errors.IsNotFound(err) {
			p.logger.Error(err, "Failed to release QuotaClaim of a rejected request", "claimName", c.name, "namespace", c.namespace)
		}
	}
}

// matchingPolicies returns the enabled policies whose trigger constraints
// match the object. Any evaluation error is returned as a claimFailure so
// the caller denies the request: a requester controls the object and so
// controls whether an expression errors, which must never let them skip
// quota.
func (p *QuotaEnforcementPlugin) matchingPolicies(ctx context.Context, policies []*quotav1alpha1.ClaimCreationPolicy, attrs admission.Attributes, obj *unstructured.Unstructured) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	gvk := attrs.GetKind()
	var matched []*quotav1alpha1.ClaimCreationPolicy
	for _, policy := range policies {
		if policy.Spec.Disabled != nil && *policy.Spec.Disabled {
			admissionResultTotal.WithLabelValues("policy_disabled", policy.Name, policy.Namespace, gvk.Group, gvk.Kind).Inc()
			continue
		}
		evalCtx, cancel := context.WithTimeout(ctx, p.config.ConditionTimeout)
		met, err := p.templateEngine.EvaluateConditions(evalCtx, policy.Spec.Trigger.Constraints, obj)
		cancel()
		if err != nil {
			admissionResultTotal.WithLabelValues("evaluation_error", policy.Name, policy.Namespace, gvk.Group, gvk.Kind).Inc()
			p.logger.Error(err, "Failed to evaluate policy constraints; denying request",
				"policy", policy.Name,
				"resourceName", attrs.GetName(),
				"gvk", gvk)
			return nil, newEvaluationFailure(policy.Name, err)
		}
		if !met {
			p.logger.V(3).Info("Policy constraints not met", "policy", policy.Name, "resourceName", attrs.GetName(), "gvk", gvk)
			continue
		}
		matched = append(matched, policy)
	}
	return matched, nil
}

// lookupPoliciesForResource retrieves the policies for a group/kind with tracing.
func (p *QuotaEnforcementPlugin) lookupPoliciesForResource(ctx context.Context, gk schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	_, policySpan := p.startSpan(ctx, "quota.admission.QuotaEnforcement.policyLookup",
		trace.WithAttributes(
			attribute.String("gvk.group", gk.Group),
			attribute.String("gvk.kind", gk.Kind),
		))
	defer policySpan.End()

	if !p.policyEngine.HasSynced() {
		err := fmt.Errorf("quota policies are not loaded yet")
		policySpan.RecordError(err)
		policySpan.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	policies, err := p.policyEngine.GetPoliciesForKind(gk)
	if err != nil {
		policySpan.RecordError(err)
		policySpan.SetStatus(codes.Error, fmt.Sprintf("Failed to get policies: %v", err))
		return nil, err
	}
	policySpan.SetAttributes(attribute.Int("policy.count", len(policies)))
	return policies, nil
}

// newNotReadyError is returned while the policy cache is loading. It is a 503
// with Retry-After, which client-go and kubectl retry on their own.
func newNotReadyError() error {
	return &errors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusServiceUnavailable,
		Reason:  metav1.StatusReasonServiceUnavailable,
		Message: "Quota enforcement is starting up and cannot check this request yet. Please try again in a moment.",
		Details: &metav1.StatusDetails{RetryAfterSeconds: 1},
	}}
}

// toUnstructured returns the admission object as unstructured.
func toUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	switch v := obj.(type) {
	case nil:
		return nil, fmt.Errorf("admission object is nil")
	case *unstructured.Unstructured:
		return v, nil
	default:
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %T to unstructured: %w", obj, err)
		}
		return &unstructured.Unstructured{Object: m}, nil
	}
}

// asFailure returns err as a claimFailure, wrapping anything else as internal.
func asFailure(err error) *claimFailure {
	if f, ok := asClaimFailure(err); ok {
		return f
	}
	return newInternalFailure(err)
}

// admissionOutcomeFor maps a claim failure kind to the label emitted on
// the enforcement result metric so operators can tell denials apart from
// timeouts and infrastructure errors.
func admissionOutcomeFor(kind claimFailureKind) string {
	switch kind {
	case claimFailureDenied:
		return "denied"
	case claimFailureTimeout:
		return "timeout"
	case claimFailureConflict:
		return "conflict"
	case claimFailureMisconfigured:
		return "policy_misconfigured"
	case claimFailureEvaluation:
		return "evaluation_error"
	default:
		return "internal_error"
	}
}

// userMessage is an error whose text is shown verbatim to API clients. It is
// a distinct type so the sentence-style messages are not mistaken for
// internal error strings.
type userMessage string

func (m userMessage) Error() string { return string(m) }

// userFacingClaimError converts a typed claim failure into the error surfaced
// in the 403 Forbidden response body. Messages are shown verbatim by kubectl
// and API clients, so they explain the cause and the next step in plain
// language.
func userFacingClaimError(f *claimFailure) error {
	switch f.kind {
	case claimFailureDenied:
		if f.message != "" {
			return userMessage(fmt.Sprintf("You've reached your quota for this resource type (%s). Delete unused resources to free up capacity, or request a higher limit.", f.message))
		}
		return userMessage("You've reached your quota for this resource type. Delete unused resources to free up capacity, or request a higher limit.")
	case claimFailureTimeout:
		return userMessage("Your request took too long to be checked against your quota. Please try again in a moment.")
	case claimFailureConflict:
		if f.message != "" {
			return userMessage(fmt.Sprintf("A previous attempt to create this resource is still being cleaned up (%s). Please try again in a few seconds.", f.message))
		}
		return userMessage("A previous attempt to create this resource is still being cleaned up. Please try again in a few seconds.")
	case claimFailureMisconfigured:
		if f.cause != nil {
			return userMessage(fmt.Sprintf("Quota enforcement for this resource type is misconfigured and can't be applied: %v", f.cause))
		}
		return userMessage("Quota enforcement for this resource type is misconfigured and can't be applied.")
	case claimFailureEvaluation:
		// The expression error is logged, not returned: it can describe the
		// policy's internals.
		return userMessage(fmt.Sprintf("Quota enforcement couldn't evaluate ClaimCreationPolicy %q against this resource, so the request was denied. Ask the policy's owner to check its trigger constraints.", f.reason))
	default:
		return userMessage("Something went wrong while checking your quota for this request. Please try again.")
	}
}

// createAndWaitForQuotaClaim creates a QuotaClaim and blocks until the claim
// is resolved. On a grant it returns the claim when this call created it
// (nil when an earlier attempt's claim was reused). All error returns are
// *claimFailure so the caller can produce precise user-facing messages and
// metric labels.
func (p *QuotaEnforcementPlugin) createAndWaitForQuotaClaim(ctx context.Context, attrs admission.Attributes, policy *quotav1alpha1.ClaimCreationPolicy, evalContext *EvaluationContext) (*grantedClaim, error) {
	ctx, span := p.startSpan(ctx, "quota.admission.QuotaEnforcement.createAndWaitForQuotaClaim",
		trace.WithAttributes(
			attribute.String("policy.name", policy.Name),
			attribute.String("resource.name", evalContext.Object.GetName()),
			attribute.String("resource.namespace", evalContext.Object.GetNamespace()),
		))
	defer span.End()

	fail := func(err error, msg string) error {
		span.RecordError(err)
		span.SetStatus(codes.Error, msg)
		if _, ok := asClaimFailure(err); ok {
			return err
		}
		return newInternalFailure(err)
	}

	scopeID, client, err := p.resolveScope(ctx)
	if err != nil {
		return nil, fail(err, "Failed to resolve client")
	}

	claim, err := p.buildClaim(ctx, attrs, policy, evalContext)
	if err != nil {
		return nil, fail(err, "Failed to build QuotaClaim")
	}
	claimName, namespace := claim.Name, claim.Namespace

	span.SetAttributes(
		attribute.String("claim.name", claimName),
		attribute.String("claim.namespace", namespace),
	)

	// When a policy pins the claim name, a retry of the same request can
	// meet the claim from a previous attempt. Resolve it before registering
	// the waiter so a stale claim's events can't be mistaken for this
	// attempt's.
	switch action, existing := p.resolveExistingClaim(ctx, client, claimName, namespace); action {
	case existingClaimGranted:
		// The earlier attempt never produced a resource (or this create
		// would fail AlreadyExists in storage), so the claim now belongs to
		// this attempt: record this object's UID so the ownership
		// controller attaches it here instead of deleting it as an orphan.
		if err := p.adoptClaim(ctx, client, existing, claim.Annotations[ResourceUIDAnnotation]); err != nil {
			return nil, fail(newConflictFailure("unable to reuse the existing claim", err), "Failed to adopt existing claim")
		}
		claimResolutionTotal.WithLabelValues("granted").Inc()
		span.SetAttributes(attribute.String("claim.result", "short_circuit_granted"))
		p.logger.V(2).Info("Existing QuotaClaim already granted; short-circuiting admission",
			"claimName", claimName, "namespace", namespace)
		return nil, nil
	case existingClaimDenied:
		claimResolutionTotal.WithLabelValues("denied").Inc()
		return nil, fail(newConflictFailure(extractGrantedConditionMessage(existing), nil), "Existing claim denied")
	case existingClaimPending:
		// Leftover from an earlier admission timeout. Delete it so this
		// attempt can create a fresh claim; the cleanup controller would
		// eventually do the same.
		claimResolutionTotal.WithLabelValues("deleted_pending").Inc()
		if delErr := client.Resource(quotaClaimGVR).Namespace(namespace).Delete(ctx, claimName, metav1.DeleteOptions{}); delErr != nil && !errors.IsNotFound(delErr) {
			return nil, fail(newConflictFailure("unable to clean up stale pending claim", delErr), "Failed to delete stale claim")
		}
		p.logger.V(2).Info("Deleted stale pending QuotaClaim before retry",
			"claimName", claimName, "namespace", namespace)
	}

	watchManager, err := p.getWatchManager(scopeID, client)
	if err != nil {
		return nil, fail(fmt.Errorf("failed to get watch manager: %w", err), "Failed to get watch manager")
	}

	// Register waiter before claim exists to ensure watch stream catches the ADDED event.
	timeout := p.config.WatchManager.DefaultTimeout
	resultChan, cancelFunc, err := watchManager.RegisterClaimWaiter(ctx, claimName, namespace, timeout)
	if err != nil {
		return nil, fail(fmt.Errorf("failed to register waiter: %w", err), "Failed to register waiter")
	}
	defer cancelFunc()

	created, err := p.createQuotaClaim(ctx, client, claim)
	if err != nil {
		return nil, fail(err, "Failed to create QuotaClaim")
	}
	mine := &grantedClaim{client: client, name: claimName, namespace: namespace}

	// A synchronous allocator (the postgres backend) returns the claim with
	// its final Granted condition already set. Use it instead of waiting for
	// the watch to echo the same state back.
	if result := ClaimResultFromObject(created); result != nil {
		span.SetAttributes(attribute.String("claim.decided_by", "create_response"))
		return mine, p.claimResultToError(span, claimName, namespace, *result)
	}

	select {
	case result, ok := <-resultChan:
		if !ok {
			span.SetStatus(codes.Error, "Result channel closed")
			return nil, newInternalFailure(fmt.Errorf("result channel closed unexpectedly"))
		}
		span.SetAttributes(attribute.String("claim.decided_by", "watch"))
		return mine, p.claimResultToError(span, claimName, namespace, result)

	case <-ctx.Done():
		span.SetStatus(codes.Error, "Context cancelled")
		watchManager.UnregisterClaimWaiter(claimName, namespace)
		return nil, newTimeoutFailure(ctx.Err())
	}
}

// claimResultToError converts a final claim result into nil (granted) or a
// typed claimFailure.
func (p *QuotaEnforcementPlugin) claimResultToError(span trace.Span, claimName, namespace string, result ClaimResult) error {
	if result.Error != nil {
		span.RecordError(result.Error)
		span.SetStatus(codes.Error, "Wait failed")
		if result.Reason == "timeout" {
			return newTimeoutFailure(result.Error)
		}
		return newInternalFailure(result.Error)
	}
	if result.Granted {
		span.SetAttributes(attribute.String("claim.result", "granted"))
		p.logger.V(2).Info("QuotaClaim granted", "claimName", claimName, "namespace", namespace)
		return nil
	}
	span.SetAttributes(
		attribute.String("claim.result", "denied"),
		attribute.String("claim.denial_reason", result.Reason),
	)
	p.logger.Info("QuotaClaim denied", "claimName", claimName, "namespace", namespace, "reason", result.Reason)
	return newDeniedFailure(quotav1alpha1.QuotaClaimDeniedReason, result.Reason)
}

// buildClaim renders the policy template once and fills in everything the
// plugin owns: name, namespace, resourceRef, consumer and the auto-created
// markers the lifecycle controllers key off.
func (p *QuotaEnforcementPlugin) buildClaim(ctx context.Context, attrs admission.Attributes, policy *quotav1alpha1.ClaimCreationPolicy, evalContext *EvaluationContext) (*quotav1alpha1.QuotaClaim, error) {
	claim, err := p.templateEngine.RenderClaim(policy, evalContext)
	if err != nil {
		return nil, fmt.Errorf("failed to render QuotaClaim: %w", err)
	}

	claim.Name = p.claimNameFor(claim, evalContext)
	claim.GenerateName = ""

	namespace, err := p.claimNamespaceFor(attrs, policy, claim, evalContext)
	if err != nil {
		return nil, err
	}
	claim.Namespace = namespace

	claim.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{
		APIGroup:  evalContext.GVK.Group,
		Kind:      evalContext.GVK.Kind,
		Name:      resourceName(attrs, evalContext.Object),
		Namespace: attrs.GetNamespace(),
	}

	consumer, err := p.resolveConsumer(ctx, attrs, policy, claim.Spec.ConsumerRef)
	if err != nil {
		return nil, err
	}
	claim.Spec.ConsumerRef = consumer

	if claim.Labels == nil {
		claim.Labels = make(map[string]string)
	}
	if claim.Annotations == nil {
		claim.Annotations = make(map[string]string)
	}

	claim.Labels["quota.miloapis.com/auto-created"] = "true"
	claim.Labels["quota.miloapis.com/policy"] = policy.Name
	// Use "core" for empty group to match Kubernetes convention
	group := evalContext.GVK.Group
	if group == "" {
		group = "core"
	}
	claim.Labels["quota.miloapis.com/gvk"] = fmt.Sprintf("%s.%s.%s", group, evalContext.GVK.Version, evalContext.GVK.Kind)

	claim.Annotations["quota.miloapis.com/created-by"] = "claim-creation-plugin"
	claim.Annotations["quota.miloapis.com/created-at"] = time.Now().Format(time.RFC3339)
	claim.Annotations["quota.miloapis.com/resource-name"] = resourceName(attrs, evalContext.Object)
	claim.Annotations["quota.miloapis.com/policy"] = policy.Name
	// The apiserver assigns the UID before validating admission runs, so it
	// identifies this create attempt. The ownership controller matches the
	// claim to its resource by it: a later object reusing the name is not
	// this claim's owner.
	if uid := evalContext.Object.GetUID(); uid != "" {
		claim.Annotations[ResourceUIDAnnotation] = string(uid)
	}

	return claim, nil
}

// ResourceUIDAnnotation records, on an auto-created QuotaClaim, the UID of
// the object whose create produced it.
const ResourceUIDAnnotation = "quota.miloapis.com/resource-uid"

// resourceName is the triggering object's name. The object carries the
// generated name for generateName creates; the request attributes may not.
func resourceName(attrs admission.Attributes, obj *unstructured.Unstructured) string {
	if obj != nil && obj.GetName() != "" {
		return obj.GetName()
	}
	return attrs.GetName()
}

// adoptClaim points a granted claim left by an earlier attempt at this
// attempt's object UID. Claims that already have an owner are left alone.
func (p *QuotaEnforcementPlugin) adoptClaim(ctx context.Context, client dynamic.Interface, existing *unstructured.Unstructured, uid string) error {
	if uid == "" || len(existing.GetOwnerReferences()) > 0 || existing.GetAnnotations()[ResourceUIDAnnotation] == uid {
		return nil
	}
	patch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": existing.GetResourceVersion(),
			"annotations":     map[string]string{ResourceUIDAnnotation: uid},
		},
	})
	if err != nil {
		return err
	}
	_, err = client.Resource(quotaClaimGVR).Namespace(existing.GetNamespace()).
		Patch(ctx, existing.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// claimNameFor picks the claim name: the rendered name, a name generated
// from the rendered generateName, or one derived from the resource.
// QuotaClaim names must be DNS subdomains, but templates and trigger
// resources may carry names that are not (uppercase, ':' in RBAC names,
// more than 253 characters), so every candidate is sanitized. A rendered
// name that needs rewriting gets a hash of the original appended, so two
// pinned names never collapse into one claim.
func (p *QuotaEnforcementPlugin) claimNameFor(rendered *quotav1alpha1.QuotaClaim, evalContext *EvaluationContext) string {
	if rendered.Name != "" {
		return ClaimName(rendered.Name)
	}
	if rendered.GenerateName != "" {
		return names.SimpleNameGenerator.GenerateName(sanitizeGenerateName(rendered.GenerateName))
	}
	baseName := sanitizeGenerateName(fmt.Sprintf("%s-%s-claim-",
		evalContext.Object.GetName(),
		strings.ToLower(evalContext.GVK.Kind)))
	return names.SimpleNameGenerator.GenerateName(baseName)
}

// claimNamespaceFor picks the claim namespace: the policy-pinned namespace,
// else the triggering resource's namespace, else (cluster-scoped resources)
// the configured cluster-scoped claim namespace.
func (p *QuotaEnforcementPlugin) claimNamespaceFor(attrs admission.Attributes, policy *quotav1alpha1.ClaimCreationPolicy, rendered *quotav1alpha1.QuotaClaim, evalContext *EvaluationContext) (string, error) {
	namespace := evalContext.Object.GetNamespace()
	if policy.Spec.Target.ResourceClaimTemplate.Metadata.Namespace != "" {
		namespace = rendered.Namespace
	}
	if namespace == "" {
		namespace = attrs.GetNamespace()
	}
	if attrs.GetNamespace() != "" && namespace != attrs.GetNamespace() {
		// A claim for a namespaced resource must live beside it: owner
		// references cannot cross namespaces, so the claim could never be
		// tied to the resource's lifetime and would either leak or be
		// garbage-collected while the resource still exists.
		return "", newMisconfiguredFailure("CrossNamespaceClaim", fmt.Errorf(
			"ClaimCreationPolicy %q places the QuotaClaim in namespace %q, but the resource is in namespace %q; claims for namespaced resources must be in the resource's namespace",
			policy.Name, namespace, attrs.GetNamespace()))
	}
	if namespace != "" {
		return namespace, nil
	}

	// QuotaClaims are namespaced, but a cluster-scoped target resource has
	// no namespace to inherit and ClaimCreationPolicy is itself cluster
	// scoped. Fall back to the configured namespace so quota still enforces;
	// a policy can override per resource by pinning
	// spec.target.resourceClaimTemplate.metadata.namespace.
	namespace = p.config.ClusterScopedClaimNamespace
	if namespace == "" {
		return "", newMisconfiguredFailure("EmptyClaimNamespace", fmt.Errorf(
			"ClaimCreationPolicy %q resolved an empty QuotaClaim namespace for cluster-scoped resource %s and no cluster-scoped claim namespace is configured; set spec.target.resourceClaimTemplate.metadata.namespace or Config.ClusterScopedClaimNamespace",
			policy.Name, attrs.GetResource().GroupResource()))
	}
	p.logger.V(2).Info("Defaulting QuotaClaim namespace for cluster-scoped resource",
		"namespace", namespace,
		"policy", policy.Name,
		"resource", attrs.GetResource().GroupResource().String())
	return namespace, nil
}

// createQuotaClaim creates the claim and returns the object the server
// persisted.
func (p *QuotaEnforcementPlugin) createQuotaClaim(ctx context.Context, client dynamic.Interface, claim *quotav1alpha1.QuotaClaim) (*unstructured.Unstructured, error) {
	ctx, span := p.startSpan(ctx, "quota.admission.QuotaEnforcement.createQuotaClaim",
		trace.WithAttributes(
			attribute.String("claim.name", claim.Name),
			attribute.String("claim.namespace", claim.Namespace),
		))
	defer span.End()

	unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(claim)
	if err != nil {
		return nil, fmt.Errorf("failed to convert QuotaClaim to unstructured: %w", err)
	}
	obj := &unstructured.Unstructured{Object: unstructuredMap}
	obj.SetAPIVersion(quotav1alpha1.SchemeGroupVersion.String())
	obj.SetKind("QuotaClaim")

	created, err := client.Resource(quotaClaimGVR).Namespace(claim.Namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			// A concurrent admission pass for the same resource created the
			// claim between our lookup and Create. Ask the caller to retry
			// rather than guessing which attempt owns it.
			claimResolutionTotal.WithLabelValues("create_conflict").Inc()
			return nil, newConflictFailure("concurrent quota evaluation in progress", err)
		}
		return nil, fmt.Errorf("failed to create QuotaClaim: %w", err)
	}

	p.logger.V(2).Info("QuotaClaim created successfully",
		"claimName", claim.Name,
		"namespace", claim.Namespace,
		"policy", claim.Labels["quota.miloapis.com/policy"],
	)
	return created, nil
}

// existingClaimAction classifies how to handle a QuotaClaim that already
// exists at the predetermined name.
type existingClaimAction int

const (
	// existingClaimAbsent: no usable existing claim; Create.
	existingClaimAbsent existingClaimAction = iota
	// existingClaimGranted: capacity is already reserved; allow.
	existingClaimGranted
	// existingClaimDenied: a previous denial awaiting cleanup; retry later.
	existingClaimDenied
	// existingClaimPending: a stale claim with no final condition; delete
	// and recreate.
	existingClaimPending
)

// resolveExistingClaim inspects the predetermined name for a lingering
// QuotaClaim from a prior admission pass. Read errors other than NotFound
// fall through to Create (which reports AlreadyExists as a conflict), so a
// transient read failure never blocks admission on its own.
func (p *QuotaEnforcementPlugin) resolveExistingClaim(ctx context.Context, client dynamic.Interface, claimName, namespace string) (existingClaimAction, *unstructured.Unstructured) {
	existing, err := client.Resource(quotaClaimGVR).Namespace(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			p.logger.V(2).Info("Failed to look up existing QuotaClaim, proceeding to Create",
				"claimName", claimName, "namespace", namespace, "error", err)
		}
		return existingClaimAbsent, nil
	}

	// Only auto-created claims are ours to reuse or replace; never touch a
	// manually created claim that happens to share the name.
	if existing.GetLabels()["quota.miloapis.com/auto-created"] != "true" {
		return existingClaimAbsent, nil
	}

	switch classifyGrantedCondition(existing) {
	case claimConditionGranted:
		return existingClaimGranted, existing
	case claimConditionDenied:
		return existingClaimDenied, existing
	default:
		return existingClaimPending, existing
	}
}

// claimConditionState is the coarse Granted-condition state of a claim.
type claimConditionState int

const (
	claimConditionPending claimConditionState = iota
	claimConditionGranted
	claimConditionDenied
)

// classifyGrantedCondition reads the Granted condition from a QuotaClaim.
// Missing conditions and non-denied False reasons map to pending.
func classifyGrantedCondition(claim *unstructured.Unstructured) claimConditionState {
	cond := grantedCondition(claim)
	if cond == nil {
		return claimConditionPending
	}
	status, _, _ := unstructured.NestedString(cond, "status")
	reason, _, _ := unstructured.NestedString(cond, "reason")
	switch {
	case status == string(metav1.ConditionTrue):
		return claimConditionGranted
	case status == string(metav1.ConditionFalse) && reason == quotav1alpha1.QuotaClaimDeniedReason:
		return claimConditionDenied
	default:
		return claimConditionPending
	}
}

// extractGrantedConditionMessage returns the Granted condition's message.
func extractGrantedConditionMessage(claim *unstructured.Unstructured) string {
	cond := grantedCondition(claim)
	if cond == nil {
		return ""
	}
	msg, _, _ := unstructured.NestedString(cond, "message")
	return msg
}

// grantedCondition returns the Granted condition of a claim, if present.
func grantedCondition(claim *unstructured.Unstructured) map[string]interface{} {
	if claim == nil {
		return nil
	}
	conditions, found, err := unstructured.NestedSlice(claim.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	for _, c := range conditions {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if t, _, _ := unstructured.NestedString(m, "type"); t == quotav1alpha1.QuotaClaimGranted {
			return m
		}
	}
	return nil
}

// ClaimResultFromObject returns the final result recorded on a claim, or nil
// while the claim is still pending. Granted=True is a grant; Granted=False
// with reason QuotaExceeded is a denial; anything else is not final.
func ClaimResultFromObject(claim *unstructured.Unstructured) *ClaimResult {
	cond := grantedCondition(claim)
	if cond == nil {
		return nil
	}
	status, _, _ := unstructured.NestedString(cond, "status")
	reason, _, _ := unstructured.NestedString(cond, "reason")
	message, _, _ := unstructured.NestedString(cond, "message")
	switch {
	case status == string(metav1.ConditionTrue):
		return &ClaimResult{Granted: true, Reason: reason}
	case status == string(metav1.ConditionFalse) && reason == quotav1alpha1.QuotaClaimDeniedReason:
		return &ClaimResult{Granted: false, Reason: message}
	default:
		return nil
	}
}

// startSpan safely starts a span using the tracer provider from the context
func (p *QuotaEnforcementPlugin) startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracerProvider := trace.SpanFromContext(ctx).TracerProvider()
	tracer := tracerProvider.Tracer("go.miloapis.com/quota/admission/enforcement")
	return tracer.Start(ctx, name, opts...)
}

// buildEvaluationContext creates an EvaluationContext from admission attributes.
func (p *QuotaEnforcementPlugin) buildEvaluationContext(attrs admission.Attributes, obj *unstructured.Unstructured) *EvaluationContext {
	user := UserContext{
		Name:   attrs.GetUserInfo().GetName(),
		UID:    attrs.GetUserInfo().GetUID(),
		Groups: attrs.GetUserInfo().GetGroups(),
		Extra:  attrs.GetUserInfo().GetExtra(),
	}

	verb := strings.ToLower(string(attrs.GetOperation()))

	requestInfo := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              verb,
		APIGroup:          attrs.GetKind().Group,
		APIVersion:        attrs.GetKind().Version,
		Namespace:         attrs.GetNamespace(),
		Resource:          attrs.GetResource().Resource,
		Subresource:       attrs.GetSubresource(),
		Name:              attrs.GetName(),
	}

	return &EvaluationContext{
		Object:      obj,
		User:        user,
		RequestInfo: requestInfo,
		Namespace:   attrs.GetNamespace(),
		GVK: schema.GroupVersionKind{
			Group:   attrs.GetKind().Group,
			Version: attrs.GetKind().Version,
			Kind:    attrs.GetKind().Kind,
		},
	}
}
