package enforcement

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// PolicyEngine manages ClaimCreationPolicy resources and answers which
// policies apply to a resource kind. Consumers provide implementations of
// this interface.
//
// Policies match by API group and kind, regardless of version: a policy for
// example.com/v1 Widget also applies to Widgets created through v1beta1.
// Constraints and templates see the object in the version the request used.
type PolicyEngine interface {
	// GetPoliciesForKind returns every ready, enabled policy that targets
	// gk, in a stable order. It must return an error (never an empty
	// result) while the engine has not loaded its policies.
	GetPoliciesForKind(gk schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error)

	// HasSynced reports whether the engine has loaded and indexed its
	// policies. The plugin rejects requests with a retryable error until
	// it is true, so a starting replica never admits a create unchecked.
	HasSynced() bool

	// Start begins the policy loading and watching process.
	Start(ctx context.Context) error

	// Close stops the policy engine and cleans up resources like watchers.
	Close()
}

// TemplateEngine handles QuotaClaim generation from policy templates.
// Consumers provide implementations of this interface.
type TemplateEngine interface {
	// RenderClaim renders a complete QuotaClaim from a ClaimCreationPolicy.
	RenderClaim(policy *quotav1alpha1.ClaimCreationPolicy, evalContext *EvaluationContext) (*quotav1alpha1.QuotaClaim, error)

	// EvaluateConditions evaluates trigger conditions against a resource
	// object. Evaluation must stop when ctx is done. Any error makes the
	// plugin deny the request.
	EvaluateConditions(ctx context.Context, conditions []quotav1alpha1.ConditionExpression, obj *unstructured.Unstructured) (bool, error)
}

// EvaluationContext provides context for template evaluation in admission scenarios.
type EvaluationContext struct {
	Object      *unstructured.Unstructured
	User        UserContext
	RequestInfo *request.RequestInfo
	Namespace   string
	GVK         schema.GroupVersionKind
}

// UserContext provides user information for template evaluation.
type UserContext struct {
	Name   string
	UID    string
	Groups []string
	Extra  map[string][]string
}
