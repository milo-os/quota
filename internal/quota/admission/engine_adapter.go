package admission

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/pkg/admission/enforcement"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// PolicyEngineAdapter wraps the internal PolicyEngine to satisfy the enforcement.PolicyEngine interface.
type PolicyEngineAdapter struct {
	engine engine.PolicyEngine
}

// NewPolicyEngineAdapter creates a new adapter around an internal PolicyEngine.
func NewPolicyEngineAdapter(e engine.PolicyEngine) enforcement.PolicyEngine {
	return &PolicyEngineAdapter{engine: e}
}

func (a *PolicyEngineAdapter) GetPoliciesForKind(gk schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	return a.engine.GetPoliciesForKind(gk)
}

func (a *PolicyEngineAdapter) HasSynced() bool {
	return a.engine.HasSynced()
}

func (a *PolicyEngineAdapter) Start(ctx context.Context) error {
	return a.engine.Start(ctx)
}

func (a *PolicyEngineAdapter) Close() {
	a.engine.Close()
}

// TemplateEngineAdapter wraps the internal TemplateEngine to satisfy the enforcement.TemplateEngine interface.
type TemplateEngineAdapter struct {
	engine engine.TemplateEngine
}

// NewTemplateEngineAdapter creates a new adapter around an internal TemplateEngine.
func NewTemplateEngineAdapter(e engine.TemplateEngine) enforcement.TemplateEngine {
	return &TemplateEngineAdapter{engine: e}
}

func (a *TemplateEngineAdapter) RenderClaim(policy *quotav1alpha1.ClaimCreationPolicy, evalContext *enforcement.EvaluationContext) (*quotav1alpha1.QuotaClaim, error) {
	internalCtx := &engine.EvaluationContext{
		Object: evalContext.Object,
		User: engine.UserContext{
			Name:   evalContext.User.Name,
			UID:    evalContext.User.UID,
			Groups: evalContext.User.Groups,
			Extra:  evalContext.User.Extra,
		},
		RequestInfo: evalContext.RequestInfo,
		Namespace:   evalContext.Namespace,
		GVK: struct {
			Group   string
			Version string
			Kind    string
		}{
			Group:   evalContext.GVK.Group,
			Version: evalContext.GVK.Version,
			Kind:    evalContext.GVK.Kind,
		},
	}
	return a.engine.RenderClaim(policy, internalCtx)
}

func (a *TemplateEngineAdapter) EvaluateConditions(ctx context.Context, conditions []quotav1alpha1.ConditionExpression, obj *unstructured.Unstructured) (bool, error) {
	return a.engine.EvaluateConditions(ctx, conditions, obj)
}
