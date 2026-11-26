package enforcement

import (
	"context"
	"fmt"

	"k8s.io/apiserver/pkg/admission"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// ConsumerResolver derives the consumer for a claim whose ClaimCreationPolicy
// template did not set one. It returns ok=false when it has no answer for the
// request, so the next configured source is tried. A non-nil error fails
// admission.
//
// The quota service ships no resolver: the consumer model (organizations,
// projects, tenants, ...) belongs to the control plane that embeds the
// plugin. Consumers install one with WithConsumerResolver.
type ConsumerResolver func(ctx context.Context, attrs admission.Attributes) (ref quotav1alpha1.ConsumerRef, ok bool, err error)

// NamespaceLookup returns the annotations of a namespace in the control
// plane the triggering request was made against. It backs
// ConsumerMapping.NamespaceAnnotation.
type NamespaceLookup func(ctx context.Context, namespace string) (map[string]string, error)

// consumerRefSet reports whether a consumerRef names a consumer.
func consumerRefSet(ref quotav1alpha1.ConsumerRef) bool {
	return ref.Kind != "" && ref.Name != ""
}

// firstExtra returns the first value of a user-info extra, or "" when the key
// is empty or unset.
func firstExtra(extra map[string][]string, key string) string {
	if key == "" {
		return ""
	}
	if vals := extra[key]; len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// resolveConsumer returns the consumer for a claim. The template-rendered
// consumerRef wins; then the custom resolver; then the configured mapping.
// Returns a misconfigured failure when nothing resolves, because a claim
// with an empty consumer would land in a shared bucket.
func (p *QuotaEnforcementPlugin) resolveConsumer(ctx context.Context, attrs admission.Attributes, policy *quotav1alpha1.ClaimCreationPolicy, rendered quotav1alpha1.ConsumerRef) (quotav1alpha1.ConsumerRef, error) {
	if consumerRefSet(rendered) {
		return rendered, nil
	}

	if p.consumerResolver != nil {
		ref, ok, err := p.consumerResolver(ctx, attrs)
		if err != nil {
			return quotav1alpha1.ConsumerRef{}, newInternalFailure(fmt.Errorf("consumer resolver failed: %w", err))
		}
		if ok && consumerRefSet(ref) {
			return ref, nil
		}
	}

	mapping := p.config.ConsumerMapping

	if info := attrs.GetUserInfo(); info != nil {
		extra := info.GetExtra()
		for _, src := range mapping.UserExtraRef {
			ref := quotav1alpha1.ConsumerRef{
				APIGroup: firstExtra(extra, src.APIGroupKey),
				Kind:     firstExtra(extra, src.KindKey),
				Name:     firstExtra(extra, src.NameKey),
			}
			if consumerRefSet(ref) {
				return ref, nil
			}
		}
		for _, src := range mapping.UserExtra {
			if vals := extra[src.Key]; len(vals) > 0 && vals[0] != "" {
				return quotav1alpha1.ConsumerRef{APIGroup: src.APIGroup, Kind: src.Kind, Name: vals[0]}, nil
			}
		}
	}

	if ns := attrs.GetNamespace(); ns != "" && len(mapping.NamespaceAnnotation) > 0 {
		if p.namespaceLookup == nil {
			return quotav1alpha1.ConsumerRef{}, newMisconfiguredFailure("NoNamespaceLookup",
				fmt.Errorf("consumer mapping uses namespace annotations but no namespace lookup is configured"))
		}
		annotations, err := p.namespaceLookup(ctx, ns)
		if err != nil {
			return quotav1alpha1.ConsumerRef{}, newInternalFailure(fmt.Errorf("failed to read namespace %q for consumer resolution: %w", ns, err))
		}
		for _, src := range mapping.NamespaceAnnotation {
			if v := annotations[src.Key]; v != "" {
				return quotav1alpha1.ConsumerRef{APIGroup: src.APIGroup, Kind: src.Kind, Name: v}, nil
			}
		}
	}

	return quotav1alpha1.ConsumerRef{}, newMisconfiguredFailure("EmptyConsumerRef",
		fmt.Errorf("ClaimCreationPolicy %q did not resolve a consumer for %s %q: set spec.target.resourceClaimTemplate.spec.consumerRef, "+
			"or configure a consumer mapping (user extra or namespace annotation) on the quota enforcement plugin",
			policy.Name, attrs.GetResource().GroupResource(), attrs.GetName()))
}
