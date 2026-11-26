package claimcreationpolicy

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// NewTableConvertor reproduces Milo's ClaimCreationPolicy printer columns:
// Target, Disabled, Ready, Age.
func NewTableConvertor() *registryutil.TableConvertor[*quota.ClaimCreationPolicy] {
	type col = registryutil.Column[*quota.ClaimCreationPolicy]
	return registryutil.NewTableConvertor(
		col{
			Definition: metav1.TableColumnDefinition{Name: "Target", Type: "string", Description: "Kind of the trigger resource."},
			Cell:       func(p *quota.ClaimCreationPolicy) interface{} { return p.Spec.Trigger.Resource.Kind },
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Disabled", Type: "boolean", Description: "Whether the policy is disabled."},
			Cell:       func(p *quota.ClaimCreationPolicy) interface{} { return registryutil.OptionalBool(p.Spec.Disabled) },
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Ready", Type: "string", Description: "Status of the Ready condition."},
			Cell: func(p *quota.ClaimCreationPolicy) interface{} {
				return registryutil.ConditionStatus(p.Status.Conditions, quota.ClaimCreationPolicyReady)
			},
		},
		registryutil.AgeColumn[*quota.ClaimCreationPolicy](),
	)
}
