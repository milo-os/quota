package quotadefinition

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// NewTableConvertor reproduces Milo's ResourceRegistration printer columns:
// Type, Active, Age.
func NewTableConvertor() *registryutil.TableConvertor[*quota.QuotaDefinition] {
	type col = registryutil.Column[*quota.QuotaDefinition]
	return registryutil.NewTableConvertor(
		col{
			Definition: metav1.TableColumnDefinition{Name: "Type", Type: "string", Description: "Measurement method (Entity, Allocation, Feature)."},
			Cell:       func(d *quota.QuotaDefinition) interface{} { return d.Spec.Type },
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Active", Type: "string", Description: "Status of the Active condition."},
			Cell: func(d *quota.QuotaDefinition) interface{} {
				return registryutil.ConditionStatus(d.Status.Conditions, quota.QuotaDefinitionActive)
			},
		},
		registryutil.AgeColumn[*quota.QuotaDefinition](),
	)
}
