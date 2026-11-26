package quotagrant

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// NewTableConvertor reproduces Milo's ResourceGrant printer columns:
// Active, then (wide) Consumer Group, Consumer Type, Consumer, then Age.
func NewTableConvertor() *registryutil.TableConvertor[*quota.QuotaGrant] {
	type col = registryutil.Column[*quota.QuotaGrant]
	return registryutil.NewTableConvertor(
		col{
			Definition: metav1.TableColumnDefinition{Name: "Active", Type: "string", Description: "Status of the Active condition."},
			Cell: func(g *quota.QuotaGrant) interface{} {
				return registryutil.ConditionStatus(g.Status.Conditions, quota.QuotaGrantActive)
			},
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Consumer Group", Type: "string", Priority: 1, Description: "API group of the consumer."},
			Cell:       func(g *quota.QuotaGrant) interface{} { return registryutil.OmitEmpty(g.Spec.ConsumerRef.APIGroup) },
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Consumer Type", Type: "string", Priority: 1, Description: "Kind of the consumer."},
			Cell:       func(g *quota.QuotaGrant) interface{} { return g.Spec.ConsumerRef.Kind },
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Consumer", Type: "string", Priority: 1, Description: "Name of the consumer."},
			Cell:       func(g *quota.QuotaGrant) interface{} { return g.Spec.ConsumerRef.Name },
		},
		registryutil.AgeColumn[*quota.QuotaGrant](),
	)
}
