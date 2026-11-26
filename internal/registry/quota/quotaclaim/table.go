package quotaclaim

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// NewTableConvertor reproduces Milo's ResourceClaim printer columns:
// Granted, (wide) Resource, Age. Exported so the Postgres-native claim REST
// renders the same table as the generic store.
func NewTableConvertor() *registryutil.TableConvertor[*quota.QuotaClaim] {
	type col = registryutil.Column[*quota.QuotaClaim]
	return registryutil.NewTableConvertor(
		col{
			Definition: metav1.TableColumnDefinition{Name: "Granted", Type: "string", Description: "Status of the Granted condition."},
			Cell: func(c *quota.QuotaClaim) interface{} {
				return registryutil.ConditionStatus(c.Status.Conditions, quota.QuotaClaimGranted)
			},
		},
		col{
			Definition: metav1.TableColumnDefinition{Name: "Resource", Type: "string", Priority: 1, Description: "Resource type of the first request."},
			Cell: func(c *quota.QuotaClaim) interface{} {
				if len(c.Spec.Requests) == 0 {
					return nil
				}
				return c.Spec.Requests[0].ResourceType
			},
		},
		registryutil.AgeColumn[*quota.QuotaClaim](),
	)
}
