package quotabucket

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/internal/registry/quota/registryutil"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// NewTableConvertor reproduces Milo's AllowanceBucket printer columns:
// Resource Type, Limit, Allocated, Available, Claims, Age.
func NewTableConvertor() *registryutil.TableConvertor[*quota.QuotaBucket] {
	type col = registryutil.Column[*quota.QuotaBucket]
	integer := func(name, desc string, v func(*quota.QuotaBucket) int64) col {
		return col{
			Definition: metav1.TableColumnDefinition{Name: name, Type: "integer", Description: desc},
			Cell:       func(b *quota.QuotaBucket) interface{} { return v(b) },
		}
	}
	return registryutil.NewTableConvertor(
		col{
			Definition: metav1.TableColumnDefinition{Name: "Resource Type", Type: "string", Description: "Resource type this bucket aggregates."},
			Cell:       func(b *quota.QuotaBucket) interface{} { return b.Spec.ResourceType },
		},
		integer("Limit", "Total quota capacity.", func(b *quota.QuotaBucket) int64 { return b.Status.Limit }),
		integer("Allocated", "Quota currently consumed.", func(b *quota.QuotaBucket) int64 { return b.Status.Allocated }),
		integer("Available", "Quota remaining for new claims.", func(b *quota.QuotaBucket) int64 { return b.Status.Available }),
		integer("Claims", "Number of granted claims.", func(b *quota.QuotaBucket) int64 { return int64(b.Status.ClaimCount) }),
		registryutil.AgeColumn[*quota.QuotaBucket](),
	)
}
