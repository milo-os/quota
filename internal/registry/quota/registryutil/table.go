// Package registryutil holds helpers shared by the quota registry strategies
// and storage: server-side table conversion and validation utilities that
// stand in for the CRD schema features (printer columns, OpenAPI patterns,
// validation ratcheting) Milo got for free from apiextensions.
package registryutil

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metatable "k8s.io/apimachinery/pkg/api/meta/table"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var objectMetaSwaggerDocs = metav1.ObjectMeta{}.SwaggerDoc()

// Column describes one printer column after Name. Cell returns the value for
// one object; returning nil renders as "<none>", matching CRD printer columns
// whose JSONPath resolves to nothing.
type Column[T runtime.Object] struct {
	Definition metav1.TableColumnDefinition
	Cell       func(obj T) interface{}
}

// AgeColumn renders .metadata.creationTimestamp exactly as an apiextensions
// `type: date` printer column does (a human-readable duration).
func AgeColumn[T runtime.Object]() Column[T] {
	return Column[T]{
		Definition: metav1.TableColumnDefinition{
			Name:        "Age",
			Type:        "date",
			Description: objectMetaSwaggerDocs["creationTimestamp"],
		},
		Cell: func(obj T) interface{} {
			m, err := meta.Accessor(obj)
			if err != nil {
				return nil
			}
			return metatable.ConvertToHumanReadableDateType(m.GetCreationTimestamp())
		},
	}
}

// TableConvertor is a typed rest.TableConvertor that reproduces a CRD's
// additionalPrinterColumns: a leading Name column followed by Columns.
type TableConvertor[T runtime.Object] struct {
	headers []metav1.TableColumnDefinition
	columns []Column[T]
}

var _ rest.TableConvertor = &TableConvertor[runtime.Object]{}

// NewTableConvertor builds a TableConvertor for objects of type T (and lists
// of T, which are walked item by item).
func NewTableConvertor[T runtime.Object](columns ...Column[T]) *TableConvertor[T] {
	headers := make([]metav1.TableColumnDefinition, 0, len(columns)+1)
	headers = append(headers, metav1.TableColumnDefinition{
		Name: "Name", Type: "string", Format: "name", Description: objectMetaSwaggerDocs["name"],
	})
	for _, c := range columns {
		headers = append(headers, c.Definition)
	}
	return &TableConvertor[T]{headers: headers, columns: columns}
}

// ConvertToTable implements rest.TableConvertor.
func (c *TableConvertor[T]) ConvertToTable(ctx context.Context, obj runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	table := &metav1.Table{}
	if opt, ok := tableOptions.(*metav1.TableOptions); !ok || opt == nil || !opt.NoHeaders {
		table.ColumnDefinitions = c.headers
	}

	if m, err := meta.ListAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else if m, err := meta.CommonAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
	}

	var err error
	table.Rows, err = metatable.MetaToTableRow(obj, func(o runtime.Object, _ metav1.Object, name, _ string) ([]interface{}, error) {
		typed, ok := o.(T)
		if !ok {
			var zero T
			return nil, fmt.Errorf("table convertor: expected %T, got %T", zero, o)
		}
		cells := make([]interface{}, 0, len(c.columns)+1)
		cells = append(cells, name)
		for _, col := range c.columns {
			cells = append(cells, col.Cell(typed))
		}
		return cells, nil
	})
	return table, err
}

// ConditionStatus returns the status of the condition with the given type, or
// nil when absent (CRD JSONPath `.status.conditions[?(@.type=='X')].status`).
func ConditionStatus(conditions []metav1.Condition, conditionType string) interface{} {
	if c := meta.FindStatusCondition(conditions, conditionType); c != nil {
		return string(c.Status)
	}
	return nil
}

// OptionalBool dereferences b, returning nil when unset.
func OptionalBool(b *bool) interface{} {
	if b == nil {
		return nil
	}
	return *b
}

// OmitEmpty mirrors JSONPath over an `omitempty` string field: an empty value
// is absent from the serialized object, so the CRD column renders "<none>".
func OmitEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
