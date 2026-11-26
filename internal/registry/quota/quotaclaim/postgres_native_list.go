package quotaclaim

import (
	"fmt"
	"strings"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// allSelector and allFieldSelector are sentinel empty selectors used when
// the caller does not supply their own. Using them uniformly lets the query
// builder treat "no selector" and "Everything() selector" as the same path.
var (
	allSelector      = labels.Everything()
	allFieldSelector = fields.Everything()
)

// buildListQuery translates a (namespace, labelSelector, fieldSelector,
// options) tuple into a SQL SELECT against quota_objects and a post-filter
// function that handles selector requirements that could not be pushed
// down to SQL.
//
// The SQL always restricts kind='QuotaClaim' and scopes namespace if the
// request was namespaced. Additional WHERE clauses are appended for
// selector requirements the builder recognizes (metadata.name,
// metadata.namespace, spec.consumerRef.{kind,name}, spec.resourceRef.{kind,
// name}, and labels with equality/IN operators). Unrecognized requirements
// fall through to the post-filter.
//
// The query selects (key, data, resource_version) and has no ORDER BY or
// LIMIT; callers add those with appendPage so LIST pagination and the
// watch's initial list share one builder. opts is accepted for symmetry
// with the REST signature; paging is handled by appendPage.
func buildListQuery(
	namespace string,
	labelSel labels.Selector,
	fieldSel fields.Selector,
	opts *metainternalversion.ListOptions,
) (string, []any, func(*quota.QuotaClaim) bool, error) {
	_ = opts
	var (
		b    strings.Builder
		args []any
	)
	b.WriteString("SELECT key, data, resource_version FROM quota_objects WHERE kind = 'QuotaClaim'")

	if namespace != "" {
		args = append(args, namespace)
		fmt.Fprintf(&b, " AND namespace = $%d", len(args))
	}

	// Field selector pushdown. The supported subset mirrors SelectableFields
	// in strategy.go. Operators other than Equals/NotEquals fall through to
	// the post-filter.
	fieldPost := pushdownFieldSelectors(&b, &args, fieldSel)

	// Label selector pushdown. Equals and In on non-nil values push down to
	// indexable JSONB expressions; NotEquals/NotIn/Exists/DoesNotExist and
	// complex requirements fall through to the post-filter.
	labelPost := pushdownLabelSelectors(&b, &args, labelSel)

	postFilter := combinePostFilters(fieldPost, labelPost)
	return b.String(), args, postFilter, nil
}

// appendPage adds the keyset lower bound, byte-order ORDER BY (the same
// order etcd and the generic Postgres store use), and an optional LIMIT to
// a query built by buildListQuery.
func appendPage(query string, args []any, from string, fromInclusive bool, limit int64) (string, []any) {
	if from != "" {
		args = append(args, from)
		op := ">"
		if fromInclusive {
			op = ">="
		}
		query += fmt.Sprintf(` AND key %s $%d`, op, len(args))
	}
	query += ` ORDER BY key`
	if limit > 0 {
		args = append(args, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	return query, args
}

// pushdownFieldSelectors appends WHERE fragments for recognized field
// selector requirements. It returns a function that further filters claims
// by any unpushed portion (returning true if a claim passes).
func pushdownFieldSelectors(b *strings.Builder, args *[]any, sel fields.Selector) func(*quota.QuotaClaim) bool {
	if sel == nil || sel.Empty() {
		return nil
	}
	reqs := sel.Requirements()
	var unpushed fields.Requirements
	for _, req := range reqs {
		op := req.Operator
		if op != selection.Equals && op != selection.DoubleEquals && op != selection.NotEquals {
			unpushed = append(unpushed, req)
			continue
		}
		sqlOp := "="
		if op == selection.NotEquals {
			sqlOp = "<>"
		}
		col := fieldColumnFor(req.Field)
		if col == "" {
			unpushed = append(unpushed, req)
			continue
		}
		*args = append(*args, req.Value)
		fmt.Fprintf(b, " AND %s %s $%d", col, sqlOp, len(*args))
	}
	if len(unpushed) == 0 {
		return nil
	}
	return func(c *quota.QuotaClaim) bool {
		fieldSet := SelectableFields(c)
		for _, req := range unpushed {
			got := fieldSet.Get(req.Field)
			switch req.Operator {
			case selection.Equals, selection.DoubleEquals:
				if got != req.Value {
					return false
				}
			case selection.NotEquals:
				if got == req.Value {
					return false
				}
			default:
				// Any operator the pushdown could not handle AND the
				// post-filter also cannot handle — punt by failing closed.
				return false
			}
		}
		return true
	}
}

// fieldColumnFor maps a k8s field selector path to a SQL expression. The
// indexed spec fields have their own columns (claim_*, see the schema). The
// result is empty if the field is not pushable.
//
// Every expression is non-NULL for a claim: field selectors treat an absent
// (omitempty) field as "", and a NULL would make both "f=" and "f!=x" drop
// the row. The claim_* columns hold the empty string for absent fields (see
// the schema), and the unindexed JSON paths are wrapped in COALESCE.
func fieldColumnFor(field string) string {
	switch field {
	case "metadata.name":
		return "name"
	case "metadata.namespace":
		return "namespace"
	case "spec.consumerRef.kind":
		return "claim_consumer_kind"
	case "spec.consumerRef.name":
		return "claim_consumer_name"
	case "spec.resourceRef.apiGroup":
		return "COALESCE(data_json->'spec'->'resourceRef'->>'apiGroup', '')"
	case "spec.resourceRef.kind":
		return "claim_resource_kind"
	case "spec.resourceRef.name":
		return "claim_resource_name"
	case "spec.resourceRef.namespace":
		return "COALESCE(data_json->'spec'->'resourceRef'->>'namespace', '')"
	default:
		return ""
	}
}

// pushdownLabelSelectors appends WHERE fragments for recognized label
// selector requirements. The result filters any remaining requirements in
// memory (Exists/DoesNotExist/NotIn/NotEquals always fall back).
func pushdownLabelSelectors(b *strings.Builder, args *[]any, sel labels.Selector) func(*quota.QuotaClaim) bool {
	if sel == nil || sel.Empty() {
		return nil
	}
	reqs, selectable := sel.Requirements()
	if !selectable || len(reqs) == 0 {
		return nil
	}
	pushed := make([]bool, len(reqs))
	for i, req := range reqs {
		op := req.Operator()
		key := req.Key()
		values := req.Values().List()
		switch op {
		case selection.Equals, selection.DoubleEquals:
			if len(values) != 1 {
				continue
			}
			*args = append(*args, key)
			keyIdx := len(*args)
			*args = append(*args, values[0])
			valIdx := len(*args)
			fmt.Fprintf(b, " AND (data_json->'metadata'->'labels'->>$%d) = $%d", keyIdx, valIdx)
			pushed[i] = true
		case selection.In:
			if len(values) == 0 {
				continue
			}
			*args = append(*args, key)
			keyIdx := len(*args)
			placeholders := make([]string, 0, len(values))
			for _, v := range values {
				*args = append(*args, v)
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(*args)))
			}
			fmt.Fprintf(b, " AND (data_json->'metadata'->'labels'->>$%d) IN (%s)", keyIdx, strings.Join(placeholders, ","))
			pushed[i] = true
		}
	}

	// If any requirement was not pushed, the selector result must be
	// re-checked in memory. Because labels.Selector.Matches combines all
	// requirements with AND semantics, the easiest correct thing is to
	// re-run the whole selector against every row: the pushed fragments
	// already pre-filtered, so the postFilter just enforces the rest.
	allPushed := true
	for _, p := range pushed {
		if !p {
			allPushed = false
			break
		}
	}
	if allPushed {
		return nil
	}
	return func(c *quota.QuotaClaim) bool {
		return sel.Matches(labels.Set(c.Labels))
	}
}

// combinePostFilters returns a predicate that's the AND of the two supplied
// predicates. If either is nil the other is returned directly; if both are
// nil the result is nil (no post-filter needed).
func combinePostFilters(a, b func(*quota.QuotaClaim) bool) func(*quota.QuotaClaim) bool {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return func(c *quota.QuotaClaim) bool { return a(c) && b(c) }
	}
}
