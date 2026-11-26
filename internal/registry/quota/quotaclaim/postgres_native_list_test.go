package quotaclaim

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"go.miloapis.com/quota/pkg/apis/quota"
)

func TestBuildListQuery_BaseClauses(t *testing.T) {
	sqlText, args, post, err := buildListQuery("", nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "kind = 'QuotaClaim'") {
		t.Errorf("expected kind filter, got: %s", sqlText)
	}
	if strings.Contains(sqlText, "namespace = ") {
		t.Errorf("cluster-scope query should not filter namespace, got: %s", sqlText)
	}
	if len(args) != 0 {
		t.Errorf("expected zero args, got %d", len(args))
	}
	if post != nil {
		t.Errorf("expected no post filter for empty selectors")
	}
}

func TestBuildListQuery_NamespaceScope(t *testing.T) {
	sqlText, args, _, err := buildListQuery("tenant-a", nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "AND namespace = $1") {
		t.Errorf("expected namespace predicate, got: %s", sqlText)
	}
	if len(args) != 1 || args[0] != "tenant-a" {
		t.Errorf("expected args [tenant-a], got %v", args)
	}
}

func TestBuildListQuery_FieldSelectorPushdown(t *testing.T) {
	fs := fields.SelectorFromSet(fields.Set{
		"spec.consumerRef.kind": "Organization",
		"spec.consumerRef.name": "acme",
	})
	sqlText, args, post, err := buildListQuery("", nil, fs, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "claim_consumer_kind") {
		t.Errorf("expected consumerRef.kind pushdown, got: %s", sqlText)
	}
	if !strings.Contains(sqlText, "claim_consumer_name") {
		t.Errorf("expected consumerRef.name pushdown, got: %s", sqlText)
	}
	if len(args) != 2 {
		t.Errorf("expected 2 args for two-requirement selector, got %d (%v)", len(args), args)
	}
	if post != nil {
		t.Errorf("expected no post filter for fully-pushed selector")
	}
}

func TestBuildListQuery_FieldMetaNamePushdown(t *testing.T) {
	fs := fields.SelectorFromSet(fields.Set{"metadata.name": "claim-1"})
	sqlText, args, _, err := buildListQuery("tenant-a", nil, fs, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "AND name =") {
		t.Errorf("expected name pushdown, got: %s", sqlText)
	}
	if len(args) != 2 || args[0] != "tenant-a" || args[1] != "claim-1" {
		t.Errorf("unexpected args: %v", args)
	}
}

func TestBuildListQuery_LabelSelectorEqualsPushdown(t *testing.T) {
	sel, err := labels.Parse("env=prod")
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	sqlText, args, post, err := buildListQuery("", sel, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "data_json->'metadata'->'labels'->>") {
		t.Errorf("expected label pushdown, got: %s", sqlText)
	}
	if len(args) != 2 || args[0] != "env" || args[1] != "prod" {
		t.Errorf("unexpected args: %v", args)
	}
	if post != nil {
		t.Errorf("expected no post filter for simple equals selector")
	}
}

func TestBuildListQuery_LabelSelectorInPushdown(t *testing.T) {
	sel, err := labels.Parse("team in (alpha,beta)")
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	sqlText, args, _, err := buildListQuery("", sel, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "IN (") {
		t.Errorf("expected IN clause, got: %s", sqlText)
	}
	if len(args) != 3 {
		t.Errorf("expected key+2 values, got %d args (%v)", len(args), args)
	}
}

func TestBuildListQuery_LabelSelectorExistsFallback(t *testing.T) {
	sel, err := labels.Parse("env")
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	_, _, post, err := buildListQuery("", sel, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if post == nil {
		t.Fatalf("expected post filter for Exists selector fallback")
	}
	hasLabel := &quota.QuotaClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"env": "x"}}}
	noLabel := &quota.QuotaClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"other": "x"}}}
	if !post(hasLabel) {
		t.Errorf("post filter should accept claim with matching label")
	}
	if post(noLabel) {
		t.Errorf("post filter should reject claim without matching label")
	}
}

func TestBuildListQuery_FieldSelectorNotEqualsPushdown(t *testing.T) {
	fs, err := fields.ParseSelector("spec.consumerRef.kind!=Organization")
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	sqlText, args, _, err := buildListQuery("", nil, fs, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sqlText, "<>") {
		t.Errorf("expected <> operator, got: %s", sqlText)
	}
	if len(args) != 1 || args[0] != "Organization" {
		t.Errorf("unexpected args: %v", args)
	}
}
