package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func cond(expr string) []quotav1alpha1.ConditionExpression {
	return []quotav1alpha1.ConditionExpression{{Expression: expr}}
}

func TestEvaluateConditionsErrors(t *testing.T) {
	e, err := NewCELEngine()
	if err != nil {
		t.Fatal(err)
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "w"}}}

	// Missing field is an error, not "no match": the caller fails closed.
	if _, err := e.EvaluateConditions(context.Background(), cond("trigger.spec.tier != 'free'"), obj); err == nil {
		t.Fatal("expected an error for a missing field")
	}
	// has() is the author's tool for optional fields.
	ok, err := e.EvaluateConditions(context.Background(), cond("has(trigger.spec) && trigger.spec.tier != 'free'"), obj)
	if err != nil || ok {
		t.Fatalf("guarded expression = %v, %v; want false, nil", ok, err)
	}
}

// A comprehension over a huge requester-controlled list fails fast on the
// cost limit or the deadline rather than burning CPU for tens of seconds.
func TestEvaluateConditionsBounded(t *testing.T) {
	e, err := NewCELEngine()
	if err != nil {
		t.Fatal(err)
	}
	ports := make([]interface{}, 200000)
	for i := range ports {
		ports[i] = map[string]interface{}{"port": int64(i + 1)}
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"ports": ports}}}

	start := time.Now()
	_, err = e.EvaluateConditions(context.Background(), cond("trigger.spec.ports.all(p, p.port > 0)"), obj)
	if err == nil {
		t.Fatal("expected the cost limit or deadline to stop evaluation")
	}
	t.Logf("err=%v took=%v", err, time.Since(start))
	if d := time.Since(start); d > MaxConditionEvaluationTime+time.Second {
		t.Fatalf("evaluation took %v", d)
	}

	// A caller deadline stops evaluation too.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.EvaluateConditions(ctx, cond("trigger.spec.ports.all(p, p.port > 0)"), obj)
	if err == nil || !strings.Contains(err.Error(), "interrupted") && !strings.Contains(err.Error(), "cost") {
		t.Fatalf("expected interruption, got %v", err)
	}
}
