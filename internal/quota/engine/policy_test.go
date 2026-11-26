package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

var widgetGK = schema.GroupKind{Group: "example.com", Kind: "Widget"}

func ccp(name, apiVersion, kind string, ready bool) *quotav1alpha1.ClaimCreationPolicy {
	p := &quotav1alpha1.ClaimCreationPolicy{ObjectMeta: metav1.ObjectMeta{Name: name}}
	p.Spec.Trigger.Resource = quotav1alpha1.ClaimTriggerResource{APIVersion: apiVersion, Kind: kind}
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	p.Status.Conditions = []metav1.Condition{{Type: quotav1alpha1.ClaimCreationPolicyReady, Status: status}}
	return p
}

func names(ps []*quotav1alpha1.ClaimCreationPolicy) string {
	var out []string
	for _, p := range ps {
		out = append(out, p.Name)
	}
	return strings.Join(out, ",")
}

func TestPolicyIndex(t *testing.T) {
	idx := newPolicyIndex()

	// Two policies for the same kind are both kept, version-agnostic.
	idx.upsert(ccp("b", "example.com/v1", "Widget", true))
	idx.upsert(ccp("a", "example.com/v1beta1", "Widget", true))
	if got := names(idx.forKind(widgetGK)); got != "a,b" {
		t.Fatalf("forKind = %q, want a,b", got)
	}

	// Deleting one leaves the other enforcing.
	idx.remove("b")
	if got := names(idx.forKind(widgetGK)); got != "a" {
		t.Fatalf("after delete forKind = %q, want a", got)
	}

	// Retargeting moves the policy; the old kind no longer matches it.
	idx.upsert(ccp("a", "example.com/v1", "Gadget", true))
	if got := names(idx.forKind(widgetGK)); got != "" {
		t.Fatalf("old target still matches %q", got)
	}
	if got := names(idx.forKind(schema.GroupKind{Group: "example.com", Kind: "Gadget"})); got != "a" {
		t.Fatalf("new target = %q, want a", got)
	}

	// Not ready or disabled removes it.
	idx.upsert(ccp("a", "example.com/v1", "Gadget", false))
	disabled := ccp("c", "example.com/v1", "Widget", true)
	yes := true
	disabled.Spec.Disabled = &yes
	idx.upsert(disabled)
	if len(idx.byName) != 0 || len(idx.byKind) != 0 {
		t.Fatalf("index not empty: %v %v", idx.byName, idx.byKind)
	}
}

func toUnstructured(t *testing.T, p *quotav1alpha1.ClaimCreationPolicy) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(quotav1alpha1.SchemeGroupVersion.String())
	u.SetKind("ClaimCreationPolicy")
	return u
}

func TestPolicyEngineSyncAndEvents(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{claimCreationPolicyGVR: "ClaimCreationPolicyList"},
		toUnstructured(t, ccp("first", "example.com/v1", "Widget", true)))

	e := NewPolicyEngine(client, logr.Discard())
	if _, err := e.GetPoliciesForKind(widgetGK); !errors.Is(err, ErrPoliciesNotSynced) {
		t.Fatalf("expected ErrPoliciesNotSynced before Start, got %v", err)
	}
	if e.HasSynced() {
		t.Fatal("synced before Start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Indexed as soon as Start returns: no window where a listed policy is
	// missing.
	got, err := e.GetPoliciesForKind(widgetGK)
	if err != nil || names(got) != "first" {
		t.Fatalf("after Start got %q, %v", names(got), err)
	}

	res := client.Resource(claimCreationPolicyGVR)
	if _, err := res.Create(ctx, toUnstructured(t, ccp("second", "example.com/v1beta1", "Widget", true)), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { p, _ := e.GetPoliciesForKind(widgetGK); return names(p) == "first,second" })

	if err := res.Delete(ctx, "second", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { p, _ := e.GetPoliciesForKind(widgetGK); return names(p) == "first" })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
