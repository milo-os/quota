package enforcement

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	k8stesting "k8s.io/client-go/testing"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func newPluginWith(t *testing.T, client dynamic.Interface, pe *stubPolicyEngine, te stubTemplateEngine, wm ClaimWatchManager) *QuotaEnforcementPlugin {
	t.Helper()
	p, err := NewQuotaEnforcementPlugin(pe, te,
		WithLogger(logr.Discard()),
		WithWatchManagerFactory(func(dynamic.Interface, logr.Logger, string) ClaimWatchManager { return wm }))
	if err != nil {
		t.Fatal(err)
	}
	p.SetDynamicClient(client)
	return p
}

func namedPolicy(name string, constraint string) *quotav1alpha1.ClaimCreationPolicy {
	p := testPolicy(tenant, "")
	p.Name = name
	if constraint != "" {
		p.Spec.Trigger.Constraints = []quotav1alpha1.ConditionExpression{{Expression: constraint}}
	}
	return p
}

// A constraint that errors on the requester's object must deny the request,
// not admit it without a claim, and must not echo the expression error.
func TestConstraintEvaluationErrorFailsClosed(t *testing.T) {
	client := newFakeDynamic()
	grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, nil)
	te := stubTemplateEngine{conditions: func(context.Context, []quotav1alpha1.ConditionExpression) (bool, error) {
		return false, fmt.Errorf("no such key: tier")
	}}
	p := newPluginWith(t, client, &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{namedPolicy("widgets", "trigger.spec.tier != 'free'")}}, te, &stubWatchManager{})

	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden, got %v", err)
	}
	if strings.Contains(err.Error(), "no such key") {
		t.Fatalf("expression error leaked to the client: %v", err)
	}
	if !strings.Contains(err.Error(), `"widgets"`) {
		t.Fatalf("message should name the policy: %v", err)
	}
	if n := countActions(client, "create"); n != 0 {
		t.Fatalf("expected no claim, got %d creates", n)
	}
}

// Constraint evaluation runs under a deadline no longer than ConditionTimeout.
func TestConstraintEvaluationIsBounded(t *testing.T) {
	var deadline time.Time
	te := stubTemplateEngine{conditions: func(ctx context.Context, _ []quotav1alpha1.ConditionExpression) (bool, error) {
		deadline, _ = ctx.Deadline()
		<-ctx.Done()
		return false, ctx.Err()
	}}
	p := newPluginWith(t, newFakeDynamic(), &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{namedPolicy("widgets", "x")}}, te, &stubWatchManager{})
	start := time.Now()
	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden, got %v", err)
	}
	if deadline.IsZero() || deadline.Sub(start) > DefaultConditionTimeout+100*time.Millisecond {
		t.Fatalf("evaluation deadline %v not bounded by %v", deadline.Sub(start), DefaultConditionTimeout)
	}
}

// Before the policy cache is loaded every create is rejected with a
// retryable 503, never admitted unchecked.
func TestNotSyncedRejectsRetryably(t *testing.T) {
	client := newFakeDynamic()
	p := newPluginWith(t, client, &stubPolicyEngine{unsynced: true}, stubTemplateEngine{}, &stubWatchManager{})
	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	var status apierrors.APIStatus
	if !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("expected 503, got %v", err)
	}
	status = err.(apierrors.APIStatus)
	if status.Status().Code != http.StatusServiceUnavailable || status.Status().Details == nil || status.Status().Details.RetryAfterSeconds == 0 {
		t.Fatalf("expected Retry-After, got %+v", status.Status())
	}
	if countActions(client, "create") != 0 {
		t.Fatal("no claim may be created before policies are loaded")
	}
}

// Every matching policy gets its own claim; a policy whose constraints do
// not match is skipped.
func TestMultiplePoliciesAllEnforced(t *testing.T) {
	client := newFakeDynamic()
	var captured []*unstructured.Unstructured
	grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, &captured)
	te := stubTemplateEngine{conditions: func(_ context.Context, exprs []quotav1alpha1.ConditionExpression) (bool, error) {
		return len(exprs) == 0 || exprs[0].Expression != "false", nil
	}}
	pe := &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{
		namedPolicy("a", ""), namedPolicy("b", ""), namedPolicy("skipped", "false"),
	}}
	p := newPluginWith(t, client, pe, te, &stubWatchManager{})
	if err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}
	var got []string
	for _, c := range captured {
		got = append(got, c.GetLabels()["quota.miloapis.com/policy"])
	}
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("claims created for policies %v, want a,b", got)
	}
}

// When a later policy's claim is denied, the claim already granted for the
// same request is deleted so the rejected create does not hold capacity.
func TestMultiplePoliciesReleaseOnDenial(t *testing.T) {
	client := newFakeDynamic()
	client.PrependReactor("create", "quotaclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		obj := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		status, reason := "True", quotav1alpha1.QuotaClaimGrantedReason
		if obj.GetLabels()["quota.miloapis.com/policy"] == "b" {
			status, reason = "False", quotav1alpha1.QuotaClaimDeniedReason
		}
		_ = unstructured.SetNestedSlice(obj.Object, []interface{}{map[string]interface{}{
			"type": quotav1alpha1.QuotaClaimGranted, "status": status, "reason": reason,
		}}, "status", "conditions")
		return true, obj, nil
	})
	var deleted []string
	client.PrependReactor("delete", "quotaclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleted = append(deleted, action.(k8stesting.DeleteAction).GetName())
		return true, nil, nil
	})
	pe := &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{namedPolicy("a", ""), namedPolicy("b", "")}}
	p := newPluginWith(t, client, pe, stubTemplateEngine{}, &stubWatchManager{})
	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "reached your quota") {
		t.Fatalf("expected quota denial, got %v", err)
	}
	if strings.Join(deleted, ",") != "w1-a-claim" {
		t.Fatalf("deleted %v, want the granted claim of policy a", deleted)
	}
}

// The claim records the UID of the object being created.
func TestClaimRecordsResourceUID(t *testing.T) {
	client := newFakeDynamic()
	var captured []*unstructured.Unstructured
	grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, &captured)
	p := newPluginWith(t, client, &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{namedPolicy("widgets", "")}},
		stubTemplateEngine{randomNames: true}, &stubWatchManager{})

	attrs := widgetAttrs("w1", "team-a", nil)
	attrs.GetObject().(*unstructured.Unstructured).SetUID("uid-1")
	if err := p.Validate(context.Background(), attrs, nil); err != nil {
		t.Fatal(err)
	}
	if len(captured) != 1 || captured[0].GetAnnotations()[ResourceUIDAnnotation] != "uid-1" {
		t.Fatalf("claim annotations = %v", captured[0].GetAnnotations())
	}
	if !strings.HasPrefix(captured[0].GetName(), "widgets-claim-") {
		t.Fatalf("expected a generated claim name, got %q", captured[0].GetName())
	}
}

// A granted claim left by an earlier attempt at a pinned name is re-pointed
// at the new object's UID, so the ownership controller attaches it instead
// of deleting it as belonging to a different object.
func TestExistingGrantedClaimIsAdopted(t *testing.T) {
	existing := claimObj("w1-claim", "team-a", true, "True", quotav1alpha1.QuotaClaimGrantedReason)
	existing.SetAnnotations(map[string]string{ResourceUIDAnnotation: "old-uid"})
	client := newFakeDynamic(existing)
	p := newPluginWith(t, client, &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{namedPolicy("widgets", "")}},
		stubTemplateEngine{}, &stubWatchManager{})

	attrs := widgetAttrs("w1", "team-a", nil)
	attrs.GetObject().(*unstructured.Unstructured).SetUID("new-uid")
	if err := p.Validate(context.Background(), attrs, nil); err != nil {
		t.Fatal(err)
	}
	got, err := client.Resource(quotaClaimGVR).Namespace("team-a").Get(context.Background(), "w1-claim", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetAnnotations()[ResourceUIDAnnotation] != "new-uid" {
		t.Fatalf("claim not adopted: %v", got.GetAnnotations())
	}
	if countActions(client, "create") != 0 {
		t.Fatal("reused claim must not be recreated")
	}
}

// A policy that pins a claim namespace different from a namespaced
// resource's namespace is rejected: the claim could not be owned by the
// resource.
func TestCrossNamespaceClaimRejected(t *testing.T) {
	client := newFakeDynamic()
	grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, nil)
	p := newPluginWith(t, client, &stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{testPolicy(tenant, "billing")}},
		stubTemplateEngine{}, &stubWatchManager{})
	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "misconfigured") {
		t.Fatalf("expected misconfigured denial, got %v", err)
	}
	if countActions(client, "create") != 0 {
		t.Fatal("no claim may be created")
	}
}
