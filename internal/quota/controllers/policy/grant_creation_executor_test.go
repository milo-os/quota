package policy

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	"go.miloapis.com/quota/internal/quota/engine"
	"go.miloapis.com/quota/internal/quota/informer"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// Interface embedding gives the fakes every method; tests only call the
// ones overridden here.
type fakeLocalManager struct {
	manager.Manager
	c client.Client
}

func (f fakeLocalManager) GetClient() client.Client { return f.c }

type fakeMCManager struct {
	mcmanager.Manager
	local manager.Manager
}

func (f fakeMCManager) GetLocalManager() manager.Manager { return f.local }

type watchCall struct {
	op  string
	gvk schema.GroupVersionKind
	id  string
}

type recordingInformerManager struct {
	informer.Manager
	calls []watchCall
}

func (m *recordingInformerManager) AddWatch(_ context.Context, req informer.WatchRequest) error {
	m.calls = append(m.calls, watchCall{"add", req.GVK, req.ConsumerID})
	return nil
}

func (m *recordingInformerManager) RemoveWatch(_ context.Context, gvk schema.GroupVersionKind, id string) error {
	m.calls = append(m.calls, watchCall{"remove", gvk, id})
	return nil
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := quotav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func grantPolicy(name, apiVersion, kind string) *quotav1alpha1.GrantCreationPolicy {
	p := &quotav1alpha1.GrantCreationPolicy{ObjectMeta: metav1.ObjectMeta{Name: name}}
	p.Spec.Trigger.Resource = quotav1alpha1.GrantTriggerResource{APIVersion: apiVersion, Kind: kind}
	p.Spec.Target.ResourceGrantTemplate.Metadata.Namespace = "grants"
	p.Spec.Target.ResourceGrantTemplate.Spec.ConsumerRef = quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Org", Name: "{{trigger.metadata.name}}"}
	return p
}

func trigger(name, uid string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("example.com/v1")
	u.SetKind("Org")
	u.SetName(name)
	u.SetUID(types.UID(uid))
	return u
}

func newGrantController(t *testing.T, objs ...client.Object) (*GrantCreationController, client.Client, *recordingInformerManager) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
	celEngine, err := engine.NewCELEngine()
	if err != nil {
		t.Fatal(err)
	}
	im := &recordingInformerManager{}
	r := NewGrantCreationController(
		fakeMCManager{local: fakeLocalManager{c: c}},
		testScheme(t),
		engine.NewTemplateEngine(celEngine, logrDiscard()),
		celEngine, nil, events.NewFakeRecorder(100), im)
	return r, c, im
}

// Deleting a trigger deletes the grant its policy produced. Grants created
// in a parent context carry no owner reference, so this is the only path
// that removes them; it runs the same way for a local target.
func TestGrantDeletedWithTrigger(t *testing.T) {
	policy := grantPolicy("orgs", "example.com/v1", "Org")
	r, c, _ := newGrantController(t, policy)
	ctx := context.Background()
	org := trigger("acme", "uid-acme")

	r.processTriggerResource(org, policy.Name, "ADD")
	var grant quotav1alpha1.QuotaGrant
	key := client.ObjectKey{Namespace: "grants", Name: "orgs-acme-grant"}
	if err := c.Get(ctx, key, &grant); err != nil {
		t.Fatalf("grant not created: %v", err)
	}
	if grant.Labels[grantPolicyLabel] != "orgs" || grant.Labels[grantTriggerUIDLabel] != "uid-acme" {
		t.Fatalf("grant labels = %v", grant.Labels)
	}

	// A different object that reused the name does not delete the grant.
	r.processTriggerResource(trigger("acme", "uid-other"), policy.Name, "DELETE")
	if err := c.Get(ctx, key, &grant); err != nil {
		t.Fatalf("grant of another trigger UID was deleted: %v", err)
	}

	r.processTriggerResource(org, policy.Name, "DELETE")
	if err := c.Get(ctx, key, &grant); !apierrors.IsNotFound(err) {
		t.Fatalf("grant not deleted with its trigger: %v", err)
	}
}

// A trigger that stops matching the constraints has its grant deleted, even
// when the template sets no labels of its own.
func TestGrantDeletedWhenTriggerStopsMatching(t *testing.T) {
	policy := grantPolicy("orgs", "example.com/v1", "Org")
	policy.Spec.Trigger.Constraints = []quotav1alpha1.ConditionExpression{{Expression: "!has(trigger.spec) || trigger.spec.tier != 'free'"}}
	r, c, _ := newGrantController(t, policy)
	ctx := context.Background()
	org := trigger("acme", "uid-acme")
	r.processTriggerResource(org, policy.Name, "ADD")

	free := org.DeepCopy()
	_ = unstructured.SetNestedField(free.Object, "free", "spec", "tier")
	r.processTriggerResource(free, policy.Name, "UPDATE")
	var grant quotav1alpha1.QuotaGrant
	if err := c.Get(ctx, client.ObjectKey{Namespace: "grants", Name: "orgs-acme-grant"}, &grant); !apierrors.IsNotFound(err) {
		t.Fatalf("grant kept after the trigger stopped matching: %v", err)
	}
}

// Watches are removed after the policy is gone and moved when it is
// retargeted.
func TestGrantPolicyWatchTracking(t *testing.T) {
	r, _, im := newGrantController(t)
	ctx := context.Background()
	v1 := grantPolicy("orgs", "example.com/v1", "Org")
	if err := r.addWatchForPolicy(ctx, v1); err != nil {
		t.Fatal(err)
	}
	retargeted := grantPolicy("orgs", "example.com/v1", "Team")
	if err := r.addWatchForPolicy(ctx, retargeted); err != nil {
		t.Fatal(err)
	}
	// Policy deleted: it no longer exists in the API, the watch must go.
	if err := r.removeWatchForPolicy(ctx, "orgs"); err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(im.calls)
	want := fmt.Sprint([]watchCall{
		{"add", v1.Spec.Trigger.Resource.GetGVK(), "grant-creation-policy-orgs"},
		{"remove", v1.Spec.Trigger.Resource.GetGVK(), "grant-creation-policy-orgs"},
		{"add", retargeted.Spec.Trigger.Resource.GetGVK(), "grant-creation-policy-orgs"},
		{"remove", retargeted.Spec.Trigger.Resource.GetGVK(), "grant-creation-policy-orgs"},
	})
	if got != want {
		t.Fatalf("watch calls\n got %s\nwant %s", got, want)
	}
}

func logrDiscard() logr.Logger { return logr.Discard() }
