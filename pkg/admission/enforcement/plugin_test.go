package enforcement

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// --- test doubles -----------------------------------------------------------

type stubPolicyEngine struct {
	policies []*quotav1alpha1.ClaimCreationPolicy
	unsynced bool
}

func (s *stubPolicyEngine) GetPoliciesForKind(schema.GroupKind) ([]*quotav1alpha1.ClaimCreationPolicy, error) {
	if s.unsynced {
		return nil, fmt.Errorf("not synced")
	}
	return s.policies, nil
}
func (s *stubPolicyEngine) HasSynced() bool             { return !s.unsynced }
func (s *stubPolicyEngine) Start(context.Context) error { return nil }
func (s *stubPolicyEngine) Close()                      {}

// stubTemplateEngine renders a fixed claim name with the policy's consumerRef.
// conditions, when set, scripts EvaluateConditions per policy constraint
// expression.
type stubTemplateEngine struct {
	conditions func(ctx context.Context, exprs []quotav1alpha1.ConditionExpression) (bool, error)
	// randomNames leaves the claim name empty, like the real engine's
	// default generateName.
	randomNames bool
}

func (s stubTemplateEngine) RenderClaim(policy *quotav1alpha1.ClaimCreationPolicy, ec *EvaluationContext) (*quotav1alpha1.QuotaClaim, error) {
	tmpl := policy.Spec.Target.ResourceClaimTemplate
	name := ec.Object.GetName() + "-claim"
	if policy.Name != "widgets" {
		name = ec.Object.GetName() + "-" + policy.Name + "-claim"
	}
	generateName := ""
	if s.randomNames {
		name, generateName = "", policy.Name+"-claim-"
	}
	return &quotav1alpha1.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:         name,
			GenerateName: generateName,
			Namespace:    tmpl.Metadata.Namespace,
		},
		Spec: quotav1alpha1.QuotaClaimSpec{
			ConsumerRef: tmpl.Spec.ConsumerRef,
			Requests:    []quotav1alpha1.ResourceRequest{{ResourceType: "example.com/widgets", Amount: 1}},
		},
	}, nil
}

func (s stubTemplateEngine) EvaluateConditions(ctx context.Context, exprs []quotav1alpha1.ConditionExpression, _ *unstructured.Unstructured) (bool, error) {
	if s.conditions != nil {
		return s.conditions(ctx, exprs)
	}
	return true, nil
}

// stubWatchManager records registrations and delivers a scripted result.
type stubWatchManager struct {
	mu         sync.Mutex
	registered []string
	result     *ClaimResult // nil: never deliver
}

func (s *stubWatchManager) RegisterClaimWaiter(_ context.Context, name, ns string, _ time.Duration) (<-chan ClaimResult, context.CancelFunc, error) {
	s.mu.Lock()
	s.registered = append(s.registered, ns+"/"+name)
	s.mu.Unlock()
	ch := make(chan ClaimResult, 1)
	if s.result != nil {
		ch <- *s.result
	}
	return ch, func() {}, nil
}
func (s *stubWatchManager) UnregisterClaimWaiter(string, string) {}
func (s *stubWatchManager) Start(context.Context) error          { return nil }

// --- helpers ----------------------------------------------------------------

var widgetGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}

func testPolicy(consumer quotav1alpha1.ConsumerRef, claimNamespace string) *quotav1alpha1.ClaimCreationPolicy {
	p := &quotav1alpha1.ClaimCreationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "widgets"}}
	p.Spec.Target.ResourceClaimTemplate.Spec.ConsumerRef = consumer
	p.Spec.Target.ResourceClaimTemplate.Metadata.Namespace = claimNamespace
	return p
}

var tenant = quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Tenant", Name: "acme"}

func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{quotaClaimGVR: "QuotaClaimList"}, objs...)
}

func newTestPlugin(t *testing.T, client dynamic.Interface, policy *quotav1alpha1.ClaimCreationPolicy, wm ClaimWatchManager, opts ...Option) *QuotaEnforcementPlugin {
	t.Helper()
	opts = append([]Option{
		WithLogger(logr.Discard()),
		WithWatchManagerFactory(func(dynamic.Interface, logr.Logger, string) ClaimWatchManager { return wm }),
	}, opts...)
	var policies []*quotav1alpha1.ClaimCreationPolicy
	if policy != nil {
		policies = append(policies, policy)
	}
	p, err := NewQuotaEnforcementPlugin(&stubPolicyEngine{policies: policies}, stubTemplateEngine{}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	p.SetDynamicClient(client)
	return p
}

func widgetAttrs(name, namespace string, extra map[string][]string) admission.Attributes {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("example.com/v1")
	obj.SetKind("Widget")
	obj.SetName(name)
	obj.SetNamespace(namespace)
	return admission.NewAttributesRecord(obj, nil, widgetGVK, namespace, name,
		schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"},
		"", admission.Create, nil, false, &user.DefaultInfo{Name: "alice", Extra: extra})
}

func claimObj(name, namespace string, autoCreated bool, granted string, reason string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(quotav1alpha1.SchemeGroupVersion.String())
	u.SetKind("QuotaClaim")
	u.SetName(name)
	u.SetNamespace(namespace)
	if autoCreated {
		u.SetLabels(map[string]string{"quota.miloapis.com/auto-created": "true"})
	}
	if granted != "" {
		_ = unstructured.SetNestedSlice(u.Object, []interface{}{map[string]interface{}{
			"type": quotav1alpha1.QuotaClaimGranted, "status": granted, "reason": reason, "message": "limit 1",
		}}, "status", "conditions")
	}
	return u
}

// grantOnCreate makes Create return the claim with the given Granted status,
// as the synchronous postgres allocator does.
func grantOnCreate(client *dynamicfake.FakeDynamicClient, status, reason string, captured *[]*unstructured.Unstructured) {
	client.PrependReactor("create", "quotaclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		obj := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		if captured != nil {
			*captured = append(*captured, obj.DeepCopy())
		}
		if status != "" {
			_ = unstructured.SetNestedSlice(obj.Object, []interface{}{map[string]interface{}{
				"type": quotav1alpha1.QuotaClaimGranted, "status": status, "reason": reason, "message": "limit 1",
			}}, "status", "conditions")
		}
		return true, obj, nil
	})
}

func countActions(client *dynamicfake.FakeDynamicClient, verb string) int {
	n := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == verb && a.GetResource() == quotaClaimGVR {
			n++
		}
	}
	return n
}

// --- tests ------------------------------------------------------------------

// TestCreateResponseDecides covers 6d: when Create already returns a final
// status the plugin does not wait on the watch.
func TestCreateResponseDecides(t *testing.T) {
	cases := []struct {
		name      string
		status    string
		reason    string
		wantAllow bool
		wantMsg   string
	}{
		{"granted in create response", "True", quotav1alpha1.QuotaClaimGrantedReason, true, ""},
		{"denied in create response", "False", quotav1alpha1.QuotaClaimDeniedReason, false, "reached your quota"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeDynamic()
			grantOnCreate(client, tc.status, tc.reason, nil)
			wm := &stubWatchManager{} // never delivers; a wait would hang until the context ends
			p := newTestPlugin(t, client, testPolicy(tenant, ""), wm)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := p.Validate(ctx, widgetAttrs("w1", "team-a", nil), nil)
			if tc.wantAllow && err != nil {
				t.Fatalf("expected allow, got %v", err)
			}
			if !tc.wantAllow {
				if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), tc.wantMsg) {
					t.Fatalf("expected forbidden containing %q, got %v", tc.wantMsg, err)
				}
			}
			if ctx.Err() != nil {
				t.Fatalf("plugin waited on the watch instead of using the create response")
			}
		})
	}
}

// TestWatchDecides covers the controller-mode path: Create returns a pending
// claim and the result comes from the watch.
func TestWatchDecides(t *testing.T) {
	client := newFakeDynamic()
	wm := &stubWatchManager{result: &ClaimResult{Granted: true, Reason: "QuotaAvailable"}}
	p := newTestPlugin(t, client, testPolicy(tenant, ""), wm)
	if err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}
	if len(wm.registered) != 1 || wm.registered[0] != "team-a/w1-claim" {
		t.Fatalf("waiter registrations = %v", wm.registered)
	}

	timeoutWM := &stubWatchManager{result: &ClaimResult{Reason: "timeout", Error: fmt.Errorf("timed out")}}
	p = newTestPlugin(t, newFakeDynamic(), testPolicy(tenant, ""), timeoutWM)
	err := p.Validate(context.Background(), widgetAttrs("w2", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "took too long") {
		t.Fatalf("expected timeout message, got %v", err)
	}
}

// TestExistingClaimResolution covers 6a: retries that meet a claim from a
// previous attempt.
func TestExistingClaimResolution(t *testing.T) {
	cases := []struct {
		name        string
		existing    *unstructured.Unstructured
		createErr   error
		wantAllow   bool
		wantMsg     string
		wantCreates int
		wantDeletes int
	}{
		{
			name:        "granted auto-created claim short-circuits",
			existing:    claimObj("w1-claim", "team-a", true, "True", quotav1alpha1.QuotaClaimGrantedReason),
			wantAllow:   true,
			wantCreates: 0,
		},
		{
			name:        "denied auto-created claim is a conflict",
			existing:    claimObj("w1-claim", "team-a", true, "False", quotav1alpha1.QuotaClaimDeniedReason),
			wantMsg:     "still being cleaned up",
			wantCreates: 0,
		},
		{
			name:        "stale pending auto-created claim is replaced",
			existing:    claimObj("w1-claim", "team-a", true, "", ""),
			wantAllow:   true,
			wantCreates: 1,
			wantDeletes: 1,
		},
		{
			name:        "AlreadyExists on create is a conflict",
			createErr:   apierrors.NewAlreadyExists(quotaClaimGVR.GroupResource(), "w1-claim"),
			wantMsg:     "concurrent quota evaluation",
			wantCreates: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var objs []runtime.Object
			if tc.existing != nil {
				objs = append(objs, tc.existing)
			}
			client := newFakeDynamic(objs...)
			if tc.createErr != nil {
				client.PrependReactor("create", "quotaclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.createErr
				})
			} else {
				grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, nil)
			}
			p := newTestPlugin(t, client, testPolicy(tenant, ""), &stubWatchManager{})

			err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
			if tc.wantAllow && err != nil {
				t.Fatalf("expected allow, got %v", err)
			}
			if !tc.wantAllow && (!apierrors.IsForbidden(err) || !strings.Contains(err.Error(), tc.wantMsg)) {
				t.Fatalf("expected forbidden containing %q, got %v", tc.wantMsg, err)
			}
			if got := countActions(client, "create"); got != tc.wantCreates {
				t.Errorf("creates = %d, want %d", got, tc.wantCreates)
			}
			if got := countActions(client, "delete"); got != tc.wantDeletes {
				t.Errorf("deletes = %d, want %d", got, tc.wantDeletes)
			}
		})
	}
}

// TestManualClaimWithSameNameIsNotTouched verifies resolveExistingClaim
// ignores claims without the auto-created marker.
func TestManualClaimWithSameNameIsNotTouched(t *testing.T) {
	client := newFakeDynamic(claimObj("w1-claim", "team-a", false, "", ""))
	p := newTestPlugin(t, client, testPolicy(tenant, ""), &stubWatchManager{})
	err := p.Validate(context.Background(), widgetAttrs("w1", "team-a", nil), nil)
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "concurrent quota evaluation") {
		t.Fatalf("expected create conflict, got %v", err)
	}
	if countActions(client, "delete") != 0 {
		t.Fatalf("manual claim must never be deleted")
	}
}

// TestClusterScopedClaimNamespace covers 6b.
func TestClusterScopedClaimNamespace(t *testing.T) {
	t.Run("defaults to configured namespace", func(t *testing.T) {
		client := newFakeDynamic()
		var created []*unstructured.Unstructured
		grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, &created)
		p := newTestPlugin(t, client, testPolicy(tenant, ""), &stubWatchManager{})
		if err := p.Validate(context.Background(), widgetAttrs("w1", "", nil), nil); err != nil {
			t.Fatalf("expected allow, got %v", err)
		}
		if len(created) != 1 || created[0].GetNamespace() != DefaultClusterScopedClaimNamespace {
			t.Fatalf("claim namespace = %v, want %s", created, DefaultClusterScopedClaimNamespace)
		}
	})
	t.Run("policy-pinned namespace wins", func(t *testing.T) {
		client := newFakeDynamic()
		var created []*unstructured.Unstructured
		grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, &created)
		p := newTestPlugin(t, client, testPolicy(tenant, "claims"), &stubWatchManager{})
		if err := p.Validate(context.Background(), widgetAttrs("w1", "", nil), nil); err != nil {
			t.Fatalf("expected allow, got %v", err)
		}
		if created[0].GetNamespace() != "claims" {
			t.Fatalf("claim namespace = %s, want claims", created[0].GetNamespace())
		}
	})
	t.Run("cleared default is a misconfiguration", func(t *testing.T) {
		client := newFakeDynamic()
		cfg := DefaultConfig()
		cfg.ClusterScopedClaimNamespace = ""
		p := newTestPlugin(t, client, testPolicy(tenant, ""), &stubWatchManager{}, WithConfig(cfg))
		err := p.Validate(context.Background(), widgetAttrs("w1", "", nil), nil)
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "misconfigured") {
			t.Fatalf("expected misconfigured, got %v", err)
		}
		if countActions(client, "create") != 0 {
			t.Fatalf("no claim may be created")
		}
	})
}

// TestConsumerResolution covers 6c: template, resolver, user extra,
// namespace annotation, and denial when nothing resolves.
func TestConsumerResolution(t *testing.T) {
	empty := quotav1alpha1.ConsumerRef{}
	mapping := ConsumerMapping{
		UserExtraRef:        []ConsumerRefSource{{NameKey: "example.com/parent-name", KindKey: "example.com/parent-type", APIGroupKey: "example.com/parent-api-group"}},
		UserExtra:           []ConsumerSource{{Key: "example.com/tenant", APIGroup: "example.com", Kind: "Tenant"}},
		NamespaceAnnotation: []ConsumerSource{{Key: "example.com/owner", APIGroup: "example.com", Kind: "Owner"}},
	}
	lookup := func(_ context.Context, ns string) (map[string]string, error) {
		if ns == "annotated" {
			return map[string]string{"example.com/owner": "bob"}, nil
		}
		return nil, nil
	}

	cases := []struct {
		name      string
		template  quotav1alpha1.ConsumerRef
		resolver  ConsumerResolver
		namespace string
		extra     map[string][]string
		want      quotav1alpha1.ConsumerRef
		wantDeny  bool
	}{
		{name: "template wins", template: tenant, extra: map[string][]string{"example.com/tenant": {"other"}}, namespace: "ns", want: tenant},
		{
			name:     "resolver before mapping",
			template: empty, namespace: "ns",
			resolver: func(context.Context, admission.Attributes) (quotav1alpha1.ConsumerRef, bool, error) {
				return quotav1alpha1.ConsumerRef{Kind: "Org", Name: "o1"}, true, nil
			},
			extra: map[string][]string{"example.com/tenant": {"t1"}},
			want:  quotav1alpha1.ConsumerRef{Kind: "Org", Name: "o1"},
		},
		{name: "user extra", template: empty, namespace: "ns", extra: map[string][]string{"example.com/tenant": {"t1"}},
			want: quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Tenant", Name: "t1"}},
		{name: "user extra ref", template: empty, namespace: "ns",
			extra: map[string][]string{
				"example.com/parent-name": {"p1"}, "example.com/parent-type": {"Project"},
				"example.com/parent-api-group": {"resourcemanager.example.com"}, "example.com/tenant": {"t1"},
			},
			want: quotav1alpha1.ConsumerRef{APIGroup: "resourcemanager.example.com", Kind: "Project", Name: "p1"}},
		{name: "user extra ref without kind falls through", template: empty, namespace: "ns",
			extra: map[string][]string{"example.com/parent-name": {"p1"}, "example.com/tenant": {"t1"}},
			want:  quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Tenant", Name: "t1"}},
		{name: "namespace annotation", template: empty, namespace: "annotated",
			want: quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Owner", Name: "bob"}},
		{name: "nothing resolves is denied", template: empty, namespace: "plain", wantDeny: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeDynamic()
			var created []*unstructured.Unstructured
			grantOnCreate(client, "True", quotav1alpha1.QuotaClaimGrantedReason, &created)
			cfg := DefaultConfig()
			cfg.ConsumerMapping = mapping
			opts := []Option{WithConfig(cfg), WithNamespaceLookup(lookup)}
			if tc.resolver != nil {
				opts = append(opts, WithConsumerResolver(tc.resolver))
			}
			p := newTestPlugin(t, client, testPolicy(tc.template, ""), &stubWatchManager{}, opts...)

			err := p.Validate(context.Background(), widgetAttrs("w1", tc.namespace, tc.extra), nil)
			if tc.wantDeny {
				if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "did not resolve a consumer") {
					t.Fatalf("expected consumer denial, got %v", err)
				}
				if len(created) != 0 {
					t.Fatalf("a claim with an empty consumer must never be created")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected allow, got %v", err)
			}
			var claim quotav1alpha1.QuotaClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(created[0].Object, &claim); err != nil {
				t.Fatal(err)
			}
			if claim.Spec.ConsumerRef != tc.want {
				t.Fatalf("consumerRef = %+v, want %+v", claim.Spec.ConsumerRef, tc.want)
			}
		})
	}

	t.Run("default config with empty template is denied", func(t *testing.T) {
		client := newFakeDynamic()
		p := newTestPlugin(t, client, testPolicy(empty, ""), &stubWatchManager{})
		err := p.Validate(context.Background(), widgetAttrs("w1", "ns", nil), nil)
		if !apierrors.IsForbidden(err) || countActions(client, "create") != 0 {
			t.Fatalf("expected denial without create, got %v", err)
		}
	})
}

// TestClientResolverRoutesPerScope covers 6f.
func TestClientResolverRoutesPerScope(t *testing.T) {
	root := newFakeDynamic()
	scoped := newFakeDynamic()
	grantOnCreate(scoped, "True", quotav1alpha1.QuotaClaimGrantedReason, nil)

	var factoryScopes []string
	wm := &stubWatchManager{}
	p, err := NewQuotaEnforcementPlugin(&stubPolicyEngine{policies: []*quotav1alpha1.ClaimCreationPolicy{testPolicy(tenant, "")}}, stubTemplateEngine{},
		WithLogger(logr.Discard()),
		WithClientResolver(func(context.Context) (string, dynamic.Interface, error) { return "project-1", scoped, nil }),
		WithWatchManagerFactory(func(_ dynamic.Interface, _ logr.Logger, scopeID string) ClaimWatchManager {
			factoryScopes = append(factoryScopes, scopeID)
			return wm
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	p.SetDynamicClient(root)

	for i := 0; i < 2; i++ {
		if err := p.Validate(context.Background(), widgetAttrs(fmt.Sprintf("w%d", i), "team-a", nil), nil); err != nil {
			t.Fatalf("expected allow, got %v", err)
		}
	}
	if countActions(root, "create") != 0 || countActions(scoped, "create") != 2 {
		t.Fatalf("claims went to the wrong client: root=%d scoped=%d", countActions(root, "create"), countActions(scoped, "create"))
	}
	if len(factoryScopes) != 1 || factoryScopes[0] != "project-1" {
		t.Fatalf("watch manager should be built once for the scope, got %v", factoryScopes)
	}
}

func TestClaimResultFromObject(t *testing.T) {
	cases := []struct {
		name string
		obj  *unstructured.Unstructured
		want *ClaimResult
	}{
		{"no status", claimObj("c", "n", true, "", ""), nil},
		{"granted", claimObj("c", "n", true, "True", "QuotaAvailable"), &ClaimResult{Granted: true, Reason: "QuotaAvailable"}},
		{"denied", claimObj("c", "n", true, "False", quotav1alpha1.QuotaClaimDeniedReason), &ClaimResult{Granted: false, Reason: "limit 1"}},
		{"pending", claimObj("c", "n", true, "False", quotav1alpha1.QuotaClaimPendingReason), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClaimResultFromObject(tc.obj)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
