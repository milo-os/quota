package policy

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"go.miloapis.com/quota/pkg/admission/enforcement"
	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func dedupeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := quotav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func widgetTrigger() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("example.com/v1")
	u.SetKind("Widget")
	u.SetNamespace("ns")
	u.SetName("w1")
	u.SetUID("uid-1")
	return u
}

func renderedClaim(name, generateName string) *quotav1alpha1.QuotaClaim {
	return &quotav1alpha1.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, GenerateName: generateName, Namespace: "ns"},
		Spec: quotav1alpha1.QuotaClaimSpec{
			ConsumerRef: quotav1alpha1.ConsumerRef{APIGroup: "example.com", Kind: "Project", Name: "p"},
			ResourceRef: quotav1alpha1.UnversionedObjectReference{APIGroup: "example.com", Kind: "Widget", Name: "w1", Namespace: "ns"},
			Requests:    []quotav1alpha1.ResourceRequest{{ResourceType: "example.com/widgets", Amount: 1}},
		},
	}
}

// admissionClaim is the claim the enforcement plugin created for the
// trigger before it was persisted.
func admissionClaim(name string) *quotav1alpha1.QuotaClaim {
	c := renderedClaim(name, "")
	c.Labels = map[string]string{policyLabel: "widgets"}
	return c
}

// The controller must not create a second claim for a trigger admission
// already charged, whatever the naming scheme, and must still create one
// for a trigger nobody has charged (an object that predates the policy).
func TestCreateOrUpdateClaimDoesNotDoubleCharge(t *testing.T) {
	policy := &quotav1alpha1.ClaimCreationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "widgets"}}
	cases := []struct {
		name       string
		rendered   *quotav1alpha1.QuotaClaim
		cached     []client.Object
		live       []client.Object
		wantClaims int
	}{
		{name: "generateName, admission claim cached", rendered: renderedClaim("", "widgets-claim-"),
			cached: []client.Object{admissionClaim("widgets-claim-abcde")}, live: []client.Object{admissionClaim("widgets-claim-abcde")}, wantClaims: 1},
		{name: "generateName, admission claim not yet cached", rendered: renderedClaim("", "widgets-claim-"),
			live: []client.Object{admissionClaim("widgets-claim-abcde")}, wantClaims: 0},
		{name: "pinned name, admission claim exists", rendered: renderedClaim("widget-w1", ""),
			cached: []client.Object{admissionClaim("widget-w1")}, live: []client.Object{admissionClaim("widget-w1")}, wantClaims: 1},
		{name: "pinned name needing sanitizing matches admission's name", rendered: renderedClaim("Widget-W1", ""),
			cached: []client.Object{func() client.Object {
				c := admissionClaim("ignored")
				c.Name = sanitizedName("Widget-W1")
				c.Labels = nil
				return c
			}()},
			wantClaims: 1},
		{name: "uncharged trigger gets a claim", rendered: renderedClaim("", "widgets-claim-"), wantClaims: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := dedupeScheme(t)
			cache := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.cached...).Build()
			live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.live...).Build()
			r := &ClaimCreationController{Scheme: scheme, EventRecorder: events.NewFakeRecorder(10)}

			if err := r.createOrUpdateClaim(context.Background(), cache, live, tc.rendered, policy, widgetTrigger()); err != nil {
				t.Fatalf("createOrUpdateClaim: %v", err)
			}
			var got quotav1alpha1.QuotaClaimList
			if err := cache.List(context.Background(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Items) != tc.wantClaims {
				t.Fatalf("cache client holds %d claims, want %d: %+v", len(got.Items), tc.wantClaims, got.Items)
			}
		})
	}
}

func sanitizedName(n string) string {
	c := renderedClaim(n, "")
	return enforcement.ClaimName(c.Name)
}
