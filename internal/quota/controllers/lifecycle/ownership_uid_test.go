package lifecycle

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func owner(uid string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("example.com/v1")
	u.SetKind("Widget")
	u.SetName("foo")
	u.SetUID(types.UID(uid))
	return u
}

func claimFor(uid string) *quotav1alpha1.QuotaClaim {
	c := autoClaim("foo-claim", "team-a", "True", quotav1alpha1.QuotaClaimGrantedReason, time.Now())
	c.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{APIGroup: "example.com", Kind: "Widget", Name: "foo", Namespace: "team-a"}
	if uid != "" {
		c.Annotations[resourceUIDAnnotation] = uid
	}
	return c
}

func TestDecideOwnership(t *testing.T) {
	cases := []struct {
		name    string
		claim   *quotav1alpha1.QuotaClaim
		owner   *unstructured.Unstructured
		ownerNS string
		want    ownershipAction
	}{
		{"matching UID attaches", claimFor("uid-1"), owner("uid-1"), "team-a", attachOwner},
		{"claim without UID falls back to the name", claimFor(""), owner("uid-1"), "team-a", attachOwner},
		{"name reused by another object", claimFor("uid-old"), owner("uid-new"), "team-a", deleteStaleClaim},
		{"cross-namespace owner is never set", claimFor("uid-1"), owner("uid-1"), "other", skipCrossNamespace},
		{"cluster-scoped owner attaches", claimFor("uid-1"), owner("uid-1"), "", attachOwner},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideOwnership(tc.claim, tc.owner, tc.ownerNS); got != tc.want {
				t.Fatalf("decideOwnership = %v, want %v", got, tc.want)
			}
		})
	}
}

// A granted claim from an attempt whose create never landed is deleted when
// a different object now holds the name, instead of being attached to it
// (which would charge the new object twice).
func TestStaleClaimForReusedNameIsDeleted(t *testing.T) {
	stale := claimFor("uid-old")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(stale).Build()
	r := &QuotaClaimOwnershipController{}

	var got quotav1alpha1.QuotaClaim
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stale), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.actOnOwner(context.Background(), c, &got, owner("uid-new"), "team-a", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stale), &got); !apierrors.IsNotFound(err) {
		t.Fatalf("stale claim not deleted: %v", err)
	}
}

// A cross-namespace owner is never attached, so the garbage collector does
// not delete the claim while its resource still exists.
func TestCrossNamespaceOwnerNotAttached(t *testing.T) {
	claim := claimFor("uid-1")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(claim).Build()
	r := &QuotaClaimOwnershipController{}
	var got quotav1alpha1.QuotaClaim
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(claim), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.actOnOwner(context.Background(), c, &got, owner("uid-1"), "other", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(claim), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.OwnerReferences) != 0 {
		t.Fatalf("owner references set across namespaces: %v", got.OwnerReferences)
	}
}
