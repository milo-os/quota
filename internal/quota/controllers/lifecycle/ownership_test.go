package lifecycle

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// grantedManualClaim returns a manually created QuotaClaim (no auto-created
// markers) with a Granted=True condition and no owner references.
func grantedManualClaim(name, namespace string) *quotav1alpha1.QuotaClaim {
	claim := manualClaim(name, namespace)
	claim.Status.Conditions = []metav1.Condition{{
		Type:               quotav1alpha1.QuotaClaimGranted,
		Status:             metav1.ConditionTrue,
		Reason:             quotav1alpha1.QuotaClaimGrantedReason,
		LastTransitionTime: metav1.Time{Time: time.Now()},
	}}
	return claim
}

// TestIsAutoCreatedClaim verifies the package-level helper correctly
// distinguishes admission-plugin claims from manually created ones.
func TestIsAutoCreatedClaim(t *testing.T) {
	cases := []struct {
		name  string
		claim *quotav1alpha1.QuotaClaim
		want  bool
	}{
		{
			name:  "both markers present",
			claim: autoClaim("auto", "default", "", "", time.Now()),
			want:  true,
		},
		{
			name:  "manual claim - no markers",
			claim: manualClaim("manual", "default"),
			want:  false,
		},
		{
			name: "label present, annotation missing",
			claim: &quotav1alpha1.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "label-only",
					Namespace: "default",
					Labels:    map[string]string{"quota.miloapis.com/auto-created": "true"},
				},
			},
			want: false,
		},
		{
			name: "annotation present, label missing",
			claim: &quotav1alpha1.QuotaClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "annotation-only",
					Namespace:   "default",
					Annotations: map[string]string{"quota.miloapis.com/created-by": "claim-creation-plugin"},
				},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAutoCreatedClaim(tc.claim); got != tc.want {
				t.Errorf("isAutoCreatedClaim() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQuotaClaimOwnershipController_Reconcile verifies that Reconcile skips
// manually created claims and only proceeds with admission-plugin claims.
func TestQuotaClaimOwnershipController_Reconcile(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	scheme := testScheme(t)

	cases := []struct {
		name        string
		claim       *quotav1alpha1.QuotaClaim
		wantErr     bool
		wantDeleted bool
	}{
		{
			name:        "manual claim with Granted=True is not touched",
			claim:       grantedManualClaim("manual-granted", "default"),
			wantErr:     false,
			wantDeleted: false,
		},
		{
			name: "claim with auto-created label but missing annotation is not touched",
			claim: func() *quotav1alpha1.QuotaClaim {
				c := grantedManualClaim("label-only", "default")
				c.Labels = map[string]string{"quota.miloapis.com/auto-created": "true"}
				return c
			}(),
			wantErr:     false,
			wantDeleted: false,
		},
		{
			name: "claim with created-by annotation but missing label is not touched",
			claim: func() *quotav1alpha1.QuotaClaim {
				c := grantedManualClaim("annotation-only", "default")
				c.Annotations = map[string]string{"quota.miloapis.com/created-by": "claim-creation-plugin"}
				return c
			}(),
			wantErr:     false,
			wantDeleted: false,
		},
		{
			// Auto-created claim passes the guard and reaches resolveOwner, which
			// returns an error because restMapper is nil in this unit test.
			// This confirms the guard does not block auto-created claims.
			name: "auto-created granted claim proceeds past guard to owner resolution",
			claim: func() *quotav1alpha1.QuotaClaim {
				c := autoClaim("auto-granted", "default", "True", quotav1alpha1.QuotaClaimGrantedReason, now.Add(-10*time.Second))
				c.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{Kind: "Widget", Name: "w1"}
				return c
			}(),
			wantErr:     true,
			wantDeleted: false,
		},
		{
			// An auto-created, granted, old claim with no resourceRef has no owner
			// to resolve and must never be treated as an orphan.
			name:        "auto-created granted claim without resourceRef is not touched",
			claim:       autoClaim("no-ref", "default", "True", quotav1alpha1.QuotaClaimGrantedReason, now.Add(-time.Hour)),
			wantErr:     false,
			wantDeleted: false,
		},
		{
			// Regression: a manual granted claim whose owner cannot be found must
			// not be deleted after the orphan max age.
			name: "old manual granted claim with unresolvable resourceRef is not deleted",
			claim: func() *quotav1alpha1.QuotaClaim {
				c := grantedManualClaim("manual-old", "default")
				c.CreationTimestamp = metav1.Time{Time: now.Add(-time.Hour)}
				c.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{Kind: "Widget", Name: "missing"}
				return c
			}(),
			wantErr:     false,
			wantDeleted: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.claim).Build()
			mgr := &testManager{cluster: &testCluster{client: cli}}

			r := &QuotaClaimOwnershipController{
				Scheme:  scheme,
				Manager: mgr,
				// restMapper intentionally nil so resolveOwner returns early with
				// "RESTMapper not initialized", which is the expected error for
				// auto-created claims that pass the guard in this unit test.
			}

			req := mcreconcile.Request{
				Request: ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      tc.claim.Name,
						Namespace: tc.claim.Namespace,
					},
				},
			}

			_, err := r.Reconcile(context.Background(), req)
			if (err != nil) != tc.wantErr {
				t.Errorf("Reconcile() error = %v, wantErr %v", err, tc.wantErr)
			}

			var got quotav1alpha1.QuotaClaim
			getErr := cli.Get(context.Background(), client.ObjectKey{Name: tc.claim.Name, Namespace: tc.claim.Namespace}, &got)
			if deleted := (getErr != nil); deleted != tc.wantDeleted {
				t.Errorf("deleted=%v want=%v (get err=%v)", deleted, tc.wantDeleted, getErr)
			}
		})
	}
}

func TestIsOwnershipCandidate(t *testing.T) {
	withRef := func(c *quotav1alpha1.QuotaClaim, kind, name string) *quotav1alpha1.QuotaClaim {
		c.Spec.ResourceRef = quotav1alpha1.UnversionedObjectReference{Kind: kind, Name: name}
		return c
	}
	cases := []struct {
		name  string
		claim *quotav1alpha1.QuotaClaim
		want  bool
	}{
		{"auto with ref", withRef(autoClaim("a", "ns", "", "", time.Now()), "Widget", "w"), true},
		{"auto without ref", autoClaim("b", "ns", "", "", time.Now()), false},
		{"auto with kind only", withRef(autoClaim("c", "ns", "", "", time.Now()), "Widget", ""), false},
		{"manual with ref", withRef(manualClaim("d", "ns"), "Widget", "w"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOwnershipCandidate(tc.claim); got != tc.want {
				t.Errorf("isOwnershipCandidate() = %v, want %v", got, tc.want)
			}
		})
	}
}
