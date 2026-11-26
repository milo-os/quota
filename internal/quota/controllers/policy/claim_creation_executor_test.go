package policy

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// removeWatchForPolicy must work after the policy is deleted (it used to
// re-read the deleted policy and silently keep the watch), and a retarget
// moves the watch.
func TestClaimPolicyWatchTracking(t *testing.T) {
	im := &recordingInformerManager{}
	r := NewClaimCreationController(nil, nil, nil, nil, nil, im)
	ctx := context.Background()

	p := &quotav1alpha1.ClaimCreationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "widgets"}}
	p.Spec.Trigger.Resource = quotav1alpha1.ClaimTriggerResource{APIVersion: "example.com/v1", Kind: "Widget"}
	if err := r.addWatchForPolicy(ctx, p); err != nil {
		t.Fatal(err)
	}
	moved := p.DeepCopy()
	moved.Spec.Trigger.Resource.Kind = "Gadget"
	if err := r.addWatchForPolicy(ctx, moved); err != nil {
		t.Fatal(err)
	}
	if err := r.removeWatchForPolicy(ctx, "widgets"); err != nil {
		t.Fatal(err)
	}
	if len(im.calls) != 4 ||
		im.calls[1].op != "remove" || im.calls[1].gvk.Kind != "Widget" ||
		im.calls[3].op != "remove" || im.calls[3].gvk.Kind != "Gadget" {
		t.Fatalf("watch calls = %v", im.calls)
	}
}
