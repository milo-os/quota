package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestFieldLabelConversions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for kind, labels := range SelectableFieldLabels {
		gvk := SchemeGroupVersion.WithKind(kind)
		want := append([]string{"metadata.name", "metadata.namespace"}, labels...)
		for _, label := range want {
			gotLabel, gotValue, err := scheme.ConvertFieldLabel(gvk, label, "v")
			if err != nil || gotLabel != label || gotValue != "v" {
				t.Errorf("%s: ConvertFieldLabel(%q) = %q, %q, %v", kind, label, gotLabel, gotValue, err)
			}
		}
		if _, _, err := scheme.ConvertFieldLabel(gvk, "spec.unknown", "v"); err == nil {
			t.Errorf("%s: unknown field label accepted", kind)
		}
	}
}
