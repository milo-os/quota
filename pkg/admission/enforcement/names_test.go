package enforcement

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"

	quotav1alpha1 "go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

func TestClaimNameForIsValidSubdomain(t *testing.T) {
	long := strings.Repeat("a", 300)
	cases := []struct {
		name         string
		renderedName string
		renderedGen  string
		objectName   string
		kind         string
		wantExact    string
		wantPrefix   string
	}{
		{name: "valid pinned name kept", renderedName: "web-claim", wantExact: "web-claim"},
		{name: "uppercase pinned name", renderedName: "Web-Claim"},
		{name: "colon pinned name", renderedName: "system:controller:web"},
		{name: "very long pinned name", renderedName: long},
		{name: "dots and dashes", renderedName: "-a..b-.c-"},
		{name: "only invalid characters", renderedName: ":::"},
		{name: "valid generateName kept", renderedGen: "web-", wantPrefix: "web-"},
		{name: "uppercase generateName", renderedGen: "Web:Claim-", wantPrefix: "web-claim-"},
		{name: "derived from uppercase object", objectName: "MyWidget", kind: "Widget", wantPrefix: "mywidget-widget-claim-"},
		{name: "derived from colon object", objectName: "system:admin", kind: "ClusterRole", wantPrefix: "system-admin-clusterrole-"},
		{name: "derived from long object", objectName: long, kind: "Widget"},
	}
	p := &QuotaEnforcementPlugin{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
			obj.SetName(tc.objectName)
			ec := &EvaluationContext{Object: obj, GVK: schema.GroupVersionKind{Kind: tc.kind}}
			rendered := &quotav1alpha1.QuotaClaim{}
			rendered.Name = tc.renderedName
			rendered.GenerateName = tc.renderedGen

			got := p.claimNameFor(rendered, ec)
			if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
				t.Fatalf("claimNameFor = %q, not a DNS subdomain: %v", got, errs)
			}
			if tc.wantExact != "" && got != tc.wantExact {
				t.Fatalf("claimNameFor = %q, want %q", got, tc.wantExact)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("claimNameFor = %q, want prefix %q", got, tc.wantPrefix)
			}
		})
	}
}

// Pinned names are deterministic and must not collide after sanitizing.
func TestSanitizeObjectNameKeepsDistinctNamesDistinct(t *testing.T) {
	long := strings.Repeat("b", 300)
	pairs := [][2]string{
		{"Web", "web"},
		{"a:b", "a-b"},
		{long + "1", long + "2"},
	}
	for _, pr := range pairs {
		a := sanitizeObjectName(pr[0], validation.DNS1123SubdomainMaxLength)
		b := sanitizeObjectName(pr[1], validation.DNS1123SubdomainMaxLength)
		if a == b {
			t.Errorf("sanitize(%q) == sanitize(%q) == %q", pr[0], pr[1], a)
		}
		for _, n := range []string{a, b} {
			if len(n) > validation.DNS1123SubdomainMaxLength {
				t.Errorf("%q is %d characters", n, len(n))
			}
		}
	}
	if got := sanitizeObjectName("Web", 253); got != sanitizeObjectName("Web", 253) {
		t.Errorf("sanitizeObjectName is not deterministic")
	}
}
