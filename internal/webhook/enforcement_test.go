package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

type fakePlugin struct {
	err  error
	seen admission.Attributes
}

func (f *fakePlugin) Handles(admission.Operation) bool { return true }
func (f *fakePlugin) Validate(_ context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
	f.seen = a
	return f.err
}

var kubeAPIServer = &user.DefaultInfo{Name: "quota-enforcement-webhook", Groups: []string{user.AllAuthenticated}}

func review(t *testing.T, h http.Handler, namespace string) *admissionv1.AdmissionResponse {
	t.Helper()
	rec := post(t, h, namespace, kubeAPIServer)
	var out admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v (%s)", err, rec.Body.String())
	}
	return out.Response
}

// post sends an AdmissionReview as caller (nil: no user in the context).
func post(t *testing.T, h http.Handler, namespace string, caller user.Info) *httptest.ResponseRecorder {
	t.Helper()
	in := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:       "u1",
			Kind:      metav1.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"},
			Resource:  metav1.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"},
			Name:      "w1",
			Namespace: namespace,
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: []byte(`{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w1"}}`)},
		},
	}
	body, _ := json.Marshal(in)
	req := httptest.NewRequest(http.MethodPost, "/webhook/enforce", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if caller != nil {
		req = req.WithContext(request.WithUser(req.Context(), caller))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebhookRejectsUnauthenticatedCallers(t *testing.T) {
	tests := []struct {
		name   string
		caller user.Info
	}{
		{name: "no user in context", caller: nil},
		{name: "anonymous user", caller: &user.DefaultInfo{Name: user.Anonymous, Groups: []string{user.AllUnauthenticated}}},
		{name: "unauthenticated group", caller: &user.DefaultInfo{Name: "someone", Groups: []string{user.AllUnauthenticated}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakePlugin{}
			rec := post(t, NewEnforcementWebhookHandler(p, runtime.NewScheme()), "team-a", tt.caller)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
			}
			if p.seen != nil {
				t.Fatal("the plugin must not run for an unauthenticated caller")
			}
		})
	}
}

func TestWebhookDenialKeepsAPIStatus(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "example.com", Resource: "widgets"}, "w1", fmt.Errorf("quota"))
	h := NewEnforcementWebhookHandler(&fakePlugin{err: forbidden}, runtime.NewScheme())
	resp := review(t, h, "team-a")
	if resp.Allowed || resp.Result == nil || resp.Result.Code != http.StatusForbidden || resp.Result.Reason != metav1.StatusReasonForbidden {
		t.Fatalf("unexpected response: %+v", resp.Result)
	}

	h = NewEnforcementWebhookHandler(&fakePlugin{err: fmt.Errorf("boom")}, runtime.NewScheme())
	resp = review(t, h, "team-a")
	if resp.Allowed || resp.Result.Code != http.StatusForbidden {
		t.Fatalf("untyped errors must be reported as forbidden: %+v", resp.Result)
	}
}

func TestWebhookClusterScopedRequest(t *testing.T) {
	p := &fakePlugin{}
	h := NewEnforcementWebhookHandler(p, runtime.NewScheme())
	if resp := review(t, h, ""); !resp.Allowed {
		t.Fatalf("expected allow: %+v", resp.Result)
	}
	if p.seen.GetNamespace() != "" || p.seen.GetName() != "w1" {
		t.Fatalf("cluster-scoped attributes not passed through: ns=%q name=%q", p.seen.GetNamespace(), p.seen.GetName())
	}
}
