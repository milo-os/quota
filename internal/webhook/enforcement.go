package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

// EnforcementWebhookHandler wraps a quota enforcement admission plugin as
// an HTTP handler that accepts AdmissionReview requests. This lets the
// kube-apiserver send non-quota resource creates to the quota apiserver
// for policy evaluation via a ValidatingWebhookConfiguration (see
// config/components/enforcement-webhook). It is registered as validating so
// claims are only created for objects that passed mutation and schema
// validation.
//
// The handler creates QuotaClaims with the quota apiserver's own identity,
// so only the kube-apiserver may call it. It is served behind the generic
// apiserver's authentication and authorization filters, which authorize the
// caller for "post" on the /webhook/enforce non-resource URL (see
// config/components/enforcement-webhook/README.md). As a second line of
// defense it rejects anonymous callers itself, so adding the path to
// --authorization-always-allow-paths cannot make it unauthenticated.
type EnforcementWebhookHandler struct {
	plugin  admission.ValidationInterface
	decoder runtime.Decoder
}

// NewEnforcementWebhookHandler creates a new webhook handler that delegates
// AdmissionReview requests to the given admission plugin.
func NewEnforcementWebhookHandler(plugin admission.ValidationInterface, scheme *runtime.Scheme) *EnforcementWebhookHandler {
	// Build a codec factory that can decode AdmissionReview objects.
	// We register the admissionv1 types into a dedicated scheme so the
	// universal deserializer can handle them.
	webhookScheme := runtime.NewScheme()
	utilruntime.Must(admissionv1.AddToScheme(webhookScheme))
	codecs := serializer.NewCodecFactory(webhookScheme)

	return &EnforcementWebhookHandler{
		plugin:  plugin,
		decoder: codecs.UniversalDeserializer(),
	}
}

func (h *EnforcementWebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	caller, ok := request.UserFrom(r.Context())
	if !ok || isAnonymous(caller) {
		writeError(w, http.StatusForbidden, "the enforcement webhook requires an authenticated caller")
		return
	}

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, fmt.Sprintf("unsupported content type %q, expected application/json", contentType))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		klog.ErrorS(err, "Failed to read admission review request body")
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var review admissionv1.AdmissionReview
	if _, _, err := h.decoder.Decode(body, nil, &review); err != nil {
		klog.ErrorS(err, "Failed to decode AdmissionReview")
		writeError(w, http.StatusBadRequest, "failed to decode AdmissionReview")
		return
	}

	if review.Request == nil {
		writeError(w, http.StatusBadRequest, "missing request in AdmissionReview")
		return
	}

	klog.V(3).InfoS("Received webhook admission request",
		"uid", review.Request.UID,
		"kind", review.Request.Kind,
		"resource", review.Request.Resource,
		"name", review.Request.Name,
		"namespace", review.Request.Namespace,
		"operation", review.Request.Operation,
	)

	attrs, err := admissionReviewToAttributes(review.Request)
	if err != nil {
		klog.ErrorS(err, "Failed to convert AdmissionReview to attributes")
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to convert request: %v", err))
		return
	}

	// Delegate to the enforcement plugin.
	validationErr := h.plugin.Validate(r.Context(), attrs, nil)

	response := &admissionv1.AdmissionResponse{
		UID:     review.Request.UID,
		Allowed: validationErr == nil,
	}
	if validationErr != nil {
		klog.V(2).InfoS("Webhook admission denied",
			"uid", review.Request.UID,
			"kind", review.Request.Kind,
			"name", review.Request.Name,
			"namespace", review.Request.Namespace,
			"error", validationErr.Error(),
		)
		response.Result = statusForError(validationErr)
	}

	responseReview := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Response: response,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(responseReview); err != nil {
		klog.ErrorS(err, "Failed to encode admission review response")
	}
}

// admissionReviewToAttributes converts an AdmissionReview request into the
// admission.Attributes interface expected by the enforcement plugin.
func admissionReviewToAttributes(req *admissionv1.AdmissionRequest) (admission.Attributes, error) {
	// Decode the object from the raw extension into an Unstructured.
	obj := &unstructured.Unstructured{}
	if req.Object.Raw != nil {
		if err := json.Unmarshal(req.Object.Raw, &obj.Object); err != nil {
			return nil, fmt.Errorf("failed to unmarshal request object: %w", err)
		}
	}

	// Decode old object for UPDATE operations (not currently used by the
	// enforcement plugin, but included for correctness).
	var oldObj runtime.Object
	if req.OldObject.Raw != nil {
		old := &unstructured.Unstructured{}
		if err := json.Unmarshal(req.OldObject.Raw, &old.Object); err != nil {
			return nil, fmt.Errorf("failed to unmarshal old object: %w", err)
		}
		oldObj = old
	}

	dryRun := false
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}

	userInfo := toUserInfo(&req.UserInfo)

	return admission.NewAttributesRecord(
		obj,                               // object
		oldObj,                            // oldObject
		schema.GroupVersionKind(req.Kind), // kind
		req.Namespace,                     // namespace
		req.Name,                          // name
		schema.GroupVersionResource(req.Resource), // resource
		req.SubResource,                    // subresource
		admission.Operation(req.Operation), // operation
		nil,                                // operationOptions
		dryRun,                             // dryRun
		userInfo,                           // userInfo
	), nil
}

// toUserInfo converts an authenticationv1.UserInfo into the user.Info interface
// that admission.NewAttributesRecord expects.
func toUserInfo(info *authenticationv1.UserInfo) user.Info {
	extra := make(map[string][]string, len(info.Extra))
	for k, v := range info.Extra {
		extra[k] = v
	}
	return &user.DefaultInfo{
		Name:   info.Username,
		UID:    info.UID,
		Groups: info.Groups,
		Extra:  extra,
	}
}

// isAnonymous reports whether the authenticated user is the anonymous user
// or carries the unauthenticated group.
func isAnonymous(u user.Info) bool {
	if u.GetName() == user.Anonymous {
		return true
	}
	for _, g := range u.GetGroups() {
		if g == user.AllUnauthenticated {
			return true
		}
	}
	return false
}

// statusForError returns the API status to report for a denial. Typed API
// errors (the plugin returns Forbidden with a user-facing message) keep
// their code, reason and details; anything else is reported as Forbidden.
func statusForError(err error) *metav1.Status {
	var apiStatus apierrors.APIStatus
	if errors.As(err, &apiStatus) {
		status := apiStatus.Status()
		return &status
	}
	return &metav1.Status{
		Status:  metav1.StatusFailure,
		Message: err.Error(),
		Reason:  metav1.StatusReasonForbidden,
		Code:    http.StatusForbidden,
	}
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"error": message,
	}); err != nil {
		klog.ErrorS(err, "Failed to encode webhook error response")
	}
}
