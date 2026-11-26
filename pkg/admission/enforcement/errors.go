package enforcement

import (
	"errors"
	"fmt"
)

// claimFailureKind classifies why a QuotaClaim could not be resolved to a
// Granted outcome during admission. It lets the admission plugin produce a
// distinct, actionable user-facing message for each root cause rather than
// collapsing every failure into "Insufficient quota".
type claimFailureKind int

const (
	// claimFailureDenied indicates the claim was denied because there was not enough available capacity. This is the only
	// case where "Insufficient quota" is accurate.
	claimFailureDenied claimFailureKind = iota

	// claimFailureTimeout indicates the claim was created but did not reach a
	// final Granted condition within the admission plugin's watch timeout.
	// Typical causes: bucket controller still reconciling, grants still
	// propagating, or a slow control plane.
	claimFailureTimeout

	// claimFailureConflict indicates a QuotaClaim with the deterministic
	// name already exists in a non-usable state (e.g., a previously denied
	// claim that has not yet been garbage collected). The caller should retry
	// after the GC controller deletes the stale claim.
	claimFailureConflict

	// claimFailureInternal covers template render failures, watch manager
	// startup failures, dynamic client errors, and anything else that is not
	// attributable to quota capacity.
	claimFailureInternal

	// claimFailureMisconfigured indicates the ClaimCreationPolicy cannot enforce
	// quota for the triggering resource as written — e.g. it targets a
	// cluster-scoped resource but does not pin a QuotaClaim namespace. The
	// request can't succeed until an operator corrects the policy, so it is kept
	// distinct from transient internal errors (which advise "try again").
	claimFailureMisconfigured

	// claimFailureEvaluation indicates a policy's trigger constraints could
	// not be evaluated against the object (an expression error, the cost
	// limit, or the evaluation deadline). The request is denied rather than
	// admitted without a claim.
	claimFailureEvaluation
)

// claimFailure is the structured error returned from the claim creation/wait
// pipeline. It carries a classification so the admission plugin can surface a
// tailored message to the caller while still preserving the underlying error
// for logs and traces.
type claimFailure struct {
	kind    claimFailureKind
	reason  string // machine-readable reason (e.g. condition reason)
	message string // user-facing message fragment (e.g. denial message)
	cause   error  // wrapped error for logging/tracing
}

func (e *claimFailure) Error() string {
	switch {
	case e.message != "" && e.cause != nil:
		return fmt.Sprintf("%s: %v", e.message, e.cause)
	case e.message != "":
		return e.message
	case e.cause != nil:
		return e.cause.Error()
	default:
		return fmt.Sprintf("claim failure (kind=%d)", e.kind)
	}
}

func (e *claimFailure) Unwrap() error { return e.cause }

// asClaimFailure unwraps a claimFailure from err if present. It returns the
// claimFailure and true, or (nil, false) if err does not carry one.
func asClaimFailure(err error) (*claimFailure, bool) {
	var f *claimFailure
	if errors.As(err, &f) {
		return f, true
	}
	return nil, false
}

// newDeniedFailure constructs a claimFailure for an actual quota denial.
func newDeniedFailure(reason, message string) *claimFailure {
	return &claimFailure{kind: claimFailureDenied, reason: reason, message: message}
}

// newTimeoutFailure constructs a claimFailure for an admission watch timeout.
func newTimeoutFailure(cause error) *claimFailure {
	return &claimFailure{kind: claimFailureTimeout, cause: cause}
}

// newConflictFailure constructs a claimFailure for a pre-existing claim that
// blocks retry (typically a denied claim awaiting GC).
func newConflictFailure(message string, cause error) *claimFailure {
	return &claimFailure{kind: claimFailureConflict, message: message, cause: cause}
}

// newInternalFailure constructs a claimFailure for infrastructure errors
// (template render, watch manager, dynamic client, etc).
func newInternalFailure(cause error) *claimFailure {
	return &claimFailure{kind: claimFailureInternal, cause: cause}
}

// newMisconfiguredFailure constructs a claimFailure for a ClaimCreationPolicy
// that cannot enforce quota as written. cause carries the operator-facing
// detail (policy name, offending field) for logs and traces.
func newMisconfiguredFailure(reason string, cause error) *claimFailure {
	return &claimFailure{kind: claimFailureMisconfigured, reason: reason, cause: cause}
}

func newEvaluationFailure(policyName string, cause error) *claimFailure {
	return &claimFailure{kind: claimFailureEvaluation, reason: policyName, cause: cause}
}
