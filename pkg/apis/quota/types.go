package quota

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// QuotaDefinition enables quota tracking for a specific resource type.
type QuotaDefinition struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   QuotaDefinitionSpec
	Status QuotaDefinitionStatus
}

// QuotaDefinitionSpec defines the desired state of QuotaDefinition.
type QuotaDefinitionSpec struct {
	ConsumerType         ConsumerType
	Type                 string
	ResourceType         string
	Description          string
	BaseUnit             string
	DisplayUnit          string
	UnitConversionFactor int64
	ClaimingResources    []ClaimingResource
}

// ConsumerType identifies the resource type that consumes quota.
type ConsumerType struct {
	APIGroup string
	Kind     string
}

// ClaimingResource identifies a resource type that can create QuotaClaims.
type ClaimingResource struct {
	APIGroup string
	Kind     string
}

// QuotaDefinitionStatus reports the registration's operational state.
type QuotaDefinitionStatus struct {
	ObservedGeneration int64
	Conditions         []metav1.Condition
}

// QuotaDefinition measurement types (spec.type).
const (
	// QuotaDefinitionTypeEntity counts discrete resource instances.
	QuotaDefinitionTypeEntity = "Entity"
	// QuotaDefinitionTypeAllocation measures numeric capacity.
	QuotaDefinitionTypeAllocation = "Allocation"
	// QuotaDefinitionTypeFeature is a boolean entitlement with no claim
	// machinery; grants convey on/off availability rather than capacity.
	QuotaDefinitionTypeFeature = "Feature"
)

// Condition type constants for QuotaDefinition
const (
	QuotaDefinitionActive = "Active"
)

// Condition reason constants for QuotaDefinition
const (
	QuotaDefinitionActiveReason           = "RegistrationActive"
	QuotaDefinitionValidationFailedReason = "ValidationFailed"
	QuotaDefinitionNotFoundReason         = "RegistrationNotFound"
	QuotaDefinitionPendingReason          = "RegistrationPending"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QuotaDefinitionList contains a list of QuotaDefinition.
type QuotaDefinitionList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []QuotaDefinition
}

// ConsumerRef identifies a quota consumer.
type ConsumerRef struct {
	APIGroup  string
	Kind      string
	Name      string
	Namespace string
}

// MatchesConsumer checks if two ConsumerRef instances refer to the same
// consumer. Every field is part of the identity: buckets are keyed by
// apiGroup, kind, namespace and name, so two refs that differ only in
// namespace name different consumers.
func (c ConsumerRef) MatchesConsumer(other ConsumerRef) bool {
	return c == other
}

// UnversionedObjectReference provides a stable reference to a Kubernetes resource.
type UnversionedObjectReference struct {
	APIGroup  string
	Kind      string
	Name      string
	Namespace string
}

// ContributingResourceRef tracks a resource that contributes to a reference.
type ContributingResourceRef struct {
	Name               string
	ObservedGeneration int64
}

// Allocation represents a single allocation of quota from a bucket to a claim.
type Allocation struct {
	ClaimRef ConsumerRef
	Amount   int64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient

// QuotaGrant allocates quota capacity to a consumer for specific resource types.
type QuotaGrant struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   QuotaGrantSpec
	Status QuotaGrantStatus
}

// QuotaGrantSpec defines the desired state of QuotaGrant.
type QuotaGrantSpec struct {
	ConsumerRef ConsumerRef
	Allowances  []Allowance
}

// Allowance defines quota allocation for a specific resource type within a QuotaGrant.
type Allowance struct {
	ResourceType string
	Buckets      []Bucket
}

// Bucket represents a single allocation of quota capacity within an allowance.
type Bucket struct {
	Amount int64
}

// QuotaGrantStatus reports the grant's operational state.
type QuotaGrantStatus struct {
	ObservedGeneration int64
	Conditions         []metav1.Condition
}

// Condition type constants for QuotaGrant
const (
	QuotaGrantActive = "Active"
)

// Condition reason constants for QuotaGrant
const (
	QuotaGrantActiveReason           = "GrantActive"
	QuotaGrantValidationFailedReason = "ValidationFailed"
	QuotaGrantPendingReason          = "GrantPending"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QuotaGrantList contains a list of QuotaGrant.
type QuotaGrantList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []QuotaGrant
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient

// QuotaClaim requests quota allocation during resource creation.
type QuotaClaim struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   QuotaClaimSpec
	Status QuotaClaimStatus
}

// QuotaClaimSpec defines the desired state of QuotaClaim.
type QuotaClaimSpec struct {
	ConsumerRef ConsumerRef
	Requests    []ResourceRequest
	ResourceRef UnversionedObjectReference
}

// ResourceRequest defines a single resource request within a QuotaClaim.
type ResourceRequest struct {
	ResourceType string
	Amount       int64
}

// QuotaClaimAllocationStatus tracks the allocation status for a specific resource request.
type QuotaClaimAllocationStatus struct {
	ResourceType       string
	Status             string
	Reason             string
	Message            string
	AllocatedAmount    int64
	AllocatingBucket   string
	LastTransitionTime metav1.Time
}

// QuotaClaimStatus reports the claim's processing state and allocation results.
type QuotaClaimStatus struct {
	ObservedGeneration int64
	Allocations        []QuotaClaimAllocationStatus
	Conditions         []metav1.Condition
}

// Condition type constants for QuotaClaim
const (
	QuotaClaimGranted = "Granted"
)

// Condition reason constants for QuotaClaim
const (
	QuotaClaimGrantedReason          = "QuotaAvailable"
	QuotaClaimDeniedReason           = "QuotaExceeded"
	QuotaClaimValidationFailedReason = "ValidationFailed"
	QuotaClaimPendingReason          = "PendingEvaluation"
)

// QuotaClaimAllocationStatus status constants
const (
	QuotaClaimAllocationStatusGranted = "Granted"
	QuotaClaimAllocationStatusDenied  = "Denied"
	QuotaClaimAllocationStatusPending = "Pending"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QuotaClaimList contains a list of QuotaClaim.
type QuotaClaimList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []QuotaClaim
}

// ContributingGrantRef tracks a QuotaGrant that contributes capacity to a bucket.
type ContributingGrantRef struct {
	Name                   string
	LastObservedGeneration int64
	Amount                 int64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient

// QuotaBucket aggregates quota limits and usage for a single (consumer, resourceType) combination.
type QuotaBucket struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   QuotaBucketSpec
	Status QuotaBucketStatus
}

// QuotaBucketSpec defines the desired state of QuotaBucket.
type QuotaBucketSpec struct {
	ConsumerRef  ConsumerRef
	ResourceType string
}

// QuotaBucketStatus contains the quota system-computed quota aggregation.
type QuotaBucketStatus struct {
	ObservedGeneration    int64
	Limit                 int64
	Allocated             int64
	Available             int64
	ClaimCount            int32
	GrantCount            int32
	ContributingGrantRefs []ContributingGrantRef
	LastReconciliation    *metav1.Time
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QuotaBucketList contains a list of QuotaBucket.
type QuotaBucketList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []QuotaBucket
}

// --- Shared types for creation policies ---

// ConditionExpression defines a CEL expression that determines when the policy should trigger.
type ConditionExpression struct {
	Expression string
	Message    string
}

// ObjectMetaTemplate defines metadata fields that support template rendering for created objects.
type ObjectMetaTemplate struct {
	Name         string
	GenerateName string
	Namespace    string
	Labels       map[string]string
	Annotations  map[string]string
}

// ResourceGrantTemplate defines the specification for creating QuotaGrants.
type ResourceGrantTemplate struct {
	Metadata ObjectMetaTemplate
	Spec     QuotaGrantSpec
}

// ResourceClaimTemplate defines how to create QuotaClaims.
type ResourceClaimTemplate struct {
	Metadata ObjectMetaTemplate
	Spec     QuotaClaimSpec
}

// --- GrantCreationPolicy ---

// GrantCreationPolicySpec defines the desired state of GrantCreationPolicy.
type GrantCreationPolicySpec struct {
	Trigger  GrantTriggerSpec
	Target   GrantTargetSpec
	Disabled *bool
}

// GrantTriggerSpec defines the resource and conditions that trigger grant creation.
type GrantTriggerSpec struct {
	Resource    GrantTriggerResource
	Constraints []ConditionExpression
}

// GrantTriggerResource identifies the resource type that triggers grant creation.
type GrantTriggerResource struct {
	APIVersion string
	Kind       string
}

// GetGVK returns the GVK for the trigger resource.
func (t *GrantTriggerResource) GetGVK() schema.GroupVersionKind {
	gv, _ := schema.ParseGroupVersion(t.APIVersion)
	return schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    t.Kind,
	}
}

// GrantTargetSpec defines where and how grants are created.
type GrantTargetSpec struct {
	ParentContext         *GrantParentContextSpec
	ResourceGrantTemplate ResourceGrantTemplate
}

// GrantParentContextSpec enables cross-cluster grant creation by targeting a parent control plane.
type GrantParentContextSpec struct {
	APIGroup       string
	Kind           string
	NameExpression string
}

// GrantCreationPolicyStatus defines the observed state of GrantCreationPolicy.
type GrantCreationPolicyStatus struct {
	ObservedGeneration int64
	Conditions         []metav1.Condition
}

// Condition type constants for GrantCreationPolicy
const (
	GrantCreationPolicyReady              = "Ready"
	GrantCreationPolicyParentContextReady = "ParentContextReady"
)

// Condition reason constants for GrantCreationPolicy
const (
	GrantCreationPolicyReadyReason               = "PolicyReady"
	GrantCreationPolicyValidationFailedReason    = "ValidationFailed"
	GrantCreationPolicyDisabledReason            = "PolicyDisabled"
	GrantCreationPolicyParentContextReadyReason  = "ParentContextReady"
	GrantCreationPolicyParentContextFailedReason = "ParentContextFailed"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// GrantCreationPolicy automates QuotaGrant creation when observed resources meet conditions.
type GrantCreationPolicy struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   GrantCreationPolicySpec
	Status GrantCreationPolicyStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GrantCreationPolicyList contains a list of GrantCreationPolicy.
type GrantCreationPolicyList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []GrantCreationPolicy
}

// --- ClaimCreationPolicy ---

// ClaimCreationPolicySpec defines the desired state of ClaimCreationPolicy.
type ClaimCreationPolicySpec struct {
	Trigger  ClaimTriggerSpec
	Target   ClaimTargetSpec
	Disabled *bool
}

// ClaimTriggerResource identifies the resource type that triggers this policy.
type ClaimTriggerResource struct {
	APIVersion string
	Kind       string
}

// GetGVK returns the GVK for the trigger resource.
func (t *ClaimTriggerResource) GetGVK() schema.GroupVersionKind {
	gv, _ := schema.ParseGroupVersion(t.APIVersion)
	return schema.GroupVersionKind{
		Group:   gv.Group,
		Version: gv.Version,
		Kind:    t.Kind,
	}
}

// ClaimTriggerSpec defines the resource type and optional conditions for triggering claim creation.
type ClaimTriggerSpec struct {
	Resource    ClaimTriggerResource
	Constraints []ConditionExpression
}

// ClaimTargetSpec defines how QuotaClaims are created for a matched trigger.
type ClaimTargetSpec struct {
	ResourceClaimTemplate ResourceClaimTemplate
}

// ClaimCreationPolicyStatus defines the observed state of ClaimCreationPolicy.
type ClaimCreationPolicyStatus struct {
	ObservedGeneration int64
	Conditions         []metav1.Condition
}

// Condition type constants for ClaimCreationPolicy
const (
	ClaimCreationPolicyReady            = "Ready"
	ClaimCreationPolicyValidationFailed = "ValidationFailed"
)

// Condition reason constants for ClaimCreationPolicy
const (
	ClaimCreationPolicyReadyReason            = "PolicyReady"
	ClaimCreationPolicyValidationFailedReason = "ValidationFailed"
	ClaimCreationPolicyDisabledReason         = "PolicyDisabled"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced

// ClaimCreationPolicy automatically creates QuotaClaims during admission to enforce quota in real-time.
type ClaimCreationPolicy struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   ClaimCreationPolicySpec
	Status ClaimCreationPolicyStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClaimCreationPolicyList contains a list of ClaimCreationPolicy.
type ClaimCreationPolicyList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []ClaimCreationPolicy
}
