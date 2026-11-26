package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ConsumerType identifies the resource type that consumes quota.
// The consumer receives QuotaGrants and creates QuotaClaims for the registered resource.
type ConsumerType struct {
	// APIGroup specifies the API group of the quota consumer resource type.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	APIGroup string `json:"apiGroup"`

	// Kind specifies the resource type that receives quota grants and creates quota claims.
	//
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`
}

// ClaimingResource identifies a resource type that can create QuotaClaims
// for this definition.
type ClaimingResource struct {
	// APIGroup specifies the API group of the resource that can create claims.
	//
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Pattern=`^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	APIGroup string `json:"apiGroup,omitempty"`

	// Kind specifies the resource type that can create QuotaClaims for this definition.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Kind string `json:"kind"`
}

// ConsumerRef identifies a quota consumer - the entity that receives quota grants
// and creates quota claims.
type ConsumerRef struct {
	// APIGroup specifies the API group of the consumer resource.
	//
	// +kubebuilder:validation:Optional
	APIGroup string `json:"apiGroup,omitempty"`

	// Kind specifies the type of consumer resource.
	//
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// Name identifies the specific consumer resource instance.
	//
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace identifies the namespace of the consumer resource.
	//
	// +kubebuilder:validation:Optional
	Namespace string `json:"namespace,omitempty"`
}

// MatchesConsumer checks if two ConsumerRef instances refer to the same
// consumer. Every field is part of the identity: buckets are keyed by
// apiGroup, kind, namespace and name, so two refs that differ only in
// namespace name different consumers.
func (c ConsumerRef) MatchesConsumer(other ConsumerRef) bool {
	return c == other
}

// UnversionedObjectReference provides a stable reference to a Kubernetes resource
// that remains valid across API version changes.
type UnversionedObjectReference struct {
	// APIGroup specifies the API group of the referenced resource.
	//
	// +kubebuilder:validation:Optional
	APIGroup string `json:"apiGroup,omitempty"`

	// Kind specifies the type of the referenced resource.
	//
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// Name identifies the specific resource instance.
	//
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace specifies the namespace containing the referenced resource.
	//
	// +kubebuilder:validation:Optional
	Namespace string `json:"namespace,omitempty"`
}

// ContributingResourceRef tracks a resource that contributes to a reference.
type ContributingResourceRef struct {
	// Name of the resource
	//
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// ObservedGeneration is the generation of the resource.
	//
	// +kubebuilder:validation:Required
	ObservedGeneration int64 `json:"observedGeneration"`
}

// Allocation represents a single allocation of quota from a bucket to a claim.
type Allocation struct {
	// ClaimRef references the QuotaClaim that consumed this allocation.
	//
	// +kubebuilder:validation:Required
	ClaimRef ConsumerRef `json:"claimRef"`

	// Amount is the amount of quota allocated.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	Amount int64 `json:"amount"`
}

// --- QuotaDefinition ---

// QuotaDefinitionSpec defines the desired state of QuotaDefinition.
type QuotaDefinitionSpec struct {
	// ConsumerType specifies which resource type receives grants and creates claims.
	//
	// +kubebuilder:validation:Required
	ConsumerType ConsumerType `json:"consumerType"`

	// Type specifies the measurement method for quota tracking.
	// This field is immutable after creation.
	//
	// Valid values:
	//   - `Entity`: Counts discrete resource instances; each instance consumes
	//     exactly one quota unit (for example, Projects or Users).
	//   - `Allocation`: Measures numeric capacity (for example, CPU millicores
	//     or storage bytes).
	//   - `Feature`: A boolean entitlement used for feature flags. No admission
	//     enforcement or claim machinery is used; grants convey on/off
	//     entitlement rather than a numeric capacity.
	//
	// +kubebuilder:validation:Enum=Entity;Allocation;Feature
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// ResourceType identifies the resource to track with quota.
	// This field is immutable after creation.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ResourceType string `json:"resourceType"`

	// Description provides human-readable context about what this definition tracks.
	//
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MaxLength=500
	// +kubebuilder:validation:MinLength=1
	Description string `json:"description,omitempty"`

	// BaseUnit defines the internal measurement unit for all quota calculations.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	BaseUnit string `json:"baseUnit"`

	// DisplayUnit defines the unit shown in user interfaces and API responses.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	DisplayUnit string `json:"displayUnit"`

	// UnitConversionFactor converts BaseUnit values to DisplayUnit values.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	UnitConversionFactor int64 `json:"unitConversionFactor"`

	// ClaimingResources specifies which resource types can create QuotaClaims.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	ClaimingResources []ClaimingResource `json:"claimingResources"`
}

// QuotaDefinitionStatus reports the definition's operational state.
type QuotaDefinitionStatus struct {
	// ObservedGeneration indicates the most recent spec generation processed.
	//
	// +kubebuilder:validation:Optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represents the latest available observations of the definition's state.
	//
	// +kubebuilder:validation:XValidation:rule="self.all(c, c.type == 'Active' ? c.reason in ['RegistrationActive', 'ValidationFailed', 'RegistrationPending'] : true)",message="Active condition reason must be valid"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
	// QuotaDefinitionActive indicates that the quota definition is active.
	QuotaDefinitionActive = "Active"
)

// Condition reason constants for QuotaDefinition
const (
	QuotaDefinitionActiveReason           = "RegistrationActive"
	QuotaDefinitionValidationFailedReason = "ValidationFailed"
	QuotaDefinitionNotFoundReason         = "RegistrationNotFound"
	QuotaDefinitionPendingReason          = "RegistrationPending"
)

// QuotaDefinition enables quota tracking for a specific resource type.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +k8s:openapi-gen=true
// +kubebuilder:printcolumn:name="Type",type="string",JSONPath=".spec.type"
// +kubebuilder:printcolumn:name="Active",type="string",JSONPath=".status.conditions[?(@.type=='Active')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerType.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerType.apiGroup"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceType"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.resourceType) || self.spec.resourceType == oldSelf.spec.resourceType",message="spec.resourceType is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.consumerType) || self.spec.consumerType == oldSelf.spec.consumerType",message="spec.consumerType is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.type) || self.spec.type == oldSelf.spec.type",message="spec.type is immutable"
type QuotaDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   QuotaDefinitionSpec   `json:"spec"`
	Status QuotaDefinitionStatus `json:"status,omitempty"`
}

// QuotaDefinitionList contains a list of QuotaDefinition.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type QuotaDefinitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaDefinition `json:"items"`
}

// --- QuotaGrant ---

// Bucket represents a single allocation of quota capacity within an allowance.
type Bucket struct {
	// Amount specifies the quota capacity provided by this bucket.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Amount int64 `json:"amount"`
}

// Allowance defines quota allocation for a specific resource type within a QuotaGrant.
type Allowance struct {
	// ResourceType identifies the specific resource type receiving quota allocation.
	//
	// +kubebuilder:validation:Required
	ResourceType string `json:"resourceType"`

	// Buckets contains the quota allocations for this resource type.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Buckets []Bucket `json:"buckets"`
}

// QuotaGrantSpec defines the desired state of QuotaGrant.
type QuotaGrantSpec struct {
	// ConsumerRef identifies the quota consumer that receives these allowances.
	//
	// +kubebuilder:validation:Required
	ConsumerRef ConsumerRef `json:"consumerRef"`

	// Allowances specifies the quota allocations provided by this grant.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Allowances []Allowance `json:"allowances"`
}

// QuotaGrantStatus reports the grant's operational state.
type QuotaGrantStatus struct {
	// ObservedGeneration indicates the most recent spec generation processed.
	//
	// +kubebuilder:validation:Optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represents the latest available observations of the grant's state.
	//
	// +kubebuilder:validation:XValidation:rule="self.all(c, c.type == 'Active' ? c.reason in ['GrantActive', 'ValidationFailed', 'GrantPending'] : true)",message="Active condition reason must be valid"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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

// QuotaGrant allocates quota capacity to a consumer for specific resource types.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Active",type="string",JSONPath=".status.conditions[?(@.type=='Active')].status"
// +kubebuilder:printcolumn:name="Consumer Group",type="string",JSONPath=".spec.consumerRef.apiGroup",priority=1
// +kubebuilder:printcolumn:name="Consumer Type",type="string",JSONPath=".spec.consumerRef.kind",priority=1
// +kubebuilder:printcolumn:name="Consumer",type="string",JSONPath=".spec.consumerRef.name",priority=1
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +k8s:openapi-gen=true
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.name"
type QuotaGrant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   QuotaGrantSpec   `json:"spec"`
	Status QuotaGrantStatus `json:"status,omitempty"`
}

// QuotaGrantList contains a list of QuotaGrant.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type QuotaGrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaGrant `json:"items"`
}

// --- QuotaClaim ---

// ResourceRequest defines a single resource request within a QuotaClaim.
type ResourceRequest struct {
	// ResourceType identifies the specific resource type being claimed.
	//
	// +kubebuilder:validation:Required
	ResourceType string `json:"resourceType"`

	// Amount specifies how much quota to claim for this resource type.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Amount int64 `json:"amount"`
}

// QuotaClaimSpec defines the desired state of QuotaClaim.
type QuotaClaimSpec struct {
	// ConsumerRef identifies the quota consumer making this claim.
	// The quota service requires a non-empty kind and name: an empty consumer
	// would pool every such claim into a single bucket. Admission plugins that
	// create claims on a caller's behalf must resolve the consumer first.
	ConsumerRef ConsumerRef `json:"consumerRef,omitempty"`

	// Requests specifies the resource types and amounts being claimed.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	Requests []ResourceRequest `json:"requests"`

	// ResourceRef identifies the actual Kubernetes resource that triggered this claim.
	// An empty reference means the claim is not tied to a specific resource.
	// When set, both kind and name are required.
	//
	// +optional
	ResourceRef UnversionedObjectReference `json:"resourceRef,omitempty"`
}

// QuotaClaimAllocationStatus tracks the allocation status for a specific resource request.
type QuotaClaimAllocationStatus struct {
	// ResourceType identifies which resource request this allocation status describes.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ResourceType string `json:"resourceType"`

	// Status indicates the allocation result for this specific resource request.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=Granted;Denied;Pending
	Status string `json:"status"`

	// Reason provides a machine-readable explanation for the current status.
	//
	// +kubebuilder:validation:Optional
	Reason string `json:"reason,omitempty"`

	// Message provides a human-readable explanation of the allocation result.
	//
	// +kubebuilder:validation:Optional
	Message string `json:"message,omitempty"`

	// AllocatedAmount specifies how much quota was actually allocated.
	//
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Minimum=0
	AllocatedAmount int64 `json:"allocatedAmount,omitempty"`

	// AllocatingBucket identifies the QuotaBucket that provided the quota.
	//
	// +kubebuilder:validation:Optional
	AllocatingBucket string `json:"allocatingBucket,omitempty"`

	// LastTransitionTime records when this allocation status last changed.
	//
	// +kubebuilder:validation:Required
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// QuotaClaimStatus reports the claim's processing state and allocation results.
type QuotaClaimStatus struct {
	// ObservedGeneration indicates the most recent spec generation processed.
	//
	// +kubebuilder:validation:Optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Allocations provides detailed status for each resource request.
	//
	// +kubebuilder:validation:Optional
	// +listType=map
	// +listMapKey=resourceType
	Allocations []QuotaClaimAllocationStatus `json:"allocations,omitempty"`

	// Conditions represents the overall status of the claim evaluation.
	//
	// +kubebuilder:validation:XValidation:rule="self.all(c, c.type == 'Granted' ? c.reason in ['QuotaAvailable', 'QuotaExceeded', 'ValidationFailed', 'PendingEvaluation'] : true)",message="Granted condition reason must be valid"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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

// QuotaClaim requests quota allocation during resource creation.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Granted",type="string",JSONPath=".status.conditions[?(@.type=='Granted')].status"
// +kubebuilder:printcolumn:name="Resource",type="string",JSONPath=".spec.requests[0].resourceType",priority=1
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +k8s:openapi-gen=true
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceRef.apiGroup"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceRef.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceRef.namespace"
type QuotaClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec QuotaClaimSpec `json:"spec"`
	// +kubebuilder:default={conditions: {{type:"Granted",status:"False",reason:"PendingEvaluation",message:"Awaiting capacity evaluation", lastTransitionTime: "1970-01-01T00:00:00Z"}}}
	Status QuotaClaimStatus `json:"status,omitempty"`
}

// QuotaClaimList contains a list of QuotaClaim.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type QuotaClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaClaim `json:"items"`
}

// --- QuotaBucket ---

// ContributingGrantRef tracks a QuotaGrant that contributes capacity to this bucket.
type ContributingGrantRef struct {
	// Name identifies the QuotaGrant that contributes to this bucket's limit.
	//
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// LastObservedGeneration records the QuotaGrant's generation when last processed.
	//
	// +kubebuilder:validation:Required
	LastObservedGeneration int64 `json:"lastObservedGeneration"`

	// Amount specifies how much quota capacity this grant contributes.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	Amount int64 `json:"amount"`
}

// QuotaBucketSpec defines the desired state of QuotaBucket.
type QuotaBucketSpec struct {
	// ConsumerRef identifies the quota consumer tracked by this bucket.
	//
	// +kubebuilder:validation:Required
	ConsumerRef ConsumerRef `json:"consumerRef"`

	// ResourceType specifies which resource type this bucket aggregates quota for.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ResourceType string `json:"resourceType"`
}

// QuotaBucketStatus contains the system-computed quota aggregation.
type QuotaBucketStatus struct {
	// ObservedGeneration indicates the most recent spec generation processed.
	//
	// +kubebuilder:validation:Optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Limit represents the total quota capacity available.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Limit int64 `json:"limit"`

	// Allocated represents the total quota currently consumed.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Allocated int64 `json:"allocated"`

	// Available represents the quota capacity remaining for new claims.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Available int64 `json:"available"`

	// ClaimCount indicates the total number of granted claims consuming from this bucket.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	ClaimCount int32 `json:"claimCount"`

	// GrantCount indicates the total number of active grants contributing to this bucket.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	GrantCount int32 `json:"grantCount"`

	// ContributingGrantRefs provides detailed information about each contributing grant.
	//
	// +kubebuilder:validation:Optional
	ContributingGrantRefs []ContributingGrantRef `json:"contributingGrantRefs,omitempty"`

	// LastReconciliation records when the system last recalculated this status.
	//
	// +kubebuilder:validation:Optional
	LastReconciliation *metav1.Time `json:"lastReconciliation,omitempty"`
}

// QuotaBucket aggregates quota limits and usage for a single (consumer, resourceType) combination.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +k8s:openapi-gen=true
// +kubebuilder:printcolumn:name="Resource Type",type="string",JSONPath=".spec.resourceType"
// +kubebuilder:printcolumn:name="Limit",type="integer",JSONPath=".status.limit"
// +kubebuilder:printcolumn:name="Allocated",type="integer",JSONPath=".status.allocated"
// +kubebuilder:printcolumn:name="Available",type="integer",JSONPath=".status.available"
// +kubebuilder:printcolumn:name="Claims",type="integer",JSONPath=".status.claimCount"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.consumerRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.resourceType"
type QuotaBucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   QuotaBucketSpec   `json:"spec"`
	Status QuotaBucketStatus `json:"status,omitempty"`
}

// QuotaBucketList contains a list of QuotaBucket.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type QuotaBucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuotaBucket `json:"items"`
}

// --- Shared types for creation policies ---

// ConditionExpression defines a CEL expression that determines when the policy should trigger.
// All expressions in a policy's trigger conditions must evaluate to true for the policy to activate.
type ConditionExpression struct {
	// Expression specifies the CEL expression to evaluate against the trigger resource.
	// This is a pure CEL expression WITHOUT {{ }} delimiters (unlike template fields).
	// Must return a boolean value (true to match, false to skip).
	// Maximum 1024 characters.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Expression string `json:"expression"`

	// Message provides a human-readable description explaining when this condition applies.
	// Used for documentation and debugging. Maximum 256 characters.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Message string `json:"message,omitempty"`
}

// ObjectMetaTemplate defines metadata fields that support template rendering for created objects.
// Templates can access trigger resource data to generate dynamic names, namespaces, and annotations.
type ObjectMetaTemplate struct {
	// Name specifies the exact name for the created resource.
	// Supports CEL expressions wrapped in {{ }} delimiters with access to template variables.
	// Leave empty to use GenerateName for auto-generated names.
	//
	// +optional
	Name string `json:"name,omitempty"`

	// GenerateName specifies a prefix for auto-generated names when Name is empty.
	// Kubernetes appends random characters to create unique names.
	// Supports CEL expressions wrapped in {{ }} delimiters.
	//
	// +optional
	GenerateName string `json:"generateName,omitempty"`

	// Namespace specifies where the resource will be created.
	// Supports CEL expressions wrapped in {{ }} delimiters to derive namespace from trigger resource.
	// Leave empty to create in the same namespace as the trigger resource.
	//
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Labels specifies static labels to apply to the created resource.
	// Values are literal strings (no template processing).
	// The system automatically adds standard labels for policy tracking.
	//
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations specifies annotations to apply to the created resource.
	// Values support CEL expressions wrapped in {{ }} delimiters for dynamic content.
	// The system automatically adds standard annotations for tracking.
	//
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ResourceGrantTemplate defines the specification for creating QuotaGrants using actual QuotaGrant structure.
type ResourceGrantTemplate struct {
	// Metadata for the created QuotaGrant.
	// String fields support CEL expressions wrapped in {{ }} delimiters.
	//
	// +kubebuilder:validation:Required
	Metadata ObjectMetaTemplate `json:"metadata"`
	// Spec for the created QuotaGrant.
	// String fields support CEL expressions wrapped in {{ }} delimiters.
	//
	// +kubebuilder:validation:Required
	Spec QuotaGrantSpec `json:"spec"`
}

// ResourceClaimTemplate defines how to create QuotaClaims using actual QuotaClaim structure.
//
// +kubebuilder:validation:XValidation:rule="!has(self.spec.resourceRef)",message="resourceRef field is automatically populated and cannot be set in template"
type ResourceClaimTemplate struct {
	// Metadata for the created QuotaClaim.
	// String fields support CEL expressions.
	//
	// +kubebuilder:validation:Required
	Metadata ObjectMetaTemplate `json:"metadata"`
	// Spec for the created QuotaClaim.
	// String fields support CEL expressions.
	//
	// +kubebuilder:validation:Required
	Spec QuotaClaimSpec `json:"spec"`
}

// --- GrantCreationPolicy ---

// GrantCreationPolicySpec defines the desired state of GrantCreationPolicy.
type GrantCreationPolicySpec struct {
	// Trigger defines what resource changes should trigger grant creation.
	//
	// +kubebuilder:validation:Required
	Trigger GrantTriggerSpec `json:"trigger"`
	// Target defines where and how grants should be created.
	//
	// +kubebuilder:validation:Required
	Target GrantTargetSpec `json:"target"`
	// Disabled determines if this policy is inactive.
	// If true, no **QuotaGrants** will be created for matching resources.
	//
	// +kubebuilder:default=false
	// +optional
	Disabled *bool `json:"disabled,omitempty"`
}

// GrantTriggerSpec defines the resource and conditions that trigger grant creation.
type GrantTriggerSpec struct {
	// Resource specifies which resource type triggers this policy.
	//
	// +kubebuilder:validation:Required
	Resource GrantTriggerResource `json:"resource"`
	// Constraints are CEL expressions that must evaluate to true for grant creation.
	// These are pure CEL expressions WITHOUT {{ }} delimiters (unlike template fields).
	// All constraints must pass for the policy to trigger.
	// The 'object' variable contains the trigger resource being evaluated.
	// An expression that fails to evaluate is an error: no grant is created
	// or removed and the trigger is retried. Guard optional fields with has().
	//
	// +optional
	// +kubebuilder:validation:MaxItems=10
	Constraints []ConditionExpression `json:"constraints,omitempty"`
}

// GrantTriggerResource identifies the resource type that triggers grant creation.
type GrantTriggerResource struct {
	// APIVersion of the trigger resource in the format "group/version".
	// For core resources, use "v1".
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\/)?v[0-9]+((alpha|beta)[0-9]*)?$`
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the trigger resource.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Z][a-zA-Z0-9]*$`
	Kind string `json:"kind"`
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
	// ParentContext defines cross-control-plane targeting.
	// If specified, grants will be created in the target parent context
	// instead of the current control plane.
	//
	// +optional
	ParentContext *GrantParentContextSpec `json:"parentContext,omitempty"`
	// ResourceGrantTemplate defines how to create **QuotaGrants**.
	// String fields support CEL expressions wrapped in {{ }} delimiters for dynamic content.
	// Plain strings without {{ }} are treated as literal values.
	//
	// +kubebuilder:validation:Required
	ResourceGrantTemplate ResourceGrantTemplate `json:"resourceGrantTemplate"`
}

// GrantParentContextSpec enables cross-cluster grant creation by targeting a parent control plane.
// Used to create grants in infrastructure clusters when policies run in child clusters.
type GrantParentContextSpec struct {
	// APIGroup specifies the API group of the parent context resource.
	// Must follow DNS subdomain format. Maximum 253 characters.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	APIGroup string `json:"apiGroup"`

	// Kind specifies the resource type that represents the parent context.
	// Must be a valid Kubernetes resource Kind. Maximum 63 characters.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Z][a-zA-Z0-9]*$`
	Kind string `json:"kind"`

	// NameExpression is a CEL expression that resolves the name of the parent context resource.
	// Must return a string value that identifies the specific parent context instance.
	// Maximum 512 characters.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	NameExpression string `json:"nameExpression"`
}

// GrantCreationPolicyStatus defines the observed state of GrantCreationPolicy.
type GrantCreationPolicyStatus struct {
	// ObservedGeneration is the most recent generation observed.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions represent the latest available observations of the policy's current state.
	//
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition type constants for GrantCreationPolicy.
const (
	// GrantCreationPolicyReady indicates the policy is ready for use.
	GrantCreationPolicyReady = "Ready"
	// GrantCreationPolicyParentContextReady indicates parent context resolution is working.
	GrantCreationPolicyParentContextReady = "ParentContextReady"
)

// Condition reason constants for GrantCreationPolicy.
const (
	// GrantCreationPolicyReadyReason indicates the policy is ready.
	GrantCreationPolicyReadyReason = "PolicyReady"
	// GrantCreationPolicyValidationFailedReason indicates validation failed.
	GrantCreationPolicyValidationFailedReason = "ValidationFailed"
	// GrantCreationPolicyDisabledReason indicates the policy is disabled.
	GrantCreationPolicyDisabledReason = "PolicyDisabled"
	// GrantCreationPolicyParentContextReadyReason indicates parent context is ready.
	GrantCreationPolicyParentContextReadyReason = "ParentContextReady"
	// GrantCreationPolicyParentContextFailedReason indicates parent context resolution failed.
	GrantCreationPolicyParentContextFailedReason = "ParentContextFailed"
)

// GrantCreationPolicy automates QuotaGrant creation when observed resources meet conditions.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Trigger",type="string",JSONPath=".spec.trigger.resource.kind"
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +k8s:openapi-gen=true
// +kubebuilder:selectablefield:JSONPath=".spec.trigger.resource.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.trigger.resource.apiVersion"
// +kubebuilder:selectablefield:JSONPath=".spec.target.parentContext.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.target.parentContext.apiGroup"
type GrantCreationPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   GrantCreationPolicySpec   `json:"spec"`
	Status GrantCreationPolicyStatus `json:"status,omitempty"`
}

// GrantCreationPolicyList contains a list of GrantCreationPolicy.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type GrantCreationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GrantCreationPolicy `json:"items"`
}

// --- ClaimCreationPolicy ---

// ClaimCreationPolicySpec defines the desired state of ClaimCreationPolicy.
type ClaimCreationPolicySpec struct {
	// Trigger defines what resource changes should trigger claim creation.
	//
	// +kubebuilder:validation:Required
	Trigger ClaimTriggerSpec `json:"trigger"`
	// Target defines how and where **QuotaClaims** should be created.
	//
	// +kubebuilder:validation:Required
	Target ClaimTargetSpec `json:"target"`
	// Disabled determines if this policy is inactive.
	// If true, no **QuotaClaims** will be created for matching resources.
	//
	// +kubebuilder:default=false
	// +optional
	Disabled *bool `json:"disabled,omitempty"`
}

// ClaimTriggerResource identifies the resource type that triggers this policy.
// Matching is by API group and kind only: the version in apiVersion is not
// compared, so a policy for "example.com/v1" also matches creates served as
// example.com/v1beta1.
type ClaimTriggerResource struct {
	// APIVersion of the trigger resource in the format "group/version" or "version" for core resources.
	// Only the group is used for matching; the version is ignored.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^(v[0-9]+((alpha|beta)[0-9]*)?|[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\/v[0-9]+((alpha|beta)[0-9]*)?)$`
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the trigger resource.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
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
	// Resource specifies which resource type triggers this policy.
	//
	// +kubebuilder:validation:Required
	Resource ClaimTriggerResource `json:"resource"`
	// Constraints are CEL expressions that must evaluate to true for claim creation to occur.
	// These are pure CEL expressions WITHOUT {{ }} delimiters (unlike template fields).
	// Evaluated in the admission context.
	// An expression that fails to evaluate (for example, reading a field the
	// object does not set) denies the create rather than skipping the policy.
	// Guard optional fields with has().
	//
	// +optional
	// +kubebuilder:validation:MaxItems=10
	Constraints []ConditionExpression `json:"constraints,omitempty"`
}

// ClaimTargetSpec defines how **QuotaClaims** are created for a matched trigger.
type ClaimTargetSpec struct {
	// ResourceClaimTemplate defines how to create **QuotaClaims**.
	// String fields support CEL expressions for dynamic content.
	//
	// +kubebuilder:validation:Required
	ResourceClaimTemplate ResourceClaimTemplate `json:"resourceClaimTemplate"`
}

// ClaimCreationPolicyStatus defines the observed state of ClaimCreationPolicy.
type ClaimCreationPolicyStatus struct {
	// ObservedGeneration is the most recent generation observed.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions represent the latest available observations of the policy's current state.
	//
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition type constants for ClaimCreationPolicy.
const (
	// ClaimCreationPolicyReady indicates the policy is ready for use.
	ClaimCreationPolicyReady = "Ready"
	// ClaimCreationPolicyValidationFailed indicates policy validation failed.
	ClaimCreationPolicyValidationFailed = "ValidationFailed"
)

// Condition reason constants for ClaimCreationPolicy.
const (
	// ClaimCreationPolicyReadyReason indicates the policy is ready.
	ClaimCreationPolicyReadyReason = "PolicyReady"
	// ClaimCreationPolicyValidationFailedReason indicates validation failed.
	ClaimCreationPolicyValidationFailedReason = "ValidationFailed"
	// ClaimCreationPolicyDisabledReason indicates the policy is disabled.
	ClaimCreationPolicyDisabledReason = "PolicyDisabled"
)

// ClaimCreationPolicy automatically creates QuotaClaims during admission to enforce quota in real-time.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type="string",JSONPath=".spec.trigger.resource.kind"
// +kubebuilder:printcolumn:name="Disabled",type="boolean",JSONPath=".spec.disabled"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +k8s:openapi-gen=true
// +kubebuilder:selectablefield:JSONPath=".spec.trigger.resource.kind"
// +kubebuilder:selectablefield:JSONPath=".spec.trigger.resource.apiVersion"
// +kubebuilder:selectablefield:JSONPath=".spec.disabled"
type ClaimCreationPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   ClaimCreationPolicySpec   `json:"spec"`
	Status ClaimCreationPolicyStatus `json:"status,omitempty"`
}

// ClaimCreationPolicyList contains a list of ClaimCreationPolicy.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type ClaimCreationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClaimCreationPolicy `json:"items"`
}
