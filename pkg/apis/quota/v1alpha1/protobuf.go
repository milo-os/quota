package v1alpha1

// This file provides minimal protobuf Marshal/Unmarshal methods for the quota
// types so they can be served to clients that request the protobuf content
// type (e.g., the kube-apiserver's namespace garbage collector).
//
// The k8s.io/apimachinery protobuf serializer wraps objects that implement
// Marshal() ([]byte, error) in a runtime.Unknown envelope. We delegate to
// JSON encoding since our types don't have generated protobuf definitions.
// This is the standard approach for aggregated apiservers that don't want
// to generate protobuf bindings.

import "encoding/json"

// --- QuotaDefinition ---

func (in *QuotaDefinition) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *QuotaDefinition) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *QuotaDefinitionList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *QuotaDefinitionList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }

// --- QuotaGrant ---

func (in *QuotaGrant) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *QuotaGrant) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *QuotaGrantList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *QuotaGrantList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }

// --- QuotaClaim ---

func (in *QuotaClaim) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *QuotaClaim) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *QuotaClaimList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *QuotaClaimList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }

// --- QuotaBucket ---

func (in *QuotaBucket) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *QuotaBucket) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *QuotaBucketList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *QuotaBucketList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }

// --- GrantCreationPolicy ---

func (in *GrantCreationPolicy) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *GrantCreationPolicy) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *GrantCreationPolicyList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *GrantCreationPolicyList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }

// --- ClaimCreationPolicy ---

func (in *ClaimCreationPolicy) Marshal() ([]byte, error)        { return json.Marshal(in) }
func (in *ClaimCreationPolicy) Unmarshal(data []byte) error     { return json.Unmarshal(data, in) }
func (in *ClaimCreationPolicyList) Marshal() ([]byte, error)    { return json.Marshal(in) }
func (in *ClaimCreationPolicyList) Unmarshal(data []byte) error { return json.Unmarshal(data, in) }
