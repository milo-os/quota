// Package quotaclaim additionally provides an allocator-aware decorator
// around the standard QuotaClaim REST storage. The decorator runs the
// transactional allocator inline with the user's POST (and DELETE), so
// QuotaClaim creates return the final granted/denied status without
// round-tripping through a reconciliation controller.
package quotaclaim

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/klog/v2"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// Allocator runs the synchronous quota allocation transaction. It is
// defined locally so the registry package does not depend on the
// storage/postgres package — the wiring in cmd/quota/serve.go supplies a
// concrete implementation that satisfies this interface.
//
// Works with internal types: the REST registry layer uses internal types
// throughout (the Postgres codec also encodes/decodes internal types), so
// the allocator avoids unnecessary conversions in the hot path.
type Allocator interface {
	Allocate(ctx context.Context, claim *quota.QuotaClaim) (*quota.QuotaClaim, error)
	Release(ctx context.Context, claim *quota.QuotaClaim) error
}

// DryRunAllocator is an optional Allocator capability: evaluate a claim
// exactly as Allocate would, without writing anything. The Postgres
// allocator implements it. It is a separate interface so existing Allocator
// implementations (and the apiserver's ExtraConfig interface) keep compiling.
type DryRunAllocator interface {
	AllocateDryRun(ctx context.Context, claim *quota.QuotaClaim) (*quota.QuotaClaim, error)
}

// ConditionalReleaser is an optional Allocator capability: Release that
// reports whether this call removed the claim row. released=false with a nil
// error means the row was already gone. Implementations must condition the
// delete on the claim's resourceVersion/UID and return a Conflict when the
// stored row no longer matches. The Postgres allocator implements it.
type ConditionalReleaser interface {
	ReleaseClaim(ctx context.Context, claim *quota.QuotaClaim) (released bool, err error)
}

// maxReleaseAttempts bounds the re-read/retry loop when the claim changes
// between the read and the conditional release.
const maxReleaseAttempts = 5

// AllocatingREST wraps the standard QuotaClaim REST storage and routes
// writes that change bucket accounting through the Allocator:
//
//   - Create runs the allocator, which writes the claim together with its
//     bucket reservations in one transaction.
//   - Every path that physically removes a claim row (Delete,
//     DeleteCollection, and the delete-during-update performed when the last
//     finalizer is removed from a deleting claim) goes through
//     genericregistry.Store, whose storage layer is wrapped by
//     releasingStorage so the final removal releases capacity in the same
//     transaction as the row delete.
//
// Routing deletion through genericregistry.Store keeps its semantics
// verbatim: preconditions (UID/resourceVersion), finalizers (set
// deletionTimestamp, keep the object, release only at final removal), grace
// period handling, propagation-policy finalizers, and dry run (the
// DryRunnableStorage layer never calls the wrapped Delete for dry-run
// requests, so a dry-run delete never releases).
type AllocatingREST struct {
	*QuotaClaimStorage
	allocator Allocator
	scheme    *runtime.Scheme
	strategy  quotaClaimStrategy
}

// NewAllocatingStorage builds a QuotaClaim REST storage that runs the
// supplied Allocator inline with create/delete requests. The status
// subresource storage is constructed from the same underlying generic
// store so status reads/updates keep their normal behavior; its storage
// layer is wrapped too, so a status update that empties the finalizer list
// of a deleting claim also releases capacity.
func NewAllocatingStorage(
	scheme *runtime.Scheme,
	optsGetter generic.RESTOptionsGetter,
	allocator Allocator,
) (*AllocatingREST, *QuotaClaimStatusStorage, error) {
	if allocator == nil {
		return nil, nil, fmt.Errorf("quotaclaim: allocating storage requires a non-nil allocator")
	}
	claimStorage, statusStorage, err := NewStorage(scheme, optsGetter)
	if err != nil {
		return nil, nil, err
	}

	wrapReleasingStorage(claimStorage.Store, allocator)
	wrapReleasingStorage(statusStorage.store, allocator)

	return &AllocatingREST{
		QuotaClaimStorage: claimStorage,
		allocator:         allocator,
		scheme:            scheme,
		strategy:          NewStrategy(scheme),
	}, statusStorage, nil
}

// wrapReleasingStorage replaces store's storage.Interface with one whose
// Delete releases bucket capacity through the allocator.
func wrapReleasingStorage(store *genericregistry.Store, allocator Allocator) {
	if _, already := store.Storage.Storage.(*releasingStorage); already {
		return
	}
	store.Storage.Storage = &releasingStorage{Interface: store.Storage.Storage, allocator: allocator}
}

// Create runs strategy validation, then delegates to the allocator which
// performs the storage write itself. For dry-run requests the allocator's
// dry-run evaluation reports what would happen (Granted/Denied status,
// Forbidden for a missing bucket, AlreadyExists) without writing.
func (r *AllocatingREST) Create(
	ctx context.Context,
	obj runtime.Object,
	createValidation rest.ValidateObjectFunc,
	options *metav1.CreateOptions,
) (runtime.Object, error) {
	internalClaim, ok := obj.(*quota.QuotaClaim)
	if !ok {
		return nil, fmt.Errorf("expected *quota.QuotaClaim, got %T", obj)
	}

	// Populate system metadata (UID, CreationTimestamp, generation) the way
	// genericregistry.Store.Create does it. We must do this BEFORE
	// rest.BeforeCreate because BeforeCreate validates that the system
	// fields are populated and returns "system metadata was not initialized"
	// otherwise. The standard store does the same in its Create method;
	// we're replicating that prelude because we're replacing the storage
	// write entirely.
	objectMeta, err := meta.Accessor(internalClaim)
	if err != nil {
		return nil, fmt.Errorf("get object metadata: %w", err)
	}
	rest.FillObjectMetaSystemFields(objectMeta)

	// Run the same create pipeline the standard store would run:
	// PrepareForCreate + Validate. This preserves immutable-field checks,
	// defaulting, and status reset performed by the strategy.
	if err := rest.BeforeCreate(r.strategy, ctx, internalClaim); err != nil {
		return nil, err
	}
	if createValidation != nil {
		if err := createValidation(ctx, internalClaim.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	if options != nil && dryrun.IsDryRun(options.DryRun) {
		if dr, ok := r.allocator.(DryRunAllocator); ok {
			return dr.AllocateDryRun(ctx, internalClaim)
		}
		// Without dry-run support the outcome cannot be predicted; return
		// the validated object (what genericregistry returns for a dry-run
		// create) and never call Allocate.
		return internalClaim, nil
	}

	// The allocator writes the encoded internal claim directly to
	// quota_objects within a transaction that also updates the matching
	// bucket(s). Returns the persisted claim with status fully populated.
	return r.allocator.Allocate(ctx, internalClaim)
}

// Delete implements rest.GracefulDeleter with genericregistry semantics.
// Capacity is released only when the claim row is actually removed (see
// releasingStorage); while finalizers remain the claim is marked for
// deletion and kept, and its reservation stays in place.
func (r *AllocatingREST) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
) (runtime.Object, bool, error) {
	return r.Store.Delete(withDeleteRead(ctx), name, deleteValidation, options)
}

// DeleteCollection implements rest.CollectionDeleter. genericregistry's
// implementation deletes each listed item through Store.Delete, and every
// final removal goes through releasingStorage, so each item's capacity is
// released individually (with the same finalizer, precondition, and dry-run
// handling as a single Delete). It is overridden explicitly to document that
// guarantee and to keep it if the embedding ever changes.
func (r *AllocatingREST) DeleteCollection(
	ctx context.Context,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
	listOptions *metainternalversion.ListOptions,
) (runtime.Object, error) {
	return r.Store.DeleteCollection(ctx, deleteValidation, options, listOptions)
}

// FinalizeDeletion removes a claim whose deletion has become due outside the
// Delete path -- i.e. an update removed the last finalizer from a claim that
// already has a deletionTimestamp (and no remaining grace period). It is a
// no-op unless that condition holds. The removal is conditioned on the
// claim's UID and resourceVersion and releases its capacity. A claim that is
// already gone is not an error.
//
// genericregistry.Store.Update does this automatically for the generic
// path; REST implementations that bypass Store.Update call this after
// committing their update.
func (r *AllocatingREST) FinalizeDeletion(ctx context.Context, claim *quota.QuotaClaim, dryRunFlags []string) error {
	if claim.DeletionTimestamp == nil || len(claim.Finalizers) > 0 {
		return nil
	}
	if gp := claim.DeletionGracePeriodSeconds; gp != nil && *gp > 0 {
		return nil
	}
	uid := claim.UID
	rv := claim.ResourceVersion
	_, _, err := r.Store.Delete(withDeleteRead(ctx), claim.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
		DryRun:        dryRunFlags,
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// releasingStorage decorates the claim store's storage.Interface so that the
// physical removal of a claim row is performed by the allocator, which
// releases the claim's bucket reservations in the same transaction. All
// other operations pass through unchanged.
type releasingStorage struct {
	storage.Interface
	allocator Allocator
}

// deleteReadKey is the context key for a deleteRead.
type deleteReadKey struct{}

// deleteRead carries the claim that genericregistry.Store.Delete reads at
// the start of a delete request down to releasingStorage.Delete, so the
// release does not read the same row again. genericregistry passes no
// cached object to storage.Interface.Delete, so this is how the read is
// shared. It serves a single Delete call: Store.Delete runs its read and
// its storage delete sequentially on one goroutine.
type deleteRead struct {
	key   string
	claim *quota.QuotaClaim
	used  bool
}

func withDeleteRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, deleteReadKey{}, &deleteRead{})
}

func deleteReadFrom(ctx context.Context) *deleteRead {
	dr, _ := ctx.Value(deleteReadKey{}).(*deleteRead)
	return dr
}

// Get records the first claim read during a delete request (see
// deleteRead) and otherwise passes through.
func (s *releasingStorage) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	if err := s.Interface.Get(ctx, key, opts, out); err != nil {
		return err
	}
	if dr := deleteReadFrom(ctx); dr != nil && !dr.used && dr.claim == nil {
		if c, ok := out.(*quota.QuotaClaim); ok && c.ResourceVersion != "" {
			dr.key, dr.claim = key, c.DeepCopy()
		}
	}
	return nil
}

// GuaranteedUpdate discards a recorded delete read: graceful deletion and
// finalizer handling update the claim before removing it, so the recorded
// version is stale.
func (s *releasingStorage) GuaranteedUpdate(
	ctx context.Context,
	key string,
	destination runtime.Object,
	ignoreNotFound bool,
	preconditions *storage.Preconditions,
	tryUpdate storage.UpdateFunc,
	cachedExistingObject runtime.Object,
) error {
	if dr := deleteReadFrom(ctx); dr != nil {
		dr.used = true
	}
	guarded := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		out, ttl, err := tryUpdate(input, res)
		if err != nil {
			return nil, nil, err
		}
		oldClaim, okOld := input.(*quota.QuotaClaim)
		newClaim, okNew := out.(*quota.QuotaClaim)
		if okOld && okNew && oldClaim.ResourceVersion != "" {
			if err := checkAllocationOutcomeUnchanged(oldClaim, newClaim); err != nil {
				return nil, nil, err
			}
		}
		return out, ttl, nil
	}
	return s.Interface.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, guarded, cachedExistingObject)
}

// checkAllocationOutcomeUnchanged rejects an update that changes what the
// allocator decided for a claim: status.allocations or the Granted
// condition's status and reason. With a synchronous allocator those are
// written once, in the transaction that reserves capacity, and nothing else
// may change them. Release returns capacity for the resource types the
// status says were granted, so a client with quotaclaims/status write
// access could otherwise flip a Denied claim to Granted and have its
// deletion drain a bucket.
func checkAllocationOutcomeUnchanged(oldClaim, newClaim *quota.QuotaClaim) error {
	if !allocationOutcomeChanged(oldClaim, newClaim) {
		return nil
	}
	return apierrors.NewInvalid(quota.Kind("QuotaClaim"), newClaim.Name, field.ErrorList{
		field.Forbidden(field.NewPath("status", "allocations"),
			"allocation results and the Granted condition are set by the quota allocator when the claim is created and cannot be changed"),
	})
}

// allocationOutcomeChanged reports whether newClaim differs from oldClaim in
// status.allocations or in the Granted condition's status or reason.
func allocationOutcomeChanged(oldClaim, newClaim *quota.QuotaClaim) bool {
	if !equality.Semantic.DeepEqual(oldClaim.Status.Allocations, newClaim.Status.Allocations) {
		return true
	}
	oldCond := meta.FindStatusCondition(oldClaim.Status.Conditions, quota.QuotaClaimGranted)
	newCond := meta.FindStatusCondition(newClaim.Status.Conditions, quota.QuotaClaimGranted)
	switch {
	case oldCond == nil && newCond == nil:
		return false
	case oldCond == nil || newCond == nil:
		return true
	default:
		return oldCond.Status != newCond.Status || oldCond.Reason != newCond.Reason
	}
}

// cachedClaim returns the claim recorded for key by this request's delete
// read, at most once. It is only used with a ConditionalReleaser: the
// release is conditioned on the claim's resourceVersion and UID, so a copy
// that went stale fails with a Conflict and the caller re-reads. A plain
// Release has no such check, so it always gets a fresh read.
func (s *releasingStorage) cachedClaim(ctx context.Context, key string) *quota.QuotaClaim {
	if _, ok := s.allocator.(ConditionalReleaser); !ok {
		return nil
	}
	dr := deleteReadFrom(ctx)
	if dr == nil || dr.used || dr.claim == nil || dr.key != key {
		return nil
	}
	dr.used = true
	return dr.claim
}

// Delete reads the claim, enforces preconditions and delete validation,
// then asks the allocator to delete exactly that version and release its
// capacity. The first attempt reuses the claim genericregistry.Store.Delete
// just read, when there is one (see deleteRead). If the claim changes
// between the read and the release (a conditional-release Conflict), the
// loop re-reads and re-checks, mirroring the optimistic-concurrency retry
// the etcd3 store performs with its cached existing object.
func (s *releasingStorage) Delete(
	ctx context.Context,
	key string,
	out runtime.Object,
	preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc,
	_ runtime.Object,
	_ storage.DeleteOptions,
) error {
	outClaim, ok := out.(*quota.QuotaClaim)
	if !ok {
		return storage.NewInternalError(fmt.Errorf("expected *quota.QuotaClaim output, got %T", out))
	}

	for attempt := 0; attempt < maxReleaseAttempts; attempt++ {
		current := s.cachedClaim(ctx, key)
		if current == nil {
			current = &quota.QuotaClaim{}
			if err := s.Interface.Get(ctx, key, storage.GetOptions{}, current); err != nil {
				return err
			}
		}
		if err := preconditions.Check(key, current); err != nil {
			return err
		}
		if validateDeletion != nil {
			if err := validateDeletion(ctx, current); err != nil {
				return err
			}
		}

		released, err := s.release(ctx, current)
		if apierrors.IsConflict(err) {
			klog.V(4).InfoS("QuotaClaim changed during release; retrying", "key", key, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return err
		}
		if !released {
			// Removed concurrently by another request; that request
			// performed the release.
			return storage.NewKeyNotFoundError(key, 0)
		}
		*outClaim = *current
		return nil
	}
	return storage.NewResourceVersionConflictsError(key, 0)
}

func (s *releasingStorage) release(ctx context.Context, claim *quota.QuotaClaim) (bool, error) {
	if cr, ok := s.allocator.(ConditionalReleaser); ok {
		return cr.ReleaseClaim(ctx, claim)
	}
	if err := s.allocator.Release(ctx, claim); err != nil {
		return false, err
	}
	return true, nil
}
