package quotaclaim

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	"go.miloapis.com/quota/pkg/apis/quota"
)

// maxUpdateRetries bounds retries of storage-level races: the row changed
// between the unlocked read and the conditional write, or Postgres reported
// a serialization failure (40001, e.g. the commit-order trigger) or a
// deadlock (40P01). Each retry re-reads the row and re-applies the update,
// so a client-supplied resourceVersion is checked again and yields 409.
const maxUpdateRetries = 5

// updateCTE writes the claim and its MODIFIED changelog row in one
// autocommit statement. The claim row is locked and its RV re-checked in
// tgt before quota_next_rv() runs (rv reads FROM tgt), so the row lock is
// always taken before the commit-order lock. The statement touches only the
// claim row and the claim's current changelog row: no bucket lock is ever
// taken after the claim lock. The changelog row is back-filled before the
// commit-order lock (pgstore.ChangelogBackfillCTE).
const updateCTE = `WITH tgt AS MATERIALIZED (
	   SELECT key, data FROM quota_objects WHERE key = $1 AND resource_version = $2 FOR UPDATE
	 ),
	 ` + pgstore.ChangelogBackfillCTE + `,
	 rv AS MATERIALIZED (SELECT quota_next_rv() AS v FROM tgt, (SELECT count(*) FROM backfill) bf),
	 upd AS (
	   UPDATE quota_objects o
	      SET resource_version = rv.v, data = $3, updated_at = NOW()
	     FROM rv, tgt
	    WHERE o.key = tgt.key
	   RETURNING o.resource_version
	 ),
	 log AS (
	   INSERT INTO quota_changelog (key, resource_version, event_type, data, created_at)
	   SELECT $1, resource_version, 'MODIFIED', $3, NOW() FROM upd
	 )
	 SELECT resource_version FROM upd`

// errStorageRace marks an attempt that lost a storage-level race.
var errStorageRace = errors.New("postgres-native: claim changed during update")

// rowUpdateStrategy is the minimal strategy interface rest.BeforeUpdate needs
// to run PrepareForUpdate + ValidateUpdate. Both quotaClaimStrategy and
// quotaClaimStatusStrategy satisfy it.
type rowUpdateStrategy interface {
	rest.RESTUpdateStrategy
}

// Update runs a read-modify-write for the QuotaClaim at the request path. It
// applies the spec strategy (not status) so status fields are preserved from
// the existing object.
func (r *PostgresNativeREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	return r.updateWithStrategy(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options, r.strategy)
}

// Update runs the status-subresource read-modify-write. PostgresNativeStatusREST
// pins forceAllowCreate to false (like the standard QuotaClaimStatusStorage
// does) so status patches against missing objects return NotFound rather than
// routing through CreateStrategy.
func (r *PostgresNativeStatusREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	return r.parent.updateWithStrategy(ctx, name, objInfo, createValidation, updateValidation, false, options, r.statusStrategy)
}

// updateWithStrategy is the shared read-modify-write loop. After a write
// that removes the last finalizer from a claim that is being deleted, it
// performs the final removal through AllocatingREST.FinalizeDeletion, which
// releases the claim's capacity and deletes the row exactly once.
func (r *PostgresNativeREST) updateWithStrategy(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
	strat rowUpdateStrategy,
) (runtime.Object, bool, error) {
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok || namespace == "" {
		return nil, false, apierrors.NewBadRequest("Namespace parameter required.")
	}
	key := claimKeyPrefix + namespace + "/" + name
	var dryRun []string
	if options != nil {
		dryRun = options.DryRun
	}

	for attempt := 0; attempt < maxUpdateRetries; attempt++ {
		obj, existing, err := r.updateOnce(ctx, key, name, objInfo, updateValidation, strat, len(dryRun) > 0)
		if errors.Is(err, errStorageRace) || isRetryableTxError(err) {
			klog.V(4).InfoS("postgres-native: QuotaClaim update lost a storage race; retrying", "key", key, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return nil, false, err
		}
		if genericregistry.ShouldDeleteDuringUpdate(ctx, key, obj, existing) {
			// The update committed with no finalizers left on a claim that
			// is being deleted. FinalizeDeletion is conditioned on the
			// committed UID and RV, releases capacity, and is idempotent.
			if err := r.FinalizeDeletion(ctx, obj, dryRun); err != nil {
				return nil, false, err
			}
		}
		return obj, false, nil
	}
	return nil, false, apierrors.NewConflict(r.gr, name, errors.New(genericregistry.OptimisticLockErrorMsg))
}

// updateOnce performs one attempt. It returns the new object and the
// existing object it was computed from.
func (r *PostgresNativeREST) updateOnce(
	ctx context.Context,
	key, name string,
	objInfo rest.UpdatedObjectInfo,
	updateValidation rest.ValidateObjectUpdateFunc,
	strat rowUpdateStrategy,
	dryRun bool,
) (*quota.QuotaClaim, *quota.QuotaClaim, error) {
	var existingData []byte
	var existingRV int64
	err := r.pool.Query.QueryRow(ctx,
		`SELECT data, resource_version FROM quota_objects WHERE key = $1`,
		key,
	).Scan(&existingData, &existingRV)
	if errors.Is(err, pgx.ErrNoRows) {
		// strategy.AllowCreateOnUpdate() is false for both endpoints, so a
		// missing row is always NotFound.
		return nil, nil, apierrors.NewNotFound(r.gr, name)
	}
	if err != nil {
		return nil, nil, apierrors.NewInternalError(fmt.Errorf("read existing: %w", err))
	}

	existing, err := r.decodeClaim(existingData, existingRV)
	if err != nil {
		return nil, nil, apierrors.NewInternalError(err)
	}

	// Preconditions (UID / resourceVersion) supplied by the request.
	if pre := objInfo.Preconditions(); pre != nil {
		if pre.UID != nil && *pre.UID != existing.UID {
			return nil, nil, apierrors.NewConflict(r.gr, name,
				fmt.Errorf("precondition failed: UID in precondition: %v, UID in object meta: %v", *pre.UID, existing.UID))
		}
		if pre.ResourceVersion != nil && *pre.ResourceVersion != existing.ResourceVersion {
			return nil, nil, apierrors.NewConflict(r.gr, name,
				fmt.Errorf("precondition failed: ResourceVersion in precondition: %v, ResourceVersion in object meta: %v", *pre.ResourceVersion, existing.ResourceVersion))
		}
	}

	newObj, err := objInfo.UpdatedObject(ctx, existing.DeepCopyObject())
	if err != nil {
		return nil, nil, err
	}
	newClaim, ok := newObj.(*quota.QuotaClaim)
	if !ok {
		return nil, nil, apierrors.NewInternalError(fmt.Errorf("expected *quota.QuotaClaim, got %T", newObj))
	}

	// Optimistic concurrency, as genericregistry.Store.Update does it: a
	// non-zero resourceVersion must match the stored one; zero means an
	// unconditional update, which the strategy must allow.
	newRVParsed, err := r.versioner.ObjectResourceVersion(newClaim)
	if err != nil {
		return nil, nil, apierrors.NewBadRequest(fmt.Sprintf("invalid resourceVersion: %v", err))
	}
	if newRVParsed == 0 {
		if !strat.AllowUnconditionalUpdate() {
			return nil, nil, apierrors.NewConflict(r.gr, name, errors.New("metadata.resourceVersion must be specified for an update"))
		}
	} else if int64(newRVParsed) != existingRV {
		return nil, nil, apierrors.NewConflict(r.gr, name, errors.New(genericregistry.OptimisticLockErrorMsg))
	}

	if err := rest.BeforeUpdate(strat, ctx, newClaim, existing); err != nil {
		return nil, nil, err
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, newClaim.DeepCopyObject(), existing.DeepCopyObject()); err != nil {
			return nil, nil, err
		}
	}
	// The synchronous allocator owns the allocation outcome; release trusts
	// it (see checkAllocationOutcomeUnchanged).
	if err := checkAllocationOutcomeUnchanged(existing, newClaim); err != nil {
		return nil, nil, err
	}

	// Encode both sides with the RV cleared so the no-op check compares
	// like with like; the authoritative RV is the row column.
	newData, err := r.encodeForStorage(newClaim.DeepCopy())
	if err != nil {
		return nil, nil, apierrors.NewInternalError(fmt.Errorf("encode updated: %w", err))
	}
	if oldData, err := r.encodeForStorage(existing.DeepCopy()); err == nil && bytes.Equal(newData, oldData) {
		return existing, existing, nil
	}

	if dryRun {
		newClaim.ResourceVersion = existing.ResourceVersion
		return newClaim, existing, nil
	}

	var newRV int64
	err = r.pool.Query.QueryRow(ctx, updateCTE, key, existingRV, newData).Scan(&newRV)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, errStorageRace
	}
	if err != nil {
		if isRetryableTxError(err) {
			return nil, nil, err
		}
		return nil, nil, apierrors.NewInternalError(fmt.Errorf("update claim: %w", err))
	}
	if err := r.versioner.UpdateObject(newClaim, uint64(newRV)); err != nil {
		return nil, nil, apierrors.NewInternalError(fmt.Errorf("set rv on new: %w", err))
	}
	return newClaim, existing, nil
}

// encodeForStorage encodes a claim with its resourceVersion cleared.
func (r *PostgresNativeREST) encodeForStorage(claim *quota.QuotaClaim) ([]byte, error) {
	if err := r.versioner.PrepareObjectForStorage(claim); err != nil {
		return nil, err
	}
	return runtime.Encode(r.codec, claim)
}

// isRetryableTxError reports serialization failures (40001) and deadlocks
// (40P01): the attempt lost a race and can be redone from a fresh read.
func isRetryableTxError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}
