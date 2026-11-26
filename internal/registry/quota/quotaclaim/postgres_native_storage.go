// Package quotaclaim also provides the postgres-native rest.Storage layer.
//
// PostgresNativeREST bypasses k8s.io/apiserver/pkg/registry/generic/registry.Store
// and the storage.Interface cacher entirely. It speaks directly to Postgres via
// a pgx-native pool for GET/LIST/UPDATE/WATCH. Watches take their wake-ups
// from the watch hub's shared LISTEN connection. Create/Delete remain delegated to the existing
// AllocatingREST so the round-7 CTE allocator continues to own the write path.
//
// This file houses the type scaffolding, constructor, interface markers, and
// the GET + LIST read path. UPDATE and WATCH are implemented in sibling files.
//
// Wire-up is conditional on StorageBackend == "postgres"; etcd and dual modes
// continue to use quotaclaim.NewStorage (unchanged).
package quotaclaim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	pgwatch "go.miloapis.com/quota/internal/watch"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// claimKeyPrefix is the Postgres storage-key prefix under which QuotaClaim
// rows live. It matches the key produced by the apiserver's generic store
// when the postgres RESTOptionsGetter hands out its ResourcePrefix
// ("quota.miloapis.com/quotaclaims"), to which CompleteWithOptions prepends
// a leading slash before calling NamespaceKeyFunc.
const claimKeyPrefix = "/" + quota.GroupName + "/quotaclaims/"

// PostgresNativeREST is a rest.Storage implementation that talks directly to
// Postgres. It composes AllocatingREST for Create/Delete (which run the
// round-7 CTE allocator inside a pessimistic-locked transaction) and
// implements GET/LIST/WATCH/UPDATE/PATCH natively against the pgx pool.
//
// Instances are safe for concurrent use. Watches hold no connection of
// their own; see Watch.
type PostgresNativeREST struct {
	// AllocatingREST owns Create/Delete. We embed by pointer so method
	// promotion handles the CREATE/DELETE contracts (including strategy
	// validation, UID/timestamp stamping, and allocator delegation)
	// without duplication.
	*AllocatingREST

	pool *pgstore.NativePool
	// hub supplies watch wake-ups from its one shared LISTEN connection.
	// Nil means watches rely on their backup poll only.
	hub       *pgwatch.Hub
	codec     runtime.Codec
	versioner storage.Versioner
	strategy  quotaClaimStrategy
	gvk       schema.GroupVersionKind
	gr        schema.GroupResource
	// tableConvertor implements rest.TableConvertor for kubectl columnar
	// output. Uses the claim convertor from table.go so the columns match
	// the generic storage path.
	tableConvertor rest.TableConvertor
}

// PostgresNativeStatusREST is the /status subresource analog of
// PostgresNativeREST. It implements rest.Updater using the status strategy so
// server-side apply patches targeting /status only touch status fields.
type PostgresNativeStatusREST struct {
	parent *PostgresNativeREST
	// statusStrategy runs status-only PrepareForUpdate + ValidateUpdate.
	statusStrategy quotaClaimStatusStrategy
}

// NewPostgresNativeStorage builds the native QuotaClaim REST storage plus its
// /status subresource counterpart. Both share the same native pool and the
// same AllocatingREST for Create/Delete.
func NewPostgresNativeStorage(
	scheme *runtime.Scheme,
	optsGetter generic.RESTOptionsGetter,
	allocator Allocator,
	nativePool *pgstore.NativePool,
) (*PostgresNativeREST, *PostgresNativeStatusREST, error) {
	if allocator == nil {
		return nil, nil, fmt.Errorf("quotaclaim: native storage requires a non-nil allocator")
	}
	if nativePool == nil {
		return nil, nil, fmt.Errorf("quotaclaim: native storage requires a non-nil native pool")
	}

	allocating, _, err := NewAllocatingStorage(scheme, optsGetter, allocator)
	if err != nil {
		return nil, nil, fmt.Errorf("quotaclaim: construct allocating storage: %w", err)
	}

	// Extract the codec and versioner from the RESTOptions we just opened.
	// The pgstore.RESTOptionsGetter exposes the codec via its Codec()
	// method; we can also fall back to a safe default if that fails.
	codec, err := codecFromOptions(optsGetter)
	if err != nil {
		return nil, nil, fmt.Errorf("quotaclaim: resolve codec: %w", err)
	}

	// Share the getter's watch hub: one LISTEN connection for every claim
	// watch and every other resource's watches.
	var hub *pgwatch.Hub
	if pg, ok := optsGetter.(*pgstore.RESTOptionsGetter); ok {
		hub = pg.Hub()
	}

	native := &PostgresNativeREST{
		AllocatingREST: allocating,
		pool:           nativePool,
		hub:            hub,
		codec:          codec,
		versioner:      storage.APIObjectVersioner{},
		strategy:       NewStrategy(scheme),
		gvk:            v1alpha1.SchemeGroupVersion.WithKind("QuotaClaim"),
		gr:             v1alpha1.Resource("quotaclaims"),
		tableConvertor: NewTableConvertor(),
	}

	status := &PostgresNativeStatusREST{
		parent:         native,
		statusStrategy: NewStatusStrategy(scheme),
	}

	return native, status, nil
}

// codecFromOptions pulls the runtime codec out of a RESTOptionsGetter. The
// postgres RESTOptionsGetter exposes it directly; for other implementations
// we open a RESTOptions for QuotaClaim and dig the codec out of
// StorageConfig.Config.Codec.
func codecFromOptions(optsGetter generic.RESTOptionsGetter) (runtime.Codec, error) {
	if pg, ok := optsGetter.(*pgstore.RESTOptionsGetter); ok {
		if codec := pg.Codec(); codec != nil {
			return codec, nil
		}
	}
	opts, err := optsGetter.GetRESTOptions(v1alpha1.Resource("quotaclaims"), &quota.QuotaClaim{})
	if err != nil {
		return nil, fmt.Errorf("get rest options for codec: %w", err)
	}
	if opts.StorageConfig == nil || opts.StorageConfig.Codec == nil {
		return nil, fmt.Errorf("rest options did not expose a codec")
	}
	return opts.StorageConfig.Codec, nil
}

// --- rest.Storage ---

// New returns an empty *quota.QuotaClaim. Required by rest.Storage.
func (r *PostgresNativeREST) New() runtime.Object { return &quota.QuotaClaim{} }

// Destroy releases the native pool. Called by the apiserver on shutdown.
// Callers that share a NativePool across multiple resources should leave
// Destroy as a no-op — the cmd/quota/serve.go wiring closes the pool
// explicitly.
func (r *PostgresNativeREST) Destroy() {
	// The pool lifecycle is owned by cmd/quota/serve.go so multiple
	// rest.Storage instances could safely share it. Leave this as a no-op
	// intentionally; the serve binary closes the pool on shutdown.
}

// --- rest.Scoper ---

// NamespaceScoped mirrors quotaClaimStrategy.NamespaceScoped.
func (r *PostgresNativeREST) NamespaceScoped() bool { return true }

// --- rest.SingularNameProvider ---

// GetSingularName returns the singular resource name for kubectl.
func (r *PostgresNativeREST) GetSingularName() string { return "quotaclaim" }

// --- rest.GroupVersionKindProvider ---

// GroupVersionKind returns the external GVK served by this storage.
func (r *PostgresNativeREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	return r.gvk
}

// --- rest.TableConvertor ---

// ConvertToTable delegates to the default table convertor for QuotaClaim,
// matching the etcd-backed storage's kubectl output.
func (r *PostgresNativeREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.tableConvertor.ConvertToTable(ctx, object, tableOptions)
}

// --- rest.ResetFieldsStrategy ---

// GetResetFields returns the set of fields that should be reset when the
// field manager merges server-side apply patches. For the main REST endpoint
// we reset nothing (spec is owned by the caller); the status subresource
// resets spec via its own strategy.
func (r *PostgresNativeREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return nil
}

// --- rest.Getter ---

// Get returns the QuotaClaim at (ctx namespace, name). ResourceVersion is
// populated from the stored row's resource_version column.
func (r *PostgresNativeREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok || namespace == "" {
		return nil, apierrors.NewBadRequest("Namespace parameter required.")
	}
	key := claimKeyPrefix + namespace + "/" + name

	var data []byte
	var rv int64
	err := r.pool.Query.QueryRow(ctx,
		`SELECT data, resource_version FROM quota_objects WHERE key = $1`,
		key,
	).Scan(&data, &rv)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return nil, apierrors.NewNotFound(r.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: get claim: %w", err))
	}

	obj, err := r.decodeClaim(data, rv)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	return obj, nil
}

// decodeClaim decodes a row's encoded data into a *quota.QuotaClaim and
// stamps the authoritative resource_version from the row column.
func (r *PostgresNativeREST) decodeClaim(data []byte, rv int64) (*quota.QuotaClaim, error) {
	obj := &quota.QuotaClaim{}
	if _, _, err := r.codec.Decode(data, nil, obj); err != nil {
		return nil, fmt.Errorf("decode claim: %w", err)
	}
	if err := r.versioner.UpdateObject(obj, uint64(rv)); err != nil {
		return nil, fmt.Errorf("set resource version: %w", err)
	}
	return obj, nil
}

// --- rest.Lister ---

// NewList returns an empty QuotaClaimList.
func (r *PostgresNativeREST) NewList() runtime.Object { return &quota.QuotaClaimList{} }

// List returns a QuotaClaimList matching the request scope and selectors.
// Selector pushdown is handled by buildListQuery; any selector that cannot
// be pushed down is applied in memory after the SQL fetch.
//
// Rows and the list RV come from one REPEATABLE READ snapshot. Resource
// versions are assigned in commit order, so a snapshot is
// exactly the state at its high-water mark and the list RV is consistent
// with the items. limit/continue use the upstream continue-token encoding,
// with the same semantics as the generic Postgres store: a page at an
// older RV is served only if nothing in the remaining range changed since
// then, otherwise 410 Gone (with an inconsistent-continue token for
// continue requests).
func (r *PostgresNativeREST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	namespace, _ := genericapirequest.NamespaceFrom(ctx)

	var labelSel, fieldSel = allSelector, allFieldSelector
	var storageOpts storage.ListOptions
	storageOpts.Recursive = true
	if options != nil {
		if options.LabelSelector != nil {
			labelSel = options.LabelSelector
		}
		if options.FieldSelector != nil {
			fieldSel = options.FieldSelector
		}
		storageOpts.ResourceVersion = options.ResourceVersion
		storageOpts.ResourceVersionMatch = options.ResourceVersionMatch
		storageOpts.Predicate.Limit = options.Limit
		storageOpts.Predicate.Continue = options.Continue
	}

	keyPrefix := claimKeyPrefix
	if namespace != "" {
		keyPrefix += namespace + "/"
	}
	withRev, continueKey, err := storage.ValidateListOptions(keyPrefix, r.versioner, storageOpts)
	if err != nil {
		return nil, err
	}
	limit := storageOpts.Predicate.Limit

	baseSQL, baseArgs, postFilter, err := buildListQuery(namespace, labelSel, fieldSel, options)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}

	tx, err := r.pool.Query.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: begin list snapshot: %w", err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var committed, compacted int64
	if err := tx.QueryRow(ctx, pgwatch.StateQuery).Scan(&committed, &compacted); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: read resource version state: %w", err))
	}

	from, fromInclusive := "", false
	if continueKey != "" {
		from, fromInclusive = continueKey, true
		if strings.HasSuffix(continueKey, "\x00") {
			from, fromInclusive = strings.TrimSuffix(continueKey, "\x00"), false
		}
	}

	listRV := committed
	if withRev > 0 {
		if withRev > committed {
			return nil, storage.NewTooLargeResourceVersionError(uint64(withRev), uint64(committed), 0)
		}
		if withRev < committed {
			changed := withRev < compacted
			if !changed {
				lo := keyPrefix
				if from != "" && from > lo {
					lo = from
				}
				if err := tx.QueryRow(ctx,
					`SELECT EXISTS (SELECT 1 FROM quota_changelog
					   WHERE resource_version > $1 AND key >= $2 AND key < $3)`,
					withRev, lo, pgstore.PrefixRangeEnd(keyPrefix),
				).Scan(&changed); err != nil {
					return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: check changes since %d: %w", withRev, err))
				}
			}
			if changed {
				if continueKey != "" {
					return nil, nativeExpiredContinue(continueKey, keyPrefix)
				}
				return nil, apierrors.NewResourceExpired("The resourceVersion for the provided list is too old.")
			}
		}
		listRV = withRev
	} else if minRV, _ := r.versioner.ParseResourceVersion(storageOpts.ResourceVersion); minRV > uint64(committed) {
		return nil, storage.NewTooLargeResourceVersionError(minRV, uint64(committed), 0)
	}

	list := &quota.QuotaClaimList{Items: []quota.QuotaClaim{}}
	batch := limit
	var lastKey string
	hasMore := false
	for {
		sqlText, args := appendPage(baseSQL, append([]any(nil), baseArgs...), from, fromInclusive, batch)
		n, more, err := r.scanClaims(ctx, tx, sqlText, args, func(key string, item *quota.QuotaClaim) bool {
			if limit > 0 && int64(len(list.Items)) >= limit {
				return false
			}
			lastKey = key
			if postFilter == nil || postFilter(item) {
				list.Items = append(list.Items, *item)
			}
			return true
		})
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: list claims: %w", err))
		}
		if more {
			hasMore = true
			break
		}
		if limit <= 0 || int64(n) < batch {
			break
		}
		from, fromInclusive = lastKey, false
		if batch < nativeMaxListBatch {
			batch *= 2
			if batch > nativeMaxListBatch {
				batch = nativeMaxListBatch
			}
		}
	}

	list.ResourceVersion = strconv.FormatInt(listRV, 10)
	if hasMore {
		token, err := storage.EncodeContinue(lastKey+"\x00", keyPrefix, listRV)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: encode continue: %w", err))
		}
		list.Continue = token
		if postFilter == nil && labelSel.Empty() && fieldSel.Empty() {
			var remaining int64
			if err := tx.QueryRow(ctx,
				`SELECT COUNT(*) FROM quota_objects WHERE kind = 'QuotaClaim' AND key > $1 AND key < $2`,
				lastKey, pgstore.PrefixRangeEnd(keyPrefix),
			).Scan(&remaining); err != nil {
				return nil, apierrors.NewInternalError(fmt.Errorf("postgres-native: count remaining: %w", err))
			}
			list.RemainingItemCount = &remaining
		}
	}
	return list, nil
}

// nativeMaxListBatch caps a single page query when post-filtering forces
// extra fetches. Mirrors etcd3's maxLimit.
const nativeMaxListBatch = 10000

// scanClaims runs a list query in tx and feeds decoded claims to fn until
// it returns false. It returns rows consumed and whether rows remained.
func (r *PostgresNativeREST) scanClaims(ctx context.Context, tx pgx.Tx, sqlText string, args []any, fn func(key string, item *quota.QuotaClaim) bool) (int, bool, error) {
	rows, err := tx.Query(ctx, sqlText, args...)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var key string
		var data []byte
		var rv int64
		if err := rows.Scan(&key, &data, &rv); err != nil {
			return n, false, err
		}
		item, err := r.decodeClaim(data, rv)
		if err != nil {
			return n, false, err
		}
		if !fn(key, item) {
			return n, true, nil
		}
		n++
	}
	return n, false, rows.Err()
}

// nativeExpiredContinue mirrors etcd3's handleCompactedErrorForPaging.
func nativeExpiredContinue(continueKey, keyPrefix string) error {
	const msg = "The provided continue parameter is too old to display a consistent list result. You can start a new list without the continue parameter, or use the continue token in this response to retrieve the remainder of the results. Continuing with the provided token results in an inconsistent list - objects that were created, modified, or deleted between the time the first chunk was returned and now may show up in the list."
	token, err := storage.EncodeContinue(continueKey, keyPrefix, -1)
	if err != nil {
		return apierrors.NewResourceExpired("The provided continue parameter is too old to display a consistent list result. You can start a new list without the continue parameter.")
	}
	status := apierrors.NewResourceExpired(msg)
	status.ErrStatus.Continue = token
	return status
}

// currentResourceVersion returns the committed high-water mark, the same
// value the generic Postgres store reports.
func (r *PostgresNativeREST) currentResourceVersion(ctx context.Context) (int64, error) {
	var rv int64
	err := r.pool.Query.QueryRow(ctx, pgwatch.CurrentRVQuery).Scan(&rv)
	return rv, err
}
