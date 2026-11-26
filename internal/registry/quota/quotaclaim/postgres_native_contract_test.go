package quotaclaim

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"

	pgstore "go.miloapis.com/quota/internal/storage/postgres"
	"go.miloapis.com/quota/internal/testutil/pgtest"
	pgwatch "go.miloapis.com/quota/internal/watch"
	"go.miloapis.com/quota/pkg/apis/quota"
	"go.miloapis.com/quota/pkg/apis/quota/v1alpha1"
)

// --- DB-free tests ---

func TestAppendPage(t *testing.T) {
	base, args, _, err := buildListQuery("ns", nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	q, a := appendPage(base, args, "/k/ns/b", false, 3)
	if !strings.Contains(q, `key > $2`) || !strings.HasSuffix(q, `ORDER BY key LIMIT $3`) {
		t.Errorf("unexpected page query: %s", q)
	}
	if len(a) != 3 || a[1] != "/k/ns/b" || a[2] != int64(3) {
		t.Errorf("unexpected args: %v", a)
	}
	q, a = appendPage(base, args, "", false, 0)
	if strings.Contains(q, "LIMIT") || strings.Contains(q, ">") || len(a) != 1 {
		t.Errorf("unpaged query should have no bound or limit: %s %v", q, a)
	}
	q, _ = appendPage(base, args, "/k/ns/b", true, 0)
	if !strings.Contains(q, `>= $2`) {
		t.Errorf("inclusive bound missing: %s", q)
	}
}

// --- DB-gated tests ---

func nativeTestREST(t *testing.T) *PostgresNativeREST {
	t.Helper()
	dsn := pgtest.DSN(t)
	ctx := context.Background()
	pool, err := pgstore.NewNativePool(ctx, dsn)
	if err != nil {
		t.Fatalf("native pool: %v", err)
	}
	t.Cleanup(pool.Close)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	pgtest.ApplyMigrations(t, db)
	hub := pgwatch.NewHub(db, dsn, pgwatch.Options{})
	t.Cleanup(hub.Stop)
	if _, err := pool.Query.Exec(ctx, `DELETE FROM quota_objects WHERE kind = 'QuotaClaim'`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	scheme := testScheme(t)
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)
	return &PostgresNativeREST{
		pool:      pool,
		hub:       hub,
		codec:     codec,
		versioner: storage.APIObjectVersioner{},
		strategy:  NewStrategy(scheme),
		gvk:       v1alpha1.SchemeGroupVersion.WithKind("QuotaClaim"),
		gr:        v1alpha1.Resource("quotaclaims"),
	}
}

// seedClaim writes a claim row and its changelog entry the way the writers
// do and returns the stored claim.
func seedClaim(t *testing.T, r *PostgresNativeREST, name string) *quota.QuotaClaim {
	t.Helper()
	claim := validQuotaClaim()
	claim.Name = name
	claim.UID = "uid-test"
	data, err := runtime.Encode(r.codec, claim)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	key := claimKeyPrefix + claim.Namespace + "/" + name
	var rv int64
	err = r.pool.Query.QueryRow(context.Background(),
		`WITH rv AS MATERIALIZED (SELECT quota_next_rv() AS v),
		 ins AS (INSERT INTO quota_objects (key, resource_version, kind, namespace, name, data)
		         SELECT $1, v, 'QuotaClaim', $2, $3, $4 FROM rv RETURNING resource_version),
		 log AS (INSERT INTO quota_changelog (key, resource_version, event_type, data)
		         SELECT $1, resource_version, 'ADDED', $4 FROM ins)
		 SELECT resource_version FROM ins`,
		key, claim.Namespace, name, data,
	).Scan(&rv)
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	got, err := r.decodeClaim(data, rv)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func nsCtx() context.Context {
	return genericapirequest.WithNamespace(context.Background(), "default")
}

// A PUT carrying a stale resourceVersion must get 409, not overwrite.
func TestNativeUpdate_StaleRVConflicts(t *testing.T) {
	r := nativeTestREST(t)
	claim := seedClaim(t, r, "occ")

	fresh := claim.DeepCopy()
	fresh.Labels = map[string]string{"a": "1"}
	if _, _, err := r.Update(nsCtx(), "occ", rest.DefaultUpdatedObjectInfo(fresh), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("first update: %v", err)
	}

	stale := claim.DeepCopy() // still carries the original RV
	stale.Labels = map[string]string{"a": "2"}
	_, _, err := r.Update(nsCtx(), "occ", rest.DefaultUpdatedObjectInfo(stale), nil, nil, false, &metav1.UpdateOptions{})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected 409 Conflict for stale RV, got %v", err)
	}
}

func TestNativeList_Pagination(t *testing.T) {
	r := nativeTestREST(t)
	for _, n := range []string{"p-a", "p-b", "p-c"} {
		seedClaim(t, r, n)
	}
	var names []string
	cont := ""
	for i := 0; i < 5; i++ {
		obj, err := r.List(nsCtx(), &metainternalversion.ListOptions{Limit: 2, Continue: cont})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		list := obj.(*quota.QuotaClaimList)
		if len(list.Items) > 2 {
			t.Fatalf("page returned %d items for limit 2", len(list.Items))
		}
		for _, it := range list.Items {
			names = append(names, it.Name)
		}
		if list.Continue == "" {
			break
		}
		cont = list.Continue
	}
	if strings.Join(names, ",") != "p-a,p-b,p-c" {
		t.Fatalf("pagination returned %v", names)
	}
}

func TestNativeWatch_ResumeFromRV(t *testing.T) {
	r := nativeTestREST(t)
	first := seedClaim(t, r, "w-a")
	second := seedClaim(t, r, "w-b")

	ctx, cancel := context.WithTimeout(nsCtx(), 10*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: first.ResourceVersion})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	select {
	case ev := <-w.ResultChan():
		if ev.Type != watch.Added || ev.Object.(*quota.QuotaClaim).ResourceVersion != second.ResourceVersion {
			t.Fatalf("expected ADDED at %s, got %v %v", second.ResourceVersion, ev.Type, ev.Object)
		}
	case <-ctx.Done():
		t.Fatal("timed out")
	}
}

// The native /status endpoint cannot change the allocation outcome either.
func TestNativeStatusUpdate_CannotForgeGrant(t *testing.T) {
	r := nativeTestREST(t)
	seedClaim(t, r, "forge")
	status := &PostgresNativeStatusREST{parent: r, statusStrategy: NewStatusStrategy(testScheme(t))}
	_, _, err := status.Update(nsCtx(), "forge", flipToGranted{}, nil, nil, false, &metav1.UpdateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("forged status update: got %v, want Invalid", err)
	}
	got, err := r.Get(nsCtx(), "forge", &metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(got.(*quota.QuotaClaim).Status.Allocations); n != 0 {
		t.Fatalf("forged allocations stored: %d", n)
	}
}

// Field selectors pushed into SQL treat an absent (omitempty) field as the
// empty string, as SelectableFields does.
func TestNativeList_FieldSelectorsOnAbsentFields(t *testing.T) {
	r := nativeTestREST(t)
	seedClaimWith(t, r, "noref", func(c *quota.QuotaClaim) { c.Spec.ResourceRef = quota.UnversionedObjectReference{} })
	seedClaim(t, r, "withref") // resourceRef namespace "default"

	cases := []struct {
		selector string
		want     string
	}{
		{"spec.resourceRef.namespace!=default", "noref"},
		{"spec.resourceRef.apiGroup=", "noref"},
		{"spec.resourceRef.kind=", "noref"},
		{"spec.resourceRef.name=", "noref"},
		{"spec.resourceRef.kind!=Widget", "noref"},
		{"spec.resourceRef.namespace=default", "withref"},
	}
	for _, tc := range cases {
		t.Run(tc.selector, func(t *testing.T) {
			sel, err := fields.ParseSelector(tc.selector)
			if err != nil {
				t.Fatal(err)
			}
			obj, err := r.List(nsCtx(), &metainternalversion.ListOptions{FieldSelector: sel})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			var names []string
			for _, it := range obj.(*quota.QuotaClaimList).Items {
				names = append(names, it.Name)
			}
			if strings.Join(names, ",") != tc.want {
				t.Fatalf("got %v, want [%s]", names, tc.want)
			}
		})
	}
}
