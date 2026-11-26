package quotaclaim

import (
	"context"
	"strconv"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"

	"go.miloapis.com/quota/pkg/apis/quota"
)

// The allocator writes a claim's ADDED changelog row without
// data. These tests write such rows directly, the way the allocator does,
// and check that the native watch rebuilds them and that a native update
// back-fills them.

// seedElidedClaim writes a claim row and an ADDED changelog row without data.
func seedElidedClaim(t *testing.T, r *PostgresNativeREST, name string) (*quota.QuotaClaim, []byte) {
	t.Helper()
	claim := validQuotaClaim()
	claim.Name = name
	claim.UID = "uid-elided"
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
		         SELECT $1, resource_version, 'ADDED', NULL FROM ins)
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
	return got, data
}

// nativeEvents watches from rv and returns the first n events.
func nativeEvents(t *testing.T, r *PostgresNativeREST, rv string, n int) []watch.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(nsCtx(), 10*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: rv})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	var events []watch.Event
	for len(events) < n {
		select {
		case ev := <-w.ResultChan():
			events = append(events, ev)
		case <-ctx.Done():
			t.Fatalf("timed out after %d of %d events", len(events), n)
		}
	}
	return events
}

// previousRV returns rv - 1, the RV to watch from to see the write at rv.
func previousRV(t *testing.T, rv string) string {
	t.Helper()
	n, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		t.Fatalf("bad rv %q: %v", rv, err)
	}
	return strconv.FormatInt(n-1, 10)
}

func TestNativeElision_WatchRebuildsAndUpdateBackfills(t *testing.T) {
	r := nativeTestREST(t)
	claim, stored := seedElidedClaim(t, r, "elided")
	key := claimKeyPrefix + claim.Namespace + "/" + claim.Name
	from := previousRV(t, claim.ResourceVersion)

	// Live object: rebuilt from quota_objects.
	ev := nativeEvents(t, r, from, 1)[0]
	if ev.Type != watch.Added || !apiequality.Semantic.DeepEqual(ev.Object, claim) {
		t.Fatalf("ADDED event = %s %+v, want %+v", ev.Type, ev.Object, claim)
	}

	updated := claim.DeepCopy()
	updated.Labels = map[string]string{"example.com/updated": "true"}
	obj, _, err := r.Update(nsCtx(), claim.Name, rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	var backfilled []byte
	if err := r.pool.Query.QueryRow(context.Background(),
		`SELECT data FROM quota_changelog WHERE key = $1 AND event_type = 'ADDED'`, key).Scan(&backfilled); err != nil {
		t.Fatal(err)
	}
	if string(backfilled) != string(stored) {
		t.Fatalf("ADDED row after update = %q, want the pre-update object", backfilled)
	}

	events := nativeEvents(t, r, from, 2)
	if events[0].Type != watch.Added || !apiequality.Semantic.DeepEqual(events[0].Object, claim) {
		t.Errorf("replayed ADDED = %s %+v, want %+v", events[0].Type, events[0].Object, claim)
	}
	if events[1].Type != watch.Modified || events[1].Object.(*quota.QuotaClaim).ResourceVersion != obj.(*quota.QuotaClaim).ResourceVersion {
		t.Errorf("replayed MODIFIED = %s %+v", events[1].Type, events[1].Object)
	}
}

func TestNativeElision_RebuiltFromDeletedRow(t *testing.T) {
	r := nativeTestREST(t)
	claim, _ := seedElidedClaim(t, r, "elided-deleted")
	key := claimKeyPrefix + claim.Namespace + "/" + claim.Name
	from := previousRV(t, claim.ResourceVersion)

	// Delete the way the allocator's release does: the DELETED row carries
	// the row's last data.
	if _, err := r.pool.Query.Exec(context.Background(),
		`WITH del AS (DELETE FROM quota_objects WHERE key = $1 RETURNING key, data)
		 INSERT INTO quota_changelog (key, resource_version, event_type, data)
		 SELECT key, quota_next_rv(), 'DELETED', data FROM del`, key); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events := nativeEvents(t, r, from, 2)
	if events[0].Type != watch.Added || !apiequality.Semantic.DeepEqual(events[0].Object, claim) {
		t.Errorf("ADDED = %s %+v, want %+v", events[0].Type, events[0].Object, claim)
	}
	deleted, ok := events[1].Object.(*quota.QuotaClaim)
	if events[1].Type != watch.Deleted || !ok {
		t.Fatalf("second event = %s %+v, want DELETED", events[1].Type, events[1].Object)
	}
	want := claim.DeepCopy()
	want.ResourceVersion = deleted.ResourceVersion
	if !apiequality.Semantic.DeepEqual(deleted, want) {
		t.Errorf("DELETED object = %+v, want %+v", deleted, want)
	}
}
