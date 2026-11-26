package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"

	pgwatch "go.miloapis.com/quota/internal/watch"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

// ADDED changelog rows of allocator-created claims
// are written without data and rebuilt by the watch readers.

// addedRowData returns the stored data of the claim's ADDED row.
func addedRowData(t *testing.T, db *sql.DB, key string) []byte {
	t.Helper()
	var data []byte
	if err := db.QueryRow(`SELECT data FROM quota_changelog WHERE key = $1 AND event_type = 'ADDED'`, key).Scan(&data); err != nil {
		t.Fatalf("read ADDED row for %s: %v", key, err)
	}
	return data
}

// newFastWatcher returns a watcher whose hub polls every 50ms, so tests do
// not depend on LISTEN/NOTIFY.
func newFastWatcher(t *testing.T, db *sql.DB) *pgwatch.PostgresWatcher {
	t.Helper()
	hub := pgwatch.NewHub(db, "", pgwatch.Options{PollInterval: 50 * time.Millisecond})
	t.Cleanup(hub.Stop)
	return pgwatch.NewWithHub(hub, allocatorTestCodec, nil)
}

// collectEvents watches the claims in namespace from rv and returns the
// first n events.
func collectEvents(t *testing.T, w *pgwatch.PostgresWatcher, namespace string, rv int64, n int) []watch.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wi, err := w.Watch(ctx, "/quota.miloapis.com/quotaclaims/"+namespace, storage.ListOptions{
		ResourceVersion: fmt.Sprint(rv),
		Recursive:       true,
		Predicate:       storage.Everything,
	}, nil)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer wi.Stop()
	var events []watch.Event
	for len(events) < n {
		select {
		case ev, ok := <-wi.ResultChan():
			if !ok {
				t.Fatalf("watch closed after %d of %d events", len(events), n)
			}
			events = append(events, ev)
			if ev.Type == watch.Error {
				return events
			}
		case <-ctx.Done():
			t.Fatalf("timed out after %d of %d events", len(events), n)
		}
	}
	return events
}

// storedClaim decodes the claim's current quota_objects row: what an
// un-elided changelog row written with it would carry.
func storedClaim(t *testing.T, db *sql.DB, key string) *quotainternal.QuotaClaim {
	t.Helper()
	var data []byte
	if err := db.QueryRow(`SELECT data FROM quota_objects WHERE key = $1`, key).Scan(&data); err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	obj, err := decodeToObject(allocatorTestCodec, data)
	if err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return obj.(*quotainternal.QuotaClaim)
}

func currentRV(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var rv int64
	if err := db.QueryRow(pgwatch.CurrentRVQuery).Scan(&rv); err != nil {
		t.Fatalf("current rv: %v", err)
	}
	return rv
}

// assertClaimEvent checks an event's type, its resource version and that
// its object equals want apart from the resource version.
func assertClaimEvent(t *testing.T, ev watch.Event, typ watch.EventType, rv string, want *quotainternal.QuotaClaim) {
	t.Helper()
	if ev.Type != typ {
		t.Fatalf("event type = %s (%v), want %s", ev.Type, ev.Object, typ)
	}
	got, ok := ev.Object.(*quotainternal.QuotaClaim)
	if !ok {
		t.Fatalf("event object is %T", ev.Object)
	}
	if got.ResourceVersion != rv {
		t.Errorf("%s event rv = %s, want %s", typ, got.ResourceVersion, rv)
	}
	g, w := got.DeepCopy(), want.DeepCopy()
	g.ResourceVersion, w.ResourceVersion = "", ""
	if !apiequality.Semantic.DeepEqual(g, w) {
		t.Errorf("%s event object differs:\n got %+v\nwant %+v", typ, g, w)
	}
}

func TestChangelogElision_AddedRebuiltFromObjectAndDeletedRow(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	consumer := benchConsumer(1)
	seedBucket(t, db, consumer, "cpu", 10)
	alloc := NewAllocator(db, allocatorTestCodec)
	w := newFastWatcher(t, db)

	start := currentRV(t, db)
	claim, err := alloc.Allocate(ctx, enforcementShapedClaim(consumer, 1))
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	key := claimKey(claim.Namespace, claim.Name)
	if data := addedRowData(t, db, key); data != nil {
		t.Fatalf("ADDED row stored %d bytes, want none", len(data))
	}
	stored := storedClaim(t, db, key)

	// While the claim is live the row is rebuilt from quota_objects.
	events := collectEvents(t, w, claim.Namespace, start, 1)
	assertClaimEvent(t, events[0], watch.Added, claim.ResourceVersion, stored)

	if err := alloc.Release(ctx, claim); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// After the delete it is rebuilt from the DELETED row, and a replay from
	// the old RV yields exactly what an un-elided changelog would.
	events = collectEvents(t, w, claim.Namespace, start, 2)
	assertClaimEvent(t, events[0], watch.Added, claim.ResourceVersion, stored)
	if events[1].Type != watch.Deleted {
		t.Fatalf("second event = %s, want DELETED", events[1].Type)
	}
	deleted := events[1].Object.(*quotainternal.QuotaClaim)
	if deleted.ResourceVersion == claim.ResourceVersion {
		t.Errorf("DELETED event carries the ADDED rv %s", deleted.ResourceVersion)
	}
	assertClaimEvent(t, events[1], watch.Deleted, deleted.ResourceVersion, stored)
}

func TestChangelogElision_UpdateBackfillsAddedRow(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	consumer := benchConsumer(2)
	seedBucket(t, db, consumer, "cpu", 10)
	alloc := NewAllocator(db, allocatorTestCodec)
	store := New(db, allocatorTestCodec, "")
	w := newFastWatcher(t, db)

	start := currentRV(t, db)
	claim, err := alloc.Allocate(ctx, enforcementShapedClaim(consumer, 2))
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	key := claimKey(claim.Namespace, claim.Name)
	stored := storedClaim(t, db, key)

	updated := &quotainternal.QuotaClaim{}
	if err := store.GuaranteedUpdate(ctx, key, updated, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			c := input.(*quotainternal.QuotaClaim)
			c.Labels["example.com/updated"] = "true"
			return c, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}

	// The update copied the claim's previous state into the ADDED row, so
	// the row no longer depends on the object.
	if data := addedRowData(t, db, key); data == nil {
		t.Fatal("ADDED row still has no data after an update")
	}

	events := collectEvents(t, w, claim.Namespace, start, 2)
	assertClaimEvent(t, events[0], watch.Added, claim.ResourceVersion, stored)
	assertClaimEvent(t, events[1], watch.Modified, updated.ResourceVersion, storedClaim(t, db, key))
}

// A NULL row that can no longer be rebuilt must end the watch with 410
// Gone rather than skip the event. That only happens if a writer broke the
// back-fill rule; simulate it by clearing an ADDED row whose successor is a
// MODIFIED row.
func TestChangelogElision_UnresolvableRowEndsWatchWithGone(t *testing.T) {
	db := getTestDB(t)
	cleanupTestData(t, db)
	t.Cleanup(func() { cleanupTestData(t, db) })

	ctx := context.Background()
	store := New(db, testCodec, "")
	w := pgwatch.NewWithHub(pgwatch.NewHub(db, "", pgwatch.Options{PollInterval: 50 * time.Millisecond}), testCodec, nil)
	t.Cleanup(w.Hub().Stop)

	start := currentRV(t, db)
	key := "/quota/default/broken"
	if err := store.Create(ctx, key, newTestObject("broken", "v1"), &testKind{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	out := &testKind{}
	if err := store.GuaranteedUpdate(ctx, key, out, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			o := input.(*testKind)
			o.Spec.Value = "v2"
			return o, nil, nil
		}, nil); err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if _, err := db.Exec(`UPDATE quota_changelog SET data = NULL WHERE key = $1 AND event_type = 'ADDED'`, key); err != nil {
		t.Fatal(err)
	}

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	wi, err := w.Watch(wctx, key, storage.ListOptions{ResourceVersion: fmt.Sprint(start), Predicate: storage.Everything}, nil)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer wi.Stop()
	select {
	case ev := <-wi.ResultChan():
		status, ok := ev.Object.(*metav1.Status)
		if ev.Type != watch.Error || !ok || status.Code != 410 {
			t.Fatalf("first event = %s %+v, want a 410 error", ev.Type, ev.Object)
		}
	case <-wctx.Done():
		t.Fatal("timed out waiting for the 410")
	}
}
